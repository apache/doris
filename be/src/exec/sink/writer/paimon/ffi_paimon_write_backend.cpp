// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/sink/writer/paimon/ffi_paimon_write_backend.h"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <paimon_rust/paimon.h>

#include <algorithm>
#include <mutex>
#include <string>
#include <utility>

#include "common/check.h"
#include "common/config.h"
#include "common/logging.h"
#include "format/parquet/arrow_memory_pool.h"
#include "format/table/paimon/paimon_arrow_block_convertor.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "util/defer_op.h"

namespace doris {
namespace {

constexpr size_t MAX_COMMIT_PAYLOAD = 8 * 1024 * 1024;
constexpr size_t DPCM_HEADER_BYTES = 12;
constexpr size_t MAX_COMMIT_TOTAL = 64 * 1024 * 1024;
constexpr size_t MAX_INPUT_BATCH_BYTES = 8 * 1024 * 1024;
constexpr size_t MAX_INPUT_ROW_BYTES = 1024 * 1024;
std::mutex rust_writer_admission_mutex;
int rust_active_writers = 0;

template <typename T, void (*Free)(T*)>
struct RustHandleDeleter {
    void operator()(T* handle) const { Free(handle); }
};

template <typename T, void (*Free)(T*)>
using RustHandle = std::unique_ptr<T, RustHandleDeleter<T, Free>>;

Status rust_status(paimon_error* error, const char* operation) {
    if (error == nullptr) {
        return Status::OK();
    }
    RustHandle<paimon_error, paimon_error_free> owned(error);
    if (error->code == 6) {
        return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                "Paimon Rust {} exhausted its resource budget", operation);
    }
    // SDK errors may include storage options. Do not copy credential values into query errors.
    return Status::InternalError("Paimon Rust {} failed, SDK error code={}", operation,
                                 error->code);
}

void append_big_endian(std::string& output, uint32_t value) {
    for (int shift : {24, 16, 8, 0}) {
        output.push_back(static_cast<char>(value >> shift));
    }
}

// Allocation stays on the attached pipeline thread. Rust can release imported
// buffers on a runtime thread after write() returns.
class RustWriterArrowPool final : public ArrowMemoryPool<> {
public:
    explicit RustWriterArrowPool(std::shared_ptr<ResourceContext> context)
            : _context(std::move(context)) {}

    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
        auto free_buffer = [&] { ArrowMemoryPool<>::Free(buffer, size, alignment); };
        if (!pthread_context_ptr_init && bthread_self() == 0) {
            SCOPED_ATTACH_TASK(_context);
            free_buffer();
            return;
        }
        if (thread_context()->is_attach_task()) {
            SCOPED_SWITCH_RESOURCE_CONTEXT(_context);
            free_buffer();
            return;
        }
        SCOPED_ATTACH_TASK(_context);
        free_buffer();
    }

private:
    std::shared_ptr<ResourceContext> _context;
};

} // namespace

class FfiPaimonWriteBackend::Impl {
public:
    explicit Impl(RuntimeState* state)
            : _arrow_pool(state->get_query_ctx()->resource_ctx()),
              _state(state),
              _tracker(state->query_mem_tracker()) {}

    ~Impl() {
        if (!_closed) {
            Status abort_status = abort();
            if (!abort_status.ok()) {
                LOG(WARNING) << "Paimon Rust abort failed: " << abort_status;
            }
            Status close_status = close();
            DORIS_CHECK(close_status.ok()) << close_status;
        }
    }

    Status open(const TPaimonTableSink& sink, RuntimeProfile* profile) {
        if (!sink.__isset.table_desc || sink.write_mode != TPaimonWriteMode::APPEND) {
            return Status::InvalidArgument("Paimon Rust requires resolved append table metadata");
        }
        const auto& desc = sink.table_desc;
        if (!desc.__isset.table_path || !desc.__isset.schema_json || !desc.__isset.database_name ||
            !desc.__isset.table_name || !desc.__isset.branch ||
            !desc.__isset.commit_message_version || desc.commit_message_version != 11) {
            return Status::NotSupported("Paimon Rust requires Java commit protocol version 11");
        }
        DORIS_CHECK(sink.__isset.commit_user && !sink.commit_user.empty());
        DORIS_CHECK(sink.__isset.column_names);
        _commit_version = desc.commit_message_version;
        RETURN_IF_ERROR(admit());

        RuntimeProfile* rust_profile = profile->create_child("RustPaimonWriteBackend", true, true);
        _reserved_counter = ADD_COUNTER(rust_profile, "ReservedMemoryBytes", TUnit::BYTES);
        _peak_counter = ADD_COUNTER(rust_profile, "PeakReservedMemoryBytes", TUnit::BYTES);
        _budget_counter = ADD_COUNTER(rust_profile, "FixedBudgetBytes", TUnit::BYTES);
        _prepare_timer = ADD_TIMER(rust_profile, "RustPrepareTime");
        _stop_timer = ADD_TIMER(rust_profile, "RustStopTime");
        COUNTER_SET(_budget_counter, _budget);

        auto resources = paimon_resource_context_create(_budget);
        RETURN_IF_ERROR(rust_status(resources.error, "create resource context"));
        _resources.reset(resources.context);

        RETURN_IF_ERROR(init_table(desc, sink.column_names));

        auto builder = paimon_table_new_write_builder_with_commit_user(_table.get(),
                                                                       sink.commit_user.c_str());
        RETURN_IF_ERROR(rust_status(builder.error, "create write builder"));
        _builder.reset(builder.write_builder);
        RETURN_IF_ERROR(
                rust_status(paimon_write_builder_with_resources(_builder.get(), _resources.get()),
                            "bind budget"));
        auto writer = paimon_write_builder_new_write(_builder.get());
        RETURN_IF_ERROR(rust_status(writer.error, "create writer"));
        _writer.reset(writer.write);
        return Status::OK();
    }

    class Writer final : public IPaimonWriter {
    public:
        explicit Writer(std::shared_ptr<Impl> impl) : _impl(std::move(impl)) {}
        Status write(RuntimeState* state, Block& block) override {
            DCHECK_EQ(state, _impl->_state);
            return _impl->write(block);
        }
        Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) override {
            return _impl->prepare(messages);
        }
        Status abort() override { return _impl->abort(); }

    private:
        std::shared_ptr<Impl> _impl;
    };

    Status write(Block& block) {
        RETURN_IF_CANCELLED(_state);
        DORIS_CHECK(_writer && !_prepared && !_closed);
        size_t begin = 0;
        while (begin < block.rows()) {
            size_t end = begin;
            size_t bytes = 0;
            while (end < block.rows() && end - begin < 65536) {
                size_t row_bytes = 8 * block.columns();
                for (const auto& item : block) {
                    row_bytes += item.column->serialize_size_at(end);
                }
                if (row_bytes > MAX_INPUT_ROW_BYTES) {
                    return Status::InvalidArgument("Paimon Rust input row exceeds 1 MiB");
                }
                if (row_bytes > MAX_INPUT_BATCH_BYTES - bytes) {
                    break;
                }
                bytes += row_bytes;
                ++end;
            }
            RETURN_IF_CANCELLED(_state);
            std::shared_ptr<arrow::RecordBatch> batch;
            RETURN_IF_ERROR(_convertor->convert_to_arrow(block, &_arrow_pool, &batch, begin, end));
            ArrowArray array {};
            ArrowSchema schema {};
            auto exported = arrow::ExportRecordBatch(*batch, &array, &schema);
            if (!exported.ok()) {
                return Status::InternalError("Cannot export Paimon Arrow batch: {}",
                                             exported.ToString());
            }
            Defer release_data([&] {
                if (array.release != nullptr) {
                    array.release(&array);
                }
                if (schema.release != nullptr) {
                    schema.release(&schema);
                }
            });
            RETURN_IF_ERROR(rust_status(
                    paimon_table_write_write_arrow_batch(_writer.get(), &array, &schema), "write"));
            update_metrics();
            begin = end;
        }
        return Status::OK();
    }

    Status prepare(std::vector<TPaimonCommitMessage>& messages) {
        RETURN_IF_CANCELLED(_state);
        DORIS_CHECK(_writer && !_prepared && !_closed);
        SCOPED_TIMER(_prepare_timer);
        auto prepared = paimon_table_write_prepare_commit(_writer.get());
        RETURN_IF_ERROR(rust_status(prepared.error, "prepare commit"));
        _messages.reset(prepared.messages);
        _prepared = true;
        size_t offset = 0;
        size_t total = 0;
        const size_t count = paimon_commit_messages_count(_messages.get());
        while (offset < count) {
            RETURN_IF_CANCELLED(_state);
            size_t chunk = std::min<size_t>(512, count - offset);
            paimon_bytes encoded {};
            for (;;) {
                paimon_error* error = paimon_commit_messages_serialize_java(
                        _messages.get(), _commit_version, offset, chunk,
                        MAX_COMMIT_PAYLOAD - DPCM_HEADER_BYTES, &encoded);
                if (error != nullptr && error->code == 6 && chunk > 1) {
                    paimon_error_free(error);
                    chunk = std::max<size_t>(1, chunk / 2);
                    continue;
                }
                RETURN_IF_ERROR(rust_status(error, "serialize commit messages"));
                break;
            }
            Defer release_bytes([&] { paimon_bytes_free(encoded); });
            const size_t framed_size = encoded.len + DPCM_HEADER_BYTES;
            if (framed_size > MAX_COMMIT_TOTAL - total) {
                return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                        "Paimon Rust commit payloads exceed 64 MiB");
            }
            std::string payload;
            payload.reserve(framed_size);
            payload.append("DPCM", 4);
            append_big_endian(payload, _commit_version);
            append_big_endian(payload, static_cast<uint32_t>(encoded.len));
            payload.append(reinterpret_cast<const char*>(encoded.data), encoded.len);
            TPaimonCommitMessage message;
            message.payload = std::move(payload);
            message.__isset.payload = true;
            messages.push_back(std::move(message));
            total += framed_size;
            offset += chunk;
        }
        update_metrics();
        return Status::OK();
    }

    Status abort() {
        RETURN_IF_ERROR(stop());
        // No messages have been published before backend.close() succeeds.
        if (_messages) {
            auto committer = paimon_write_builder_new_commit(_builder.get());
            RETURN_IF_ERROR(rust_status(committer.error, "create abort committer"));
            RustHandle<paimon_table_commit, paimon_table_commit_free> owned(committer.commit);
            RETURN_IF_ERROR(rust_status(paimon_table_commit_abort(owned.get(), _messages.get()),
                                        "abort prepared files"));
            _messages.reset();
        }
        return Status::OK();
    }

    Status close() {
        if (_closed) {
            return Status::OK();
        }
        RETURN_IF_ERROR(stop());
        update_metrics();
        _messages.reset();
        _builder.reset();
        _table.reset();
        _resources.reset();
        DORIS_CHECK(_arrow_pool.bytes_allocated() == 0);
        if (_admitted) {
            _tracker->release(_budget);
            std::lock_guard<std::mutex> guard(rust_writer_admission_mutex);
            --rust_active_writers;
            _admitted = false;
        }
        _closed = true;
        return Status::OK();
    }

private:
    Status init_table(const TPaimonTableDesc& desc, const std::vector<std::string>& column_names) {
        std::vector<paimon_option> options;
        options.reserve(desc.storage_options.size());
        for (const auto& [key, value] : desc.storage_options) {
            options.push_back({key.c_str(), value.c_str()});
        }
        auto table = paimon_table_from_schema_json(
                desc.table_path.c_str(), desc.schema_json.c_str(), desc.database_name.c_str(),
                desc.table_name.c_str(), desc.branch.c_str(), options.data(), options.size());
        RETURN_IF_ERROR(rust_status(table.error, "resolve table"));
        _table.reset(table.table);

        ArrowSchema c_schema {};
        Defer release_schema([&] {
            if (c_schema.release != nullptr) {
                c_schema.release(&c_schema);
            }
        });
        RETURN_IF_ERROR(rust_status(paimon_table_export_arrow_schema(_table.get(), &c_schema),
                                    "export table schema"));
        auto imported = arrow::ImportSchema(&c_schema);
        if (!imported.ok()) {
            return Status::InvalidArgument("Cannot import Paimon Arrow schema: {}",
                                           imported.status().ToString());
        }
        auto schema = std::move(imported).ValueUnsafe();
        if (schema->num_fields() != column_names.size()) {
            return Status::InvalidArgument(
                    "Paimon Rust input column count differs from table schema");
        }
        for (size_t i = 0; i < column_names.size(); ++i) {
            if (schema->field(static_cast<int>(i))->name() != column_names[i]) {
                return Status::InvalidArgument("Paimon Rust input columns must follow table order");
            }
        }
        _convertor =
                std::make_unique<paimon::PaimonArrowBlockConvertor>(schema, _state->timezone_obj());
        return _convertor->init();
    }

    Status admit() {
        const int64_t tasks = std::max<int64_t>(1, _state->task_num());
        const int64_t query_limit = _tracker->limit();
        _budget = config::paimon_rust_writer_memory_limit_bytes;
        if (query_limit > 0) {
            _budget = std::min(_budget, query_limit / tasks / 2);
        }
        if (_budget <= 0) {
            return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                    "Insufficient fixed budget for Paimon Rust writer");
        }
        {
            std::lock_guard<std::mutex> guard(rust_writer_admission_mutex);
            if (rust_active_writers >= config::paimon_rust_writer_max_concurrency) {
                return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                        "Paimon Rust writer concurrency limit {} reached; lower "
                        "parallel_pipeline_task_num or raise paimon_rust_writer_max_concurrency",
                        config::paimon_rust_writer_max_concurrency);
            }
            ++rust_active_writers;
        }
        Defer return_slot([&] {
            if (!_admitted) {
                std::lock_guard<std::mutex> guard(rust_writer_admission_mutex);
                --rust_active_writers;
            }
        });
        if (_budget >= MemInfo::soft_mem_limit() - GlobalMemoryArbitrator::process_memory_usage() ||
            GlobalMemoryArbitrator::sys_mem_available() - _budget <
                    MemInfo::sys_mem_available_warning_water_mark()) {
            return Status::Error<ErrorCode::PROCESS_MEMORY_EXCEEDED>(
                    "Insufficient process memory for Paimon Rust fixed budget");
        }
        if (!_tracker->try_consume(_budget)) {
            return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                    "Cannot charge Paimon Rust fixed budget to query");
        }
        Defer return_credit([&] {
            if (!_admitted) {
                _tracker->release(_budget);
            }
        });
        auto group = _state->get_query_ctx()->workload_group();
        if (group) {
            // Include this fixed credit in the group snapshot. Refreshing may
            // visit other queries, so keep it outside the writer admission lock.
            group->refresh_memory_usage();
            bool low = false;
            bool high = false;
            group->check_mem_used(&low, &high);
            if (high) {
                return Status::Error<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>(
                        "Insufficient workload group memory for Paimon Rust fixed budget");
            }
        }
        _admitted = true;
        return Status::OK();
    }

    Status stop() {
        if (_writer) {
            SCOPED_TIMER(_stop_timer);
            RETURN_IF_ERROR(rust_status(paimon_table_write_close(_writer.get()), "stop writer"));
            _writer.reset();
        }
        return Status::OK();
    }

    void update_metrics() {
        if (_resources) {
            paimon_resource_metrics metrics {};
            auto status = rust_status(paimon_resource_context_metrics(_resources.get(), &metrics),
                                      "read metrics");
            DORIS_CHECK(status.ok()) << status;
            COUNTER_SET(_reserved_counter, static_cast<int64_t>(metrics.reserved_memory_bytes));
            COUNTER_SET(_peak_counter, static_cast<int64_t>(metrics.peak_reserved_memory_bytes));
        }
    }

    RustWriterArrowPool _arrow_pool;
    RuntimeState* _state;
    std::unique_ptr<paimon::PaimonArrowBlockConvertor> _convertor;
    RustHandle<paimon_table, paimon_table_free> _table;
    RustHandle<paimon_resource_context, paimon_resource_context_free> _resources;
    RustHandle<paimon_write_builder, paimon_write_builder_free> _builder;
    RustHandle<paimon_table_write, paimon_table_write_free> _writer;
    RustHandle<paimon_commit_messages, paimon_commit_messages_free> _messages;
    int64_t _budget = 0;
    RuntimeProfile::Counter* _reserved_counter = nullptr;
    RuntimeProfile::Counter* _peak_counter = nullptr;
    RuntimeProfile::Counter* _budget_counter = nullptr;
    RuntimeProfile::Counter* _prepare_timer = nullptr;
    RuntimeProfile::Counter* _stop_timer = nullptr;
    std::shared_ptr<MemTrackerLimiter> _tracker;
    int32_t _commit_version = 0;
    bool _admitted = false;
    bool _prepared = false;
    bool _closed = false;
};

FfiPaimonWriteBackend::FfiPaimonWriteBackend() = default;
FfiPaimonWriteBackend::~FfiPaimonWriteBackend() = default;

Status FfiPaimonWriteBackend::open(const TPaimonTableSink& sink, RuntimeState* state,
                                   RuntimeProfile* profile) {
    DORIS_CHECK(state->get_query_ctx() != nullptr);
    DORIS_CHECK(state->query_mem_tracker() != nullptr);
    _impl = std::make_shared<Impl>(state);
    return _impl->open(sink, profile);
}

Status FfiPaimonWriteBackend::create_writer(std::unique_ptr<IPaimonWriter>* writer) {
    DORIS_CHECK(_impl != nullptr);
    *writer = std::make_unique<Impl::Writer>(_impl);
    return Status::OK();
}

Status FfiPaimonWriteBackend::close() {
    return _impl ? _impl->close() : Status::OK();
}

} // namespace doris
