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

#include "exec/sink/writer/paimon/cpp_paimon_write_backend.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <paimon/commit_message.h>
#include <paimon/file_store_write.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/record_batch.h>
#include <paimon/schema/schema.h>
#include <paimon/write_context.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/allocator.h"
#include "exec/sink/writer/paimon/doris_paimon_file_system.h"
#include "exec/sink/writer/paimon/paimon_resource_context.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/parquet/arrow_memory_pool.h"
#include "io/file_factory.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {

Status frame_paimon_cpp_commit(const std::string& data, int32_t version,
                               TPaimonCommitMessage* message) {
    // Same per-frame bound as the JNI codec. This does NOT bound SDK metadata accumulation.
    constexpr size_t max_payload = 8 * 1024 * 1024;
    if (message == nullptr || version < 0 || data.size() > max_payload - 12) {
        return Status::InvalidArgument("Invalid or oversized Paimon commit payload");
    }
    std::string framed("DPCM");
    auto append_int = [&](uint32_t value) {
        for (int shift = 24; shift >= 0; shift -= 8) {
            framed.push_back(static_cast<char>((value >> shift) & 0xff));
        }
    };
    append_int(static_cast<uint32_t>(version));
    append_int(static_cast<uint32_t>(data.size()));
    framed.append(data);
    message->__set_payload(std::move(framed));
    return Status::OK();
}

namespace {

Status sdk_status(const paimon::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    if (status.IsOutOfMemory()) {
        return Status::MemoryLimitExceeded(status.ToString());
    }
    return Status::InternalError("Paimon native: {}", status.ToString());
}

class QueryMemoryPool final : public paimon::MemoryPool {
public:
    QueryMemoryPool(std::shared_ptr<ResourceContext> context, uint64_t limit)
            : _context(std::move(context)), _limit(limit) {}

    void* Malloc(uint64_t size, uint64_t alignment = 0) override {
        if (size > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            (alignment != 0 && (alignment & (alignment - 1)) != 0)) {
            throw std::bad_alloc();
        }
        const uint64_t charged = std::max<uint64_t>(size, 1);
        uint64_t used = _used.load();
        do {
            if (used > _limit || charged > _limit - used) {
                throw std::bad_alloc();
            }
        } while (!_used.compare_exchange_weak(used, used + charged));
        void* ptr = nullptr;
        Defer rollback {[&] {
            if (!ptr) _used.fetch_sub(charged);
        }};
        try {
            ptr = with_paimon_resource_context(_context, [&] {
                enable_thread_catch_bad_alloc++;
                Defer restore {[&] { enable_thread_catch_bad_alloc--; }};
                return _allocator.alloc(charged, std::max<uint64_t>(alignment, 64));
            });
            if (ptr == nullptr) {
                throw std::bad_alloc();
            }
            auto peak = _peak.load();
            while (peak < used + charged && !_peak.compare_exchange_weak(peak, used + charged)) {
            }
            return ptr;
        } catch (const doris::Exception& e) {
            // Paimon's ArrowMemPoolAdaptor catches std::bad_alloc, not Doris exceptions.
            // Keep allocation failure inside the SDK's Status-based error/cleanup path.
            if (e.code() == ErrorCode::MEM_ALLOC_FAILED ||
                e.code() == ErrorCode::MEM_LIMIT_EXCEEDED ||
                e.code() == ErrorCode::BUFFER_ALLOCATION_FAILED ||
                e.code() == ErrorCode::QUERY_MEMORY_EXCEEDED ||
                e.code() == ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED ||
                e.code() == ErrorCode::PROCESS_MEMORY_EXCEEDED) {
                throw std::bad_alloc();
            }
            throw;
        }
    }

    void* Realloc(void* ptr, size_t old_size, size_t new_size, uint64_t alignment = 0) override {
        // Allocate-copy-free deliberately accounts for both live buffers at the expansion peak.
        // A failed allocation leaves ptr, its data and its accounting unchanged.
        void* replacement = Malloc(new_size, alignment);
        if (ptr != nullptr) {
            std::memcpy(replacement, ptr, std::min(old_size, new_size));
            Free(ptr, old_size);
        }
        return replacement;
    }

    void Free(void* ptr, uint64_t size) override {
        if (ptr != nullptr) {
            with_paimon_resource_context(
                    _context, [&] { _allocator.free(ptr, std::max<uint64_t>(size, 1)); });
            _used.fetch_sub(std::max<uint64_t>(size, 1));
        }
    }
    uint64_t CurrentUsage() const override { return _used.load(); }
    uint64_t MaxMemoryUsage() const override { return _peak.load(); }

private:
    std::shared_ptr<ResourceContext> _context;
    uint64_t _limit;
    Allocator<false> _allocator;
    std::atomic<uint64_t> _used {0};
    std::atomic<uint64_t> _peak {0};
};

// Conversion buffers can outlive Write() and be freed by an SDK worker thread.
class QueryArrowPool final : public ArrowMemoryPool<> {
public:
    explicit QueryArrowPool(std::shared_ptr<ResourceContext> context)
            : _context(std::move(context)) {}
    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        return with_paimon_resource_context(
                _context, [&] { return ArrowMemoryPool<>::Allocate(size, alignment, out); });
    }
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                             uint8_t** ptr) override {
        return with_paimon_resource_context(_context, [&] {
            return ArrowMemoryPool<>::Reallocate(old_size, new_size, alignment, ptr);
        });
    }
    void Free(uint8_t* ptr, int64_t size, int64_t alignment) override {
        with_paimon_resource_context(_context,
                                     [&] { ArrowMemoryPool<>::Free(ptr, size, alignment); });
    }
    std::string backend_name() const override { return "DorisPaimonConversion"; }

private:
    std::shared_ptr<ResourceContext> _context;
};

struct ExportOwner {
    ArrowArray array;
    std::shared_ptr<QueryArrowPool> pool;
    static void release(ArrowArray* exported) {
        auto* owner = static_cast<ExportOwner*>(exported->private_data);
        exported->release = nullptr;
        if (owner->array.release != nullptr) {
            owner->array.release(&owner->array);
        }
        delete owner; // pool outlives all buffer destructors invoked by original callback
    }
};

} // namespace

std::shared_ptr<paimon::MemoryPool> make_paimon_query_memory_pool(
        std::shared_ptr<ResourceContext> context, uint64_t limit) {
    return std::make_shared<QueryMemoryPool>(std::move(context), limit);
}

class CppPaimonWriteBackend::Impl {
public:
    Status open(const TPaimonTableSink& sink, RuntimeState* state, RuntimeProfile* profile) {
        if (!sink.__isset.table_descriptor || !sink.__isset.write_mode ||
            sink.write_mode != TPaimonWriteMode::APPEND || !sink.__isset.commit_user ||
            sink.commit_user.empty() || state->get_query_ctx() == nullptr ||
            state->query_mem_tracker() == nullptr) {
            return Status::InvalidArgument("Incomplete native Paimon write description");
        }
        const auto& desc = sink.table_descriptor;
        if (!sink.__isset.column_names || sink.column_names.empty()) {
            return Status::InvalidArgument("Native Paimon column order is missing");
        }
        // Reject configuration mismatch rather than silently falling back after dispatch.
        if (!desc.__isset.storage || desc.root_path.empty() || desc.storage.root_path.empty() ||
            (desc.storage.file_type != TFileType::FILE_LOCAL &&
             desc.storage.file_type != TFileType::FILE_S3)) {
            return Status::NotSupported("Missing or unsupported Doris Paimon storage descriptor");
        }
        const auto& storage = desc.storage;
        const auto& root_path = desc.root_path;
        // FE selects supported write options and normalizes storage locations. The filesystem
        // adapter owns logical-to-storage path mapping; the backend only consumes the descriptor.
        auto table_schema = paimon::DataSchema::FromJson(desc.schema_json);
        if (!table_schema.ok()) return sdk_status(table_schema.status());
        // Import through C Data, never pass Arrow C++ objects between the two SDK versions.
        auto c_schema_result = table_schema.value()->GetArrowSchema();
        if (!c_schema_result.ok()) return sdk_status(c_schema_result.status());
        auto c_schema = std::move(c_schema_result).value();
        Defer release_schema {[&] {
            if (c_schema->release) c_schema->release(c_schema.get());
        }};
        auto schema = arrow::ImportSchema(c_schema.get());
        if (!schema.ok())
            return Status::InternalError("Paimon Arrow schema: {}", schema.status().ToString());
        _schema = std::move(schema).ValueOrDie();
        auto context = state->get_query_ctx()->resource_ctx();
        int64_t limit = config::paimon_cpp_writer_memory_limit_bytes;
        const auto query_limit = state->query_mem_tracker()->limit();
        if (query_limit > 0) {
            limit = std::min(limit, query_limit / std::max(1, state->task_num()));
        }
        if (limit <= 0) return Status::MemoryLimitExceeded("No Paimon native writer memory budget");
        _pool = make_paimon_query_memory_pool(context, limit);
        _arrow_pool = std::make_shared<QueryArrowPool>(context);
        COUNTER_SET(ADD_COUNTER(profile, "PaimonSdkPoolLimit", TUnit::BYTES), limit);
        _sdk_pool_peak = ADD_COUNTER(profile, "PaimonSdkPoolPeak", TUnit::BYTES);
        _conversion_peak = ADD_COUNTER(profile, "PaimonArrowConversionPeak", TUnit::BYTES);
        profile->add_info_string("PaimonMemoryScope",
                                 "SDK pool limit excludes conversion, IO and non-pool allocations");
        io::FSPropertiesRef fs_properties(storage.file_type);
        fs_properties.properties = &storage.properties;
        io::FileDescription file_description {.path = storage.root_path};
        auto fs = with_paimon_resource_context(
                context, [&] { return FileFactory::create_fs(fs_properties, file_description); });
        if (!fs.has_value()) return fs.error();
        _filesystem = std::make_shared<DorisPaimonFileSystem>(std::move(fs.value()), root_path,
                                                              storage.root_path, context);
        paimon::WriteContextBuilder builder(root_path, sink.commit_user);
        const auto& options = table_schema.value()->Options();
        auto branch = options.find("branch");
        auto ctx = builder.WithTableSchema(table_schema.value())
                           .WithBranch(branch == options.end() ? "main" : branch->second)
                           .WithFileSystem(_filesystem)
                           .WithMemoryPool(_pool)
                           .WithWriteSchema(sink.column_names)
                           .Finish();
        if (!ctx.ok()) return sdk_status(ctx.status());
        auto writer = paimon::FileStoreWrite::Create(std::move(ctx).value());
        if (!writer.ok()) return sdk_status(writer.status());
        _sdk = std::move(writer).value();
        profile->add_info_string("PaimonWriteBackend", "CPP");
        return Status::OK();
    }

    Status write(RuntimeState* state, Block& block) {
        if (block.rows() == 0) return Status::OK();
        DCHECK(_sdk);
        if (state->is_cancelled()) return Status::Cancelled("Paimon native write cancelled");
        std::shared_ptr<arrow::RecordBatch> batch;
        RETURN_IF_ERROR(convert_to_arrow_batch(block, _schema, _arrow_pool.get(), &batch,
                                               state->timezone_obj()));
        auto validation = batch->Validate();
        if (!validation.ok()) {
            return Status::InvalidArgument("Paimon Arrow batch: {}", validation.ToString());
        }
        ArrowArray data {};
        Defer release {[&] {
            if (data.release) data.release(&data);
        }};
        auto status = arrow::ExportRecordBatch(*batch, &data);
        if (!status.ok()) return Status::InternalError("Arrow export: {}", status.ToString());
        // Allocate owner before overwriting callback; the guard still releases data on failure.
        auto owner = std::make_unique<ExportOwner>();
        owner->array = data;
        owner->pool = _arrow_pool;
        data.private_data = owner.release();
        data.release = ExportOwner::release;
        auto record = paimon::RecordBatchBuilder(&data).Finish();
        if (!record.ok()) return sdk_status(record.status());
        return sdk_status(_sdk->Write(std::move(record).value()));
    }

    Status prepare(std::vector<TPaimonCommitMessage>& messages) {
        DCHECK(_sdk);
        auto commits = _sdk->PrepareCommit();
        if (!commits.ok()) return sdk_status(commits.status());
        DBUG_EXECUTE_IF("CppPaimonWriteBackend.prepare.serialize_oom", { throw std::bad_alloc(); });
        std::vector<TPaimonCommitMessage> staged;
        // One message per frame avoids materializing another full serialized list.
        for (const auto& commit : commits.value()) {
            auto bytes = paimon::CommitMessage::SerializeList({commit}, _pool);
            if (!bytes.ok()) return sdk_status(bytes.status());
            TPaimonCommitMessage message;
            RETURN_IF_ERROR(frame_paimon_cpp_commit(
                    bytes.value(), paimon::CommitMessage::CurrentVersion(), &message));
            staged.push_back(std::move(message));
        }
        messages.swap(staged);
        return Status::OK();
    }

    Status close() {
        Status result;
        if (_sdk) {
            result = paimon_write_call([&] {
                // Destroy the SDK even if Close throws; never retry the failed instance.
                // SDK destruction must drain background users before releasing its pools.
                auto sdk = std::move(_sdk);
                auto status = sdk_status(sdk->Close());
                sdk.reset();
                DBUG_EXECUTE_IF("CppPaimonWriteBackend.close.inject_failure", {
                    status = Status::InternalError("Injected Paimon native close failure");
                });
                return status;
            });
        }
        _schema.reset();
        if (_sdk_pool_peak && _pool) COUNTER_SET(_sdk_pool_peak, _pool->MaxMemoryUsage());
        if (_conversion_peak && _arrow_pool)
            COUNTER_SET(_conversion_peak, _arrow_pool->max_memory());
        _arrow_pool.reset();
        _pool.reset();
        // Files remain owned until message handoff. Abort or filesystem destruction removes
        // them, including when prepare, SDK close or RuntimeState message retention fails.
        return result;
    }

    void on_commit_messages_transferred() {
        DCHECK(!_sdk);
        if (_filesystem) _filesystem->release_owned_files();
        _filesystem.reset();
    }

    std::shared_ptr<paimon::MemoryPool> _pool;
    std::shared_ptr<QueryArrowPool> _arrow_pool;
    std::shared_ptr<arrow::Schema> _schema;
    std::shared_ptr<DorisPaimonFileSystem> _filesystem;
    std::unique_ptr<paimon::FileStoreWrite> _sdk;
    RuntimeProfile::Counter* _sdk_pool_peak = nullptr;
    RuntimeProfile::Counter* _conversion_peak = nullptr;
};

class CppPaimonWriteBackend::Writer final : public IPaimonWriter {
public:
    explicit Writer(std::shared_ptr<Impl> impl) : _impl(std::move(impl)) {}
    Status write(RuntimeState* state, Block& block) override {
        return paimon_write_call([&] { return _impl->write(state, block); });
    }
    Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) override {
        return paimon_write_call([&] { return _impl->prepare(messages); });
    }
    Status abort() override {
        auto status = _impl->close();
        if (_impl->_filesystem) {
            auto cleanup = paimon_write_call(
                    [&] { return sdk_status(_impl->_filesystem->cleanup_owned_files()); });
            if (status.ok()) status = std::move(cleanup);
        }
        return status;
    }

private:
    std::shared_ptr<Impl> _impl;
};

CppPaimonWriteBackend::CppPaimonWriteBackend() : _impl(std::make_shared<Impl>()) {}
CppPaimonWriteBackend::~CppPaimonWriteBackend() {
    try {
        (void)close();
    } catch (...) { /* never propagate through a destructor */
    }
}
Status CppPaimonWriteBackend::open(const TPaimonTableSink& sink, RuntimeState* state,
                                   RuntimeProfile* profile) {
    return paimon_write_call([&] { return _impl->open(sink, state, profile); });
}
Status CppPaimonWriteBackend::create_writer(std::unique_ptr<IPaimonWriter>* writer) {
    DCHECK(_impl->_sdk);
    *writer = std::make_unique<Writer>(_impl);
    return Status::OK();
}
Status CppPaimonWriteBackend::close() {
    return _impl->close();
}
void CppPaimonWriteBackend::on_commit_messages_transferred() {
    _impl->on_commit_messages_transferred();
}

} // namespace doris
