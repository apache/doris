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

#include <limits>

#include "common/exception.h"

#ifdef USE_PAIMON_CPP
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <dlfcn.h>
#include <paimon/commit_message.h>
#include <paimon/file_store_write.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/record_batch.h>
#include <paimon/write_context.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <mutex>

#include "common/config.h"
#include "common/logging.h"
#include "core/allocator.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/parquet/arrow_memory_pool.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#endif

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

#ifdef USE_PAIMON_CPP
namespace {

template <typename F>
auto with_query(const std::shared_ptr<ResourceContext>& context, F&& f) -> decltype(f()) {
    if (!pthread_context_ptr_init && bthread_self() == 0) {
        SCOPED_ATTACH_TASK(context);
        return f();
    }
    if (thread_context()->is_attach_task()) {
        SCOPED_SWITCH_RESOURCE_CONTEXT(context);
        return f();
    }
    SCOPED_ATTACH_TASK(context);
    return f();
}

Status sdk_status(const paimon::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    if (status.IsOutOfMemory()) {
        return Status::MemoryLimitExceeded(status.ToString());
    }
    return Status::InternalError("Paimon native: {}", status.ToString());
}

template <typename F>
Status invoke_sdk(F&& function) {
    try {
        return function();
    } catch (const doris::Exception& e) {
        return e.to_status();
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Paimon native allocation failed");
    } catch (const std::exception& e) {
        return Status::InternalError("Paimon native exception: {}", e.what());
    } catch (...) {
        return Status::InternalError("Paimon native unknown exception");
    }
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
        try {
            void* ptr = with_query(_context, [&] {
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
        } catch (...) {
            _used.fetch_sub(charged);
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
            with_query(_context, [&] { _allocator.free(ptr, std::max<uint64_t>(size, 1)); });
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
class QueryArrowPool final : public arrow::MemoryPool {
public:
    explicit QueryArrowPool(std::shared_ptr<ResourceContext> context)
            : _context(std::move(context)) {}
    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        return with_query(_context, [&] { return _delegate.Allocate(size, alignment, out); });
    }
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                             uint8_t** ptr) override {
        return with_query(_context,
                          [&] { return _delegate.Reallocate(old_size, new_size, alignment, ptr); });
    }
    void Free(uint8_t* ptr, int64_t size, int64_t alignment) override {
        with_query(_context, [&] { _delegate.Free(ptr, size, alignment); });
    }
    int64_t bytes_allocated() const override { return _delegate.bytes_allocated(); }
    int64_t max_memory() const override { return _delegate.max_memory(); }
    int64_t total_bytes_allocated() const override { return _delegate.total_bytes_allocated(); }
    int64_t num_allocations() const override { return _delegate.num_allocations(); }
    std::string backend_name() const override { return "DorisPaimonConversion"; }

private:
    std::shared_ptr<ResourceContext> _context;
    ArrowMemoryPool<> _delegate;
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

Status load_plugins() {
    // No dlclose: registered factories and vtables must remain valid for process lifetime.
    static std::mutex mutex;
    static void* handles[3] {};
    std::lock_guard<std::mutex> lock(mutex);
#ifdef __APPLE__
    const char* names[] = {"libpaimon_local_file_system.dylib",
                           "libpaimon_parquet_file_format.dylib",
                           "libpaimon_avro_file_format.dylib"};
#else
    const char* names[] = {"libpaimon_local_file_system.so", "libpaimon_parquet_file_format.so",
                           "libpaimon_avro_file_format.so"};
#endif
    for (size_t i = 0; i < 3; ++i) {
        if (handles[i] == nullptr) {
            handles[i] = dlopen(names[i], RTLD_NOW | RTLD_LOCAL);
            if (handles[i] == nullptr) {
                return Status::InternalError("Cannot load Paimon plugin {}: {}", names[i],
                                             dlerror());
            }
        }
    }
    return Status::OK();
}

std::shared_ptr<arrow::DataType> arrow_type(const std::string& type) {
    if (type == "BOOLEAN") return arrow::boolean();
    if (type == "TINYINT") return arrow::int8();
    if (type == "SMALLINT") return arrow::int16();
    if (type == "INTEGER") return arrow::int32();
    if (type == "BIGINT") return arrow::int64();
    if (type == "FLOAT") return arrow::float32();
    if (type == "DOUBLE") return arrow::float64();
    if (type == "VARCHAR") return arrow::utf8();
    if (type == "VARBINARY") return arrow::binary();
    return nullptr;
}

} // namespace

class CppPaimonWriteBackend::Impl {
public:
    Status open(const TPaimonTableSink& sink, RuntimeState* state, RuntimeProfile* profile) {
        if (!sink.__isset.cpp_descriptor || sink.cpp_descriptor.version != 1 ||
            !sink.__isset.write_mode || sink.write_mode != TPaimonWriteMode::APPEND ||
            !sink.__isset.commit_user || sink.commit_user.empty() ||
            state->get_query_ctx() == nullptr || state->query_mem_tracker() == nullptr) {
            return Status::InvalidArgument("Incomplete native Paimon v1 write description");
        }
        const auto& desc = sink.cpp_descriptor;
        if (desc.columns.empty() || !sink.__isset.column_names ||
            desc.columns.size() != sink.column_names.size()) {
            return Status::InvalidArgument("Native Paimon column order is missing");
        }
        // Reject configuration mismatch rather than silently falling back after dispatch.
        if (desc.root_path.empty() ||
            (desc.root_path.front() != '/' && desc.root_path.compare(0, 7, "file://") != 0)) {
            return Status::NotSupported("Native Paimon v1 only supports local POSIX storage");
        }
        auto options = desc.options;
        if (options["file.format"] != "parquet" || options["write-only"] != "true" ||
            (options.count("bucket") && options["bucket"] != "-1")) {
            return Status::NotSupported("Native Paimon v1 requires write-only Parquet append");
        }
        RETURN_IF_ERROR(load_plugins());
        // The v1 wire audit is for 0.3.0's version 12 append messages without indexes.
        // Do not silently accept a future SDK's changed serialization layout.
        if (paimon::CommitMessage::CurrentVersion() != 12) {
            return Status::NotSupported("Unvalidated Paimon native commit serializer version");
        }
        arrow::FieldVector fields;
        for (size_t i = 0; i < desc.columns.size(); ++i) {
            const auto& column = desc.columns[i];
            auto type = arrow_type(column.type);
            if (!type || column.name != sink.column_names[i]) {
                return Status::NotSupported("Unsupported native Paimon column {}", column.name);
            }
            fields.push_back(arrow::field(column.name, type, column.nullable));
        }
        _schema = arrow::schema(fields);
        auto context = state->get_query_ctx()->resource_ctx();
        int64_t limit = config::paimon_cpp_writer_memory_limit_bytes;
        const auto query_limit = state->query_mem_tracker()->limit();
        if (query_limit > 0) {
            limit = std::min(limit, query_limit / std::max(1, state->task_num()));
        }
        if (limit <= 0) return Status::MemoryLimitExceeded("No Paimon native writer memory budget");
        _pool = std::make_shared<QueryMemoryPool>(context, limit);
        _arrow_pool = std::make_shared<QueryArrowPool>(context);
        options["doris.expected-schema-id"] = std::to_string(desc.schema_id);
        options["file-system"] = "local";
        paimon::WriteContextBuilder builder(desc.root_path, sink.commit_user);
        auto ctx = builder.SetOptions(options)
                           .WithMemoryPool(_pool)
                           .WithWriteSchema(sink.column_names)
                           .Finish();
        if (!ctx.ok()) return sdk_status(ctx.status());
        auto writer = paimon::FileStoreWrite::Create(std::move(ctx).value());
        if (!writer.ok()) return sdk_status(writer.status());
        _sdk = std::move(writer).value();
        profile->add_info_string("PaimonWriteBackend", "CPP (experimental v1)");
        _opened = true;
        return Status::OK();
    }

    Status write(RuntimeState* state, Block& block) {
        if (!_opened || _failed || _prepared || _closed) {
            return Status::InternalError("Paimon native writer is not writable");
        }
        if (state->is_cancelled()) return Status::Cancelled("Paimon native write cancelled");
        if (block.rows() == 0) return Status::OK();
        std::shared_ptr<arrow::RecordBatch> batch;
        RETURN_IF_ERROR(convert_to_arrow_batch(block, _schema, _arrow_pool.get(), &batch,
                                               state->timezone_obj()));
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
        if (!_opened || _failed || _prepared || _closed) {
            return Status::InternalError("Paimon native writer cannot prepare");
        }
        _prepared = true; // never retry a partially completed prepare
        auto commits = _sdk->PrepareCommit();
        if (!commits.ok()) return sdk_status(commits.status());
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
        if (_closed) return Status::OK();
        _closed = true;
        Status result = Status::OK();
        if (_sdk) {
            result = invoke_sdk([&] { return sdk_status(_sdk->Close()); });
            // v1 is restricted to write-only append (no compaction). Full recovery and
            // partial-prepare file ownership remain explicit follow-up release gates.
            _sdk.reset();
        }
        _schema.reset();
        _arrow_pool.reset();
        _pool.reset();
        return result;
    }

    std::shared_ptr<QueryMemoryPool> _pool;
    std::shared_ptr<QueryArrowPool> _arrow_pool;
    std::shared_ptr<arrow::Schema> _schema;
    std::unique_ptr<paimon::FileStoreWrite> _sdk;
    bool _opened = false;
    bool _failed = false;
    bool _prepared = false;
    bool _closed = false;
};

class CppPaimonWriteBackend::Writer final : public IPaimonWriter {
public:
    explicit Writer(std::shared_ptr<Impl> impl) : _impl(std::move(impl)) {}
    Status write(RuntimeState* state, Block& block) override {
        auto result = invoke_sdk([&] { return _impl->write(state, block); });
        if (!result.ok()) _impl->_failed = true;
        return result;
    }
    Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) override {
        auto result = invoke_sdk([&] { return _impl->prepare(messages); });
        if (!result.ok()) _impl->_failed = true;
        return result;
    }
    Status abort() override {
        _impl->_failed = true;
        return invoke_sdk([&] { return _impl->close(); });
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
    return invoke_sdk([&] { return _impl->open(sink, state, profile); });
}
Status CppPaimonWriteBackend::create_writer(std::unique_ptr<IPaimonWriter>* writer) {
    if (!_impl->_opened || _impl->_closed)
        return Status::InternalError("Native writer is not open");
    *writer = std::make_unique<Writer>(_impl);
    return Status::OK();
}
Status CppPaimonWriteBackend::close() {
    return invoke_sdk([&] { return _impl->close(); });
}
#else
class CppPaimonWriteBackend::Impl {};
CppPaimonWriteBackend::CppPaimonWriteBackend() = default;
CppPaimonWriteBackend::~CppPaimonWriteBackend() = default;
Status CppPaimonWriteBackend::open(const TPaimonTableSink&, RuntimeState*, RuntimeProfile*) {
    return Status::NotSupported(
            "This BE was built without WITH_PAIMON_CPP; no runtime JNI fallback");
}
Status CppPaimonWriteBackend::create_writer(std::unique_ptr<IPaimonWriter>*) {
    return Status::NotSupported("This BE was built without WITH_PAIMON_CPP");
}
Status CppPaimonWriteBackend::close() {
    return Status::OK();
}
#endif

} // namespace doris
