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
#include <arrow/compute/api_vector.h>
#include <arrow/compute/exec.h>
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
#include <map>
#include <tuple>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/allocator.h"
#include "exec/partitioner/external/paimon_row_hash_partition_function.h"
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

Status validate_paimon_cpp_batch(const arrow::RecordBatch& batch, const arrow::Schema& schema) {
    if (batch.num_columns() != schema.num_fields() ||
        !batch.schema()->Equals(schema, /*check_metadata=*/true)) {
        return Status::InvalidArgument(
                "Paimon Arrow batch schema differs from the pinned table schema");
    }
    // RecordBatch::Make can associate a schema with arrays of different types. In particular,
    // the generic converter may promote binary/string to large_binary/large_utf8. Never let
    // the schema-less C Data boundary reinterpret their 64-bit offsets as 32-bit offsets.
    for (int i = 0; i < batch.num_columns(); ++i) {
        if (!batch.column(i)->type()->Equals(schema.field(i)->type(), /*check_metadata=*/true)) {
            return Status::InvalidArgument(
                    "Paimon Arrow column {} type mismatch: expected {}, got {}",
                    schema.field(i)->name(), schema.field(i)->type()->ToString(),
                    batch.column(i)->type()->ToString());
        }
    }
    auto status = batch.Validate();
    return status.ok() ? Status::OK()
                       : Status::InvalidArgument("Paimon Arrow batch: {}", status.ToString());
}

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

constexpr int32_t UNSPECIFIED_BUCKET = std::numeric_limits<int32_t>::min();

struct PaimonRoute {
    std::map<std::string, std::string> partition;
    int32_t bucket = UNSPECIFIED_BUCKET;

    bool operator<(const PaimonRoute& other) const {
        return std::tie(partition, bucket) < std::tie(other.partition, other.bucket);
    }
};

struct RoutedArrowBatch {
    PaimonRoute route;
    std::shared_ptr<arrow::RecordBatch> batch;
};

Status take_paimon_rows(const std::shared_ptr<arrow::RecordBatch>& input,
                        const std::vector<uint64_t>& rows, arrow::MemoryPool* pool,
                        std::shared_ptr<arrow::RecordBatch>* output) {
    if (rows.size() == static_cast<size_t>(input->num_rows())) {
        *output = input;
        return Status::OK();
    }
    arrow::UInt64Builder builder(pool);
    auto append = builder.AppendValues(rows);
    if (!append.ok()) {
        return Status::InternalError("Paimon route indexes: {}", append.ToString());
    }
    std::shared_ptr<arrow::UInt64Array> indexes;
    auto finish = builder.Finish(&indexes);
    if (!finish.ok()) {
        return Status::InternalError("Paimon route indexes: {}", finish.ToString());
    }
    arrow::compute::ExecContext exec_context(pool);
    arrow::ArrayVector columns;
    columns.reserve(input->num_columns());
    for (const auto& column : input->columns()) {
        auto taken = arrow::compute::Take(
                *column, *indexes, arrow::compute::TakeOptions::NoBoundsCheck(), &exec_context);
        if (!taken.ok()) {
            return Status::InternalError("Paimon route batch: {}", taken.status().ToString());
        }
        columns.push_back(std::move(taken).ValueOrDie());
    }
    *output = arrow::RecordBatch::Make(input->schema(), rows.size(), std::move(columns));
    return Status::OK();
}

} // namespace

std::shared_ptr<paimon::MemoryPool> make_paimon_query_memory_pool(
        std::shared_ptr<ResourceContext> context, uint64_t limit) {
    return std::make_shared<QueryMemoryPool>(std::move(context), limit);
}

class CppPaimonWriteBackend::Impl {
public:
    Status open(const TPaimonTableSink& sink, RuntimeState* state, RuntimeProfile* profile) {
        if (!sink.__isset.table_descriptor || !sink.__isset.write_mode ||
            !sink.__isset.commit_user || sink.commit_user.empty() ||
            state->get_query_ctx() == nullptr || state->query_mem_tracker() == nullptr) {
            return Status::InvalidArgument("Incomplete native Paimon write description");
        }
        if (sink.write_mode != TPaimonWriteMode::APPEND &&
            sink.write_mode != TPaimonWriteMode::OVERWRITE) {
            return Status::NotSupported("Native Paimon supports append and overwrite writes");
        }
        const auto& desc = sink.table_descriptor;
        if (!sink.__isset.column_names || sink.column_names.empty()) {
            return Status::InvalidArgument("Native Paimon column order is missing");
        }
        // Reject configuration mismatch rather than silently falling back after dispatch.
        if (!desc.__isset.storage || desc.root_path.empty() || desc.storage.root_path.empty() ||
            (desc.storage.file_type != TFileType::FILE_LOCAL &&
             desc.storage.file_type != TFileType::FILE_S3 &&
             desc.storage.file_type != TFileType::FILE_HDFS)) {
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
        auto table_arrow_schema = std::move(schema).ValueOrDie();
        arrow::FieldVector write_fields;
        write_fields.reserve(sink.column_names.size());
        std::vector<std::string> write_column_names;
        write_column_names.reserve(sink.column_names.size());
        for (const auto& name : sink.column_names) {
            auto field = table_arrow_schema->GetFieldByName(name);
            if (!field) {
                return Status::InvalidArgument("Native Paimon write column does not exist: {}",
                                               name);
            }
            write_fields.push_back(std::move(field));
            write_column_names.push_back(name);
        }
        // FileStoreWrite::WithWriteSchema consumes children in precisely this order. Keep each
        // SDK field (including nested Paimon IDs and Variant metadata) as the Arrow conversion
        // target instead of requiring a full table-ordered block.
        _schema = arrow::schema(std::move(write_fields));
        auto context = state->get_query_ctx()->resource_ctx();
        int64_t limit = config::paimon_cpp_writer_memory_limit_bytes;
        const auto query_limit = state->query_mem_tracker()->limit();
        if (query_limit > 0) {
            limit = std::min(limit, query_limit / std::max(1, state->task_num()));
        }
        if (limit <= 0) return Status::MemoryLimitExceeded("No Paimon native writer memory budget");
        _pool = make_paimon_query_memory_pool(context, limit);
        _arrow_pool = std::make_shared<QueryArrowPool>(context);

        _partition_keys = table_schema.value()->PartitionKeys();
        _partition_indexes.reserve(_partition_keys.size());
        for (const auto& key : _partition_keys) {
            int index = _schema->GetFieldIndex(key);
            if (index < 0) {
                return Status::InvalidArgument("Native Paimon partition column is missing: {}",
                                               key);
            }
            _partition_indexes.push_back(index);
        }
        const auto& options = table_schema.value()->Options();
        auto default_partition = options.find("partition.default-name");
        _default_partition_name = default_partition == options.end() ? "__DEFAULT_PARTITION__"
                                                                     : default_partition->second;
        _num_buckets = table_schema.value()->NumBuckets();
        if (_num_buckets > 0) {
            const auto& bucket_keys = table_schema.value()->BucketKeys();
            _bucket_indexes.reserve(bucket_keys.size());
            for (const auto& key : bucket_keys) {
                int index = _schema->GetFieldIndex(key);
                if (index < 0) {
                    return Status::InvalidArgument("Native Paimon bucket column is missing: {}",
                                                   key);
                }
                _bucket_indexes.push_back(index);
            }
            if (_bucket_indexes.empty()) {
                return Status::InvalidArgument("Native Paimon fixed-bucket keys are missing");
            }
        } else if (!table_schema.value()->PrimaryKeys().empty()) {
            return Status::NotSupported(
                    "Native Paimon dynamic and postpone primary-key routing is not enabled");
        }
        COUNTER_SET(ADD_COUNTER(profile, "PaimonSdkPoolLimit", TUnit::BYTES), limit);
        _sdk_pool_peak = ADD_COUNTER(profile, "PaimonSdkPoolPeak", TUnit::BYTES);
        _conversion_peak = ADD_COUNTER(profile, "PaimonArrowConversionPeak", TUnit::BYTES);
        profile->add_info_string("PaimonMemoryScope",
                                 "SDK pool limit excludes conversion, IO and non-pool allocations");
        io::FSPropertiesRef fs_properties(storage.file_type);
        fs_properties.properties = &storage.properties;
        io::FileDescription file_description;
        file_description.path = storage.root_path;
        auto fs = with_paimon_resource_context(
                context, [&] { return FileFactory::create_fs(fs_properties, file_description); });
        if (!fs.has_value()) return fs.error();
        _filesystem = std::make_shared<DorisPaimonFileSystem>(std::move(fs.value()), root_path,
                                                              storage.root_path, context);
        paimon::WriteContextBuilder builder(root_path, sink.commit_user);
        auto branch = options.find("branch");
        auto ctx = builder.WithTableSchema(table_schema.value())
                           .WithBranch(branch == options.end() ? "main" : branch->second)
                           .WithFileSystem(_filesystem)
                           .WithMemoryPool(_pool)
                           .WithIgnorePreviousFiles(sink.write_mode == TPaimonWriteMode::OVERWRITE)
                           .WithWriteSchema(write_column_names)
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
        RETURN_IF_ERROR(validate_paimon_cpp_batch(*batch, *_schema));
        std::vector<RoutedArrowBatch> routed;
        RETURN_IF_ERROR(route_batch(block, batch, &routed));
        for (auto& item : routed) {
            RETURN_IF_ERROR(write_batch(std::move(item)));
        }
        return Status::OK();
    }

    Status route_batch(const Block& block, const std::shared_ptr<arrow::RecordBatch>& batch,
                       std::vector<RoutedArrowBatch>* routed) {
        std::vector<int32_t> bucket_ids(batch->num_rows(), UNSPECIFIED_BUCKET);
        const auto& fields = block.get_columns_with_type_and_name();
        if (_num_buckets > 0) {
            RETURN_IF_ERROR(paimon_native::fixed_bucket_ids(_bucket_indexes, fields, _num_buckets,
                                                            bucket_ids));
        }
        std::vector<std::map<std::string, std::string>> partitions;
        RETURN_IF_ERROR(paimon_native::partition_values(_partition_keys, _partition_indexes, fields,
                                                        _default_partition_name, partitions));

        std::map<PaimonRoute, std::vector<uint64_t>> groups;
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            PaimonRoute route;
            route.bucket = bucket_ids[row];
            route.partition = std::move(partitions[row]);
            groups[std::move(route)].push_back(row);
        }

        routed->clear();
        routed->reserve(groups.size());
        for (auto& [route, rows] : groups) {
            std::shared_ptr<arrow::RecordBatch> group;
            RETURN_IF_ERROR(take_paimon_rows(batch, rows, _arrow_pool.get(), &group));
            routed->push_back({std::move(route), std::move(group)});
        }
        return Status::OK();
    }

    Status write_batch(RoutedArrowBatch item) {
        ArrowArray data {};
        Defer release {[&] {
            if (data.release) data.release(&data);
        }};
        auto status = arrow::ExportRecordBatch(*item.batch, &data);
        if (!status.ok()) return Status::InternalError("Arrow export: {}", status.ToString());
        // Allocate owner before overwriting callback; the guard still releases data on failure.
        auto owner = std::make_unique<ExportOwner>();
        owner->array = data;
        owner->pool = _arrow_pool;
        data.private_data = owner.release();
        data.release = ExportOwner::release;
        paimon::RecordBatchBuilder builder(&data);
        builder.SetPartition(item.route.partition);
        if (item.route.bucket != UNSPECIFIED_BUCKET) {
            builder.SetBucket(item.route.bucket);
        }
        auto record = builder.Finish();
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
        if (_sdk_pool_peak && _pool)
            COUNTER_SET(_sdk_pool_peak, static_cast<int64_t>(_pool->MaxMemoryUsage()));
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
    std::vector<std::string> _partition_keys;
    std::vector<int> _partition_indexes;
    std::vector<int> _bucket_indexes;
    std::string _default_partition_name;
    int32_t _num_buckets = -1;
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
