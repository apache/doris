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

#include "vec/sink/writer/paimon/vpaimon_partition_writer.h"

#include <gen_cpp/DataSinks_types.h>

#include <map>
#include <utility>

#include "common/metrics/doris_metrics.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "runtime/runtime_state.h"

#ifdef WITH_PAIMON_CPP
#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <string>

#include "paimon/file_store_write.h"
#include "paimon/record_batch.h"
#endif

namespace doris {
namespace vectorized {

#ifdef WITH_PAIMON_CPP
namespace {
class PaimonArrowMemPoolAdaptor : public arrow::MemoryPool {
public:
    explicit PaimonArrowMemPoolAdaptor(std::shared_ptr<::paimon::MemoryPool> pool)
            : pool_(std::move(pool)) {}

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        *out = reinterpret_cast<uint8_t*>(pool_->Malloc(size, alignment));
        if (*out == nullptr) {
            return arrow::Status::OutOfMemory("paimon memory pool malloc of size ", size,
                                              " failed");
        }
        stats_.DidAllocateBytes(size);
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                             uint8_t** ptr) override {
        uint8_t* previous_ptr = *ptr;
        uint8_t* new_ptr =
                reinterpret_cast<uint8_t*>(pool_->Realloc(*ptr, old_size, new_size, alignment));
        if (new_ptr == nullptr) {
            *ptr = previous_ptr;
            return arrow::Status::OutOfMemory("paimon memory pool realloc of size ", new_size,
                                              " failed");
        }
        *ptr = new_ptr;
        stats_.DidReallocateBytes(old_size, new_size);
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size, int64_t /*alignment*/) override {
        pool_->Free(buffer, size);
        stats_.DidFreeBytes(size);
    }

    int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

    int64_t max_memory() const override { return stats_.max_memory(); }

    std::string backend_name() const override { return "Doris Paimon Pool"; }

    int64_t total_bytes_allocated() const override { return stats_.total_bytes_allocated(); }

    int64_t num_allocations() const override { return stats_.num_allocations(); }

private:
    std::shared_ptr<::paimon::MemoryPool> pool_;
    arrow::internal::MemoryPoolStats stats_;
};
} // namespace
#endif

VPaimonPartitionWriter::VPaimonPartitionWriter(const TDataSink& t_sink,
                                               std::vector<std::string> partition_values,
                                               int32_t bucket_id
#ifdef WITH_PAIMON_CPP
                                               ,
                                               ::paimon::FileStoreWrite* file_store_write,
                                               std::shared_ptr<::paimon::MemoryPool> pool
#endif
                                               )
        : _t_sink(t_sink),
          _partition_values(std::move(partition_values)),
          _bucket_id(bucket_id)
#ifdef WITH_PAIMON_CPP
          ,
          _file_store_write(file_store_write),
          _pool(std::move(pool))
#endif
{
#ifdef WITH_PAIMON_CPP
    if (_pool) {
        _arrow_pool = std::make_shared<PaimonArrowMemPoolAdaptor>(_pool);
    }
#endif
}

Status VPaimonPartitionWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;

    _arrow_convert_timer = ADD_CHILD_TIMER(_profile, "ArrowConvertTime", "PartitionsWriteTime");
    _file_store_write_timer =
            ADD_CHILD_TIMER(_profile, "FileStoreWriteTime", "PartitionsWriteTime");
    _buffer_flush_count =
            ADD_CHILD_COUNTER(_profile, "BufferFlushCount", TUnit::UNIT, "PartitionsWriteTime");

    return Status::OK();
}

Status VPaimonPartitionWriter::write(::doris::Block& block) {
#ifndef WITH_PAIMON_CPP
    return Status::NotSupported("paimon-cpp is not enabled");
#else
    if (block.rows() == 0) {
        return Status::OK();
    }
    if (!_file_store_write) {
        return Status::InternalError("paimon file store write is null");
    }

    RETURN_IF_ERROR(_append_to_buffer(block));
    if (_buffered_rows >= _batch_max_rows || _buffered_bytes >= _batch_max_bytes) {
        return _flush_buffer();
    }
    return Status::OK();
#endif
}

Status VPaimonPartitionWriter::_write_block(::doris::Block& block) {
#ifndef WITH_PAIMON_CPP
    return Status::NotSupported("paimon-cpp is not enabled");
#else
    std::unique_ptr<::paimon::RecordBatch> batch;
    ArrowArray c_array;
    int64_t arrow_convert_ns = 0;
    {
        SCOPED_TIMER(_arrow_convert_timer);
        SCOPED_RAW_TIMER(&arrow_convert_ns);
        std::shared_ptr<arrow::Schema> arrow_schema;
        RETURN_IF_ERROR(get_arrow_schema_from_block(block, &arrow_schema, _state->timezone()));

        std::shared_ptr<arrow::RecordBatch> record_batch;
        RETURN_IF_ERROR(convert_to_arrow_batch(block, arrow_schema, _arrow_pool.get(),
                                               &record_batch, _state->timezone_obj()));

        auto struct_array_result =
                arrow::StructArray::Make(record_batch->columns(), arrow_schema->fields());
        if (!struct_array_result.ok()) {
            return Status::InternalError("failed to build arrow struct array: {}",
                                         struct_array_result.status().ToString());
        }
        std::shared_ptr<arrow::Array> struct_array = struct_array_result.ValueOrDie();

        auto arrow_status = arrow::ExportArray(*struct_array, &c_array);
        if (!arrow_status.ok()) {
            return Status::InternalError("failed to export arrow array: {}",
                                         arrow_status.ToString());
        }

        std::map<std::string, std::string> partition;
        const auto& paimon_sink = _t_sink.paimon_table_sink;
        if (paimon_sink.__isset.partition_keys) {
            const auto& keys = paimon_sink.partition_keys;
            for (size_t i = 0; i < keys.size() && i < _partition_values.size(); ++i) {
                partition[keys[i]] = _partition_values[i];
            }
        }

        ::paimon::RecordBatchBuilder builder(&c_array);
        builder.SetPartition(std::move(partition));
        builder.SetBucket(_bucket_id);
        auto batch_result = builder.Finish();
        if (!batch_result.ok()) {
            return Status::InternalError("failed to build paimon record batch: {}",
                                         batch_result.status().ToString());
        }
        batch = std::move(batch_result).value();
    }
    DorisMetrics::instance()->paimon_write_arrow_convert_latency_ms->add(static_cast<uint64_t>(
            arrow_convert_ns <= 0 ? 0 : (arrow_convert_ns + 999999) / 1000000));

    int64_t file_store_write_ns = 0;
    auto write_status = [&]() {
        SCOPED_TIMER(_file_store_write_timer);
        SCOPED_RAW_TIMER(&file_store_write_ns);
        return _file_store_write->Write(std::move(batch));
    }();
    if (!write_status.ok()) {
        return Status::InternalError("paimon write failed: {}", write_status.ToString());
    }
    DorisMetrics::instance()->paimon_write_file_store_write_latency_ms->add(static_cast<uint64_t>(
            file_store_write_ns <= 0 ? 0 : (file_store_write_ns + 999999) / 1000000));
    return Status::OK();
#endif
}

Status VPaimonPartitionWriter::_append_to_buffer(const ::doris::Block& block) {
#ifndef WITH_PAIMON_CPP
    return Status::NotSupported("paimon-cpp is not enabled");
#else
    if (!_buffer) {
        _buffer = ::doris::Block::create_unique(block.clone_empty());
        _buffered_rows = 0;
        _buffered_bytes = 0;
    }
    auto columns = _buffer->mutate_columns();
    const int cols = block.columns();
    const size_t rows = block.rows();
    for (int col = 0; col < cols; ++col) {
        columns[col]->insert_range_from(*block.get_by_position(col).column, 0, rows);
    }
    _buffer->set_columns(std::move(columns));
    _buffered_rows += rows;
    _buffered_bytes += block.bytes();
    return Status::OK();
#endif
}

Status VPaimonPartitionWriter::_flush_buffer() {
#ifndef WITH_PAIMON_CPP
    return Status::NotSupported("paimon-cpp is not enabled");
#else
    if (!_buffer || _buffered_rows == 0) {
        return Status::OK();
    }
    Status st = _write_block(*_buffer);
    COUNTER_UPDATE(_buffer_flush_count, 1);
    _buffer.reset();
    _buffered_rows = 0;
    _buffered_bytes = 0;
    return st;
#endif
}

Status VPaimonPartitionWriter::close(const Status& status) {
#ifdef WITH_PAIMON_CPP
    if (status.ok()) {
        Status st = _flush_buffer();
        if (!st.ok()) {
            return st;
        }
    } else {
        _buffer.reset();
        _buffered_rows = 0;
        _buffered_bytes = 0;
    }
#endif
    return status;
}

} // namespace vectorized
} // namespace doris
