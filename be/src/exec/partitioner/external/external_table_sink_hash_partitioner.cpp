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

#include "exec/partitioner/external/external_table_sink_hash_partitioner.h"

#include <limits>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/partitioner/external/external_partition_function_factory.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {
Status logical_partition_count(uint32_t writer_count,
                               TExternalTableSinkWriterAssignment::type assignment,
                               uint32_t* result) {
    if (writer_count == 0) {
        return Status::InvalidArgument("External sink writer count must be positive");
    }
    if (assignment != TExternalTableSinkWriterAssignment::SKEWED) {
        *result = writer_count;
        return Status::OK();
    }
    const uint32_t partitions_per_writer = static_cast<uint32_t>(
            std::max(1, config::table_sink_partition_write_max_partition_nums_per_writer));
    if (writer_count > std::numeric_limits<uint32_t>::max() / partitions_per_writer) {
        return Status::InvalidArgument("External sink logical partition count overflows");
    }
    *result = writer_count * partitions_per_writer;
    return Status::OK();
}
} // namespace

ExternalTableSinkHashPartitioner::ExternalTableSinkHashPartitioner(
        HashValType partition_count, ShuffleHashMethod hash_method,
        TExternalTableSinkHashPartitionInfo partition_info)
        : PartitionerBase(partition_count),
          _hash_method(hash_method),
          _partition_info(std::move(partition_info)),
          _logical_partition_count(partition_count) {}

Status ExternalTableSinkHashPartitioner::init(const std::vector<TExpr>& texprs) {
    if (!_partition_info.__isset.writer_assignment) {
        return Status::InvalidArgument("External sink writer assignment is missing");
    }
    if (_partition_info.writer_assignment != TExternalTableSinkWriterAssignment::IDENTITY &&
        _partition_info.writer_assignment != TExternalTableSinkWriterAssignment::SKEWED) {
        return Status::InvalidArgument("Unsupported external sink writer assignment {}",
                                       static_cast<int>(_partition_info.writer_assignment));
    }
    RETURN_IF_ERROR(logical_partition_count(_partition_count, _partition_info.writer_assignment,
                                            &_logical_partition_count));
    const bool requires_identity =
            _partition_info.algorithm == TExternalTableSinkHashAlgorithm::PAIMON_FIXED_BUCKET;
    if (requires_identity &&
        _partition_info.writer_assignment != TExternalTableSinkWriterAssignment::IDENTITY) {
        return Status::InvalidArgument("Paimon bucket routing requires identity writer assignment");
    }
    return create_external_partition_function(_partition_info, _logical_partition_count,
                                              _hash_method, texprs, &_partition_function);
}

Status ExternalTableSinkHashPartitioner::prepare(RuntimeState* state,
                                                 const RowDescriptor& row_desc) {
    return _partition_function->prepare(state, row_desc);
}

Status ExternalTableSinkHashPartitioner::open(RuntimeState* state) {
    RETURN_IF_ERROR(_partition_function->open(state));

    if (_partition_info.writer_assignment == TExternalTableSinkWriterAssignment::IDENTITY) {
        _writer_assigner = std::make_unique<IdentityWriterAssigner>(_partition_count);
    } else {
        const int task_num = state == nullptr ? 0 : state->task_num();
        _writer_assigner = std::make_unique<SkewedWriterAssigner>(
                cast_set<int>(_logical_partition_count), cast_set<int>(_partition_count), 1,
                scale_writer_threshold_by_task(
                        config::table_sink_partition_write_min_partition_data_processed_rebalance_threshold,
                        task_num),
                scale_writer_threshold_by_task(
                        config::table_sink_partition_write_min_data_processed_rebalance_threshold,
                        task_num));
    }
    return Status::OK();
}

Status ExternalTableSinkHashPartitioner::close(RuntimeState* state) {
    return _partition_function->close(state);
}

Status ExternalTableSinkHashPartitioner::do_partitioning(RuntimeState* state, Block* block) const {
    if (_writer_assigner == nullptr) {
        return Status::InternalError("External sink writer assigner is not open");
    }
    const size_t rows = block->rows();
    const size_t block_bytes = block->bytes();
    if (rows == 0) {
        _logical_partition_ids.clear();
        _channel_ids.clear();
        return Status::OK();
    }
    RETURN_IF_ERROR(_partition_function->get_partitions(state, block, _logical_partition_count,
                                                        _logical_partition_ids));

    return _writer_assigner->assign(_logical_partition_ids, nullptr, rows, block_bytes,
                                    _channel_ids);
}

const std::vector<ExternalTableSinkHashPartitioner::HashValType>&
ExternalTableSinkHashPartitioner::get_channel_ids() const {
    return _channel_ids;
}

Status ExternalTableSinkHashPartitioner::clone(RuntimeState* state,
                                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto cloned = std::make_unique<ExternalTableSinkHashPartitioner>(_partition_count, _hash_method,
                                                                     _partition_info);
    RETURN_IF_ERROR(_partition_function->clone(state, cloned->_partition_function));
    partitioner = std::move(cloned);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
