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

#include "exec/sink/external_table_sink_hash_partitioner.h"

#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/sink/paimon_fixed_bucket_partition_function.h"
#include "format/transformer/iceberg_partition_function.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {
int64_t scale_threshold_by_task(int64_t value, int task_num) {
    if (task_num <= 0) {
        return value;
    }
    int64_t scaled = value / task_num;
    return scaled == 0 ? value : scaled;
}

uint32_t logical_partition_count(uint32_t writer_count,
                                 TExternalTableSinkWriterAssignment::type assignment) {
    if (assignment != TExternalTableSinkWriterAssignment::SKEWED) {
        return writer_count;
    }
    return writer_count *
           std::max(1, config::table_sink_partition_write_max_partition_nums_per_writer);
}
} // namespace

ExternalTableSinkHashPartitioner::ExternalTableSinkHashPartitioner(
        HashValType partition_count, bool use_new_shuffle_hash_method,
        TExternalTableSinkHashPartitionInfo partition_info)
        : PartitionerBase(partition_count),
          _use_new_shuffle_hash_method(use_new_shuffle_hash_method),
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
    _logical_partition_count =
            logical_partition_count(_partition_count, _partition_info.writer_assignment);
    if (_partition_info.algorithm == TExternalTableSinkHashAlgorithm::PAIMON_FIXED_BUCKET) {
        if (_partition_info.writer_assignment != TExternalTableSinkWriterAssignment::IDENTITY) {
            return Status::InvalidArgument(
                    "Paimon fixed-bucket routing requires identity writer assignment");
        }
        if (!_partition_info.__isset.paimon_fixed_bucket_info) {
            return Status::InvalidArgument("Paimon fixed-bucket routing metadata is missing");
        }
        if (_partition_info.__isset.partition_transforms &&
            !_partition_info.partition_transforms.empty()) {
            return Status::InvalidArgument(
                    "Paimon fixed-bucket routing must not contain Iceberg transforms");
        }
        _partition_function = std::make_unique<PaimonFixedBucketPartitionFunction>(
                _logical_partition_count, _partition_info.paimon_fixed_bucket_info);
        return _partition_function->init(texprs);
    }
    if (_partition_info.algorithm == TExternalTableSinkHashAlgorithm::ICEBERG_TRANSFORM) {
        if (_partition_info.__isset.paimon_fixed_bucket_info) {
            return Status::InvalidArgument(
                    "Iceberg external sink routing must not contain Paimon metadata");
        }
        if (!_partition_info.__isset.partition_transforms) {
            return Status::InvalidArgument(
                    "Iceberg external sink partition transforms are missing");
        }
        if (_partition_info.partition_transforms.size() != texprs.size()) {
            return Status::InvalidArgument(
                    "External sink partition transform count {} does not match expression count {}",
                    _partition_info.partition_transforms.size(), texprs.size());
        }
        std::vector<TIcebergPartitionField> fields;
        fields.reserve(texprs.size());
        for (size_t i = 0; i < texprs.size(); ++i) {
            TIcebergPartitionField field;
            field.__set_transform(_partition_info.partition_transforms[i]);
            field.__set_source_expr(texprs[i]);
            fields.emplace_back(std::move(field));
        }
        _partition_function = std::make_unique<IcebergInsertPartitionFunction>(
                _logical_partition_count, _hash_method(), std::vector<TExpr> {}, std::move(fields));
        return _partition_function->init({});
    }

    if (_partition_info.algorithm != TExternalTableSinkHashAlgorithm::DIRECT_HASH) {
        return Status::InvalidArgument("Unsupported external sink hash algorithm {}",
                                       static_cast<int>(_partition_info.algorithm));
    }
    if (_partition_info.__isset.partition_transforms &&
        !_partition_info.partition_transforms.empty()) {
        return Status::InvalidArgument(
                "Direct external sink hash must not contain partition transforms");
    }
    if (_partition_info.__isset.paimon_fixed_bucket_info) {
        return Status::InvalidArgument(
                "Direct external sink hash must not contain Paimon metadata");
    }

    _partition_function =
            std::make_unique<HashPartitionFunction>(_logical_partition_count, _hash_method());
    return _partition_function->init(texprs);
}

Status ExternalTableSinkHashPartitioner::prepare(RuntimeState* state,
                                                 const RowDescriptor& row_desc) {
    return _partition_function->prepare(state, row_desc);
}

Status ExternalTableSinkHashPartitioner::open(RuntimeState* state) {
    RETURN_IF_ERROR(_partition_function->open(state));
    if (_partition_info.algorithm == TExternalTableSinkHashAlgorithm::ICEBERG_TRANSFORM) {
        auto* iceberg_function =
                assert_cast<IcebergInsertPartitionFunction*>(_partition_function.get());
        if (iceberg_function->fallback_to_random()) {
            return Status::NotSupported("External sink partition transform is not supported");
        }
    }

    if (_partition_info.writer_assignment == TExternalTableSinkWriterAssignment::IDENTITY) {
        _writer_assigner = std::make_unique<IdentityWriterAssigner>();
    } else {
        const int task_num = state == nullptr ? 0 : state->task_num();
        _writer_assigner = std::make_unique<SkewedWriterAssigner>(
                cast_set<int>(_logical_partition_count), cast_set<int>(_partition_count), 1,
                scale_threshold_by_task(
                        config::table_sink_partition_write_min_partition_data_processed_rebalance_threshold,
                        task_num),
                scale_threshold_by_task(
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

    _writer_assigner->assign(_logical_partition_ids, nullptr, rows, block_bytes, _channel_ids);
    return Status::OK();
}

const std::vector<ExternalTableSinkHashPartitioner::HashValType>&
ExternalTableSinkHashPartitioner::get_channel_ids() const {
    return _channel_ids;
}

Status ExternalTableSinkHashPartitioner::clone(RuntimeState* state,
                                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto cloned = std::make_unique<ExternalTableSinkHashPartitioner>(
            _partition_count, _use_new_shuffle_hash_method, _partition_info);
    RETURN_IF_ERROR(_partition_function->clone(state, cloned->_partition_function));
    partitioner = std::move(cloned);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
