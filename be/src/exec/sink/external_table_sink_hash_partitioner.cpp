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
#include "common/status.h"
#include "format/transformer/iceberg_partition_function.h"

namespace doris {
#include "common/compile_check_begin.h"

ExternalTableSinkHashPartitioner::ExternalTableSinkHashPartitioner(
        HashValType partition_count, bool use_new_shuffle_hash_method,
        TExternalTableSinkHashPartitionInfo partition_info)
        : PartitionerBase(partition_count),
          _use_new_shuffle_hash_method(use_new_shuffle_hash_method),
          _partition_info(std::move(partition_info)) {}

Status ExternalTableSinkHashPartitioner::init(const std::vector<TExpr>& texprs) {
    if (_partition_info.algorithm == TExternalTableSinkHashAlgorithm::ICEBERG_TRANSFORM) {
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
                _partition_count, _hash_method(), std::vector<TExpr> {}, std::move(fields));
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

    if (_use_new_shuffle_hash_method) {
        _hash_partitioner = std::make_unique<Crc32CHashPartitioner>(_partition_count);
    } else {
        _hash_partitioner =
                std::make_unique<Crc32HashPartitioner<ShuffleChannelIds>>(_partition_count);
    }
    return _hash_partitioner->init(texprs);
}

Status ExternalTableSinkHashPartitioner::prepare(RuntimeState* state,
                                                 const RowDescriptor& row_desc) {
    if (_partition_function != nullptr) {
        return _partition_function->prepare(state, row_desc);
    }
    return _hash_partitioner->prepare(state, row_desc);
}

Status ExternalTableSinkHashPartitioner::open(RuntimeState* state) {
    if (_partition_function != nullptr) {
        RETURN_IF_ERROR(_partition_function->open(state));
        auto* iceberg_function =
                assert_cast<IcebergInsertPartitionFunction*>(_partition_function.get());
        if (iceberg_function->fallback_to_random()) {
            return Status::NotSupported("External sink partition transform is not supported");
        }
        return Status::OK();
    }
    return _hash_partitioner->open(state);
}

Status ExternalTableSinkHashPartitioner::close(RuntimeState* state) {
    if (_partition_function != nullptr) {
        return _partition_function->close(state);
    }
    return _hash_partitioner->close(state);
}

Status ExternalTableSinkHashPartitioner::do_partitioning(RuntimeState* state, Block* block) const {
    if (_partition_function != nullptr) {
        return _partition_function->get_partitions(state, block, _partition_count, _channel_ids);
    }
    return _hash_partitioner->do_partitioning(state, block);
}

const std::vector<ExternalTableSinkHashPartitioner::HashValType>&
ExternalTableSinkHashPartitioner::get_channel_ids() const {
    if (_partition_function != nullptr) {
        return _channel_ids;
    }
    return _hash_partitioner->get_channel_ids();
}

Status ExternalTableSinkHashPartitioner::clone(RuntimeState* state,
                                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto cloned = std::make_unique<ExternalTableSinkHashPartitioner>(
            _partition_count, _use_new_shuffle_hash_method, _partition_info);
    if (_partition_function != nullptr) {
        RETURN_IF_ERROR(_partition_function->clone(state, cloned->_partition_function));
    } else {
        RETURN_IF_ERROR(_hash_partitioner->clone(state, cloned->_hash_partitioner));
    }
    partitioner = std::move(cloned);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
