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

#include "exec/partitioner/external/paimon_hash_dynamic_partition_function.h"

#include "common/status.h"
#include "exec/partitioner/external/paimon_native_row_hash.h"

namespace doris {
#include "common/compile_check_begin.h"

PaimonHashDynamicPartitionFunction::PaimonHashDynamicPartitionFunction(
        HashValType partition_count, TPaimonHashDynamicInfo dynamic_info)
        : PaimonRowHashPartitionFunction(partition_count), _dynamic_info(std::move(dynamic_info)) {}

Status PaimonHashDynamicPartitionFunction::init(const std::vector<TExpr>& texprs) {
    RETURN_IF_ERROR(PaimonRowHashPartitionFunction::init(texprs));
    RETURN_IF_ERROR(_validate_field_indexes(_dynamic_info.partition_field_indexes, false));
    RETURN_IF_ERROR(_validate_field_indexes(_dynamic_info.primary_key_field_indexes, true));
    return Status::OK();
}

Status PaimonHashDynamicPartitionFunction::get_partitions(
        RuntimeState* /*state*/, Block* block, size_t partition_count,
        std::vector<HashValType>& partitions) const {
    if (partition_count != _partition_count) {
        return Status::InvalidArgument("Paimon writer count {} does not match planned count {}",
                                       partition_count, _partition_count);
    }
    if (block->rows() == 0) {
        partitions.clear();
        return Status::OK();
    }

    std::vector<ColumnWithTypeAndName> fields;
    RETURN_IF_ERROR(_evaluate_fields(block, fields));
    std::vector<int32_t> partition_hashes;
    std::vector<int32_t> primary_key_hashes;
    RETURN_IF_ERROR(_hash_fields(_dynamic_info.partition_field_indexes, fields, partition_hashes));
    RETURN_IF_ERROR(
            _hash_fields(_dynamic_info.primary_key_field_indexes, fields, primary_key_hashes));

    partitions.resize(block->rows());
    for (size_t row = 0; row < block->rows(); ++row) {
        auto channel = paimon_native::dynamic_bucket_assigner_channel(
                partition_hashes[row], primary_key_hashes[row], _partition_count, _partition_count);
        if (!channel.has_value()) {
            return Status::InternalError("Failed to compute Paimon HASH_DYNAMIC assigner");
        }
        partitions[row] = *channel;
    }
    return Status::OK();
}

Status PaimonHashDynamicPartitionFunction::clone(
        RuntimeState* state, std::unique_ptr<PartitionFunction>& function) const {
    auto cloned =
            std::make_unique<PaimonHashDynamicPartitionFunction>(_partition_count, _dynamic_info);
    RETURN_IF_ERROR(_clone_expr_ctxs(state, cloned->_field_expr_ctxs));
    function = std::move(cloned);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
