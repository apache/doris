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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/partitioner/external/paimon_fixed_bucket_partition_function.h"

#include "common/status.h"
#include "exec/partitioner/external/paimon_native_row_hash.h"

namespace doris {

PaimonFixedBucketPartitionFunction::PaimonFixedBucketPartitionFunction(
        HashValType partition_count, TPaimonFixedBucketInfo fixed_bucket_info)
        : PaimonRowHashPartitionFunction(partition_count),
          _fixed_bucket_info(std::move(fixed_bucket_info)) {}

Status PaimonFixedBucketPartitionFunction::init(const std::vector<TExpr>& texprs) {
    RETURN_IF_ERROR(PaimonRowHashPartitionFunction::init(texprs));
    if (_fixed_bucket_info.num_buckets <= 0) {
        return Status::InvalidArgument("Paimon fixed-bucket count must be positive");
    }
    RETURN_IF_ERROR(_validate_field_indexes(_fixed_bucket_info.partition_field_indexes, false));
    return _validate_field_indexes(_fixed_bucket_info.bucket_field_indexes, true);
}

Status PaimonFixedBucketPartitionFunction::get_partitions(
        RuntimeState* /*state*/, Block* block, size_t partition_count,
        std::vector<HashValType>& partitions) const {
    if (partition_count != _partition_count) {
        return Status::InvalidArgument("Paimon writer count {} does not match planned count {}",
                                       partition_count, _partition_count);
    }
    const size_t rows = block->rows();
    if (rows == 0) {
        partitions.clear();
        return Status::OK();
    }

    std::vector<ColumnWithTypeAndName> fields;
    RETURN_IF_ERROR(_evaluate_fields(block, fields));
    std::vector<int32_t> partition_hashes;
    std::vector<int32_t> bucket_hashes;
    RETURN_IF_ERROR(
            _hash_fields(_fixed_bucket_info.partition_field_indexes, fields, partition_hashes));
    RETURN_IF_ERROR(_hash_fields(_fixed_bucket_info.bucket_field_indexes, fields, bucket_hashes));
    partitions.resize(rows);
    for (size_t row = 0; row < rows; ++row) {
        auto bucket =
                paimon_native::default_bucket(bucket_hashes[row], _fixed_bucket_info.num_buckets);
        if (!bucket.has_value()) {
            return Status::InternalError("Failed to compute Paimon fixed bucket");
        }
        auto channel = paimon_native::fixed_bucket_channel(partition_hashes[row], *bucket,
                                                           _partition_count);
        if (!channel.has_value()) {
            return Status::InternalError("Failed to compute Paimon fixed-bucket writer");
        }
        partitions[row] = *channel;
    }
    return Status::OK();
}

Status PaimonFixedBucketPartitionFunction::clone(
        RuntimeState* state, std::unique_ptr<PartitionFunction>& function) const {
    auto cloned = std::make_unique<PaimonFixedBucketPartitionFunction>(_partition_count,
                                                                       _fixed_bucket_info);
    RETURN_IF_ERROR(_clone_expr_ctxs(state, cloned->_field_expr_ctxs));
    function = std::move(cloned);
    return Status::OK();
}

} // namespace doris
