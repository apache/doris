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

#pragma once

#include <gen_cpp/Partitions_types.h>

#include <vector>

#include "exec/partitioner/partitioner.h"

namespace doris {

class PaimonFixedBucketPartitioner final : public Crc32HashPartitioner<ShuffleChannelIds> {
public:
    PaimonFixedBucketPartitioner(int partition_count, TPaimonRouteBucketInfo route_bucket_info);

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status do_partitioning(RuntimeState* state, Block* block) const override;
    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

#ifdef BE_TEST
    Status compute_bucket_ids_for_test(Block* block, size_t rows,
                                       std::vector<int32_t>& bucket_ids) const {
        return _compute_bucket_ids(block, rows, bucket_ids);
    }
#endif

private:
    Status _compute_bucket_ids(Block* block, size_t rows, std::vector<int32_t>& bucket_ids) const;
    Status _compute_default_bucket_ids(Block* block, size_t rows,
                                       std::vector<int32_t>& bucket_ids) const;
    Status _compute_mod_bucket_ids(Block* block, size_t rows,
                                   std::vector<int32_t>& bucket_ids) const;

    TPaimonRouteBucketInfo _route_bucket_info;
    VExprContextSPtrs _bucket_key_expr_ctxs;
};

} // namespace doris
