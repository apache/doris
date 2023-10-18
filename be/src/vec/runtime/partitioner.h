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

#include "util/runtime_profile.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class MemTracker;

namespace vectorized {

class Partitioner {
public:
    Partitioner(int partition_count) : _partition_count(partition_count) {}
    virtual ~Partitioner() = default;

    Status init(const std::vector<TExpr>& texprs) {
        return VExpr::create_expr_trees(texprs, _partition_expr_ctxs);
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) {
        return VExpr::prepare(_partition_expr_ctxs, state, row_desc);
    }

    Status open(RuntimeState* state) { return VExpr::open(_partition_expr_ctxs, state); }

    virtual Status do_partitioning(RuntimeState* state, Block* block,
                                   MemTracker* mem_tracker) const = 0;

    virtual uint8* get_hash_values() const = 0;

protected:
    Status _get_partition_column_result(Block* block, int* result) const {
        int counter = 0;
        for (auto ctx : _partition_expr_ctxs) {
            RETURN_IF_ERROR(ctx->execute(block, &result[counter++]));
        }
        return Status::OK();
    }

    VExprContextSPtrs _partition_expr_ctxs;
    const int _partition_count;
};

class HashPartitioner final : public Partitioner {
public:
    HashPartitioner(int partition_count, RuntimeProfile::Counter** split_block_hash_compute_timer)
            : Partitioner(partition_count),
              _split_block_hash_compute_timer(split_block_hash_compute_timer) {}
    ~HashPartitioner() override = default;

    Status do_partitioning(RuntimeState* state, Block* block,
                           MemTracker* mem_tracker) const override;

    uint8* get_hash_values() const override { return (uint8*)_hash_vals.data(); }

private:
    RuntimeProfile::Counter** _split_block_hash_compute_timer;
    mutable std::vector<uint64_t> _hash_vals;
};

class BucketHashPartitioner final : public Partitioner {
public:
    BucketHashPartitioner(int partition_count) : Partitioner(partition_count) {}
    ~BucketHashPartitioner() override = default;

    Status do_partitioning(RuntimeState* state, Block* block,
                           MemTracker* mem_tracker) const override;

    uint8* get_hash_values() const override { return (uint8*)_hash_vals.data(); }

private:
    mutable std::vector<uint32_t> _hash_vals;
};

} // namespace vectorized
} // namespace doris
