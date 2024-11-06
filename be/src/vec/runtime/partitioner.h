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
#include "common/compile_check_begin.h"
class MemTracker;

namespace vectorized {

struct ChannelField {
    const void* channel_id;
    const uint32_t len;

    template <typename T>
    const T* get() const {
        CHECK_EQ(sizeof(T), len) << " sizeof(T): " << sizeof(T) << " len: " << len;
        return reinterpret_cast<const T*>(channel_id);
    }
};

class PartitionerBase {
public:
    PartitionerBase(size_t partition_count) : _partition_count(partition_count) {}
    virtual ~PartitionerBase() = default;

    virtual Status init(const std::vector<TExpr>& texprs) = 0;

    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc) = 0;

    virtual Status open(RuntimeState* state) = 0;

    virtual Status do_partitioning(RuntimeState* state, Block* block) const = 0;

    virtual ChannelField get_channel_ids() const = 0;

    virtual Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) = 0;

protected:
    const size_t _partition_count;
};

template <typename ChannelIds>
class Crc32HashPartitioner : public PartitionerBase {
public:
    Crc32HashPartitioner(int partition_count) : PartitionerBase(partition_count) {}
    ~Crc32HashPartitioner() override = default;

    Status init(const std::vector<TExpr>& texprs) override {
        return VExpr::create_expr_trees(texprs, _partition_expr_ctxs);
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override {
        return VExpr::prepare(_partition_expr_ctxs, state, row_desc);
    }

    Status open(RuntimeState* state) override { return VExpr::open(_partition_expr_ctxs, state); }

    Status do_partitioning(RuntimeState* state, Block* block) const override;

    ChannelField get_channel_ids() const override { return {_hash_vals.data(), sizeof(uint32_t)}; }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

protected:
    Status _get_partition_column_result(Block* block, std::vector<int>& result) const {
        int counter = 0;
        for (auto ctx : _partition_expr_ctxs) {
            RETURN_IF_ERROR(ctx->execute(block, &result[counter++]));
        }
        return Status::OK();
    }

    void _do_hash(const ColumnPtr& column, uint32_t* __restrict result, int idx) const;

    VExprContextSPtrs _partition_expr_ctxs;
    mutable std::vector<uint32_t> _hash_vals;
};

struct ShuffleChannelIds {
    template <typename HashValueType>
    HashValueType operator()(HashValueType l, size_t r) {
        return l % r;
    }
};

struct SpillPartitionChannelIds {
    template <typename HashValueType>
    HashValueType operator()(HashValueType l, size_t r) {
        return ((l >> 16) | (l << 16)) % r;
    }
};

} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
