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

#include <algorithm>

#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
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

    virtual Status close(RuntimeState* state) = 0;

    virtual Status do_partitioning(RuntimeState* state, Block* block, bool eos = false,
                                   bool* already_sent = nullptr) const = 0;

    virtual ChannelField get_channel_ids() const = 0;

    virtual Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) = 0;

    size_t partition_count() const { return _partition_count; }

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

    Status close(RuntimeState* state) override { return Status::OK(); }

    Status do_partitioning(RuntimeState* state, Block* block, bool eos,
                           bool* already_sent) const override;

    ChannelField get_channel_ids() const override {
        return {.channel_id = _hash_vals.data(), .len = sizeof(uint32_t)};
    }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

protected:
    Status _get_partition_column_result(Block* block, std::vector<int>& result) const {
        int counter = 0;
        for (auto ctx : _partition_expr_ctxs) {
            RETURN_IF_ERROR(ctx->execute(block, &result[counter++]));
        }
        return Status::OK();
    }

    Status _clone_expr_ctxs(RuntimeState* state, VExprContextSPtrs& new_partition_expr_ctxs) const {
        new_partition_expr_ctxs.resize(_partition_expr_ctxs.size());
        for (size_t i = 0; i < _partition_expr_ctxs.size(); i++) {
            RETURN_IF_ERROR(_partition_expr_ctxs[i]->clone(state, new_partition_expr_ctxs[i]));
        }
        return Status::OK();
    }

    virtual void _do_hash(const ColumnPtr& column, uint32_t* __restrict result, int idx) const;
    virtual void _initialize_hash_vals(size_t rows) const {
        _hash_vals.resize(rows);
        std::ranges::fill(_hash_vals, 0);
    }

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

static inline uint32_t crc32c_shuffle_mix(uint32_t h) {
    // Step 1: fold high entropy into low bits
    h ^= h >> 16;
    // Step 2: odd multiplicative scramble (cheap avalanche)
    h *= 0xA5B35705U;
    // Step 3: final fold to break remaining linearity
    h ^= h >> 13;
    return h;
}

// use high 16 bits as channel id to avoid conflict with crc32c hash table
// shuffle hash function same with crc32c hash table(eg join hash table) will lead bad performance
// hash table offten use low 16 bits as bucket index, so we shift 16 bits to high bits to avoid conflict
struct ShiftChannelIds {
    template <typename HashValueType>
    HashValueType operator()(HashValueType l, size_t r) {
        return crc32c_shuffle_mix(l) % r;
    }
};

class Crc32CHashPartitioner : public Crc32HashPartitioner<ShiftChannelIds> {
public:
    Crc32CHashPartitioner(int partition_count)
            : Crc32HashPartitioner<ShiftChannelIds>(partition_count) {}

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

private:
    void _do_hash(const ColumnPtr& column, uint32_t* __restrict result, int idx) const override;

    void _initialize_hash_vals(size_t rows) const override {
        _hash_vals.resize(rows);
        // use golden ratio to initialize hash values to avoid collision with hash table's hash function
        constexpr uint32_t CRC32C_SHUFFLE_SEED = 0x9E3779B9U;
        std::ranges::fill(_hash_vals, CRC32C_SHUFFLE_SEED);
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
