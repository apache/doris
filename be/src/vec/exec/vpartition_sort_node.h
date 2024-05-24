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

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "exec/exec_node.h"
#include "vec/columns/column.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/common/sort/partition_sorter.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/block.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
namespace vectorized {
static constexpr size_t INITIAL_BUFFERED_BLOCK_BYTES = 64 << 20;
static constexpr size_t PARTITION_SORT_ROWS_THRESHOLD = 20000;

struct PartitionSortInfo {
    ~PartitionSortInfo() = default;

    PartitionSortInfo(VSortExecExprs* vsort_exec_exprs, int64_t limit, int64_t offset,
                      ObjectPool* pool, const std::vector<bool>& is_asc_order,
                      const std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                      RuntimeState* runtime_state, RuntimeProfile* runtime_profile,
                      bool has_global_limit, int64_t partition_inner_limit,
                      TopNAlgorithm::type top_n_algorithm, TPartTopNPhase::type topn_phase)
            : _vsort_exec_exprs(vsort_exec_exprs),
              _limit(limit),
              _offset(offset),
              _pool(pool),
              _is_asc_order(is_asc_order),
              _nulls_first(nulls_first),
              _row_desc(row_desc),
              _runtime_state(runtime_state),
              _runtime_profile(runtime_profile),
              _has_global_limit(has_global_limit),
              _partition_inner_limit(partition_inner_limit),
              _top_n_algorithm(top_n_algorithm),
              _topn_phase(topn_phase) {}

public:
    VSortExecExprs* _vsort_exec_exprs = nullptr;
    int64_t _limit = -1;
    int64_t _offset = 0;
    ObjectPool* _pool = nullptr;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    const RowDescriptor& _row_desc;
    RuntimeState* _runtime_state = nullptr;
    RuntimeProfile* _runtime_profile = nullptr;
    bool _has_global_limit = false;
    int64_t _partition_inner_limit = 0;
    TopNAlgorithm::type _top_n_algorithm = TopNAlgorithm::ROW_NUMBER;
    TPartTopNPhase::type _topn_phase = TPartTopNPhase::TWO_PHASE_GLOBAL;
};

struct PartitionBlocks {
public:
    PartitionBlocks(std::shared_ptr<PartitionSortInfo> partition_sort_info, bool is_first_sorter)
            : _is_first_sorter(is_first_sorter), _partition_sort_info(partition_sort_info) {}
    ~PartitionBlocks() = default;

    void add_row_idx(size_t row) { _selector.push_back(row); }

    Status append_block_by_selector(const vectorized::Block* input_block, bool eos);

    Status do_partition_topn_sort();

    void create_or_reset_sorter_state();

    void append_whole_block(vectorized::Block* input_block, const RowDescriptor& row_desc) {
        auto empty_block = Block::create_unique(VectorizedUtils::create_empty_block(row_desc));
        empty_block->swap(*input_block);
        _blocks.emplace_back(std::move(empty_block));
    }

    bool reach_limit() {
        return _init_rows <= 0 || _blocks.back()->bytes() > INITIAL_BUFFERED_BLOCK_BYTES;
    }

    size_t get_total_rows() const { return _total_rows; }
    size_t get_topn_filter_rows() const { return _topn_filter_rows; }
    size_t get_do_topn_count() const { return _do_partition_topn_count; }

    IColumn::Selector _selector;
    std::vector<std::unique_ptr<Block>> _blocks;
    size_t _total_rows = 0;
    size_t _current_input_rows = 0;
    size_t _topn_filter_rows = 0;
    size_t _do_partition_topn_count = 0;
    int _init_rows = 4096;
    bool _is_first_sorter = false;

    std::unique_ptr<SortCursorCmp> _previous_row;
    std::unique_ptr<PartitionSorter> _partition_topn_sorter = nullptr;
    std::shared_ptr<PartitionSortInfo> _partition_sort_info = nullptr;
};

using PartitionDataPtr = PartitionBlocks*;
using PartitionDataWithStringKey = PHHashMap<StringRef, PartitionDataPtr>;
using PartitionDataWithShortStringKey = StringHashMap<PartitionDataPtr>;
using PartitionDataWithUInt8Key = PHHashMap<UInt8, PartitionDataPtr>;
using PartitionDataWithUInt16Key = PHHashMap<UInt16, PartitionDataPtr>;
using PartitionDataWithUInt32Key = PHHashMap<UInt32, PartitionDataPtr, HashCRC32<UInt32>>;
using PartitionDataWithUInt64Key = PHHashMap<UInt64, PartitionDataPtr, HashCRC32<UInt64>>;
using PartitionDataWithUInt128Key = PHHashMap<UInt128, PartitionDataPtr, HashCRC32<UInt128>>;
using PartitionDataWithUInt256Key = PHHashMap<UInt256, PartitionDataPtr, HashCRC32<UInt256>>;
using PartitionDataWithUInt136Key = PHHashMap<UInt136, PartitionDataPtr, HashCRC32<UInt136>>;

using PartitionedMethodVariants = std::variant<
        std::monostate, MethodSerialized<PartitionDataWithStringKey>,
        MethodOneNumber<UInt8, PartitionDataWithUInt8Key>,
        MethodOneNumber<UInt16, PartitionDataWithUInt16Key>,
        MethodOneNumber<UInt32, PartitionDataWithUInt32Key>,
        MethodOneNumber<UInt64, PartitionDataWithUInt64Key>,
        MethodOneNumber<UInt128, PartitionDataWithUInt128Key>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt8, DataWithNullKey<PartitionDataWithUInt8Key>>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt16, DataWithNullKey<PartitionDataWithUInt16Key>>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt32, DataWithNullKey<PartitionDataWithUInt32Key>>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt64, DataWithNullKey<PartitionDataWithUInt64Key>>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt128, DataWithNullKey<PartitionDataWithUInt128Key>>>,
        MethodKeysFixed<PartitionDataWithUInt64Key, false>,
        MethodKeysFixed<PartitionDataWithUInt64Key, true>,
        MethodKeysFixed<PartitionDataWithUInt128Key, false>,
        MethodKeysFixed<PartitionDataWithUInt128Key, true>,
        MethodKeysFixed<PartitionDataWithUInt256Key, false>,
        MethodKeysFixed<PartitionDataWithUInt256Key, true>,
        MethodKeysFixed<PartitionDataWithUInt136Key, false>,
        MethodKeysFixed<PartitionDataWithUInt136Key, true>,
        MethodStringNoCache<PartitionDataWithShortStringKey>,
        MethodSingleNullableColumn<
                MethodStringNoCache<DataWithNullKey<PartitionDataWithShortStringKey>>>>;

struct PartitionedHashMapVariants
        : public DataVariants<PartitionedMethodVariants, MethodSingleNullableColumn,
                              MethodOneNumber, MethodKeysFixed, DataWithNullKey> {
    template <bool nullable>
    void init(Type type) {
        _type = type;
        switch (_type) {
        case Type::serialized: {
            method_variant.emplace<MethodSerialized<PartitionDataWithStringKey>>();
            break;
        }
        case Type::int8_key: {
            emplace_single<UInt8, PartitionDataWithUInt8Key, nullable>();
            break;
        }
        case Type::int16_key: {
            emplace_single<UInt16, PartitionDataWithUInt16Key, nullable>();
            break;
        }
        case Type::int32_key: {
            emplace_single<UInt32, PartitionDataWithUInt32Key, nullable>();
            break;
        }
        case Type::int64_key: {
            emplace_single<UInt64, PartitionDataWithUInt64Key, nullable>();
            break;
        }
        case Type::int128_key: {
            emplace_single<UInt128, PartitionDataWithUInt128Key, nullable>();
            break;
        }
        case Type::string_key: {
            if (nullable) {
                method_variant.emplace<MethodSingleNullableColumn<
                        MethodStringNoCache<DataWithNullKey<PartitionDataWithShortStringKey>>>>();
            } else {
                method_variant.emplace<MethodStringNoCache<PartitionDataWithShortStringKey>>();
            }
            break;
        }
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid key type, type={}", type);
        }
    }
    void init(Type type, bool is_nullable = false) {
        if (is_nullable) {
            init<true>(type);
        } else {
            init<false>(type);
        }
    }
};

class VExprContext;

class VPartitionSortNode : public ExecNode {
public:
    VPartitionSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VPartitionSortNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status alloc_resource(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;
    Status sink(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    void debug_profile();
    bool can_read();

private:
    Status _init_hash_method();
    Status _split_block_by_partition(vectorized::Block* input_block, bool eos);
    Status _emplace_into_hash_table(const ColumnRawPtrs& key_columns,
                                    const vectorized::Block* input_block, bool eos);
    Status get_sorted_block(RuntimeState* state, Block* output_block, bool* eos);

    // hash table
    std::unique_ptr<PartitionedHashMapVariants> _partitioned_data;
    std::unique_ptr<Arena> _agg_arena_pool;
    // partition by k1,k2
    int _partition_exprs_num = 0;
    VExprContextSPtrs _partition_expr_ctxs;
    std::vector<const IColumn*> _partition_columns;

    std::vector<std::unique_ptr<PartitionSorter>> _partition_sorts;
    std::vector<PartitionDataPtr> _value_places;
    std::shared_ptr<PartitionSortInfo> _partition_sort_info = nullptr;
    // Expressions and parameters used for build _sort_description
    VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    TopNAlgorithm::type _top_n_algorithm = TopNAlgorithm::ROW_NUMBER;
    bool _has_global_limit = false;
    int _num_partition = 0;
    int64_t _partition_inner_limit = 0;
    int _sort_idx = 0;
    std::queue<Block> _blocks_buffer;
    int64_t child_input_rows = 0;
    std::mutex _buffer_mutex;
    TPartTopNPhase::type _topn_phase;

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _emplace_key_timer = nullptr;
    RuntimeProfile::Counter* _partition_sort_timer = nullptr;
    RuntimeProfile::Counter* _get_sorted_timer = nullptr;
    RuntimeProfile::Counter* _selector_block_timer = nullptr;

    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    //only for profile record
    std::vector<int> partition_profile_output_rows;
};

} // namespace vectorized

constexpr auto init_partition_hash_method =
        init_hash_method<vectorized::PartitionedHashMapVariants, vectorized::PartitionDataPtr>;

} // namespace doris
