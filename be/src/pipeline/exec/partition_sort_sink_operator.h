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

#include <stdint.h>

#include <cstdint>

#include "operator.h"
#include "vec/common/sort/partition_sorter.h"

namespace doris::pipeline {

struct PartitionSortInfo {
    ~PartitionSortInfo() = default;

    PartitionSortInfo(vectorized::VSortExecExprs* vsort_exec_exprs, int64_t limit, int64_t offset,
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
    vectorized::VSortExecExprs* _vsort_exec_exprs = nullptr;
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

static constexpr size_t INITIAL_BUFFERED_BLOCK_BYTES = 64 << 20;
static constexpr size_t PARTITION_SORT_ROWS_THRESHOLD = 20000;

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
        auto empty_block = vectorized::Block::create_unique(
                vectorized::VectorizedUtils::create_empty_block(row_desc));
        empty_block->swap(*input_block);
        _blocks.emplace_back(std::move(empty_block));
    }

    bool reach_limit() {
        return _init_rows <= 0 || _blocks.back()->bytes() > INITIAL_BUFFERED_BLOCK_BYTES;
    }

    size_t get_total_rows() const { return _total_rows; }
    size_t get_topn_filter_rows() const { return _topn_filter_rows; }
    size_t get_do_topn_count() const { return _do_partition_topn_count; }

    vectorized::IColumn::Selector _selector;
    std::vector<std::unique_ptr<vectorized::Block>> _blocks;
    size_t _total_rows = 0;
    size_t _current_input_rows = 0;
    size_t _topn_filter_rows = 0;
    size_t _do_partition_topn_count = 0;
    int _init_rows = 4096;
    bool _is_first_sorter = false;

    std::unique_ptr<vectorized::SortCursorCmp> _previous_row;
    std::unique_ptr<vectorized::PartitionSorter> _partition_topn_sorter = nullptr;
    std::shared_ptr<PartitionSortInfo> _partition_sort_info = nullptr;
};

using PartitionDataPtr = PartitionBlocks*;
using PartitionDataWithStringKey = PHHashMap<StringRef, PartitionDataPtr>;
using PartitionDataWithShortStringKey = StringHashMap<PartitionDataPtr>;
using PartitionDataWithUInt8Key = PHHashMap<vectorized::UInt8, PartitionDataPtr>;
using PartitionDataWithUInt16Key = PHHashMap<vectorized::UInt16, PartitionDataPtr>;
using PartitionDataWithUInt32Key =
        PHHashMap<vectorized::UInt32, PartitionDataPtr, HashCRC32<vectorized::UInt32>>;
using PartitionDataWithUInt64Key = PHHashMap<UInt64, PartitionDataPtr, HashCRC32<UInt64>>;
using PartitionDataWithUInt128Key = PHHashMap<UInt128, PartitionDataPtr, HashCRC32<UInt128>>;
using PartitionDataWithUInt256Key = PHHashMap<UInt256, PartitionDataPtr, HashCRC32<UInt256>>;
using PartitionDataWithUInt136Key = PHHashMap<UInt136, PartitionDataPtr, HashCRC32<UInt136>>;

using PartitionedMethodVariants = std::variant<
        std::monostate, vectorized::MethodSerialized<PartitionDataWithStringKey>,
        vectorized::MethodOneNumber<vectorized::UInt8, PartitionDataWithUInt8Key>,
        vectorized::MethodOneNumber<vectorized::UInt16, PartitionDataWithUInt16Key>,
        vectorized::MethodOneNumber<vectorized::UInt32, PartitionDataWithUInt32Key>,
        vectorized::MethodOneNumber<UInt64, PartitionDataWithUInt64Key>,
        vectorized::MethodOneNumber<UInt128, PartitionDataWithUInt128Key>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt8, vectorized::DataWithNullKey<PartitionDataWithUInt8Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt16, vectorized::DataWithNullKey<PartitionDataWithUInt16Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt32, vectorized::DataWithNullKey<PartitionDataWithUInt32Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                UInt64, vectorized::DataWithNullKey<PartitionDataWithUInt64Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                UInt128, vectorized::DataWithNullKey<PartitionDataWithUInt128Key>>>,
        vectorized::MethodKeysFixed<PartitionDataWithUInt64Key, false>,
        vectorized::MethodKeysFixed<PartitionDataWithUInt64Key, true>,
        vectorized::MethodKeysFixed<PartitionDataWithUInt128Key, false>,
        vectorized::MethodKeysFixed<PartitionDataWithUInt128Key, true>,
        vectorized::MethodKeysFixed<PartitionDataWithUInt256Key, false>,
        vectorized::MethodKeysFixed<PartitionDataWithUInt256Key, true>,
        vectorized::MethodKeysFixed<PartitionDataWithUInt136Key, false>,
        vectorized::MethodKeysFixed<PartitionDataWithUInt136Key, true>,
        vectorized::MethodStringNoCache<PartitionDataWithShortStringKey>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodStringNoCache<
                vectorized::DataWithNullKey<PartitionDataWithShortStringKey>>>>;

struct PartitionedHashMapVariants
        : public vectorized::DataVariants<
                  PartitionedMethodVariants, vectorized::MethodSingleNullableColumn,
                  vectorized::MethodOneNumber, MethodKeysFixed, vectorized::DataWithNullKey> {
    template <bool nullable>
    void init(vectorized::HashKeyType type) {
        _type = type;
        switch (_type) {
        case vectorized::HashKeyType::serialized: {
            method_variant.emplace<vectorized::MethodSerialized<PartitionDataWithStringKey>>();
            break;
        }
        case vectorized::HashKeyType::int8_key: {
            emplace_single<vectorized::UInt8, PartitionDataWithUInt8Key, nullable>();
            break;
        }
        case vectorized::HashKeyType::int16_key: {
            emplace_single<vectorized::UInt16, PartitionDataWithUInt16Key, nullable>();
            break;
        }
        case vectorized::HashKeyType::int32_key: {
            emplace_single<vectorized::UInt32, PartitionDataWithUInt32Key, nullable>();
            break;
        }
        case vectorized::HashKeyType::int64_key: {
            emplace_single<UInt64, PartitionDataWithUInt64Key, nullable>();
            break;
        }
        case vectorized::HashKeyType::int128_key: {
            emplace_single<UInt128, PartitionDataWithUInt128Key, nullable>();
            break;
        }
        case vectorized::HashKeyType::string_key: {
            if (nullable) {
                method_variant.emplace<
                        vectorized::MethodSingleNullableColumn<vectorized::MethodStringNoCache<
                                vectorized::DataWithNullKey<PartitionDataWithShortStringKey>>>>();
            } else {
                method_variant.emplace<
                        vectorized::MethodStringNoCache<PartitionDataWithShortStringKey>>();
            }
            break;
        }
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid key type, type={}", type);
        }
    }
    void init(vectorized::HashKeyType type, bool is_nullable = false) {
        if (is_nullable) {
            init<true>(type);
        } else {
            init<false>(type);
        }
    }
};

class PartitionSortSinkOperatorX;
class PartitionSortSinkLocalState : public PipelineXSinkLocalState<PartitionSortNodeSharedState> {
    ENABLE_FACTORY_CREATOR(PartitionSortSinkLocalState);

public:
    PartitionSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<PartitionSortNodeSharedState>(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

private:
    friend class PartitionSortSinkOperatorX;

    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;
    vectorized::VExprContextSPtrs _partition_expr_ctxs;
    int64_t child_input_rows = 0;
    std::vector<PartitionDataPtr> _value_places;
    int _num_partition = 0;
    std::vector<const vectorized::IColumn*> _partition_columns;
    std::unique_ptr<PartitionedHashMapVariants> _partitioned_data;
    std::unique_ptr<vectorized::Arena> _agg_arena_pool;
    int _partition_exprs_num = 0;
    std::shared_ptr<PartitionSortInfo> _partition_sort_info = nullptr;

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _emplace_key_timer = nullptr;
    RuntimeProfile::Counter* _selector_block_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _passthrough_rows_counter = nullptr;
    Status _init_hash_method();
};

class PartitionSortSinkOperatorX final : public DataSinkOperatorX<PartitionSortSinkLocalState> {
public:
    PartitionSortSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                               const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<PartitionSortSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        if (_topn_phase == TPartTopNPhase::TWO_PHASE_GLOBAL) {
            return DataSinkOperatorX<PartitionSortSinkLocalState>::required_data_distribution();
        }
        return {ExchangeType::PASSTHROUGH};
    }

private:
    friend class PartitionSortSinkLocalState;
    ObjectPool* _pool = nullptr;
    const RowDescriptor _row_descriptor;
    const int64_t _limit = -1;
    const int _partition_exprs_num = 0;
    const TPartTopNPhase::type _topn_phase;
    const bool _has_global_limit = false;
    const TopNAlgorithm::type _top_n_algorithm = TopNAlgorithm::ROW_NUMBER;
    const int64_t _partition_inner_limit = 0;

    vectorized::VExprContextSPtrs _partition_expr_ctxs;
    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    Status _split_block_by_partition(vectorized::Block* input_block,
                                     PartitionSortSinkLocalState& local_state, bool eos);
    Status _emplace_into_hash_table(const vectorized::ColumnRawPtrs& key_columns,
                                    const vectorized::Block* input_block,
                                    PartitionSortSinkLocalState& local_state, bool eos);
};

} // namespace doris::pipeline
