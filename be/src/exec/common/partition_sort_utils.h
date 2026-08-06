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

#include <utility>
#include <variant>
#include <vector>

#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/hash_table/ph_hash_map.h"
#include "exec/common/hash_table/string_hash_map.h"
#include "exec/sort/partition_sorter.h"
#include "exprs/vexpr_fwd.h"

namespace doris {

struct PartitionSortInfo {
    ~PartitionSortInfo() = default;

    PartitionSortInfo(const VExprContextSPtrs* ordering_expr_ctxs, int64_t limit, int64_t offset,
                      ObjectPool* pool, const std::vector<bool>& is_asc_order,
                      const std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                      RuntimeState* runtime_state, RuntimeProfile* runtime_profile,
                      bool has_global_limit, int64_t partition_inner_limit,
                      TopNAlgorithm::type top_n_algorithm, TPartTopNPhase::type topn_phase)
            : _ordering_expr_ctxs(ordering_expr_ctxs),
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
    const VExprContextSPtrs* _ordering_expr_ctxs = nullptr;
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

#ifndef NDEBUG
static constexpr size_t PARTITION_SORT_ROWS_THRESHOLD = 5;
#else
static constexpr size_t PARTITION_SORT_ROWS_THRESHOLD = 20000;
#endif

struct PartitionBlocks {
public:
    PartitionBlocks(std::shared_ptr<PartitionSortInfo> partition_sort_info, bool is_first_sorter)
            : _is_first_sorter(is_first_sorter),
              _partition_sort_info(std::move(partition_sort_info)) {}
    ~PartitionBlocks() = default;

    void add_row_idx(size_t row) { _selector.push_back(row); }

    Status append_block_by_selector(const Block* input_block, bool eos);

    Status do_partition_topn_sort();

    void create_or_reset_sorter_state();

    void append_whole_block(Block* input_block, const RowDescriptor& row_desc) {
        auto empty_block = Block::create_unique(VectorizedUtils::create_empty_block(row_desc));
        empty_block->swap(*input_block);
        _blocks.emplace_back(std::move(empty_block));
    }

    bool reach_limit() {
        return _init_rows <= 0 || _blocks.back()->bytes() > INITIAL_BUFFERED_BLOCK_BYTES;
    }

    IColumn::Selector _selector;
    std::vector<std::unique_ptr<Block>> _blocks;
    size_t _current_input_rows = 0;
    int64_t _init_rows = 4096;
    bool _is_first_sorter = false;

    std::unique_ptr<SortCursorCmp> _previous_row;
    std::unique_ptr<PartitionSorter> _partition_topn_sorter = nullptr;
    std::shared_ptr<PartitionSortInfo> _partition_sort_info = nullptr;
};

using PartitionDataPtr = PartitionBlocks*;
using PartitionDataWithStringKey = PHHashMap<StringRef, PartitionDataPtr>;
using PartitionDataWithShortStringKey = StringHashMap<PartitionDataPtr>;

template <typename T>
using PartitionData = PHHashMap<T, PartitionDataPtr, HashCRC32<T>>;

template <typename T>
using PartitionDataSingle = MethodOneNumber<T, PartitionData<T>>;

template <typename T>
using PartitionDataSingleNullable =
        MethodSingleNullableColumn<MethodOneNumber<T, DataWithNullKey<PartitionData<T>>>>;

using PartitionedMethodVariants = std::variant<
        std::monostate, MethodSerialized<PartitionDataWithStringKey>, PartitionDataSingle<UInt8>,
        PartitionDataSingle<UInt16>, PartitionDataSingle<UInt32>, PartitionDataSingle<UInt64>,
        PartitionDataSingle<UInt128>, PartitionDataSingle<UInt256>,
        PartitionDataSingleNullable<UInt8>, PartitionDataSingleNullable<UInt16>,
        PartitionDataSingleNullable<UInt32>, PartitionDataSingleNullable<UInt64>,
        PartitionDataSingleNullable<UInt128>, PartitionDataSingleNullable<UInt256>,
        MethodKeysFixed<PartitionData<UInt64>>, MethodKeysFixed<PartitionData<UInt72>>,
        MethodKeysFixed<PartitionData<UInt96>>, MethodKeysFixed<PartitionData<UInt104>>,
        MethodKeysFixed<PartitionData<UInt128>>, MethodKeysFixed<PartitionData<UInt136>>,
        MethodKeysFixed<PartitionData<UInt256>>,
        MethodStringNoCache<PartitionDataWithShortStringKey>,
        MethodSingleNullableColumn<
                MethodStringNoCache<DataWithNullKey<PartitionDataWithShortStringKey>>>>;

struct PartitionedHashMapVariants
        : public DataVariants<PartitionedMethodVariants, MethodSingleNullableColumn,
                              MethodOneNumber, DataWithNullKey> {
    void init(const std::vector<DataTypePtr>& data_types, HashKeyType type) {
        bool nullable = data_types.size() == 1 && data_types[0]->is_nullable();
        switch (type) {
        case HashKeyType::without_key: {
            break;
        }
        case HashKeyType::serialized: {
            method_variant.emplace<MethodSerialized<PartitionDataWithStringKey>>();
            break;
        }
        case HashKeyType::int8_key: {
            emplace_single<UInt8, PartitionData<UInt8>>(nullable);
            break;
        }
        case HashKeyType::int16_key: {
            emplace_single<UInt16, PartitionData<UInt16>>(nullable);
            break;
        }
        case HashKeyType::int32_key: {
            emplace_single<UInt32, PartitionData<UInt32>>(nullable);
            break;
        }
        case HashKeyType::int64_key: {
            emplace_single<UInt64, PartitionData<UInt64>>(nullable);
            break;
        }
        case HashKeyType::int128_key: {
            emplace_single<UInt128, PartitionData<UInt128>>(nullable);
            break;
        }
        case HashKeyType::int256_key: {
            emplace_single<UInt256, PartitionData<UInt256>>(nullable);
            break;
        }
        case HashKeyType::string_key: {
            if (nullable) {
                method_variant.emplace<MethodSingleNullableColumn<
                        MethodStringNoCache<DataWithNullKey<PartitionDataWithShortStringKey>>>>();
            } else {
                method_variant.emplace<MethodStringNoCache<PartitionDataWithShortStringKey>>();
            }
            break;
        }
        case HashKeyType::fixed64:
            method_variant.emplace<MethodKeysFixed<PartitionData<UInt64>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed72:
            method_variant.emplace<MethodKeysFixed<PartitionData<UInt72>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed96:
            method_variant.emplace<MethodKeysFixed<PartitionData<UInt96>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed104:
            method_variant.emplace<MethodKeysFixed<PartitionData<UInt104>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<MethodKeysFixed<PartitionData<UInt128>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<MethodKeysFixed<PartitionData<UInt136>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<MethodKeysFixed<PartitionData<UInt256>>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "PartitionedHashMapVariants meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris
