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

#include <variant>
#include <vector>

#include "vec/common/arena.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/hash_map_util.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/common/sort/partition_sorter.h"
#include "vec/common/sort/vsort_exec_exprs.h"

namespace doris {

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

#ifndef NDEBUG
static constexpr size_t PARTITION_SORT_ROWS_THRESHOLD = 10;
#else
static constexpr size_t PARTITION_SORT_ROWS_THRESHOLD = 20000;
#endif

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
using PartitionDataWithUInt64Key =
        PHHashMap<vectorized::UInt64, PartitionDataPtr, HashCRC32<vectorized::UInt64>>;
using PartitionDataWithUInt128Key =
        PHHashMap<vectorized::UInt128, PartitionDataPtr, HashCRC32<vectorized::UInt128>>;
using PartitionDataWithUInt256Key =
        PHHashMap<vectorized::UInt256, PartitionDataPtr, HashCRC32<vectorized::UInt256>>;
using PartitionDataWithUInt136Key =
        PHHashMap<vectorized::UInt136, PartitionDataPtr, HashCRC32<vectorized::UInt136>>;

using PartitionedMethodVariants = std::variant<
        std::monostate, vectorized::MethodSerialized<PartitionDataWithStringKey>,
        vectorized::MethodOneNumber<vectorized::UInt8, PartitionDataWithUInt8Key>,
        vectorized::MethodOneNumber<vectorized::UInt16, PartitionDataWithUInt16Key>,
        vectorized::MethodOneNumber<vectorized::UInt32, PartitionDataWithUInt32Key>,
        vectorized::MethodOneNumber<vectorized::UInt64, PartitionDataWithUInt64Key>,
        vectorized::MethodOneNumber<vectorized::UInt128, PartitionDataWithUInt128Key>,
        vectorized::MethodOneNumber<vectorized::UInt256, PartitionDataWithUInt256Key>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt8, vectorized::DataWithNullKey<PartitionDataWithUInt8Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt16, vectorized::DataWithNullKey<PartitionDataWithUInt16Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt32, vectorized::DataWithNullKey<PartitionDataWithUInt32Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt64, vectorized::DataWithNullKey<PartitionDataWithUInt64Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt128, vectorized::DataWithNullKey<PartitionDataWithUInt128Key>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt256, vectorized::DataWithNullKey<PartitionDataWithUInt256Key>>>,
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
        : public DataVariants<PartitionedMethodVariants, vectorized::MethodSingleNullableColumn,
                              vectorized::MethodOneNumber, vectorized::MethodKeysFixed,
                              vectorized::DataWithNullKey> {
    template <bool nullable>
    void init(const std::vector<vectorized::DataTypePtr>& data_types, HashKeyType type) {
        switch (type) {
        case HashKeyType::without_key: {
            break;
        }
        case HashKeyType::serialized: {
            method_variant.emplace<vectorized::MethodSerialized<PartitionDataWithStringKey>>();
            break;
        }
        case HashKeyType::int8_key: {
            emplace_single<vectorized::UInt8, PartitionDataWithUInt8Key, nullable>();
            break;
        }
        case HashKeyType::int16_key: {
            emplace_single<vectorized::UInt16, PartitionDataWithUInt16Key, nullable>();
            break;
        }
        case HashKeyType::int32_key: {
            emplace_single<vectorized::UInt32, PartitionDataWithUInt32Key, nullable>();
            break;
        }
        case HashKeyType::int64_key: {
            emplace_single<vectorized::UInt64, PartitionDataWithUInt64Key, nullable>();
            break;
        }
        case HashKeyType::int128_key: {
            emplace_single<vectorized::UInt128, PartitionDataWithUInt128Key, nullable>();
            break;
        }
        case HashKeyType::int256_key: {
            emplace_single<vectorized::UInt256, PartitionDataWithUInt256Key, nullable>();
            break;
        }
        case HashKeyType::string_key: {
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
        case HashKeyType::fixed64:
            method_variant
                    .emplace<vectorized::MethodKeysFixed<PartitionDataWithUInt64Key, nullable>>(
                            get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant
                    .emplace<vectorized::MethodKeysFixed<PartitionDataWithUInt128Key, nullable>>(
                            get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant
                    .emplace<vectorized::MethodKeysFixed<PartitionDataWithUInt136Key, nullable>>(
                            get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant
                    .emplace<vectorized::MethodKeysFixed<PartitionDataWithUInt256Key, nullable>>(
                            get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "PartitionedHashMapVariants meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris
