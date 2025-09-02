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
#include <stdint.h>

#include <boost/iterator/iterator_facade.hpp>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "olap/column_predicate.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "vec/columns/column.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/functions/like.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class FunctionContext;

namespace segment_v2 {
class BitmapIndexIterator;
} // namespace segment_v2

template <PrimitiveType T>
class LikeColumnPredicate : public ColumnPredicate {
public:
    LikeColumnPredicate(bool opposite, uint32_t column_id, doris::FunctionContext* fn_ctx,
                        doris::StringRef val);
    ~LikeColumnPredicate() override = default;

    PredicateType type() const override { return PredicateType::EQ; }
    void evaluate_vec(const vectorized::IColumn& column, uint16_t size, bool* flags) const override;

    Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* roaring) const override {
        return Status::OK();
    }

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        return input_type == T || (is_string_type(input_type) && is_string_type(T));
    }

    void evaluate_and_vec(const vectorized::IColumn& column, uint16_t size,
                          bool* flags) const override;

    std::string get_search_str() const override {
        return std::string(reinterpret_cast<const char*>(pattern.data), pattern.size);
    }
    bool is_opposite() const { return _opposite; }

    void set_page_ng_bf(std::unique_ptr<segment_v2::BloomFilter> src) override {
        _page_ng_bf = std::move(src);
    }
    bool evaluate_and(const BloomFilter* bf) const override {
        // like predicate can not use normal bf, just return true to accept
        if (!bf->is_ngram_bf()) return true;
        if (_page_ng_bf) {
            return bf->contains(*_page_ng_bf);
        }
        return true;
    }
    bool can_do_bloom_filter(bool ngram) const override { return ngram; }

private:
    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override;

    template <bool is_and>
    void _evaluate_vec(const vectorized::IColumn& column, uint16_t size, bool* flags) const {
        if (column.is_nullable()) {
            auto* nullable_col =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& null_map_data = nullable_col->get_null_map_column().get_data();
            auto& nested_col = nullable_col->get_nested_column();
            if (nested_col.is_column_dictionary()) {
                auto* nested_col_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnDictI32>(nested_col);
                const auto& dict_res = _find_code_from_dictionary_column(*nested_col_ptr);
                auto& data_array = nested_col_ptr->get_data();
                for (uint16_t i = 0; i < size; i++) {
                    if (null_map_data[i]) {
                        if constexpr (is_and) {
                            flags[i] &= _opposite;
                        } else {
                            flags[i] = _opposite;
                        }
                        continue;
                    }

                    unsigned char flag = dict_res[data_array[i]];
                    if constexpr (is_and) {
                        flags[i] &= _opposite ^ flag;
                    } else {
                        flags[i] = _opposite ^ flag;
                    }
                }
            } else {
                throw Exception(Status::FatalError(
                        "vectorized (not) like predicates should be dict column"));
            }
        } else {
            if (column.is_column_dictionary()) {
                auto* nested_col_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
                auto& data_array = nested_col_ptr->get_data();
                const auto& dict_res = _find_code_from_dictionary_column(*nested_col_ptr);
                for (uint16_t i = 0; i < size; i++) {
                    unsigned char flag = dict_res[data_array[i]];
                    if constexpr (is_and) {
                        flags[i] &= _opposite ^ flag;
                    } else {
                        flags[i] = _opposite ^ flag;
                    }
                }
            } else {
                throw Exception(Status::FatalError(
                        "vectorized (not) like predicates should be dict column"));
            }
        }
    }
    std::vector<bool> __attribute__((flatten))
    _find_code_from_dictionary_column(const vectorized::ColumnDictI32& column) const {
        std::vector<bool> res;
        if (_segment_id_to_cached_res_flags.if_contains(
                    column.get_rowset_segment_id(),
                    [&res](const auto& pair) { res = pair.second; })) {
            return res;
        }

        std::vector<bool> tmp_res(column.dict_size(), false);
        for (int i = 0; i < column.dict_size(); i++) {
            StringRef cell_value = column.get_shrink_value(i);
            unsigned char flag = 0;
            THROW_IF_ERROR((_state->scalar_function)(
                    &_like_state,
                    StringRef(cell_value.data, cell_value.size), pattern, &flag));
            tmp_res[i] = flag;
        }
        // Sometimes the dict is not initialized when run comparison predicate here, for example,
        // the full page is null, then the reader will skip read, so that the dictionary is not
        // inited. The cached code is wrong during this case, because the following page maybe not
        // null, and the dict should have items in the future.
        //
        // Cached code may have problems, so that add a config here, if not opened, then
        // we will return the code and not cache it.
        if (!column.is_dict_empty() && config::enable_low_cardinality_cache_code) {
            _segment_id_to_cached_res_flags.emplace(
                    std::pair {column.get_rowset_segment_id(), tmp_res});
        }

        return tmp_res;
    }

    mutable phmap::parallel_flat_hash_map<
            std::pair<RowsetId, uint32_t>, std::vector<bool>,
            phmap::priv::hash_default_hash<std::pair<RowsetId, uint32_t>>,
            phmap::priv::hash_default_eq<std::pair<RowsetId, uint32_t>>,
            std::allocator<std::pair<const std::pair<RowsetId, uint32_t>, int32_t>>, 4,
            std::shared_mutex>
            _segment_id_to_cached_res_flags;

    std::string _debug_string() const override {
        std::string info = "LikeColumnPredicate";
        return info;
    }

    std::string _origin;
    // lifetime controlled by scan node
    using StateType = vectorized::LikeState;
    StringRef pattern;

    StateType* _state = nullptr;

    // A separate scratch region is required for every concurrent caller of the
    // Hyperscan API. So here _like_state is separate for each instance of
    // LikeColumnPredicate.
    vectorized::LikeSearchState _like_state;
    std::unique_ptr<segment_v2::BloomFilter> _page_ng_bf; // for ngram-bf index
};

} // namespace doris
