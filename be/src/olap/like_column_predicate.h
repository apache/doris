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
        return input_type == T;
    }

    uint16_t evaluate(const vectorized::IColumn& column, uint16_t* sel,
                      uint16_t size) const override;

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
    template <bool is_and>
    void _evaluate_vec(const vectorized::IColumn& column, uint16_t size, bool* flags) const {
        if (column.is_nullable()) {
            auto* nullable_col =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& null_map_data = nullable_col->get_null_map_column().get_data();
            auto& nested_col = nullable_col->get_nested_column();
            if (nested_col.is_column_dictionary()) {
                auto* nested_col_ptr = vectorized::check_and_get_column<
                        vectorized::ColumnDictionary<vectorized::Int32>>(nested_col);
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

                    StringRef cell_value = nested_col_ptr->get_shrink_value(data_array[i]);
                    if constexpr (is_and) {
                        unsigned char flag = 0;
                        (_state->scalar_function)(
                                const_cast<vectorized::LikeSearchState*>(&_like_state),
                                StringRef(cell_value.data, cell_value.size), pattern, &flag);
                        flags[i] &= _opposite ^ flag;
                    } else {
                        unsigned char flag = 0;
                        (_state->scalar_function)(
                                const_cast<vectorized::LikeSearchState*>(&_like_state),
                                StringRef(cell_value.data, cell_value.size), pattern, &flag);
                        flags[i] = _opposite ^ flag;
                    }
                }
            } else {
                LOG(FATAL) << "vectorized (not) like predicates should be dict column";
            }
        } else {
            if (column.is_column_dictionary()) {
                auto* nested_col_ptr = vectorized::check_and_get_column<
                        vectorized::ColumnDictionary<vectorized::Int32>>(column);
                auto& data_array = nested_col_ptr->get_data();
                for (uint16_t i = 0; i < size; i++) {
                    StringRef cell_value = nested_col_ptr->get_shrink_value(data_array[i]);
                    if constexpr (is_and) {
                        unsigned char flag = 0;
                        (_state->scalar_function)(
                                const_cast<vectorized::LikeSearchState*>(&_like_state),
                                StringRef(cell_value.data, cell_value.size), pattern, &flag);
                        flags[i] &= _opposite ^ flag;
                    } else {
                        unsigned char flag = 0;
                        (_state->scalar_function)(
                                const_cast<vectorized::LikeSearchState*>(&_like_state),
                                StringRef(cell_value.data, cell_value.size), pattern, &flag);
                        flags[i] = _opposite ^ flag;
                    }
                }
            } else {
                LOG(FATAL) << "vectorized (not) like predicates should be dict column";
            }
        }
    }

    std::string _debug_string() const override {
        std::string info = "LikeColumnPredicate";
        return info;
    }

    std::string _origin;
    // lifetime controlled by scan node
    using StateType = vectorized::LikeState;
    StringRef pattern;

    StateType* _state;

    // A separate scratch region is required for every concurrent caller of the
    // Hyperscan API. So here _like_state is separate for each instance of
    // LikeColumnPredicate.
    vectorized::LikeSearchState _like_state;
    std::unique_ptr<segment_v2::BloomFilter> _page_ng_bf; // for ngram-bf index
};

} // namespace doris
