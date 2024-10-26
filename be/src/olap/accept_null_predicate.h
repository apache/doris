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

#include <cstdint>
#include <memory>

#include "common/factory_creator.h"
#include "olap/column_predicate.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/wrapper_field.h"
#include "vec/columns/column_dictionary.h"

namespace doris {

/**
 * A wrapper predicate that delegate to nested predicate
 *  but pass (set/return true) for NULL value rows.
 *
 * At parent, it's used for topn runtime predicate.
 * Eg: original input indexs is '1,2,3,7,8,9' and value of index9 is null, we get nested predicate output index is '1,2,3', but we finally output '1,2,3,9'
*/
class AcceptNullPredicate : public ColumnPredicate {
    ENABLE_FACTORY_CREATOR(AcceptNullPredicate);

public:
    AcceptNullPredicate(ColumnPredicate* nested)
            : ColumnPredicate(nested->column_id(), nested->opposite()), _nested {nested} {}

    PredicateType type() const override { return _nested->type(); }

    Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* roaring) const override {
        return _nested->evaluate(iterator, num_rows, roaring);
    }

    Status evaluate(const vectorized::IndexFieldNameAndTypePair& name_with_type,
                    InvertedIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override {
        return _nested->evaluate(name_with_type, iterator, num_rows, bitmap);
    }

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        return _nested->can_do_apply_safely(input_type, is_null);
    }

    void evaluate_and(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        if (column.has_null()) {
            std::vector<uint8_t> original_flags(size);
            memcpy(original_flags.data(), flags, size);

            const auto& nullable_col = assert_cast<const vectorized::ColumnNullable&>(column);
            _nested->evaluate_and(nullable_col.get_nested_column(), sel, size, flags);
            const auto& nullmap = nullable_col.get_null_map_data();
            for (uint16_t i = 0; i < size; ++i) {
                flags[i] |= (original_flags[i] && nullmap[sel[i]]);
            }
        } else {
            _nested->evaluate_and(column, sel, size, flags);
        }
    }

    void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override {
        DCHECK(false) << "should not reach here";
    }

    bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        // there is null in range, accept it
        if (statistic.first->is_null() || statistic.second->is_null()) {
            return true;
        }
        return _nested->evaluate_and(statistic);
    }

    bool evaluate_del(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        return _nested->evaluate_del(statistic);
    }

    bool evaluate_and(const BloomFilter* bf) const override { return _nested->evaluate_and(bf); }

    bool can_do_bloom_filter(bool ngram) const override {
        return _nested->can_do_bloom_filter(ngram);
    }

    void evaluate_vec(const vectorized::IColumn& column, uint16_t size,
                      bool* flags) const override {
        if (column.has_null()) {
            const auto& nullable_col = assert_cast<const vectorized::ColumnNullable&>(column);
            _nested->evaluate_vec(nullable_col.get_nested_column(), size, flags);
            for (uint16_t i = 0; i < size; ++i) {
                if (!flags[i] && nullable_col.is_null_at(i)) {
                    // set true for NULL rows
                    flags[i] = true;
                }
            }
        } else {
            _nested->evaluate_vec(column, size, flags);
        }
    }

    void evaluate_and_vec(const vectorized::IColumn& column, uint16_t size,
                          bool* flags) const override {
        if (column.has_null()) {
            // copy original flags
            std::vector<uint8_t> original_flags(size);
            memcpy(original_flags.data(), flags, size);

            const auto& nullable_col = assert_cast<const vectorized::ColumnNullable&>(column);
            // call evaluate_and_vec and restore true for NULL rows
            _nested->evaluate_and_vec(nullable_col.get_nested_column(), size, flags);
            for (uint16_t i = 0; i < size; ++i) {
                if (original_flags[i] && !flags[i] && nullable_col.is_null_at(i)) {
                    flags[i] = true;
                }
            }
        } else {
            _nested->evaluate_and_vec(column, size, flags);
        }
    }

    std::string get_search_str() const override { return _nested->get_search_str(); }

private:
    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override {
        if (column.has_null()) {
            if (size == 0) {
                return 0;
            }
            // create selected_flags
            uint16_t max_idx = sel[size - 1];
            std::vector<uint16_t> old_sel(size);
            memcpy(old_sel.data(), sel, sizeof(uint16_t) * size);

            const auto& nullable_col = assert_cast<const vectorized::ColumnNullable&>(column);
            // call nested predicate evaluate
            uint16_t new_size = _nested->evaluate(nullable_col.get_nested_column(), sel, size);

            // process NULL values
            if (new_size < size) {
                std::vector<uint8_t> selected(max_idx + 1, 0);
                const auto* nullmap = nullable_col.get_null_map_data().data();
                // add rows selected by _nested->evaluate
                for (uint16_t i = 0; i < new_size; ++i) {
                    uint16_t row_idx = sel[i];
                    selected[row_idx] = true;
                }
                // reset null from original data
                for (uint16_t i = 0; i < size; ++i) {
                    uint16_t row_idx = old_sel[i];
                    selected[row_idx] |= nullmap[row_idx];
                }

                // recaculate new_size and sel array
                new_size = 0;
                for (uint16_t row_idx = 0; row_idx < max_idx + 1; ++row_idx) {
                    if (selected[row_idx]) {
                        sel[new_size++] = row_idx;
                    }
                }
            }
            return new_size;
        }
        return _nested->evaluate(column, sel, size);
    }

    std::string _debug_string() const override {
        return "passnull predicate for " + _nested->debug_string();
    }

    std::unique_ptr<ColumnPredicate> _nested;
};

} //namespace doris
