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

    Status evaluate(const Schema& schema, InvertedIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override {
        return _nested->evaluate(schema, iterator, num_rows, bitmap);
    }

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        return _nested->can_do_apply_safely(input_type, is_null);
    }

    uint16_t evaluate(const vectorized::IColumn& column, uint16_t* sel,
                      uint16_t size) const override {
        if (column.has_null()) {
            // create selected_flags
            uint16_t max_idx = *std::max_element(sel, sel + size);
            auto selected_flags_ptr = std::make_unique<bool[]>(max_idx + 1);
            auto selected_flags = selected_flags_ptr.get();
            // init to 0 / false
            memset(selected_flags, 0, (max_idx + 1) * sizeof(bool));
            for (uint16_t i = 0; i < size; ++i) {
                uint16_t row_idx = sel[i];
                if (column.is_null_at(row_idx)) {
                    // set selected flag true for NULL value
                    selected_flags[row_idx] = true;
                }
            }

            // call nested predicate evaluate
            uint16_t new_size = _nested->evaluate(column, sel, size);

            // process NULL values
            if (new_size < size) {
                // add rows selected by _nested->evaluate
                for (uint16_t i = 0; i < new_size; ++i) {
                    uint16_t row_idx = sel[i];
                    selected_flags[row_idx] = true;
                }

                // recaculate new_size and sel array
                new_size = 0;
                for (uint16_t row_idx = 0; row_idx < max_idx + 1; ++row_idx) {
                    if (selected_flags[row_idx]) {
                        sel[new_size++] = row_idx;
                    }
                }
            }

            return new_size;
        } else {
            return _nested->evaluate(column, sel, size);
        }
    }

    void evaluate_and(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        if (column.has_null()) {
            // copy original flags
            auto original_flags_buf = std::make_unique<bool[]>(size);
            auto original_flags = original_flags_buf.get();
            memcpy(original_flags, flags, size * sizeof(bool));

            // call evaluate_and and restore true for NULL rows
            _nested->evaluate_and(column, sel, size, flags);
            for (uint16_t i = 0; i < size; ++i) {
                uint16_t idx = sel[i];
                if (original_flags[idx] && !flags[idx] && column.is_null_at(idx)) {
                    flags[i] = true;
                }
            }
        } else {
            _nested->evaluate_and(column, sel, size, flags);
        }
    }

    void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override {
        if (column.has_null()) {
            // call evaluate_or and set true for NULL rows
            _nested->evaluate_or(column, sel, size, flags);
            for (uint16_t i = 0; i < size; ++i) {
                uint16_t idx = sel[i];
                if (!flags[idx] && column.is_null_at(idx)) {
                    flags[i] = true;
                }
            }
        } else {
            _nested->evaluate_or(column, sel, size, flags);
        }
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
        _nested->evaluate_vec(column, size, flags);
        if (column.has_null()) {
            for (uint16_t i = 0; i < size; ++i) {
                if (!flags[i] && column.is_null_at(i)) {
                    // set true for NULL rows
                    flags[i] = true;
                }
            }
        }
    }

    void evaluate_and_vec(const vectorized::IColumn& column, uint16_t size,
                          bool* flags) const override {
        if (column.has_null()) {
            // copy original flags
            auto original_flags_buf = std::make_unique<bool[]>(size);
            auto original_flags = original_flags_buf.get();
            memcpy(original_flags, flags, size * sizeof(bool));

            // call evaluate_and_vec and restore true for NULL rows
            _nested->evaluate_and_vec(column, size, flags);
            for (uint16_t i = 0; i < size; ++i) {
                if (original_flags[i] && !flags[i] && column.is_null_at(i)) {
                    flags[i] = true;
                }
            }
        } else {
            _nested->evaluate_and_vec(column, size, flags);
        }
    }

    std::string get_search_str() const override { return _nested->get_search_str(); }

    std::string debug_string() const override {
        return "passnull predicate for " + _nested->debug_string();
    }

    /// Some predicates need to be cloned for each segment.
    bool need_to_clone() const override { return _nested->need_to_clone(); }

    void clone(ColumnPredicate** to) const override {
        if (need_to_clone()) {
            ColumnPredicate* clone_nested;
            _nested->clone(&clone_nested);
            *to = new AcceptNullPredicate(clone_nested);
        }
    }

private:
    std::string _debug_string() const override {
        return "passnull predicate for " + _nested->debug_string();
    }

    std::unique_ptr<ColumnPredicate> _nested;
};

} //namespace doris
