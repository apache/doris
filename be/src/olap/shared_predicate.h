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

// SharedPredicate only used on topn runtime predicate.
// Runtime predicate globally share one predicate, to ensure that updates can be real-time.
// At the beginning nested predicate may be nullptr, in which case predicate always returns true.
class SharedPredicate : public ColumnPredicate {
    ENABLE_FACTORY_CREATOR(SharedPredicate);

public:
    SharedPredicate(uint32_t column_id) : ColumnPredicate(column_id) {}

    PredicateType type() const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            // topn filter is le or ge
            return PredicateType::LE;
        }
        return _nested->type();
    }

    void set_nested(ColumnPredicate* nested) {
        std::unique_lock<std::shared_mutex> lock(_mtx);
        _nested.reset(nested);
    }

    Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* roaring) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return Status::OK();
        }
        return _nested->evaluate(iterator, num_rows, roaring);
    }

    Status evaluate(const vectorized::IndexFieldNameAndTypePair& name_with_type,
                    InvertedIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return Status::OK();
        }
        return _nested->evaluate(name_with_type, iterator, num_rows, bitmap);
    }

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return true;
        }
        return _nested->can_do_apply_safely(input_type, is_null);
    }

    void evaluate_and(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return;
        }
        return _nested->evaluate_and(column, sel, size, flags);
    }

    void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override {
        DCHECK(false) << "should not reach here";
    }

    bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return ColumnPredicate::evaluate_and(statistic);
        }
        return _nested->evaluate_and(statistic);
    }

    bool evaluate_del(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return ColumnPredicate::evaluate_del(statistic);
        }
        return _nested->evaluate_del(statistic);
    }

    bool evaluate_and(const BloomFilter* bf) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return ColumnPredicate::evaluate_and(bf);
        }
        return _nested->evaluate_and(bf);
    }

    bool can_do_bloom_filter(bool ngram) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return ColumnPredicate::can_do_bloom_filter(ngram);
        }
        return _nested->can_do_bloom_filter(ngram);
    }

    void evaluate_vec(const vectorized::IColumn& column, uint16_t size,
                      bool* flags) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            for (uint16_t i = 0; i < size; ++i) {
                flags[i] = true;
            }
            return;
        }
        _nested->evaluate_vec(column, size, flags);
    }

    void evaluate_and_vec(const vectorized::IColumn& column, uint16_t size,
                          bool* flags) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return;
        }
        _nested->evaluate_and_vec(column, size, flags);
    }

    std::string get_search_str() const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            DCHECK(false) << "should not reach here";
        }
        return _nested->get_search_str();
    }

private:
    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return size;
        }
        return _nested->evaluate(column, sel, size);
    }

    std::string _debug_string() const override {
        std::shared_lock<std::shared_mutex> lock(_mtx);
        if (!_nested) {
            return "shared_predicate<unknow>";
        }
        return "shared_predicate<" + _nested->debug_string() + ">";
    }

    mutable std::shared_mutex _mtx;
    std::shared_ptr<ColumnPredicate> _nested;
};

} //namespace doris
