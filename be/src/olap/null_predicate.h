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

#include <roaring/roaring.hh>

#include "olap/column_predicate.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/wrapper_field.h"

namespace doris {

class NullPredicate : public ColumnPredicate {
public:
    NullPredicate(uint32_t column_id, bool is_null, bool opposite = false);

    PredicateType type() const override;

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;

    void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) const override;

    void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) const override;

    Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* roaring) const override;

    uint16_t evaluate(const vectorized::IColumn& column, uint16_t* sel,
                      uint16_t size) const override;

    void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override;

    void evaluate_and(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override;

    bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        if (_is_null) {
            return statistic.first->is_null();
        } else {
            return !statistic.second->is_null();
        }
    }

    bool evaluate_del(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        if (_is_null) {
            return statistic.first->is_null() && statistic.second->is_null();
        } else {
            return !statistic.first->is_null() && !statistic.second->is_null();
        }
    }

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override {
        if (_is_null) {
            return bf->test_bytes(nullptr, 0);
        } else {
            LOG(FATAL) << "Bloom filter is not supported by predicate type: is_null=" << _is_null;
            return true;
        }
    }

    bool can_do_bloom_filter() const override { return _is_null; }

    void evaluate_vec(const vectorized::IColumn& column, uint16_t size, bool* flags) const override;

private:
    std::string _debug_string() const override {
        std::string info = "NullPredicate(" + std::string(_is_null ? "is_null" : "not_null") + ")";
        return info;
    }

    bool _is_null; //true for null, false for not null
};

} //namespace doris
