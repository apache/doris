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

#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "olap/column_predicate.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/schema.h"
#include "olap/wrapper_field.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
namespace segment_v2 {
class BitmapIndexIterator;
class InvertedIndexIterator;
} // namespace segment_v2
namespace vectorized {
class IColumn;
} // namespace vectorized

class NullPredicate : public ColumnPredicate {
public:
    NullPredicate(uint32_t column_id, bool is_null, bool opposite = false);

    PredicateType type() const override;

    Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* roaring) const override;

    Status evaluate(const vectorized::IndexFieldNameAndTypePair& name_with_type,
                    InvertedIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override;

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
        // evaluate_del only use for delete condition to filter page, need use delete condition origin value,
        // when opposite==true, origin value 'is null'->'is not null' and 'is not null'->'is null',
        // so when _is_null==true, need check 'is not null' and _is_null==false, need check 'is null'
        if (_is_null) {
            return !statistic.first->is_null() && !statistic.second->is_null();
        } else {
            return statistic.first->is_null() && statistic.second->is_null();
        }
    }

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override {
        // null predicate can not use ngram bf, just return true to accept
        if (bf->is_ngram_bf()) return true;
        if (_is_null) {
            return bf->test_bytes(nullptr, 0);
        } else {
            LOG(FATAL) << "Bloom filter is not supported by predicate type: is_null=" << _is_null;
            return true;
        }
    }

    bool can_do_bloom_filter(bool ngram) const override { return _is_null && !ngram; }

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        // Always safe to apply is null predicate
        return true;
    }

    void evaluate_vec(const vectorized::IColumn& column, uint16_t size, bool* flags) const override;

private:
    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override;

    std::string _debug_string() const override {
        std::string info = "NullPredicate(" + std::string(_is_null ? "is_null" : "not_null") + ")";
        return info;
    }

    bool _is_null; //true for null, false for not null
};

} //namespace doris
