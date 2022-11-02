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

#include <roaring/roaring.hh>

#include "olap/column_block.h"
#include "olap/rowset/segment_v2/bitmap_index_reader.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/selection_vector.h"
#include "vec/columns/column.h"

using namespace doris::segment_v2;

namespace doris {

class Schema;
class RowBlockV2;

enum class PredicateType {
    UNKNOWN = 0,
    EQ = 1,
    NE = 2,
    LT = 3,
    LE = 4,
    GT = 5,
    GE = 6,
    IN_LIST = 7,
    NOT_IN_LIST = 8,
    IS_NULL = 9,
    IS_NOT_NULL = 10,
    BF = 11, // BloomFilter
};

inline std::string type_to_string(PredicateType type) {
    switch (type) {
    case PredicateType::UNKNOWN:
        return "UNKNOWN";

    case PredicateType::EQ:
        return "EQ";

    case PredicateType::NE:
        return "NE";

    case PredicateType::LT:
        return "LT";

    case PredicateType::LE:
        return "LE";

    case PredicateType::GT:
        return "GT";

    case PredicateType::GE:
        return "GE";

    case PredicateType::IN_LIST:
        return "IN_LIST";

    case PredicateType::NOT_IN_LIST:
        return "NOT_IN_LIST";

    case PredicateType::IS_NULL:
        return "IS_NULL";

    case PredicateType::IS_NOT_NULL:
        return "IS_NOT_NULL";

    case PredicateType::BF:
        return "BF";
    default:
        return "";
    };

    return "";
}

struct PredicateTypeTraits {
    static constexpr bool is_range(PredicateType type) {
        return (type == PredicateType::LT || type == PredicateType::LE ||
                type == PredicateType::GT || type == PredicateType::GE);
    }

    static constexpr bool is_bloom_filter(PredicateType type) { return type == PredicateType::BF; }

    static constexpr bool is_list(PredicateType type) {
        return (type == PredicateType::IN_LIST || type == PredicateType::NOT_IN_LIST);
    }

    static constexpr bool is_comparison(PredicateType type) {
        return (type == PredicateType::EQ || type == PredicateType::NE ||
                type == PredicateType::LT || type == PredicateType::LE ||
                type == PredicateType::GT || type == PredicateType::GE);
    }
};

class ColumnPredicate {
public:
    explicit ColumnPredicate(uint32_t column_id, bool opposite = false)
            : _column_id(column_id), _opposite(opposite) {}

    virtual ~ColumnPredicate() = default;

    virtual PredicateType type() const = 0;

    // evaluate predicate on ColumnBlock
    virtual void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const = 0;
    virtual void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size,
                             bool* flags) const = 0;
    virtual void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,
                              bool* flags) const = 0;

    //evaluate predicate on Bitmap
    virtual Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                            roaring::Roaring* roaring) const = 0;

    // evaluate predicate on IColumn
    // a short circuit eval way
    virtual uint16_t evaluate(const vectorized::IColumn& column, uint16_t* sel,
                              uint16_t size) const {
        return size;
    };
    virtual void evaluate_and(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                              bool* flags) const {};
    virtual void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                             bool* flags) const {};

    virtual bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const {
        return true;
    }

    virtual bool evaluate_del(const std::pair<WrapperField*, WrapperField*>& statistic) const {
        return false;
    }

    virtual bool evaluate_and(const BloomFilter* bf) const { return true; }

    virtual bool can_do_bloom_filter() const { return false; }

    // used to evaluate pre read column in lazy materialization
    // now only support integer/float
    // a vectorized eval way
    virtual void evaluate_vec(const vectorized::IColumn& column, uint16_t size, bool* flags) const {
        DCHECK(false) << "should not reach here";
    }
    virtual void evaluate_and_vec(const vectorized::IColumn& column, uint16_t size,
                                  bool* flags) const {
        DCHECK(false) << "should not reach here";
    }
    uint32_t column_id() const { return _column_id; }

    virtual std::string debug_string() {
        return _debug_string() + ", column_id=" + std::to_string(_column_id) +
               ", opposite=" + (_opposite ? "true" : "false");
    }

protected:
    virtual std::string _debug_string() = 0;

    uint32_t _column_id;
    // TODO: the value is only in delete condition, better be template value
    bool _opposite;
};

} //namespace doris
