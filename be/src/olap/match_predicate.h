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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_MATCH_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_MATCH_PREDICATE_H

#include <glog/logging.h>
#include <stdint.h>

#include <ostream>
#include <string>

#include "common/status.h"
#include "olap/column_predicate.h"
#include "olap/schema.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {

enum class MatchType;

namespace segment_v2 {
class BitmapIndexIterator;
class InvertedIndexIterator;
enum class InvertedIndexQueryType;
} // namespace segment_v2

class MatchPredicate : public ColumnPredicate {
public:
    MatchPredicate(uint32_t column_id, const std::string& value, MatchType match_type);

    virtual PredicateType type() const override;

    const std::string& get_value() const { return _value; }

    bool support_zonemap() const override { return false; }

    //evaluate predicate on Bitmap
    Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* roaring) const override {
        LOG(FATAL) << "Not Implemented MatchPredicate::evaluate";
        __builtin_unreachable();
    }

    //evaluate predicate on inverted
    Status evaluate(const vectorized::IndexFieldNameAndTypePair& name_with_type,
                    InvertedIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override;

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        return is_string_type(input_type);
    }

private:
    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override {
        return size;
    }

    InvertedIndexQueryType _to_inverted_index_query_type(MatchType match_type) const;
    std::string _debug_string() const override {
        std::string info = "MatchPredicate";
        return info;
    }
    bool _check_evaluate(InvertedIndexIterator* iterator) const;

private:
    std::string _value;
    MatchType _match_type;
};

} // namespace doris

#endif