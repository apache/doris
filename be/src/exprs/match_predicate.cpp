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

#include "exprs/match_predicate.h"

#include <string.h>

#include <memory>
#include <sstream>

#include "exec/olap_utils.h"
#include "exprs/string_functions.h"
#include "olap/schema.h"
#include "runtime/string_value.hpp"

namespace doris {

MatchPredicate::MatchPredicate(uint32_t column_id, const std::string& value, MatchType match_type)
        : ColumnPredicate(column_id), _value(value), _match_type(match_type) {}

PredicateType MatchPredicate::type() const {
    return PredicateType::MATCH;
}

Status MatchPredicate::evaluate(const Schema& schema, InvertedIndexIterator* iterator,
                                uint32_t num_rows, roaring::Roaring* bitmap) const {
    if (iterator == nullptr) {
        return Status::OK();
    }
    auto column_desc = schema.column(_column_id);
    roaring::Roaring roaring;
    Status s = Status::OK();
    auto inverted_index_query_type = _to_inverted_index_query_type(_match_type);

    if (is_string_type(column_desc->type()) ||
        (column_desc->type() == OLAP_FIELD_TYPE_ARRAY &&
         is_string_type(column_desc->get_sub_field(0)->type_info()->type()))) {
        StringValue match_value;
        int32_t length = _value.length();
        char* buffer = const_cast<char*>(_value.c_str());
        match_value.replace(buffer, length);
        s = iterator->read_from_inverted_index(column_desc->name(), &match_value,
                                               inverted_index_query_type, num_rows, &roaring);
    } else if (column_desc->type() == OLAP_FIELD_TYPE_ARRAY &&
               is_numeric_type(column_desc->get_sub_field(0)->type_info()->type())) {
        char buf[column_desc->get_sub_field(0)->type_info()->size()];
        column_desc->get_sub_field(0)->from_string(buf, _value);
        s = iterator->read_from_inverted_index(column_desc->name(), buf, inverted_index_query_type,
                                               num_rows, &roaring);
    }
    *bitmap &= roaring;
    return s;
}

InvertedIndexQueryType MatchPredicate::_to_inverted_index_query_type(MatchType match_type) const {
    auto ret = InvertedIndexQueryType::UNKNOWN_QUERY;
    switch (match_type) {
    case MatchType::MATCH_ANY:
        ret = InvertedIndexQueryType::MATCH_ANY_QUERY;
        break;
    case MatchType::MATCH_ALL:
        ret = InvertedIndexQueryType::MATCH_ALL_QUERY;
        break;
    case MatchType::MATCH_PHRASE:
        ret = InvertedIndexQueryType::MATCH_PHRASE_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_EQ:
        ret = InvertedIndexQueryType::EQUAL_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_LT:
        ret = InvertedIndexQueryType::LESS_THAN_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_GT:
        ret = InvertedIndexQueryType::GREATER_THAN_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_LE:
        ret = InvertedIndexQueryType::LESS_EQUAL_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_GE:
        ret = InvertedIndexQueryType::GREATER_EQUAL_QUERY;
        break;
    default:
        DCHECK(false);
    }
    return ret;
}

} // namespace doris