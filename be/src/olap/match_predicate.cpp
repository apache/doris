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

#include "olap/match_predicate.h"

#include <string.h>

#include <memory>
#include <sstream>

#include "exec/olap_utils.h"
#include "exprs/string_functions.h"
#include "olap/schema.h"
#include "vec/common/string_ref.h"

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
    auto inverted_index_query_op = _to_inverted_index_query_op(_match_type);
    InvertedIndexQueryType* query = nullptr;
    bool skip_try_inverted_index = false;

    if (is_string_type(column_desc->type())) {
        RETURN_IF_ERROR(InvertedIndexQueryRangeTypeFactory::create_inverted_index_query(
                column_desc->type_info(), &query));
        RETURN_IF_ERROR(std::visit(
                [&](auto& q) -> Status {
                    return q.add_value(inverted_index_query_op, predicate_params().get());
                },
                *query));
        s = iterator->read_from_inverted_index(column_desc->name(), query, num_rows, &roaring,
                                               skip_try_inverted_index);
    } else if (column_desc->type() == OLAP_FIELD_TYPE_ARRAY) {
        RETURN_IF_ERROR(InvertedIndexQueryRangeTypeFactory::create_inverted_index_query(
                column_desc->get_sub_field(0)->type_info(), &query));

        if (is_string_type(column_desc->get_sub_field(0)->type_info()->type())) {
            RETURN_IF_ERROR(std::visit(
                    [&](auto& q) -> Status {
                        return q.add_value(inverted_index_query_op, predicate_params().get());
                    },
                    *query));
            s = iterator->read_from_inverted_index(column_desc->name(), query, num_rows, &roaring,
                                                   skip_try_inverted_index);
        } else if (is_numeric_type(column_desc->get_sub_field(0)->type_info()->type())) {
            RETURN_IF_ERROR(std::visit(
                    [&](auto& q) -> Status {
                        return q.add_value(inverted_index_query_op, predicate_params().get());
                    },
                    *query));
            skip_try_inverted_index = true;
            s = iterator->read_from_inverted_index(column_desc->name(), query, num_rows, &roaring,
                                                   skip_try_inverted_index);
        }
    }
    bitmap->swap(roaring);
    return s;
}

InvertedIndexQueryOp MatchPredicate::_to_inverted_index_query_op(MatchType match_type) const {
    auto ret = InvertedIndexQueryOp::UNKNOWN_QUERY;
    switch (match_type) {
    case MatchType::MATCH_ANY:
        ret = InvertedIndexQueryOp::MATCH_ANY_QUERY;
        break;
    case MatchType::MATCH_ALL:
        ret = InvertedIndexQueryOp::MATCH_ALL_QUERY;
        break;
    case MatchType::MATCH_PHRASE:
        ret = InvertedIndexQueryOp::MATCH_PHRASE_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_EQ:
        ret = InvertedIndexQueryOp::EQUAL_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_LT:
        ret = InvertedIndexQueryOp::LESS_THAN_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_GT:
        ret = InvertedIndexQueryOp::GREATER_THAN_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_LE:
        ret = InvertedIndexQueryOp::LESS_EQUAL_QUERY;
        break;
    case MatchType::MATCH_ELEMENT_GE:
        ret = InvertedIndexQueryOp::GREATER_EQUAL_QUERY;
        break;
    default:
        DCHECK(false);
    }
    return ret;
}

} // namespace doris