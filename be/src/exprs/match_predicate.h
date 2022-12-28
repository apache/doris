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

#include <memory>
#include <string>

#include "exprs/predicate.h"
#include "gen_cpp/Exprs_types.h"
#include "olap/column_predicate.h"
#include "runtime/string_search.hpp"

namespace doris {

enum class MatchType;

class MatchPredicateExpr : public Predicate {
public:
    MatchPredicateExpr(const TExprNode& node) : Predicate(node) {}
    virtual ~MatchPredicateExpr() {}
    Expr* clone(ObjectPool* pool) const override {
        return pool->add(new MatchPredicateExpr(*this));
    }

    static bool is_valid(std::string fn_name) {
        return fn_name == "match_any" || fn_name == "match_all" || fn_name == "match_phrase" ||
               fn_name == "match_element_eq" || fn_name == "match_element_lt" ||
               fn_name == "match_element_gt" || fn_name == "match_element_le" ||
               fn_name == "match_element_ge";
    }

protected:
    friend class Expr;
};

class MatchPredicate : public ColumnPredicate {
public:
    static void init() {}

public:
    MatchPredicate(uint32_t column_id, const std::string& value, MatchType match_type);

    virtual PredicateType type() const override;

    //evaluate predicate on Bitmap
    virtual Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                            roaring::Roaring* roaring) const override {
        LOG(FATAL) << "Not Implemented MatchPredicate::evaluate";
    }

    //evaluate predicate on inverted
    Status evaluate(const Schema& schema, InvertedIndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override;

private:
    InvertedIndexQueryType _to_inverted_index_query_type(MatchType match_type) const;
    std::string _debug_string() const override {
        std::string info = "MatchPredicate";
        return info;
    }

private:
    std::string _value;
    MatchType _match_type;
};

} // namespace doris

#endif