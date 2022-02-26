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

#include <string>
#include <vector>

#include "common/status.h"
#include "exec/es/es_predicate.h"
#include "rapidjson/document.h"

namespace doris {

class QueryBuilder {
public:
    virtual void to_json(rapidjson::Document* document, rapidjson::Value* query) = 0;
    virtual ~QueryBuilder() {}
};

// process esquery(fieldA, json dsl) function
class ESQueryBuilder : public QueryBuilder {
public:
    ESQueryBuilder(const std::string& es_query_str);
    ESQueryBuilder(const ExtFunction& es_query);
    void to_json(rapidjson::Document* document, rapidjson::Value* query) override;

private:
    std::string _es_query_str;
};

// process field = value
class TermQueryBuilder : public QueryBuilder {
public:
    TermQueryBuilder(const std::string& field, const std::string& term);
    TermQueryBuilder(const ExtBinaryPredicate& binary_predicate);
    void to_json(rapidjson::Document* document, rapidjson::Value* query) override;

private:
    std::string _field;
    std::string _term;
    bool _match_none;
};

// process range predicate field >= value or field < value etc.
class RangeQueryBuilder : public QueryBuilder {
public:
    RangeQueryBuilder(const ExtBinaryPredicate& range_predicate);
    void to_json(rapidjson::Document* document, rapidjson::Value* query) override;

private:
    std::string _field;
    std::string _value;
    TExprOpcode::type _op;
};

// process in predicate :  field in [value1, value2]
class TermsInSetQueryBuilder : public QueryBuilder {
public:
    TermsInSetQueryBuilder(const ExtInPredicate& in_predicate);
    void to_json(rapidjson::Document* document, rapidjson::Value* query) override;

private:
    std::string _field;
    std::vector<std::string> _values;
};

// process like predicate : field like "a%b%c_"
class WildCardQueryBuilder : public QueryBuilder {
public:
    WildCardQueryBuilder(const ExtLikePredicate& like_predicate);
    void to_json(rapidjson::Document* document, rapidjson::Value* query) override;

private:
    std::string _like_value;
    std::string _field;
};

// no predicates: all document match
class MatchAllQueryBuilder : public QueryBuilder {
public:
    void to_json(rapidjson::Document* document, rapidjson::Value* query) override;
};

// process like predicate : k1 is null or k1 is not null"
class ExistsQueryBuilder : public QueryBuilder {
public:
    ExistsQueryBuilder(const ExtIsNullPredicate& like_predicate);
    void to_json(rapidjson::Document* document, rapidjson::Value* query) override;

private:
    std::string _field;
};

// process bool compound query, and play the role of a bridge for transferring predicates to es native query
class BooleanQueryBuilder : public QueryBuilder {
public:
    BooleanQueryBuilder(const std::vector<ExtPredicate*>& predicates);
    BooleanQueryBuilder();
    virtual ~BooleanQueryBuilder();
    // class method for transfer predicate to es query value, invoker should enclose this value with `query`
    static void to_query(const std::vector<EsPredicate*>& predicates, rapidjson::Document* root,
                         rapidjson::Value* query);
    // validate esquery syntax
    static Status check_es_query(const ExtFunction& extFunction);
    // decide which predicate can process
    static void validate(const std::vector<EsPredicate*>& espredicates, std::vector<bool>* result);
    static bool validate(const EsPredicate* espredicate);

private:
    // add child query
    void should(QueryBuilder* filter);
    void filter(QueryBuilder* filter);
    void must(QueryBuilder* filter);
    void must_not(QueryBuilder* filter);
    void to_json(rapidjson::Document* document, rapidjson::Value* query) override;

    std::vector<QueryBuilder*> _must_clauses;
    std::vector<QueryBuilder*> _must_not_clauses;
    std::vector<QueryBuilder*> _filter_clauses;
    std::vector<QueryBuilder*> _should_clauses;
};

} // namespace doris
