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
#include<string>
#include<vector>
#include "rapidjson/document.h"
#include "exec/es_predicate.h"
#include "common/status.h"

namespace doris {

class QueryBuilder {

public:
    virtual rapidjson::Value to_json(rapidjson::Document& allocator) = 0;
    virtual ~QueryBuilder() {
    };
};

// process esquery(fieldA, json dsl) function
class ESQueryBuilder : public QueryBuilder {
public:
    ESQueryBuilder(const std::string& es_query_str);
    ESQueryBuilder(ExtFunction* es_query);
    rapidjson::Value to_json(rapidjson::Document& allocator) override;
private:
    std::string _es_query_str;
};

// process field = value 
class TermQueryBuilder : public QueryBuilder {

public:
    TermQueryBuilder(const std::string& field, const std::string& term);
    TermQueryBuilder(ExtBinaryPredicate* binary_predicate);
    rapidjson::Value to_json(rapidjson::Document& document) override;

private:
    std::string _field;
    std::string _term;
};

// process range predicate field >= value or field < value etc.
class RangeQueryBuilder : public QueryBuilder {

public:
    rapidjson::Value to_json(rapidjson::Document& document) override;
    RangeQueryBuilder(ExtBinaryPredicate* range_predicate);
private:
    ExtBinaryPredicate* _range_predicate;
};

// process in predicate :  field in [value1, value2]
class TermsInSetQueryBuilder : public QueryBuilder {

public:
    rapidjson::Value to_json(rapidjson::Document& document) override;
    TermsInSetQueryBuilder(ExtInPredicate* in_predicate);
private:
    ExtInPredicate* _in_predicate;
};

// process like predicate : field like "a%b%c_"
class WildCardQueryBuilder : public QueryBuilder {

public:
    rapidjson::Value to_json(rapidjson::Document& document) override;
    WildCardQueryBuilder(ExtLikePredicate* like_predicate);

private:
    std::string _like_value;
    std::string _field;
};

// no predicates: all doccument match
class MatchAllQueryBuilder : public QueryBuilder {

public:
    rapidjson::Value to_json(rapidjson::Document& document) override;
};

// proccess bool compound query, and play the role of a bridge for transferring predicates to es native query
class BooleanQueryBuilder : public QueryBuilder {

public:
    BooleanQueryBuilder(const std::vector<ExtPredicate*>& predicates);
    BooleanQueryBuilder();
    ~BooleanQueryBuilder();
    rapidjson::Value to_json(rapidjson::Document& document) override;
    void should(QueryBuilder* filter);
    void filter(QueryBuilder* filter);
    void must(QueryBuilder* filter);
    void must_not(QueryBuilder* filter);
    // class method for transfer predicate to es query value, invoker should enclose this value with `query`
    static rapidjson::Value to_query(const std::vector<EsPredicate*>& predicates, rapidjson::Document& root);
    static Status check_es_query(ExtFunction extFunction);
    static std::vector<bool> validate(const std::vector<EsPredicate*>& espredicates);

private:
    std::vector<QueryBuilder*> _must_clauses;
    std::vector<QueryBuilder*> _must_not_clauses;
    std::vector<QueryBuilder*> _filter_clauses;
    std::vector<QueryBuilder*> _should_clauses;
};

}
