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
#include <boost/algorithm/string/replace.hpp>
#include "util/es_query_builder.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "common/logging.h"

namespace doris {

ESQueryBuilder::ESQueryBuilder(const std::string& es_query_str) : _es_query_str(es_query_str) {

}
ESQueryBuilder::ESQueryBuilder(ExtFunction* es_query) {
    auto first = es_query->values.front();
    _es_query_str = first.value_to_string();
}

rapidjson::Value ESQueryBuilder::to_json(rapidjson::Document& docuemnt) {
    rapidjson::Document draft;
    draft.Parse<0>(_es_query_str.c_str());
    rapidjson::Document::AllocatorType& draft_allocator = draft.GetAllocator();
    rapidjson::Value query_key;
    rapidjson::Value query_value;
    //{ "term": { "dv": "2" } }
    if (!draft.HasParseError()) {
        for (rapidjson::Value::ConstMemberIterator itr = draft.MemberBegin(); itr != draft.MemberEnd(); itr++) {
            // deep copy, reference http://rapidjson.org/md_doc_tutorial.html#DeepCopyValue
            query_key.CopyFrom(itr->name, draft_allocator);
            query_value.CopyFrom(itr->value, draft_allocator);
           if (query_key.IsString()) {
               // if we found one key, then end loop as QueryDSL only support one `query` root
               break;
            }
        }
    }
    rapidjson::Document::AllocatorType& allocator = docuemnt.GetAllocator();
    rapidjson::Value es_query(rapidjson::kObjectType);
    es_query.SetObject();
    // Move Semantics, reference http://rapidjson.org/md_doc_tutorial.html#MoveSemantics 
    es_query.AddMember(query_key, query_value, allocator);
    return es_query;
}
rapidjson::Value WildCardQueryBuilder::to_json(rapidjson::Document& document) {
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value term_node(rapidjson::kObjectType);
    term_node.SetObject();
    rapidjson::Value field_value(_field.c_str(), allocator);
    rapidjson::Value term_value(_like_value.c_str(), allocator);
    term_node.AddMember(field_value, term_value, allocator);
    rapidjson::Value wildcard_query(rapidjson::kObjectType);
    wildcard_query.SetObject();
    wildcard_query.AddMember("wildcard", term_node, allocator);
    return wildcard_query;

}
WildCardQueryBuilder::WildCardQueryBuilder(ExtLikePredicate* like_predicate) {
    _like_value = like_predicate->value.value_to_string();
    std::replace(_like_value.begin(), _like_value.end(), '_', '?');
    std::replace(_like_value.begin(), _like_value.end(), '%', '*');
    _field = like_predicate->col.name;
}

TermQueryBuilder::TermQueryBuilder(const std::string& field, const std::string& term) : _field(field), _term(term) {

}

TermQueryBuilder::TermQueryBuilder(ExtBinaryPredicate* binary_predicate) {
    _field =  binary_predicate->col.name;
    ExtLiteral literal = binary_predicate->value;
    _term = literal.value_to_string();
}

rapidjson::Value TermQueryBuilder::to_json(rapidjson::Document& docuemnt) {
    rapidjson::Document::AllocatorType& allocator = docuemnt.GetAllocator();
    rapidjson::Value term_node(rapidjson::kObjectType);
    term_node.SetObject();
    rapidjson::Value field_value(_field.c_str(), allocator);
    rapidjson::Value term_value(_term.c_str(), allocator);
    term_node.AddMember(field_value, term_value, allocator);
    rapidjson::Value term_query(rapidjson::kObjectType);
    term_query.SetObject();
    term_query.AddMember("term", term_node, allocator);
    return term_query;
}

rapidjson::Value TermsInSetQueryBuilder::to_json(rapidjson::Document& document) {
    std::string field = _in_predicate->col.name;
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value terms_node(rapidjson::kObjectType);
    rapidjson::Value values_node(rapidjson::kArrayType);
    for (auto value : _in_predicate->values) {
         rapidjson::Value value_value(value.value_to_string().c_str(), allocator);
        values_node.PushBack(value_value, allocator);
    }
    rapidjson::Value field_value(field.c_str(), allocator);
    terms_node.AddMember(field_value, values_node, allocator);
    rapidjson::Value terms_in_set_query(rapidjson::kObjectType);
    terms_in_set_query.SetObject();
    terms_in_set_query.AddMember("terms", terms_node, allocator);
    return terms_in_set_query;
}

TermsInSetQueryBuilder::TermsInSetQueryBuilder(ExtInPredicate* in_predicate) {
    _in_predicate = in_predicate;
}

rapidjson::Value RangeQueryBuilder::to_json(rapidjson::Document& document) {
    std::string field = _range_predicate->col.name;
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value field_value(field.c_str(), allocator);
    ExtLiteral b_value = _range_predicate->value;
    rapidjson::Value value(b_value.value_to_string().c_str(), allocator);
    rapidjson::Value op_node(rapidjson::kObjectType);
    op_node.SetObject();
    switch (_range_predicate->op)
    {
        case TExprOpcode::LT:
            op_node.AddMember("lt", value, allocator);
            break;
        case TExprOpcode::LE:
            op_node.AddMember("le", value, allocator);
            break;
        case TExprOpcode::GT:
            op_node.AddMember("gt", value, allocator);
            break;
        case TExprOpcode::GE:
            op_node.AddMember("ge", value, allocator);
            break;
        default:
            break;
    }
    rapidjson::Value field_node(rapidjson::kObjectType);
    field_node.SetObject();
    field_node.AddMember(field_value, op_node, allocator);

    rapidjson::Value range_query(rapidjson::kObjectType);
    range_query.SetObject();
    range_query.AddMember("range", field_node, allocator);
    return range_query;
}

RangeQueryBuilder::RangeQueryBuilder(ExtBinaryPredicate* range_predicate) {
    _range_predicate = range_predicate;
}

rapidjson::Value MatchAllQueryBuilder::to_json(rapidjson::Document& document) {
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value match_all_node(rapidjson::kObjectType);
    match_all_node.SetObject();
    rapidjson::Value match_all_query(rapidjson::kObjectType);
    match_all_query.SetObject();
    match_all_query.AddMember("match_all", match_all_node, allocator);
    return match_all_query;
}

BooleanQueryBuilder::BooleanQueryBuilder() {

}
BooleanQueryBuilder::~BooleanQueryBuilder() {
    for (auto clause : _must_clauses) {
        delete clause;
        clause = nullptr;
    }
    for (auto clause : _must_not_clauses) {
        delete clause;
        clause = nullptr;
    }
    for (auto clause : _filter_clauses) {
        delete clause;
        clause = nullptr;
    }
    for (auto clause : _should_clauses) {
        delete clause;
        clause = nullptr;
    }
}

BooleanQueryBuilder::BooleanQueryBuilder(const std::vector<ExtPredicate*>& predicates) {
    for (auto predicate : predicates) {
        switch (predicate->node_type) {
            case TExprNodeType::BINARY_PRED: {
                ExtBinaryPredicate* binary_predicate = (ExtBinaryPredicate*)predicate;
                switch (binary_predicate->op)
                {
                    case TExprOpcode::EQ: {
                        TermQueryBuilder* term_query = new TermQueryBuilder(binary_predicate);
                        _should_clauses.push_back(term_query);
                        break;
                        }
                    case TExprOpcode::NE:{ // process NE
                        TermQueryBuilder* term_query = new TermQueryBuilder(binary_predicate);
                        BooleanQueryBuilder* bool_query = new BooleanQueryBuilder();
                        bool_query->must_not(term_query);
                        _should_clauses.push_back(bool_query);
                        break;
                        }
                    case TExprOpcode::LT:
                    case TExprOpcode::LE:
                    case TExprOpcode::GT:
                    case TExprOpcode::GE: {
                        RangeQueryBuilder* range_query = new RangeQueryBuilder(binary_predicate);
                        _should_clauses.push_back(range_query);
                        break;
                        }
                    default:
                        break;
                }
                break;
            }
            case TExprNodeType::IN_PRED: {
                ExtInPredicate* in_predicate = (ExtInPredicate *)predicate;
                    bool is_not_in = in_predicate->is_not_in;
                    if (is_not_in) { // process not in predicate
                        TermsInSetQueryBuilder* terms_predicate = new TermsInSetQueryBuilder(in_predicate);
                        BooleanQueryBuilder* bool_query = new BooleanQueryBuilder();
                        bool_query->must_not(terms_predicate);
                        _should_clauses.push_back(bool_query);
                    } else { // process in predicate 
                        TermsInSetQueryBuilder* terms_query= new TermsInSetQueryBuilder(in_predicate);
                        _should_clauses.push_back(terms_query);
                    }
                    break;
            }
            case TExprNodeType::LIKE_PRED: {
                ExtLikePredicate* like_predicate = (ExtLikePredicate *)predicate;
                WildCardQueryBuilder* wild_card_query = new WildCardQueryBuilder(like_predicate);
                _should_clauses.push_back(wild_card_query);
                break;
            }
            case TExprNodeType::FUNCTION_CALL: {
                ExtFunction* function_predicate = (ExtFunction *)predicate;
                if ("esquery" == function_predicate->func_name ) {
                    ESQueryBuilder* es_query = new ESQueryBuilder(function_predicate);
                    _should_clauses.push_back(es_query);
                };
                break;
            }
            default:
                break;
        }
    }
}

rapidjson::Value BooleanQueryBuilder::to_json(rapidjson::Document& docuemnt) {
    rapidjson::Document::AllocatorType &allocator = docuemnt.GetAllocator();
    rapidjson::Value root_node_object(rapidjson::kObjectType);
    if (_filter_clauses.size() > 0) {
        rapidjson::Value filter_node(rapidjson::kArrayType);
        for (auto must_clause : _filter_clauses) {
            filter_node.PushBack(must_clause->to_json(docuemnt), allocator);
        }
        root_node_object.AddMember("filter", filter_node, allocator);
    }

    if (_should_clauses.size() > 0) {
        rapidjson::Value should_node(rapidjson::kArrayType);
        for (auto should_clause : _should_clauses) {
            should_node.PushBack(should_clause->to_json(docuemnt), allocator);
        }
        root_node_object.AddMember("should", should_node, allocator);
    }

    if (_must_not_clauses.size() > 0) {
        rapidjson::Value must_not_node(rapidjson::kArrayType);
        for (auto must_not_clause : _must_not_clauses) {
            must_not_node.PushBack(must_not_clause->to_json(docuemnt), allocator);
        }
        root_node_object.AddMember("must_not", must_not_node, allocator);
    }

    rapidjson::Value bool_query(rapidjson::kObjectType);
    bool_query.AddMember("bool", root_node_object, allocator);
    return bool_query;
}

void BooleanQueryBuilder::should(QueryBuilder* filter) {
    _should_clauses.push_back(filter);
}
void BooleanQueryBuilder::filter(QueryBuilder* filter) {
    _filter_clauses.push_back(filter);
}
void BooleanQueryBuilder::must(QueryBuilder* filter) {
    _filter_clauses.push_back(filter);
}
void BooleanQueryBuilder::must_not(QueryBuilder* filter) {
    _must_not_clauses.push_back(filter);
}

rapidjson::Value BooleanQueryBuilder::to_query(const std::vector<EsPredicate*>& predicates, rapidjson::Document& root) {
    if (predicates.size() == 0) {
        MatchAllQueryBuilder match_all_query;
        return match_all_query.to_json(root);
    }
    root.SetObject();
    BooleanQueryBuilder *bool_query = new BooleanQueryBuilder();
    for (auto es_predicate : predicates) {
        vector<ExtPredicate*> or_predicates = es_predicate->get_predicate_list();
        BooleanQueryBuilder* inner_bool_query = new BooleanQueryBuilder(or_predicates);
        bool_query->must(inner_bool_query);
    }
    rapidjson::Value root_value_node = bool_query->to_json(root);
    // root.AddMember("query", root_value_node, allocator);
    // rapidjson::StringBuffer buffer;
    // rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    // root.Accept(writer);
    // std::string es_query_dsl_json = buffer.GetString();
    return root_value_node;   
}
}
