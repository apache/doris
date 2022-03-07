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

#include "exec/es/es_query_builder.h"

#include <boost/algorithm/string/replace.hpp>

#include "common/logging.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace doris {

ESQueryBuilder::ESQueryBuilder(const std::string& es_query_str) : _es_query_str(es_query_str) {}
ESQueryBuilder::ESQueryBuilder(const ExtFunction& es_query) {
    auto first = es_query.values.front();
    _es_query_str = first.to_string();
}

// note: call this function must invoke BooleanQueryBuilder::check_es_query to check validation
void ESQueryBuilder::to_json(rapidjson::Document* document, rapidjson::Value* query) {
    rapidjson::Document scratch_document;
    scratch_document.Parse(_es_query_str.c_str(), _es_query_str.length());
    rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
    rapidjson::Value query_key;
    rapidjson::Value query_value;
    //{ "term": { "dv": "2" } }
    rapidjson::Value::ConstMemberIterator first = scratch_document.MemberBegin();
    // deep copy, reference http://rapidjson.org/md_doc_tutorial.html#DeepCopyValue
    query_key.CopyFrom(first->name, allocator);
    // if we found one key, then end loop as QueryDSL only support one `query` root
    query_value.CopyFrom(first->value, allocator);
    // Move Semantics, reference http://rapidjson.org/md_doc_tutorial.html#MoveSemantics
    query->AddMember(query_key, query_value, allocator);
}

TermQueryBuilder::TermQueryBuilder(const std::string& field, const std::string& term)
        : _field(field), _term(term), _match_none(false) {}

TermQueryBuilder::TermQueryBuilder(const ExtBinaryPredicate& binary_predicate)
        : _field(binary_predicate.col.name), _match_none(false) {
    if (binary_predicate.col.type.type == PrimitiveType::TYPE_BOOLEAN) {
        int val = atoi(binary_predicate.value.to_string().c_str());
        if (val == 1) {
            _term = std::string("true");
        } else if (val == 0) {
            _term = std::string("false");
        } else {
            // keep semantic consistent with mysql
            _match_none = true;
        }
    } else {
        _term = binary_predicate.value.to_string();
    }
}

void TermQueryBuilder::to_json(rapidjson::Document* document, rapidjson::Value* query) {
    rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
    rapidjson::Value term_node(rapidjson::kObjectType);
    term_node.SetObject();
    if (!_match_none) {
        rapidjson::Value field_value(_field.c_str(), allocator);
        rapidjson::Value term_value(_term.c_str(), allocator);
        term_node.AddMember(field_value, term_value, allocator);
        query->AddMember("term", term_node, allocator);
    } else {
        // this would only appear `bool` column's predicate (a = 2)
        query->AddMember("match_none", term_node, allocator);
    }
}

RangeQueryBuilder::RangeQueryBuilder(const ExtBinaryPredicate& range_predicate)
        : _field(range_predicate.col.name),
          _value(range_predicate.value.to_string()),
          _op(range_predicate.op) {}

void RangeQueryBuilder::to_json(rapidjson::Document* document, rapidjson::Value* query) {
    rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
    rapidjson::Value field_value(_field.c_str(), allocator);
    rapidjson::Value value(_value.c_str(), allocator);
    rapidjson::Value op_node(rapidjson::kObjectType);
    op_node.SetObject();
    switch (_op) {
    case TExprOpcode::LT:
        op_node.AddMember("lt", value, allocator);
        break;
    case TExprOpcode::LE:
        op_node.AddMember("lte", value, allocator);
        break;
    case TExprOpcode::GT:
        op_node.AddMember("gt", value, allocator);
        break;
    case TExprOpcode::GE:
        op_node.AddMember("gte", value, allocator);
        break;
    default:
        break;
    }
    rapidjson::Value field_node(rapidjson::kObjectType);
    field_node.SetObject();
    field_node.AddMember(field_value, op_node, allocator);
    query->AddMember("range", field_node, allocator);
}

void WildCardQueryBuilder::to_json(rapidjson::Document* document, rapidjson::Value* query) {
    rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
    rapidjson::Value term_node(rapidjson::kObjectType);
    term_node.SetObject();
    rapidjson::Value field_value(_field.c_str(), allocator);
    rapidjson::Value term_value(_like_value.c_str(), allocator);
    term_node.AddMember(field_value, term_value, allocator);
    query->AddMember("wildcard", term_node, allocator);
}
WildCardQueryBuilder::WildCardQueryBuilder(const ExtLikePredicate& like_predicate)
        : _field(like_predicate.col.name) {
    _like_value = like_predicate.value.to_string();
    // example of translation :
    //      abc_123  ===> abc?123
    //      abc%ykz  ===> abc*123
    //      %abc123  ===> *abc123
    //      _abc123  ===> ?abc123
    //      \\_abc1  ===> \\_abc1
    //      abc\\_123 ===> abc\\_123
    //      abc\\%123 ===> abc\\%123
    // NOTE. user must input sql like 'abc\\_123' or 'abc\\%ykz'
    for (int i = 0; i < _like_value.size(); i++) {
        if (_like_value[i] == '_' || _like_value[i] == '%') {
            if (i == 0) {
                _like_value[i] = (_like_value[i] == '_') ? '?' : '*';
            } else if (_like_value[i - 1] != '\\') {
                _like_value[i] = (_like_value[i] == '_') ? '?' : '*';
            }
        }
    }
}

void TermsInSetQueryBuilder::to_json(rapidjson::Document* document, rapidjson::Value* query) {
    rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
    rapidjson::Value terms_node(rapidjson::kObjectType);
    rapidjson::Value values_node(rapidjson::kArrayType);
    for (auto& value : _values) {
        rapidjson::Value value_value(value.c_str(), allocator);
        values_node.PushBack(value_value, allocator);
    }
    rapidjson::Value field_value(_field.c_str(), allocator);
    terms_node.AddMember(field_value, values_node, allocator);
    query->AddMember("terms", terms_node, allocator);
}

TermsInSetQueryBuilder::TermsInSetQueryBuilder(const ExtInPredicate& in_predicate)
        : _field(in_predicate.col.name) {
    for (auto& value : in_predicate.values) {
        _values.push_back(value.to_string());
    }
}

void MatchAllQueryBuilder::to_json(rapidjson::Document* document, rapidjson::Value* query) {
    rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
    rapidjson::Value match_all_node(rapidjson::kObjectType);
    match_all_node.SetObject();
    query->AddMember("match_all", match_all_node, allocator);
}

ExistsQueryBuilder::ExistsQueryBuilder(const ExtIsNullPredicate& is_null_predicate)
        : _field(is_null_predicate.col.name) {}

void ExistsQueryBuilder::to_json(rapidjson::Document* document, rapidjson::Value* query) {
    rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
    rapidjson::Value term_node(rapidjson::kObjectType);
    term_node.SetObject();
    rapidjson::Value field_name(_field.c_str(), allocator);
    term_node.AddMember("field", field_name, allocator);
    query->AddMember("exists", term_node, allocator);
}

BooleanQueryBuilder::BooleanQueryBuilder() {}
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
            switch (binary_predicate->op) {
            case TExprOpcode::EQ: {
                TermQueryBuilder* term_query = new TermQueryBuilder(*binary_predicate);
                _should_clauses.push_back(term_query);
                break;
            }
            case TExprOpcode::NE: { // process NE
                TermQueryBuilder* term_query = new TermQueryBuilder(*binary_predicate);
                BooleanQueryBuilder* bool_query = new BooleanQueryBuilder();
                bool_query->must_not(term_query);
                _should_clauses.push_back(bool_query);
                break;
            }
            case TExprOpcode::LT:
            case TExprOpcode::LE:
            case TExprOpcode::GT:
            case TExprOpcode::GE: {
                RangeQueryBuilder* range_query = new RangeQueryBuilder(*binary_predicate);
                _should_clauses.push_back(range_query);
                break;
            }
            default:
                break;
            }
            break;
        }
        case TExprNodeType::IN_PRED: {
            ExtInPredicate* in_predicate = (ExtInPredicate*)predicate;
            bool is_not_in = in_predicate->is_not_in;
            if (is_not_in) { // process not in predicate
                TermsInSetQueryBuilder* terms_predicate = new TermsInSetQueryBuilder(*in_predicate);
                BooleanQueryBuilder* bool_query = new BooleanQueryBuilder();
                bool_query->must_not(terms_predicate);
                _should_clauses.push_back(bool_query);
            } else { // process in predicate
                TermsInSetQueryBuilder* terms_query = new TermsInSetQueryBuilder(*in_predicate);
                _should_clauses.push_back(terms_query);
            }
            break;
        }
        case TExprNodeType::LIKE_PRED: {
            ExtLikePredicate* like_predicate = (ExtLikePredicate*)predicate;
            WildCardQueryBuilder* wild_card_query = new WildCardQueryBuilder(*like_predicate);
            _should_clauses.push_back(wild_card_query);
            break;
        }
        case TExprNodeType::IS_NULL_PRED: {
            ExtIsNullPredicate* is_null_predicate = (ExtIsNullPredicate*)predicate;
            ExistsQueryBuilder* exists_query = new ExistsQueryBuilder(*is_null_predicate);
            if (is_null_predicate->is_not_null) {
                _should_clauses.push_back(exists_query);
            } else {
                BooleanQueryBuilder* bool_query = new BooleanQueryBuilder();
                bool_query->must_not(exists_query);
                _should_clauses.push_back(bool_query);
            }
            break;
        }
        case TExprNodeType::FUNCTION_CALL: {
            ExtFunction* function_predicate = (ExtFunction*)predicate;
            if ("esquery" == function_predicate->func_name) {
                ESQueryBuilder* es_query = new ESQueryBuilder(*function_predicate);
                _should_clauses.push_back(es_query);
            };
            break;
        }
        case TExprNodeType::COMPOUND_PRED: {
            ExtCompPredicates* compound_predicates = (ExtCompPredicates*)predicate;
            // reserved for compound_not
            if (compound_predicates->op == TExprOpcode::COMPOUND_AND) {
                BooleanQueryBuilder* bool_query = new BooleanQueryBuilder();
                for (auto es_predicate : compound_predicates->conjuncts) {
                    std::vector<ExtPredicate*> or_predicates = es_predicate->get_predicate_list();
                    BooleanQueryBuilder* inner_bool_query = new BooleanQueryBuilder(or_predicates);
                    bool_query->must(inner_bool_query);
                }
                _should_clauses.push_back(bool_query);
            }
            break;
        }
        default:
            break;
        }
    }
}

void BooleanQueryBuilder::to_json(rapidjson::Document* document, rapidjson::Value* query) {
    rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
    rapidjson::Value root_node_object(rapidjson::kObjectType);
    if (_filter_clauses.size() > 0) {
        rapidjson::Value filter_node(rapidjson::kArrayType);
        for (auto must_clause : _filter_clauses) {
            rapidjson::Value must_clause_query(rapidjson::kObjectType);
            must_clause_query.SetObject();
            must_clause->to_json(document, &must_clause_query);
            filter_node.PushBack(must_clause_query, allocator);
        }
        root_node_object.AddMember("filter", filter_node, allocator);
    }

    if (_should_clauses.size() > 0) {
        rapidjson::Value should_node(rapidjson::kArrayType);
        for (auto should_clause : _should_clauses) {
            rapidjson::Value should_clause_query(rapidjson::kObjectType);
            should_clause_query.SetObject();
            should_clause->to_json(document, &should_clause_query);
            should_node.PushBack(should_clause_query, allocator);
        }
        root_node_object.AddMember("should", should_node, allocator);
    }

    if (_must_not_clauses.size() > 0) {
        rapidjson::Value must_not_node(rapidjson::kArrayType);
        for (auto must_not_clause : _must_not_clauses) {
            rapidjson::Value must_not_clause_query(rapidjson::kObjectType);
            must_not_clause_query.SetObject();
            must_not_clause->to_json(document, &must_not_clause_query);
            must_not_node.PushBack(must_not_clause_query, allocator);
        }
        root_node_object.AddMember("must_not", must_not_node, allocator);
    }
    query->AddMember("bool", root_node_object, allocator);
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

Status BooleanQueryBuilder::check_es_query(const ExtFunction& extFunction) {
    const std::string& esquery_str = extFunction.values.front().to_string();
    rapidjson::Document scratch_document;
    scratch_document.Parse(esquery_str.c_str(), esquery_str.length());
    rapidjson::Document::AllocatorType& allocator = scratch_document.GetAllocator();
    rapidjson::Value query_key;
    // { "term": { "dv": "2" } }
    if (!scratch_document.HasParseError()) {
        if (!scratch_document.IsObject()) {
            return Status::InvalidArgument("esquery must be a object");
        }
        rapidjson::SizeType object_count = scratch_document.MemberCount();
        if (object_count != 1) {
            return Status::InvalidArgument("esquery must only one root");
        }
        // deep copy, reference http://rapidjson.org/md_doc_tutorial.html#DeepCopyValue
        rapidjson::Value::ConstMemberIterator first = scratch_document.MemberBegin();
        query_key.CopyFrom(first->name, allocator);
        if (!query_key.IsString()) {
            // if we found one key, then end loop as QueryDSL only support one `query` root
            return Status::InvalidArgument("esquery root key must be string");
        }
    } else {
        return Status::InvalidArgument("malformed esquery json");
    }
    return Status::OK();
}

void BooleanQueryBuilder::validate(const std::vector<EsPredicate*>& espredicates,
                                   std::vector<bool>* result) {
    for (auto espredicate : espredicates) {
        result->push_back(validate(espredicate));
    }
}

bool BooleanQueryBuilder::validate(const EsPredicate* espredicate) {
    for (auto predicate : espredicate->get_predicate_list()) {
        switch (predicate->node_type) {
        case TExprNodeType::BINARY_PRED: {
            ExtBinaryPredicate* binary_predicate = (ExtBinaryPredicate*)predicate;
            TExprOpcode::type op = binary_predicate->op;
            if (op != TExprOpcode::EQ && op != TExprOpcode::NE && op != TExprOpcode::LT &&
                op != TExprOpcode::LE && op != TExprOpcode::GT && op != TExprOpcode::GE) {
                return false;
            }
            break;
        }
        case TExprNodeType::COMPOUND_PRED: {
            ExtCompPredicates* compound_predicates = (ExtCompPredicates*)predicate;
            if (compound_predicates->op != TExprOpcode::COMPOUND_AND) {
                // reserved for compound_not
                return false;
            }
            std::vector<bool> list;
            validate(compound_predicates->conjuncts, &list);
            for (int i = list.size() - 1; i >= 0; i--) {
                if (!list[i]) {
                    return false;
                }
            }
            break;
        }
        case TExprNodeType::LIKE_PRED:
        case TExprNodeType::IS_NULL_PRED:
        case TExprNodeType::IN_PRED: {
            break;
        }
        case TExprNodeType::FUNCTION_CALL: {
            ExtFunction* function_predicate = (ExtFunction*)predicate;
            if ("esquery" != function_predicate->func_name) {
                return false;
            }
            Status st = check_es_query(*function_predicate);
            if (!st.ok()) {
                return false;
            }
            break;
        }
        default: {
            return false;
            break;
        }
        }
    }

    return true;
}

void BooleanQueryBuilder::to_query(const std::vector<EsPredicate*>& predicates,
                                   rapidjson::Document* root, rapidjson::Value* query) {
    if (predicates.size() == 0) {
        MatchAllQueryBuilder match_all_query;
        match_all_query.to_json(root, query);
        return;
    }
    root->SetObject();
    BooleanQueryBuilder bool_query;
    for (auto es_predicate : predicates) {
        std::vector<ExtPredicate*> or_predicates = es_predicate->get_predicate_list();
        BooleanQueryBuilder* inner_bool_query = new BooleanQueryBuilder(or_predicates);
        bool_query.must(inner_bool_query);
    }
    bool_query.to_json(root, query);
}
} // namespace doris
