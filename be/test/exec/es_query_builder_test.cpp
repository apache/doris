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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/logging.h"
#include "exec/es/es_predicate.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/string_value.h"
#include "util/debug/leakcheck_disabler.h"

namespace doris {

class BooleanQueryBuilderTest : public testing::Test {
public:
    BooleanQueryBuilderTest() {}
    virtual ~BooleanQueryBuilderTest() {}
};

TEST_F(BooleanQueryBuilderTest, term_query) {
    // content = "wyf"
    char str[] = "wyf";
    StringValue value(str, 3);
    ExtLiteral term_literal(TYPE_VARCHAR, &value);
    TypeDescriptor type_desc = TypeDescriptor::create_varchar_type(3);
    std::string name = "content";
    ExtBinaryPredicate term_predicate(TExprNodeType::BINARY_PRED, name, type_desc, TExprOpcode::EQ,
                                      term_literal);
    TermQueryBuilder term_query(term_predicate);
    rapidjson::Document document;
    rapidjson::Value term_value(rapidjson::kObjectType);
    term_value.SetObject();
    term_query.to_json(&document, &term_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    term_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "term query" << actual_json;
    EXPECT_STREQ("{\"term\":{\"content\":\"wyf\"}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, range_query) {
    // k >= a
    char str[] = "a";
    StringValue value(str, 1);
    ExtLiteral term_literal(TYPE_VARCHAR, &value);
    TypeDescriptor type_desc = TypeDescriptor::create_varchar_type(1);
    std::string name = "k";
    ExtBinaryPredicate range_predicate(TExprNodeType::BINARY_PRED, name, type_desc, TExprOpcode::GE,
                                       term_literal);
    RangeQueryBuilder range_query(range_predicate);
    rapidjson::Document document;
    rapidjson::Value range_value(rapidjson::kObjectType);
    range_value.SetObject();
    range_query.to_json(&document, &range_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    range_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "range query" << actual_json;
    EXPECT_STREQ("{\"range\":{\"k\":{\"gte\":\"a\"}}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, es_query) {
    // esquery('random', "{\"bool\": {\"must_not\": {\"exists\": {\"field\": \"f1\"}}}}")
    char str[] = "{\"bool\": {\"must_not\": {\"exists\": {\"field\": \"f1\"}}}}";
    int length = (int)strlen(str);
    TypeDescriptor type_desc = TypeDescriptor::create_varchar_type(length);
    std::string name = "random";
    ExtColumnDesc col_des(name, type_desc);
    std::vector<ExtColumnDesc> cols = {col_des};
    StringValue value(str, length);
    ExtLiteral term_literal(TYPE_VARCHAR, &value);
    std::vector<ExtLiteral> values = {term_literal};
    std::string function_name = "esquery";
    ExtFunction function_predicate(TExprNodeType::FUNCTION_CALL, function_name, cols, values);
    ESQueryBuilder es_query(function_predicate);
    rapidjson::Document document;
    rapidjson::Value es_query_value(rapidjson::kObjectType);
    es_query_value.SetObject();
    es_query.to_json(&document, &es_query_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    es_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "es query" << actual_json;
    EXPECT_STREQ("{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"f1\"}}}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, like_query) {
    // content like 'a%e%g_'
    char str[] = "a%e%g_";
    int length = (int)strlen(str);
    LOG(INFO) << "length " << length;
    TypeDescriptor type_desc = TypeDescriptor::create_varchar_type(length);
    StringValue value(str, length);
    ExtLiteral like_literal(TYPE_VARCHAR, &value);
    std::string name = "content";
    ExtLikePredicate like_predicate(TExprNodeType::LIKE_PRED, name, type_desc, like_literal);
    WildCardQueryBuilder like_query(like_predicate);
    rapidjson::Document document;
    rapidjson::Value like_query_value(rapidjson::kObjectType);
    like_query_value.SetObject();
    like_query.to_json(&document, &like_query_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    like_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    // LOG(INFO) << "wildcard query" << actual_json;
    EXPECT_STREQ("{\"wildcard\":{\"content\":\"a*e*g?\"}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, terms_in_query) {
    // dv in ["2.0", "4.0", "8.0"]
    std::string terms_in_field = "dv";
    int terms_in_field_length = terms_in_field.length();
    TypeDescriptor terms_in_col_type_desc =
            TypeDescriptor::create_varchar_type(terms_in_field_length);

    char value_1[] = "2.0";
    int value_1_length = (int)strlen(value_1);
    StringValue string_value_1(value_1, value_1_length);
    ExtLiteral term_literal_1(TYPE_VARCHAR, &string_value_1);

    char value_2[] = "4.0";
    int value_2_length = (int)strlen(value_2);
    StringValue string_value_2(value_2, value_2_length);
    ExtLiteral term_literal_2(TYPE_VARCHAR, &string_value_2);

    char value_3[] = "8.0";
    int value_3_length = (int)strlen(value_3);
    StringValue string_value_3(value_3, value_3_length);
    ExtLiteral term_literal_3(TYPE_VARCHAR, &string_value_3);

    std::vector<ExtLiteral> terms_values = {term_literal_1, term_literal_2, term_literal_3};
    ExtInPredicate in_predicate(TExprNodeType::IN_PRED, false, terms_in_field,
                                terms_in_col_type_desc, terms_values);
    TermsInSetQueryBuilder terms_query(in_predicate);
    rapidjson::Document document;
    rapidjson::Value in_query_value(rapidjson::kObjectType);
    in_query_value.SetObject();
    terms_query.to_json(&document, &in_query_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    in_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "terms in sets query" << actual_json;
    EXPECT_STREQ("{\"terms\":{\"dv\":[\"2.0\",\"4.0\",\"8.0\"]}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, match_all_query) {
    // match all docs
    MatchAllQueryBuilder match_all_query;
    rapidjson::Document document;
    rapidjson::Value match_all_query_value(rapidjson::kObjectType);
    match_all_query_value.SetObject();
    match_all_query.to_json(&document, &match_all_query_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    match_all_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "match all query" << actual_json;
    EXPECT_STREQ("{\"match_all\":{}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, exists_query) {
    // k1 is not null
    // {"exists":{"field":"k1"}}
    std::string exists_field = "k1";
    int exists_field_length = exists_field.length();
    TypeDescriptor exists_col_type_desc = TypeDescriptor::create_varchar_type(exists_field_length);
    ExtIsNullPredicate isNullPredicate(TExprNodeType::IS_NULL_PRED, "k1", exists_col_type_desc,
                                       true);
    ExistsQueryBuilder exists_query(isNullPredicate);
    rapidjson::Document document;
    rapidjson::Value exists_query_value(rapidjson::kObjectType);
    exists_query_value.SetObject();
    exists_query.to_json(&document, &exists_query_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    exists_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    EXPECT_STREQ("{\"exists\":{\"field\":\"k1\"}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, bool_query) {
    // content like 'a%e%g_'
    char like_value[] = "a%e%g_";
    int like_value_length = (int)strlen(like_value);
    TypeDescriptor like_type_desc = TypeDescriptor::create_varchar_type(like_value_length);
    StringValue like_term_value(like_value, like_value_length);
    ExtLiteral like_literal(TYPE_VARCHAR, &like_term_value);
    std::string like_field_name = "content";
    ExtLikePredicate* like_predicate = new ExtLikePredicate(
            TExprNodeType::LIKE_PRED, like_field_name, like_type_desc, like_literal);
    // esquery("random", "{\"bool\": {\"must_not\": {\"exists\": {\"field\": \"f1\"}}}}")
    char es_query_str[] = "{\"bool\": {\"must_not\": {\"exists\": {\"field\": \"f1\"}}}}";
    int es_query_length = (int)strlen(es_query_str);
    StringValue value(es_query_str, es_query_length);
    TypeDescriptor es_query_type_desc = TypeDescriptor::create_varchar_type(es_query_length);
    std::string es_query_field_name = "random";
    ExtColumnDesc es_query_col_des(es_query_field_name, es_query_type_desc);
    std::vector<ExtColumnDesc> es_query_cols = {es_query_col_des};
    StringValue es_query_value(es_query_str, es_query_length);
    ExtLiteral es_query_term_literal(TYPE_VARCHAR, &es_query_value);
    std::vector<ExtLiteral> es_query_values = {es_query_term_literal};
    std::string function_name = "esquery";
    ExtFunction* function_predicate = new ExtFunction(TExprNodeType::FUNCTION_CALL, function_name,
                                                      es_query_cols, es_query_values);
    // k >= a
    char range_value_str[] = "a";
    int range_value_length = (int)strlen(range_value_str);
    StringValue range_value(range_value_str, range_value_length);
    ExtLiteral range_literal(TYPE_VARCHAR, &range_value);
    TypeDescriptor range_type_desc = TypeDescriptor::create_varchar_type(range_value_length);
    std::string range_field_name = "k";
    ExtBinaryPredicate* range_predicate =
            new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, range_field_name, range_type_desc,
                                   TExprOpcode::GE, range_literal);
    // content = "wyf"
    char term_str[] = "wyf";
    int term_value_length = (int)strlen(term_str);
    StringValue term_value(term_str, term_value_length);
    ExtLiteral term_literal(TYPE_VARCHAR, &term_value);
    TypeDescriptor term_type_desc = TypeDescriptor::create_varchar_type(term_value_length);
    std::string term_field_name = "content";
    ExtBinaryPredicate* term_predicate =
            new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, term_field_name, term_type_desc,
                                   TExprOpcode::EQ, term_literal);

    // content like 'a%e%g_' or k >= a or content = "wyf"
    std::vector<ExtPredicate*> or_predicates = {like_predicate, function_predicate, range_predicate,
                                                term_predicate};
    BooleanQueryBuilder bool_query(or_predicates);
    rapidjson::Document document;
    rapidjson::Value bool_query_value(rapidjson::kObjectType);
    bool_query_value.SetObject();
    bool_query.to_json(&document, &bool_query_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    bool_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    std::string expected_json =
            "{\"bool\":{\"should\":[{\"wildcard\":{\"content\":\"a*e*g?\"}},{\"bool\":{\"must_"
            "not\":{\"exists\":{\"field\":\"f1\"}}}},{\"range\":{\"k\":{\"gte\":\"a\"}}},{\"term\":"
            "{\"content\":\"wyf\"}}]}}";
    //LOG(INFO) << "bool query" << actual_json;
    EXPECT_STREQ(expected_json.c_str(), actual_json.c_str());

    delete like_predicate;
    delete function_predicate;
    delete range_predicate;
    delete term_predicate;
}

TEST_F(BooleanQueryBuilderTest, compound_bool_query) {
    //  content like "a%e%g_" or esquery(random, '{"bool": {"must_not": {"exists": {"field": "f1"}}}}')
    char like_value[] = "a%e%g_";
    int like_value_length = (int)strlen(like_value);
    TypeDescriptor like_type_desc = TypeDescriptor::create_varchar_type(like_value_length);
    StringValue like_term_value(like_value, like_value_length);
    ExtLiteral like_literal(TYPE_VARCHAR, &like_term_value);
    std::string like_field_name = "content";
    ExtLikePredicate* like_predicate = new ExtLikePredicate(
            TExprNodeType::LIKE_PRED, like_field_name, like_type_desc, like_literal);

    char es_query_str[] = "{\"bool\": {\"must_not\": {\"exists\": {\"field\": \"f1\"}}}}";
    int es_query_length = (int)strlen(es_query_str);
    StringValue value(es_query_str, es_query_length);
    TypeDescriptor es_query_type_desc = TypeDescriptor::create_varchar_type(es_query_length);
    std::string es_query_field_name = "random";
    ExtColumnDesc es_query_col_des(es_query_field_name, es_query_type_desc);
    std::vector<ExtColumnDesc> es_query_cols = {es_query_col_des};
    StringValue es_query_value(es_query_str, es_query_length);
    ExtLiteral es_query_term_literal(TYPE_VARCHAR, &es_query_value);
    std::vector<ExtLiteral> es_query_values = {es_query_term_literal};
    std::string function_name = "esquery";
    ExtFunction* function_predicate = new ExtFunction(TExprNodeType::FUNCTION_CALL, function_name,
                                                      es_query_cols, es_query_values);

    std::vector<ExtPredicate*> bool_predicates_1 = {like_predicate, function_predicate};
    EsPredicate* bool_predicate_1 = new EsPredicate(bool_predicates_1);

    // k >= "a"
    char range_value_str[] = "a";
    int range_value_length = (int)strlen(range_value_str);
    StringValue range_value(range_value_str, range_value_length);
    ExtLiteral range_literal(TYPE_VARCHAR, &range_value);
    TypeDescriptor range_type_desc = TypeDescriptor::create_varchar_type(range_value_length);
    std::string range_field_name = "k";
    ExtBinaryPredicate* range_predicate =
            new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, range_field_name, range_type_desc,
                                   TExprOpcode::GE, range_literal);

    std::vector<ExtPredicate*> bool_predicates_2 = {range_predicate};
    EsPredicate* bool_predicate_2 = new EsPredicate(bool_predicates_2);

    // content != "wyf"
    char term_str[] = "wyf";
    int term_value_length = (int)strlen(term_str);
    StringValue term_value(term_str, term_value_length);
    ExtLiteral term_literal(TYPE_VARCHAR, &term_value);
    TypeDescriptor term_type_desc = TypeDescriptor::create_varchar_type(term_value_length);
    std::string term_field_name = "content";
    ExtBinaryPredicate* term_ne_predicate =
            new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, term_field_name, term_type_desc,
                                   TExprOpcode::NE, term_literal);
    std::vector<ExtPredicate*> bool_predicates_3 = {term_ne_predicate};
    EsPredicate* bool_predicate_3 = new EsPredicate(bool_predicates_3);

    // fv not in [8.0, 16.0]
    std::string terms_in_field = "fv";
    int terms_in_field_length = terms_in_field.length();
    TypeDescriptor terms_in_col_type_desc =
            TypeDescriptor::create_varchar_type(terms_in_field_length);

    char value_1[] = "8.0";
    int value_1_length = (int)strlen(value_1);
    StringValue string_value_1(value_1, value_1_length);
    ExtLiteral term_literal_1(TYPE_VARCHAR, &string_value_1);

    char value_2[] = "16.0";
    int value_2_length = (int)strlen(value_2);
    StringValue string_value_2(value_2, value_2_length);
    ExtLiteral term_literal_2(TYPE_VARCHAR, &string_value_2);

    std::vector<ExtLiteral> terms_values = {term_literal_1, term_literal_2};
    ExtInPredicate* in_predicate = new ExtInPredicate(TExprNodeType::IN_PRED, true, terms_in_field,
                                                      terms_in_col_type_desc, terms_values);
    std::vector<ExtPredicate*> bool_predicates_4 = {in_predicate};
    EsPredicate* bool_predicate_4 = new EsPredicate(bool_predicates_4);

    // (content like "a%e%g_" or esquery(random, '{"bool": {"must_not": {"exists": {"field": "f1"}}}}')) and content != "wyf" and fv not in [8.0, 16.0]
    std::vector<EsPredicate*> and_bool_predicates = {bool_predicate_1, bool_predicate_2,
                                                     bool_predicate_3, bool_predicate_4};

    rapidjson::Document document;
    rapidjson::Value compound_bool_value(rapidjson::kObjectType);
    compound_bool_value.SetObject();
    BooleanQueryBuilder::to_query(and_bool_predicates, &document, &compound_bool_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    compound_bool_value.Accept(writer);
    std::string actual_bool_json = buffer.GetString();
    std::string expected_json =
            "{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"wildcard\":{\"content\":\"a*e*g?\"}},"
            "{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"f1\"}}}}]}},{\"bool\":{\"should\":[{"
            "\"range\":{\"k\":{\"gte\":\"a\"}}}]}},{\"bool\":{\"should\":[{\"bool\":{\"must_not\":["
            "{\"term\":{\"content\":\"wyf\"}}]}}]}},{\"bool\":{\"should\":[{\"bool\":{\"must_not\":"
            "[{\"terms\":{\"fv\":[\"8.0\",\"16.0\"]}}]}}]}}]}}";
    //LOG(INFO) << "compound bool query" << actual_bool_json;
    EXPECT_STREQ(expected_json.c_str(), actual_bool_json.c_str());
    delete bool_predicate_1;
    delete bool_predicate_2;
    delete bool_predicate_3;
    delete bool_predicate_4;
}

TEST_F(BooleanQueryBuilderTest, validate_esquery) {
    std::string function_name = "esquery";
    char field[] = "random";
    int field_length = (int)strlen(field);
    TypeDescriptor es_query_type_desc = TypeDescriptor::create_varchar_type(field_length);
    ExtColumnDesc es_query_col_des(field, es_query_type_desc);
    std::vector<ExtColumnDesc> es_query_cols = {es_query_col_des};
    char es_query_str[] = "{\"bool\": {\"must_not\": {\"exists\": {\"field\": \"f1\"}}}}";
    int es_query_length = (int)strlen(es_query_str);
    StringValue es_query_value(es_query_str, es_query_length);
    ExtLiteral es_query_term_literal(TYPE_VARCHAR, &es_query_value);
    std::vector<ExtLiteral> es_query_values = {es_query_term_literal};
    ExtFunction legal_es_query(TExprNodeType::FUNCTION_CALL, function_name, es_query_cols,
                               es_query_values);
    auto st = BooleanQueryBuilder::check_es_query(legal_es_query);
    EXPECT_TRUE(st.ok());
    char empty_query[] = "{}";
    int empty_query_length = (int)strlen(empty_query);
    StringValue empty_query_value(empty_query, empty_query_length);
    ExtLiteral empty_query_term_literal(TYPE_VARCHAR, &empty_query_value);
    std::vector<ExtLiteral> empty_query_values = {empty_query_term_literal};
    ExtFunction empty_es_query(TExprNodeType::FUNCTION_CALL, function_name, es_query_cols,
                               empty_query_values);
    st = BooleanQueryBuilder::check_es_query(empty_es_query);
    EXPECT_STREQ(st.get_error_msg().c_str(), "esquery must only one root");
    //LOG(INFO) <<"error msg:" << st1.get_error_msg();
    char malformed_query[] = "{\"bool\": {\"must_not\": {\"exists\": {";
    int malformed_query_length = (int)strlen(malformed_query);
    StringValue malformed_query_value(malformed_query, malformed_query_length);
    ExtLiteral malformed_query_term_literal(TYPE_VARCHAR, &malformed_query_value);
    std::vector<ExtLiteral> malformed_query_values = {malformed_query_term_literal};
    ExtFunction malformed_es_query(TExprNodeType::FUNCTION_CALL, function_name, es_query_cols,
                                   malformed_query_values);
    st = BooleanQueryBuilder::check_es_query(malformed_es_query);
    EXPECT_STREQ(st.get_error_msg().c_str(), "malformed esquery json");
    char illegal_query[] = "{\"term\": {\"k1\" : \"2\"},\"match\": {\"k1\": \"3\"}}";
    int illegal_query_length = (int)strlen(illegal_query);
    StringValue illegal_query_value(illegal_query, illegal_query_length);
    ExtLiteral illegal_query_term_literal(TYPE_VARCHAR, &illegal_query_value);
    std::vector<ExtLiteral> illegal_query_values = {illegal_query_term_literal};
    ExtFunction illegal_es_query(TExprNodeType::FUNCTION_CALL, function_name, es_query_cols,
                                 illegal_query_values);
    st = BooleanQueryBuilder::check_es_query(illegal_es_query);
    EXPECT_STREQ(st.get_error_msg().c_str(), "esquery must only one root");
    char illegal_key_query[] = "[\"22\"]";
    int illegal_key_query_length = (int)strlen(illegal_key_query);
    StringValue illegal_key_query_value(illegal_key_query, illegal_key_query_length);
    ExtLiteral illegal_key_query_term_literal(TYPE_VARCHAR, &illegal_key_query_value);
    std::vector<ExtLiteral> illegal_key_query_values = {illegal_key_query_term_literal};
    ExtFunction illegal_key_es_query(TExprNodeType::FUNCTION_CALL, function_name, es_query_cols,
                                     illegal_key_query_values);
    st = BooleanQueryBuilder::check_es_query(illegal_key_es_query);
    EXPECT_STREQ(st.get_error_msg().c_str(), "esquery must be a object");
}

TEST_F(BooleanQueryBuilderTest, validate_partial) {
    // TODO(yingchun): LSAN will report some errors in this scope, we should improve the code and enable LSAN later.
    debug::ScopedLeakCheckDisabler disable_lsan;
    char like_value[] = "a%e%g_";
    int like_value_length = (int)strlen(like_value);
    TypeDescriptor like_type_desc = TypeDescriptor::create_varchar_type(like_value_length);
    StringValue like_term_value(like_value, like_value_length);
    ExtLiteral like_literal(TYPE_VARCHAR, &like_term_value);
    std::string like_field_name = "content";
    ExtLikePredicate* like_predicate = new ExtLikePredicate(
            TExprNodeType::LIKE_PRED, like_field_name, like_type_desc, like_literal);

    // k >= "a"
    char range_value_str[] = "a";
    int range_value_length = (int)strlen(range_value_str);
    StringValue range_value(range_value_str, range_value_length);
    ExtLiteral range_literal(TYPE_VARCHAR, &range_value);
    TypeDescriptor range_type_desc = TypeDescriptor::create_varchar_type(range_value_length);
    std::string range_field_name = "k";
    ExtBinaryPredicate* range_predicate =
            new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, range_field_name, range_type_desc,
                                   TExprOpcode::GE, range_literal);

    std::vector<ExtPredicate*> bool_predicates_1 = {like_predicate, range_predicate};
    EsPredicate* bool_predicate_1 = new EsPredicate(bool_predicates_1);

    // fv not in [8.0, 16.0]
    std::string terms_in_field = "fv";
    int terms_in_field_length = terms_in_field.length();
    TypeDescriptor terms_in_col_type_desc =
            TypeDescriptor::create_varchar_type(terms_in_field_length);

    char value_1[] = "8.0";
    int value_1_length = (int)strlen(value_1);
    StringValue string_value_1(value_1, value_1_length);
    ExtLiteral term_literal_1(TYPE_VARCHAR, &string_value_1);

    char value_2[] = "16.0";
    int value_2_length = (int)strlen(value_2);
    StringValue string_value_2(value_2, value_2_length);
    ExtLiteral term_literal_2(TYPE_VARCHAR, &string_value_2);

    std::vector<ExtLiteral> terms_values = {term_literal_1, term_literal_2};
    ExtInPredicate* in_predicate = new ExtInPredicate(TExprNodeType::IN_PRED, true, terms_in_field,
                                                      terms_in_col_type_desc, terms_values);
    std::vector<ExtPredicate*> bool_predicates_2 = {in_predicate};
    EsPredicate* bool_predicate_2 = new EsPredicate(bool_predicates_2);

    // content != "wyf"
    char term_str[] = "wyf";
    int term_value_length = (int)strlen(term_str);
    StringValue term_value(term_str, term_value_length);
    ExtLiteral term_literal(TYPE_VARCHAR, &term_value);
    TypeDescriptor term_type_desc = TypeDescriptor::create_varchar_type(term_value_length);
    std::string term_field_name = "content";
    ExtBinaryPredicate* term_ne_predicate =
            new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, term_field_name, term_type_desc,
                                   TExprOpcode::NE, term_literal);

    char es_query_str[] = "{\"bool\": {\"must_not\": {\"exists\": {\"field\": \"f1\"}}}}";
    int es_query_length = (int)strlen(es_query_str);
    StringValue value(es_query_str, es_query_length);
    TypeDescriptor es_query_type_desc = TypeDescriptor::create_varchar_type(es_query_length);
    std::string es_query_field_name = "random";
    ExtColumnDesc es_query_col_des(es_query_field_name, es_query_type_desc);
    std::vector<ExtColumnDesc> es_query_cols = {es_query_col_des};
    StringValue es_query_value(es_query_str, es_query_length);
    ExtLiteral es_query_term_literal(TYPE_VARCHAR, &es_query_value);
    std::vector<ExtLiteral> es_query_values = {es_query_term_literal};
    std::string function_name = "esquery";
    ExtFunction* function_predicate = new ExtFunction(TExprNodeType::FUNCTION_CALL, function_name,
                                                      es_query_cols, es_query_values);
    std::vector<ExtPredicate*> bool_predicates_3 = {term_ne_predicate, function_predicate};
    EsPredicate* bool_predicate_3 = new EsPredicate(bool_predicates_3);

    std::vector<EsPredicate*> and_bool_predicates = {bool_predicate_1, bool_predicate_2,
                                                     bool_predicate_3};
    std::vector<bool> result;
    BooleanQueryBuilder::validate(and_bool_predicates, &result);
    std::vector<bool> expected = {true, true, true};
    EXPECT_EQ(result, expected);
    char illegal_query[] = "{\"term\": {\"k1\" : \"2\"},\"match\": {\"k1\": \"3\"}}";
    int illegal_query_length = (int)strlen(illegal_query);
    StringValue illegal_query_value(illegal_query, illegal_query_length);
    ExtLiteral illegal_query_term_literal(TYPE_VARCHAR, &illegal_query_value);
    std::vector<ExtLiteral> illegal_query_values = {illegal_query_term_literal};
    ExtFunction* illegal_function_preficate = new ExtFunction(
            TExprNodeType::FUNCTION_CALL, function_name, es_query_cols, illegal_query_values);
    std::vector<ExtPredicate*> illegal_bool_predicates_3 = {term_ne_predicate,
                                                            illegal_function_preficate};
    EsPredicate* illegal_bool_predicate_3 = new EsPredicate(illegal_bool_predicates_3);
    std::vector<EsPredicate*> and_bool_predicates_1 = {bool_predicate_1, bool_predicate_2,
                                                       illegal_bool_predicate_3};
    std::vector<bool> result1;
    BooleanQueryBuilder::validate(and_bool_predicates_1, &result1);
    std::vector<bool> expected1 = {true, true, false};
    EXPECT_EQ(result1, expected1);
}

// ( k >= "a" and (fv not in [8.0, 16.0]) or (content != "wyf") ) or content like "a%e%g_"

TEST_F(BooleanQueryBuilderTest, validate_compound_and) {
    // TODO(yingchun): LSAN will report some errors in this scope, we should improve the code and enable LSAN later.
    debug::ScopedLeakCheckDisabler disable_lsan;
    std::string terms_in_field = "fv"; // fv not in [8.0, 16.0]
    int terms_in_field_length = terms_in_field.length();
    TypeDescriptor terms_in_col_type_desc =
            TypeDescriptor::create_varchar_type(terms_in_field_length);

    char value_1[] = "8.0";
    int value_1_length = (int)strlen(value_1);
    StringValue string_value_1(value_1, value_1_length);
    ExtLiteral term_literal_1(TYPE_VARCHAR, &string_value_1);

    char value_2[] = "16.0";
    int value_2_length = (int)strlen(value_2);
    StringValue string_value_2(value_2, value_2_length);
    ExtLiteral term_literal_2(TYPE_VARCHAR, &string_value_2);

    std::vector<ExtLiteral> terms_values = {term_literal_1, term_literal_2};
    ExtInPredicate* in_predicate = new ExtInPredicate(TExprNodeType::IN_PRED, true, terms_in_field,
                                                      terms_in_col_type_desc, terms_values);

    char term_str[] = "wyf";
    int term_value_length = (int)strlen(term_str);
    StringValue term_value(term_str, term_value_length);
    ExtLiteral term_literal(TYPE_VARCHAR, &term_value);
    TypeDescriptor term_type_desc = TypeDescriptor::create_varchar_type(term_value_length);
    std::string term_field_name = "content";
    ExtBinaryPredicate* term_ne_predicate =
            new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, term_field_name, term_type_desc,
                                   TExprOpcode::NE, term_literal);

    std::vector<ExtPredicate*> inner_or_content = {term_ne_predicate, in_predicate};

    EsPredicate* inner_or_predicate = new EsPredicate(inner_or_content);

    char range_value_str[] = "a"; // k >= "a"
    int range_value_length = (int)strlen(range_value_str);
    StringValue range_value(range_value_str, range_value_length);
    ExtLiteral range_literal(TYPE_VARCHAR, &range_value);
    TypeDescriptor range_type_desc = TypeDescriptor::create_varchar_type(range_value_length);
    std::string range_field_name = "k";
    ExtBinaryPredicate* range_predicate =
            new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, range_field_name, range_type_desc,
                                   TExprOpcode::GE, range_literal);
    std::vector<ExtPredicate*> range_predicates = {range_predicate};
    EsPredicate* left_inner_or_predicate = new EsPredicate(range_predicates);

    std::vector<EsPredicate*> outer_left_predicates_1 = {left_inner_or_predicate,
                                                         inner_or_predicate};

    ExtCompPredicates* comp_predicate =
            new ExtCompPredicates(TExprOpcode::COMPOUND_AND, outer_left_predicates_1);

    char like_value[] = "a%e%g_";
    int like_value_length = (int)strlen(like_value);
    TypeDescriptor like_type_desc = TypeDescriptor::create_varchar_type(like_value_length);
    StringValue like_term_value(like_value, like_value_length);
    ExtLiteral like_literal(TYPE_VARCHAR, &like_term_value);
    std::string like_field_name = "content";
    ExtLikePredicate* like_predicate = new ExtLikePredicate(
            TExprNodeType::LIKE_PRED, like_field_name, like_type_desc, like_literal);

    std::vector<ExtPredicate*> or_predicate_vector = {comp_predicate, like_predicate};
    EsPredicate* or_predicate = new EsPredicate(or_predicate_vector);

    std::vector<EsPredicate*> or_predicates = {or_predicate};
    std::vector<bool> result1;
    BooleanQueryBuilder::validate(or_predicates, &result1);
    std::vector<bool> expected1 = {true};
    EXPECT_TRUE(result1 == expected1);

    rapidjson::Document document;
    rapidjson::Value compound_and_value(rapidjson::kObjectType);
    compound_and_value.SetObject();
    BooleanQueryBuilder::to_query(or_predicates, &document, &compound_and_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    compound_and_value.Accept(writer);
    std::string actual_bool_json = buffer.GetString();
    std::string expected_json =
            "{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"bool\":{\"filter\":[{\"bool\":{"
            "\"should\":[{\"range\":{\"k\":{\"gte\":\"a\"}}}]}},{\"bool\":{\"should\":[{\"bool\":{"
            "\"must_not\":[{\"term\":{\"content\":\"wyf\"}}]}},{\"bool\":{\"must_not\":[{\"terms\":"
            "{\"fv\":[\"8.0\",\"16.0\"]}}]}}]}}]}},{\"wildcard\":{\"content\":\"a*e*g?\"}}]}}]}}";
    EXPECT_STREQ(expected_json.c_str(), actual_bool_json.c_str());
}
} // namespace doris
