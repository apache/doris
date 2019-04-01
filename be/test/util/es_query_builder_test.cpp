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

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include "common/logging.h"
#include "util/es_query_builder.h"
#include "rapidjson/document.h"
#include "exec/es_predicate.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/string_value.h"
namespace doris {

class BooleanQueryBuilderTest : public testing::Test {
public:
    BooleanQueryBuilderTest() { }
    virtual ~BooleanQueryBuilderTest() { }
};
TEST_F(BooleanQueryBuilderTest, term_query) {
    // content = "wyf" 
    char str[] = "wyf";
    StringValue value(str, 3);
    ExtLiteral term_literal(TYPE_VARCHAR, &value);
    TypeDescriptor type_desc = TypeDescriptor::create_varchar_type(3);
    std::string name = "content";
    ExtBinaryPredicate* term_predicate = new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, name, type_desc, TExprOpcode::EQ, term_literal);
    TermQueryBuilder term_query(term_predicate);
    rapidjson::Document document;
    rapidjson::Value term_value = term_query.to_json(document);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    term_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "term query" << actual_json;
    ASSERT_STREQ("{\"term\":{\"content\":\"wyf\"}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, range_query) {
    // k >= a
    char str[] = "a";
    StringValue value(str, 1);
    ExtLiteral term_literal(TYPE_VARCHAR, &value);
    TypeDescriptor type_desc = TypeDescriptor::create_varchar_type(1);
    std::string name = "k";
    ExtBinaryPredicate* range_predicate = new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, name, type_desc, TExprOpcode::GE, term_literal);
    RangeQueryBuilder range_query(range_predicate);
    rapidjson::Document document;
    rapidjson::Value range_value = range_query.to_json(document);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    range_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "range query" << actual_json;
    ASSERT_STREQ("{\"range\":{\"k\":{\"ge\":\"a\"}}}", actual_json.c_str());
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
    std::string function_name = "es_query";
    ExtFunction* function_predicate = new ExtFunction(TExprNodeType::FUNCTION_CALL, function_name, cols, values);
    ESQueryBuilder es_query(function_predicate);
    rapidjson::Document document;
    rapidjson::Value es_query_value = es_query.to_json(document);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    es_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "es query" << actual_json;
    ASSERT_STREQ("{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"f1\"}}}}", actual_json.c_str());
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
    ExtLikePredicate* like_predicate = new ExtLikePredicate(TExprNodeType::LIKE_PRED, name, type_desc, like_literal);
    WildCardQueryBuilder like_query(like_predicate);
    rapidjson::Document document;
    rapidjson::Value like_query_value = like_query.to_json(document);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    like_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    // LOG(INFO) << "wildcard query" << actual_json;
    ASSERT_STREQ("{\"wildcard\":{\"content\":\"a*e*g?\"}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, terms_in_query) {
    // dv in ["2.0", "4.0", "8.0"]
    std::string terms_in_field = "dv";
    int terms_in_field_length = terms_in_field.length();
    TypeDescriptor terms_in_col_type_desc = TypeDescriptor::create_varchar_type(terms_in_field_length);

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
    ExtInPredicate* in_predicate = new ExtInPredicate(TExprNodeType::IN_PRED, terms_in_field, terms_in_col_type_desc, terms_values);
    TermsInSetQueryBuilder terms_query(in_predicate);
    rapidjson::Document document;
    rapidjson::Value in_query_value = terms_query.to_json(document);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    in_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "terms in sets query" << actual_json;
    ASSERT_STREQ("{\"terms\":{\"dv\":[\"2.0\",\"4.0\",\"8.0\"]}}", actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, match_all_query) {
    // match all docs
    MatchAllQueryBuilder match_all_query;
    rapidjson::Document document;
    rapidjson::Value match_all_query_value = match_all_query.to_json(document);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    match_all_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    //LOG(INFO) << "match all query" << actual_json;
    ASSERT_STREQ("{\"match_all\":{}}", actual_json.c_str());
}


TEST_F(BooleanQueryBuilderTest, bool_query) {
    // content like 'a%e%g_'
    char like_value[] = "a%e%g_";
    int like_value_length = (int)strlen(like_value);
    TypeDescriptor like_type_desc = TypeDescriptor::create_varchar_type(like_value_length);
    StringValue like_term_value(like_value, like_value_length);
    ExtLiteral like_literal(TYPE_VARCHAR, &like_term_value);
    std::string like_field_name = "content";
    ExtLikePredicate* like_predicate = new ExtLikePredicate(TExprNodeType::LIKE_PRED, like_field_name, like_type_desc, like_literal);
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
    std::string function_name = "es_query";
    ExtFunction* function_predicate = new ExtFunction(TExprNodeType::FUNCTION_CALL, function_name, es_query_cols, es_query_values);
    // k >= a
    char range_value_str[] = "a";
    int range_value_length = (int)strlen(range_value_str);
    StringValue range_value(range_value_str, range_value_length);
    ExtLiteral range_literal(TYPE_VARCHAR, &range_value);
    TypeDescriptor range_type_desc = TypeDescriptor::create_varchar_type(range_value_length);
    std::string range_field_name = "k";
    ExtBinaryPredicate* range_predicate = new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, range_field_name, range_type_desc, TExprOpcode::GE, range_literal);
    // content = "wyf"
    char term_str[] = "wyf";
    int term_value_length = (int)strlen(term_str);
    StringValue term_value(term_str, term_value_length);
    ExtLiteral term_literal(TYPE_VARCHAR, &term_value);
    TypeDescriptor term_type_desc = TypeDescriptor::create_varchar_type(term_value_length);
    std::string term_field_name = "content";
    ExtBinaryPredicate* term_predicate = new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, term_field_name, term_type_desc, TExprOpcode::EQ, term_literal);
    
    // content like 'a%e%g_' or k >= a or content = "wyf"
    std::vector<ExtPredicate*> or_predicates = {like_predicate, function_predicate, range_predicate, term_predicate};
    BooleanQueryBuilder bool_query(or_predicates);
    rapidjson::Document document;
    rapidjson::Value bool_query_value = bool_query.to_json(document);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    bool_query_value.Accept(writer);
    std::string actual_json = buffer.GetString();
    std::string expected_json = "{\"bool\":{\"should\":[{\"wildcard\":{\"content\":\"a*e*g?\"}},{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"f1\"}}}},{\"range\":{\"k\":{\"ge\":\"a\"}}},{\"term\":{\"content\":\"wyf\"}}]}}";
    //LOG(INFO) << "bool query" << actual_json;
    ASSERT_STREQ(expected_json.c_str(), actual_json.c_str());
}

TEST_F(BooleanQueryBuilderTest, compound_bool_query) {
    //  content like "a%e%g_" or esquery(random, '{"bool": {"must_not": {"exists": {"field": "f1"}}}}')
    char like_value[] = "a%e%g_";
    int like_value_length = (int)strlen(like_value);
    TypeDescriptor like_type_desc = TypeDescriptor::create_varchar_type(like_value_length);
    StringValue like_term_value(like_value, like_value_length);
    ExtLiteral like_literal(TYPE_VARCHAR, &like_term_value);
    std::string like_field_name = "content";
    ExtLikePredicate* like_predicate = new ExtLikePredicate(TExprNodeType::LIKE_PRED, like_field_name, like_type_desc, like_literal);

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
    std::string function_name = "es_query";
    ExtFunction* function_predicate = new ExtFunction(TExprNodeType::FUNCTION_CALL, function_name, es_query_cols, es_query_values);
    std::vector<ExtPredicate*> bool_predicates_1 = {like_predicate, function_predicate};
    EsPredicate* bool_predicate_1 = new EsPredicate(bool_predicates_1);

    // k >= "a"
    char range_value_str[] = "a";
    int range_value_length = (int)strlen(range_value_str);
    StringValue range_value(range_value_str, range_value_length);
    ExtLiteral range_literal(TYPE_VARCHAR, &range_value);
    TypeDescriptor range_type_desc = TypeDescriptor::create_varchar_type(range_value_length);
    std::string range_field_name = "k";
    ExtBinaryPredicate* range_predicate = new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, range_field_name, range_type_desc, TExprOpcode::GE, range_literal);
    
    std::vector<ExtPredicate*> bool_predicates_2 = {range_predicate};
    EsPredicate* bool_predicate_2 = new EsPredicate(bool_predicates_2);

    // content != "wyf"
    char term_str[] = "wyf";
    int term_value_length = (int)strlen(term_str);
    StringValue term_value(term_str, term_value_length);
    ExtLiteral term_literal(TYPE_VARCHAR, &term_value);
    TypeDescriptor term_type_desc = TypeDescriptor::create_varchar_type(term_value_length);
    std::string term_field_name = "content";
    ExtBinaryPredicate* term_ne_predicate = new ExtBinaryPredicate(TExprNodeType::BINARY_PRED, term_field_name, term_type_desc, TExprOpcode::NE, term_literal);
    std::vector<ExtPredicate*> bool_predicates_3 = {term_ne_predicate};
    EsPredicate* bool_predicate_3 = new EsPredicate(bool_predicates_3);

    // fv not in [8.0, 16.0]
    std::string terms_in_field = "fv";
    int terms_in_field_length = terms_in_field.length();
    TypeDescriptor terms_in_col_type_desc = TypeDescriptor::create_varchar_type(terms_in_field_length);

    char value_1[] = "8.0";
    int value_1_length = (int)strlen(value_1);
    StringValue string_value_1(value_1, value_1_length);
    ExtLiteral term_literal_1(TYPE_VARCHAR, &string_value_1);

    char value_2[] = "16.0";
    int value_2_length = (int)strlen(value_2);
    StringValue string_value_2(value_2, value_2_length);
    ExtLiteral term_literal_2(TYPE_VARCHAR, &string_value_2);

    std::vector<ExtLiteral> terms_values = {term_literal_1, term_literal_2};
    ExtInPredicate* in_predicate = new ExtInPredicate(TExprNodeType::IN_PRED, terms_in_field, terms_in_col_type_desc, terms_values);
    in_predicate->is_not_in = true;
    std::vector<ExtPredicate*> bool_predicates_4 = {in_predicate};
    EsPredicate* bool_predicate_4 = new EsPredicate(bool_predicates_4);

    // (content like "a%e%g_" or esquery(random, '{"bool": {"must_not": {"exists": {"field": "f1"}}}}')) and content != "wyf" and fv not in [8.0, 16.0]
    std::vector<EsPredicate*> and_bool_predicates = {bool_predicate_1, bool_predicate_2, bool_predicate_3, bool_predicate_4};
    
    rapidjson::Document document;
    rapidjson::Value compound_bool_value = BooleanQueryBuilder::to_query(and_bool_predicates);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    compound_bool_value.Accept(writer);
    std::string actual_bool_json = buffer.GetString();
    std::string expected_json = "{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"wildcard\":{\"content\":\"a*e*g?\"}},{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"f1\"}}}}]}},{\"bool\":{\"should\":[{\"range\":{\"k\":{\"ge\":\"a\"}}}]}},{\"bool\":{\"should\":[{\"bool\":{\"must_not\":[{\"term\":{\"content\":\"wyf\"}}]}}]}},{\"bool\":{\"should\":[{\"bool\":{\"must_not\":[{\"terms\":{\"fv\":[\"8.0\",\"16.0\"]}}]}}]}}]}}";
    //LOG(INFO) << "compound bool query" << actual_bool_json;
    ASSERT_STREQ(expected_json.c_str(), actual_bool_json.c_str());
}
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
