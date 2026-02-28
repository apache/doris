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

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/boolean_query_builder.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur_boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/operator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/operator_boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"

namespace doris::segment_v2::inverted_index::query_v2 {

namespace {

class MockQuery : public Query {
public:
    MockQuery(int id = 0) : _id(id) {}
    ~MockQuery() override = default;

    WeightPtr weight(bool enable_scoring) override { return nullptr; }

    int id() const { return _id; }

private:
    int _id;
};

using MockQueryPtr = std::shared_ptr<MockQuery>;

} // namespace

class BooleanQueryBuilderTest : public ::testing::Test {};

TEST_F(BooleanQueryBuilderTest, OccurBooleanQueryBuilderEmpty) {
    auto builder = create_occur_boolean_query_builder();
    ASSERT_NE(nullptr, builder);

    auto query = builder->build();
    ASSERT_NE(nullptr, query);

    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(query);
    ASSERT_NE(nullptr, occur_query);
    EXPECT_TRUE(occur_query->clauses().empty());
}

TEST_F(BooleanQueryBuilderTest, OccurBooleanQueryBuilderSingleMust) {
    auto builder = create_occur_boolean_query_builder();
    auto mock_query = std::make_shared<MockQuery>(1);

    builder->add(mock_query, Occur::MUST);
    auto query = builder->build();

    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(query);
    ASSERT_NE(nullptr, occur_query);
    ASSERT_EQ(1u, occur_query->clauses().size());
    EXPECT_EQ(Occur::MUST, occur_query->clauses()[0].first);
    EXPECT_EQ(mock_query, occur_query->clauses()[0].second);
}

TEST_F(BooleanQueryBuilderTest, OccurBooleanQueryBuilderSingleShould) {
    auto builder = create_occur_boolean_query_builder();
    auto mock_query = std::make_shared<MockQuery>(2);

    builder->add(mock_query, Occur::SHOULD);
    auto query = builder->build();

    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(query);
    ASSERT_NE(nullptr, occur_query);
    ASSERT_EQ(1u, occur_query->clauses().size());
    EXPECT_EQ(Occur::SHOULD, occur_query->clauses()[0].first);
}

TEST_F(BooleanQueryBuilderTest, OccurBooleanQueryBuilderSingleMustNot) {
    auto builder = create_occur_boolean_query_builder();
    auto mock_query = std::make_shared<MockQuery>(3);

    builder->add(mock_query, Occur::MUST_NOT);
    auto query = builder->build();

    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(query);
    ASSERT_NE(nullptr, occur_query);
    ASSERT_EQ(1u, occur_query->clauses().size());
    EXPECT_EQ(Occur::MUST_NOT, occur_query->clauses()[0].first);
}

TEST_F(BooleanQueryBuilderTest, OccurBooleanQueryBuilderMultipleClauses) {
    auto builder = create_occur_boolean_query_builder();
    auto query1 = std::make_shared<MockQuery>(1);
    auto query2 = std::make_shared<MockQuery>(2);
    auto query3 = std::make_shared<MockQuery>(3);

    builder->add(query1, Occur::MUST);
    builder->add(query2, Occur::SHOULD);
    builder->add(query3, Occur::MUST_NOT);

    auto query = builder->build();
    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(query);
    ASSERT_NE(nullptr, occur_query);
    ASSERT_EQ(3u, occur_query->clauses().size());

    EXPECT_EQ(Occur::MUST, occur_query->clauses()[0].first);
    EXPECT_EQ(query1, occur_query->clauses()[0].second);

    EXPECT_EQ(Occur::SHOULD, occur_query->clauses()[1].first);
    EXPECT_EQ(query2, occur_query->clauses()[1].second);

    EXPECT_EQ(Occur::MUST_NOT, occur_query->clauses()[2].first);
    EXPECT_EQ(query3, occur_query->clauses()[2].second);
}

TEST_F(BooleanQueryBuilderTest, OccurBooleanQueryBuilderMixedOccurs) {
    auto builder = create_occur_boolean_query_builder();

    builder->add(std::make_shared<MockQuery>(1), Occur::MUST);
    builder->add(std::make_shared<MockQuery>(2), Occur::MUST);
    builder->add(std::make_shared<MockQuery>(3), Occur::SHOULD);
    builder->add(std::make_shared<MockQuery>(4), Occur::SHOULD);
    builder->add(std::make_shared<MockQuery>(5), Occur::MUST_NOT);

    auto query = builder->build();
    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(query);
    ASSERT_NE(nullptr, occur_query);
    ASSERT_EQ(5u, occur_query->clauses().size());

    int must_count = 0;
    int should_count = 0;
    int must_not_count = 0;
    for (const auto& [occur, q] : occur_query->clauses()) {
        switch (occur) {
        case Occur::MUST:
            must_count++;
            break;
        case Occur::SHOULD:
            should_count++;
            break;
        case Occur::MUST_NOT:
            must_not_count++;
            break;
        }
    }
    EXPECT_EQ(2, must_count);
    EXPECT_EQ(2, should_count);
    EXPECT_EQ(1, must_not_count);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderEmpty) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_AND);
    ASSERT_NE(nullptr, builder);

    auto query = builder->build();
    ASSERT_NE(nullptr, query);

    auto op_query = std::dynamic_pointer_cast<OperatorBooleanQuery>(query);
    ASSERT_NE(nullptr, op_query);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderOpAnd) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_AND);
    auto query1 = std::make_shared<MockQuery>(1);
    auto query2 = std::make_shared<MockQuery>(2);

    builder->add(query1);
    builder->add(query2);

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderOpOr) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_OR);
    auto query1 = std::make_shared<MockQuery>(1);
    auto query2 = std::make_shared<MockQuery>(2);

    builder->add(query1);
    builder->add(query2);

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderOpNot) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_NOT);
    auto query1 = std::make_shared<MockQuery>(1);
    auto query2 = std::make_shared<MockQuery>(2);

    builder->add(query1);
    builder->add(query2);

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderWithBindingKeys) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_AND);
    auto query1 = std::make_shared<MockQuery>(1);
    auto query2 = std::make_shared<MockQuery>(2);
    auto query3 = std::make_shared<MockQuery>(3);

    builder->add(query1, "field1");
    builder->add(query2, "field2");
    builder->add(query3, "field3");

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderMixedBindingKeys) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_OR);

    builder->add(std::make_shared<MockQuery>(1), "key1");
    builder->add(std::make_shared<MockQuery>(2));
    builder->add(std::make_shared<MockQuery>(3), "key3");

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderEmptyBindingKey) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_AND);

    builder->add(std::make_shared<MockQuery>(1), "");
    builder->add(std::make_shared<MockQuery>(2), std::string {});

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderSingleQuery) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_AND);
    builder->add(std::make_shared<MockQuery>(1), "single_key");

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderManyQueries) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_OR);

    for (int i = 0; i < 100; ++i) {
        builder->add(std::make_shared<MockQuery>(i), "key_" + std::to_string(i));
    }

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, CreateOccurBooleanQueryBuilderFunction) {
    auto builder1 = create_occur_boolean_query_builder();
    auto builder2 = create_occur_boolean_query_builder();

    ASSERT_NE(nullptr, builder1);
    ASSERT_NE(nullptr, builder2);
    EXPECT_NE(builder1, builder2);
}

TEST_F(BooleanQueryBuilderTest, CreateOperatorBooleanQueryBuilderFunction) {
    auto builder_and = create_operator_boolean_query_builder(OperatorType::OP_AND);
    auto builder_or = create_operator_boolean_query_builder(OperatorType::OP_OR);
    auto builder_not = create_operator_boolean_query_builder(OperatorType::OP_NOT);

    ASSERT_NE(nullptr, builder_and);
    ASSERT_NE(nullptr, builder_or);
    ASSERT_NE(nullptr, builder_not);
}

TEST_F(BooleanQueryBuilderTest, OccurBooleanQueryBuilderAddOrder) {
    auto builder = create_occur_boolean_query_builder();
    std::vector<MockQueryPtr> queries;
    std::vector<Occur> occurs {Occur::MUST, Occur::SHOULD, Occur::MUST_NOT, Occur::MUST,
                               Occur::SHOULD};

    for (size_t i = 0; i < occurs.size(); ++i) {
        auto q = std::make_shared<MockQuery>(static_cast<int>(i));
        queries.push_back(q);
        builder->add(q, occurs[i]);
    }

    auto query = builder->build();
    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(query);
    ASSERT_NE(nullptr, occur_query);
    ASSERT_EQ(occurs.size(), occur_query->clauses().size());

    for (size_t i = 0; i < occurs.size(); ++i) {
        EXPECT_EQ(occurs[i], occur_query->clauses()[i].first);
        auto mock = std::dynamic_pointer_cast<MockQuery>(occur_query->clauses()[i].second);
        ASSERT_NE(nullptr, mock);
        EXPECT_EQ(static_cast<int>(i), mock->id());
    }
}

TEST_F(BooleanQueryBuilderTest, OccurBooleanQueryBuilderSameQueryMultipleTimes) {
    auto builder = create_occur_boolean_query_builder();
    auto shared_query = std::make_shared<MockQuery>(42);

    builder->add(shared_query, Occur::MUST);
    builder->add(shared_query, Occur::SHOULD);
    builder->add(shared_query, Occur::MUST_NOT);

    auto query = builder->build();
    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(query);
    ASSERT_NE(nullptr, occur_query);
    ASSERT_EQ(3u, occur_query->clauses().size());

    for (const auto& [occur, q] : occur_query->clauses()) {
        EXPECT_EQ(shared_query, q);
    }
}

TEST_F(BooleanQueryBuilderTest, OperatorBooleanQueryBuilderSameQueryMultipleTimes) {
    auto builder = create_operator_boolean_query_builder(OperatorType::OP_AND);
    auto shared_query = std::make_shared<MockQuery>(99);

    builder->add(shared_query, "key1");
    builder->add(shared_query, "key2");
    builder->add(shared_query, "key3");

    auto query = builder->build();
    ASSERT_NE(nullptr, query);
}

TEST_F(BooleanQueryBuilderTest, NestedOccurBooleanQueries) {
    auto inner_builder = create_occur_boolean_query_builder();
    inner_builder->add(std::make_shared<MockQuery>(1), Occur::MUST);
    inner_builder->add(std::make_shared<MockQuery>(2), Occur::SHOULD);
    auto inner_query = inner_builder->build();

    auto outer_builder = create_occur_boolean_query_builder();
    outer_builder->add(inner_query, Occur::MUST);
    outer_builder->add(std::make_shared<MockQuery>(3), Occur::MUST_NOT);

    auto outer_query = outer_builder->build();
    auto occur_query = std::dynamic_pointer_cast<OccurBooleanQuery>(outer_query);
    ASSERT_NE(nullptr, occur_query);
    ASSERT_EQ(2u, occur_query->clauses().size());
    EXPECT_EQ(inner_query, occur_query->clauses()[0].second);
}

TEST_F(BooleanQueryBuilderTest, NestedOperatorBooleanQueries) {
    auto inner_builder = create_operator_boolean_query_builder(OperatorType::OP_OR);
    inner_builder->add(std::make_shared<MockQuery>(1), "inner1");
    inner_builder->add(std::make_shared<MockQuery>(2), "inner2");
    auto inner_query = inner_builder->build();

    auto outer_builder = create_operator_boolean_query_builder(OperatorType::OP_AND);
    outer_builder->add(inner_query, "nested");
    outer_builder->add(std::make_shared<MockQuery>(3), "outer1");

    auto outer_query = outer_builder->build();
    ASSERT_NE(nullptr, outer_query);
}

TEST_F(BooleanQueryBuilderTest, MixedNestedQueries) {
    auto occur_builder = create_occur_boolean_query_builder();
    occur_builder->add(std::make_shared<MockQuery>(1), Occur::MUST);
    occur_builder->add(std::make_shared<MockQuery>(2), Occur::SHOULD);
    auto occur_query = occur_builder->build();

    auto operator_builder = create_operator_boolean_query_builder(OperatorType::OP_AND);
    operator_builder->add(occur_query, "occur_nested");
    operator_builder->add(std::make_shared<MockQuery>(3), "simple");

    auto final_query = operator_builder->build();
    ASSERT_NE(nullptr, final_query);
}

} // namespace doris::segment_v2::inverted_index::query_v2
