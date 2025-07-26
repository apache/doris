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

#include "olap/predicate_creator.h"

namespace doris {

class NullPredicateTest : public testing::Test {
public:
    NullPredicateTest() = default;
    ~NullPredicateTest() override = default;

    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(NullPredicateTest, TestNullPredicate) {
    TabletColumn column;
    column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    column.set_name("test");
    column.set_unique_id(1);
    column.set_is_nullable(true);

    TCondition condition;
    condition.condition_op = "is";
    condition.condition_values.emplace_back("null");
    condition.column_unique_id = 1;

    vectorized::Arena arena;
    auto* predicate = parse_to_predicate(column, 0, condition, arena, false);
    EXPECT_TRUE(predicate->can_do_apply_safely(PrimitiveType::TYPE_INT, true));
    EXPECT_FALSE(predicate->can_do_apply_safely(PrimitiveType::TYPE_STRING, true));
    EXPECT_FALSE(predicate->can_do_apply_safely(PrimitiveType::TYPE_VARIANT, true));
    delete predicate;
}

} // namespace doris
