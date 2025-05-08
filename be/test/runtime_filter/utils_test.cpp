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

#include "runtime_filter/utils.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vliteral.h"

namespace doris {

class RuntimeFilterUtilsTest : public testing::Test {
public:
    RuntimeFilterUtilsTest() = default;
    ~RuntimeFilterUtilsTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RuntimeFilterUtilsTest, TestConvertorException) {
    class TestClass {};
    bool exception = false;
    try {
        get_convertor<TestClass>();
    } catch (std::exception) {
        exception = true;
    }
    EXPECT_TRUE(exception);
}

TEST_F(RuntimeFilterUtilsTest, TestFilterTypeFunction) {
#define CHECK_FOR_FILTER(TYPE)                                        \
    EXPECT_EQ(filter_type_to_string(RuntimeFilterType::TYPE), #TYPE); \
    EXPECT_EQ(get_type(RuntimeFilterType::TYPE), PFilterType::TYPE);  \
    EXPECT_EQ(get_type(PFilterType::TYPE), RuntimeFilterType::TYPE);
    CHECK_FOR_FILTER(UNKNOWN_FILTER);
    CHECK_FOR_FILTER(IN_FILTER);
    CHECK_FOR_FILTER(MINMAX_FILTER);
    CHECK_FOR_FILTER(BLOOM_FILTER);
    CHECK_FOR_FILTER(IN_OR_BLOOM_FILTER);
    CHECK_FOR_FILTER(MIN_FILTER);
    CHECK_FOR_FILTER(MAX_FILTER);
#undef CHECK_FOR_FILTER

    EXPECT_EQ(filter_type_to_string(RuntimeFilterType::BITMAP_FILTER), "BITMAP_FILTER");
    EXPECT_EQ(get_type(RuntimeFilterType::BITMAP_FILTER), PFilterType::UNKNOWN_FILTER);
}

TEST_F(RuntimeFilterUtilsTest, TestRuntimeFilterFromThrift) {
    TRuntimeFilterDesc desc;
    desc.__set_type(TRuntimeFilterType::BLOOM);
    EXPECT_EQ(get_runtime_filter_type(&desc), RuntimeFilterType::BLOOM_FILTER);

    desc.__set_type(TRuntimeFilterType::MIN_MAX);
    desc.__set_min_max_type(TMinMaxRuntimeFilterType::MIN);
    EXPECT_EQ(get_runtime_filter_type(&desc), RuntimeFilterType::MIN_FILTER);
    desc.__set_min_max_type(TMinMaxRuntimeFilterType::MAX);
    EXPECT_EQ(get_runtime_filter_type(&desc), RuntimeFilterType::MAX_FILTER);
    desc.__set_min_max_type(TMinMaxRuntimeFilterType::MIN_MAX);
    EXPECT_EQ(get_runtime_filter_type(&desc), RuntimeFilterType::MINMAX_FILTER);

    desc.__set_type(TRuntimeFilterType::IN_OR_BLOOM);
    EXPECT_EQ(get_runtime_filter_type(&desc), RuntimeFilterType::IN_OR_BLOOM_FILTER);
    desc.__set_type(TRuntimeFilterType::BITMAP);
    EXPECT_EQ(get_runtime_filter_type(&desc), RuntimeFilterType::BITMAP_FILTER);
    desc.__set_type(TRuntimeFilterType::IN);
    EXPECT_EQ(get_runtime_filter_type(&desc), RuntimeFilterType::IN_FILTER);
}

TEST_F(RuntimeFilterUtilsTest, TestCreateLiteral) {
    vectorized::VExprSPtr literal;
    auto type = vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT,
                                                                         false);
    const int value = 1;
    EXPECT_TRUE(create_literal(type, (const void*)&value, literal).ok());
    EXPECT_TRUE(literal->is_literal());
    EXPECT_EQ(((vectorized::VLiteral*)literal.get())->value(), std::to_string(value));
}

TEST_F(RuntimeFilterUtilsTest, TestCreateBinaryPredicate) {
    {
        vectorized::VExprSPtr expr;
        TExprNode pred_node;
        auto type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, false);
        auto op = TExprOpcode::EQ;
        EXPECT_FALSE(create_vbin_predicate(type, op, expr, &pred_node, false).ok());
    }
    {
        vectorized::VExprSPtr expr;
        TExprNode pred_node;
        auto type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, false);
        auto op = TExprOpcode::GE;
        EXPECT_TRUE(create_vbin_predicate(type, op, expr, &pred_node, true).ok());
    }
    {
        vectorized::VExprSPtr expr;
        TExprNode pred_node;
        auto type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, false);
        auto op = TExprOpcode::LE;
        EXPECT_TRUE(create_vbin_predicate(type, op, expr, &pred_node, false).ok());
    }
}

} // namespace doris
