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

#include <memory>

#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "vec/data_types/data_type_number.h"
namespace doris::pipeline {

TEST(AssertNumRowsOperatorTest, testEQOK) {
    using namespace vectorized;
    OperatorContext ctx;
    // init op
    AssertNumRowsOperatorX op;
    op._assertion = TAssertion::EQ;
    op._desired_num_rows = 3;
    // init local state
    OperatorHelper::init_local_state(ctx, op);

    // inti mock op
    std::shared_ptr<MockOperatorX> mock_op = std::make_shared<MockOperatorX>();
    mock_op->_outout_blocks.push_back(ColumnHelper::create_block<DataTypeInt32>({1, 2, 3}));
    EXPECT_TRUE(op.set_child(mock_op).ok());

    // execute op
    bool eos = true;
    Block block;
    EXPECT_TRUE(op.get_block(&ctx.state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 3);
    EXPECT_TRUE(eos);
}

TEST(AssertNumRowsOperatorTest, testEQERROR) {
    using namespace vectorized;
    OperatorContext ctx;
    // init op
    AssertNumRowsOperatorX op;
    op._assertion = TAssertion::EQ;
    op._desired_num_rows = 3;
    // init local state
    OperatorHelper::init_local_state(ctx, op);

    // inti mock op
    std::shared_ptr<MockOperatorX> mock_op = std::make_shared<MockOperatorX>();
    mock_op->_outout_blocks.push_back(ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4}));
    EXPECT_TRUE(op.set_child(mock_op).ok());

    // execute op
    bool eos = true;
    Block block;
    Status st = op.get_block(&ctx.state, &block, &eos);
    EXPECT_FALSE(st.ok());
    std::cout << st.msg() << std::endl;

    EXPECT_EQ(block.rows(), 4);
    EXPECT_TRUE(eos);
}

TEST(AssertNumRowsOperatorTest, testEQ2) {
    using namespace vectorized;
    OperatorContext ctx;
    // init op
    AssertNumRowsOperatorX op;
    op._assertion = TAssertion::EQ;
    op._desired_num_rows = 7;
    // init local state
    OperatorHelper::init_local_state(ctx, op);

    // inti mock op
    std::shared_ptr<MockOperatorX> mock_op = std::make_shared<MockOperatorX>();
    mock_op->_outout_blocks.push_back(ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4}));
    mock_op->_outout_blocks.push_back(ColumnHelper::create_block<DataTypeInt32>({1, 2, 3}));
    EXPECT_TRUE(op.set_child(mock_op).ok());

    // execute op
    {
        bool eos = true;
        Block block;
        EXPECT_TRUE(op.get_block(&ctx.state, &block, &eos).ok());
        EXPECT_EQ(block.rows(), 4);
        EXPECT_FALSE(eos);
    }
    {
        bool eos = true;
        Block block;
        EXPECT_TRUE(op.get_block(&ctx.state, &block, &eos).ok());
        EXPECT_EQ(block.rows(), 3);
        EXPECT_TRUE(eos);
    }
}

} // namespace doris::pipeline
