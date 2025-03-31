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

#include "pipeline/exec/datagen_operator.h"

#include <gtest/gtest.h>

#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
namespace doris::pipeline {

TEST(DataGenSourceOperatorTest, testRows) {
    using namespace vectorized;

    OperatorContext ctx;

    DataGenSourceOperatorX op;

    // init op
    std::vector<DataTypePtr> data_types {std::make_shared<DataTypeInt64>()};
    auto row_desc = std::make_unique<MockRowDescriptor>(data_types, &ctx.pool);
    op._tuple_id = 0;
    op._tuple_desc = row_desc->tuple_desc_map[0];

    // init local state
    TDataGenScanRange data_gen_scan_range;
    data_gen_scan_range.numbers_params.useConst = false;
    data_gen_scan_range.numbers_params.constValue = 0;
    data_gen_scan_range.numbers_params.totalNumbers = 10;

    TScanRangeParams scan_range_param;
    scan_range_param.scan_range.data_gen_scan_range = data_gen_scan_range;

    OperatorHelper::init_local_state(ctx, op, {scan_range_param});

    // execute op
    bool eos = false;
    vectorized::Block block;
    EXPECT_TRUE(op.get_block(&ctx.state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 10);
}

TEST(DataGenSourceOperatorTest, testtotalNumbers) {
    using namespace vectorized;

    OperatorContext ctx;

    DataGenSourceOperatorX op;

    // init op
    std::vector<DataTypePtr> data_types {std::make_shared<DataTypeInt64>()};
    auto row_desc = std::make_unique<MockRowDescriptor>(data_types, &ctx.pool);
    op._tuple_id = 0;
    op._tuple_desc = row_desc->tuple_desc_map[0];

    // init local state
    TDataGenScanRange data_gen_scan_range;
    data_gen_scan_range.numbers_params.useConst = false;
    data_gen_scan_range.numbers_params.constValue = 0;
    data_gen_scan_range.numbers_params.totalNumbers = 10;

    TScanRangeParams scan_range_param;
    scan_range_param.scan_range.data_gen_scan_range = data_gen_scan_range;

    OperatorHelper::init_local_state(ctx, op, {scan_range_param});

    // execute op
    bool eos = false;
    vectorized::Block block;
    EXPECT_TRUE(op.get_block(&ctx.state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 10);

    EXPECT_TRUE(ColumnHelper::column_equal(
            block.get_by_position(0).column,
            ColumnHelper::create_column<DataTypeInt64>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9})));
}

TEST(DataGenSourceOperatorTest, testConst) {
    using namespace vectorized;

    OperatorContext ctx;

    DataGenSourceOperatorX op;

    // init op
    std::vector<DataTypePtr> data_types {std::make_shared<DataTypeInt64>()};
    auto row_desc = std::make_unique<MockRowDescriptor>(data_types, &ctx.pool);
    op._tuple_id = 0;
    op._tuple_desc = row_desc->tuple_desc_map[0];

    // init local state
    TDataGenScanRange data_gen_scan_range;
    data_gen_scan_range.numbers_params.useConst = true;
    data_gen_scan_range.numbers_params.constValue = 5;
    data_gen_scan_range.numbers_params.totalNumbers = 10;

    TScanRangeParams scan_range_param;
    scan_range_param.scan_range.data_gen_scan_range = data_gen_scan_range;

    OperatorHelper::init_local_state(ctx, op, {scan_range_param});

    // execute op
    bool eos = false;
    vectorized::Block block;
    EXPECT_TRUE(op.get_block(&ctx.state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 10);

    EXPECT_TRUE(ColumnHelper::column_equal(
            block.get_by_position(0).column,
            ColumnHelper::create_column<DataTypeInt64>({5, 5, 5, 5, 5, 5, 5, 5, 5, 5})));
}

} // namespace doris::pipeline
