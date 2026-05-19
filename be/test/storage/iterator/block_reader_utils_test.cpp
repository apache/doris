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

#include "storage/iterator/block_reader_utils.h"

#include <gtest/gtest.h>

#include "vec/columns/columns_number.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized::detail {

TEST(BlockReaderUtilsTest, BuildBeforeColumnName) {
    EXPECT_EQ(build_before_column_name("v1"), "__BEFORE__v1__");
}

TEST(BlockReaderUtilsTest, ResolveBeforeColumnIndex) {
    auto int_type = std::make_shared<DataTypeInt64>();
    auto col_key = ColumnInt64::create();
    auto col_val = ColumnInt64::create();
    auto col_before_val = ColumnInt64::create();
    auto col_op = ColumnInt64::create();

    Block block;
    block.insert({std::move(col_key), int_type, "k1"});
    block.insert({std::move(col_val), int_type, "v1"});
    block.insert({std::move(col_before_val), int_type, "__BEFORE__v1__"});
    block.insert({std::move(col_op), int_type, "__DORIS_BINLOG_OP__"});

    EXPECT_EQ(resolve_before_column_index(block, 1, 3), 2);
    EXPECT_EQ(resolve_before_column_index(block, 3, 3), 3);
}

TEST(BlockReaderUtilsTest, ResolveBeforeColumnIndexFallbackWhenMissing) {
    auto int_type = std::make_shared<DataTypeInt64>();
    auto col_key = ColumnInt64::create();
    auto col_val = ColumnInt64::create();
    auto col_op = ColumnInt64::create();

    Block block;
    block.insert({std::move(col_key), int_type, "k1"});
    block.insert({std::move(col_val), int_type, "v1"});
    block.insert({std::move(col_op), int_type, "__DORIS_BINLOG_OP__"});

    // If __BEFORE__v1__ is missing, fall back to the current column to avoid out-of-bounds.
    EXPECT_EQ(resolve_before_column_index(block, 1, 2), 1);
    // Non-op columns should return themselves.
    EXPECT_EQ(resolve_before_column_index(block, 0, 2), 0);
}

} // namespace doris::vectorized::detail
