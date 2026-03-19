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

#include <unordered_map>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/exec/scan/file_scanner.h"

namespace doris::vectorized {

class FileScannerIOContextTest : public testing::Test {
protected:
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile {"file_scanner_io_context"};
    TFileScanRangeParams params;
    std::unordered_map<std::string, int> col_name_to_slot_id;
};

TEST_F(FileScannerIOContextTest, UsesExplicitRangeContext) {
    FileScanner scanner(&runtime_state, &profile, &params, &col_name_to_slot_id, nullptr);

    TFileRangeDesc range;
    range.__set_table_name("catalog.db.tbl");
    range.__set_partition_name("dt=20260319");
    range.columns_from_path_keys = {"dt"};
    range.__isset.columns_from_path_keys = true;
    range.columns_from_path = {"20260320"};
    range.__isset.columns_from_path = true;

    scanner._current_range = range;
    ASSERT_TRUE(scanner._init_io_ctx().ok());

    scanner._update_io_context_from_range();

    ASSERT_NE(scanner._io_ctx, nullptr);
    EXPECT_EQ(scanner._io_ctx->table_name, "catalog.db.tbl");
    EXPECT_EQ(scanner._io_ctx->partition_name, "dt=20260319");
}

TEST_F(FileScannerIOContextTest, BuildsPartitionNameFromPathWhenRangePartitionMissing) {
    FileScanner scanner(&runtime_state, &profile, &params, &col_name_to_slot_id, nullptr);

    TFileRangeDesc range;
    range.columns_from_path_keys = {"country", "dt"};
    range.__isset.columns_from_path_keys = true;
    range.columns_from_path = {"cn", "20260319"};
    range.__isset.columns_from_path = true;

    scanner._current_range = range;
    ASSERT_TRUE(scanner._init_io_ctx().ok());

    scanner._update_io_context_from_range();

    ASSERT_NE(scanner._io_ctx, nullptr);
    EXPECT_TRUE(scanner._io_ctx->table_name.empty());
    EXPECT_EQ(scanner._io_ctx->partition_name, "country=cn/dt=20260319");
    EXPECT_EQ(scanner._build_partition_name(range), "country=cn/dt=20260319");
}

TEST_F(FileScannerIOContextTest, ClearsStaleTableNameWhenRangeContextMissing) {
    FileScanner scanner(&runtime_state, &profile, &params, &col_name_to_slot_id, nullptr);

    scanner._current_range.__set_table_name("catalog.db.tbl");
    ASSERT_TRUE(scanner._init_io_ctx().ok());
    scanner._update_io_context_from_range();
    ASSERT_EQ(scanner._io_ctx->table_name, "catalog.db.tbl");

    TFileRangeDesc range_without_context;
    range_without_context.columns_from_path_keys = {"dt"};
    range_without_context.__isset.columns_from_path_keys = true;
    range_without_context.columns_from_path = {"20260320"};
    range_without_context.__isset.columns_from_path = true;
    scanner._current_range = range_without_context;

    scanner._update_io_context_from_range();

    EXPECT_TRUE(scanner._io_ctx->table_name.empty());
    EXPECT_EQ(scanner._io_ctx->partition_name, "dt=20260320");
}

} // namespace doris::vectorized
