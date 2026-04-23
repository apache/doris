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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <unordered_map>

#include "common/object_pool.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "exec/operator/file_scan_operator.h"
#include "exec/scan/file_scanner.h"
#undef protected
#undef private
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {

class FileScannerIOContextTest : public testing::Test {
protected:
    void SetUp() override {
        obj_pool = std::make_unique<ObjectPool>();

        plan_node.__set_node_type(TPlanNodeType::FILE_SCAN_NODE);
        plan_node.row_tuples.push_back(TTupleId(0));
        plan_node.file_scan_node.__set_tuple_id(0);
        plan_node.file_scan_node.__set_table_name("catalog.db.tbl");
        plan_node.__isset.file_scan_node = true;

        TTupleDescriptor tuple_desc;
        tuple_desc.id = 0;
        thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);

        ASSERT_TRUE(DescriptorTbl::create(obj_pool.get(), thrift_desc_tbl, &descriptor_table).ok());

        scan_operator = std::make_unique<FileScanOperatorX>(obj_pool.get(), plan_node, 0,
                                                            *descriptor_table, 1);
        local_state = FileScanLocalState::create_unique(&runtime_state, scan_operator.get());
    }

    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile {"file_scanner_io_context"};
    TPlanNode plan_node;
    TDescriptorTable thrift_desc_tbl;
    DescriptorTbl* descriptor_table = nullptr;
    std::unique_ptr<ObjectPool> obj_pool;
    std::unique_ptr<FileScanOperatorX> scan_operator;
    std::unique_ptr<FileScanLocalState> local_state;
    TFileScanRangeParams params;
    std::unordered_map<std::string, int> col_name_to_slot_id;
};

TEST_F(FileScannerIOContextTest, UsesScanNodeTableNameWhenRangeContextIsMissing) {
    FileScanner scanner(&runtime_state, &profile, &params, &col_name_to_slot_id, nullptr);
    scanner._local_state = local_state.get();

    TFileRangeDesc range;
    range.__set_partition_name("dt=20260319");
    scanner._current_range = range;
    ASSERT_TRUE(scanner._init_io_ctx().ok());

    scanner._update_io_context_from_range();

    ASSERT_NE(scanner._io_ctx, nullptr);
    EXPECT_EQ(scanner._io_ctx->table_name, "catalog.db.tbl");
    EXPECT_EQ(scanner._io_ctx->partition_name, "dt=20260319");
}

TEST_F(FileScannerIOContextTest, BuildsPartitionNameFromPathWhenRangePartitionMissing) {
    FileScanner scanner(&runtime_state, &profile, &params, &col_name_to_slot_id, nullptr);
    scanner._local_state = local_state.get();

    TFileRangeDesc range;
    range.columns_from_path_keys = {"country", "dt"};
    range.__isset.columns_from_path_keys = true;
    range.columns_from_path = {"cn", "20260319"};
    range.__isset.columns_from_path = true;

    scanner._current_range = range;
    ASSERT_TRUE(scanner._init_io_ctx().ok());

    scanner._update_io_context_from_range();

    ASSERT_NE(scanner._io_ctx, nullptr);
    EXPECT_EQ(scanner._io_ctx->table_name, "catalog.db.tbl");
    EXPECT_EQ(scanner._io_ctx->partition_name, "country=cn/dt=20260319");
    EXPECT_EQ(scanner._build_partition_name(range), "country=cn/dt=20260319");
}

} // namespace doris
