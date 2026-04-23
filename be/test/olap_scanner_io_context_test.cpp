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
#include <gen_cpp/QueryCache_types.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/object_pool.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/olap_scanner.h"
#undef private
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {

class OlapScannerIOContextTest : public testing::Test {
protected:
    void SetUp() override {
        obj_pool = std::make_unique<ObjectPool>();

        plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
        plan_node.row_tuples.push_back(TTupleId(0));
        plan_node.row_tuples.push_back(TTupleId(1));
        plan_node.olap_scan_node.__set_tuple_id(0);
        plan_node.__isset.olap_scan_node = true;

        table_desc.tableType = TTableType::OLAP_TABLE;
        thrift_desc_tbl.tableDescriptors.push_back(table_desc);

        TTupleDescriptor tuple_desc;
        tuple_desc.id = 0;
        thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);
        tuple_desc.id = 1;
        thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);

        ASSERT_TRUE(DescriptorTbl::create(obj_pool.get(), thrift_desc_tbl, &descriptor_table).ok());
    }

    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile {"olap_scanner_io_context"};
    TPlanNode plan_node;
    TTableDescriptor table_desc;
    TDescriptorTable thrift_desc_tbl;
    DescriptorTbl* descriptor_table = nullptr;
    std::unique_ptr<ObjectPool> obj_pool;
};

TEST_F(OlapScannerIOContextTest, ConstructorStoresRangeFileCacheContext) {
    auto scan_operator = std::make_unique<OlapScanOperatorX>(obj_pool.get(), plan_node, 0,
                                                             *descriptor_table, 1,
                                                             TQueryCacheParam {});
    auto local_state = OlapScanLocalState::create_unique(&runtime_state, scan_operator.get());

    OlapScanner::Params params;
    params.state = &runtime_state;
    params.profile = &profile;
    params.limit = -1;
    params.key_ranges = {};
    params.table_name = "internal.test.tbl(base_index)";
    params.partition_name = "p20260319";

    auto scanner = OlapScanner::create_shared(local_state.get(), std::move(params));

    EXPECT_EQ(scanner->_tablet_reader_params.table_name, "internal.test.tbl(base_index)");
    EXPECT_EQ(scanner->_tablet_reader_params.partition_name, "p20260319");
}

} // namespace doris
