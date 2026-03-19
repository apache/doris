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
#include <tuple>

#include "common/object_pool.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/exec/scan/olap_scanner.h"

namespace doris::vectorized {

class OlapScannerIOContextTest : public testing::Test {
protected:
    void SetUp() override {
        _obj_pool = std::make_unique<ObjectPool>();

        _plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
        _plan_node.row_tuples.push_back(TTupleId(0));
        _plan_node.row_tuples.push_back(TTupleId(1));
        _plan_node.olap_scan_node.__set_tuple_id(0);
        _plan_node.__isset.olap_scan_node = true;

        _table_desc.tableType = TTableType::OLAP_TABLE;
        _thrift_desc_tbl.tableDescriptors.push_back(_table_desc);

        TTupleDescriptor tuple_desc;
        tuple_desc.id = 0;
        _thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);
        tuple_desc.id = 1;
        _thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);

        ASSERT_TRUE(
                DescriptorTbl::create(_obj_pool.get(), _thrift_desc_tbl, &_descriptor_table).ok());
    }

    RuntimeState _runtime_state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile _profile {"olap_scanner_io_context"};
    TPlanNode _plan_node;
    TTableDescriptor _table_desc;
    TDescriptorTable _thrift_desc_tbl;
    DescriptorTbl* _descriptor_table = nullptr;
    std::unique_ptr<ObjectPool> _obj_pool;
};

TEST_F(OlapScannerIOContextTest, ConstructorStoresRangeFileCacheContext) {
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            _obj_pool.get(), _plan_node, 0, *_descriptor_table, 1, TQueryCacheParam {});
    auto local_state =
            pipeline::OlapScanLocalState::create_unique(&_runtime_state, scan_operator.get());

    OlapScanner::Params params;
    params.state = &_runtime_state;
    params.profile = &_profile;
    params.limit = -1;
    params.key_ranges = {};
    params.table_name = "internal.test.tbl(base_index)";
    params.partition_name = "p20260319";

    auto scanner = OlapScanner::create_shared(local_state.get(), std::move(params));

    EXPECT_EQ(scanner->_tablet_reader_params.table_name, "internal.test.tbl(base_index)");
    EXPECT_EQ(scanner->_tablet_reader_params.partition_name, "p20260319");
}

} // namespace doris::vectorized
