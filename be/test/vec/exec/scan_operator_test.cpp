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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/QueryCache_types.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "runtime/descriptors.h"

namespace doris::vectorized {
class ScanOperatorTest : public testing::Test {
public:
    void SetUp() override {
        obj_pool = std::make_unique<ObjectPool>();
        // This ScanNode has two tuples.
        // First one is input tuple, second one is output tuple.
        tbl_desc.tableType = TTableType::OLAP_TABLE;

        tuple_desc.id = 0;
        tuple_descs.push_back(tuple_desc);
        tuple_desc.id = 1;
        tuple_descs.push_back(tuple_desc);
        thrift_tbl.tableDescriptors.push_back(tbl_desc);
        thrift_tbl.tupleDescriptors = tuple_descs;
        thrift_tbl.slotDescriptors = slot_descs;
        scalar_type.__set_type(TPrimitiveType::STRING);
        std::ignore = DescriptorTbl::create(obj_pool.get(), thrift_tbl, &descs);
    }

private:
    std::unique_ptr<ObjectPool> obj_pool;
    TTupleDescriptor tuple_desc;
    std::vector<TTupleDescriptor> tuple_descs;
    DescriptorTbl* descs = nullptr;
    TTableDescriptor tbl_desc;
    TScalarType scalar_type;
    TDescriptorTable thrift_tbl;
    std::vector<TSlotDescriptor> slot_descs;
    std::unique_ptr<RuntimeState> state = std::make_unique<RuntimeState>();
};

TEST_F(ScanOperatorTest, adaptive_pipeline_task_serial_read_on_limit) {
    const int parallel_pipeline_task_num = 24;
    TPlanNode tnode;
    tnode.row_tuples.push_back(TTupleId(0));
    tnode.row_tuples.push_back(TTupleId(1));
    std::vector<bool> null_map {false, false};
    tnode.nullable_tuples = null_map;

    // Scan with conjuncts
    TExpr conjunct;
    std::vector<TExpr> conjuncts {conjunct};
    tnode.__set_conjuncts(conjuncts);
    auto scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_pipeline_task_num, TQueryCacheParam {});

    TQueryOptions query_options;
    // enable_adaptive_pipeline_task_serial_read_on_limit is true
    query_options.__set_enable_adaptive_pipeline_task_serial_read_on_limit(true);
    state->set_query_options(query_options);

    std::ignore = scan_operator->init(tnode, state.get());
    // With conjuncts, should_run_serial is false
    ASSERT_EQ(scan_operator->_should_run_serial, false);

    // Scan without conjuncts
    conjuncts.clear();
    tnode.__set_conjuncts(conjuncts);
    // limit 10
    tnode.__set_limit(10);
    scan_operator = std::make_unique<pipeline::OlapScanOperatorX>(
            obj_pool.get(), tnode, 0, *descs, parallel_pipeline_task_num, TQueryCacheParam {});

    // enable_adaptive_pipeline_task_serial_read_on_limit is true
    query_options.__set_enable_adaptive_pipeline_task_serial_read_on_limit(true);
    query_options.__set_adaptive_pipeline_task_serial_read_on_limit(10);
    state->set_query_options(query_options);

    std::ignore = scan_operator->init(tnode, state.get());
    // Without conjuncts, limit 10 <= adaptive_pipeline_task_serial_read_on_limit 10
    ASSERT_EQ(scan_operator->_should_run_serial, true);

    query_options.__set_adaptive_pipeline_task_serial_read_on_limit(9);
    state->set_query_options(query_options);
    std::ignore = scan_operator->init(tnode, state.get());
    // Without conjuncts, limit 10 > adaptive_pipeline_task_serial_read_on_limit 9
    ASSERT_EQ(scan_operator->_should_run_serial, true);

    query_options.__set_enable_adaptive_pipeline_task_serial_read_on_limit(false);
    query_options.__set_adaptive_pipeline_task_serial_read_on_limit(900);
    state->set_query_options(query_options);
    scan_operator->_should_run_serial = false;
    std::ignore = scan_operator->init(tnode, state.get());
    // Without conjuncts, enable_adaptive_pipeline_task_serial_read_on_limit is false
    ASSERT_EQ(scan_operator->_should_run_serial, false);
}
} // namespace doris::vectorized