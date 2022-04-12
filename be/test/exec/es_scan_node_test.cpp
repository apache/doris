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

#include "exec/es_scan_node.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

using std::vector;

namespace doris {

// mock
class EsScanNodeTest : public testing::Test {
public:
    EsScanNodeTest() : _runtime_state(TQueryGlobals()) {
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
        TDescriptorTable t_desc_table;

        // table descriptors
        TTableDescriptor t_table_desc;

        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::ES_TABLE;
        t_table_desc.numCols = 0;
        t_table_desc.numClusteringCols = 0;
        t_table_desc.__isset.esTable = true;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // id
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_slotType(TypeDescriptor(TYPE_INT).to_thrift());
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int);
        }

        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.__isset.slotDescriptors = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);
        _runtime_state.set_desc_tbl(_desc_tbl);

        // Node Id
        _tnode.node_id = 0;
        _tnode.node_type = TPlanNodeType::SCHEMA_SCAN_NODE;
        _tnode.num_children = 0;
        _tnode.limit = -1;
        _tnode.row_tuples.push_back(0);
        _tnode.nullable_tuples.push_back(false);
        _tnode.es_scan_node.tuple_id = 0;
        std::map<std::string, std::string> properties;
        _tnode.es_scan_node.__set_properties(properties);
        _tnode.__isset.es_scan_node = true;
    }

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtime_state;
};

TEST_F(EsScanNodeTest, normal_use) {
    EsScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtime_state);
    EXPECT_TRUE(status.ok());
    TEsScanRange es_scan_range;
    es_scan_range.__set_index("index1");
    es_scan_range.__set_type("docs");
    es_scan_range.__set_shard_id(0);
    TNetworkAddress es_host;
    es_host.__set_hostname("host");
    es_host.__set_port(8200);
    std::vector<TNetworkAddress> es_hosts;
    es_hosts.push_back(es_host);
    es_scan_range.__set_es_hosts(es_hosts);
    TScanRange scan_range;
    scan_range.__set_es_scan_range(es_scan_range);
    TScanRangeParams scan_range_params;
    scan_range_params.__set_scan_range(scan_range);
    std::vector<TScanRangeParams> scan_ranges;
    scan_ranges.push_back(scan_range_params);

    status = scan_node.set_scan_ranges(scan_ranges);
    EXPECT_TRUE(status.ok());
    std::stringstream out;
    scan_node.debug_string(1, &out);
    LOG(WARNING) << out.str();

    status = scan_node.open(&_runtime_state);
    EXPECT_TRUE(status.ok());
    RowBatch row_batch(scan_node._row_descriptor, _runtime_state.batch_size());
    bool eos = false;
    status = scan_node.get_next(&_runtime_state, &row_batch, &eos);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(2, row_batch.num_rows());
    EXPECT_TRUE(eos);

    status = scan_node.close(&_runtime_state);
    EXPECT_TRUE(status.ok());
}

} // namespace doris
