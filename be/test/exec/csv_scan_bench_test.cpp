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

#include <vector>

#include "exec/csv_scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "gperftools/profiler.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/logging.h"

namespace doris {

class CsvScanNodeBenchTest : public testing::Test {
public:
    CsvScanNodeBenchTest() {}
    ~CsvScanNodeBenchTest() {}

protected:
    virtual void SetUp() {
        config::mini_load_download_path = "./test_run";

        system("mkdir -p ./test_run/test_db_name/test_label");
        system("pwd");
        system("cp -r ./be/test/query/exec/test_data/csv_scanner ./test_run/.");
        init();
    }
    virtual void TearDown() { system("rm -rf ./test_run"); }

    void init();
    void init_desc_tbl();
    void init_runtime_state();

private:
    ObjectPool _obj_pool;
    TDescriptorTable _t_desc_table;
    DescriptorTbl* _desc_tbl;
    RuntimeState* _state;
    TPlanNode _tnode;
}; // end class CsvScanNodeBenchTest

void CsvScanNodeBenchTest::init() {
    init_desc_tbl();
    init_runtime_state();
}

void CsvScanNodeBenchTest::init_runtime_state() {
    _state = _obj_pool.add(new RuntimeState("2015-04-27 01:01:01"));
    _state->set_desc_tbl(_desc_tbl);
}

void CsvScanNodeBenchTest::init_desc_tbl() {
    // TTableDescriptor
    TTableDescriptor t_table_desc;
    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_table_desc.olapTable.tableName = "test";
    t_table_desc.tableName = "test_table_name";
    t_table_desc.dbName = "test_db_name";
    t_table_desc.__isset.olapTable = true;

    _t_desc_table.tableDescriptors.push_back(t_table_desc);
    _t_desc_table.__isset.tableDescriptors = true;

    // TSlotDescriptor
    std::vector<TSlotDescriptor> slot_descs;
    int offset = 1;
    int i = 0;
    // UserId
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(to_thrift(TYPE_INT));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column0");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(int32_t);
    }
    ++i;
    // column 2
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(to_thrift(TYPE_INT));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column1");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(int32_t);
    }
    ++i;
    // column 3
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(to_thrift(TYPE_INT));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column2");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(int32_t);
    }
    ++i;
    // column 4: varchar
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(to_thrift(TYPE_VARCHAR));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column3");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(StringValue);
    }
    ++i;
    // Date
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(to_thrift(TYPE_DATE));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column4");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(DateTimeValue);
    }
    ++i;
    // DateTime
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(to_thrift(TYPE_DATETIME));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column5");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(DateTimeValue);
    }
    ++i;
    //
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(to_thrift(TYPE_VARCHAR));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("column6");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(StringValue);
    }
    _t_desc_table.__set_slotDescriptors(slot_descs);

    // TTupleDescriptor
    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.id = 0;
    t_tuple_desc.byteSize = offset;
    t_tuple_desc.numNullBytes = 1;
    t_tuple_desc.tableId = 0;
    t_tuple_desc.__isset.tableId = true;
    _t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

    DescriptorTbl::create(&_obj_pool, _t_desc_table, &_desc_tbl);

    // node
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::CSV_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.csv_scan_node.tuple_id = 0;

    _tnode.csv_scan_node.__set_column_separator(",");
    _tnode.csv_scan_node.__set_line_delimiter("\n");

    // column_type_mapping
    std::map<std::string, TColumnType> column_type_map;
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::INT);
        column_type_map["column0"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::INT);
        column_type_map["column1"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::INT);
        column_type_map["column2"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::VARCHAR);
        column_type_map["column3"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::DATE);
        column_type_map["column4"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::DATETIME);
        column_type_map["column5"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::VARCHAR);
        column_type_map["column6"] = column_type;
    }
    _tnode.csv_scan_node.__set_column_type_mapping(column_type_map);

    std::vector<std::string> file_paths;
    // file_paths.push_back("./test_run/csv_scanner/csv_file1");
    // file_paths.push_back("./test_run/csv_scanner/csv_file2");
    file_paths.push_back("/home/ling/tmp/100_wan_line_data");
    _tnode.csv_scan_node.__set_file_paths(file_paths);

    _tnode.csv_scan_node.__set_column_separator("\t");
    _tnode.csv_scan_node.__set_line_delimiter("\n");

    std::vector<std::string> columns;
    columns.push_back("column0");
    columns.push_back("column1");
    columns.push_back("column2");
    columns.push_back("column3");
    columns.push_back("column4");
    columns.push_back("column5");
    columns.push_back("column6");
    _tnode.csv_scan_node.__set_columns(columns);

    _tnode.csv_scan_node.__isset.unspecified_columns = true;
    _tnode.csv_scan_node.__isset.default_values = true;
    _tnode.csv_scan_node.max_filter_ratio = 0.5;
    _tnode.__isset.csv_scan_node = true;
}

TEST_F(CsvScanNodeBenchTest, NormalUse) {
    CsvScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(_state);
    EXPECT_TRUE(status.ok());

    status = scan_node.open(_state);
    EXPECT_TRUE(status.ok());

    bool eos = false;

    while (!eos) {
        // RowBatch row_batch(scan_node._row_descriptor, _state->batch_size());
        RowBatch row_batch(scan_node._row_descriptor, 1024);
        status = scan_node.get_next(_state, &row_batch, &eos);
        EXPECT_TRUE(status.ok());
        // int num = std::min(row_batch.num_rows(), 10);
        int num = row_batch.num_rows();
        // EXPECT_TRUE(num > 0);
    }

    EXPECT_TRUE(scan_node.close(_state).ok());

    {
        std::stringstream ss;
        scan_node.runtime_profile()->pretty_print(&ss);
        LOG(WARNING) << ss.str();
    }
}

} // end namespace doris
