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

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <vector>

#include "common/logging.h"
#include "exec/csv_scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/row.h"
#include "runtime/bufferpool/reservation_tracker.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_resource_mgr.h"
#include "runtime/tuple_row.h"
#include "util/arrow/row_batch.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/logging.h"

namespace doris {

class ArrowWorkFlowTest : public testing::Test {
public:
    ArrowWorkFlowTest() {}
    ~ArrowWorkFlowTest() {}

protected:
    virtual void SetUp() {
        config::periodic_counter_update_period_ms = 500;
        config::storage_root_path = "./data";

        ASSERT_EQ(system("mkdir -p ./test_run/output/"), 0);
        ASSERT_EQ(system("pwd"), 0);
        ASSERT_EQ(system("cp -r ./be/test/util/test_data/ ./test_run/."), 0);

        init();
    }
    virtual void TearDown() {
        _obj_pool.clear();
        ASSERT_EQ(system("rm -rf ./test_run"), 0);

        delete _state;
    }

    void init();
    void init_desc_tbl();
    void init_runtime_state();

private:
    ObjectPool _obj_pool;
    TDescriptorTable _t_desc_table;
    DescriptorTbl* _desc_tbl = nullptr;
    TPlanNode _tnode;
    ExecEnv* _exec_env = nullptr;
    RuntimeState* _state = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker;
}; // end class ArrowWorkFlowTest

void ArrowWorkFlowTest::init() {
    _exec_env = ExecEnv::GetInstance();
    init_desc_tbl();
    init_runtime_state();
}

void ArrowWorkFlowTest::init_runtime_state() {
    _exec_env->_result_queue_mgr = new ResultQueueMgr();
    _exec_env->_thread_mgr = new ThreadResourceMgr();
    _exec_env->_buffer_reservation = new ReservationTracker();
    TQueryOptions query_options;
    query_options.batch_size = 1024;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;
    _state = new RuntimeState(query_id, query_options, TQueryGlobals(), _exec_env);
    _state->init_instance_mem_tracker();
    _mem_tracker =
            MemTracker::CreateTracker(-1, "ArrowWorkFlowTest", _state->instance_mem_tracker());
    _state->set_desc_tbl(_desc_tbl);
    _state->_load_dir = "./test_run/output/";
    _state->init_mem_trackers(TUniqueId());
}

void ArrowWorkFlowTest::init_desc_tbl() {
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
    // int_column
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::INT));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("int_column");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(int32_t);
    }
    ++i;
    // date_column
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::DATE));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("date_column");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(DateTimeValue);
    }
    ++i;
    // decimalv2_column
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        TTypeDesc ttype = gen_type_desc(TPrimitiveType::DECIMALV2);
        ttype.types[0].scalar_type.__set_precision(9);
        ttype.types[0].scalar_type.__set_scale(3);
        t_slot_desc.__set_slotType(ttype);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("decimalv2_column");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(DecimalV2Value);
    }
    ++i;
    // fix_len_string_column
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        TTypeDesc ttype = gen_type_desc(TPrimitiveType::CHAR);
        ttype.types[0].scalar_type.__set_len(5);
        t_slot_desc.__set_slotType(ttype);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("fix_len_string_column");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(StringValue);
    }
    ++i;
    // largeint
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        TTypeDesc ttype = gen_type_desc(TPrimitiveType::LARGEINT);
        t_slot_desc.__set_slotType(ttype);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("largeint_column");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(LargeIntVal);
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

    std::vector<TTupleId> row_tids;
    row_tids.push_back(0);

    std::vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);

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
        column_type_map["int_column"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::DATE);
        column_type_map["date_column"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::DECIMALV2);
        column_type.__set_precision(9);
        column_type.__set_scale(3);
        column_type_map["decimalv2_column"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::CHAR);
        column_type.__set_len(5);
        column_type_map["fix_len_string_column"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::LARGEINT);
        column_type_map["largeint_column"] = column_type;
    }
    _tnode.csv_scan_node.__set_column_type_mapping(column_type_map);

    std::vector<std::string> columns;
    columns.push_back("int_column");
    columns.push_back("date_column");
    columns.push_back("decimalv2_column");
    columns.push_back("fix_len_string_column");
    columns.push_back("largeint_column");
    _tnode.csv_scan_node.__set_columns(columns);

    _tnode.csv_scan_node.__isset.unspecified_columns = true;
    _tnode.csv_scan_node.__isset.default_values = true;
    _tnode.csv_scan_node.max_filter_ratio = 0.5;
    _tnode.__isset.csv_scan_node = true;
}

TEST_F(ArrowWorkFlowTest, NormalUse) {
    std::vector<std::string> file_paths;
    file_paths.push_back("./test_run/test_data/csv_data");
    _tnode.csv_scan_node.__set_file_paths(file_paths);

    CsvScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node.init(_tnode);
    Status status = scan_node.prepare(_state);
    ASSERT_TRUE(status.ok());

    status = scan_node.open(_state);
    ASSERT_TRUE(status.ok());

    auto mem_tracker = std::make_shared<MemTracker>(-1);
    RowBatch row_batch(scan_node._row_descriptor, _state->batch_size(), mem_tracker.get());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        // int num = std::min(row_batch.num_rows(), 10);
        int num = row_batch.num_rows();
        ASSERT_EQ(6, num);
        std::shared_ptr<arrow::Schema> schema;
        status = convert_to_arrow_schema(scan_node._row_descriptor, &schema);
        ASSERT_TRUE(status.ok());
        std::shared_ptr<arrow::RecordBatch> record_batch;
        status = convert_to_arrow_batch(row_batch, schema, arrow::default_memory_pool(),
                                        &record_batch);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(6, record_batch->num_rows());
        ASSERT_EQ(5, record_batch->num_columns());
        std::string result;
        status = serialize_record_batch(*record_batch, &result);
        ASSERT_TRUE(status.ok());
        size_t len = result.length();
        ASSERT_TRUE(len > 0);
    }

    ASSERT_TRUE(scan_node.close(_state).ok());
}

} // end namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
