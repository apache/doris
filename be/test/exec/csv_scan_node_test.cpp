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

#include "exec/csv_scan_node.h"

#include <gtest/gtest.h>

#include <vector>

#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/logging.h"

namespace doris {

class CsvScanNodeTest : public testing::Test {
public:
    CsvScanNodeTest() {}
    ~CsvScanNodeTest() {}

protected:
    virtual void SetUp() {
        config::periodic_counter_update_period_ms = 500;
        config::storage_root_path = "./data";
        _env.reset(new ExecEnv());

        system("mkdir -p ./test_run/output/");
        system("pwd");
        system("cp -r ./be/test/exec/test_data/csv_scan_node ./test_run/.");
        init();
    }
    virtual void TearDown() {
        _obj_pool.clear();
        _env.reset();
        // system("rm -rf ./test_run");
    }

    void init();
    void init_desc_tbl();
    void init_runtime_state();

private:
    ObjectPool _obj_pool;
    TDescriptorTable _t_desc_table;
    DescriptorTbl* _desc_tbl;
    TPlanNode _tnode;
    std::unique_ptr<ExecEnv> _env;
    RuntimeState* _state;
}; // end class CsvScanNodeTest

void CsvScanNodeTest::init() {
    _env->init_for_tests();
    init_desc_tbl();
    init_runtime_state();
}

void CsvScanNodeTest::init_runtime_state() {
    _state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), "", _env.get()));
    _state->set_desc_tbl(_desc_tbl);
    _state->_load_dir = "./test_run/output/";
    _state->init_mem_trackers(TUniqueId());
}

void CsvScanNodeTest::init_desc_tbl() {
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
    // decimal_column
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        TTypeDesc ttype = gen_type_desc(TPrimitiveType::DECIMALV2);
        ttype.types[0].scalar_type.__set_precision(10);
        ttype.types[0].scalar_type.__set_scale(5);
        t_slot_desc.__set_slotType(ttype);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("decimal_column");

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(DecimalValueV2);
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
        column_type_map["int_column"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::DECIMALV2);
        column_type.__set_precision(10);
        column_type.__set_scale(5);
        column_type_map["decimal_column"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::DATE);
        column_type_map["date_column"] = column_type;
    }
    {
        TColumnType column_type;
        column_type.__set_type(TPrimitiveType::BIGINT);
        column_type.__set_len(5);
        column_type_map["fix_len_string_column"] = column_type;
    }
    _tnode.csv_scan_node.__set_column_type_mapping(column_type_map);

    std::vector<std::string> columns;
    columns.push_back("int_column");
    columns.push_back("date_column");
    columns.push_back("decimal_column");
    columns.push_back("fix_len_string_column");
    _tnode.csv_scan_node.__set_columns(columns);

    _tnode.csv_scan_node.__isset.unspecified_columns = true;
    _tnode.csv_scan_node.__isset.default_values = true;
    _tnode.csv_scan_node.max_filter_ratio = 0.5;
    _tnode.__isset.csv_scan_node = true;
}

TEST_F(CsvScanNodeTest, NormalUse) {
    std::vector<std::string> file_paths;
    file_paths.push_back("./test_run/csv_scan_node/normal_use");
    _tnode.csv_scan_node.__set_file_paths(file_paths);

    CsvScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(_state);
    ASSERT_TRUE(status.ok());

    status = scan_node.open(_state);
    ASSERT_TRUE(status.ok());

    RowBatch row_batch(scan_node._row_descriptor, _state->batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        // int num = std::min(row_batch.num_rows(), 10);
        int num = row_batch.num_rows();
        std::cout << "num: " << num << std::endl;
        ASSERT_EQ(num, 6);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            // LOG(WARNING) << "input row[" << i << "]: " << print_row(row, scan_node._row_descriptor);
            std::cout << "input row: " << print_row(row, scan_node._row_descriptor) << std::endl;

            if (i == 0) {
                ASSERT_EQ(std::string("[(1 -12345.67891 2015-04-20 abc\0\0)]", 35),
                          print_row(row, scan_node._row_descriptor));
            }
        }
    }

    ASSERT_TRUE(scan_node.close(_state).ok());
}

TEST_F(CsvScanNodeTest, continuousDelim) {
    std::vector<std::string> file_paths;
    file_paths.push_back("./test_run/csv_scan_node/continuous_delim");
    _tnode.csv_scan_node.__set_file_paths(file_paths);

    CsvScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(_state);
    ASSERT_TRUE(status.ok());

    status = scan_node.open(_state);
    ASSERT_TRUE(status.ok());

    RowBatch row_batch(scan_node._row_descriptor, _state->batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        // int num = std::min(row_batch.num_rows(), 10);
        int num = row_batch.num_rows();
        std::cout << "num: " << num << std::endl;
        ASSERT_EQ(num, 1);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            // LOG(WARNING) << "input row[" << i << "]: " << print_row(row, scan_node._row_descriptor);
            std::cout << "input row: " << print_row(row, scan_node._row_descriptor) << std::endl;

            if (i == 0) {
                ASSERT_EQ(std::string("[(1 -12345.67891 2015-04-20 \0\0\0\0\0)]", 35),
                          print_row(row, scan_node._row_descriptor));
            }
        }
    }

    ASSERT_TRUE(scan_node.close(_state).ok());
}

TEST_F(CsvScanNodeTest, wrong_decimal_format_test) {
    std::vector<std::string> file_paths;
    file_paths.push_back("./test_run/csv_scan_node/wrong_decimal_format");
    _tnode.csv_scan_node.__set_file_paths(file_paths);

    CsvScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(_state);
    ASSERT_TRUE(status.ok());

    status = scan_node.open(_state);
    ASSERT_TRUE(status.ok());

    RowBatch row_batch(scan_node._row_descriptor, _state->batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        // int num = std::min(row_batch.num_rows(), 10);
        int num = row_batch.num_rows();
        std::cout << "num: " << num << std::endl;
        ASSERT_EQ(0, num);
    }

    // Failed because reach max_filter_ratio
    ASSERT_TRUE(!scan_node.close(_state).ok());
}

TEST_F(CsvScanNodeTest, fill_fix_len_stringi_test) {
    std::vector<std::string> file_paths;
    file_paths.push_back("./test_run/csv_scan_node/fill_string_len");
    _tnode.csv_scan_node.__set_file_paths(file_paths);

    CsvScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(_state);
    ASSERT_TRUE(status.ok());

    status = scan_node.open(_state);
    ASSERT_TRUE(status.ok());

    RowBatch row_batch(scan_node._row_descriptor, _state->batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        // int num = std::min(row_batch.num_rows(), 10);
        int num = row_batch.num_rows();
        std::cout << "num: " << num << std::endl;
        ASSERT_TRUE(num > 0);

        // 1,2015-04-20,12345.67891,abcdefg
        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            LOG(WARNING) << "input row[" << i << "]: " << print_row(row, scan_node._row_descriptor);
            std::cout << "input row: " << print_row(row, scan_node._row_descriptor) << std::endl;

            if (i == 0) {
                ASSERT_EQ(std::string("[(1 12345.67891 2015-04-20 ab\0\0\0)]", 34),
                          print_row(row, scan_node._row_descriptor));
                Tuple* tuple = row->get_tuple(0);
                StringValue* str_slot =
                        tuple->get_string_slot(_t_desc_table.slotDescriptors[3].byteOffset);
                std::cout << "str_slot len: " << str_slot->len << std::endl;
                ASSERT_EQ(5, str_slot->len);
            }
        }
    }

    ASSERT_TRUE(scan_node.close(_state).ok());
}

TEST_F(CsvScanNodeTest, wrong_fix_len_string_format_test) {
    std::vector<std::string> file_paths;
    file_paths.push_back("./test_run/csv_scan_node/wrong_fix_len_string");
    _tnode.csv_scan_node.__set_file_paths(file_paths);

    CsvScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(_state);
    ASSERT_TRUE(status.ok());

    status = scan_node.open(_state);
    ASSERT_TRUE(status.ok());

    RowBatch row_batch(scan_node._row_descriptor, _state->batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());
        // int num = std::min(row_batch.num_rows(), 10);
        int num = row_batch.num_rows();
        std::cout << "num: " << num << std::endl;
        ASSERT_EQ(0, num);
    }

    // Failed because reach max_filter_ratio
    ASSERT_TRUE(!scan_node.close(_state).ok());
}

// To be added test case
// 1. String import
// 2. Do not specify columns with default values
// 3. If there is a column in the file but not in the table, the column is skipped in the import command
// 4. max_filter_ratio

} // end namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::config::read_size = 8388608;
    doris::config::min_buffer_size = 1024;

    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    doris::CpuInfo::init();
    doris::DiskInfo::init();

    return RUN_ALL_TESTS();
}
