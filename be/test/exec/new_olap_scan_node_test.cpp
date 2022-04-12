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

#include <sstream>

#include "exec/olap_scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/batch_reader_interface.h"
#include "olap/field.h"
#include "olap/olap_configure.h"
#include "olap/olap_reader.h"
#include "olap/session_manager.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris {

//using namespace testing;
//using namespace olap::storage;
//using namespace doris;
//using namespace std;

class TestOlapScanNode : public testing::Test {
public:
    TestOlapScanNode() : _runtime_stat("test") {}

    void SetUp() {
        init_olap();
        init_scan_node();
    }

    void TearDown() {
        StorageEngine::get_instance()->clear();
        SessionManager::get_instance()->delete_session_by_fd(123);

        system("rm -rf ./testrun");
    }

    void init_olap() {
        system("mkdir -p ./testrun");
        system("cp -r ./testdata/case3 ./testrun/.");

        string tables_root_path = "./testrun/case3";
        memcpy(OLAPConfigure::get_instance()->_tables_root_path, tables_root_path.c_str(),
               tables_root_path.size());
        string unused_flag_path = "./testrun/unused_flag";
        memcpy(OLAPConfigure::get_instance()->_unused_flag_path, unused_flag_path.c_str(),
               unused_flag_path.size());

        StorageEngine::get_instance()->_lru_cache = newLRU_cache(10000);

        _tablet_meta = new TabletMeta(
                "./testrun/case3/clickuserid_online_userid_type_planid_unitid_winfoid.hdr");
        _tablet_meta->load();
        tablet = new Tablet(_tablet_meta);
        tablet->load_indices();
        tablet->_root_path_name = "./testrun/case3";

        TableDescription description("fc", "clickuserid_online",
                                     "userid_type_planid_unitid_winfoid");
        StorageEngine::get_instance()->add_table(description, tablet);

        // init session manager
        SessionManager::get_instance()->init();
    }

    void init_scan_node() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.disable_codegen = true;
        ExecEnv* exec_env = new ExecEnv();
        _runtime_stat.init(fragment_id, query_options, "test", exec_env);

        TDescriptorTable t_desc_table;

        // table descriptors
        TTableDescriptor t_table_desc;

        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::OLAP_TABLE;
        t_table_desc.numCols = 0;
        t_table_desc.numClusteringCols = 0;
        t_table_desc.olapTable.tableName = "";
        t_table_desc.tableName = "";
        t_table_desc.dbName = "";
        t_table_desc.__isset.mysqlTable = true;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;
        // TSlotDescriptor
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
            t_slot_desc.__set_colName("userid");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;
        // planid
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
            t_slot_desc.__set_colName("planid");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;
        // winfoid
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
            t_slot_desc.__set_colName("winfoid");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;
        // pv
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
            t_slot_desc.__set_colName("pv");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;
        // pay
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
            t_slot_desc.__set_colName("pay");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }

        t_desc_table.__isset.slotDescriptors = true;
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

        _runtime_stat.set_desc_tbl(_desc_tbl);

        // Node Id
        _tnode.node_id = 0;
        _tnode.node_type = TPlanNodeType::OLAP_SCAN_NODE;
        _tnode.num_children = 0;
        _tnode.limit = -1;
        _tnode.row_tuples.push_back(0);
        _tnode.nullable_tuples.push_back(false);
        _tnode.tuple_ids.push_back(0);
        _tnode.olap_scan_node.tuple_id = 0;
        _tnode.olap_scan_node.key_column_name.push_back("userid");
        _tnode.olap_scan_node.key_column_type.push_back(to_thrift(TYPE_INT));
        _tnode.__isset.olap_scan_node = true;

        {
            TScanRangeParams param;
            TPaloScanRange doris_scan_range;
            TNetworkAddress host;
            host.__set_hostname("host");
            host.__set_port(port);
            doris_scan_range.hosts.push_back(host);
            doris_scan_range.__set_schema_hash("1709394");
            doris_scan_range.__set_version("0");
            // Useless but it is required in TPaloScanRange
            doris_scan_range.__set_version_hash("0");
            config::olap_index_name = "userid_type_planid_unitid_winfoid";
            doris_scan_range.engine_table_name.push_back("clickuserid_online");
            doris_scan_range.__set_db_name("fc");
            param.scan_range.__set_doris_scan_range(doris_scan_range);
            _scan_ranges.push_back(param);
        }
    }

    void read_data(int version, std::vector<string>* data) {
        data->clear();

        int row[21];

        for (int i = 0; i <= version; ++i) {
            std::stringstream ss;
            ss << "./testrun/case3/_fc_dayhour" << i << ".txt";
            fstream f(ss.str());

            while (true) {
                for (int j = 0; j < 21; ++j) {
                    f >> row[j];
                }

                if (f.eof()) {
                    break;
                }

                std::stringstream str;
                str << "[(";
                str << row[0] << " ";
                str << row[2] << " ";
                str << row[4] << " ";
                str << row[18] << " ";
                str << row[20] << ")]";
                data->push_back(str.str());
                VLOG_NOTICE << "Read Row: " << str.str();
            }
        }
    }

private:
    TabletMeta* _tablet_meta;
    Tablet* tablet;

    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtime_stat;
    std::vector<TScanRangeParams> _scan_ranges;
};

TEST_F(TestOlapScanNode, SimpleTest) {
    OlapScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    status = scan_node.open(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(scan_node.set_scan_ranges(_scan_ranges).ok());

    RowBatch row_batch(scan_node._row_descriptor, _runtime_stat.batch_size());
    int num_rows = 0;
    bool eos = false;

    while (!eos) {
        row_batch.reset();
        status = scan_node.get_next(&_runtime_stat, &row_batch, &eos);
        EXPECT_TRUE(status.ok());
        VLOG_CRITICAL << "num_rows: " << row_batch.num_rows();
        num_rows += row_batch.num_rows();
    }

    EXPECT_EQ(num_rows, 1000);
    EXPECT_TRUE(scan_node.close(&_runtime_stat).ok());
}

TEST_F(TestOlapScanNode, MultiColumnSingleVersionTest) {
    _scan_ranges[0].scan_range.doris_scan_range.__set_version("0");
    // Useless but it is required in TPaloScanRange
    _scan_ranges[0].scan_range.doris_scan_range.__set_version_hash("0");
    std::vector<string> data;
    read_data(0, &data);

    OlapScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    status = scan_node.open(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(scan_node.set_scan_ranges(_scan_ranges).ok());

    RowBatch row_batch(scan_node._row_descriptor, _runtime_stat.batch_size());
    int num_rows = 0;
    bool eos = false;
    int data_index = 0;

    while (!eos) {
        row_batch.reset();
        status = scan_node.get_next(&_runtime_stat, &row_batch, &eos);
        EXPECT_TRUE(status.ok());

        for (int i = 0; i < row_batch.num_rows(); ++i) {
            TupleRow* row = row_batch.get_row(i);
            VLOG_NOTICE << "input row: " << print_row(row, scan_node._row_descriptor);
            EXPECT_LT(data_index, data.size());
            EXPECT_EQ(data[data_index], print_row(row, scan_node._row_descriptor));
            ++data_index;
        }

        num_rows += row_batch.num_rows();
    }

    EXPECT_EQ(num_rows, data.size());
    EXPECT_TRUE(scan_node.close(&_runtime_stat).ok());
}

TEST_F(TestOlapScanNode, MultiColumnMultiVersionTest) {
    _scan_ranges[0].scan_range.doris_scan_range.__set_version("9");
    // Useless but it is required in TPaloScanRange
    _scan_ranges[0].scan_range.doris_scan_range.__set_version_hash("0");
    std::vector<string> data;
    read_data(9, &data);

    OlapScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    status = scan_node.open(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(scan_node.set_scan_ranges(_scan_ranges).ok());

    RowBatch row_batch(scan_node._row_descriptor, _runtime_stat.batch_size());
    int num_rows = 0;
    bool eos = false;
    int data_index = 0;

    while (!eos) {
        row_batch.reset();
        status = scan_node.get_next(&_runtime_stat, &row_batch, &eos);
        EXPECT_TRUE(status.ok());

        for (int i = 0; i < row_batch.num_rows(); ++i) {
            TupleRow* row = row_batch.get_row(i);
            VLOG_NOTICE << "input row: " << print_row(row, scan_node._row_descriptor);
            EXPECT_LT(data_index, data.size());
            EXPECT_EQ(data[data_index], print_row(row, scan_node._row_descriptor));
            ++data_index;
        }

        num_rows += row_batch.num_rows();
    }

    EXPECT_EQ(num_rows, data.size());
    EXPECT_TRUE(scan_node.close(&_runtime_stat).ok());
}

} // namespace doris

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
