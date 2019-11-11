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

#include "exec/olap_scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/command_executor.h"
#include "olap/field.h"
#include "olap/olap_reader.h"
#include "olap/olap_main.cpp"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/debug_util.h"
#include "util/logging.h"
#include "util/file_utils.h"

using namespace testing;
using namespace doris;
using namespace std;

namespace doris {

void set_up() {
    config::storage_root_path = "./test_run/data_test";
    system("rm -rf ./test_run && mkdir -p ./test_run");
    FileUtils::create_dir(config::storage_root_path);
    touch_all_singleton();    
}

void tear_down() {
    system("rm -rf ./test_run");
    FileUtils::remove_all(string(getenv("DORIS_HOME")) + UNUSED_PREFIX);
}

void set_default_create_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = 10003;
    request->__set_version(1);
    request->__set_version_hash(0);
    request->tablet_schema.schema_hash = 1508825676;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k1;
    k1.column_name = "k1";
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.column_type.type = TPrimitiveType::VARCHAR;
    k3.column_type.__set_len(64);
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.column_type.type = TPrimitiveType::DATE;
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.column_type.type = TPrimitiveType::DATETIME;
    request->tablet_schema.columns.push_back(k5);

    TColumn k6;
    k6.column_name = "k6";
    k6.column_type.type = TPrimitiveType::DECIMAL;
    k6.column_type.__set_precision(6);
    k6.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k6);

    TColumn k7;
    k7.column_name = "k7";
    k7.column_type.type = TPrimitiveType::SMALLINT;
    k7.default_value = "0";
    request->tablet_schema.columns.push_back(k7);

    TColumn k8;
    k8.column_name = "k8";
    k8.column_type.type = TPrimitiveType::CHAR;
    k8.column_type.__set_len(16);
    k8.default_value = "char";
    request->tablet_schema.columns.push_back(k8);

    TColumn v;
    v.column_name = "v";
    v.column_type.type = TPrimitiveType::BIGINT;
    v.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v);
}

// SQL for generate data(./be/test/olap/test_data/all_types_1000):
//
// create tablet delete_test_row (k1 tinyint, k2 int, k3 varchar(64), 
// k4 date, k5 datetime, k6 decimal(6,3), k7 smallint default "0", 
// k8 char(16) default "char", v bigint sum) engine=olap distributed by 
// random buckets 1 properties ("storage_type" = "row");
//
// load label label1 (data infile 
// ("hdfs://host:port/dir") 
// into tablet `delete_test_row` (k1,k2,v,k3,k4,k5,k6));
void set_default_push_request(TPushReq* request) {
    request->tablet_id = 10003;
    request->schema_hash = 1508825676;
    request->__set_version(2);
    request->__set_version_hash(1);
    request->timeout = 86400;
    request->push_type = TPushType::LOAD;
    request->__set_http_file_path("./be/test/olap/test_data/all_types_1000");
}

class TestOLAPReaderRow : public testing::Test {
public:
    TestOLAPReaderRow() : _runtime_stat("test") { 
        _profile = _obj_pool.add(new RuntimeProfile(&_obj_pool, "OlapScanner"));
        OLAPReader::init_profile(_profile);
    }
    
    void SetUp() {
        init_olap();
    }

    void TearDown() {
        // Remove all dir.
        StorageEngine::get_instance()->drop_tablet(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        while (0 == access(_tablet_path.c_str(), F_OK)) {
            sleep(1);
        }
        ASSERT_TRUE(, FileUtils::remove_all(config::storage_root_path).ok());
    }

    void init_olap() {
        // Create local data dir for StorageEngine.
        config::storage_root_path = "./test_run/row_tablet";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());

        // 1. Prepare for query split key.
        // create base tablet
        OLAPStatus res = OLAP_SUCCESS;
        set_default_create_tablet_request(&_create_tablet);
        CommandExecutor command_executor = CommandExecutor();
        res = command_executor.create_tablet(_create_tablet);
        ASSERT_EQ(OLAP_SUCCESS, res);
        TabletSharedPtr tablet = command_executor.get_tablet(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        ASSERT_TRUE(tablet.get() != NULL);
        _tablet_path = tablet->tablet_path();

        // push data
        set_default_push_request(&_push_req);
        std::vector<TTabletInfo> tablets_info;
        res = command_executor.push(_push_req, &tablets_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        ASSERT_EQ(1, tablets_info.size());
    }

    void init_scan_node() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.disable_codegen = true;
        //ExecEnv* exec_env = new ExecEnv();
        //_runtime_stat.init(fragment_id, query_options, "test", exec_env);

        TDescriptorTable t_desc_tablet;

        // tablet descriptors
        TTableDescriptor t_tablet_desc;

        t_tablet_desc.id = 0;
        t_tablet_desc.tableType = TTableType::OLAP_TABLE;
        t_tablet_desc.numCols = 0;
        t_tablet_desc.numClusteringCols = 0;
        t_tablet_desc.olapTable.tableName = "";
        t_tablet_desc.tableName = "";
        t_tablet_desc.dbName = "";
        t_tablet_desc.__isset.mysqlTable = true;
        t_desc_tablet.tableDescriptors.push_back(t_tablet_desc);
        t_desc_tablet.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // k1
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::TINYINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k1");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int8_t);
        }
        ++i;
        // k2
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
            t_slot_desc.__set_colName("k2");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;       
        // k3
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::VARCHAR));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k3");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }
        ++i;       
        // k4
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
            t_slot_desc.__set_colName("k4");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(DateTimeValue);
        }
        ++i;
        // k5
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::DATETIME));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k5");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(DateTimeValue);
        }
        ++i;
        // k6
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::DECIMAL));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k6");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(decimal12_t);
        }
        ++i;
        // k7
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::SMALLINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k7");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int16_t);
        }
        ++i;       
        // k8
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::CHAR));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k8");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }
        ++i;      
        // v
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::BIGINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("v");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int64_t);
        }

        t_desc_tablet.__isset.slotDescriptors = true;
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_tablet.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_tablet, &_desc_tbl);
    }

    void init_scan_node_k1_v() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.disable_codegen = true;
        //ExecEnv* exec_env = new ExecEnv();
        //_runtime_stat.init(fragment_id, query_options, "test", exec_env);

        TDescriptorTable t_desc_tablet;

        // tablet descriptors
        TTableDescriptor t_tablet_desc;

        t_tablet_desc.id = 0;
        t_tablet_desc.tableType = TTableType::OLAP_TABLE;
        t_tablet_desc.numCols = 0;
        t_tablet_desc.numClusteringCols = 0;
        t_tablet_desc.olapTable.tableName = "";
        t_tablet_desc.tableName = "";
        t_tablet_desc.dbName = "";
        t_tablet_desc.__isset.mysqlTable = true;
        t_desc_tablet.tableDescriptors.push_back(t_tablet_desc);
        t_desc_tablet.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // k1
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::TINYINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k1");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int8_t);
        }
        ++i;    
        // v
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::BIGINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("v");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int64_t);
        }

        t_desc_tablet.__isset.slotDescriptors = true;
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_tablet.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_tablet, &_desc_tbl);
    }

private:
    TCreateTabletReq _create_tablet;
    std::string _tablet_path;
    TPushReq _push_req;

    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtime_stat;
    vector<TScanRangeParams> _scan_ranges;
    RuntimeProfile* _profile;
};

TEST_F(TestOLAPReaderRow, next_tuple_with_key_range) {
    init_scan_node();
    
    TFetchRequest fetch_reques;

    fetch_reques.__set_aggregation(false);
    fetch_reques.__set_schema_hash(1508825676);
    fetch_reques.__set_version(_push_req.version);
    fetch_reques.__set_version_hash(_push_req.version_hash);
    fetch_reques.__set_tablet_id(10003);

    TFetchStartKey start_key;
    start_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_start_key(std::vector<TFetchStartKey>(1, start_key));
    fetch_reques.__set_range("ge");

    TFetchEndKey end_key;
    end_key.__set_key(std::vector<std::string>(1, "100"));
    fetch_reques.__set_end_key(std::vector<TFetchEndKey>(1, end_key));
    fetch_reques.__set_end_range("le");

    std::vector<std::string> field_vec;
    field_vec.push_back("k1");
    field_vec.push_back("k2");
    field_vec.push_back("k3");
    field_vec.push_back("k4");
    field_vec.push_back("k5");
    field_vec.push_back("k6");
    field_vec.push_back("k7");
    field_vec.push_back("k8");
    field_vec.push_back("v");
    fetch_reques.__set_field(field_vec);

    TupleDescriptor *tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    OLAPReader olap_reader(*tuple_desc);
    ASSERT_TRUE(olap_reader.init(fetch_reques, NULL, _profile).ok());

    char tuple_buf[1024]; 
    bzero(tuple_buf, 1024);
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);
    bool eof;
    int64_t raw_rows_read = 0;
    ASSERT_TRUE(olap_reader.next_tuple(tuple, &raw_rows_read, &eof).ok());

    ASSERT_EQ(0, *reinterpret_cast<const int8_t*>
            (tuple->get_slot(tuple_desc->slots()[0]->tuple_offset())));
    ASSERT_EQ(0, *reinterpret_cast<const int32_t*>
            (tuple->get_slot(tuple_desc->slots()[1]->tuple_offset())));
    {
        StringValue *slot = reinterpret_cast<StringValue *>(
                tuple->get_slot(tuple_desc->slots()[2]->tuple_offset()));   
        ASSERT_STREQ("26a91490-1851-4001-a9ad-6ef0cf2c3aa4", 
                std::string(slot->ptr, slot->len).c_str());
    }
    {
        DateTimeValue *slot = reinterpret_cast<DateTimeValue *>(
                tuple->get_slot(tuple_desc->slots()[3]->tuple_offset())); 
        ASSERT_STREQ("2014-04-27", slot->debug_string().c_str());
    }
    {
        DateTimeValue *slot = reinterpret_cast<DateTimeValue *>(
                tuple->get_slot(tuple_desc->slots()[4]->tuple_offset())); 
        ASSERT_STREQ("2014-04-27 22:23:20", slot->debug_string().c_str());
    }
    {
        DecimalValue *slot = reinterpret_cast<DecimalValue *>(
                tuple->get_slot(tuple_desc->slots()[5]->tuple_offset())); 
        ASSERT_STREQ("978.371", slot->to_string().c_str());
    }
    ASSERT_EQ(0, *reinterpret_cast<const int16_t*>
            (tuple->get_slot(tuple_desc->slots()[6]->tuple_offset())));
    {
        StringValue *slot = reinterpret_cast<StringValue *>(
                tuple->get_slot(tuple_desc->slots()[7]->tuple_offset()));   
        ASSERT_STREQ("char", std::string(slot->ptr, slot->len).c_str());
    }
    ASSERT_EQ(0, *reinterpret_cast<const int64_t*>
            (tuple->get_slot(tuple_desc->slots()[8]->tuple_offset())));
}

TEST_F(TestOLAPReaderRow, next_tuple_with_where_condition) {
    init_scan_node();
    
    TFetchRequest fetch_reques;

    fetch_reques.__set_aggregation(false);
    fetch_reques.__set_schema_hash(1508825676);
    fetch_reques.__set_version(_push_req.version);
    fetch_reques.__set_version_hash(_push_req.version_hash);
    fetch_reques.__set_tablet_id(10003);

    TCondition condition;
    condition.column_name = "k2";
    condition.condition_op = "<<";
    condition.condition_values.push_back("0");
    fetch_reques.where.push_back(condition);

    TFetchStartKey start_key;
    start_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_start_key(std::vector<TFetchStartKey>(1, start_key));
    fetch_reques.__set_range("ge");

    TFetchEndKey end_key;
    end_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_end_key(std::vector<TFetchEndKey>(1, end_key));
    fetch_reques.__set_end_range("le");

    std::vector<std::string> field_vec;
    field_vec.push_back("k1");
    field_vec.push_back("k2");
    field_vec.push_back("k3");
    field_vec.push_back("k4");
    field_vec.push_back("k5");
    field_vec.push_back("k6");
    field_vec.push_back("k7");
    field_vec.push_back("k8");
    field_vec.push_back("v");
    fetch_reques.__set_field(field_vec);

    TupleDescriptor *tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    OLAPReader olap_reader(*tuple_desc);
    ASSERT_TRUE(olap_reader.init(fetch_reques, NULL, _profile).ok());

    char tuple_buf[1024]; 
    bzero(tuple_buf, 1024);
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);
    bool eof;
    int64_t raw_rows_read = 0;
    ASSERT_TRUE(olap_reader.next_tuple(tuple, &raw_rows_read, &eof).ok());
    ASSERT_TRUE(eof);
}


TEST_F(TestOLAPReaderRow, next_tuple_without_aggregation) {
    init_scan_node_k1_v();
    
    TFetchRequest fetch_reques;

    fetch_reques.__set_aggregation(false);
    fetch_reques.__set_schema_hash(1508825676);
    fetch_reques.__set_version(_push_req.version);
    fetch_reques.__set_version_hash(_push_req.version_hash);
    fetch_reques.__set_tablet_id(10003);

    TFetchStartKey start_key;
    start_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_start_key(std::vector<TFetchStartKey>(1, start_key));
    fetch_reques.__set_range("ge");

    TFetchEndKey end_key;
    end_key.__set_key(std::vector<std::string>(1, "100"));
    fetch_reques.__set_end_key(std::vector<TFetchEndKey>(1, end_key));
    fetch_reques.__set_end_range("le");

    std::vector<std::string> field_vec;
    field_vec.push_back("k1");
    field_vec.push_back("v");
    fetch_reques.__set_field(field_vec);

    TupleDescriptor *tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    OLAPReader olap_reader(*tuple_desc);
    ASSERT_TRUE(olap_reader.init(fetch_reques, NULL, _profile).ok());

    char tuple_buf[1024]; 
    bzero(tuple_buf, 1024);
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);
    bool eof;
    int64_t raw_rows_read = 0;
    ASSERT_TRUE(olap_reader.next_tuple(tuple, &raw_rows_read, &eof).ok());

    ASSERT_EQ(0, *reinterpret_cast<const int8_t*>
            (tuple->get_slot(tuple_desc->slots()[0]->tuple_offset())));
    ASSERT_EQ(0, *reinterpret_cast<const int64_t*>
            (tuple->get_slot(tuple_desc->slots()[1]->tuple_offset())));
}

TEST_F(TestOLAPReaderRow, next_tuple_with_aggregation) {
    init_scan_node_k1_v();
    
    TFetchRequest fetch_reques;

    fetch_reques.__set_aggregation(true);
    fetch_reques.__set_schema_hash(1508825676);
    fetch_reques.__set_version(_push_req.version);
    fetch_reques.__set_version_hash(_push_req.version_hash);
    fetch_reques.__set_tablet_id(10003);

    TFetchStartKey start_key;
    start_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_start_key(std::vector<TFetchStartKey>(1, start_key));
    fetch_reques.__set_range("ge");

    TFetchEndKey end_key;
    end_key.__set_key(std::vector<std::string>(1, "100"));
    fetch_reques.__set_end_key(std::vector<TFetchEndKey>(1, end_key));
    fetch_reques.__set_end_range("le");

    std::vector<std::string> field_vec;
    field_vec.push_back("k1");
    field_vec.push_back("v");
    fetch_reques.__set_field(field_vec);

    TupleDescriptor *tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    OLAPReader olap_reader(*tuple_desc);
    ASSERT_TRUE(olap_reader.init(fetch_reques, NULL, _profile).ok());

    char tuple_buf[1024]; 
    bzero(tuple_buf, 1024);
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);
    bool eof;
    int64_t raw_rows_read = 0;
    ASSERT_TRUE(olap_reader.next_tuple(tuple, &raw_rows_read, &eof).ok());

    ASSERT_EQ(0, *reinterpret_cast<const int8_t*>
            (tuple->get_slot(tuple_desc->slots()[0]->tuple_offset())));
    ASSERT_EQ(153600, *reinterpret_cast<const int64_t*>
            (tuple->get_slot(tuple_desc->slots()[1]->tuple_offset())));
}

class TestOLAPReaderColumn : public testing::Test {
public:
    TestOLAPReaderColumn() : _runtime_stat("test") { 
        _profile = _obj_pool.add(new RuntimeProfile(&_obj_pool, "OlapScanner"));
        OLAPReader::init_profile(_profile);

    }
    
    void SetUp() {
        init_olap();
    }

    void TearDown() {
        // Remove all dir.
        StorageEngine::get_instance()->drop_tablet(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        while (0 == access(_tablet_path.c_str(), F_OK)) {
            sleep(1);
        }
        ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path));
    }

    void init_olap() {
        // Create local data dir for StorageEngine.
        config::storage_root_path = "./test_run/column_tablet";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());

        // 1. Prepare for query split key.
        // create base tablet
        OLAPStatus res = OLAP_SUCCESS;
        set_default_create_tablet_request(&_create_tablet);
        _create_tablet.tablet_schema.storage_type = TStorageType::COLUMN;
        CommandExecutor command_executor = CommandExecutor();
        res = command_executor.create_tablet(_create_tablet);
        ASSERT_EQ(OLAP_SUCCESS, res);
        TabletSharedPtr tablet = command_executor.get_tablet(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        ASSERT_TRUE(tablet.get() != NULL);
        _tablet_path = tablet->tablet_path();

        // push data
        set_default_push_request(&_push_req);
        std::vector<TTabletInfo> tablets_info;
        res = command_executor.push(_push_req, &tablets_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        ASSERT_EQ(1, tablets_info.size());
    }

    void init_scan_node() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.disable_codegen = true;
        //ExecEnv* exec_env = new ExecEnv();
        //_runtime_stat.init(fragment_id, query_options, "test", exec_env);

        TDescriptorTable t_desc_tablet;

        // tablet descriptors
        TTableDescriptor t_tablet_desc;

        t_tablet_desc.id = 0;
        t_tablet_desc.tableType = TTableType::OLAP_TABLE;
        t_tablet_desc.numCols = 0;
        t_tablet_desc.numClusteringCols = 0;
        t_tablet_desc.olapTable.tableName = "";
        t_tablet_desc.tableName = "";
        t_tablet_desc.dbName = "";
        t_tablet_desc.__isset.mysqlTable = true;
        t_desc_tablet.tableDescriptors.push_back(t_tablet_desc);
        t_desc_tablet.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // k1
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::TINYINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k1");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int8_t);
        }
        ++i;
        // k2
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
            t_slot_desc.__set_colName("k2");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;       
        // k3
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::VARCHAR));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k3");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }
        ++i;       
        // k4
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
            t_slot_desc.__set_colName("k4");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(DateTimeValue);
        }
        ++i;
        // k5
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::DATETIME));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k5");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(DateTimeValue);
        }
        ++i;
        // k6
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::DECIMAL));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k6");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(decimal12_t);
        }
        ++i;
        // k7
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::SMALLINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k7");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int16_t);
        }
        ++i;       
        // k8
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::CHAR));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k8");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }
        ++i;      
        // v
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::BIGINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("v");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int64_t);
        }

        t_desc_tablet.__isset.slotDescriptors = true;
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_tablet.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_tablet, &_desc_tbl);
    }

    
    void init_scan_node_k1_v() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.disable_codegen = true;
        //ExecEnv* exec_env = new ExecEnv();
        //_runtime_stat.init(fragment_id, query_options, "test", exec_env);

        TDescriptorTable t_desc_tablet;

        // tablet descriptors
        TTableDescriptor t_tablet_desc;

        t_tablet_desc.id = 0;
        t_tablet_desc.tableType = TTableType::OLAP_TABLE;
        t_tablet_desc.numCols = 0;
        t_tablet_desc.numClusteringCols = 0;
        t_tablet_desc.olapTable.tableName = "";
        t_tablet_desc.tableName = "";
        t_tablet_desc.dbName = "";
        t_tablet_desc.__isset.mysqlTable = true;
        t_desc_tablet.tableDescriptors.push_back(t_tablet_desc);
        t_desc_tablet.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // k1
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::TINYINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k1");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int8_t);
        }
        ++i;    
        // v
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::BIGINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("v");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int64_t);
        }

        t_desc_tablet.__isset.slotDescriptors = true;
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_tablet.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_tablet, &_desc_tbl);
    }

private:
    TCreateTabletReq _create_tablet;
    std::string _tablet_path;
    TPushReq _push_req;

    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtime_stat;
    vector<TScanRangeParams> _scan_ranges;
    RuntimeProfile* _profile;
};


TEST_F(TestOLAPReaderColumn, next_tuple) {
    init_scan_node();

    TFetchRequest fetch_reques;

    fetch_reques.__set_schema_hash(1508825676);
    fetch_reques.__set_version(_push_req.version);
    fetch_reques.__set_version_hash(_push_req.version_hash);
    fetch_reques.__set_tablet_id(10003);

    TFetchStartKey start_key;
    start_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_start_key(std::vector<TFetchStartKey>(1, start_key));
    fetch_reques.__set_range("ge");

    TFetchEndKey end_key;
    end_key.__set_key(std::vector<std::string>(1, "100"));
    fetch_reques.__set_end_key(std::vector<TFetchEndKey>(1, end_key));
    fetch_reques.__set_end_range("le");

    std::vector<std::string> field_vec;
    field_vec.push_back("k1");
    field_vec.push_back("k2");
    field_vec.push_back("k3");
    field_vec.push_back("k4");
    field_vec.push_back("k5");
    field_vec.push_back("k6");
    field_vec.push_back("k7");
    field_vec.push_back("k8");
    field_vec.push_back("v");
    fetch_reques.__set_field(field_vec);

    TupleDescriptor *tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    OLAPReader olap_reader(*tuple_desc);
    ASSERT_TRUE(olap_reader.init(fetch_reques, NULL, _profile).ok());

    char tuple_buf[1024]; 
    bzero(tuple_buf, 1024);
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);
    bool eof;
    int64_t raw_rows_read = 0;
    ASSERT_TRUE(olap_reader.next_tuple(tuple, &raw_rows_read, &eof).ok());

    ASSERT_EQ(0, *reinterpret_cast<const int8_t*>
            (tuple->get_slot(tuple_desc->slots()[0]->tuple_offset())));
    ASSERT_EQ(0, *reinterpret_cast<const int32_t*>
            (tuple->get_slot(tuple_desc->slots()[1]->tuple_offset())));
    {
        StringValue *slot = reinterpret_cast<StringValue *>(
                tuple->get_slot(tuple_desc->slots()[2]->tuple_offset()));   
        ASSERT_STREQ("26a91490-1851-4001-a9ad-6ef0cf2c3aa4", 
                std::string(slot->ptr, slot->len).c_str());
    }
    {
        DateTimeValue *slot = reinterpret_cast<DateTimeValue *>(
                tuple->get_slot(tuple_desc->slots()[3]->tuple_offset())); 
        ASSERT_STREQ("2014-04-27", slot->debug_string().c_str());
    }
    {
        DateTimeValue *slot = reinterpret_cast<DateTimeValue *>(
                tuple->get_slot(tuple_desc->slots()[4]->tuple_offset())); 
        ASSERT_STREQ("2014-04-27 22:23:20", slot->debug_string().c_str());
    }
    {
        DecimalValue *slot = reinterpret_cast<DecimalValue *>(
                tuple->get_slot(tuple_desc->slots()[5]->tuple_offset())); 
        ASSERT_STREQ("978.371", slot->to_string().c_str());
    }
    ASSERT_EQ(0, *reinterpret_cast<const int16_t*>
            (tuple->get_slot(tuple_desc->slots()[6]->tuple_offset())));
    {
        StringValue *slot = reinterpret_cast<StringValue *>(
                tuple->get_slot(tuple_desc->slots()[7]->tuple_offset()));   
        ASSERT_STREQ("char", std::string(slot->ptr, slot->len).c_str());
    }
    ASSERT_EQ(0, *reinterpret_cast<const int64_t*>
            (tuple->get_slot(tuple_desc->slots()[8]->tuple_offset())));
}

TEST_F(TestOLAPReaderColumn, next_tuple_without_aggregation) {
    init_scan_node_k1_v();
    
    TFetchRequest fetch_reques;

    fetch_reques.__set_aggregation(false);
    fetch_reques.__set_schema_hash(1508825676);
    fetch_reques.__set_version(_push_req.version);
    fetch_reques.__set_version_hash(_push_req.version_hash);
    fetch_reques.__set_tablet_id(10003);

    TFetchStartKey start_key;
    start_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_start_key(std::vector<TFetchStartKey>(1, start_key));
    fetch_reques.__set_range("ge");

    TFetchEndKey end_key;
    end_key.__set_key(std::vector<std::string>(1, "100"));
    fetch_reques.__set_end_key(std::vector<TFetchEndKey>(1, end_key));
    fetch_reques.__set_end_range("le");
    
    std::vector<std::string> field_vec;
    field_vec.push_back("k1");
    field_vec.push_back("v");
    fetch_reques.__set_field(field_vec);

    TupleDescriptor *tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    OLAPReader olap_reader(*tuple_desc);
    ASSERT_TRUE(olap_reader.init(fetch_reques, NULL, _profile).ok());

    char tuple_buf[1024]; 
    bzero(tuple_buf, 1024);
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);
    bool eof;
    int64_t raw_rows_read = 0;
    ASSERT_TRUE(olap_reader.next_tuple(tuple, &raw_rows_read, &eof).ok());

    ASSERT_EQ(0, *reinterpret_cast<const int8_t*>
            (tuple->get_slot(tuple_desc->slots()[0]->tuple_offset())));
    ASSERT_EQ(0, *reinterpret_cast<const int64_t*>
            (tuple->get_slot(tuple_desc->slots()[1]->tuple_offset())));
}

TEST_F(TestOLAPReaderColumn, next_tuple_with_aggregation) {
    init_scan_node_k1_v();
    
    TFetchRequest fetch_reques;

    fetch_reques.__set_aggregation(true);
    fetch_reques.__set_schema_hash(1508825676);
    fetch_reques.__set_version(_push_req.version);
    fetch_reques.__set_version_hash(_push_req.version_hash);
    fetch_reques.__set_tablet_id(10003);

    TFetchStartKey start_key;
    start_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_start_key(std::vector<TFetchStartKey>(1, start_key));
    fetch_reques.__set_range("ge");

    TFetchEndKey end_key;
    end_key.__set_key(std::vector<std::string>(1, "100"));
    fetch_reques.__set_end_key(std::vector<TFetchEndKey>(1, end_key));
    fetch_reques.__set_end_range("le");

    std::vector<std::string> field_vec;
    field_vec.push_back("k1");
    field_vec.push_back("v");
    fetch_reques.__set_field(field_vec);

    TupleDescriptor *tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    OLAPReader olap_reader(*tuple_desc);
    ASSERT_TRUE(olap_reader.init(fetch_reques, NULL, _profile).ok());

    char tuple_buf[1024]; 
    bzero(tuple_buf, 1024);
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);
    bool eof;
    int64_t raw_rows_read = 0;
    ASSERT_TRUE(olap_reader.next_tuple(tuple, &raw_rows_read, &eof).ok());

    ASSERT_EQ(0, *reinterpret_cast<const int8_t*>
            (tuple->get_slot(tuple_desc->slots()[0]->tuple_offset())));
    ASSERT_EQ(153600, *reinterpret_cast<const int64_t*>
            (tuple->get_slot(tuple_desc->slots()[1]->tuple_offset())));
}

class TestOLAPReaderColumnDeleteCondition : public testing::Test {
public:
    TestOLAPReaderColumnDeleteCondition() : _runtime_stat("test") {
        _profile = _obj_pool.add(new RuntimeProfile(&_obj_pool, "OlapScanner"));
        OLAPReader::init_profile(_profile);
    }
    
    void SetUp() {
        init_olap();
        init_scan_node();
    }

    void TearDown() {
        // Remove all dir.
        StorageEngine::get_instance()->drop_tablet(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        while (0 == access(_tablet_path.c_str(), F_OK)) {
            sleep(1);
        }
        ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path));
    }

    void init_olap() {
        // Create local data dir for StorageEngine.
        config::storage_root_path = "./test_run/row_tablet";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());

        // 1. Prepare for query split key.
        // create base tablet
        OLAPStatus res = OLAP_SUCCESS;
        set_default_create_tablet_request(&_create_tablet);
        CommandExecutor command_executor = CommandExecutor();
        res = command_executor.create_tablet(_create_tablet);
        ASSERT_EQ(OLAP_SUCCESS, res);
        TabletSharedPtr tablet = command_executor.get_tablet(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        ASSERT_TRUE(tablet.get() != NULL);
        _tablet_path = tablet->tablet_path();

        // push data
        set_default_push_request(&_push_req);
        std::vector<TTabletInfo> tablets_info;
        res = command_executor.push(_push_req, &tablets_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        ASSERT_EQ(1, tablets_info.size());
        
        // delete data
        set_default_push_request(&_delete_req);
        _delete_req.version = 3;
        _delete_req.version_hash = 2;
        _delete_req.push_type = TPushType::DELETE;
        _delete_req.__isset.http_file_path = false;

        TCondition condition;
        condition.column_name = "k2";
        condition.condition_op = "=";
        condition.condition_values.push_back("0");
        _delete_req.delete_conditions.push_back(condition);
        tablets_info.clear();
        res = command_executor.delete_data(_delete_req, &tablets_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        ASSERT_EQ(1, tablets_info.size());
    }

    void init_scan_node() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.disable_codegen = true;
        //ExecEnv* exec_env = new ExecEnv();
        //_runtime_stat.init(fragment_id, query_options, "test", exec_env);

        TDescriptorTable t_desc_tablet;

        // tablet descriptors
        TTableDescriptor t_tablet_desc;

        t_tablet_desc.id = 0;
        t_tablet_desc.tableType = TTableType::OLAP_TABLE;
        t_tablet_desc.numCols = 0;
        t_tablet_desc.numClusteringCols = 0;
        t_tablet_desc.olapTable.tableName = "";
        t_tablet_desc.tableName = "";
        t_tablet_desc.dbName = "";
        t_tablet_desc.__isset.mysqlTable = true;
        t_desc_tablet.tableDescriptors.push_back(t_tablet_desc);
        t_desc_tablet.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // k1
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::TINYINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k1");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int8_t);
        }
        ++i;
        // k2
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
            t_slot_desc.__set_colName("k2");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;       
        // k3
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::VARCHAR));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k3");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }
        ++i;       
        // k4
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
            t_slot_desc.__set_colName("k4");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(DateTimeValue);
        }
        ++i;
        // k5
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::DATETIME));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k5");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(DateTimeValue);
        }
        ++i;
        // k6
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::DECIMAL));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k6");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(decimal12_t);
        }
        ++i;
        // k7
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::SMALLINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k7");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int16_t);
        }
        ++i;       
        // k8
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::CHAR));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k8");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }
        ++i;      
        // v
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::BIGINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("v");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int64_t);
        }

        t_desc_tablet.__isset.slotDescriptors = true;
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_tablet.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_tablet, &_desc_tbl);
    }

    
    void init_scan_node_k1_v() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.disable_codegen = true;
        //ExecEnv* exec_env = new ExecEnv();
        //_runtime_stat.init(fragment_id, query_options, "test", exec_env);

        TDescriptorTable t_desc_tablet;

        // tablet descriptors
        TTableDescriptor t_tablet_desc;

        t_tablet_desc.id = 0;
        t_tablet_desc.tableType = TTableType::OLAP_TABLE;
        t_tablet_desc.numCols = 0;
        t_tablet_desc.numClusteringCols = 0;
        t_tablet_desc.olapTable.tableName = "";
        t_tablet_desc.tableName = "";
        t_tablet_desc.dbName = "";
        t_tablet_desc.__isset.mysqlTable = true;
        t_desc_tablet.tableDescriptors.push_back(t_tablet_desc);
        t_desc_tablet.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // k1
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::TINYINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("k1");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int8_t);
        }
        ++i;    
        // v
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::BIGINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("v");
            t_desc_tablet.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int64_t);
        }

        t_desc_tablet.__isset.slotDescriptors = true;
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_tablet.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_tablet, &_desc_tbl);
    }

private:
    TCreateTabletReq _create_tablet;
    std::string _tablet_path;
    TPushReq _push_req;
    TPushReq _delete_req;

    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtime_stat;
    vector<TScanRangeParams> _scan_ranges;
    RuntimeProfile* _profile;
};

TEST_F(TestOLAPReaderColumnDeleteCondition, next_tuple) {
    init_scan_node();

    TFetchRequest fetch_reques;

    fetch_reques.__set_schema_hash(1508825676);
    fetch_reques.__set_version(_delete_req.version);
    fetch_reques.__set_version_hash(_delete_req.version_hash);
    fetch_reques.__set_tablet_id(10003);

    TFetchStartKey start_key;
    start_key.__set_key(std::vector<std::string>(1, "0"));
    fetch_reques.__set_start_key(std::vector<TFetchStartKey>(1, start_key));
    fetch_reques.__set_range("ge");

    TFetchEndKey end_key;
    end_key.__set_key(std::vector<std::string>(1, "100"));
    fetch_reques.__set_end_key(std::vector<TFetchEndKey>(1, end_key));
    fetch_reques.__set_end_range("le");

    std::vector<std::string> field_vec;
    field_vec.push_back("k1");
    field_vec.push_back("k2");
    field_vec.push_back("k3");
    field_vec.push_back("k4");
    field_vec.push_back("k5");
    field_vec.push_back("k6");
    field_vec.push_back("k7");
    field_vec.push_back("k8");
    field_vec.push_back("v");
    fetch_reques.__set_field(field_vec);

    TupleDescriptor *tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    OLAPReader olap_reader(*tuple_desc);
    ASSERT_TRUE(olap_reader.init(fetch_reques, NULL, _profile).ok());

    char tuple_buf[1024]; 
    bzero(tuple_buf, 1024);
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);
    bool eof;
    int64_t raw_rows_read = 0;
    ASSERT_TRUE(olap_reader.next_tuple(tuple, &raw_rows_read, &eof).ok());

    ASSERT_EQ(0, *reinterpret_cast<const int8_t*>
            (tuple->get_slot(tuple_desc->slots()[0]->tuple_offset())));
    ASSERT_EQ(128, *reinterpret_cast<const int32_t*>
            (tuple->get_slot(tuple_desc->slots()[1]->tuple_offset())));
    {
        StringValue *slot = reinterpret_cast<StringValue *>(
                tuple->get_slot(tuple_desc->slots()[2]->tuple_offset()));   
        ASSERT_STREQ("a1d033e6-2944-4b4e-b432-ea804c89dcd7", 
                std::string(slot->ptr, slot->len).c_str());
    }
    {
        DateTimeValue *slot = reinterpret_cast<DateTimeValue *>(
                tuple->get_slot(tuple_desc->slots()[3]->tuple_offset())); 
        ASSERT_STREQ("2014-03-23", slot->debug_string().c_str());
    }
    {
        DateTimeValue *slot = reinterpret_cast<DateTimeValue *>(
                tuple->get_slot(tuple_desc->slots()[4]->tuple_offset())); 
        ASSERT_STREQ("2014-03-23 05:39:17", slot->debug_string().c_str());
    }
    {
        DecimalValue *slot = reinterpret_cast<DecimalValue *>(
                tuple->get_slot(tuple_desc->slots()[5]->tuple_offset())); 
        ASSERT_STREQ("-484.89", slot->to_string().c_str());
    }
    ASSERT_EQ(0, *reinterpret_cast<const int16_t*>
            (tuple->get_slot(tuple_desc->slots()[6]->tuple_offset())));
    {
        StringValue *slot = reinterpret_cast<StringValue *>(
                tuple->get_slot(tuple_desc->slots()[7]->tuple_offset()));   
        ASSERT_STREQ("char", std::string(slot->ptr, slot->len).c_str());
    }
    ASSERT_EQ(12800, *reinterpret_cast<const int64_t*>
            (tuple->get_slot(tuple_desc->slots()[8]->tuple_offset())));
   
}

}  // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    int ret = doris::OLAP_SUCCESS;
    doris::set_up();
    ret = RUN_ALL_TESTS();
    doris::tear_down();

    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}
