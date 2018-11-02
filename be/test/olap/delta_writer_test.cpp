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

#include "olap/delta_writer.h"

#include <sys/file.h>
#include <string>
#include <gtest/gtest.h>

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/field.h"
#include "olap/olap_engine.h"
#include "olap/olap_table.h"
#include "olap/utils.h"
#include "runtime/tuple.h"
#include "util/descriptor_helper.h"
#include "util/logging.h"
#include "olap/options.h"
#include "olap/olap_header_manager.h"

namespace doris {

// This is DeltaWriter unit test which used by streaming load.
// And also it should take schema change into account after streaming load.

static const uint32_t MAX_RETRY_TIMES = 10;
static const uint32_t MAX_PATH_LEN = 1024;

OLAPEngine* k_engine = nullptr;

void set_up() {
    char buffer[MAX_PATH_LEN];
    getcwd(buffer, MAX_PATH_LEN);
    config::storage_root_path = std::string(buffer) + "/data_test";
    remove_all_dir(config::storage_root_path);
    create_dir(config::storage_root_path);
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    doris::OLAPEngine::open(options, &k_engine);
}

void tear_down() {
    system("rm -rf ./data_test");
    remove_all_dir(std::string(getenv("DORIS_HOME")) + UNUSED_PREFIX);
}

void create_table_request(TCreateTabletReq* request) {
    request->tablet_id = 10003;
    request->__set_version(1);
    request->__set_version_hash(0);
    request->tablet_schema.schema_hash = 270068375;
    request->tablet_schema.short_key_column_count = 6;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.__set_is_key(true);
    k4.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.__set_is_key(true);
    k5.column_type.type = TPrimitiveType::LARGEINT;
    request->tablet_schema.columns.push_back(k5);

    TColumn k6;
    k6.column_name = "k6";
    k6.__set_is_key(true);
    k6.column_type.type = TPrimitiveType::DATE;
    request->tablet_schema.columns.push_back(k6);

    TColumn k7;
    k7.column_name = "k7";
    k7.__set_is_key(true);
    k7.column_type.type = TPrimitiveType::DATETIME;
    request->tablet_schema.columns.push_back(k7);

    TColumn k8;
    k8.column_name = "k8";
    k8.__set_is_key(true);
    k8.column_type.type = TPrimitiveType::CHAR;
    k8.column_type.__set_len(4);
    request->tablet_schema.columns.push_back(k8);

    TColumn k9;
    k9.column_name = "k9";
    k9.__set_is_key(true);
    k9.column_type.type = TPrimitiveType::VARCHAR;
    k9.column_type.__set_len(65);
    request->tablet_schema.columns.push_back(k9);

    TColumn k10;
    k10.column_name = "k10";
    k10.__set_is_key(true);
    k10.column_type.type = TPrimitiveType::DECIMAL;
    k10.column_type.__set_precision(6);
    k10.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k10);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::TINYINT;
    v1.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v1);

    TColumn v2;
    v2.column_name = "v2";
    v2.__set_is_key(false);
    v2.column_type.type = TPrimitiveType::SMALLINT;
    v2.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v2);

    TColumn v3;
    v3.column_name = "v3";
    v3.__set_is_key(false);
    v3.column_type.type = TPrimitiveType::INT;
    v3.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v3);

    TColumn v4;
    v4.column_name = "v4";
    v4.__set_is_key(false);
    v4.column_type.type = TPrimitiveType::BIGINT;
    v4.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v4);

    TColumn v5;
    v5.column_name = "v5";
    v5.__set_is_key(false);
    v5.column_type.type = TPrimitiveType::LARGEINT;
    v5.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v5);

    TColumn v6;
    v6.column_name = "v6";
    v6.__set_is_key(false);
    v6.column_type.type = TPrimitiveType::DATE;
    v6.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v6);

    TColumn v7;
    v7.column_name = "v7";
    v7.__set_is_key(false);
    v7.column_type.type = TPrimitiveType::DATETIME;
    v7.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v7);

    TColumn v8;
    v8.column_name = "v8";
    v8.__set_is_key(false);
    v8.column_type.type = TPrimitiveType::CHAR;
    v8.column_type.__set_len(4);
    v8.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v8);

    TColumn v9;
    v9.column_name = "v9";
    v9.__set_is_key(false);
    v9.column_type.type = TPrimitiveType::VARCHAR;
    v9.column_type.__set_len(65);
    v9.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v9);

    TColumn v10;
    v10.column_name = "v10";
    v10.__set_is_key(false);
    v10.column_type.type = TPrimitiveType::DECIMAL;
    v10.column_type.__set_precision(6);
    v10.column_type.__set_scale(3);
    v10.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v10);
}

TDescriptorTable create_descriptor_table() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("k2").column_pos(1).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_INT).column_name("k3").column_pos(2).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k4").column_pos(3).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_LARGEINT).column_name("k5").column_pos(4).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_DATE).column_name("k6").column_pos(5).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("k7").column_pos(6).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().string_type(4).column_name("k8").column_pos(7).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().string_type(65).column_name("k9").column_pos(8).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().decimal_type(6, 3).column_name("k10").column_pos(9).build());

    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("v1").column_pos(10).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("v2").column_pos(11).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_INT).column_name("v3").column_pos(12).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("v4").column_pos(13).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_LARGEINT).column_name("v5").column_pos(14).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_DATE).column_name("v6").column_pos(15).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("v7").column_pos(16).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().string_type(4).column_name("v8").column_pos(17).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().string_type(65).column_name("v9").column_pos(18).build());
    tuple_builder.add_slot(
        TSlotDescriptorBuilder().decimal_type(6, 3).column_name("v10").column_pos(19).build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

class TestDeltaWriter : public ::testing::Test {
public:
    TestDeltaWriter() { }
    ~TestDeltaWriter() { }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = std::string(buffer) + "/data_push";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        // OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }
};

TEST_F(TestDeltaWriter, open) {
    TCreateTabletReq request;
    create_table_request(&request);
    OLAPStatus res = k_engine->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    TDescriptorTable tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10003, 270068375, WriteType::LOAD,
                              20001, 30001, load_id, false, tuple_desc};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer); 
    ASSERT_NE(delta_writer, nullptr);
    res = delta_writer->close(nullptr);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SAFE_DELETE(delta_writer);

    TDropTabletReq drop_request;
    auto tablet_id = 10003;
    auto schema_hash = 270068375;
    res = k_engine->drop_table(tablet_id, schema_hash);
    ASSERT_EQ(OLAP_SUCCESS, res);
}

TEST_F(TestDeltaWriter, write) {
    TCreateTabletReq request;
    create_table_request(&request);
    OLAPStatus res = k_engine->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    TDescriptorTable tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10003, 270068375, WriteType::LOAD,
                              20001, 30001, load_id, false, tuple_desc};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer); 
    ASSERT_NE(delta_writer, nullptr);

    const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();
    Arena arena;
    // Tuple 1
    {
        Tuple* tuple = reinterpret_cast<Tuple*>(arena.Allocate(tuple_desc->byte_size()));
        memset(tuple, 0, tuple_desc->byte_size());
        *(int8_t*)(tuple->get_slot(slots[0]->tuple_offset())) = -127;
        *(int16_t*)(tuple->get_slot(slots[1]->tuple_offset())) = -32767;
        *(int32_t*)(tuple->get_slot(slots[2]->tuple_offset())) = -2147483647;
        *(int64_t*)(tuple->get_slot(slots[3]->tuple_offset())) = -9223372036854775807L;

        int128_t large_int_value = -90000;
        memcpy(tuple->get_slot(slots[4]->tuple_offset()), &large_int_value, sizeof(int128_t));

        ((DateTimeValue*)(tuple->get_slot(slots[5]->tuple_offset())))->from_date_str("2048-11-10", 10); 
        ((DateTimeValue*)(tuple->get_slot(slots[6]->tuple_offset())))->from_date_str("2636-08-16 19:39:43", 19); 

        StringValue* char_ptr = (StringValue*)(tuple->get_slot(slots[7]->tuple_offset()));
        char_ptr->ptr = arena.Allocate(4);
        memcpy(char_ptr->ptr, "abcd", 4);
        char_ptr->len = 4; 

        StringValue* var_ptr = (StringValue*)(tuple->get_slot(slots[8]->tuple_offset()));
        var_ptr->ptr = arena.Allocate(5);
        memcpy(var_ptr->ptr, "abcde", 5);
        var_ptr->len = 5; 

        DecimalValue decimal_value(1.1);
        *(DecimalValue*)(tuple->get_slot(slots[9]->tuple_offset())) = decimal_value;

        *(int8_t*)(tuple->get_slot(slots[10]->tuple_offset())) = -127;
        *(int16_t*)(tuple->get_slot(slots[11]->tuple_offset())) = -32767;
        *(int32_t*)(tuple->get_slot(slots[12]->tuple_offset())) = -2147483647;
        *(int64_t*)(tuple->get_slot(slots[13]->tuple_offset())) = -9223372036854775807L;

        memcpy(tuple->get_slot(slots[14]->tuple_offset()), &large_int_value, sizeof(int128_t));

        ((DateTimeValue*)(tuple->get_slot(slots[15]->tuple_offset())))->from_date_str("2048-11-10", 10); 
        ((DateTimeValue*)(tuple->get_slot(slots[16]->tuple_offset())))->from_date_str("2636-08-16 19:39:43", 19); 

        char_ptr = (StringValue*)(tuple->get_slot(slots[17]->tuple_offset()));
        char_ptr->ptr = arena.Allocate(4);
        memcpy(char_ptr->ptr, "abcd", 4);
        char_ptr->len = 4; 

        var_ptr = (StringValue*)(tuple->get_slot(slots[18]->tuple_offset()));
        var_ptr->ptr = arena.Allocate(5);
        memcpy(var_ptr->ptr, "abcde", 5);
        var_ptr->len = 5; 

        DecimalValue val_decimal(1.1);
        *(DecimalValue*)(tuple->get_slot(slots[19]->tuple_offset())) = val_decimal;

        res = delta_writer->write(tuple);
        ASSERT_EQ(OLAP_SUCCESS, res);
    }

    res = delta_writer->close(nullptr);
    ASSERT_EQ(res, OLAP_SUCCESS);

    // publish version success
    OLAPTablePtr table = OLAPEngine::get_instance()->get_table(write_req.tablet_id, write_req.schema_hash);
    TPublishVersionRequest publish_req;
    publish_req.transaction_id = write_req.transaction_id;
    TPartitionVersionInfo info;
    info.partition_id = write_req.partition_id;
    info.version = table->lastest_version()->end_version() + 1;
    info.version_hash = table->lastest_version()->version_hash() + 1;
    std::vector<TPartitionVersionInfo> partition_version_infos;
    partition_version_infos.push_back(info);
    publish_req.partition_version_infos = partition_version_infos;
    std::vector<TTabletId> error_tablet_ids;
    res = k_engine->publish_version(publish_req, &error_tablet_ids);

    ASSERT_EQ(1, table->get_num_rows());

    auto tablet_id = 10003;
    auto schema_hash = 270068375;
    res = k_engine->drop_table(tablet_id, schema_hash);
    ASSERT_EQ(OLAP_SUCCESS, res);
}

// ######################### ALTER TABLE TEST BEGIN #########################

void schema_change_request(const TCreateTabletReq& base_request, TCreateTabletReq* request) {
    //linked schema change, add a value column
    request->tablet_id = base_request.tablet_id + 1;
    request->__set_version(base_request.version);
    request->__set_version_hash(base_request.version_hash);
    request->tablet_schema.schema_hash = base_request.tablet_schema.schema_hash + 1;
    request->tablet_schema.short_key_column_count = 3;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[2]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[3]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[4]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[5]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[6]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[7]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[8]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[9]);

    TColumn v0;
    v0.column_name = "v0";
    v0.column_type.type = TPrimitiveType::BIGINT;
    v0.__set_is_key(false);
    v0.__set_default_value("0");
    v0.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v0);

    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[10]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[11]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[12]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[13]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[14]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[15]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[16]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[17]);

    TColumn v9;
    v9.column_name = "v9";
    v9.__set_is_key(false);
    v9.column_type.type = TPrimitiveType::VARCHAR;
    v9.column_type.__set_len(130);
    v9.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v9);

    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[19]);
}

AlterTableStatus show_alter_table_status(const TAlterTabletReq& request) {
    AlterTableStatus status = ALTER_TABLE_RUNNING;
    uint32_t max_retry = MAX_RETRY_TIMES;
    while (max_retry > 0) {
        status = k_engine->show_alter_table_status(
                request.base_tablet_id, request.base_schema_hash);
        if (status != ALTER_TABLE_RUNNING) { break; }
        LOG(INFO) << "doing alter table......";
        --max_retry;
        sleep(1);
    }
    return status;
}

class TestSchemaChange : public ::testing::Test {
public:
    TestSchemaChange() { }
    ~TestSchemaChange() { }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = std::string(buffer) + "/data_schema_change";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        // OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }
};

TEST_F(TestSchemaChange, schema_change) {
    OLAPStatus res = OLAP_SUCCESS;
    AlterTableStatus status = ALTER_TABLE_WAITING;

    // 1. Prepare for schema change.
    // create base table
    TCreateTabletReq create_base_tablet;
    create_table_request(&create_base_tablet);
    res = k_engine->create_table(create_base_tablet);
    ASSERT_EQ(OLAP_SUCCESS, res);

    TDescriptorTable tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10003, 270068375, WriteType::LOAD,
                              20001, 30001, load_id, false, tuple_desc};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer); 
    ASSERT_NE(delta_writer, nullptr);

    const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();
    Arena arena;
    // streaming load data
    {
        Tuple* tuple = reinterpret_cast<Tuple*>(arena.Allocate(tuple_desc->byte_size()));
        memset(tuple, 0, tuple_desc->byte_size());
        *(int8_t*)(tuple->get_slot(slots[0]->tuple_offset())) = -127;
        *(int16_t*)(tuple->get_slot(slots[1]->tuple_offset())) = -32767;
        *(int32_t*)(tuple->get_slot(slots[2]->tuple_offset())) = -2147483647;
        *(int64_t*)(tuple->get_slot(slots[3]->tuple_offset())) = -9223372036854775807L;

        int128_t large_int_value = -90000;
        memcpy(tuple->get_slot(slots[4]->tuple_offset()), &large_int_value, sizeof(int128_t));

        ((DateTimeValue*)(tuple->get_slot(slots[5]->tuple_offset())))->from_date_str("2048-11-10", 10); 
        ((DateTimeValue*)(tuple->get_slot(slots[6]->tuple_offset())))->from_date_str("2636-08-16 19:39:43", 19); 

        StringValue* char_ptr = (StringValue*)(tuple->get_slot(slots[7]->tuple_offset()));
        char_ptr->ptr = arena.Allocate(4);
        memcpy(char_ptr->ptr, "abcd", 4);
        char_ptr->len = 4; 

        StringValue* var_ptr = (StringValue*)(tuple->get_slot(slots[8]->tuple_offset()));
        var_ptr->ptr = arena.Allocate(5);
        memcpy(var_ptr->ptr, "abcde", 5);
        var_ptr->len = 5; 

        DecimalValue decimal_value(1.1);
        *(DecimalValue*)(tuple->get_slot(slots[9]->tuple_offset())) = decimal_value;

        *(int8_t*)(tuple->get_slot(slots[10]->tuple_offset())) = -127;
        *(int16_t*)(tuple->get_slot(slots[11]->tuple_offset())) = -32767;
        *(int32_t*)(tuple->get_slot(slots[12]->tuple_offset())) = -2147483647;
        *(int64_t*)(tuple->get_slot(slots[13]->tuple_offset())) = -9223372036854775807L;

        memcpy(tuple->get_slot(slots[14]->tuple_offset()), &large_int_value, sizeof(int128_t));

        ((DateTimeValue*)(tuple->get_slot(slots[15]->tuple_offset())))->from_date_str("2048-11-10", 10); 
        ((DateTimeValue*)(tuple->get_slot(slots[16]->tuple_offset())))->from_date_str("2636-08-16 19:39:43", 19); 

        char_ptr = (StringValue*)(tuple->get_slot(slots[17]->tuple_offset()));
        char_ptr->ptr = arena.Allocate(4);
        memcpy(char_ptr->ptr, "abcd", 4);
        char_ptr->len = 4; 

        var_ptr = (StringValue*)(tuple->get_slot(slots[18]->tuple_offset()));
        var_ptr->ptr = arena.Allocate(5);
        memcpy(var_ptr->ptr, "abcde", 5);
        var_ptr->len = 5; 

        DecimalValue val_decimal(1.1);
        *(DecimalValue*)(tuple->get_slot(slots[19]->tuple_offset())) = val_decimal;

        res = delta_writer->write(tuple);
        ASSERT_EQ(OLAP_SUCCESS, res);
    }

    // publish version 
    res = delta_writer->close(nullptr);
    ASSERT_EQ(res, OLAP_SUCCESS);

    // publish version success
    OLAPTablePtr table = OLAPEngine::get_instance()->get_table(write_req.tablet_id, write_req.schema_hash);
    TPublishVersionRequest publish_req;
    publish_req.transaction_id = write_req.transaction_id;
    TPartitionVersionInfo info;
    info.partition_id = write_req.partition_id;
    info.version = table->lastest_version()->end_version() + 1;
    info.version_hash = table->lastest_version()->version_hash() + 1;
    std::vector<TPartitionVersionInfo> partition_version_infos;
    partition_version_infos.push_back(info);
    publish_req.partition_version_infos = partition_version_infos;
    std::vector<TTabletId> error_tablet_ids;
    res = k_engine->publish_version(publish_req, &error_tablet_ids);
    ASSERT_EQ(res, OLAP_SUCCESS); 

    // 1. set add column request
    TCreateTabletReq create_new_tablet;
    schema_change_request(create_base_tablet, &create_new_tablet);
    TAlterTabletReq request;
    request.__set_base_tablet_id(create_base_tablet.tablet_id);
    request.__set_base_schema_hash(create_base_tablet.tablet_schema.schema_hash);
    request.__set_new_tablet_req(create_new_tablet);

    // 2. Submit schema change
    request.base_schema_hash = create_base_tablet.tablet_schema.schema_hash;
    res = k_engine->schema_change(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // 3. Verify schema change result.
    // show schema change status
    status = show_alter_table_status(request);
    ASSERT_EQ(ALTER_TABLE_FINISHED, status);
    
    // check new tablet information
    TTabletInfo tablet_info;
    tablet_info.tablet_id = create_new_tablet.tablet_id;
    tablet_info.schema_hash = create_new_tablet.tablet_schema.schema_hash;
    res = k_engine->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(info.version, tablet_info.version);
    ASSERT_EQ(info.version_hash, tablet_info.version_hash);
    ASSERT_EQ(1, tablet_info.row_count);

    // 4. Retry the same schema change request.
    res = k_engine->schema_change(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    status = k_engine->show_alter_table_status(
            request.base_tablet_id, request.base_schema_hash);
    ASSERT_EQ(ALTER_TABLE_FINISHED, status);

    auto tablet_id = create_new_tablet.tablet_id;
    auto schema_hash = create_new_tablet.tablet_schema.schema_hash;
    res = k_engine->drop_table(tablet_id, schema_hash);
    ASSERT_EQ(OLAP_SUCCESS, res);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();

    doris::set_up();
    ret = RUN_ALL_TESTS();
    doris::tear_down();

    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}
