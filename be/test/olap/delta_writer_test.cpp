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

#include <gtest/gtest.h>
#include <sys/file.h>

#include <string>

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/field.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"
#include "util/file_utils.h"
#include "util/logging.h"

namespace doris {

// This is DeltaWriter unit test which used by streaming load.
// And also it should take schema change into account after streaming load.

static const uint32_t MAX_PATH_LEN = 1024;

static StorageEngine* k_engine = nullptr;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = std::string(buffer) + "/data_test";
    FileUtils::remove_all(config::storage_root_path);
    FileUtils::create_dir(config::storage_root_path);
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    Status s = doris::StorageEngine::open(options, &k_engine);
    EXPECT_TRUE(s.ok()) << s.to_string();

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_storage_engine(k_engine);
    k_engine->start_bg_threads();
}

static void tear_down() {
    if (k_engine != nullptr) {
        k_engine->stop();
        delete k_engine;
        k_engine = nullptr;
    }
    EXPECT_EQ(system("rm -rf ./data_test"), 0);
    FileUtils::remove_all(std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX);
}

static void create_tablet_request(int64_t tablet_id, int32_t schema_hash,
                                  TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 6;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->__set_storage_format(TStorageFormat::V2);

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
    k10.column_type.type = TPrimitiveType::DECIMALV2;
    k10.column_type.__set_precision(6);
    k10.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k10);

    TColumn k11;
    k11.column_name = "k11";
    k11.__set_is_key(true);
    k11.column_type.type = TPrimitiveType::DATEV2;
    request->tablet_schema.columns.push_back(k11);

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
    v10.column_type.type = TPrimitiveType::DECIMALV2;
    v10.column_type.__set_precision(6);
    v10.column_type.__set_scale(3);
    v10.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v10);

    TColumn v11;
    v11.column_name = "v11";
    v11.__set_is_key(false);
    v11.column_type.type = TPrimitiveType::DATEV2;
    v11.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v11);
}

static void create_tablet_request_with_sequence_col(int64_t tablet_id, int32_t schema_hash,
                                                    TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::UNIQUE_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->tablet_schema.__set_sequence_col_idx(2);
    request->__set_storage_format(TStorageFormat::V2);

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

    TColumn sequence_col;
    sequence_col.column_name = SEQUENCE_COL;
    sequence_col.__set_is_key(false);
    sequence_col.column_type.type = TPrimitiveType::INT;
    sequence_col.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(sequence_col);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::DATETIME;
    v1.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v1);

    TColumn v2;
    v2.column_name = "v2";
    v2.__set_is_key(false);
    v2.column_type.type = TPrimitiveType::DATEV2;
    v2.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v2);
}

static TDescriptorTable create_descriptor_tablet() {
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
            TSlotDescriptorBuilder().type(TYPE_DATEV2).column_name("k11").column_pos(10).build());

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("v1").column_pos(11).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("v2").column_pos(12).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("v3").column_pos(13).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("v4").column_pos(14).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_LARGEINT).column_name("v5").column_pos(15).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATE).column_name("v6").column_pos(16).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("v7").column_pos(17).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().string_type(4).column_name("v8").column_pos(18).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().string_type(65).column_name("v9").column_pos(19).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().decimal_type(6, 3).column_name("v10").column_pos(20).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATEV2).column_name("v11").column_pos(21).build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

static TDescriptorTable create_descriptor_tablet_with_sequence_col() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("k2").column_pos(1).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name(SEQUENCE_COL)
                                   .column_pos(2)
                                   .build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("v1").column_pos(3).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATEV2).column_name("v2").column_pos(4).build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

class TestDeltaWriter : public ::testing::Test {
public:
    TestDeltaWriter() {}
    ~TestDeltaWriter() {}
    static void SetUpTestSuite() {
        config::min_file_descriptor_number = 100;
        set_up();
    }

    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(TestDeltaWriter, open) {
    TCreateTabletReq request;
    create_tablet_request(10003, 270068375, &request);
    Status res = k_engine->create_tablet(request);
    EXPECT_EQ(Status::OK(), res);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10003,   270068375,  WriteType::LOAD,      20001, 30001,
                              load_id, tuple_desc, &tuple_desc->slots(), true};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer);
    EXPECT_NE(delta_writer, nullptr);
    res = delta_writer->close();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->close_wait();
    EXPECT_EQ(Status::OK(), res);
    SAFE_DELETE(delta_writer);

    // test vec delta writer
    DeltaWriter::open(&write_req, &delta_writer, true);
    EXPECT_NE(delta_writer, nullptr);
    res = delta_writer->close();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->close_wait();
    EXPECT_EQ(Status::OK(), res);
    SAFE_DELETE(delta_writer);

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id);
    EXPECT_EQ(Status::OK(), res);
}

TEST_F(TestDeltaWriter, write) {
    TCreateTabletReq request;
    create_tablet_request(10004, 270068376, &request);
    Status res = k_engine->create_tablet(request);
    EXPECT_EQ(Status::OK(), res);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10004, 270068376, WriteType::LOAD, 20002,
                              30002, load_id,   tuple_desc,      &(tuple_desc->slots())};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer);
    EXPECT_NE(delta_writer, nullptr);

    MemPool pool;
    // Tuple 1
    {
        Tuple* tuple = reinterpret_cast<Tuple*>(pool.allocate(tuple_desc->byte_size()));
        memset(tuple, 0, tuple_desc->byte_size());
        *(int8_t*)(tuple->get_slot(slots[0]->tuple_offset())) = -127;
        *(int16_t*)(tuple->get_slot(slots[1]->tuple_offset())) = -32767;
        *(int32_t*)(tuple->get_slot(slots[2]->tuple_offset())) = -2147483647;
        *(int64_t*)(tuple->get_slot(slots[3]->tuple_offset())) = -9223372036854775807L;

        int128_t large_int_value = -90000;
        memcpy(tuple->get_slot(slots[4]->tuple_offset()), &large_int_value, sizeof(int128_t));

        ((DateTimeValue*)(tuple->get_slot(slots[5]->tuple_offset())))
                ->from_date_str("2048-11-10", 10);
        ((DateTimeValue*)(tuple->get_slot(slots[6]->tuple_offset())))
                ->from_date_str("2636-08-16 19:39:43", 19);

        StringValue* char_ptr = (StringValue*)(tuple->get_slot(slots[7]->tuple_offset()));
        char_ptr->ptr = (char*)pool.allocate(4);
        memcpy(char_ptr->ptr, "abcd", 4);
        char_ptr->len = 4;

        StringValue* var_ptr = (StringValue*)(tuple->get_slot(slots[8]->tuple_offset()));
        var_ptr->ptr = (char*)pool.allocate(5);
        memcpy(var_ptr->ptr, "abcde", 5);
        var_ptr->len = 5;

        DecimalV2Value decimal_value;
        decimal_value.assign_from_double(1.1);
        *(DecimalV2Value*)(tuple->get_slot(slots[9]->tuple_offset())) = decimal_value;
        ((doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*)(tuple->get_slot(
                 slots[10]->tuple_offset())))
                ->from_date_str("2048-11-10", 10);

        *(int8_t*)(tuple->get_slot(slots[11]->tuple_offset())) = -127;
        *(int16_t*)(tuple->get_slot(slots[12]->tuple_offset())) = -32767;
        *(int32_t*)(tuple->get_slot(slots[13]->tuple_offset())) = -2147483647;
        *(int64_t*)(tuple->get_slot(slots[14]->tuple_offset())) = -9223372036854775807L;

        memcpy(tuple->get_slot(slots[15]->tuple_offset()), &large_int_value, sizeof(int128_t));

        ((DateTimeValue*)(tuple->get_slot(slots[16]->tuple_offset())))
                ->from_date_str("2048-11-10", 10);
        ((DateTimeValue*)(tuple->get_slot(slots[17]->tuple_offset())))
                ->from_date_str("2636-08-16 19:39:43", 19);

        char_ptr = (StringValue*)(tuple->get_slot(slots[18]->tuple_offset()));
        char_ptr->ptr = (char*)pool.allocate(4);
        memcpy(char_ptr->ptr, "abcd", 4);
        char_ptr->len = 4;

        var_ptr = (StringValue*)(tuple->get_slot(slots[19]->tuple_offset()));
        var_ptr->ptr = (char*)pool.allocate(5);
        memcpy(var_ptr->ptr, "abcde", 5);
        var_ptr->len = 5;

        DecimalV2Value val_decimal;
        val_decimal.assign_from_double(1.1);

        *(DecimalV2Value*)(tuple->get_slot(slots[20]->tuple_offset())) = val_decimal;

        ((doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*)(tuple->get_slot(
                 slots[21]->tuple_offset())))
                ->from_date_str("2048-11-10", 10);

        res = delta_writer->write(tuple);
        EXPECT_EQ(Status::OK(), res);
    }

    res = delta_writer->close();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->close_wait();
    EXPECT_EQ(Status::OK(), res);

    // publish version success
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(write_req.tablet_id);
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->rowset_with_max_version()->end_version() + 1;
    version.second = tablet->rowset_with_max_version()->end_version() + 1;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
            write_req.txn_id, write_req.partition_id, &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        RowsetSharedPtr rowset = tablet_rs.second;
        res = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                   write_req.tablet_id, write_req.schema_hash,
                                                   tablet_rs.first.tablet_uid, version);
        EXPECT_EQ(Status::OK(), res);
        res = tablet->add_inc_rowset(rowset);
        EXPECT_EQ(Status::OK(), res);
    }
    EXPECT_EQ(1, tablet->num_rows());

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id);
    EXPECT_EQ(Status::OK(), res);
    delete delta_writer;
}

TEST_F(TestDeltaWriter, vec_write) {
    TCreateTabletReq request;
    create_tablet_request(10004, 270068376, &request);
    Status res = k_engine->create_tablet(request);
    ASSERT_TRUE(res.ok());

    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    //     const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10004, 270068376, WriteType::LOAD, 20002,
                              30002, load_id,   tuple_desc,      &(tuple_desc->slots())};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer, true);
    ASSERT_NE(delta_writer, nullptr);

    MemPool pool;

    vectorized::Block block;
    for (const auto& slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }

    auto columns = block.mutate_columns();
    {
        int8_t k1 = -127;
        columns[0]->insert_data((const char*)&k1, sizeof(k1));

        int16_t k2 = -32767;
        columns[1]->insert_data((const char*)&k2, sizeof(k2));

        int32_t k3 = -2147483647;
        columns[2]->insert_data((const char*)&k3, sizeof(k3));

        int64_t k4 = -9223372036854775807L;
        columns[3]->insert_data((const char*)&k4, sizeof(k4));

        int128_t k5 = -90000;
        columns[4]->insert_data((const char*)&k5, sizeof(k5));

        DateTimeValue k6;
        k6.from_date_str("2048-11-10", 10);
        auto k6_int = k6.to_int64();
        columns[5]->insert_data((const char*)&k6_int, sizeof(k6_int));

        DateTimeValue k7;
        k7.from_date_str("2636-08-16 19:39:43", 19);
        auto k7_int = k7.to_int64();
        columns[6]->insert_data((const char*)&k7_int, sizeof(k7_int));

        columns[7]->insert_data("abcd", 4);
        columns[8]->insert_data("abcde", 5);

        DecimalV2Value decimal_value;
        decimal_value.assign_from_double(1.1);
        columns[9]->insert_data((const char*)&decimal_value, sizeof(decimal_value));

        doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType> date_v2;
        date_v2.from_date_str("2048-11-10", 10);
        auto date_v2_int = date_v2.to_date_int_val();
        columns[10]->insert_data((const char*)&date_v2_int, sizeof(date_v2_int));

        int8_t v1 = -127;
        columns[11]->insert_data((const char*)&v1, sizeof(v1));

        int16_t v2 = -32767;
        columns[12]->insert_data((const char*)&v2, sizeof(v2));

        int32_t v3 = -2147483647;
        columns[13]->insert_data((const char*)&v3, sizeof(v3));

        int64_t v4 = -9223372036854775807L;
        columns[14]->insert_data((const char*)&v4, sizeof(v4));

        int128_t v5 = -90000;
        columns[15]->insert_data((const char*)&v5, sizeof(v5));

        DateTimeValue v6;
        v6.from_date_str("2048-11-10", 10);
        auto v6_int = v6.to_int64();
        columns[16]->insert_data((const char*)&v6_int, sizeof(v6_int));

        DateTimeValue v7;
        v7.from_date_str("2636-08-16 19:39:43", 19);
        auto v7_int = v7.to_int64();
        columns[17]->insert_data((const char*)&v7_int, sizeof(v7_int));

        columns[18]->insert_data("abcd", 4);
        columns[19]->insert_data("abcde", 5);

        decimal_value.assign_from_double(1.1);
        columns[20]->insert_data((const char*)&decimal_value, sizeof(decimal_value));

        date_v2.from_date_str("2048-11-10", 10);
        date_v2_int = date_v2.to_date_int_val();
        columns[21]->insert_data((const char*)&date_v2_int, sizeof(date_v2_int));

        res = delta_writer->write(&block, {0});
        ASSERT_TRUE(res.ok());
    }

    res = delta_writer->close();
    ASSERT_TRUE(res.ok());
    res = delta_writer->close_wait();
    ASSERT_TRUE(res.ok());

    // publish version success
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(write_req.tablet_id);
    std::cout << "before publish, tablet row nums:" << tablet->num_rows() << std::endl;
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->rowset_with_max_version()->end_version() + 1;
    version.second = tablet->rowset_with_max_version()->end_version() + 1;
    std::cout << "start to add rowset version:" << version.first << "-" << version.second
              << std::endl;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
            write_req.txn_id, write_req.partition_id, &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        std::cout << "start to publish txn" << std::endl;
        RowsetSharedPtr rowset = tablet_rs.second;
        res = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                   write_req.tablet_id, write_req.schema_hash,
                                                   tablet_rs.first.tablet_uid, version);
        ASSERT_TRUE(res.ok());
        std::cout << "start to add inc rowset:" << rowset->rowset_id()
                  << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
                  << "-" << rowset->version().second << std::endl;
        res = tablet->add_inc_rowset(rowset);
        ASSERT_TRUE(res.ok());
    }
    ASSERT_EQ(1, tablet->num_rows());

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id);
    ASSERT_TRUE(res.ok());
    delete delta_writer;
}

TEST_F(TestDeltaWriter, sequence_col) {
    TCreateTabletReq request;
    create_tablet_request_with_sequence_col(10005, 270068377, &request);
    Status res = k_engine->create_tablet(request);
    EXPECT_EQ(Status::OK(), res);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10005, 270068377, WriteType::LOAD, 20003,
                              30003, load_id,   tuple_desc,      &(tuple_desc->slots())};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer);
    EXPECT_NE(delta_writer, nullptr);

    MemPool pool;
    // Tuple 1
    {
        Tuple* tuple = reinterpret_cast<Tuple*>(pool.allocate(tuple_desc->byte_size()));
        memset(tuple, 0, tuple_desc->byte_size());
        *(int8_t*)(tuple->get_slot(slots[0]->tuple_offset())) = 123;
        *(int16_t*)(tuple->get_slot(slots[1]->tuple_offset())) = 456;
        *(int32_t*)(tuple->get_slot(slots[2]->tuple_offset())) = 1;
        ((DateTimeValue*)(tuple->get_slot(slots[3]->tuple_offset())))
                ->from_date_str("2020-07-16 19:39:43", 19);

        res = delta_writer->write(tuple);
        EXPECT_EQ(Status::OK(), res);
    }

    res = delta_writer->close();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->close_wait();
    EXPECT_EQ(Status::OK(), res);

    // publish version success
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(write_req.tablet_id);
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->rowset_with_max_version()->end_version() + 1;
    version.second = tablet->rowset_with_max_version()->end_version() + 1;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
            write_req.txn_id, write_req.partition_id, &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        RowsetSharedPtr rowset = tablet_rs.second;
        res = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                   write_req.tablet_id, write_req.schema_hash,
                                                   tablet_rs.first.tablet_uid, version);
        EXPECT_EQ(Status::OK(), res);
        res = tablet->add_inc_rowset(rowset);
        EXPECT_EQ(Status::OK(), res);
    }
    EXPECT_EQ(1, tablet->num_rows());

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id);
    EXPECT_EQ(Status::OK(), res);
    delete delta_writer;
}

TEST_F(TestDeltaWriter, vec_sequence_col) {
    TCreateTabletReq request;
    sleep(20);
    create_tablet_request_with_sequence_col(10005, 270068377, &request);
    Status res = k_engine->create_tablet(request);
    ASSERT_TRUE(res.ok());

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10005, 270068377, WriteType::LOAD, 20003,
                              30003, load_id,   tuple_desc,      &(tuple_desc->slots())};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer, true);
    ASSERT_NE(delta_writer, nullptr);

    MemPool pool;

    vectorized::Block block;
    for (const auto& slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }

    auto columns = block.mutate_columns();
    {
        int8_t c1 = 123;
        columns[0]->insert_data((const char*)&c1, sizeof(c1));

        int16_t c2 = 456;
        columns[1]->insert_data((const char*)&c2, sizeof(c2));

        int32_t c3 = 1;
        columns[2]->insert_data((const char*)&c3, sizeof(c2));

        DateTimeValue c4;
        c4.from_date_str("2020-07-16 19:39:43", 19);
        int64_t c4_int = c4.to_int64();
        columns[3]->insert_data((const char*)&c4_int, sizeof(c4));

        doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType> c5;
        c5.set_time(2022, 6, 6, 0, 0, 0, 0);
        uint32_t c5_int = c5.to_date_int_val();
        columns[4]->insert_data((const char*)&c5_int, sizeof(c5));

        res = delta_writer->write(&block, {0});
        ASSERT_TRUE(res.ok());
    }

    res = delta_writer->close();
    ASSERT_TRUE(res.ok());
    res = delta_writer->close_wait();
    ASSERT_TRUE(res.ok());

    // publish version success
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(write_req.tablet_id);
    std::cout << "before publish, tablet row nums:" << tablet->num_rows() << std::endl;
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->rowset_with_max_version()->end_version() + 1;
    version.second = tablet->rowset_with_max_version()->end_version() + 1;
    std::cout << "start to add rowset version:" << version.first << "-" << version.second
              << std::endl;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
            write_req.txn_id, write_req.partition_id, &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        std::cout << "start to publish txn" << std::endl;
        RowsetSharedPtr rowset = tablet_rs.second;
        res = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                   write_req.tablet_id, write_req.schema_hash,
                                                   tablet_rs.first.tablet_uid, version);
        ASSERT_TRUE(res.ok());
        std::cout << "start to add inc rowset:" << rowset->rowset_id()
                  << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
                  << "-" << rowset->version().second << std::endl;
        res = tablet->add_inc_rowset(rowset);
        ASSERT_TRUE(res.ok());
    }
    ASSERT_EQ(1, tablet->num_rows());

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id);
    ASSERT_TRUE(res.ok());
    delete delta_writer;
}

} // namespace doris
