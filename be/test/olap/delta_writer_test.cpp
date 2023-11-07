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

#include <gen_cpp/AgentService_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdlib.h>
#include <unistd.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/object_pool.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/iterators.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/txn_manager.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class OlapMeta;

// This is DeltaWriter unit test which used by streaming load.
// And also it should take schema change into account after streaming load.

static const uint32_t MAX_PATH_LEN = 1024;

static std::unique_ptr<StorageEngine> k_engine;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = std::string(buffer) + "/data_test";
    static_cast<void>(
            io::global_local_filesystem()->delete_and_create_directory(config::storage_root_path));
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    k_engine = std::make_unique<StorageEngine>(options);
    Status s = k_engine->open();
    EXPECT_TRUE(s.ok()) << s.to_string();

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    static_cast<void>(exec_env->set_storage_engine(k_engine.get()));
    static_cast<void>(k_engine->start_bg_threads());
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    exec_env->set_storage_engine(nullptr);
    k_engine.reset();
    EXPECT_EQ(system("rm -rf ./data_test"), 0);
    static_cast<void>(io::global_local_filesystem()->delete_directory(
            std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX));
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
                                                    TCreateTabletReq* request,
                                                    bool enable_mow = false) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::UNIQUE_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->tablet_schema.__set_sequence_col_idx(4);
    request->__set_storage_format(TStorageFormat::V2);
    request->__set_enable_unique_key_merge_on_write(enable_mow);

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

    TColumn sequence_col;
    sequence_col.column_name = SEQUENCE_COL;
    sequence_col.__set_is_key(false);
    sequence_col.column_type.type = TPrimitiveType::INT;
    sequence_col.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(sequence_col);
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

    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_TINYINT)
                                   .column_name("v1")
                                   .column_pos(11)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_SMALLINT)
                                   .column_name("v2")
                                   .column_pos(12)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name("v3")
                                   .column_pos(13)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_BIGINT)
                                   .column_name("v4")
                                   .column_pos(14)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_LARGEINT)
                                   .column_name("v5")
                                   .column_pos(15)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATE)
                                   .column_name("v6")
                                   .column_pos(16)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATETIME)
                                   .column_name("v7")
                                   .column_pos(17)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .string_type(4)
                                   .column_name("v8")
                                   .column_pos(18)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .string_type(65)
                                   .column_name("v9")
                                   .column_pos(19)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .decimal_type(6, 3)
                                   .column_name("v10")
                                   .column_pos(20)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATEV2)
                                   .column_name("v11")
                                   .column_pos(21)
                                   .nullable(false)
                                   .build());
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
                                   .type(TYPE_DATETIME)
                                   .column_name("v1")
                                   .column_pos(2)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATEV2)
                                   .column_name("v2")
                                   .column_pos(3)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name(SEQUENCE_COL)
                                   .column_pos(4)
                                   .nullable(false)
                                   .build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

static void generate_data(vectorized::Block* block, int8_t k1, int16_t k2, int32_t seq) {
    auto columns = block->mutate_columns();
    int8_t c1 = k1;
    columns[0]->insert_data((const char*)&c1, sizeof(c1));

    int16_t c2 = k2;
    columns[1]->insert_data((const char*)&c2, sizeof(c2));

    VecDateTimeValue c3;
    c3.from_date_str("2020-07-16 19:39:43", 19);
    int64_t c3_int = c3.to_int64();
    columns[2]->insert_data((const char*)&c3_int, sizeof(c3));

    DateV2Value<DateV2ValueType> c4;
    c4.set_time(2022, 6, 6, 0, 0, 0, 0);
    uint32_t c4_int = c4.to_date_int_val();
    columns[3]->insert_data((const char*)&c4_int, sizeof(c4));

    int32_t c5 = seq;
    columns[4]->insert_data((const char*)&c5, sizeof(c2));
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
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    create_tablet_request(10003, 270068375, &request);
    Status res = k_engine->create_tablet(request, profile.get());
    EXPECT_EQ(Status::OK(), res);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    OlapTableSchemaParam param;

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = 10003;
    write_req.schema_hash = 270068375;
    write_req.txn_id = 20001;
    write_req.partition_id = 30001;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = true;
    write_req.table_schema_param = &param;
    DeltaWriter* delta_writer = nullptr;

    // test vec delta writer
    profile = std::make_unique<RuntimeProfile>("LoadChannels");
    static_cast<void>(DeltaWriter::open(&write_req, &delta_writer, profile.get(), TUniqueId()));
    EXPECT_NE(delta_writer, nullptr);
    res = delta_writer->close();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->build_rowset();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->commit_txn(PSlaveTabletNodes(), false);
    EXPECT_EQ(Status::OK(), res);
    SAFE_DELETE(delta_writer);

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    EXPECT_EQ(Status::OK(), res);
}

TEST_F(TestDeltaWriter, vec_write) {
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    create_tablet_request(10004, 270068376, &request);
    Status res = k_engine->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok());

    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    //     const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();
    OlapTableSchemaParam param;

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = 10004;
    write_req.schema_hash = 270068376;
    write_req.txn_id = 20002;
    write_req.partition_id = 30002;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = &param;
    DeltaWriter* delta_writer = nullptr;
    profile = std::make_unique<RuntimeProfile>("LoadChannels");
    static_cast<void>(DeltaWriter::open(&write_req, &delta_writer, profile.get(), TUniqueId()));
    ASSERT_NE(delta_writer, nullptr);

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

        VecDateTimeValue k6;
        k6.from_date_str("2048-11-10", 10);
        auto k6_int = k6.to_int64();
        columns[5]->insert_data((const char*)&k6_int, sizeof(k6_int));

        VecDateTimeValue k7;
        k7.from_date_str("2636-08-16 19:39:43", 19);
        auto k7_int = k7.to_int64();
        columns[6]->insert_data((const char*)&k7_int, sizeof(k7_int));

        columns[7]->insert_data("abcd", 4);
        columns[8]->insert_data("abcde", 5);

        DecimalV2Value decimal_value;
        decimal_value.assign_from_double(1.1);
        columns[9]->insert_data((const char*)&decimal_value, sizeof(decimal_value));

        DateV2Value<DateV2ValueType> date_v2;
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

        VecDateTimeValue v6;
        v6.from_date_str("2048-11-10", 10);
        auto v6_int = v6.to_int64();
        columns[16]->insert_data((const char*)&v6_int, sizeof(v6_int));

        VecDateTimeValue v7;
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
    res = delta_writer->wait_flush();
    ASSERT_TRUE(res.ok());
    res = delta_writer->build_rowset();
    ASSERT_TRUE(res.ok());
    res = delta_writer->submit_calc_delete_bitmap_task();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_calc_delete_bitmap();
    ASSERT_TRUE(res.ok());
    res = delta_writer->commit_txn(PSlaveTabletNodes(), false);
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
        TabletPublishStatistics stats;
        res = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                   write_req.tablet_id, tablet_rs.first.tablet_uid,
                                                   version, &stats);
        ASSERT_TRUE(res.ok());
        std::cout << "start to add inc rowset:" << rowset->rowset_id()
                  << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
                  << "-" << rowset->version().second << std::endl;
        res = tablet->add_inc_rowset(rowset);
        ASSERT_TRUE(res.ok());
    }
    ASSERT_EQ(1, tablet->num_rows());

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok());
    delete delta_writer;
}

TEST_F(TestDeltaWriter, vec_sequence_col) {
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    sleep(20);
    create_tablet_request_with_sequence_col(10005, 270068377, &request);
    Status res = k_engine->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok());

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    OlapTableSchemaParam param;

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = 10005;
    write_req.schema_hash = 270068377;
    write_req.txn_id = 20003;
    write_req.partition_id = 30003;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = &param;
    DeltaWriter* delta_writer = nullptr;
    profile = std::make_unique<RuntimeProfile>("LoadChannels");
    static_cast<void>(DeltaWriter::open(&write_req, &delta_writer, profile.get(), TUniqueId()));
    ASSERT_NE(delta_writer, nullptr);

    vectorized::Block block;
    for (const auto& slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }

    generate_data(&block, 123, 456, 100);
    res = delta_writer->write(&block, {0});
    ASSERT_TRUE(res.ok());

    generate_data(&block, 123, 456, 90);
    res = delta_writer->write(&block, {1});
    ASSERT_TRUE(res.ok());

    res = delta_writer->close();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_flush();
    ASSERT_TRUE(res.ok());
    res = delta_writer->build_rowset();
    ASSERT_TRUE(res.ok());
    res = delta_writer->submit_calc_delete_bitmap_task();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_calc_delete_bitmap();
    ASSERT_TRUE(res.ok());
    res = delta_writer->commit_txn(PSlaveTabletNodes(), false);
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
    ASSERT_EQ(1, tablet_related_rs.size());

    std::cout << "start to publish txn" << std::endl;
    RowsetSharedPtr rowset = tablet_related_rs.begin()->second;
    TabletPublishStatistics pstats;
    res = k_engine->txn_manager()->publish_txn(
            meta, write_req.partition_id, write_req.txn_id, write_req.tablet_id,
            tablet_related_rs.begin()->first.tablet_uid, version, &pstats);
    ASSERT_TRUE(res.ok());
    std::cout << "start to add inc rowset:" << rowset->rowset_id()
              << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
              << "-" << rowset->version().second << std::endl;
    res = tablet->add_inc_rowset(rowset);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(1, tablet->num_rows());
    std::vector<segment_v2::SegmentSharedPtr> segments;
    res = ((BetaRowset*)rowset.get())->load_segments(&segments);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(1, rowset->num_segments());
    ASSERT_EQ(1, segments.size());

    // read data, verify the data correct
    OlapReaderStatistics stats;
    StorageReadOptions opts;
    opts.stats = &stats;
    opts.tablet_schema = rowset->tablet_schema();

    std::unique_ptr<RowwiseIterator> iter;
    std::shared_ptr<Schema> schema = std::make_shared<Schema>(rowset->tablet_schema());
    auto s = segments[0]->new_iterator(schema, opts, &iter);
    ASSERT_TRUE(s.ok());
    auto read_block = rowset->tablet_schema()->create_block();
    res = iter->next_batch(&read_block);
    ASSERT_TRUE(res.ok()) << res;
    ASSERT_EQ(1, read_block.rows());
    // get the value from sequence column
    auto seq_v = read_block.get_by_position(4).column->get_int(0);
    ASSERT_EQ(100, seq_v);

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok());
    delete delta_writer;
}

TEST_F(TestDeltaWriter, vec_sequence_col_concurrent_write) {
    RuntimeProfile profile("CreateTablet");
    TCreateTabletReq request;
    sleep(20);
    create_tablet_request_with_sequence_col(10006, 270068377, &request, true);
    Status res = k_engine->create_tablet(request, &profile);
    ASSERT_TRUE(res.ok());

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    OlapTableSchemaParam param;

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = 10006;
    write_req.schema_hash = 270068377;
    write_req.txn_id = 20003;
    write_req.partition_id = 30003;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = &param;
    DeltaWriter* delta_writer1 = nullptr;
    DeltaWriter* delta_writer2 = nullptr;
    std::unique_ptr<RuntimeProfile> profile1;
    profile1 = std::make_unique<RuntimeProfile>("LoadChannels1");
    std::unique_ptr<RuntimeProfile> profile2;
    profile2 = std::make_unique<RuntimeProfile>("LoadChannels2");
    static_cast<void>(DeltaWriter::open(&write_req, &delta_writer1, profile1.get(), TUniqueId()));
    static_cast<void>(DeltaWriter::open(&write_req, &delta_writer2, profile2.get(), TUniqueId()));
    ASSERT_NE(delta_writer1, nullptr);
    ASSERT_NE(delta_writer2, nullptr);

    // write data in delta writer 1
    {
        vectorized::Block block;
        for (const auto& slot_desc : tuple_desc->slots()) {
            block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                           slot_desc->get_data_type_ptr(),
                                                           slot_desc->col_name()));
        }

        generate_data(&block, 10, 123, 100);
        res = delta_writer1->write(&block, {0});
        ASSERT_TRUE(res.ok());

        generate_data(&block, 20, 123, 100);
        res = delta_writer1->write(&block, {1});
        ASSERT_TRUE(res.ok());

        res = delta_writer1->close();
        ASSERT_TRUE(res.ok());
        res = delta_writer1->wait_flush();
        ASSERT_TRUE(res.ok());
        res = delta_writer1->build_rowset();
        ASSERT_TRUE(res.ok());
        res = delta_writer1->submit_calc_delete_bitmap_task();
        ASSERT_TRUE(res.ok());
        res = delta_writer1->wait_calc_delete_bitmap();
        ASSERT_TRUE(res.ok());
        res = delta_writer1->commit_txn(PSlaveTabletNodes(), false);
        ASSERT_TRUE(res.ok());
    }
    // write data in delta writer 2
    {
        vectorized::Block block;
        for (const auto& slot_desc : tuple_desc->slots()) {
            block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                           slot_desc->get_data_type_ptr(),
                                                           slot_desc->col_name()));
        }

        generate_data(&block, 10, 123, 110);
        res = delta_writer2->write(&block, {0});
        ASSERT_TRUE(res.ok());

        generate_data(&block, 20, 123, 90);
        res = delta_writer2->write(&block, {1});
        ASSERT_TRUE(res.ok());

        res = delta_writer2->close();
        ASSERT_TRUE(res.ok());
        res = delta_writer2->wait_flush();
        ASSERT_TRUE(res.ok());
    }
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(write_req.tablet_id);
    std::cout << "before publish, tablet row nums:" << tablet->num_rows() << std::endl;
    OlapMeta* meta = tablet->data_dir()->get_meta();
    RowsetSharedPtr rowset1 = nullptr;
    RowsetSharedPtr rowset2 = nullptr;

    // publish version on delta writer 1 success
    {
        Version version;
        version.first = tablet->rowset_with_max_version()->end_version() + 1;
        version.second = tablet->rowset_with_max_version()->end_version() + 1;
        std::cout << "start to add rowset version:" << version.first << "-" << version.second
                  << std::endl;
        std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
                write_req.txn_id, write_req.partition_id, &tablet_related_rs);
        ASSERT_EQ(1, tablet_related_rs.size());

        std::cout << "start to publish txn" << std::endl;
        rowset1 = tablet_related_rs.begin()->second;
        TabletPublishStatistics pstats;
        res = k_engine->txn_manager()->publish_txn(
                meta, write_req.partition_id, write_req.txn_id, write_req.tablet_id,
                tablet_related_rs.begin()->first.tablet_uid, version, &pstats);
        ASSERT_TRUE(res.ok());
        std::cout << "start to add inc rowset:" << rowset1->rowset_id()
                  << ", num rows:" << rowset1->num_rows()
                  << ", version:" << rowset1->version().first << "-" << rowset1->version().second
                  << std::endl;
        res = tablet->add_inc_rowset(rowset1);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(2, tablet->num_rows());
        std::vector<segment_v2::SegmentSharedPtr> segments;
        res = ((BetaRowset*)rowset1.get())->load_segments(&segments);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(1, rowset1->num_segments());
        ASSERT_EQ(1, segments.size());
    }

    // commit delta writer2, then publish it.
    {
        // commit, calc delete bitmap should happen here
        res = delta_writer2->build_rowset();
        ASSERT_TRUE(res.ok());
        res = delta_writer2->submit_calc_delete_bitmap_task();
        ASSERT_TRUE(res.ok());
        res = delta_writer2->wait_calc_delete_bitmap();
        ASSERT_TRUE(res.ok());

        // verify that delete bitmap calculated correctly
        // since the delete bitmap not published, versions are 0
        auto delete_bitmap = delta_writer2->get_delete_bitmap();
        ASSERT_TRUE(delete_bitmap->contains({rowset1->rowset_id(), 0, 0}, 0));
        // We can't get the rowset id of rowset2 now, will check the delete bitmap
        // contains row 0 of rowset2 at L929.

        res = delta_writer2->commit_txn(PSlaveTabletNodes(), false);
        ASSERT_TRUE(res.ok());

        Version version;
        version.first = tablet->rowset_with_max_version()->end_version() + 1;
        version.second = tablet->rowset_with_max_version()->end_version() + 1;
        std::cout << "start to add rowset version:" << version.first << "-" << version.second
                  << std::endl;
        std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
                write_req.txn_id, write_req.partition_id, &tablet_related_rs);
        ASSERT_EQ(1, tablet_related_rs.size());

        std::cout << "start to publish txn" << std::endl;
        rowset2 = tablet_related_rs.begin()->second;
        ASSERT_TRUE(delete_bitmap->contains({rowset2->rowset_id(), 0, 0}, 1));

        TabletPublishStatistics pstats;
        res = k_engine->txn_manager()->publish_txn(
                meta, write_req.partition_id, write_req.txn_id, write_req.tablet_id,
                tablet_related_rs.begin()->first.tablet_uid, version, &pstats);
        ASSERT_TRUE(res.ok());
        std::cout << "start to add inc rowset:" << rowset2->rowset_id()
                  << ", num rows:" << rowset2->num_rows()
                  << ", version:" << rowset2->version().first << "-" << rowset2->version().second
                  << std::endl;
        res = tablet->add_inc_rowset(rowset2);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(4, tablet->num_rows());
        std::vector<segment_v2::SegmentSharedPtr> segments;
        res = ((BetaRowset*)rowset2.get())->load_segments(&segments);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(1, rowset2->num_segments());
        ASSERT_EQ(1, segments.size());
    }

    auto cur_version = tablet->rowset_with_max_version()->end_version();
    // read data from rowset 1, verify the data correct
    {
        OlapReaderStatistics stats;
        StorageReadOptions opts;
        opts.stats = &stats;
        opts.tablet_schema = rowset1->tablet_schema();
        opts.delete_bitmap.emplace(0, tablet->tablet_meta()->delete_bitmap().get_agg(
                                              {rowset1->rowset_id(), 0, cur_version}));
        std::unique_ptr<RowwiseIterator> iter;
        std::shared_ptr<Schema> schema = std::make_shared<Schema>(rowset1->tablet_schema());
        std::vector<segment_v2::SegmentSharedPtr> segments;
        static_cast<void>(((BetaRowset*)rowset1.get())->load_segments(&segments));
        auto s = segments[0]->new_iterator(schema, opts, &iter);
        ASSERT_TRUE(s.ok());
        auto read_block = rowset1->tablet_schema()->create_block();
        res = iter->next_batch(&read_block);
        ASSERT_TRUE(res.ok()) << res;
        // key of (10, 123) is deleted
        ASSERT_EQ(1, read_block.rows());
        auto k1 = read_block.get_by_position(0).column->get_int(0);
        ASSERT_EQ(20, k1);
        auto k2 = read_block.get_by_position(1).column->get_int(0);
        ASSERT_EQ(123, k2);
        // get the value from sequence column
        auto seq_v = read_block.get_by_position(4).column->get_int(0);
        ASSERT_EQ(100, seq_v);
    }

    // read data from rowset 2, verify the data correct
    {
        OlapReaderStatistics stats;
        StorageReadOptions opts;
        opts.stats = &stats;
        opts.tablet_schema = rowset2->tablet_schema();
        opts.delete_bitmap.emplace(0, tablet->tablet_meta()->delete_bitmap().get_agg(
                                              {rowset2->rowset_id(), 0, cur_version}));
        std::unique_ptr<RowwiseIterator> iter;
        std::shared_ptr<Schema> schema = std::make_shared<Schema>(rowset2->tablet_schema());
        std::vector<segment_v2::SegmentSharedPtr> segments;
        static_cast<void>(((BetaRowset*)rowset2.get())->load_segments(&segments));
        auto s = segments[0]->new_iterator(schema, opts, &iter);
        ASSERT_TRUE(s.ok());
        auto read_block = rowset2->tablet_schema()->create_block();
        res = iter->next_batch(&read_block);
        ASSERT_TRUE(res.ok());
        // key of (20, 123) is deleted, because it's seq value is low
        ASSERT_EQ(1, read_block.rows());
        auto k1 = read_block.get_by_position(0).column->get_int(0);
        ASSERT_EQ(10, k1);
        auto k2 = read_block.get_by_position(1).column->get_int(0);
        ASSERT_EQ(123, k2);
        // get the value from sequence column
        auto seq_v = read_block.get_by_position(4).column->get_int(0);
        ASSERT_EQ(110, seq_v);
    }

    res = k_engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok());
    delete delta_writer1;
    delete delta_writer2;
}
} // namespace doris
