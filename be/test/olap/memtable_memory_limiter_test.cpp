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

#include "olap/memtable_memory_limiter.h"

#include "exec/tablet_info.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/delta_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"

namespace doris {
static const uint32_t MAX_PATH_LEN = 1024;

static void create_tablet_request(int64_t tablet_id, int32_t schema_hash,
                                  TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 3;
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

    tuple_builder.build(&dtb);
    return dtb.desc_tbl();
}

class MemTableMemoryLimiterTest : public testing::Test {
protected:
    void SetUp() override {
        // set path
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = std::string(buffer) + "/data_test";
        static_cast<void>(io::global_local_filesystem()->delete_and_create_directory(
                config::storage_root_path));
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        doris::EngineOptions options;
        options.store_paths = paths;
        _engine = std::make_unique<StorageEngine>(options);
        Status st = _engine->open();
        EXPECT_TRUE(st.ok()) << st.to_string();

        ExecEnv* exec_env = doris::ExecEnv::GetInstance();
        // ExecEnv's storage_engine will be read by storage_engine's other operations.
        // So we must do this before storage engine's other operation.
        exec_env->set_storage_engine(_engine.get());
        exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
        static_cast<void>(_engine->start_bg_threads());
    }

    void TearDown() override {
        ExecEnv* exec_env = doris::ExecEnv::GetInstance();
        exec_env->set_memtable_memory_limiter(nullptr);
        exec_env->set_storage_engine(nullptr);
        _engine.reset(nullptr);
        EXPECT_EQ(system("rm -rf ./data_test"), 0);
        static_cast<void>(io::global_local_filesystem()->delete_directory(
                std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX));
    }

    std::unique_ptr<StorageEngine> _engine;
};

TEST_F(MemTableMemoryLimiterTest, handle_memtable_flush_test) {
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    create_tablet_request(10000, 270068372, &request);
    Status res = _engine->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok());

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
    write_req.tablet_id = 10000;
    write_req.schema_hash = 270068372;
    write_req.txn_id = 20002;
    write_req.partition_id = 30002;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = &param;
    DeltaWriter* delta_writer = nullptr;
    profile = std::make_unique<RuntimeProfile>("MemTableMemoryLimiterTest");
    static_cast<void>(DeltaWriter::open(&write_req, &delta_writer, profile.get(), TUniqueId()));
    ASSERT_NE(delta_writer, nullptr);
    auto mem_limiter = ExecEnv::GetInstance()->memtable_memory_limiter();

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

        res = delta_writer->write(&block, {0});
        ASSERT_TRUE(res.ok());
    }
    static_cast<void>(mem_limiter->init(100));
    mem_limiter->handle_memtable_flush();
    CHECK_EQ(0, mem_limiter->mem_usage());

    res = delta_writer->close();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->build_rowset();
    EXPECT_EQ(Status::OK(), res);
    res = delta_writer->commit_txn(PSlaveTabletNodes(), false);
    EXPECT_EQ(Status::OK(), res);
    res = _engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    EXPECT_EQ(Status::OK(), res);
    delete delta_writer;
}
} // namespace doris
