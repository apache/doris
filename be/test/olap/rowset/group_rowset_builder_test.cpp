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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "exec/tablet_info.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/rowset_builder.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = std::string(buffer) + "/data_test";
    auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    engine_ref = engine.get();
    Status s = engine->open();
    ASSERT_TRUE(s.ok()) << s;

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    exec_env->set_storage_engine(std::move(engine));
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    engine_ref = nullptr;
    exec_env->set_storage_engine(nullptr);
    EXPECT_EQ(system("rm -rf ./data_test"), 0);
    static_cast<void>(io::global_local_filesystem()->delete_directory(
            std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX));
}

static void create_tablet_request(int64_t tablet_id, int32_t schema_hash,
                                  TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->partition_id = 10001;
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 1;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->__set_storage_format(TStorageFormat::V2);

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k1);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::INT;
    v1.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v1);
}

static void create_tablet_request_with_row_binlog(int64_t tablet_id, int32_t schema_hash,
                                                  TCreateTabletReq* request) {
    create_tablet_request(tablet_id, schema_hash, request);
    TBinlogConfig binlog_config;
    binlog_config.__set_enable(true);
    binlog_config.__set_binlog_format(TBinlogFormat::ROW);
    request->__set_binlog_config(binlog_config);
    TTabletSchema row_binlog_schema = request->tablet_schema;
    row_binlog_schema.schema_hash = schema_hash + 1;
    request->__set_row_binlog_schema(row_binlog_schema);
}

static TDescriptorTable create_descriptor_tablet() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("v1").column_pos(1).build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

static std::shared_ptr<OlapTableSchemaParam> create_table_schema_param(
        const TDescriptorTable& tdesc_tbl, int64_t index_id, int32_t schema_hash,
        const std::vector<TColumn>& columns) {
    auto param = std::make_shared<OlapTableSchemaParam>();
    TOlapTableSchemaParam tschema;
    tschema.db_id = 1;
    tschema.table_id = 2;
    tschema.version = 0;
    tschema.slot_descs = tdesc_tbl.slotDescriptors;
    tschema.tuple_desc = tdesc_tbl.tupleDescriptors[0];
    tschema.indexes.resize(1);
    tschema.indexes[0].id = index_id;
    tschema.indexes[0].schema_hash = schema_hash;
    tschema.indexes[0].columns_desc = columns;
    for (const auto& col : columns) {
        tschema.indexes[0].columns.push_back(col.column_name);
    }
    Status st = param->init(tschema);
    EXPECT_TRUE(st.ok()) << st;
    return param;
}

class GroupRowsetBuilderTest : public ::testing::Test {
public:
    static void SetUpTestSuite() { set_up(); }
    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(GroupRowsetBuilderTest, buildWithRowBinlogMeta) {
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    create_tablet_request_with_row_binlog(10010, 270068390, &request);
    Status res = engine_ref->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok());

    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(request.tablet_id);
    ASSERT_TRUE(tablet != nullptr);
    auto st = io::global_local_filesystem()->create_directory(tablet->row_binlog_path());
    ASSERT_TRUE(st.ok()) << st;

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    const int64_t index_id = 10001;

    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    auto param = create_table_schema_param(tdesc_tbl, index_id, request.tablet_schema.schema_hash,
                                           request.tablet_schema.columns);

    WriteRequest data_req;
    data_req.tablet_id = request.tablet_id;
    data_req.schema_hash = request.tablet_schema.schema_hash;
    data_req.txn_id = 20010;
    data_req.partition_id = request.partition_id;
    data_req.index_id = index_id;
    data_req.load_id = load_id;
    data_req.table_schema_param = param;
    data_req.write_req_type = WriteRequestType::DATA;

    WriteRequest row_binlog_req = data_req;
    row_binlog_req.schema_hash = request.row_binlog_schema.schema_hash;
    row_binlog_req.write_req_type = WriteRequestType::ROW_BINLOG;

    GroupRowsetBuilder builder(*engine_ref, data_req, row_binlog_req, profile.get());
    ASSERT_TRUE(builder.init().ok());
    ASSERT_TRUE(builder.rowset_writer()->flush().ok());
    ASSERT_TRUE(builder.build_rowset().ok());

    auto row_binlog_meta = builder.row_binlog_builder()->rowset()->rowset_meta();
    auto data_meta = builder.txn_rowset_builder()->rowset()->rowset_meta();
    ASSERT_TRUE(row_binlog_meta->is_row_binlog());
    ASSERT_FALSE(data_meta->is_row_binlog());
    ASSERT_EQ(request.row_binlog_schema.schema_hash, row_binlog_meta->tablet_schema_hash());
    ASSERT_EQ(request.tablet_schema.schema_hash, data_meta->tablet_schema_hash());
    ASSERT_EQ(index_id, row_binlog_meta->index_id());
    ASSERT_EQ(index_id, data_meta->index_id());

    res = engine_ref->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok());
}

} // namespace doris
