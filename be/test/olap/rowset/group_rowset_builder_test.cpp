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
#include "io/fs/local_file_system.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/binlog.h"
#include "storage/data_dir.h"
#include "storage/rowset_builder.h"
#include "storage/segment/segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet_info.h"
#include "testutil/creators.h"

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

class GroupRowsetBuilderTest : public ::testing::Test {
public:
    static void SetUpTestSuite() { set_up(); }
    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(GroupRowsetBuilderTest, buildWithRowBinlogMeta) {
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("CreateTablet");
    auto request = testutil::create_tablet_request(
            10010, 270068390, 10001, 1, TKeysType::UNIQUE_KEYS,
            {{"k1", TPrimitiveType::INT, true}, {"v1", TPrimitiveType::INT, false}});
    request.__set_enable_unique_key_merge_on_write(true);
    testutil::enable_row_binlog(&request);
    Status res = engine_ref->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok());

    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(request.tablet_id);
    ASSERT_TRUE(tablet != nullptr);

    auto row_binlog_request = request;
    row_binlog_request.tablet_id = 10011;
    auto row_binlog_tablet_schema = testutil::create_row_binlog_tablet_schema(
            request.tablet_schema, request.tablet_schema.schema_hash + 1);
    row_binlog_request.tablet_schema = row_binlog_tablet_schema;
    res = engine_ref->create_tablet(row_binlog_request, profile.get());
    ASSERT_TRUE(res.ok());
    TabletSharedPtr row_binlog_tablet =
            engine_ref->tablet_manager()->get_tablet(row_binlog_request.tablet_id);
    ASSERT_TRUE(row_binlog_tablet != nullptr);

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    const int64_t index_id = 10001;
    const int64_t row_binlog_index_id = 10002;

    TDescriptorTable tdesc_tbl =
            testutil::create_descriptor_table({{TYPE_INT, "k1", false}, {TYPE_INT, "v1", false}});
    auto param = testutil::create_table_schema_param(
            tdesc_tbl, index_id, request.tablet_schema.schema_hash, request.tablet_schema.columns,
            row_binlog_index_id, row_binlog_tablet_schema.schema_hash,
            &row_binlog_tablet_schema.columns);
    ASSERT_NE(param, nullptr);

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
    row_binlog_req.tablet_id = row_binlog_request.tablet_id;
    row_binlog_req.index_id = row_binlog_index_id;
    row_binlog_req.schema_hash = row_binlog_tablet_schema.schema_hash;
    row_binlog_req.write_req_type = WriteRequestType::ROW_BINLOG;

    WriteRequest group_req = data_req;
    group_req.write_req_type = WriteRequestType::GROUP;

    GroupRowsetBuilder builder(*engine_ref, group_req, data_req, row_binlog_req, profile.get());
    ASSERT_TRUE(builder.init().ok());
    ASSERT_TRUE(builder.rowset_writer()->flush().ok());
    ASSERT_TRUE(builder.build_rowset().ok());

    auto row_binlog_meta = builder.row_binlog_builder()->rowset()->rowset_meta();
    auto data_meta = builder.txn_rowset_builder()->rowset()->rowset_meta();
    ASSERT_TRUE(row_binlog_meta->is_row_binlog());
    ASSERT_FALSE(data_meta->is_row_binlog());
    ASSERT_EQ(row_binlog_tablet_schema.schema_hash, row_binlog_meta->tablet_schema_hash());
    ASSERT_EQ(request.tablet_schema.schema_hash, data_meta->tablet_schema_hash());
    ASSERT_EQ(row_binlog_index_id, row_binlog_meta->index_id());
    ASSERT_EQ(index_id, data_meta->index_id());

    // Row-binlog schema must contain LSN column so that RowBinlogSegmentWriter can locate it.
    ASSERT_GE(row_binlog_meta->tablet_schema()->binlog_lsn_col_idx(), 0);

    res = engine_ref->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok());
    res = engine_ref->tablet_manager()->drop_tablet(row_binlog_request.tablet_id,
                                                    row_binlog_request.replica_id, false);
    ASSERT_TRUE(res.ok());
}

} // namespace doris
