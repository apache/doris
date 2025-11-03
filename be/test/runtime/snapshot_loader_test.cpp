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

#include "runtime/snapshot_loader.h"

#include <fmt/core.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <gtest/gtest_pred_impl.h>

#include <boost/algorithm/string/replace.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <system_error>
#include <utility>

#include "common/config.h"
#include "common/object_pool.h"
#include "exec/tablet_info.h"
#include "http/action/download_action.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/delta_writer.h"
#include "olap/iterators.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/segment_loader.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/txn_manager.h"
#include "runtime/cluster_info.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"

namespace doris {

static std::string storage_root_path;

class MockDownloadHandler : public HttpHandler {
public:
    MockDownloadHandler() = default;
    void handle(HttpRequest* req) override {
        std::vector<std::string> allow_path {fmt::format("{}", storage_root_path)};
        DownloadAction action(ExecEnv::GetInstance(), nullptr, allow_path);
        action.handle(req);
    }
};

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;
static EvHttpServer* s_server = nullptr;
static MockDownloadHandler mock_download_handler;
static std::string hostname;
static std::string address;
static ClusterInfo cluster_info;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    storage_root_path = std::string(buffer) + "/snapshot_data_test";
    auto st = io::global_local_filesystem()->delete_directory(storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    std::vector<StorePath> paths;
    paths.emplace_back(storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    options.backend_uid = UniqueId::gen_uid();
    auto engine = std::make_unique<StorageEngine>(options);
    engine_ref = engine.get();
    Status s = engine->open();
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_TRUE(s.ok()) << s;

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    cluster_info.token = "fake_token";
    exec_env->set_cluster_info(&cluster_info);
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    exec_env->set_storage_engine(std::move(engine));
    s_server = new EvHttpServer(1234, 3);
    s_server->register_handler(GET, "/api/_tablet/_download", &mock_download_handler);
    s_server->register_handler(POST, "/api/_tablet/_download", &mock_download_handler);
    s_server->register_handler(HEAD, "/api/_tablet/_download", &mock_download_handler);

    static_cast<void>(s_server->start());
    address = "127.0.0.1:" + std::to_string(1234);
    hostname = "http://" + address;
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    engine_ref = nullptr;
    exec_env->set_storage_engine(nullptr);

    if (storage_root_path.empty()) {
        return;
    }

    Status st = io::global_local_filesystem()->delete_directory(storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    delete s_server;
    // Status s = io::global_local_filesystem()->delete_directory(storage_root_path);
    // EXPECT_TRUE(s.ok()) << "delete directory " << s;
}

static TCreateTabletReq create_tablet(int64_t partition_id, int64_t tablet_id,
                                      int32_t schema_hash) {
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(schema_hash);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(tablet_id);
    create_tablet_req.__set_partition_id(partition_id);
    create_tablet_req.__set_version(2);
    return create_tablet_req;
}

static TDescriptorTable create_descriptor_tablet() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("col1").column_pos(0).build());
    tuple_builder.build(&dtb);
    return dtb.desc_tbl();
}

static void add_rowset(int64_t tablet_id, int32_t schema_hash, int64_t partition_id, int64_t txn_id,
                       int16_t value) {
    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    auto param = std::make_shared<OlapTableSchemaParam>();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = tablet_id;
    write_req.schema_hash = schema_hash;
    write_req.txn_id = txn_id;
    write_req.partition_id = partition_id;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = param;
    auto profile = std::make_unique<RuntimeProfile>("LoadChannels");
    auto delta_writer =
            std::make_unique<DeltaWriter>(*engine_ref, write_req, profile.get(), TUniqueId {});

    vectorized::Block block;
    for (const auto& slot_desc : tuple_desc->slots()) {
        std::cout << "slot_desc: " << slot_desc->col_name() << std::endl;
        block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                       slot_desc->type(), slot_desc->col_name()));
    }

    std::cout << "total column " << block.mutate_columns().size() << std::endl;
    auto columns = block.mutate_columns();
    int16_t c1 = value;
    columns[0]->insert_data((const char*)&c1, sizeof(c1));
    Status res = delta_writer->write(&block, {0});
    EXPECT_TRUE(res.ok());

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
    res = delta_writer->commit_txn(PSlaveTabletNodes());
    ASSERT_TRUE(res.ok()) << res;

    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    ASSERT_TRUE(tablet != nullptr);

    std::cout << "before publish, tablet row nums:" << tablet->num_rows() << std::endl;
    Version version;
    version.first = tablet->get_rowset_with_max_version()->end_version() + 1;
    version.second = tablet->get_rowset_with_max_version()->end_version() + 1;
    std::cout << "start to add rowset version:" << version.first << "-" << version.second
              << std::endl;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    engine_ref->txn_manager()->get_txn_related_tablets(txn_id, partition_id, &tablet_related_rs);
    ASSERT_EQ(1, tablet_related_rs.size());

    std::cout << "start to publish txn" << std::endl;
    RowsetSharedPtr rowset = tablet_related_rs.begin()->second;

    TabletPublishStatistics stats;
    std::shared_ptr<TabletTxnInfo> extend_tablet_txn_info_lifetime = nullptr;
    res = engine_ref->txn_manager()->publish_txn(partition_id, tablet, txn_id, version, &stats,
                                                 extend_tablet_txn_info_lifetime);
    ASSERT_TRUE(res.ok()) << res;
    std::cout << "start to add inc rowset:" << rowset->rowset_id()
              << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
              << "-" << rowset->version().second << std::endl;
    res = tablet->add_inc_rowset(rowset);
    ASSERT_TRUE(res.ok()) << res;
}

class SnapshotLoaderTest : public ::testing::Test {
public:
    SnapshotLoaderTest() {}
    ~SnapshotLoaderTest() {}
    static void SetUpTestSuite() { set_up(); }

    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(SnapshotLoaderTest, NormalCase) {
    StorageEngine engine({});
    SnapshotLoader loader(engine, ExecEnv::GetInstance(), 1L, 2L);

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    Status st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678",
                                                                     &tablet_id, &schema_hash);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(1234, tablet_id);
    EXPECT_EQ(5678, schema_hash);

    st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678/", &tablet_id,
                                                              &schema_hash);
    EXPECT_FALSE(st.ok());

    std::filesystem::remove_all("./ss_test/");
    std::map<std::string, std::string> src_to_dest;
    src_to_dest["./ss_test/"] = "./ss_test";
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    EXPECT_FALSE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    EXPECT_FALSE(st.ok());

    std::filesystem::create_directory("./ss_test/");
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    EXPECT_TRUE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    EXPECT_TRUE(st.ok());
    std::filesystem::remove_all("./ss_test/");

    std::filesystem::create_directory("./ss_test/");
    std::vector<std::string> files;
    st = loader._get_existing_files_from_local("./ss_test/", &files);
    EXPECT_EQ(0, files.size());
    std::filesystem::remove_all("./ss_test/");

    std::string new_name;
    st = loader._replace_tablet_id("12345.hdr", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("5678.hdr", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.dat", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("1234_2_5_12345_1.dat", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.idx", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("1234_2_5_12345_1.idx", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.xxx", 5678, &new_name);
    EXPECT_FALSE(st.ok());

    st = loader._get_tablet_id_from_remote_path("/__tbl_10004/__part_10003/__idx_10004/__10005",
                                                &tablet_id);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(10005, tablet_id);
}

TEST_F(SnapshotLoaderTest, DirMoveTaskIsIdempotent) {
    // 1. create a tablet
    int64_t tablet_id = 111;
    int32_t schema_hash = 222;
    int64_t partition_id = 333;
    TCreateTabletReq req = create_tablet(partition_id, tablet_id, schema_hash);
    RuntimeProfile profile("CreateTablet");
    Status status = engine_ref->create_tablet(req, &profile);
    EXPECT_TRUE(status.ok());
    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    EXPECT_TRUE(tablet != nullptr);

    // 2. add a rowset
    add_rowset(tablet_id, schema_hash, partition_id, 100, 100);
    auto version = tablet->max_version();
    std::cout << "version: " << version.first << ", " << version.second << std::endl;

    // 3. make a snapshot
    std::string snapshot_path;
    bool allow_incremental_clone = false; // not used
    TSnapshotRequest snapshot_request;
    snapshot_request.tablet_id = tablet_id;
    snapshot_request.schema_hash = schema_hash;
    snapshot_request.version = version.second;
    status = engine_ref->snapshot_mgr()->make_snapshot(snapshot_request, &snapshot_path,
                                                       &allow_incremental_clone);
    ASSERT_TRUE(status.ok());

    // 4. load the snapshot to another tablet
    snapshot_path = fmt::format("{}/{}/{}", snapshot_path, tablet_id, schema_hash);
    SnapshotLoader loader1(*engine_ref, ExecEnv::GetInstance(), 1L, tablet_id);
    status = loader1.move(snapshot_path, tablet, true);
    ASSERT_TRUE(status.ok()) << status;

    // 5. Insert a rowset to the tablet
    // reload tablet
    tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    EXPECT_TRUE(tablet != nullptr);
    add_rowset(tablet_id, schema_hash, partition_id, 200, 200);
    version = tablet->max_version();
    std::cout << "version: " << version.first << ", " << version.second << std::endl;

    // 6. load the snapshot to the tablet again, this request should be idempotent
    SnapshotLoader loader2(*engine_ref, ExecEnv::GetInstance(), 2L, tablet_id);
    status = loader2.move(snapshot_path, tablet, true);
    ASSERT_TRUE(status.ok()) << status;

    // reload tablet
    tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    EXPECT_TRUE(tablet != nullptr);
    auto last_version = tablet->max_version();
    std::cout << "last version: " << last_version.first << ", " << last_version.second << std::endl;
    ASSERT_EQ(version.first, last_version.first);
    ASSERT_EQ(version.second, last_version.second);
}
TEST_F(SnapshotLoaderTest, TestLinkSameRowsetFiles) {
    // 1. Create a tablet
    int64_t tablet_id = 222;
    int32_t schema_hash = 333;
    int64_t partition_id = 444;
    TCreateTabletReq req = create_tablet(partition_id, tablet_id, schema_hash);
    RuntimeProfile profile("CreateTablet");
    Status status = engine_ref->create_tablet(req, &profile);
    EXPECT_TRUE(status.ok());
    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    EXPECT_TRUE(tablet != nullptr);

    // 2. Add a rowset to the tablet
    add_rowset(tablet_id, schema_hash, partition_id, 100, 100);
    auto version = tablet->max_version();
    std::cout << "Original version: " << version.first << ", " << version.second << std::endl;

    // 3. Make a snapshot of the tablet
    std::string snapshot_path;
    bool allow_incremental_clone = false;
    TSnapshotRequest snapshot_request;
    snapshot_request.tablet_id = tablet_id;
    snapshot_request.schema_hash = schema_hash;
    snapshot_request.version = version.second;
    status = engine_ref->snapshot_mgr()->make_snapshot(snapshot_request, &snapshot_path,
                                                       &allow_incremental_clone);
    ASSERT_TRUE(status.ok());
    std::cout << "snapshot_path: " << snapshot_path << std::endl;
    snapshot_path = fmt::format("{}/{}/{}", snapshot_path, tablet_id, schema_hash);

    // 4. Create a destination path for "remote" snapshot
    std::string remote_snapshot_dir = storage_root_path + "/remote_snapshot";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(remote_snapshot_dir).ok());
    std::string remote_tablet_path =
            fmt::format("{}/{}/{}", remote_snapshot_dir, tablet_id, schema_hash);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(remote_tablet_path).ok());

    // 5. Copy snapshot files to remote path and calls convert_rowset_ids
    std::vector<io::FileInfo> snapshot_files;
    bool is_exists = false;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->list(snapshot_path, true, &snapshot_files, &is_exists)
                        .ok());
    for (const auto& file : snapshot_files) {
        std::string src_file = snapshot_path + "/" + file.file_name;
        std::string dst_file = remote_tablet_path + "/" + file.file_name;
        ASSERT_TRUE(io::global_local_filesystem()->copy_path(src_file, dst_file).ok());
    }

    int64_t dest_tablet_id = 333;
    int32_t dest_schema_hash = 444;
    std::string dest_path = fmt::format("{}/dest_snapshot/{}/{}", storage_root_path, dest_tablet_id,
                                        dest_schema_hash);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dest_path).ok());

    std::string src_hdr = remote_tablet_path + "/" + std::to_string(tablet_id) + ".hdr";
    std::string dst_hdr = remote_tablet_path + "/" + std::to_string(dest_tablet_id) + ".hdr";
    ASSERT_TRUE(io::global_local_filesystem()->rename(src_hdr, dst_hdr).ok());
    auto guards = engine_ref->snapshot_mgr()->convert_rowset_ids(
            remote_tablet_path, dest_tablet_id, 0, 0, partition_id, dest_schema_hash);

    // 7. Setup a remote tablet snapshot for download
    TRemoteTabletSnapshot remote_snapshot;
    remote_snapshot.remote_tablet_id = dest_tablet_id;
    remote_snapshot.local_tablet_id = tablet_id;
    remote_snapshot.local_snapshot_path = snapshot_path;
    remote_snapshot.remote_snapshot_path = remote_tablet_path;
    remote_snapshot.remote_be_addr.hostname = "127.0.0.1";
    remote_snapshot.remote_be_addr.port = 1234;
    remote_snapshot.remote_token = "fake_token";

    // 8. Download the snapshot
    std::vector<TRemoteTabletSnapshot> remote_snapshots = {remote_snapshot};
    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 3L, tablet_id);
    status = loader.remote_http_download(remote_snapshots, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok());

    // 9. Verify skip download files
    ASSERT_EQ(loader.get_http_download_files_num(), 0);
}
} // namespace doris
