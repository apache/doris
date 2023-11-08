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

#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <unistd.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gen_cpp/internal_service.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "olap/delta_writer.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/txn_manager.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class OlapMeta;
struct Slice;

static std::unique_ptr<StorageEngine> k_engine;

static const std::string kTestDir = "ut_dir/tablet_cooldown_test";
static constexpr int64_t kResourceId = 10000;
static constexpr int64_t kStoragePolicyId = 10002;
static constexpr int64_t kTabletId = 10005;
static constexpr int64_t kReplicaId = 10009;
static constexpr int32_t kSchemaHash = 270068377;

static constexpr int32_t kTxnId = 20003;
static constexpr int32_t kPartitionId = 30003;

using io::Path;

static io::RemoteFileSystemSPtr s_fs;

static std::string get_remote_path(const Path& path) {
    return fmt::format("{}/remote/{}", config::storage_root_path, path.string());
}

class FileWriterMock : public io::FileWriter {
public:
    FileWriterMock(Path path) : io::FileWriter(std::move(path), io::global_local_filesystem()) {
        Status st = io::global_local_filesystem()->create_file(get_remote_path(_path),
                                                               &_local_file_writer);
        if (!st.ok()) {
            std::cerr << "create file writer failed: " << st << std::endl;
        }
    }

    ~FileWriterMock() override = default;

    Status close() override { return _local_file_writer->close(); }

    Status abort() override { return _local_file_writer->abort(); }

    Status appendv(const Slice* data, size_t data_cnt) override {
        return _local_file_writer->appendv(data, data_cnt);
    }

    Status write_at(size_t offset, const Slice& data) override {
        return _local_file_writer->write_at(offset, data);
    }

    Status finalize() override { return _local_file_writer->finalize(); }

private:
    std::unique_ptr<io::FileWriter> _local_file_writer;
};

class RemoteFileSystemMock : public io::RemoteFileSystem {
public:
    RemoteFileSystemMock(Path root_path, std::string&& id, io::FileSystemType type)
            : RemoteFileSystem(std::move(root_path), std::move(id), type) {
        _local_fs = io::LocalFileSystem::create(get_remote_path(_root_path));
    }
    ~RemoteFileSystemMock() override = default;

protected:
    Status create_file_impl(const Path& path, io::FileWriterPtr* writer,
                            const io::FileWriterOptions* opts = nullptr) override {
        Path fs_path = path;
        *writer = std::make_unique<FileWriterMock>(fs_path);
        return Status::OK();
    }

    Status create_directory_impl(const Path& path, bool failed_if_exists) override {
        return _local_fs->create_directory(get_remote_path(path));
    }

    Status delete_file_impl(const Path& path) override {
        return _local_fs->delete_file(get_remote_path(path));
    }

    Status batch_delete_impl(const std::vector<Path>& paths) override {
        for (int i = 0; i < paths.size(); ++i) {
            RETURN_IF_ERROR(delete_file(paths[i]));
        }
        return Status::OK();
    }

    Status delete_directory_impl(const Path& path) override {
        return _local_fs->delete_directory(get_remote_path(path));
    }

    Status exists_impl(const Path& path, bool* res) const override {
        return _local_fs->exists(get_remote_path(path), res);
    }

    Status file_size_impl(const Path& path, int64_t* file_size) const override {
        return _local_fs->file_size(get_remote_path(path), file_size);
    }

    Status list_impl(const Path& dir, bool regular_file, std::vector<io::FileInfo>* files,
                     bool* exists) override {
        RETURN_IF_ERROR(_local_fs->list(get_remote_path(dir), true, files, exists));
        // for (auto& path : local_paths) {
        //     files->emplace_back(path.file_name.substr(config::storage_root_path.size() + 1));
        // }
        return Status::OK();
    }

    Status upload_impl(const Path& local_path, const Path& dest_path) override {
        return _local_fs->link_file(local_path, get_remote_path(dest_path));
    }

    Status batch_upload_impl(const std::vector<Path>& local_paths,
                             const std::vector<Path>& dest_paths) override {
        for (int i = 0; i < local_paths.size(); ++i) {
            RETURN_IF_ERROR(upload_impl(local_paths[i], dest_paths[i]));
        }
        return Status::OK();
    }

    Status direct_upload_impl(const Path& remote_file, const std::string& content) override {
        return Status::OK();
    }

    Status upload_with_checksum_impl(const Path& local_file, const Path& remote_file,
                                     const std::string& checksum) override {
        return Status::OK();
    }

    Status download_impl(const Path& remote_file, const Path& local_file) override {
        return Status::OK();
    }

    Status direct_download_impl(const Path& remote_file, std::string* content) override {
        return Status::OK();
    }

    Status open_file_internal(const Path& file, io::FileReaderSPtr* reader,
                              const io::FileReaderOptions& opts) override {
        auto path = get_remote_path(file);
        return _local_fs->open_file(path, reader);
    }

    Status connect_impl() override { return Status::OK(); }

    Status rename_impl(const Path& orig_name, const Path& new_name) override {
        return Status::OK();
    }

    Status rename_dir_impl(const Path& orig_name, const Path& new_name) override {
        return Status::OK();
    }

private:
    std::shared_ptr<io::LocalFileSystem> _local_fs;
};

class TabletCooldownTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        s_fs.reset(
                new RemoteFileSystemMock("", std::to_string(kResourceId), io::FileSystemType::S3));
        StorageResource resource = {s_fs, 1};
        put_storage_resource(kResourceId, resource);
        auto storage_policy = std::make_shared<StoragePolicy>();
        storage_policy->name = "TabletCooldownTest";
        storage_policy->version = 1;
        storage_policy->resource_id = kResourceId;
        put_storage_policy(kStoragePolicyId, storage_policy);

        constexpr uint32_t MAX_PATH_LEN = 1024;
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = std::string(buffer) + "/" + kTestDir;
        config::min_file_descriptor_number = 1000;

        EXPECT_TRUE(io::global_local_filesystem()
                            ->delete_and_create_directory(config::storage_root_path)
                            .ok());
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(get_remote_path(remote_tablet_path(kTabletId)))
                            .ok());

        std::vector<StorePath> paths {{config::storage_root_path, -1}};

        EngineOptions options;
        options.store_paths = paths;
        k_engine = std::make_unique<StorageEngine>(options);
        auto st = k_engine->open();
        EXPECT_TRUE(st.ok()) << st.to_string();
        ExecEnv* exec_env = doris::ExecEnv::GetInstance();
        exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
        exec_env->set_storage_engine(k_engine.get());
    }

    static void TearDownTestSuite() {
        ExecEnv* exec_env = doris::ExecEnv::GetInstance();
        exec_env->set_storage_engine(nullptr);
        exec_env->set_memtable_memory_limiter(nullptr);
        k_engine.reset();
    }
};

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
}

static TDescriptorTable create_descriptor_tablet_with_sequence_col() {
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("k2").column_pos(1).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name(SEQUENCE_COL)
                                   .column_pos(2)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATETIME)
                                   .column_name("v1")
                                   .column_pos(3)
                                   .nullable(false)
                                   .build());
    tuple_builder.build(&desc_tbl_builder);

    return desc_tbl_builder.desc_tbl();
}

void createTablet(TabletSharedPtr* tablet, int64_t replica_id, int32_t schema_hash,
                  int64_t tablet_id, int64_t txn_id, int64_t partition_id) {
    // create tablet
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    create_tablet_request_with_sequence_col(tablet_id, schema_hash, &request);
    request.__set_replica_id(replica_id);
    Status st = k_engine->create_tablet(request, profile.get());
    ASSERT_EQ(Status::OK(), st);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    OlapTableSchemaParam param;

    // write data
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
    write_req.table_schema_param = &param;

    DeltaWriter* delta_writer = nullptr;
    profile = std::make_unique<RuntimeProfile>("LoadChannels");
    static_cast<void>(DeltaWriter::open(&write_req, &delta_writer, profile.get()));
    ASSERT_NE(delta_writer, nullptr);

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

        VecDateTimeValue c4;
        c4.from_date_str("2020-07-16 19:39:43", 19);
        int64_t c4_int = c4.to_int64();
        columns[3]->insert_data((const char*)&c4_int, sizeof(c4));

        st = delta_writer->write(&block, {0});
        ASSERT_EQ(Status::OK(), st);
    }

    st = delta_writer->close();
    ASSERT_EQ(Status::OK(), st);
    st = delta_writer->build_rowset();
    ASSERT_EQ(Status::OK(), st);
    st = delta_writer->commit_txn(PSlaveTabletNodes(), false);
    ASSERT_EQ(Status::OK(), st);
    delete delta_writer;

    // publish version success
    *tablet = k_engine->tablet_manager()->get_tablet(write_req.tablet_id, write_req.schema_hash);
    OlapMeta* meta = (*tablet)->data_dir()->get_meta();
    Version version;
    version.first = (*tablet)->rowset_with_max_version()->end_version() + 1;
    version.second = (*tablet)->rowset_with_max_version()->end_version() + 1;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    k_engine->txn_manager()->get_txn_related_tablets(write_req.txn_id, write_req.partition_id,
                                                     &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        RowsetSharedPtr rowset = tablet_rs.second;
        TabletPublishStatistics stats;
        st = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                  (*tablet)->tablet_id(), (*tablet)->tablet_uid(),
                                                  version, &stats);
        ASSERT_EQ(Status::OK(), st);
        st = (*tablet)->add_inc_rowset(rowset);
        ASSERT_EQ(Status::OK(), st);
    }
    EXPECT_EQ(1, (*tablet)->num_rows());
}

TEST_F(TabletCooldownTest, normal) {
    TabletSharedPtr tablet1;
    TabletSharedPtr tablet2;
    createTablet(&tablet1, kReplicaId, kSchemaHash, kTabletId, kTxnId, kPartitionId);
    // test cooldown
    tablet1->set_storage_policy_id(kStoragePolicyId);
    Status st = tablet1->cooldown(); // rowset [0-1]
    ASSERT_NE(Status::OK(), st);
    tablet1->update_cooldown_conf(1, kReplicaId);
    // cooldown for upload node
    st = tablet1->cooldown(); // rowset [0-1]
    ASSERT_EQ(Status::OK(), st);
    st = tablet1->cooldown(); // rowset [2-2]
    ASSERT_EQ(Status::OK(), st);
    auto rs = tablet1->get_rowset_by_version({2, 2});
    ASSERT_FALSE(rs->is_local());

    // test read
    ASSERT_EQ(Status::OK(), st);
    std::vector<segment_v2::SegmentSharedPtr> segments;
    st = std::static_pointer_cast<BetaRowset>(rs)->load_segments(&segments);
    ASSERT_EQ(Status::OK(), st);
    ASSERT_EQ(segments.size(), 1);
}

} // namespace doris
