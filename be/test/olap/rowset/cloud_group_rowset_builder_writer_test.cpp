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
#include <unistd.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "core/block/block.h"
#include "core/field.h"
#include "exec/sink/autoinc_buffer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "storage/binlog.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_policy.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/creators.h"
#include "util/uid_util.h"

namespace doris {
namespace {

constexpr uint32_t MAX_PATH_LEN = 1024;
constexpr std::string_view kStorageRootDir = "/ut_dir/cloud_group_rowset_builder_writer_test";
constexpr int64_t kDataTabletId = 10010;
constexpr int64_t kRowBinlogTabletId = 10011;
constexpr int64_t kDataIndexId = 10001;
constexpr int64_t kRowBinlogIndexId = 10002;

class LocalRemoteFileSystem final : public io::RemoteFileSystem {
public:
    explicit LocalRemoteFileSystem(std::string root_path)
            : RemoteFileSystem(std::move(root_path), "cloud_group_rowset_test_fs",
                               io::FileSystemType::BROKER) {}

private:
    Status create_file_impl(const io::Path& file, io::FileWriterPtr* writer,
                            const io::FileWriterOptions* opts) override {
        return io::global_local_filesystem()->create_file(file, writer, opts);
    }

    Status create_directory_impl(const io::Path& dir, bool failed_if_exists) override {
        return io::global_local_filesystem()->create_directory(dir, failed_if_exists);
    }

    Status delete_file_impl(const io::Path& file) override {
        return io::global_local_filesystem()->delete_file(file);
    }

    Status batch_delete_impl(const std::vector<io::Path>& files) override {
        return io::global_local_filesystem()->batch_delete(files);
    }

    Status delete_directory_impl(const io::Path& dir) override {
        return io::global_local_filesystem()->delete_directory(dir);
    }

    Status exists_impl(const io::Path& path, bool* res) const override {
        return io::global_local_filesystem()->exists(path, res);
    }

    Status file_size_impl(const io::Path& file, int64_t* file_size) const override {
        return io::global_local_filesystem()->file_size(file, file_size);
    }

    Status list_impl(const io::Path& dir, bool only_file, std::vector<io::FileInfo>* files,
                     bool* exists) override {
        return io::global_local_filesystem()->list(dir, only_file, files, exists);
    }

    Status rename_impl(const io::Path& orig_name, const io::Path& new_name) override {
        return io::global_local_filesystem()->rename(orig_name, new_name);
    }

    Status upload_impl(const io::Path& local_file, const io::Path& remote_file) override {
        return io::global_local_filesystem()->link_file(local_file, remote_file);
    }

    Status batch_upload_impl(const std::vector<io::Path>& local_files,
                             const std::vector<io::Path>& remote_files) override {
        DCHECK_EQ(local_files.size(), remote_files.size());
        for (size_t i = 0; i < local_files.size(); ++i) {
            RETURN_IF_ERROR(upload_impl(local_files[i], remote_files[i]));
        }
        return Status::OK();
    }

    Status download_impl(const io::Path& remote_file, const io::Path& local_file) override {
        return io::global_local_filesystem()->link_file(remote_file, local_file);
    }

    Status open_file_internal(const io::Path& file, io::FileReaderSPtr* reader,
                              const io::FileReaderOptions& opts) override {
        return io::global_local_filesystem()->open_file(file, reader, &opts);
    }
};

TabletMetaSharedPtr create_tablet_meta(const TCreateTabletReq& request, bool is_row_binlog_tablet) {
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    for (uint32_t col_idx = 0; col_idx < request.tablet_schema.columns.size(); ++col_idx) {
        col_idx_to_unique_id[col_idx] = col_idx;
    }
    auto tablet_meta = TabletMeta::create(request, TabletUid::gen_uid(), 0,
                                          cast_set<uint32_t>(request.tablet_schema.columns.size()),
                                          col_idx_to_unique_id);
    tablet_meta->set_is_row_binlog_tablet(is_row_binlog_tablet);
    return tablet_meta;
}

void add_initial_rowset(const CloudTabletSPtr& tablet, int64_t version) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_rowset_type(BETA_ROWSET);
    rs_meta->set_rowset_state(VISIBLE);
    rs_meta->set_tablet_id(tablet->tablet_id());
    rs_meta->set_tablet_schema_hash(tablet->schema_hash());
    rs_meta->set_version(Version(0, version));
    rs_meta->set_segments_overlap(OVERLAP_UNKNOWN);
    rs_meta->set_tablet_schema(tablet->tablet_schema());
    RowsetSharedPtr rowset;
    ASSERT_TRUE(RowsetFactory::create_rowset(tablet->tablet_schema(), "", rs_meta, &rowset).ok());
    std::unique_lock<std::shared_mutex> meta_lock(tablet->get_header_lock());
    tablet->add_rowsets({rowset}, false, meta_lock);
    tablet->set_cumulative_layer_point(version + 1);
}

} // namespace

class CloudGroupRowsetBuilderWriterTest : public testing::Test {
protected:
    void SetUp() override {
        config::cloud_mow_sync_rowsets_when_load_txn_begin = false;

        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _storage_root_path = std::string(buffer) + std::string(kStorageRootDir);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_storage_root_path).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_storage_root_path).ok());
        ASSERT_TRUE(
                io::global_local_filesystem()->create_directory(_storage_root_path + "/data").ok());

        _engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
        _engine->init_calc_delete_bitmap_executor_for_UT();
        _engine->set_latest_fs(std::make_shared<LocalRemoteFileSystem>(_storage_root_path));

        _request = testutil::create_tablet_request(
                kDataTabletId, 270068390, 10001, 1, TKeysType::UNIQUE_KEYS,
                {{"k1", TPrimitiveType::INT, true}, {"v1", TPrimitiveType::INT, false}});
        _request.__set_enable_unique_key_merge_on_write(true);
        testutil::enable_row_binlog(&_request);

        _row_binlog_request = _request;
        _row_binlog_request.tablet_id = kRowBinlogTabletId;
        _row_binlog_request.tablet_schema = testutil::create_row_binlog_tablet_schema(
                _request.tablet_schema, _request.tablet_schema.schema_hash + 1);
        _row_binlog_request.__set_is_row_binlog_tablet(true);

        auto data_meta = create_tablet_meta(_request, false);
        auto row_binlog_meta = create_tablet_meta(_row_binlog_request, true);
        _tablet = std::make_shared<CloudTablet>(*_engine, data_meta);
        _row_binlog_tablet = std::make_shared<CloudTablet>(*_engine, row_binlog_meta);
        add_initial_rowset(_tablet, _request.version);
        add_initial_rowset(_row_binlog_tablet, _row_binlog_request.version);
        _engine->tablet_mgr().put_tablet_for_UT(_tablet);
        _engine->tablet_mgr().put_tablet_for_UT(_row_binlog_tablet);
        create_remote_tablet_dir(kDataTabletId);
        create_remote_tablet_dir(kRowBinlogTabletId);
    }

    void TearDown() override {
        _tablet.reset();
        _row_binlog_tablet.reset();
        _engine.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_storage_root_path).ok());
        config::cloud_mow_sync_rowsets_when_load_txn_begin = true;
    }

    void create_remote_tablet_dir(int64_t tablet_id) {
        ASSERT_TRUE(
                io::global_local_filesystem()
                        ->create_directory(fmt::format("{}/data/{}", _storage_root_path, tablet_id))
                        .ok());
    }

    std::shared_ptr<OlapTableSchemaParam> create_schema_param() {
        TDescriptorTable tdesc_tbl = testutil::create_descriptor_table(
                {{TYPE_INT, "k1", false}, {TYPE_INT, "v1", false}});
        return testutil::create_table_schema_param(
                tdesc_tbl, kDataIndexId, _request.tablet_schema.schema_hash,
                _request.tablet_schema.columns, kRowBinlogIndexId,
                _row_binlog_request.tablet_schema.schema_hash,
                &_row_binlog_request.tablet_schema.columns);
    }

    void init_write_requests(WriteRequest* data_req, WriteRequest* row_binlog_req,
                             WriteRequest* group_req) {
        PUniqueId load_id;
        load_id.set_hi(0);
        load_id.set_lo(0);

        *data_req = {};
        data_req->tablet_id = _request.tablet_id;
        data_req->schema_hash = _request.tablet_schema.schema_hash;
        data_req->txn_id = 20010;
        data_req->partition_id = _request.partition_id;
        data_req->index_id = kDataIndexId;
        data_req->load_id = load_id;
        data_req->table_schema_param = create_schema_param();
        data_req->write_req_type = WriteRequestType::DATA;

        *row_binlog_req = *data_req;
        row_binlog_req->tablet_id = _row_binlog_request.tablet_id;
        row_binlog_req->index_id = kRowBinlogIndexId;
        row_binlog_req->schema_hash = _row_binlog_request.tablet_schema.schema_hash;
        row_binlog_req->write_req_type = WriteRequestType::ROW_BINLOG;

        *group_req = *data_req;
        group_req->write_req_type = WriteRequestType::GROUP;
    }

    Block create_block(int start_key, int num_rows) const {
        Block block = _tablet->tablet_schema()->create_block();
        {
            auto columns_guard = block.mutate_columns_scoped();
            auto& columns = columns_guard.mutable_columns();
            for (int i = 0; i < num_rows; ++i) {
                columns[0]->insert(Field::create_field<PrimitiveType::TYPE_INT>(start_key + i));
                columns[1]->insert(
                        Field::create_field<PrimitiveType::TYPE_INT>((start_key + i) * 10));
            }
        }
        return block;
    }

    Status create_group_writer(std::unique_ptr<GroupRowsetWriter>* group_writer, int num_rows) {
        RowsetWriterContext data_context;
        data_context.rowset_state = PREPARED;
        data_context.segments_overlap = OVERLAPPING;
        data_context.tablet_schema = _tablet->tablet_schema();
        data_context.tablet = _tablet;
        data_context.tablet_schema_hash = _request.tablet_schema.schema_hash;
        data_context.max_rows_per_segment = 1024;
        data_context.write_type = DataWriteType::TYPE_DIRECT;
        data_context.storage_resource = _engine->get_storage_resource("");
        auto data_writer_res = _tablet->create_rowset_writer(data_context, false);
        if (!data_writer_res.has_value()) {
            return data_writer_res.error();
        }

        RowsetWriterContext row_binlog_context;
        row_binlog_context.rowset_state = PREPARED;
        row_binlog_context.segments_overlap = NONOVERLAPPING;
        row_binlog_context.tablet_schema = _row_binlog_tablet->tablet_schema();
        row_binlog_context.tablet = _row_binlog_tablet;
        row_binlog_context.tablet_schema_hash = _row_binlog_request.tablet_schema.schema_hash;
        row_binlog_context.max_rows_per_segment = 1024;
        row_binlog_context.write_type = DataWriteType::TYPE_DIRECT;
        row_binlog_context.storage_resource = _engine->get_storage_resource("");
        row_binlog_context.write_binlog_opt().enable = true;
        auto& cfg = row_binlog_context.write_binlog_opt().write_binlog_config();
        cfg.source.tablet_schema = _tablet->tablet_schema();
        cfg.source.base_tablet = _tablet;
        cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
        auto lsn_buffer = AutoIncIDBuffer::create_shared(1, 1, kBinlogLsnAutoIncId);
        lsn_buffer->append_range_for_test(1000, num_rows);
        std::shared_ptr<std::vector<int64_t>> lsn_ids;
        RETURN_IF_ERROR(allocate_binlog_lsn(lsn_buffer, num_rows, &lsn_ids));
        cfg.insert_seg_lsn(0, lsn_ids);

        auto row_binlog_writer_res =
                _row_binlog_tablet->create_rowset_writer(row_binlog_context, false);
        if (!row_binlog_writer_res.has_value()) {
            return row_binlog_writer_res.error();
        }

        *group_writer = std::make_unique<GroupRowsetWriter>();
        (*group_writer)
                ->set_data_writer(
                        std::shared_ptr<RowsetWriter>(std::move(data_writer_res.value())));
        (*group_writer)
                ->set_row_binlog_writer(
                        std::shared_ptr<RowsetWriter>(std::move(row_binlog_writer_res.value())));
        return Status::OK();
    }

    void assert_rowset_meta(const RowsetSharedPtr& data_rowset,
                            const RowsetSharedPtr& row_binlog_rowset) {
        ASSERT_FALSE(data_rowset->rowset_meta()->is_row_binlog());
        ASSERT_EQ(data_rowset->rowset_meta()->tablet_id(), _tablet->tablet_id());
        ASSERT_EQ(data_rowset->rowset_meta()->tablet_schema_hash(),
                  _request.tablet_schema.schema_hash);

        ASSERT_TRUE(row_binlog_rowset->rowset_meta()->is_row_binlog());
        ASSERT_EQ(row_binlog_rowset->rowset_meta()->tablet_id(), _row_binlog_tablet->tablet_id());
        ASSERT_EQ(row_binlog_rowset->rowset_meta()->tablet_schema_hash(),
                  _row_binlog_request.tablet_schema.schema_hash);
        ASSERT_GE(row_binlog_rowset->rowset_meta()->tablet_schema()->field_index(
                          std::string(kRowBinlogLsnColName)),
                  0);
    }

    std::unique_ptr<CloudStorageEngine> _engine;
    CloudTabletSPtr _tablet;
    CloudTabletSPtr _row_binlog_tablet;
    TCreateTabletReq _request;
    TCreateTabletReq _row_binlog_request;
    std::string _storage_root_path;
};

TEST_F(CloudGroupRowsetBuilderWriterTest, builderBuildsRowBinlogMeta) {
    WriteRequest data_req;
    WriteRequest row_binlog_req;
    WriteRequest group_req;
    init_write_requests(&data_req, &row_binlog_req, &group_req);

    RuntimeProfile profile("CloudGroupRowsetBuilderWriterTest");
    CloudGroupRowsetBuilder builder(*_engine, group_req, data_req, row_binlog_req, &profile);
    builder.set_skip_writing_rowset_metadata(true);
    auto st = builder.init();
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_TRUE(builder.rowset_writer()->flush().ok());
    ASSERT_TRUE(builder.build_rowset().ok());

    assert_rowset_meta(builder.data_builder()->rowset(), builder.row_binlog_builder()->rowset());
}

TEST_F(CloudGroupRowsetBuilderWriterTest, writerBuildsRowBinlogMeta) {
    std::unique_ptr<GroupRowsetWriter> group_writer;
    auto st = create_group_writer(&group_writer, 2);
    ASSERT_TRUE(st.ok()) << st;

    auto block = create_block(100, 2);
    st = group_writer->flush_single_block(&block);
    ASSERT_TRUE(st.ok()) << st;

    std::vector<RowsetSharedPtr> rowsets;
    st = group_writer->build_rowsets(rowsets);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(2, rowsets.size());

    assert_rowset_meta(rowsets[0], rowsets[1]);
}

} // namespace doris
