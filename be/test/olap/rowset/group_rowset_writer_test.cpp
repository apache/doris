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

#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/field.h"
#include "exec/sink/autoinc_buffer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "storage/binlog.h"
#include "storage/olap_define.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "testutil/creators.h"
#include "util/debug_points.h"

namespace doris {

namespace {

constexpr uint32_t MAX_PATH_LEN = 1024;
constexpr std::string_view kStorageRootDir = "/ut_dir/group_rowset_writer_test";

} // namespace

class GroupRowsetWriterTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);

        _storage_root_path = std::string(buffer) + std::string(kStorageRootDir);
        config::storage_root_path = _storage_root_path;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_storage_root_path).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_storage_root_path).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(_storage_root_path, -1);
        EngineOptions options;
        options.store_paths = paths;
        auto engine = std::make_unique<StorageEngine>(options);
        auto* engine_ptr = engine.get();
        ASSERT_TRUE(engine_ptr->open().ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _request = testutil::create_tablet_request(
                10010, 270068390, 10001, 1, TKeysType::UNIQUE_KEYS,
                {{"k1", TPrimitiveType::INT, true}, {"v1", TPrimitiveType::INT, false}});
        _request.__set_enable_unique_key_merge_on_write(true);
        testutil::enable_row_binlog(&_request);
        auto profile = std::make_unique<RuntimeProfile>("GroupRowsetWriterTest");
        ASSERT_TRUE(engine_ptr->create_tablet(_request, profile.get()).ok());
        _tablet = engine_ptr->tablet_manager()->get_tablet(_request.tablet_id);
        ASSERT_TRUE(_tablet != nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->row_binlog_path()).ok());

        config::enable_debug_points = true;
    }

    void TearDown() override {
        DebugPoints::instance()->clear();
        config::enable_debug_points = false;
        _tablet.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_storage_root_path).ok());
    }

    Block create_block(int start_key, int num_rows) const {
        Block block = _tablet->tablet_schema()->create_block();
        auto columns = block.mutate_columns();
        for (int i = 0; i < num_rows; ++i) {
            columns[0]->insert(Field::create_field<PrimitiveType::TYPE_INT>(start_key + i));
            columns[1]->insert(Field::create_field<PrimitiveType::TYPE_INT>((start_key + i) * 10));
        }
        block.set_columns(std::move(columns));
        return block;
    }

    Status create_group_rowset_writer(std::unique_ptr<GroupRowsetWriter>* group_writer,
                                      RowsetId* data_rowset_id, size_t num_rows) {
        RowsetWriterContext data_context;
        data_context.tablet = _tablet;
        data_context.tablet_schema = _tablet->tablet_schema();
        data_context.rowset_state = PREPARED;
        data_context.segments_overlap = OVERLAPPING;
        data_context.max_rows_per_segment = 1024;
        data_context.write_type = DataWriteType::TYPE_DIRECT;
        data_context.is_transient_rowset_writer = true;
        data_context.write_binlog_opt().mark_primary_writer();
        *data_rowset_id = ExecEnv::GetInstance()->storage_engine().next_rowset_id();
        auto data_writer_res = _tablet->create_transient_rowset_writer(data_context, *data_rowset_id);
        if (!data_writer_res.has_value()) {
            return data_writer_res.error();
        }

        RowsetWriterContext row_binlog_context;
        row_binlog_context.tablet = _tablet;
        row_binlog_context.tablet_schema = _tablet->row_binlog_tablet_schema();
        row_binlog_context.rowset_state = PREPARED;
        row_binlog_context.segments_overlap = NONOVERLAPPING;
        row_binlog_context.max_rows_per_segment = 1024;
        row_binlog_context.write_type = DataWriteType::TYPE_DIRECT;
        row_binlog_context.is_transient_rowset_writer = true;
        row_binlog_context.write_binlog_opt().mark_binlog_writer();
        auto& cfg = row_binlog_context.write_binlog_opt().write_binlog_config();
        cfg.source.tablet_schema = _tablet->tablet_schema();
        cfg.source.is_transient_rowset_writer = true;
        cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
        auto lsn_buffer = AutoIncIDBuffer::create_shared(1, 1, kBinlogLsnAutoIncId);
        lsn_buffer->append_range_for_test(1000, num_rows);
        std::shared_ptr<std::vector<int128_t>> lsn_ids;
        RETURN_IF_ERROR(allocate_binlog_lsn(lsn_buffer, num_rows, &lsn_ids));
        cfg.insert_seg_lsn(0, lsn_ids);
        auto row_binlog_writer_res = _tablet->create_rowset_writer(row_binlog_context, false);
        if (!row_binlog_writer_res.has_value()) {
            return row_binlog_writer_res.error();
        }

        *group_writer = std::make_unique<GroupRowsetWriter>();
        (*group_writer)->set_data_writer(
                std::shared_ptr<RowsetWriter>(std::move(data_writer_res.value())));
        (*group_writer)->set_row_binlog_writer(
                std::shared_ptr<RowsetWriter>(std::move(row_binlog_writer_res.value())));
        return Status::OK();
    }

    bool file_exists(const std::string& path) const {
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->exists(path, &exists).ok());
        return exists;
    }

    TabletSharedPtr _tablet;
    TCreateTabletReq _request;
    std::string _storage_root_path;
};

TEST_F(GroupRowsetWriterTest, sub_writer_rollback) {
    std::unique_ptr<GroupRowsetWriter> group_writer;
    RowsetId data_rowset_id;
    auto st = create_group_rowset_writer(&group_writer, &data_rowset_id, 2);
    ASSERT_TRUE(st.ok()) << st;

    auto block = create_block(100, 2);
    st = group_writer->flush_single_block(&block);
    ASSERT_TRUE(st.ok()) << st;

    const auto transient_segment_path =
            local_segment_path(_tablet->tablet_path(), data_rowset_id.to_string(), 0);
    ASSERT_TRUE(file_exists(transient_segment_path));

    DebugPoints::instance()->add("GroupRowsetWriter::build_rowsets.row_binlog_build_failed");
    std::vector<RowsetSharedPtr> rowsets;
    st = group_writer->build_rowsets(rowsets);
    DebugPoints::instance()->remove("GroupRowsetWriter::build_rowsets.row_binlog_build_failed");

    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(rowsets.empty());
    group_writer.reset();
    bool removed = false;
    for (int i = 0; i < 3; ++i) {
        if (!file_exists(transient_segment_path)) {
            removed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_TRUE(removed);
}

TEST_F(GroupRowsetWriterTest, success) {
    std::unique_ptr<GroupRowsetWriter> group_writer;
    RowsetId data_rowset_id;
    auto st = create_group_rowset_writer(&group_writer, &data_rowset_id, 2);
    ASSERT_TRUE(st.ok()) << st;

    auto block = create_block(100, 2);
    st = group_writer->flush_single_block(&block);
    ASSERT_TRUE(st.ok()) << st;

    std::vector<RowsetSharedPtr> rowsets;
    st = group_writer->build_rowsets(rowsets);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(2, rowsets.size());

    const auto data_segment_path =
            local_segment_path(_tablet->tablet_path(), data_rowset_id.to_string(), 0);
    const auto second_segment_path = local_segment_path(_tablet->row_binlog_path(),
                                                        rowsets[1]->rowset_id().to_string(), 0);
    EXPECT_TRUE(file_exists(data_segment_path));
    EXPECT_TRUE(file_exists(second_segment_path));
}

} // namespace doris
