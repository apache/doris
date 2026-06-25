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

#include "storage/rowset/group_rowset_writer.h"

#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <set>
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
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
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
                10010, 270068390, 10001, 2, TKeysType::UNIQUE_KEYS,
                {{"k1", TPrimitiveType::INT, true},
                 {"__DORIS_TEST_HIDDEN_KEY__", TPrimitiveType::BIGINT, true},
                 {"__DORIS_TEST_HIDDEN_VALUE__", TPrimitiveType::INT, false},
                 {"v1", TPrimitiveType::INT, false},
                 {"v2", TPrimitiveType::BIGINT, false},
                 {std::string(DELETE_SIGN), TPrimitiveType::TINYINT, false}});
        _request.tablet_schema.columns[1].__set_visible(false);
        _request.tablet_schema.columns[2].__set_visible(false);
        _request.tablet_schema.columns[5].__set_visible(false);
        _request.tablet_schema.columns[2].__set_is_allow_null(true);
        _request.tablet_schema.columns[4].__set_is_allow_null(true);
        _request.__set_enable_unique_key_merge_on_write(true);
        testutil::enable_row_binlog(&_request);
        _request.row_binlog_schema.columns.erase(_request.row_binlog_schema.columns.begin() + 5);
        _request.row_binlog_schema.columns.erase(_request.row_binlog_schema.columns.begin() + 2);
        _request.row_binlog_schema.__set_binlog_tso_idx(4);
        _request.row_binlog_schema.__set_binlog_lsn_idx(5);
        _request.row_binlog_schema.__set_binlog_op_idx(6);
        auto profile = std::make_unique<RuntimeProfile>("GroupRowsetWriterTest");
        ASSERT_TRUE(engine_ptr->create_tablet(_request, profile.get()).ok());
        _tablet = engine_ptr->tablet_manager()->get_tablet(_request.tablet_id);
        ASSERT_TRUE(_tablet != nullptr);
        ASSERT_TRUE(_tablet->need_read_delete_bitmap());
        _row_binlog_request = _request;
        _row_binlog_request.tablet_id = 10011;
        _row_binlog_request.tablet_schema = testutil::create_row_binlog_tablet_schema(
                _request.tablet_schema, _request.tablet_schema.schema_hash + 1);
        ASSERT_TRUE(engine_ptr->create_tablet(_row_binlog_request, profile.get()).ok());
        _row_binlog_tablet =
                engine_ptr->tablet_manager()->get_tablet(_row_binlog_request.tablet_id);
        ASSERT_TRUE(_row_binlog_tablet != nullptr);
        ASSERT_TRUE(_row_binlog_tablet->need_read_delete_bitmap());

        config::enable_debug_points = true;
    }

    void TearDown() override {
        DebugPoints::instance()->clear();
        config::enable_debug_points = false;
        _tablet.reset();
        _row_binlog_tablet.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_storage_root_path).ok());
    }

    Block create_block(int start_key, int num_rows) const {
        Block block = _tablet->tablet_schema()->create_block();
        {
            auto columns_guard = block.mutate_columns_scoped();
            auto& columns = columns_guard.mutable_columns();
            for (int i = 0; i < num_rows; ++i) {
                columns[0]->insert(Field::create_field<PrimitiveType::TYPE_INT>(start_key + i));
                columns[1]->insert(Field::create_field<PrimitiveType::TYPE_BIGINT>(
                        static_cast<int64_t>(1000 + start_key + i)));
                columns[2]->insert(Field::create_field<PrimitiveType::TYPE_INT>(
                        static_cast<int32_t>(10000 + start_key + i)));
                columns[3]->insert(
                        Field::create_field<PrimitiveType::TYPE_INT>((start_key + i) * 10));
                columns[4]->insert(Field::create_field<PrimitiveType::TYPE_BIGINT>(
                        static_cast<int64_t>((start_key + i) * 100)));
                columns[5]->insert(Field::create_field<PrimitiveType::TYPE_TINYINT>(0));
            }
        }
        return block;
    }

    Block create_partial_update_block() const {
        Block block = _tablet->tablet_schema()->create_block_by_cids({0, 1, 2, 3});
        auto columns_guard = block.mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        columns[0]->insert(Field::create_field<PrimitiveType::TYPE_INT>(1));
        columns[1]->insert(Field::create_field<PrimitiveType::TYPE_BIGINT>(1001));
        columns[2]->insert(Field::create_field<PrimitiveType::TYPE_INT>(200));
        columns[3]->insert(Field::create_field<PrimitiveType::TYPE_INT>(20));
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
        *data_rowset_id = ExecEnv::GetInstance()->storage_engine().next_rowset_id();
        auto data_writer_res =
                _tablet->create_transient_rowset_writer(data_context, *data_rowset_id);
        if (!data_writer_res.has_value()) {
            return data_writer_res.error();
        }

        RowsetWriterContext row_binlog_context;
        row_binlog_context.tablet = _row_binlog_tablet;
        row_binlog_context.tablet_schema = _row_binlog_tablet->tablet_schema();
        row_binlog_context.rowset_state = PREPARED;
        row_binlog_context.segments_overlap = NONOVERLAPPING;
        row_binlog_context.max_rows_per_segment = 1024;
        row_binlog_context.write_type = DataWriteType::TYPE_DIRECT;
        row_binlog_context.is_transient_rowset_writer = true;
        row_binlog_context.write_binlog_opt().enable = true;
        auto& cfg = row_binlog_context.write_binlog_opt().write_binlog_config();
        cfg.source.tablet_schema = _tablet->tablet_schema();
        cfg.source.is_transient_rowset_writer = true;
        cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
        auto lsn_buffer = AutoIncIDBuffer::create_shared(1, 1, kBinlogLsnAutoIncId);
        lsn_buffer->append_range_for_test(1000, num_rows);
        auto lsn_ids = std::make_shared<std::vector<int64_t>>();
        RETURN_IF_ERROR(allocate_binlog_lsn(lsn_buffer, num_rows, *lsn_ids));
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

    bool file_exists(const std::string& path) const {
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->exists(path, &exists).ok());
        return exists;
    }

    TabletSharedPtr _tablet;
    TabletSharedPtr _row_binlog_tablet;
    TCreateTabletReq _request;
    TCreateTabletReq _row_binlog_request;
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
    bool saved_aggregate_non_mow_key_bounds = config::enable_aggregate_non_mow_key_bounds;
    config::enable_aggregate_non_mow_key_bounds = true;
    auto restore = std::shared_ptr<void>(nullptr, [&](void*) {
        config::enable_aggregate_non_mow_key_bounds = saved_aggregate_non_mow_key_bounds;
    });

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
    const auto second_segment_path = local_segment_path(_row_binlog_tablet->tablet_path(),
                                                        rowsets[1]->rowset_id().to_string(), 0);
    EXPECT_TRUE(file_exists(data_segment_path));
    EXPECT_TRUE(file_exists(second_segment_path));

    ASSERT_TRUE(rowsets[1]->rowset_meta()->is_row_binlog());
    EXPECT_FALSE(rowsets[1]->rowset_meta()->is_segments_key_bounds_aggregated());
    std::vector<KeyBoundsPB> row_binlog_key_bounds;
    rowsets[1]->rowset_meta()->get_segments_key_bounds(&row_binlog_key_bounds);
    EXPECT_EQ(row_binlog_key_bounds.size(), rowsets[1]->num_segments());
}

TEST_F(GroupRowsetWriterTest, partialUpdateSkipsHiddenNonKeyColumns) {
    ASSERT_EQ(1, _tablet->row_binlog_tablet_schema()->field_index("__DORIS_TEST_HIDDEN_KEY__"));
    ASSERT_EQ(-1, _tablet->row_binlog_tablet_schema()->field_index("__DORIS_TEST_HIDDEN_VALUE__"));
    ASSERT_EQ(-1, _tablet->row_binlog_tablet_schema()->field_index(DELETE_SIGN));

    auto partial_update_info = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(
            partial_update_info
                    ->init(_tablet->tablet_id(), 1, *_tablet->tablet_schema(),
                           UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                           PartialUpdateNewRowPolicyPB::APPEND,
                           {"k1", "__DORIS_TEST_HIDDEN_KEY__", "__DORIS_TEST_HIDDEN_VALUE__", "v1"},
                           false, 0, 0, "", "")
                    .ok());
    EXPECT_EQ((std::vector<uint32_t> {0, 1, 2, 3}), partial_update_info->update_cids);
    EXPECT_EQ((std::vector<uint32_t> {4, 5}), partial_update_info->missing_cids);

    RowsetWriterContext row_binlog_context;
    row_binlog_context.tablet = _tablet;
    row_binlog_context.tablet_schema = _tablet->row_binlog_tablet_schema();
    row_binlog_context.rowset_state = PREPARED;
    row_binlog_context.segments_overlap = NONOVERLAPPING;
    row_binlog_context.max_rows_per_segment = 1024;
    row_binlog_context.write_type = DataWriteType::TYPE_DIRECT;
    row_binlog_context.partial_update_info = partial_update_info;
    row_binlog_context.mow_context =
            std::make_shared<MowContext>(1, 1, std::make_shared<RowsetIdUnorderedSet>(),
                                         std::vector<RowsetSharedPtr> {}, nullptr);
    row_binlog_context.write_binlog_opt().enable = true;
    auto& binlog_options = row_binlog_context.write_binlog_opt().write_binlog_config();
    binlog_options.source.tablet_schema = _tablet->tablet_schema();
    binlog_options.source.partial_update_info = partial_update_info;
    binlog_options.source.mow_context = row_binlog_context.mow_context;
    binlog_options.source.source_write_type = DataWriteType::TYPE_DIRECT;

    auto lsn_buffer = AutoIncIDBuffer::create_shared(1, 1, kBinlogLsnAutoIncId);
    lsn_buffer->append_range_for_test(1000, 1);
    auto lsn_ids = std::make_shared<std::vector<int64_t>>();
    ASSERT_TRUE(allocate_binlog_lsn(lsn_buffer, 1, *lsn_ids).ok());
    binlog_options.insert_seg_lsn(0, lsn_ids);

    auto row_binlog_writer_res = _tablet->create_rowset_writer(row_binlog_context, false);
    ASSERT_TRUE(row_binlog_writer_res.has_value());
    auto row_binlog_writer = std::move(row_binlog_writer_res.value());

    Block block = create_partial_update_block();
    ASSERT_TRUE(row_binlog_writer->flush_single_block(&block).ok());

    RowsetSharedPtr row_binlog_rowset;
    ASSERT_TRUE(row_binlog_writer->build(row_binlog_rowset).ok());
    ASSERT_EQ(1, row_binlog_rowset->num_segments());

    const auto& row_binlog_schema = _tablet->row_binlog_tablet_schema();
    ASSERT_EQ(7, row_binlog_schema->num_columns());
    std::vector<uint32_t> return_columns {0, 1, 2, 3, 4, 5, 6};
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = row_binlog_schema;
    reader_context.need_ordered_result = false;
    reader_context.return_columns = &return_columns;

    RowsetReaderSharedPtr rowset_reader;
    ASSERT_TRUE(row_binlog_rowset->create_reader(&rowset_reader).ok());
    ASSERT_TRUE(rowset_reader->init(&reader_context).ok());

    Block output_block = row_binlog_schema->create_block();
    auto status = rowset_reader->next_batch(&output_block);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(1, output_block.rows());
    ASSERT_EQ(return_columns.size(), output_block.columns());

    EXPECT_EQ(1, (*output_block.get_by_position(0).column)[0].get<TYPE_INT>());
    EXPECT_EQ(1001, (*output_block.get_by_position(1).column)[0].get<TYPE_BIGINT>());
    EXPECT_EQ(20, (*output_block.get_by_position(2).column)[0].get<TYPE_INT>());
    EXPECT_TRUE(output_block.get_by_position(3).column->is_null_at(0));
    EXPECT_TRUE(output_block.get_by_position(4).column->is_null_at(0));
    EXPECT_EQ(1000, (*output_block.get_by_position(5).column)[0].get<TYPE_BIGINT>());
    EXPECT_EQ(ROW_BINLOG_APPEND, (*output_block.get_by_position(6).column)[0].get<TYPE_BIGINT>());

    Block eof_block = row_binlog_schema->create_block();
    status = rowset_reader->next_batch(&eof_block);
    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>()) << status;
}

} // namespace doris
