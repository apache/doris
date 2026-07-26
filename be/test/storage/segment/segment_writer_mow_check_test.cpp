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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/rowset/segment_creator.h"
#include "storage/segment/segment_writer.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

// Subclass in a different translation unit that calls _is_mow() and _is_mow_with_cluster_key().
// This test verifies that these inline private methods are visible across translation units.
// Previously they were defined with `inline` in .cpp files, which caused linker errors
// when compiled with -O1 or higher (TSAN/RELEASE), because the compiler inlined them
// and did not export the symbols.
class TestSegmentWriterMowCheck : public SegmentWriter {
public:
    using SegmentWriter::SegmentWriter;

    bool check_is_mow() { return _is_mow(); }
    bool check_is_mow_with_cluster_key() { return _is_mow_with_cluster_key(); }
};

class TestVerticalSegmentWriterMowCheck : public VerticalSegmentWriter {
public:
    using VerticalSegmentWriter::VerticalSegmentWriter;

    bool check_is_mow() { return _is_mow(); }
    bool check_is_mow_with_cluster_key() { return _is_mow_with_cluster_key(); }
};

static const std::string kSegmentDir = "./ut_dir/segment_writer_mow_check_test";

TabletColumnPtr create_int_key(int32_t id) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_INT;
    column->_is_key = true;
    column->_is_nullable = false;
    column->_length = 4;
    column->_index_length = 4;
    return column;
}

TabletColumnPtr create_int_value(int32_t id) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_INT;
    column->_is_key = false;
    column->_is_nullable = true;
    column->_length = 4;
    column->_index_length = 4;
    return column;
}

TabletSchemaSPtr create_unique_key_schema() {
    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
    schema->append_column(*create_int_key(0));
    schema->append_column(*create_int_value(1));
    schema->_keys_type = UNIQUE_KEYS;
    return schema;
}

TabletSchemaSPtr create_dup_key_schema() {
    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
    schema->append_column(*create_int_key(0));
    schema->append_column(*create_int_value(1));
    schema->_keys_type = DUP_KEYS;
    return schema;
}

TabletSchemaSPtr create_unique_key_schema_with_cluster_key() {
    auto schema = create_unique_key_schema();
    schema->_cluster_key_uids = {1};
    return schema;
}

class CloseOutcomeFileWriter final : public io::FileWriter {
public:
    explicit CloseOutcomeFileWriter(std::string path) : _path(std::move(path)) {}

    Status close(bool /*non_block*/ = false) override {
        ++close_calls;
        if (_fail_close) {
            return Status::IOError("injected data close failure");
        }
        _state = State::CLOSED;
        return Status::OK();
    }

    Status appendv(const Slice* /*data*/, size_t /*data_cnt*/) override { return Status::OK(); }
    const io::Path& path() const override { return _path; }
    size_t bytes_appended() const override { return 0; }
    State state() const override { return _state; }

    void set_fail_close() { _fail_close = true; }

    int close_calls = 0;

private:
    io::Path _path;
    State _state = State::OPENED;
    bool _fail_close = false;
};

class TrackingFileWriterCreator final : public FileWriterCreator {
public:
    explicit TrackingFileWriterCreator(std::string rowset_id) : _rowset_id(std::move(rowset_id)) {}

    Status create(uint32_t segment_id, io::FileWriterPtr& file_writer,
                  FileType file_type = FileType::SEGMENT_FILE) override {
        DORIS_CHECK(file_type == FileType::SEGMENT_FILE);
        created_segment_ids.push_back(segment_id);
        return io::global_local_filesystem()->create_file(
                fmt::format("{}/streaming_{}_{}.dat", kSegmentDir, _rowset_id, segment_id),
                &file_writer);
    }

    Status create(uint32_t /*segment_id*/, IndexFileWriterPtr* /*file_writer*/) override {
        return Status::InternalError("unexpected index writer creation");
    }

    std::vector<uint32_t> created_segment_ids;

private:
    std::string _rowset_id;
};

class CountingSegmentCollector final : public SegmentCollector {
public:
    Status add(uint32_t segment_id, SegmentStatistics& segstat) override {
        ++add_calls;
        last_segment_id = segment_id;
        last_row_count = segstat.row_num;
        return Status::OK();
    }

    int add_calls = 0;
    uint32_t last_segment_id = 0;
    int64_t last_row_count = 0;
};

class FailingSecondSegmentCollector final : public SegmentCollector {
public:
    Status add(uint32_t /*segment_id*/, SegmentStatistics& /*segstat*/) override {
        ++add_calls;
        if (add_calls == 2) {
            return Status::IOError("injected second segment collector failure");
        }
        return Status::OK();
    }

    int add_calls = 0;
};

class ScopedIndexOnlyFileCacheSetting {
public:
    ScopedIndexOnlyFileCacheSetting()
            : _enable_file_cache(config::enable_file_cache),
              _enable_index_only(config::enable_file_cache_write_index_file_only),
              _cloud_unique_id(config::cloud_unique_id) {
        config::enable_file_cache = true;
        config::enable_file_cache_write_index_file_only = true;
        config::cloud_unique_id = "segment_creator_preload_failure_ut";
    }

    ~ScopedIndexOnlyFileCacheSetting() {
        config::enable_file_cache = _enable_file_cache;
        config::enable_file_cache_write_index_file_only = _enable_index_only;
        config::cloud_unique_id = _cloud_unique_id;
    }

private:
    bool _enable_file_cache;
    bool _enable_index_only;
    std::string _cloud_unique_id;
};

class ScopedVerticalSegmentWriterSetting {
public:
    explicit ScopedVerticalSegmentWriterSetting(bool enabled)
            : _saved(config::enable_vertical_segment_writer) {
        config::enable_vertical_segment_writer = enabled;
    }

    ~ScopedVerticalSegmentWriterSetting() { config::enable_vertical_segment_writer = _saved; }

private:
    bool _saved;
};

class ScopedSyncPointProcessing {
public:
    ScopedSyncPointProcessing() { SyncPoint::get_instance()->enable_processing(); }
    ~ScopedSyncPointProcessing() { SyncPoint::get_instance()->disable_processing(); }
};

class SegmentWriterMowCheckTest : public testing::Test {
public:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        auto st = fs->delete_directory(kSegmentDir);
        ASSERT_TRUE(st.ok() || st.is<ErrorCode::NOT_FOUND>()) << st;
        st = fs->create_directory(kSegmentDir);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kSegmentDir).ok());
    }

    io::FileWriterPtr create_file_writer(size_t segment_id) {
        RowsetId rowset_id;
        rowset_id.init(1);
        std::string filename = fmt::format("{}_{}.dat", rowset_id.to_string(), segment_id);
        std::string path = fmt::format("{}/{}", kSegmentDir, filename);
        io::FileWriterPtr file_writer;
        auto st = io::global_local_filesystem()->create_file(path, &file_writer);
        EXPECT_TRUE(st.ok()) << st;
        return file_writer;
    }
};

class SegmentCreatorTest : public SegmentWriterMowCheckTest {};

TEST(SegmentFileCollectionTest, CloseAttemptsEveryWriterAfterFirstFailure) {
    SegmentFileCollection segment_files;
    std::unordered_map<int, CloseOutcomeFileWriter*> writers;
    for (int segment_id = 0; segment_id < 3; ++segment_id) {
        auto writer =
                std::make_unique<CloseOutcomeFileWriter>(fmt::format("segment_{}", segment_id));
        writers.emplace(segment_id, writer.get());
        ASSERT_TRUE(segment_files.add(segment_id, std::move(writer)).ok());
    }

    const auto first_segment_id = segment_files.get_file_writers().begin()->first;
    writers.at(first_segment_id)->set_fail_close();

    const auto expected_status = Status::IOError("injected data close failure");
    const auto status = segment_files.close();
    EXPECT_EQ(status.code(), expected_status.code());
    EXPECT_EQ(status.msg(), expected_status.msg());
    for (const auto& [segment_id, writer] : writers) {
        EXPECT_EQ(writer->close_calls, 1);
        if (segment_id != first_segment_id) {
            EXPECT_EQ(writer->state(), io::FileWriter::State::CLOSED);
        }
    }
}

TEST(InvertedIndexFileCollectionTest, BeginCloseHandlesAlreadyStartedWriter) {
    InvertedIndexFileCollection index_files;
    for (int segment_id = 0; segment_id < 2; ++segment_id) {
        auto writer = std::make_unique<IndexFileWriter>(
                io::global_local_filesystem(),
                fmt::format("{}/index_collection_{}", kSegmentDir, segment_id), "1015", segment_id,
                InvertedIndexStorageFormatPB::V2);
        ASSERT_TRUE(index_files.add(segment_id, std::move(writer)).ok());
    }

    ASSERT_TRUE(index_files.get_file_writers().at(0)->begin_close().ok());
    ASSERT_TRUE(index_files.begin_close().ok());
    ASSERT_TRUE(index_files.finish_close().ok());
}

TEST_F(SegmentCreatorTest, FinishIndexCloseFailureSkipsPreload) {
    RowsetWriterContext context;
    SegmentFileCollection segment_files;
    InvertedIndexFileCollection index_files;
    SegmentCreator creator(context, segment_files, index_files);

    const auto expected_status = Status::IOError("injected index finish close failure");
    int preload_calls = 0;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard finish_close_guard;
    sync_point->set_call_back(
            "InvertedIndexFileCollection::finish_close",
            [&](auto&& values) {
                auto* outcome = try_any_cast<std::pair<Status, bool>*>(values.back());
                outcome->first = expected_status;
                outcome->second = true;
            },
            &finish_close_guard);
    SyncPoint::CallbackGuard preload_guard;
    sync_point->set_call_back(
            "SegmentIndexFileCacheLoader::preload_segment_indexes_to_file_cache",
            [&](auto&& /*values*/) { ++preload_calls; }, &preload_guard);
    ScopedSyncPointProcessing sync_point_processing;

    const auto status = creator.close();
    EXPECT_EQ(status.code(), expected_status.code());
    EXPECT_EQ(status.msg(), expected_status.msg());
    EXPECT_EQ(preload_calls, 0);
}

TEST_F(SegmentCreatorTest, StreamingAddFailureTerminatesCreatorAndClosesWriter) {
    ScopedVerticalSegmentWriterSetting vertical_writer_setting(false);

    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(*create_int_key(0));
    schema->_keys_type = DUP_KEYS;
    RowsetId rowset_id;
    rowset_id.init(1014);
    auto file_writer_creator = std::make_shared<TrackingFileWriterCreator>(rowset_id.to_string());
    auto segment_collector = std::make_shared<CountingSegmentCollector>();
    RowsetWriterContext context;
    context.rowset_id = rowset_id;
    context.tablet_schema = schema;
    context.tablet_path = kSegmentDir;
    context.file_writer_creator = file_writer_creator;
    context.segment_collector = segment_collector;

    SegmentFileCollection segment_files;
    InvertedIndexFileCollection index_files;
    SegmentCreator creator(context, segment_files, index_files);

    const auto ordinary_status = Status::IOError("injected streaming add failure");
    bool inject_add_failure = true;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard add_rows_guard;
    sync_point->set_call_back(
            "SegmentFlusher::_add_rows.segment_writer",
            [&](auto&& values) {
                if (inject_add_failure) {
                    auto* outcome = try_any_cast<std::pair<Status, bool>*>(values.back());
                    outcome->first = ordinary_status;
                    outcome->second = true;
                }
            },
            &add_rows_guard);
    ScopedSyncPointProcessing sync_point_processing;

    Block block = schema->create_block();
    auto columns = std::move(block).mutate_columns();
    const int32_t value = 42;
    columns[0]->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    block.set_columns(std::move(columns));

    const auto add_status = creator.add_block(&block);
    EXPECT_EQ(add_status.code(), ordinary_status.code());
    EXPECT_EQ(add_status.msg(), ordinary_status.msg());
    ASSERT_EQ(file_writer_creator->created_segment_ids.size(), 1);
    auto* failed_file_writer =
            segment_files.get_file_writers().at(file_writer_creator->created_segment_ids[0]).get();

    inject_add_failure = false;
    const auto retry_status = creator.add_block(&block);
    EXPECT_EQ(retry_status.code(), ordinary_status.code());
    EXPECT_EQ(retry_status.msg(), ordinary_status.msg());
    EXPECT_EQ(file_writer_creator->created_segment_ids.size(), 1);

    const auto flush_status = creator.flush();
    EXPECT_EQ(flush_status.code(), ordinary_status.code());
    EXPECT_EQ(flush_status.msg(), ordinary_status.msg());

    const auto close_status = creator.close();
    EXPECT_EQ(close_status.code(), ordinary_status.code());
    EXPECT_EQ(close_status.msg(), ordinary_status.msg());
    EXPECT_EQ(failed_file_writer->state(), io::FileWriter::State::CLOSED);
    EXPECT_EQ(segment_collector->add_calls, 0);
}

TEST_F(SegmentCreatorTest, WriteFailureAfterBufferedSegmentSkipsPreload) {
    ScopedVerticalSegmentWriterSetting vertical_writer_setting(false);
    ScopedIndexOnlyFileCacheSetting file_cache_setting;

    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(*create_int_key(0));
    schema->_keys_type = DUP_KEYS;
    RowsetId rowset_id;
    rowset_id.init(1016);
    auto file_writer_creator = std::make_shared<TrackingFileWriterCreator>(rowset_id.to_string());
    auto segment_collector = std::make_shared<FailingSecondSegmentCollector>();
    RowsetWriterContext context;
    context.rowset_id = rowset_id;
    context.tablet_id = 1016;
    context.tablet_schema = schema;
    context.tablet_path = kSegmentDir;
    context.storage_resource.emplace();
    context.file_writer_creator = file_writer_creator;
    context.segment_collector = segment_collector;

    SegmentFileCollection segment_files;
    InvertedIndexFileCollection index_files;
    SegmentCreator creator(context, segment_files, index_files);

    int preload_calls = 0;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard preload_guard;
    sync_point->set_call_back(
            "SegmentIndexFileCacheLoader::preload_segment_indexes_to_file_cache",
            [&](auto&& /*values*/) {
                ++preload_calls;
                config::enable_file_cache_write_index_file_only = false;
            },
            &preload_guard);
    ScopedSyncPointProcessing sync_point_processing;

    Block block = schema->create_block();
    auto columns = std::move(block).mutate_columns();
    const int32_t value = 42;
    columns[0]->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    block.set_columns(std::move(columns));

    ASSERT_TRUE(creator.flush_single_block(&block, 0).ok());
    const auto write_status = creator.flush_single_block(&block, 1);
    EXPECT_FALSE(write_status.ok());
    EXPECT_EQ(segment_collector->add_calls, 2);

    const auto close_status = creator.close();
    EXPECT_EQ(close_status.code(), write_status.code());
    EXPECT_EQ(close_status.msg(), write_status.msg());
    EXPECT_EQ(preload_calls, 0);
}

TEST_F(SegmentWriterMowCheckTest, segment_writer_is_mow_false_for_dup_key) {
    auto schema = create_dup_key_schema();
    SegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = true;
    auto file_writer = create_file_writer(0);
    TestSegmentWriterMowCheck writer(file_writer.get(), 0, schema, nullptr, nullptr, opts, nullptr);
    EXPECT_FALSE(writer.check_is_mow());
    EXPECT_FALSE(writer.check_is_mow_with_cluster_key());
}

TEST_F(SegmentWriterMowCheckTest, segment_writer_is_mow_true_for_unique_mow) {
    auto schema = create_unique_key_schema();
    SegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = true;
    auto file_writer = create_file_writer(1);
    TestSegmentWriterMowCheck writer(file_writer.get(), 1, schema, nullptr, nullptr, opts, nullptr);
    EXPECT_TRUE(writer.check_is_mow());
    EXPECT_FALSE(writer.check_is_mow_with_cluster_key());
}

TEST_F(SegmentWriterMowCheckTest, segment_writer_is_mow_false_when_mow_disabled) {
    auto schema = create_unique_key_schema();
    SegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = false;
    auto file_writer = create_file_writer(2);
    TestSegmentWriterMowCheck writer(file_writer.get(), 2, schema, nullptr, nullptr, opts, nullptr);
    EXPECT_FALSE(writer.check_is_mow());
    EXPECT_FALSE(writer.check_is_mow_with_cluster_key());
}

TEST_F(SegmentWriterMowCheckTest, segment_writer_is_mow_with_cluster_key) {
    auto schema = create_unique_key_schema_with_cluster_key();
    SegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = true;
    auto file_writer = create_file_writer(3);
    TestSegmentWriterMowCheck writer(file_writer.get(), 3, schema, nullptr, nullptr, opts, nullptr);
    EXPECT_TRUE(writer.check_is_mow());
    EXPECT_TRUE(writer.check_is_mow_with_cluster_key());
}

TEST_F(SegmentWriterMowCheckTest, vertical_segment_writer_is_mow_false_for_dup_key) {
    auto schema = create_dup_key_schema();
    VerticalSegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = true;
    auto file_writer = create_file_writer(4);
    TestVerticalSegmentWriterMowCheck writer(file_writer.get(), 4, schema, nullptr, nullptr, opts,
                                             nullptr);
    EXPECT_FALSE(writer.check_is_mow());
    EXPECT_FALSE(writer.check_is_mow_with_cluster_key());
}

TEST_F(SegmentWriterMowCheckTest, vertical_segment_writer_is_mow_true_for_unique_mow) {
    auto schema = create_unique_key_schema();
    VerticalSegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = true;
    auto file_writer = create_file_writer(5);
    TestVerticalSegmentWriterMowCheck writer(file_writer.get(), 5, schema, nullptr, nullptr, opts,
                                             nullptr);
    EXPECT_TRUE(writer.check_is_mow());
    EXPECT_FALSE(writer.check_is_mow_with_cluster_key());
}

TEST_F(SegmentWriterMowCheckTest, vertical_segment_writer_is_mow_with_cluster_key) {
    auto schema = create_unique_key_schema_with_cluster_key();
    VerticalSegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = true;
    auto file_writer = create_file_writer(6);
    TestVerticalSegmentWriterMowCheck writer(file_writer.get(), 6, schema, nullptr, nullptr, opts,
                                             nullptr);
    EXPECT_TRUE(writer.check_is_mow());
    EXPECT_TRUE(writer.check_is_mow_with_cluster_key());
}

} // namespace doris::segment_v2
