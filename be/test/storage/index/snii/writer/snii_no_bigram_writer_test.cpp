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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment_writer.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr int64_t kIndexId = 17;
constexpr const char* kTestDir = "./ut_dir/snii_no_bigram_writer_test";
constexpr const char* kTmpRoot = "./ut_dir/snii_no_bigram_writer_tmp";

const std::vector<std::string> kRows = {
        "alpha beta gamma",
        "alpha gamma beta",
        "zeta alpha beta",
};
const std::vector<uint32_t> kExpectedPhraseDocs = {0, 2};

class SniiNoBigramWriter : public testing::Test {
protected:
    static void SetUpTestSuite() {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(kTmpRoot).ok());
        ASSERT_TRUE(fs->create_directory(kTmpRoot).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(kTmpRoot, 1024 * 1024);
        auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        // ExecEnv retains this owner process-wide, so its root outlives the suite.
    }

    void SetUp() override {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(kTestDir).ok());
        ASSERT_TRUE(fs->create_directory(kTestDir).ok());
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    TabletSchemaSPtr create_phrase_schema() const {
        auto schema = std::make_shared<TabletSchema>();
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_compress_kind(COMPRESS_NONE);
        schema_pb.set_next_column_unique_id(2);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::SNII);
        schema->init_from_pb(schema_pb);

        TabletColumn key_column;
        key_column.set_name("c1");
        key_column.set_unique_id(0);
        key_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        key_column.set_length(4);
        key_column.set_index_length(4);
        key_column.set_is_key(true);
        key_column.set_is_nullable(false);
        schema->append_column(key_column);

        TabletColumn text_column;
        text_column.set_name("c2");
        text_column.set_unique_id(1);
        text_column.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        text_column.set_length(65535);
        text_column.set_is_key(false);
        text_column.set_is_nullable(false);
        schema->append_column(text_column);

        schema->append_index(create_phrase_index());
        return schema;
    }

    TabletIndex create_phrase_index() const {
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(kIndexId);
        pb.set_index_name("idx_c2");
        pb.add_col_unique_id(1);
        pb.mutable_properties()->insert({"parser", "english"});
        pb.mutable_properties()->insert({"lower_case", "true"});
        pb.mutable_properties()->insert({"support_phrase", "true"});
        TabletIndex index;
        index.init_from_pb(pb);
        return index;
    }

    Block create_block() const {
        auto keys = ColumnInt32::create();
        auto values = ColumnString::create();
        for (size_t i = 0; i < kRows.size(); ++i) {
            keys->insert_value(static_cast<int32_t>(i + 1));
            values->insert_data(kRows[i].data(), kRows[i].size());
        }

        Block block;
        block.insert(
                ColumnWithTypeAndName(std::move(keys), std::make_shared<DataTypeInt32>(), "c1"));
        block.insert(
                ColumnWithTypeAndName(std::move(values), std::make_shared<DataTypeString>(), "c2"));
        return block;
    }

    std::unique_ptr<IndexFileWriter> create_index_file_writer(const std::string& name,
                                                              std::string* path) const {
        *path = std::string(kTestDir) + "/" + name + ".idx";
        io::FileWriterPtr file_writer;
        const Status status = io::global_local_filesystem()->create_file(*path, &file_writer);
        EXPECT_TRUE(status.ok()) << status;
        if (!status.ok()) {
            return nullptr;
        }
        return std::make_unique<IndexFileWriter>(
                io::global_local_filesystem(), *path, "test_rowset", /*seg_id=*/0,
                InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                /*can_use_ram_dir=*/true, /*tablet_id=*/300);
    }

    void write_horizontal(const std::string& name, DataWriteType write_type,
                          std::string* index_path) const {
        auto schema = create_phrase_schema();
        auto index_file_writer = create_index_file_writer(name, index_path);
        ASSERT_NE(index_file_writer, nullptr);

        const std::string data_path = std::string(kTestDir) + "/" + name + ".dat";
        io::FileWriterPtr data_file_writer;
        ASSERT_TRUE(io::global_local_filesystem()->create_file(data_path, &data_file_writer).ok());

        RowsetWriterContext rowset_context;
        rowset_context.write_type = write_type;
        SegmentWriterOptions options;
        options.write_type = write_type;
        options.rowset_ctx = &rowset_context;
        SegmentWriter writer(data_file_writer.get(), /*segment_id=*/0, schema, nullptr, nullptr,
                             options, index_file_writer.get());
        ASSERT_TRUE(writer.init().ok());
        Block block = create_block();
        ASSERT_TRUE(writer.append_block(&block, 0, block.rows()).ok());

        uint64_t segment_size = 0;
        uint64_t index_size = 0;
        ASSERT_TRUE(writer.finalize(&segment_size, &index_size).ok());
        ASSERT_TRUE(index_file_writer->begin_close().ok());
        ASSERT_TRUE(index_file_writer->finish_close().ok());
    }

    void write_vertical(const std::string& name, DataWriteType write_type,
                        std::string* index_path) const {
        auto schema = create_phrase_schema();
        auto index_file_writer = create_index_file_writer(name, index_path);
        ASSERT_NE(index_file_writer, nullptr);

        const std::string data_path = std::string(kTestDir) + "/" + name + ".dat";
        io::FileWriterPtr data_file_writer;
        ASSERT_TRUE(io::global_local_filesystem()->create_file(data_path, &data_file_writer).ok());

        RowsetWriterContext rowset_context;
        rowset_context.write_type = write_type;
        VerticalSegmentWriterOptions options;
        options.write_type = write_type;
        options.rowset_ctx = &rowset_context;
        VerticalSegmentWriter writer(data_file_writer.get(), /*segment_id=*/0, schema, nullptr,
                                     nullptr, options, index_file_writer.get());
        ASSERT_TRUE(writer.init().ok());
        Block block = create_block();
        ASSERT_TRUE(writer.batch_block(&block, 0, block.rows()).ok());
        ASSERT_TRUE(writer.write_batch().ok());

        uint64_t segment_size = 0;
        uint64_t index_size = 0;
        ASSERT_TRUE(writer.finalize(&segment_size, &index_size).ok());
        ASSERT_TRUE(index_file_writer->begin_close().ok());
        ASSERT_TRUE(index_file_writer->finish_close().ok());
    }

    void write_scalar_index(const std::string& name, bool set_direct_load,
                            std::string* index_path) const {
        auto schema = create_phrase_schema();
        TabletIndex index = create_phrase_index();
        auto index_file_writer = create_index_file_writer(name, index_path);
        ASSERT_NE(index_file_writer, nullptr);

        std::unique_ptr<IndexColumnWriter> writer;
        ASSERT_TRUE(IndexColumnWriter::create(&schema->column(1), &writer, index_file_writer.get(),
                                              &index)
                            .ok());
        if (set_direct_load) {
            writer->set_direct_load(true);
        }
        std::vector<Slice> values;
        values.reserve(kRows.size());
        for (const auto& row : kRows) {
            values.emplace_back(row);
        }
        ASSERT_TRUE(writer->add_values("c2", values.data(), values.size()).ok());
        ASSERT_TRUE(writer->finish().ok());
        ASSERT_TRUE(index_file_writer->begin_close().ok());
        ASSERT_TRUE(index_file_writer->finish_close().ok());
    }

    void write_array_index(const std::string& name, std::string* index_path) const {
        TabletColumn array_column;
        array_column.set_name("arr");
        array_column.set_unique_id(1);
        array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        array_column.set_is_nullable(false);
        TabletColumn item_column;
        item_column.set_name("item");
        item_column.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        item_column.set_length(65535);
        item_column.set_is_nullable(false);
        array_column.add_sub_column(item_column);

        TabletIndex index = create_phrase_index();
        auto index_file_writer = create_index_file_writer(name, index_path);
        ASSERT_NE(index_file_writer, nullptr);
        std::unique_ptr<IndexColumnWriter> writer;
        ASSERT_TRUE(
                IndexColumnWriter::create(&array_column, &writer, index_file_writer.get(), &index)
                        .ok());
        writer->set_direct_load(true);

        std::vector<Slice> values;
        values.reserve(kRows.size());
        for (const auto& row : kRows) {
            values.emplace_back(row);
        }
        const std::vector<uint64_t> offsets = {0, 1, 2, 3};
        ASSERT_TRUE(writer->add_array_values(field_type_size(item_column.type()), values.data(),
                                             /*nested_null_map=*/nullptr,
                                             reinterpret_cast<const uint8_t*>(offsets.data()),
                                             kRows.size())
                            .ok());
        ASSERT_TRUE(writer->finish().ok());
        ASSERT_TRUE(index_file_writer->begin_close().ok());
        ASSERT_TRUE(index_file_writer->finish_close().ok());
    }

    void assert_only_unigrams(const std::string& path) const {
        ::doris::snii::io::LocalFileReader file;
        ASSERT_TRUE(file.open(path).ok());
        ::doris::snii::reader::SniiSegmentReader segment;
        ASSERT_TRUE(::doris::snii::reader::SniiSegmentReader::open(&file, &segment).ok());
        ::doris::snii::reader::LogicalIndexReader index;
        ASSERT_TRUE(segment.open_index(kIndexId, /*index_suffix=*/"", &index).ok());

        std::vector<::doris::snii::reader::LogicalIndexReader::PrefixHit> hits;
        ASSERT_TRUE(index.prefix_terms("", &hits).ok());
        ASSERT_FALSE(hits.empty());
        for (const auto& hit : hits) {
            EXPECT_FALSE(::doris::snii::format::is_phrase_bigram_term(hit.term)) << hit.term;
        }
        std::vector<uint32_t> docids;
        ASSERT_TRUE(::doris::snii::query::phrase_query(index, {"alpha", "beta"}, &docids).ok());
        EXPECT_EQ(docids, kExpectedPhraseDocs);
    }
};

TEST_F(SniiNoBigramWriter, HorizontalDirectLoadWritesOnlyUnigrams) {
    std::string path;
    ASSERT_NO_FATAL_FAILURE(
            write_horizontal("horizontal_direct", DataWriteType::TYPE_DIRECT, &path));
    ASSERT_NO_FATAL_FAILURE(assert_only_unigrams(path));
}

TEST_F(SniiNoBigramWriter, HorizontalCompactionWritesOnlyUnigrams) {
    std::string path;
    ASSERT_NO_FATAL_FAILURE(
            write_horizontal("horizontal_compaction", DataWriteType::TYPE_COMPACTION, &path));
    ASSERT_NO_FATAL_FAILURE(assert_only_unigrams(path));
}

TEST_F(SniiNoBigramWriter, HorizontalSchemaChangeWritesOnlyUnigrams) {
    std::string path;
    ASSERT_NO_FATAL_FAILURE(
            write_horizontal("horizontal_schema_change", DataWriteType::TYPE_SCHEMA_CHANGE, &path));
    ASSERT_NO_FATAL_FAILURE(assert_only_unigrams(path));
}

TEST_F(SniiNoBigramWriter, VerticalDirectLoadWritesOnlyUnigrams) {
    std::string path;
    ASSERT_NO_FATAL_FAILURE(write_vertical("vertical_direct", DataWriteType::TYPE_DIRECT, &path));
    ASSERT_NO_FATAL_FAILURE(assert_only_unigrams(path));
}

TEST_F(SniiNoBigramWriter, VerticalCompactionWritesOnlyUnigrams) {
    std::string path;
    ASSERT_NO_FATAL_FAILURE(
            write_vertical("vertical_compaction", DataWriteType::TYPE_COMPACTION, &path));
    ASSERT_NO_FATAL_FAILURE(assert_only_unigrams(path));
}

TEST_F(SniiNoBigramWriter, VerticalSchemaChangeWritesOnlyUnigrams) {
    std::string path;
    ASSERT_NO_FATAL_FAILURE(
            write_vertical("vertical_schema_change", DataWriteType::TYPE_SCHEMA_CHANGE, &path));
    ASSERT_NO_FATAL_FAILURE(assert_only_unigrams(path));
}

TEST_F(SniiNoBigramWriter, AddIndexNoHintWritesOnlyUnigrams) {
    std::string path;
    ASSERT_NO_FATAL_FAILURE(write_scalar_index("add_index_no_hint", false, &path));
    ASSERT_NO_FATAL_FAILURE(assert_only_unigrams(path));
}

TEST_F(SniiNoBigramWriter, DirectArrayWritesOnlyUnigrams) {
    std::string path;
    ASSERT_NO_FATAL_FAILURE(write_array_index("direct_array", &path));
    ASSERT_NO_FATAL_FAILURE(assert_only_unigrams(path));
}

TEST(SniiNoBigramWriterLifecycle, ExecEnvTmpDirectoryRemainsUsableAfterWriterSuite) {
    auto* tmp_file_dirs = ExecEnv::GetInstance()->get_tmp_file_dirs();
    ASSERT_NE(tmp_file_dirs, nullptr);
    const io::Path tmp_dir = tmp_file_dirs->get_tmp_file_dir();

    auto fs = io::global_local_filesystem();
    bool tmp_dir_exists = false;
    ASSERT_TRUE(fs->exists(tmp_dir, &tmp_dir_exists).ok());
    ASSERT_TRUE(tmp_dir_exists) << tmp_dir;

    const io::Path probe_path = tmp_dir / "snii_no_bigram_writer_lifecycle_probe";
    io::FileWriterPtr probe_writer;
    ASSERT_TRUE(fs->create_file(probe_path, &probe_writer).ok());
    ASSERT_TRUE(probe_writer->close().ok());
    ASSERT_TRUE(fs->delete_file(probe_path).ok());
}

} // namespace
} // namespace doris::segment_v2
