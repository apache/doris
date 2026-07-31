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

#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_id_generator.h"
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
