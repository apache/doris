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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/segment_writer.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

// Both SegmentWriter and VerticalSegmentWriter build a ColumnMetaPB for every column via
// (respectively) init_column_meta() and _init_column_meta(). These tests verify that a
// per-column compression override on the TabletColumn is propagated into that ColumnMetaPB,
// on BOTH writers -- VerticalSegmentWriter is the default write path
// (config::enable_vertical_segment_writer defaults to true), so a regression there silently
// drops the feature. They also verify that the per-column codec takes priority over the
// writer-level compression setting.
//
// SegmentWriter::init_column_meta is public, so it is called directly. VerticalSegmentWriter's
// _init_column_meta is private, so this subclass (a declared friend in the production header,
// mirroring test_segment_writer.h's TestSegmentWriter) exposes it.
class TestVerticalSegmentWriter : public VerticalSegmentWriter {
public:
    using VerticalSegmentWriter::VerticalSegmentWriter;
    void build_meta(ColumnMetaPB* meta, const TabletColumn& column,
                    const ColumnWriterOptions& opts) {
        _init_column_meta(meta, 0, column, opts);
    }
};

static const std::string kSegmentDir = "./ut_dir/segment_writer_column_compression_test";

// Build a plain INT TabletColumn through the production ColumnPB path so the per-column
// compression override (when requested) is set exactly the way init_from_pb() does at runtime.
static TabletColumn make_int_column(bool has_compression, CompressionTypePB compression,
                                    int compression_level) {
    ColumnPB column_pb;
    column_pb.set_unique_id(0);
    column_pb.set_name("c0");
    column_pb.set_type("INT");
    column_pb.set_is_key(true);
    column_pb.set_is_nullable(false);
    column_pb.set_length(4);
    if (has_compression) {
        column_pb.set_compression_type(compression);
        if (compression_level > 0) {
            column_pb.set_compression_level(compression_level);
        }
    }
    TabletColumn column;
    column.init_from_pb(column_pb);
    return column;
}

static TabletSchemaSPtr make_schema(const TabletColumn& column) {
    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
    schema->append_column(column);
    schema->_keys_type = DUP_KEYS;
    return schema;
}

class SegmentWriterColumnCompressionTest : public testing::Test {
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

// --- SegmentWriter (legacy path) ---

TEST_F(SegmentWriterColumnCompressionTest, SegmentWriterUsesPerColumnCodec) {
    auto column = make_int_column(true, CompressionTypePB::ZSTD, 9);
    auto schema = make_schema(column);
    SegmentWriterOptions opts;
    opts.compression_type = LZ4F; // table-level default, must be overridden per column
    auto file_writer = create_file_writer(0);
    SegmentWriter writer(file_writer.get(), 0, schema, nullptr, nullptr, opts, nullptr);

    ColumnMetaPB meta;
    ColumnWriterOptions col_opts;
    writer.init_column_meta(&meta, 0, column, col_opts);
    EXPECT_EQ(meta.compression(), CompressionTypePB::ZSTD);
    ASSERT_TRUE(meta.has_compression_level());
    EXPECT_EQ(meta.compression_level(), 9);
}

TEST_F(SegmentWriterColumnCompressionTest, SegmentWriterFallsBackToTableCodec) {
    auto column = make_int_column(false, UNKNOWN_COMPRESSION, 0);
    auto schema = make_schema(column);
    SegmentWriterOptions opts;
    opts.compression_type = LZ4F;
    auto file_writer = create_file_writer(1);
    SegmentWriter writer(file_writer.get(), 1, schema, nullptr, nullptr, opts, nullptr);

    ColumnMetaPB meta;
    ColumnWriterOptions col_opts;
    writer.init_column_meta(&meta, 0, column, col_opts);
    EXPECT_EQ(meta.compression(), LZ4F);
    EXPECT_FALSE(meta.has_compression_level());
}

TEST_F(SegmentWriterColumnCompressionTest, UpgradeLegacyColumnPbInheritsTableCodec) {
    ColumnPB legacy_pb;
    legacy_pb.set_unique_id(0);
    legacy_pb.set_name("c0");
    legacy_pb.set_type("INT");
    legacy_pb.set_is_key(true);
    legacy_pb.set_is_nullable(false);
    legacy_pb.set_length(4);
    std::string serialized;
    ASSERT_TRUE(legacy_pb.SerializeToString(&serialized));

    ColumnPB upgraded_pb;
    ASSERT_TRUE(upgraded_pb.ParseFromString(serialized));
    ASSERT_FALSE(upgraded_pb.has_compression_type());
    ASSERT_EQ(upgraded_pb.compression_type(), UNKNOWN_COMPRESSION);

    TabletColumn column(upgraded_pb);
    auto schema = make_schema(column);
    SegmentWriterOptions opts;
    opts.compression_type = LZ4F;
    auto file_writer = create_file_writer(2);
    SegmentWriter writer(file_writer.get(), 2, schema, nullptr, nullptr, opts, nullptr);

    ColumnMetaPB meta;
    ColumnWriterOptions col_opts;
    writer.init_column_meta(&meta, 0, column, col_opts);
    EXPECT_EQ(meta.compression(), LZ4F);
    EXPECT_FALSE(meta.has_compression_level());
}

TEST_F(SegmentWriterColumnCompressionTest, SegmentWriterColumnCodecOverridesTableNoCompression) {
    auto column = make_int_column(true, CompressionTypePB::ZSTD, 9);
    auto schema = make_schema(column);
    SegmentWriterOptions opts;
    opts.compression_type = NO_COMPRESSION;
    auto file_writer = create_file_writer(2);
    SegmentWriter writer(file_writer.get(), 2, schema, nullptr, nullptr, opts, nullptr);

    ColumnMetaPB meta;
    ColumnWriterOptions col_opts;
    writer.init_column_meta(&meta, 0, column, col_opts);
    EXPECT_EQ(meta.compression(), CompressionTypePB::ZSTD);
    ASSERT_TRUE(meta.has_compression_level());
    EXPECT_EQ(meta.compression_level(), 9);
}

// --- VerticalSegmentWriter (default path) ---

TEST_F(SegmentWriterColumnCompressionTest, VerticalSegmentWriterUsesPerColumnCodec) {
    auto column = make_int_column(true, CompressionTypePB::LZ4HC, 12);
    auto schema = make_schema(column);
    VerticalSegmentWriterOptions opts;
    opts.compression_type = LZ4F;
    auto file_writer = create_file_writer(3);
    TestVerticalSegmentWriter writer(file_writer.get(), 3, schema, nullptr, nullptr, opts, nullptr);

    ColumnMetaPB meta;
    ColumnWriterOptions col_opts;
    writer.build_meta(&meta, column, col_opts);
    EXPECT_EQ(meta.compression(), CompressionTypePB::LZ4HC);
    ASSERT_TRUE(meta.has_compression_level());
    EXPECT_EQ(meta.compression_level(), 12);
}

TEST_F(SegmentWriterColumnCompressionTest, VerticalSegmentWriterFallsBackToTableCodec) {
    auto column = make_int_column(false, UNKNOWN_COMPRESSION, 0);
    auto schema = make_schema(column);
    VerticalSegmentWriterOptions opts;
    opts.compression_type = LZ4F;
    auto file_writer = create_file_writer(4);
    TestVerticalSegmentWriter writer(file_writer.get(), 4, schema, nullptr, nullptr, opts, nullptr);

    ColumnMetaPB meta;
    ColumnWriterOptions col_opts;
    writer.build_meta(&meta, column, col_opts);
    EXPECT_EQ(meta.compression(), LZ4F);
    EXPECT_FALSE(meta.has_compression_level());
}

TEST_F(SegmentWriterColumnCompressionTest,
       VerticalSegmentWriterColumnCodecOverridesTableNoCompression) {
    auto column = make_int_column(true, CompressionTypePB::ZSTD, 9);
    auto schema = make_schema(column);
    VerticalSegmentWriterOptions opts;
    opts.compression_type = NO_COMPRESSION;
    auto file_writer = create_file_writer(5);
    TestVerticalSegmentWriter writer(file_writer.get(), 5, schema, nullptr, nullptr, opts, nullptr);

    ColumnMetaPB meta;
    ColumnWriterOptions col_opts;
    writer.build_meta(&meta, column, col_opts);
    EXPECT_EQ(meta.compression(), CompressionTypePB::ZSTD);
    ASSERT_TRUE(meta.has_compression_level());
    EXPECT_EQ(meta.compression_level(), 9);
}

// A per-column codec with no explicit level must persist the codec but leave the level absent
// (0 => "use codec default"), on the default write path.
TEST_F(SegmentWriterColumnCompressionTest, VerticalSegmentWriterPerColumnCodecWithoutLevel) {
    auto column = make_int_column(true, CompressionTypePB::ZSTD, 0);
    auto schema = make_schema(column);
    VerticalSegmentWriterOptions opts;
    opts.compression_type = LZ4F;
    auto file_writer = create_file_writer(6);
    TestVerticalSegmentWriter writer(file_writer.get(), 6, schema, nullptr, nullptr, opts, nullptr);

    ColumnMetaPB meta;
    ColumnWriterOptions col_opts;
    writer.build_meta(&meta, column, col_opts);
    EXPECT_EQ(meta.compression(), CompressionTypePB::ZSTD);
    EXPECT_FALSE(meta.has_compression_level());
}

} // namespace doris::segment_v2
