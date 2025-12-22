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

#include "olap/rowset/segment_v2/column_meta_accessor.h"

#include <crc32c/crc32c.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "util/coding.h"

using namespace doris;
using namespace doris::segment_v2;

namespace {

constexpr std::string_view kTestDir = "./ut_dir/column_meta_accessor_test";

// Helper to create a clean test directory
std::string make_test_file_path(const std::string& file_name) {
    auto fs = io::global_local_filesystem();
    static_cast<void>(fs->delete_directory(kTestDir));
    CHECK(fs->create_directory(kTestDir).ok());
    return std::string(kTestDir) + "/" + file_name;
}

// Helper to write segment footer trailer (footer + metadata)
Status append_footer_trailer(io::FileWriter* fw, SegmentFooterPB* footer) {
    std::string footer_buf;
    if (!footer->SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize SegmentFooterPB");
    }
    faststring fixed_buf;
    put_fixed32_le(&fixed_buf, static_cast<uint32_t>(footer_buf.size()));
    uint32_t checksum = crc32c::Crc32c(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    fixed_buf.append("D0R1", 4);
    std::vector<Slice> slices {Slice(footer_buf), Slice(fixed_buf)};
    RETURN_IF_ERROR(fw->appendv(slices.data(), slices.size()));
    return Status::OK();
}

// Helper to read footer from file
Status read_footer_from_file(const io::FileReaderSPtr& fr, SegmentFooterPB* footer) {
    const auto file_size = fr->size();
    if (file_size < 12) {
        return Status::Corruption("file too small");
    }
    uint8_t fixed_buf[12];
    size_t bytes_read = 0;
    RETURN_IF_ERROR(fr->read_at(file_size - 12, Slice(fixed_buf, sizeof(fixed_buf)), &bytes_read));
    if (bytes_read != sizeof(fixed_buf)) {
        return Status::Corruption("short read footer trailer");
    }
    const uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption("bad footer length");
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    bytes_read = 0;
    RETURN_IF_ERROR(fr->read_at(file_size - 12 - footer_length,
                                Slice(footer_buf.data(), footer_buf.size()), &bytes_read));
    if (bytes_read != footer_length) {
        return Status::Corruption("short read footer");
    }
    if (!footer->ParseFromArray(footer_buf.data(), static_cast<int>(footer_buf.size()))) {
        return Status::Corruption("failed to parse footer");
    }
    return Status::OK();
}

} // namespace

namespace doris {

using Generator = std::function<void(size_t rid, int cid, RowCursorCell& cell)>;

// Helper declarations are defined in tablet_schema_helper.{h,cpp} and
// delete_bitmap_calculator_test.cpp.
TabletSchemaSPtr create_schema(const std::vector<TabletColumnPtr>& columns,
                               KeysType keys_type = UNIQUE_KEYS);

void build_segment(SegmentWriterOptions opts, TabletSchemaSPtr build_schema, size_t segment_id,
                   TabletSchemaSPtr query_schema, size_t nrows, Generator generator,
                   std::shared_ptr<Segment>* res, std::string segment_dir);

void build_segment(SegmentWriterOptions opts, TabletSchemaSPtr build_schema, size_t segment_id,
                   TabletSchemaSPtr query_schema, size_t nrows, Generator generator,
                   std::shared_ptr<Segment>* res, std::string segment_dir, std::string* out_path);

} // namespace doris

// Test V2 (inline) mode with valid column metadata
TEST(ColumnMetaAccessorTest, V2_BasicInlineColumns) {
    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V2_BASELINE);

    // Add 3 columns with different types
    auto* col0 = footer.add_columns();
    col0->set_unique_id(10);
    col0->set_column_id(0);
    col0->set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));

    auto* col1 = footer.add_columns();
    col1->set_unique_id(20);
    col1->set_column_id(1);
    col1->set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));

    auto* col2 = footer.add_columns();
    col2->set_unique_id(30);
    col2->set_column_id(2);
    col2->set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_BIGINT));

    ColumnMetaAccessor accessor;
    ASSERT_TRUE(accessor.init(footer, nullptr).ok());

    // Test get_column_meta_by_column_ordinal_id
    ColumnMetaPB meta;
    ASSERT_TRUE(accessor.get_column_meta_by_column_ordinal_id(footer, 0, &meta).ok());
    EXPECT_EQ(meta.unique_id(), 10);
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));

    ASSERT_TRUE(accessor.get_column_meta_by_column_ordinal_id(footer, 1, &meta).ok());
    EXPECT_EQ(meta.unique_id(), 20);
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));

    ASSERT_TRUE(accessor.get_column_meta_by_column_ordinal_id(footer, 2, &meta).ok());
    EXPECT_EQ(meta.unique_id(), 30);
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_BIGINT));

    // Test has_column_uid for existing and non-existing uids
    EXPECT_TRUE(accessor.has_column_uid(10));
    EXPECT_TRUE(accessor.has_column_uid(20));
    EXPECT_TRUE(accessor.has_column_uid(30));
    EXPECT_FALSE(accessor.has_column_uid(999));

    // Test get_column_meta_by_uid
    ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, 10, &meta).ok());
    EXPECT_EQ(meta.column_id(), 0);
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));

    ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, 20, &meta).ok());
    EXPECT_EQ(meta.column_id(), 1);
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));

    ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, 30, &meta).ok());
    EXPECT_EQ(meta.column_id(), 2);
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_BIGINT));

    // Test traverse_metas
    std::vector<int32_t> traversed_uids;
    ASSERT_TRUE(accessor.traverse_metas(footer,
                                        [&](const ColumnMetaPB& m) {
                                            traversed_uids.push_back(m.unique_id());
                                        })
                        .ok());
    EXPECT_EQ(traversed_uids.size(), 3);
    EXPECT_EQ(traversed_uids[0], 10);
    EXPECT_EQ(traversed_uids[1], 20);
    EXPECT_EQ(traversed_uids[2], 30);
}

// Test V2 mode with variant subcolumns (unique_id == -1 should be skipped)
TEST(ColumnMetaAccessorTest, V2_SkipVariantSubcolumns) {
    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V2_BASELINE);

    // Regular column
    auto* col0 = footer.add_columns();
    col0->set_unique_id(10);
    col0->set_column_id(0);

    // Variant subcolumn (should be skipped in uid->colid map)
    auto* col1 = footer.add_columns();
    col1->set_unique_id(-1);
    col1->set_column_id(1);

    // Regular column
    auto* col2 = footer.add_columns();
    col2->set_unique_id(30);
    col2->set_column_id(2);

    ColumnMetaAccessor accessor;
    ASSERT_TRUE(accessor.init(footer, nullptr).ok());

    // Only regular columns should be accessible by uid; variant subcolumns (uid == -1) are skipped.
    ColumnMetaPB meta;
    ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, 10, &meta).ok());
    EXPECT_EQ(meta.column_id(), 0);
    ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, 30, &meta).ok());
    EXPECT_EQ(meta.column_id(), 2);
    Status st = accessor.get_column_meta_by_uid(footer, -1, &meta);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());

    // has_column_uid should also reflect the same semantics
    EXPECT_TRUE(accessor.has_column_uid(10));
    EXPECT_TRUE(accessor.has_column_uid(30));
    EXPECT_FALSE(accessor.has_column_uid(-1));
}

// Test V2 mode error cases
TEST(ColumnMetaAccessorTest, V2_ErrorCases) {
    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V2_BASELINE);

    // Add one column
    auto* col0 = footer.add_columns();
    col0->set_unique_id(10);
    col0->set_column_id(0);

    ColumnMetaAccessor accessor;
    ASSERT_TRUE(accessor.init(footer, nullptr).ok());

    // Test get_column_meta_by_column_ordinal_id with invalid column id
    ColumnMetaPB meta;
    Status st = accessor.get_column_meta_by_column_ordinal_id(footer, 999, &meta);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>());

    // Test empty footer
    SegmentFooterPB empty_footer;
    empty_footer.set_version(SEGMENT_FOOTER_VERSION_V2_BASELINE);
    ColumnMetaAccessor empty_accessor;
    ASSERT_TRUE(empty_accessor.init(empty_footer, nullptr).ok());

    st = empty_accessor.get_column_meta_by_column_ordinal_id(empty_footer, 0, &meta);
    EXPECT_FALSE(st.ok());

    st = empty_accessor.traverse_metas(empty_footer, [](const ColumnMetaPB&) {});
    EXPECT_FALSE(st.ok());
}

// Test V3 (external) mode with valid external meta region
TEST(ColumnMetaAccessorTest, V3_ExternalMetaRegion) {
    // 1. Create a V3 footer with external meta layout
    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);

    // Prepare 2 columns
    std::vector<ColumnMetaPB> columns;
    ColumnMetaPB col0;
    col0.set_unique_id(100);
    col0.set_column_id(0);
    col0.set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    col0.set_length(4);
    columns.push_back(col0);

    ColumnMetaPB col1;
    col1.set_unique_id(200);
    col1.set_column_id(1);
    col1.set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));
    col1.set_length(16);
    columns.push_back(col1);

    // 2. Write to file with external meta region
    std::string file_path = make_test_file_path("v3_external_meta.bin");
    auto fs = io::global_local_filesystem();
    io::FileWriterPtr fw;
    ASSERT_TRUE(fs->create_file(file_path, &fw).ok());

    // Write some dummy data before meta region
    std::string dummy_data(128, 'X');
    ASSERT_TRUE(fw->append(Slice(dummy_data)).ok());

    uint64_t meta_region_start = fw->bytes_appended();
    footer.set_col_meta_region_start(meta_region_start);

    // Write each column meta and record entry
    for (const auto& col : columns) {
        std::string meta_bytes;
        ASSERT_TRUE(col.SerializeToString(&meta_bytes));
        ASSERT_TRUE(fw->append(Slice(meta_bytes)).ok());

        auto* entry = footer.add_column_meta_entries();
        entry->set_unique_id(col.unique_id());
        entry->set_length(static_cast<uint32_t>(meta_bytes.size()));
    }

    // Write footer trailer
    ASSERT_TRUE(append_footer_trailer(fw.get(), &footer).ok());
    ASSERT_TRUE(fw->close().ok());

    // 3. Read back and test accessor
    io::FileReaderSPtr fr;
    io::FileReaderOptions opts;
    ASSERT_TRUE(fs->open_file(file_path, &fr, &opts).ok());

    SegmentFooterPB loaded_footer;
    ASSERT_TRUE(read_footer_from_file(fr, &loaded_footer).ok());

    ColumnMetaAccessor accessor;
    ASSERT_TRUE(accessor.init(loaded_footer, fr).ok());

    // Test get_column_meta_by_column_ordinal_id
    ColumnMetaPB meta;
    ASSERT_TRUE(accessor.get_column_meta_by_column_ordinal_id(loaded_footer, 0, &meta).ok());
    EXPECT_EQ(meta.unique_id(), 100);
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));

    ASSERT_TRUE(accessor.get_column_meta_by_column_ordinal_id(loaded_footer, 1, &meta).ok());
    EXPECT_EQ(meta.unique_id(), 200);
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));

    // Test get_column_meta_by_uid
    ASSERT_TRUE(accessor.get_column_meta_by_uid(loaded_footer, 100, &meta).ok());
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    ASSERT_TRUE(accessor.get_column_meta_by_uid(loaded_footer, 200, &meta).ok());
    EXPECT_EQ(meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));

    // Test traverse_metas
    std::vector<int32_t> traversed_uids;
    ASSERT_TRUE(accessor.traverse_metas(loaded_footer,
                                        [&](const ColumnMetaPB& m) {
                                            traversed_uids.push_back(m.unique_id());
                                        })
                        .ok());
    EXPECT_EQ(traversed_uids.size(), 2);
    EXPECT_EQ(traversed_uids[0], 100);
    EXPECT_EQ(traversed_uids[1], 200);
}

// Test V3 mode error cases
TEST(ColumnMetaAccessorTest, V3_ErrorCases_InvalidColumnId) {
    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);

    // Create minimal valid external meta
    std::string file_path = make_test_file_path("v3_error_test.bin");
    auto fs = io::global_local_filesystem();
    io::FileWriterPtr fw;
    ASSERT_TRUE(fs->create_file(file_path, &fw).ok());

    // Write some padding before meta region so that region_start is non-zero.
    // ExternalColMetaUtil::parse_external_meta_pointers() treats region_start == 0
    // as invalid layout.
    std::string dummy_data(16, 'X');
    ASSERT_TRUE(fw->append(Slice(dummy_data)).ok());
    uint64_t meta_region_start = fw->bytes_appended();
    footer.set_col_meta_region_start(meta_region_start);

    ColumnMetaPB col0;
    col0.set_unique_id(10);
    col0.set_column_id(0);
    std::string meta_bytes;
    ASSERT_TRUE(col0.SerializeToString(&meta_bytes));
    ASSERT_TRUE(fw->append(Slice(meta_bytes)).ok());

    auto* entry = footer.add_column_meta_entries();
    entry->set_unique_id(10);
    entry->set_length(static_cast<uint32_t>(meta_bytes.size()));

    ASSERT_TRUE(append_footer_trailer(fw.get(), &footer).ok());
    ASSERT_TRUE(fw->close().ok());

    io::FileReaderSPtr fr;
    io::FileReaderOptions opts;
    ASSERT_TRUE(fs->open_file(file_path, &fr, &opts).ok());

    SegmentFooterPB loaded_footer;
    ASSERT_TRUE(read_footer_from_file(fr, &loaded_footer).ok());

    ColumnMetaAccessor accessor;
    ASSERT_TRUE(accessor.init(loaded_footer, fr).ok());

    // Test invalid column_id
    ColumnMetaPB meta;
    Status st = accessor.get_column_meta_by_column_ordinal_id(loaded_footer, 999, &meta);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>());
}

// Test V3 mode with empty external meta region
TEST(ColumnMetaAccessorTest, V3_EmptyExternalMetaRegion) {
    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);
    footer.set_col_meta_region_start(100);
    // No column_meta_entries added (empty)

    ColumnMetaAccessor accessor;
    // For an empty external meta region (num_columns == 0), the current
    // implementation treats this as a corrupted layout and init() fails.
    Status st = accessor.init(footer, nullptr);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>());
}

// Test V3 mode with mismatched entries count
TEST(ColumnMetaAccessorTest, V3_MismatchedEntriesCount) {
    std::string file_path = make_test_file_path("v3_mismatch.bin");
    auto fs = io::global_local_filesystem();
    io::FileWriterPtr fw;
    ASSERT_TRUE(fs->create_file(file_path, &fw).ok());

    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);

    // Write some padding before meta region so that region_start is non-zero.
    std::string dummy_data(16, 'X');
    ASSERT_TRUE(fw->append(Slice(dummy_data)).ok());
    uint64_t meta_region_start = fw->bytes_appended();
    footer.set_col_meta_region_start(meta_region_start);

    // Write 2 column metas
    for (int i = 0; i < 2; ++i) {
        ColumnMetaPB col;
        col.set_unique_id(10 + i);
        col.set_column_id(i);
        std::string meta_bytes;
        ASSERT_TRUE(col.SerializeToString(&meta_bytes));
        ASSERT_TRUE(fw->append(Slice(meta_bytes)).ok());

        auto* entry = footer.add_column_meta_entries();
        entry->set_unique_id(10 + i);
        entry->set_length(static_cast<uint32_t>(meta_bytes.size()));
    }

    ASSERT_TRUE(append_footer_trailer(fw.get(), &footer).ok());
    ASSERT_TRUE(fw->close().ok());

    io::FileReaderSPtr fr;
    io::FileReaderOptions opts;
    ASSERT_TRUE(fs->open_file(file_path, &fr, &opts).ok());

    SegmentFooterPB loaded_footer;
    ASSERT_TRUE(read_footer_from_file(fr, &loaded_footer).ok());

    ColumnMetaAccessor accessor;
    // Initialize accessor with the original footer (entries count matches num_columns).
    ASSERT_TRUE(accessor.init(loaded_footer, fr).ok());

    // Manually corrupt the footer by adding extra entry after init so that
    // footer.column_meta_entries_size() != ptrs.num_columns, which should be
    // detected by traverse_metas.
    loaded_footer.add_column_meta_entries()->set_unique_id(999);

    // traverse_metas should detect mismatch
    Status st = accessor.traverse_metas(loaded_footer, [](const ColumnMetaPB&) {});
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>());
}

// Test V3 mode with corrupted protobuf data
TEST(ColumnMetaAccessorTest, V3_CorruptedProtobufData) {
    std::string file_path = make_test_file_path("v3_corrupted.bin");
    auto fs = io::global_local_filesystem();
    io::FileWriterPtr fw;
    ASSERT_TRUE(fs->create_file(file_path, &fw).ok());

    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);

    // Write some padding before meta region so that region_start is non-zero.
    std::string dummy_data(16, 'X');
    ASSERT_TRUE(fw->append(Slice(dummy_data)).ok());
    uint64_t meta_region_start = fw->bytes_appended();
    footer.set_col_meta_region_start(meta_region_start);

    // Write garbage data instead of valid protobuf
    std::string garbage(100, 'Z');
    ASSERT_TRUE(fw->append(Slice(garbage)).ok());

    auto* entry = footer.add_column_meta_entries();
    entry->set_unique_id(10);
    entry->set_length(100);

    ASSERT_TRUE(append_footer_trailer(fw.get(), &footer).ok());
    ASSERT_TRUE(fw->close().ok());

    io::FileReaderSPtr fr;
    io::FileReaderOptions opts;
    ASSERT_TRUE(fs->open_file(file_path, &fr, &opts).ok());

    SegmentFooterPB loaded_footer;
    ASSERT_TRUE(read_footer_from_file(fr, &loaded_footer).ok());

    ColumnMetaAccessor accessor;
    ASSERT_TRUE(accessor.init(loaded_footer, fr).ok());

    // traverse_metas should fail to parse corrupted data
    Status st = accessor.traverse_metas(loaded_footer, [](const ColumnMetaPB&) {});
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>());
}

// Test version detection and automatic V2/V3 selection
TEST(ColumnMetaAccessorTest, VersionDetection) {
    // V1 footer should use V2 accessor (inline mode)
    {
        SegmentFooterPB footer;
        footer.set_version(1);
        auto* col = footer.add_columns();
        col->set_unique_id(10);
        col->set_column_id(0);

        ColumnMetaAccessor accessor;
        ASSERT_TRUE(accessor.init(footer, nullptr).ok());

        ColumnMetaPB meta;
        ASSERT_TRUE(accessor.get_column_meta_by_column_ordinal_id(footer, 0, &meta).ok());
        EXPECT_EQ(meta.unique_id(), 10);
    }

    // V2 footer should use V2 accessor
    {
        SegmentFooterPB footer;
        footer.set_version(SEGMENT_FOOTER_VERSION_V2_BASELINE);
        auto* col = footer.add_columns();
        col->set_unique_id(20);
        col->set_column_id(0);

        ColumnMetaAccessor accessor;
        ASSERT_TRUE(accessor.init(footer, nullptr).ok());

        ColumnMetaPB meta;
        ASSERT_TRUE(accessor.get_column_meta_by_column_ordinal_id(footer, 0, &meta).ok());
        EXPECT_EQ(meta.unique_id(), 20);
    }

    // V3 footer without valid external meta is treated as corrupted layout and
    // init() should fail (no automatic fallback to V2 inline mode).
    {
        SegmentFooterPB footer;
        footer.set_version(SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);
        auto* col = footer.add_columns();
        col->set_unique_id(30);
        col->set_column_id(0);

        ColumnMetaAccessor accessor;
        Status st = accessor.init(footer, nullptr);
        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>());
    }
}

// Test large number of columns
TEST(ColumnMetaAccessorTest, LargeNumberOfColumns) {
    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V2_BASELINE);

    const int num_columns = 1000;
    for (int i = 0; i < num_columns; ++i) {
        auto* col = footer.add_columns();
        col->set_unique_id(i);
        col->set_column_id(i);
        col->set_type(static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    }

    ColumnMetaAccessor accessor;
    ASSERT_TRUE(accessor.init(footer, nullptr).ok());

    // Spot check mapping via get_column_meta_by_uid
    ColumnMetaPB meta;
    ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, 0, &meta).ok());
    EXPECT_EQ(meta.column_id(), 0);
    ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, 500, &meta).ok());
    EXPECT_EQ(meta.column_id(), 500);
    ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, 999, &meta).ok());
    EXPECT_EQ(meta.column_id(), 999);

    // Test traverse_metas counts correctly
    int count = 0;
    ASSERT_TRUE(accessor.traverse_metas(footer, [&](const ColumnMetaPB&) { count++; }).ok());
    EXPECT_EQ(count, num_columns);
}

// Compare footer size with and without external ColumnMetaPB region under high column count
// using real segments written by SegmentWriter.
// This test builds two real segments (inline V2 vs external V3 layout) on disk:
// - schema has 10,00 INT columns
// - each column has 1,000 rows of data
// and verifies that the external layout significantly reduces the proto size of the footer.
TEST(ColumnMetaAccessorTest, FooterSizeWithManyColumnsExternalVsInline) {
    constexpr int kNumColumns = 1000;
    constexpr int kNumRowsPerColumn = 100;

    // Ensure test directory exists before building real segments.
    {
        auto fs = io::global_local_filesystem();
        static_cast<void>(fs->delete_directory(kTestDir));
        ASSERT_TRUE(fs->create_directory(kTestDir).ok());
    }

    // 1. Build common TabletSchema with many INT columns.
    std::vector<TabletColumnPtr> columns;
    columns.reserve(kNumColumns);
    for (int i = 0; i < kNumColumns; ++i) {
        auto col = std::make_shared<TabletColumn>();
        col->set_unique_id(i);
        col->set_name(fmt::format("c{}", i));
        col->set_type(FieldType::OLAP_FIELD_TYPE_INT);
        // Mark the first column as key so that SegmentWriter produces non-empty
        // min/max encoded keys, which are asserted inside the shared build_segment helper.
        col->set_is_key(i == 0);
        col->set_is_nullable(true);
        col->set_length(4);
        // Set index_length for key columns so SegmentWriter can build key index properly.
        col->set_index_length(4);
        columns.emplace_back(std::move(col));
    }

    TabletSchemaSPtr inline_schema = create_schema(columns, UNIQUE_KEYS);
    TabletSchemaSPtr external_schema = create_schema(columns, UNIQUE_KEYS);
    // Enable external ColumnMetaPB for the second schema so that SegmentWriter
    // produces a V3 footer with externalized column meta region.
    external_schema->set_external_segment_meta_used_default(true);

    // 2. Common SegmentWriter options and row generator.
    SegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = false;

    auto generator = [](size_t rid, int cid, RowCursorCell& cell) {
        cell.set_not_null();
        // deterministic int payload: value = rid * 10 + cid
        *reinterpret_cast<int*>(cell.mutable_cell_ptr()) = static_cast<int>(rid * 10 + cid);
    };

    // 3. Build inline segment (V2 footer, inline ColumnMetaPB).
    std::shared_ptr<Segment> inline_segment;
    std::string inline_segment_path;
    build_segment(opts, inline_schema,
                  /*segment_id=*/0, inline_schema, kNumRowsPerColumn, generator, &inline_segment,
                  std::string(kTestDir), &inline_segment_path);
    ASSERT_NE(inline_segment, nullptr);

    // 4. Build external segment (V3 footer, external ColumnMetaPB).
    std::shared_ptr<Segment> external_segment;
    std::string external_segment_path;
    build_segment(opts, external_schema,
                  /*segment_id=*/1, external_schema, kNumRowsPerColumn, generator,
                  &external_segment, std::string(kTestDir), &external_segment_path);
    ASSERT_NE(external_segment, nullptr);

    // 5. Read footers back from real segment files and compare proto sizes.
    auto fs = io::global_local_filesystem();
    io::FileReaderSPtr fr_inline;
    io::FileReaderSPtr fr_external;
    io::FileReaderOptions reader_opts;

    ASSERT_TRUE(fs->open_file(inline_segment_path, &fr_inline, &reader_opts).ok());
    ASSERT_TRUE(fs->open_file(external_segment_path, &fr_external, &reader_opts).ok());

    SegmentFooterPB inline_footer;
    SegmentFooterPB external_footer;
    ASSERT_TRUE(read_footer_from_file(fr_inline, &inline_footer).ok());
    ASSERT_TRUE(read_footer_from_file(fr_external, &external_footer).ok());

    const size_t inline_footer_size = inline_footer.ByteSizeLong();
    const size_t external_footer_size = external_footer.ByteSizeLong();

    std::cout << "Real segment footer size with " << kNumColumns
              << " columns (inline vs external): inline=" << inline_footer_size
              << " bytes, external=" << external_footer_size << " bytes" << std::endl;

    // External layout should significantly reduce the proto size of the footer.
    EXPECT_LT(external_footer_size, inline_footer_size / 10);
}

// Test concurrent access (thread safety not guaranteed by ColumnMetaAccessor itself,
// but test that multiple sequential calls work correctly)
TEST(ColumnMetaAccessorTest, MultipleSequentialAccesses) {
    SegmentFooterPB footer;
    footer.set_version(SEGMENT_FOOTER_VERSION_V2_BASELINE);

    for (int i = 0; i < 10; ++i) {
        auto* col = footer.add_columns();
        col->set_unique_id(i);
        col->set_column_id(i);
    }

    ColumnMetaAccessor accessor;
    ASSERT_TRUE(accessor.init(footer, nullptr).ok());

    // Multiple calls should give consistent results
    for (int round = 0; round < 100; ++round) {
        for (int i = 0; i < 10; ++i) {
            ColumnMetaPB meta;
            ASSERT_TRUE(accessor.get_column_meta_by_column_ordinal_id(footer, i, &meta).ok());
            EXPECT_EQ(meta.unique_id(), i);
            // ensure uid lookup also works repeatedly
            ASSERT_TRUE(accessor.get_column_meta_by_uid(footer, i, &meta).ok());
        }
    }
}
