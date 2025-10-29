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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/segment.h"
#include "util/coding.h"
#include "util/crc32c.h"

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
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
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

// Test V2 (inline) mode with valid column metadata
TEST(ColumnMetaAccessorTest, V2_BasicInlineColumns) {
    SegmentFooterPB footer;
    footer.set_version(kSegmentFooterVersionV2);

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
    footer.set_version(kSegmentFooterVersionV2);

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
    footer.set_version(kSegmentFooterVersionV2);

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
    empty_footer.set_version(kSegmentFooterVersionV2);
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
    footer.set_version(kSegmentFooterVersionV3_ExtColMeta);

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
    footer.set_version(kSegmentFooterVersionV3_ExtColMeta);

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
    footer.set_version(kSegmentFooterVersionV3_ExtColMeta);
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
    footer.set_version(kSegmentFooterVersionV3_ExtColMeta);

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
    footer.set_version(kSegmentFooterVersionV3_ExtColMeta);

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
        footer.set_version(kSegmentFooterVersionV2);
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
        footer.set_version(kSegmentFooterVersionV3_ExtColMeta);
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
    footer.set_version(kSegmentFooterVersionV2);

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

// Test concurrent access (thread safety not guaranteed by ColumnMetaAccessor itself,
// but test that multiple sequential calls work correctly)
TEST(ColumnMetaAccessorTest, MultipleSequentialAccesses) {
    SegmentFooterPB footer;
    footer.set_version(kSegmentFooterVersionV2);

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
