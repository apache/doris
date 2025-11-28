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

#include "olap/rowset/segment_v2/external_col_meta_util.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/rowset/segment_v2/variant/variant_ext_meta_writer.h"
#include "olap/rowset/segment_v2/variant/variant_external_meta_reader.h"
#include "olap/tablet_schema_helper.h"
#include "olap/types.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "vec/json/path_in_data.h"

using namespace doris;
using namespace doris::segment_v2;
using namespace doris::vectorized;

namespace {

constexpr std::string_view kTestDir = "./ut_dir/external_col_meta_util_test";

// Helper to create a clean local test directory and return absolute path + filename.
std::string make_test_file_path(const std::string& file_name) {
    auto fs = io::global_local_filesystem();
    // english only in comments
    static_cast<void>(fs->delete_directory(kTestDir));
    CHECK(fs->create_directory(kTestDir).ok());
    return std::string(kTestDir) + "/" + file_name;
}

// Helper to write a simple "segment-like" footer trailer. Current tests do not call
// Segment::_parse_footer, so the exact magic value is not important as long as the
// layout is self‑consistent.
Status append_footer_trailer(io::FileWriter* fw, SegmentFooterPB* footer) {
    std::string footer_buf;
    if (!footer->SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize SegmentFooterPB for test");
    }
    faststring fixed_buf;
    // footer size (4 bytes)
    put_fixed32_le(&fixed_buf, static_cast<uint32_t>(footer_buf.size()));
    // footer checksum (4 bytes)
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    // magic number (4 bytes)
    fixed_buf.append("D0R1", 4);
    std::vector<Slice> slices {Slice(footer_buf), Slice(fixed_buf)};
    RETURN_IF_ERROR(fw->appendv(slices.data(), slices.size()));
    return Status::OK();
}

// Helper to read SegmentFooterPB back from a segment-like file and validate checksum.
Status read_footer_from_file(const io::FileReaderSPtr& fr, SegmentFooterPB* footer) {
    const auto file_size = fr->size();
    if (file_size < 12) {
        return Status::Corruption("test segment file too small: {}", file_size);
    }
    uint8_t fixed_buf[12];
    size_t bytes_read = 0;
    RETURN_IF_ERROR(fr->read_at(file_size - 12, Slice(fixed_buf, sizeof(fixed_buf)), &bytes_read));
    if (bytes_read != sizeof(fixed_buf)) {
        return Status::Corruption("short read footer trailer, read={}, expect={}", bytes_read,
                                  sizeof(fixed_buf));
    }
    const uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption("bad footer length in test segment: file_size={}, footer_len={}",
                                  file_size, footer_length);
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    bytes_read = 0;
    RETURN_IF_ERROR(fr->read_at(file_size - 12 - footer_length,
                                Slice(footer_buf.data(), footer_buf.size()), &bytes_read));
    if (bytes_read != footer_length) {
        return Status::Corruption("short read footer pb, read={}, expect={}", bytes_read,
                                  footer_length);
    }
    const uint32_t expect_checksum = decode_fixed32_le(fixed_buf + 4);
    const uint32_t actual_checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption("footer checksum mismatch, actual={}, expect={}", actual_checksum,
                                  expect_checksum);
    }
    if (!footer->ParseFromArray(footer_buf.data(), static_cast<int>(footer_buf.size()))) {
        return Status::Corruption("failed to parse SegmentFooterPB from test segment");
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

// Basic unit test for ExternalColMetaUtil pointer parsing and uid‑>col_id map.
TEST(ExternalColMetaUtilTest, PointersAndUidMapValidation) {
    SegmentFooterPB footer;
    footer.set_col_meta_region_start(100U);
    auto* e0 = footer.add_column_meta_entries();
    e0->set_unique_id(10);
    e0->set_length(50);
    auto* e1 = footer.add_column_meta_entries();
    e1->set_unique_id(20);
    e1->set_length(60);

    ExternalColMetaUtil::ExternalMetaPointers ptrs;
    EXPECT_TRUE(ExternalColMetaUtil::parse_external_meta_pointers(footer, &ptrs).ok());
    EXPECT_EQ(ptrs.region_start, 100U);
    EXPECT_EQ(ptrs.num_columns, 2U);
    EXPECT_EQ(ptrs.region_end, 210U);

    std::unordered_map<int32_t, size_t> uid2colid;
    EXPECT_TRUE(ExternalColMetaUtil::parse_uid_to_colid_map(footer, ptrs, &uid2colid).ok());
    EXPECT_EQ(uid2colid.size(), 2U);
    EXPECT_EQ(uid2colid[10], 0U);
    EXPECT_EQ(uid2colid[20], 1U);

    // Negative cases for pointers
    footer.clear_column_meta_entries();
    EXPECT_FALSE(ExternalColMetaUtil::parse_external_meta_pointers(footer, &ptrs).ok());
    footer.add_column_meta_entries()->set_unique_id(10);
    footer.add_column_meta_entries()->set_unique_id(20);
    footer.mutable_column_meta_entries(0)->set_length(50);
    footer.mutable_column_meta_entries(1)->set_length(60);
    // zero length is illegal
    footer.mutable_column_meta_entries(1)->set_length(0);
    EXPECT_FALSE(ExternalColMetaUtil::parse_external_meta_pointers(footer, &ptrs).ok());
    footer.mutable_column_meta_entries(1)->set_length(60);

    // Slice validation
    ExternalColMetaUtil::ExternalMetaPointers p;
    p.region_start = 100;
    p.region_end = 200;
    EXPECT_TRUE(ExternalColMetaUtil::is_valid_meta_slice(100, 10, p));
    EXPECT_FALSE(ExternalColMetaUtil::is_valid_meta_slice(100, 0, p));  // size == 0
    EXPECT_FALSE(ExternalColMetaUtil::is_valid_meta_slice(50, 10, p));  // pos < region_start
    EXPECT_FALSE(ExternalColMetaUtil::is_valid_meta_slice(195, 10, p)); // pos + size > region_end
    // overflow guard: deliberately craft overflow
    EXPECT_FALSE(ExternalColMetaUtil::is_valid_meta_slice(std::numeric_limits<uint64_t>::max() - 1,
                                                          4, p));
}

// End‑to‑end test of VariantExtMetaWriter + VariantExternalMetaReader on a standalone file.
// This does NOT involve full tablet/rowset plumbing, but exercises:
//  - externalize_from_footer(): classification of root/non‑sparse/sparse columns
//  - flush_to_footer(): writing IndexedColumn metas into footer.file_meta_datas
//  - init_from_footer()/lookup_meta_by_path(): reading back subcolumn ColumnMetaPB by path
//  - legacy unsuffixed keys: "variant_meta_keys" / "variant_meta_values"
TEST(ExternalColMetaUtilTest, VariantExtMetaWriterAndReaderInteropAndCompatibility) {
    // 1. Prepare a simple footer with:
    //    - one non‑variant column (kept as is)
    //    - one variant root (kept, path "v1")
    //    - one non‑sparse subcolumn "v1.key0" (externalized)
    //    - one sparse subcolumn "__DORIS_VARIANT_SPARSE__.key_sparse" (embedded into root)
    SegmentFooterPB footer;
    // non‑variant top level column
    {
        auto* col = footer.add_columns();
        col->set_unique_id(0);
        // use int(FieldType) to match production code
        col->set_type(int(FieldType::OLAP_FIELD_TYPE_INT)); // simple scalar
    }

    const int32_t root_uid = 1;
    // variant root column with path "v1"
    ColumnMetaPB* root = footer.add_columns();
    root->set_unique_id(root_uid);
    root->set_type(int(FieldType::OLAP_FIELD_TYPE_VARIANT));
    {
        PathInData full_root_path("v1");
        full_root_path.to_protobuf(root->mutable_column_path_info(),
                                   /*parent_col_unique_id=*/root_uid);
    }

    // non‑sparse subcolumn: v1.key0
    ColumnMetaPB* non_sparse = footer.add_columns();
    non_sparse->set_unique_id(2);
    non_sparse->set_type(int(FieldType::OLAP_FIELD_TYPE_INT));
    {
        PathInData full_path("v1.key0");
        full_path.to_protobuf(non_sparse->mutable_column_path_info(),
                              /*parent_col_unique_id=*/root_uid);
        non_sparse->set_none_null_size(123);
    }

    // sparse subcolumn: v1.__DORIS_VARIANT_SPARSE__
    ColumnMetaPB* sparse = footer.add_columns();
    sparse->set_unique_id(3);
    sparse->set_type(int(FieldType::OLAP_FIELD_TYPE_VARIANT));
    {
        // For sparse column, the relative path (after pop_front) should contain
        // "__DORIS_VARIANT_SPARSE__" so that VariantExtMetaWriter treats it as sparse.
        PathInData full_path("v1.__DORIS_VARIANT_SPARSE__");
        full_path.to_protobuf(sparse->mutable_column_path_info(),
                              /*parent_col_unique_id=*/root_uid);
        // carry sparse statistics in VariantStatisticsPB
        auto* stats = sparse->mutable_variant_statistics();
        (*stats->mutable_sparse_column_non_null_size())["key_sparse"] = 10;
    }

    // 2. Externalize non‑sparse subcolumns into a dedicated file via VariantExtMetaWriter.
    std::string file_path = make_test_file_path("variant_ext_meta.bin");
    io::FileWriterPtr fw;
    auto fs = io::global_local_filesystem();
    Status st = fs->create_file(file_path, &fw);
    ASSERT_TRUE(st.ok()) << st.to_json();

    std::vector<ColumnMetaPB> out_metas;
    {
        VariantExtMetaWriter ext_writer(fw.get(), CompressionTypePB::LZ4);
        st = ext_writer.externalize_from_footer(&footer, &out_metas);
        ASSERT_TRUE(st.ok()) << st.to_json();
    }

    // Capture region start AFTER Variant Index is written
    uint64_t meta_region_start = fw->bytes_appended();

    // Manually write the Meta Region (simulating ExternalColMetaUtil::write_external_column_meta)
    footer.clear_column_meta_entries();
    footer.set_col_meta_region_start(meta_region_start);

    for (const auto& meta : out_metas) {
        std::string meta_bytes;
        meta.SerializeToString(&meta_bytes);
        ASSERT_TRUE(fw->append(Slice(meta_bytes)).ok());

        auto* entry = footer.add_column_meta_entries();
        entry->set_unique_id(meta.unique_id());
        entry->set_length(meta_bytes.size());
    }

    // After externalize_from_footer:
    //  - footer.columns() should contain:
    //      * non‑variant column uid=0
    //      * root variant column uid=1, whose children_columns includes the sparse meta
    //  - "v1.key0" should be removed from footer.columns() and only exist in external meta.
    ASSERT_EQ(footer.columns_size(), 2);
    const ColumnMetaPB& kept0 = footer.columns(0);
    const ColumnMetaPB& kept1 = footer.columns(1);
    EXPECT_EQ(kept0.unique_id(), 0);
    EXPECT_EQ(kept1.unique_id(), root_uid);
    // root should have embedded sparse children
    ASSERT_EQ(kept1.children_columns_size(), 1);
    const ColumnMetaPB& embedded_sparse = kept1.children_columns(0);
    EXPECT_TRUE(embedded_sparse.has_column_path_info());
    PathInData embedded_path;
    embedded_path.from_protobuf(embedded_sparse.column_path_info());
    // full path should keep root prefix
    EXPECT_EQ(embedded_path.get_path(), "v1.__DORIS_VARIANT_SPARSE__");
    ASSERT_TRUE(embedded_sparse.has_variant_statistics());
    EXPECT_EQ(embedded_sparse.variant_statistics().sparse_column_non_null_size().at("key_sparse"),
              10);

    // 3. Verify that ext meta keys metas were flushed into footer.file_meta_datas.
    bool found_keys_meta = false;
    std::string keys_meta_bytes;
    for (const auto& m : footer.file_meta_datas()) {
        if (m.key() == "variant_meta_keys." + std::to_string(root_uid)) {
            found_keys_meta = true;
            keys_meta_bytes = m.value();
        }
    }
    ASSERT_TRUE(found_keys_meta);
    // variant_meta_values should NOT be present

    // 4. Finish the "segment‑like" file by appending footer + trailer, then close.
    st = append_footer_trailer(fw.get(), &footer);
    ASSERT_TRUE(st.ok()) << st.to_json();
    ASSERT_TRUE(fw->close().ok());

    // 5. Open a reader on the same file and build VariantExternalMetaReader from the footer.
    io::FileReaderSPtr fr;
    io::FileReaderOptions opts;
    st = fs->open_file(file_path, &fr, &opts);
    ASSERT_TRUE(st.ok()) << st.to_json();

    VariantExternalMetaReader reader_new;
    auto footer_sp = std::make_shared<SegmentFooterPB>();
    footer_sp->CopyFrom(footer);
    st = reader_new.init_from_footer(footer_sp, fr, root_uid);
    ASSERT_TRUE(st.ok()) << st.to_json();
    ASSERT_TRUE(reader_new.available());

    // 6. Lookup meta by relative path and verify content.
    ColumnMetaPB loaded_meta;
    st = reader_new.lookup_meta_by_path("key0", &loaded_meta);
    ASSERT_TRUE(st.ok()) << st.to_json();
    ASSERT_TRUE(loaded_meta.has_column_path_info());
    PathInData loaded_path;
    loaded_path.from_protobuf(loaded_meta.column_path_info());
    // full path should still contain root prefix "v1.key0"
    EXPECT_EQ(loaded_path.get_path(), "v1.key0");
    EXPECT_EQ(loaded_meta.none_null_size(), 123);
    EXPECT_EQ(loaded_meta.column_path_info().parrent_column_unique_id(), root_uid);

    // 7. Legacy compatibility: construct a footer with unsuffixed keys:
    //      "variant_meta_keys"
    SegmentFooterPB legacy_footer;
    legacy_footer.CopyFrom(footer);
    legacy_footer.clear_file_meta_datas();
    {
        auto* p1 = legacy_footer.add_file_meta_datas();
        p1->set_key("variant_meta_keys");
        p1->set_value(keys_meta_bytes);
    }

    VariantExternalMetaReader reader_legacy;
    auto legacy_footer_sp = std::make_shared<SegmentFooterPB>();
    legacy_footer_sp->CopyFrom(legacy_footer);
    st = reader_legacy.init_from_footer(legacy_footer_sp, fr, root_uid);
    ASSERT_TRUE(st.ok()) << st.to_json();
    ASSERT_TRUE(reader_legacy.available());

    ColumnMetaPB loaded_meta_legacy;
    st = reader_legacy.lookup_meta_by_path("key0", &loaded_meta_legacy);
    ASSERT_TRUE(st.ok()) << st.to_json();
    PathInData loaded_path_legacy;
    loaded_path_legacy.from_protobuf(loaded_meta_legacy.column_path_info());
    EXPECT_EQ(loaded_path_legacy.get_path(), "v1.key0");
    EXPECT_EQ(loaded_meta_legacy.none_null_size(), 123);
    EXPECT_EQ(loaded_meta_legacy.column_path_info().parrent_column_unique_id(), root_uid);

    // 8. Bulk load all external metas once and verify subcolumn tree and statistics.
    SubcolumnColumnMetaInfo meta_tree;
    VariantStatistics stats;
    st = reader_new.load_all_once(&meta_tree, &stats);
    ASSERT_TRUE(st.ok()) << st.to_json();
    EXPECT_FALSE(meta_tree.empty());
    // Non-sparse subcolumn v1.key0 should be recorded as relative path "key0".
    ASSERT_EQ(stats.subcolumns_non_null_size.size(), 1);
    EXPECT_EQ(stats.subcolumns_non_null_size.at("key0"), 123);

    // load_all_once should be idempotent.
    st = reader_new.load_all_once(&meta_tree, &stats);
    ASSERT_TRUE(st.ok()) << st.to_json();

    // 9. has_prefix should detect existing keys and non-existing prefixes.
    bool has = false;
    st = reader_new.has_prefix("key", &has);
    ASSERT_TRUE(st.ok()) << st.to_json();
    EXPECT_TRUE(has);
    has = false;
    st = reader_new.has_prefix("non_existent_prefix", &has);
    ASSERT_TRUE(st.ok()) << st.to_json();
    EXPECT_FALSE(has);

    has = false;
    st = reader_new.has_prefix("", &has);
    ASSERT_TRUE(st.ok()) << st.to_json();
    EXPECT_TRUE(has);
}

// End-to-end test: use ExternalColMetaUtil to write external ColumnMeta region + CMO
// into a V3 segment-like file, then read footer back and verify:
//  - footer.version() is V3
//  - external meta pointers / uid->col_id map are present and valid
//  - ColumnMetaPB can be read back through CMO by col_id
TEST(ExternalColMetaUtilTest, WriteAndReadV3SegmentExternalMetaRoundTrip) {
    // 1. Build a simple footer with two top-level columns so uid->col_id map is deterministic.
    SegmentFooterPB footer;
    {
        auto* col0 = footer.add_columns();
        col0->set_column_id(0);
        col0->set_unique_id(10);
        col0->set_type(int(FieldType::OLAP_FIELD_TYPE_INT));
        col0->set_length(4);
        col0->set_encoding(DEFAULT_ENCODING);
        col0->set_compression(CompressionTypePB::LZ4);
        col0->set_is_nullable(false);

        auto* col1 = footer.add_columns();
        col1->set_column_id(1);
        col1->set_unique_id(20);
        col1->set_type(int(FieldType::OLAP_FIELD_TYPE_BIGINT));
        col1->set_length(8);
        col1->set_encoding(DEFAULT_ENCODING);
        col1->set_compression(CompressionTypePB::LZ4);
        col1->set_is_nullable(true);
    }

    // Stamp V3 version like SegmentWriter::_write_footer does for external meta enabled tablets.
    footer.set_version(segment_v2::SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);

    std::string file_path = make_test_file_path("v3_ext_col_meta_segment.bin");
    auto fs = io::global_local_filesystem();
    io::FileWriterPtr fw;
    Status st = fs->create_file(file_path, &fw);
    ASSERT_TRUE(st.ok()) << st.to_json();

    // Simulate real segment layout: write some dummy data pages before external meta region
    // so that `col_meta_region_start` is non-zero (as in production segments).
    {
        std::string dummy_data(16, 'X');
        ASSERT_TRUE(fw->append(Slice(dummy_data)).ok());
    }

    // 2. Write external ColumnMeta region + CMO + uid->col_id mapping.
    st = ExternalColMetaUtil::write_external_column_meta(
            fw.get(), &footer, CompressionTypePB::LZ4,
            [fw_raw = fw.get()](const std::vector<Slice>& slices) {
                return fw_raw->appendv(slices.data(), slices.size());
            });
    ASSERT_TRUE(st.ok()) << st.to_json();

    // After write_external_column_meta, inline footer.columns() are cleared in V3 mode.
    ASSERT_EQ(footer.columns_size(), 0);

    // 3. Finish by appending footer + trailer and close writer.
    st = append_footer_trailer(fw.get(), &footer);
    ASSERT_TRUE(st.ok()) << st.to_json();
    ASSERT_TRUE(fw->close().ok());

    // 4. Re-open file, parse footer back from on-disk trailer, and verify footer.version() is V3.
    io::FileReaderSPtr fr;
    io::FileReaderOptions opts;
    st = fs->open_file(file_path, &fr, &opts);
    ASSERT_TRUE(st.ok()) << st.to_json();

    SegmentFooterPB loaded_footer;
    st = read_footer_from_file(fr, &loaded_footer);
    ASSERT_TRUE(st.ok()) << st.to_json();
    EXPECT_EQ(loaded_footer.version(), segment_v2::SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);

    // 5. Parse external meta pointers and uid->col_id mapping from the loaded footer.
    ExternalColMetaUtil::ExternalMetaPointers ptrs;
    ASSERT_TRUE(ExternalColMetaUtil::parse_external_meta_pointers(loaded_footer, &ptrs).ok());
    std::unordered_map<int32_t, size_t> uid2colid;
    ASSERT_TRUE(ExternalColMetaUtil::parse_uid_to_colid_map(loaded_footer, ptrs, &uid2colid).ok());
    ASSERT_EQ(uid2colid.size(), 2U);
    EXPECT_EQ(uid2colid[10], 0U);
    EXPECT_EQ(uid2colid[20], 1U);

    // 6. Read ColumnMetaPB back from external meta region and verify it matches the
    //    original unique_ids.
    ColumnMetaPB meta0;
    st = ExternalColMetaUtil::read_col_meta(fr, loaded_footer, ptrs, /*col_id=*/0, &meta0);
    ASSERT_TRUE(st.ok()) << st.to_json();
    EXPECT_EQ(meta0.unique_id(), 10);
    EXPECT_EQ(meta0.column_id(), 0);

    ColumnMetaPB meta1;
    st = ExternalColMetaUtil::read_col_meta(fr, loaded_footer, ptrs, /*col_id=*/1, &meta1);
    ASSERT_TRUE(st.ok()) << st.to_json();
    EXPECT_EQ(meta1.unique_id(), 20);
    EXPECT_EQ(meta1.column_id(), 1);
}

// Build a real Segment file with data rows, then verify that Segment::num_rows()
// matches the written row count and that footer metadata is consistent with schema.
TEST(ExternalColMetaUtilTest, BuildSegmentAndVerifyDataAndFooterMeta) {
    // 1. Build a simple UNIQUE_KEYS tablet schema: one key column, one value column.
    std::vector<TabletColumnPtr> columns;
    columns.emplace_back(create_int_key(/*id=*/0));
    columns.emplace_back(create_int_value(/*id=*/1));
    TabletSchemaSPtr tablet_schema = create_schema(columns, UNIQUE_KEYS);
    // Enable external ColumnMetaPB so that SegmentWriter produces a V3 footer with
    // externalized column meta region + column_meta_entries.
    tablet_schema->set_external_segment_meta_used_default(true);

    // 2. Use existing build_segment helper (SegmentWriter-based) to write a segment file.
    SegmentWriterOptions opts;
    opts.enable_unique_key_merge_on_write = true;
    const size_t nrows = 16;

    auto generator = [](size_t rid, int cid, RowCursorCell& cell) {
        cell.set_not_null();
        // deterministic int payload: value = rid * 10 + cid
        *reinterpret_cast<int*>(cell.mutable_cell_ptr()) = static_cast<int>(rid * 10 + cid);
    };

    std::shared_ptr<Segment> segment;
    std::string segment_path;
    build_segment(opts, tablet_schema,
                  /*segment_id=*/0, tablet_schema, nrows, generator, &segment,
                  std::string(kTestDir), &segment_path);

    ASSERT_NE(segment, nullptr);

    // 3. Verify basic segment data property (row count).
    EXPECT_EQ(segment->num_rows(), nrows);

    // 4. Verify external ColumnMetaPB region is actually written on disk and consistent with schema.
    auto fs = io::global_local_filesystem();
    io::FileReaderSPtr fr;
    io::FileReaderOptions reader_opts;
    Status st = fs->open_file(segment_path, &fr, &reader_opts);
    ASSERT_TRUE(st.ok()) << st.to_json();

    SegmentFooterPB loaded_footer;
    st = read_footer_from_file(fr, &loaded_footer);
    ASSERT_TRUE(st.ok()) << st.to_json();
    EXPECT_EQ(loaded_footer.version(), segment_v2::SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);

    ExternalColMetaUtil::ExternalMetaPointers ptrs;
    ASSERT_TRUE(ExternalColMetaUtil::parse_external_meta_pointers(loaded_footer, &ptrs).ok());
    ASSERT_EQ(ptrs.num_columns, tablet_schema->num_columns());

    // Read back per-column meta from external region and check uid/col_id mapping.
    for (uint32_t cid = 0; cid < ptrs.num_columns; ++cid) {
        ColumnMetaPB meta;
        st = ExternalColMetaUtil::read_col_meta(fr, loaded_footer, ptrs, cid, &meta);
        ASSERT_TRUE(st.ok()) << st.to_json();
        EXPECT_EQ(meta.unique_id(), tablet_schema->column(cid).unique_id());
        EXPECT_EQ(meta.column_id(), cid);
    }
}

// Validate argument checks in read_col_meta that do not trigger any IO.
TEST(ExternalColMetaUtilTest, ReadColMetaArgumentValidation) {
    SegmentFooterPB footer;
    // Prepare one entry.
    auto* e = footer.add_column_meta_entries();
    e->set_unique_id(1);
    e->set_length(16);

    ExternalColMetaUtil::ExternalMetaPointers ptrs;
    ColumnMetaPB meta;
    io::FileReaderSPtr dummy_reader; // not used on these failing paths

    // 1) col_id out of range.
    ptrs.num_columns = 1;
    ptrs.region_start = 100;
    ptrs.region_end = 200;
    Status st = ExternalColMetaUtil::read_col_meta(dummy_reader, footer, ptrs,
                                                   /*col_id=*/1, &meta);
    EXPECT_FALSE(st.ok());

    // 2) footer.column_meta_entries_size() != ptrs.num_columns.
    ptrs.num_columns = 2;
    st = ExternalColMetaUtil::read_col_meta(dummy_reader, footer, ptrs,
                                            /*col_id=*/0, &meta);
    EXPECT_FALSE(st.ok());

    // 3) Invalid meta slice (pos + size > region_end).
    ptrs.num_columns = 1;
    ptrs.region_start = 100;
    ptrs.region_end = 105;
    footer.mutable_column_meta_entries(0)->set_length(32);
    st = ExternalColMetaUtil::read_col_meta(dummy_reader, footer, ptrs,
                                            /*col_id=*/0, &meta);
    EXPECT_FALSE(st.ok());
}

// Validate IO and parse error handling paths in read_col_meta.
TEST(ExternalColMetaUtilTest, ReadColMetaIOAndParseErrors) {
    auto fs = io::global_local_filesystem();

    // 1) Short read: file smaller than declared ColumnMetaPB length.
    std::string short_path = make_test_file_path("read_col_meta_short.bin");
    io::FileWriterPtr fw;
    Status st = fs->create_file(short_path, &fw);
    ASSERT_TRUE(st.ok()) << st.to_json();
    std::string small_payload = "abcd";
    ASSERT_TRUE(fw->append(Slice(small_payload)).ok());
    ASSERT_TRUE(fw->close().ok());

    io::FileReaderSPtr short_fr;
    io::FileReaderOptions ropts;
    st = fs->open_file(short_path, &short_fr, &ropts);
    ASSERT_TRUE(st.ok()) << st.to_json();

    SegmentFooterPB footer;
    auto* e = footer.add_column_meta_entries();
    e->set_unique_id(1);
    e->set_length(static_cast<uint32_t>(small_payload.size() * 2)); // request more than exists

    ExternalColMetaUtil::ExternalMetaPointers ptrs;
    ptrs.region_start = 0;
    ptrs.region_end = ptrs.region_start + e->length();
    ptrs.num_columns = 1;

    ColumnMetaPB meta;
    st = ExternalColMetaUtil::read_col_meta(short_fr, footer, ptrs, /*col_id=*/0, &meta);
    EXPECT_FALSE(st.ok()); // short read should fail

    // 2) Parse error: enough bytes but not a valid ColumnMetaPB.
    std::string parse_path = make_test_file_path("read_col_meta_parse.bin");
    st = fs->create_file(parse_path, &fw);
    ASSERT_TRUE(st.ok()) << st.to_json();
    std::string bogus(32, '\xff');
    ASSERT_TRUE(fw->append(Slice(bogus)).ok());
    ASSERT_TRUE(fw->close().ok());

    io::FileReaderSPtr parse_fr;
    st = fs->open_file(parse_path, &parse_fr, &ropts);
    ASSERT_TRUE(st.ok()) << st.to_json();

    footer.clear_column_meta_entries();
    e = footer.add_column_meta_entries();
    e->set_unique_id(1);
    e->set_length(static_cast<uint32_t>(bogus.size()));

    ptrs.region_start = 0;
    ptrs.region_end = ptrs.region_start + e->length();
    ptrs.num_columns = 1;

    st = ExternalColMetaUtil::read_col_meta(parse_fr, footer, ptrs, /*col_id=*/0, &meta);
    EXPECT_FALSE(st.ok()); // ParseFromArray should fail on bogus data
}

// Ensure write_external_column_meta is a no-op when there are no columns.
TEST(ExternalColMetaUtilTest, WriteExternalColumnMetaEmptyColumns) {
    SegmentFooterPB footer;
    ASSERT_EQ(footer.columns_size(), 0);

    std::string file_path = make_test_file_path("empty_footer_ext_meta.bin");
    auto fs = io::global_local_filesystem();
    io::FileWriterPtr fw;
    Status st = fs->create_file(file_path, &fw);
    ASSERT_TRUE(st.ok()) << st.to_json();

    st = ExternalColMetaUtil::write_external_column_meta(
            fw.get(), &footer, CompressionTypePB::LZ4,
            [fw_raw = fw.get()](const std::vector<Slice>& slices) {
                return fw_raw->appendv(slices.data(), slices.size());
            });
    EXPECT_TRUE(st.ok()) << st.to_json();
    // No column meta entries or region start should be written for empty footer.
    EXPECT_EQ(footer.columns_size(), 0);
    EXPECT_EQ(footer.column_meta_entries_size(), 0);
    EXPECT_EQ(footer.col_meta_region_start(), 0);
    EXPECT_TRUE(fw->close().ok());
}
