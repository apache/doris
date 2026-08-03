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

// The Doris WRITE-path adapter for the SNII-native BKD (design 10 / task P3-2a):
// a numeric column driven through the ordinary IndexColumnWriter interface must
// land in the segment's SNII container as a blob logical index that the native
// BkdReader can answer from.
//
// What is only covered HERE (bkd_container_roundtrip_test already covers the
// core riding a hand-driven container):
//   * the adapter translates Doris's (const void* values, size_t count) +
//     add_nulls row stream into (doc_id, sortable bytes) points, keeping the
//     row id in step across value runs and null runs;
//   * it encodes through the INDEX's own field type -- the one invariant that
//     silently produces a self-consistent but semantically wrong index if it
//     is taken from anywhere else (INV-1);
//   * the null rows survive as a real SNII null-bitmap POD, not as points;
//   * IndexFileWriter seals all of it into one container.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_bkd_index_reader.h"
#include "storage/index/snii/snii_bkd_index_writer.h"
#include "storage/key_coder.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

using ::doris::snii::bkd::BkdReader;
using ::doris::snii::bkd::BkdSections;
using ::doris::snii::format::LogicalIndexMetadataRef;
using ::doris::snii::format::NamedBlobFileRef;

constexpr int64_t kIndexId = 11;
constexpr FieldType kFieldType = FieldType::OLAP_FIELD_TYPE_BIGINT;
constexpr const char* kTestDir = "./ut_dir/snii_bkd_adapter_test";

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

void init_bkd_index_meta(TabletIndex* meta) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("bkd_idx");
    pb.add_col_unique_id(0);
    meta->init_from_pb(pb);
}

// The one encoder both sides must agree on, resolved from the index's OWN field
// type (INV-1).
std::string encode(int64_t value) {
    std::string out;
    get_key_coder(kFieldType)->full_encode_ascending(&value, &out);
    return out;
}

::doris::snii::Slice slice_of(const std::string& bytes) {
    return ::doris::snii::Slice(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}

// One row of the source column: either a value or a NULL.
struct Row {
    bool is_null = false;
    int64_t value = 0;
};

// A deterministic column with NULL runs interleaved between value runs, so the
// adapter's row-id bookkeeping is exercised in both directions.
std::vector<Row> sample_rows(uint32_t count, int64_t span) {
    std::vector<Row> rows;
    uint64_t state = 0x9E3779B97F4A7C15ULL;
    for (uint32_t i = 0; i < count; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        // Every 7th run of 3 rows is NULL.
        if ((i / 3) % 7 == 6) {
            rows.push_back(Row {true, 0});
            continue;
        }
        rows.push_back(
                Row {false, static_cast<int64_t>(state % static_cast<uint64_t>(2 * span)) - span});
    }
    return rows;
}

roaring::Roaring brute_force(const std::vector<Row>& rows, int64_t low, int64_t high) {
    roaring::Roaring hits;
    for (uint32_t rid = 0; rid < rows.size(); ++rid) {
        if (!rows[rid].is_null && rows[rid].value >= low && rows[rid].value <= high) {
            hits.add(rid);
        }
    }
    return hits;
}

roaring::Roaring null_rows(const std::vector<Row>& rows) {
    roaring::Roaring nulls;
    for (uint32_t rid = 0; rid < rows.size(); ++rid) {
        if (rows[rid].is_null) {
            nulls.add(rid);
        }
    }
    return nulls;
}

// Drives the adapter exactly the way the segment writer does: consecutive
// non-null rows arrive as ONE add_values call over a contiguous CppType array,
// and each NULL run as one add_nulls.
void write_segment(const std::string& prefix, const TabletIndex& meta,
                   const std::vector<Row>& rows) {
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/301);

    SniiBkdIndexColumnWriter writer(&index_file_writer, &meta, kFieldType);
    assert_ok(writer.init());

    size_t i = 0;
    while (i < rows.size()) {
        size_t j = i;
        while (j < rows.size() && rows[j].is_null == rows[i].is_null) {
            ++j;
        }
        if (rows[i].is_null) {
            assert_ok(writer.add_nulls(static_cast<uint32_t>(j - i)));
        } else {
            std::vector<int64_t> run;
            for (size_t k = i; k < j; ++k) {
                run.push_back(rows[k].value);
            }
            assert_ok(writer.add_values("c1", run.data(), run.size()));
        }
        i = j;
    }
    assert_ok(writer.finish());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());
}

// Resolves the sealed container's blob entry into the reader's two extents plus
// the raw null-bitmap sub-file bytes. Driven off the CONTAINER's own directory,
// never off anything the producer remembered.
struct OpenedIndex {
    std::unique_ptr<::doris::snii::io::LocalFileReader> file;
    std::unique_ptr<::doris::snii::reader::SniiSegmentReader> segment;
    std::unique_ptr<BkdReader> reader;
    std::vector<uint8_t> null_bitmap_bytes;
};

void open_index(const std::string& prefix, OpenedIndex* out) {
    out->file = std::make_unique<::doris::snii::io::LocalFileReader>();
    assert_ok(out->file->open(InvertedIndexDescriptor::get_index_file_path_v2(prefix)));
    out->segment = std::make_unique<::doris::snii::reader::SniiSegmentReader>();
    assert_ok(::doris::snii::reader::SniiSegmentReader::open(out->file.get(), out->segment.get()));

    const LogicalIndexMetadataRef* entry = nullptr;
    assert_ok(out->segment->blob_entry(static_cast<uint64_t>(kIndexId), "", &entry));
    ASSERT_NE(entry, nullptr);

    BkdSections sections;
    const NamedBlobFileRef* nulls = nullptr;
    for (const NamedBlobFileRef& blob : entry->files) {
        if (blob.name == "bkd_data") {
            sections.data_offset = blob.offset;
            sections.data_length = blob.length;
        } else if (blob.name == "bkd_index") {
            sections.index_offset = blob.offset;
            sections.index_length = blob.length;
        } else if (blob.name == "bkd_nulls") {
            nulls = &blob;
        }
    }
    assert_ok(BkdReader::open(out->file.get(), sections, &out->reader));
    if (nulls != nullptr && nulls->length > 0) {
        assert_ok(out->file->read_at(nulls->offset, nulls->length, &out->null_bitmap_bytes));
    }
}

roaring::Roaring decode_null_bitmap(const std::vector<uint8_t>& bytes) {
    roaring::Roaring nulls;
    if (bytes.empty()) {
        return nulls;
    }
    ::doris::snii::format::NullBitmapReader reader;
    EXPECT_TRUE(::doris::snii::format::NullBitmapReader::open(::doris::snii::Slice(bytes), &reader)
                        .ok());
    reader.copy_to(&nulls);
    return nulls;
}

class SniiBkdAdapterTest : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        init_bkd_index_meta(&_meta);
        // A REAL query cache, installed the way the other SNII reader tests do
        // it: the reader consults it on every query, so leaving the process-wide
        // one unset would exercise a path production never takes.
        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());
    }
    void TearDown() override {
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);
        _query_cache.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
    // A path PREFIX, not a file name: IndexFileWriter and IndexFileReader both
    // append the v2 suffix themselves, and the reader would look in the wrong
    // place if the two disagreed.
    std::string test_path(const std::string& name) const {
        return std::string(kTestDir) + "/" + name;
    }
    TabletIndex _meta;
};

// The whole contract in one pass: what the segment writer pushed in comes back
// out of the sealed container, answered by the native reader.
TEST_F(SniiBkdAdapterTest, NumericColumnLandsInTheContainerAsAQueryableBkd) {
    const std::vector<Row> rows = sample_rows(3000, 500);
    const std::string path = test_path("numeric");
    write_segment(path, _meta, rows);

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));

    // doc_count counts rows that own a point, so the NULL rows must NOT be in it.
    EXPECT_EQ(opened.reader->point_count(), rows.size() - null_rows(rows).cardinality());
    EXPECT_EQ(opened.reader->doc_count(), rows.size() - null_rows(rows).cardinality());

    for (const auto& [low, high] : std::vector<std::pair<int64_t, int64_t>> {
                 {-500, 499}, {-100, 100}, {0, 0}, {200, 201}, {-1000, -600}, {499, 499}}) {
        SCOPED_TRACE("range [" + std::to_string(low) + ", " + std::to_string(high) + "]");
        const std::string lower = encode(low);
        const std::string upper = encode(high);
        roaring::Roaring hits;
        assert_ok(opened.reader->range(slice_of(lower), true, slice_of(upper), true, &hits));
        EXPECT_TRUE(hits == brute_force(rows, low, high));
    }
}

// NULL rows are carried by the SNII null-bitmap POD, not by the point set: a
// NULL that leaked in as a point would answer `col > x` for a row that has no
// value at all.
TEST_F(SniiBkdAdapterTest, NullRowsBecomeANullBitmapNotPoints) {
    const std::vector<Row> rows = sample_rows(2000, 50);
    const std::string path = test_path("nulls");
    write_segment(path, _meta, rows);

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));

    const roaring::Roaring expected = null_rows(rows);
    ASSERT_GT(expected.cardinality(), 0U) << "the fixture must actually contain NULLs";
    EXPECT_TRUE(decode_null_bitmap(opened.null_bitmap_bytes) == expected);

    // The unbounded range is every row that owns a point -- exactly the
    // complement of the null set over [0, row_count).
    roaring::Roaring all;
    assert_ok(
            opened.reader->range(::doris::snii::Slice(), true, ::doris::snii::Slice(), true, &all));
    roaring::Roaring complement;
    complement.addRange(0, rows.size());
    complement -= expected;
    EXPECT_TRUE(all == complement);
}

// An all-NULL column still produces a well-formed, openable index: the empty
// BKD (design 5.3) plus a full null bitmap. Treating it as an error here would
// make a legal column unindexable.
TEST_F(SniiBkdAdapterTest, AllNullColumnSealsAnEmptyIndex) {
    std::vector<Row> rows;
    for (uint32_t i = 0; i < 500; ++i) {
        rows.push_back(Row {true, 0});
    }
    const std::string path = test_path("all_null");
    write_segment(path, _meta, rows);

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));
    EXPECT_TRUE(opened.reader->empty());
    EXPECT_EQ(opened.reader->point_count(), 0U);
    EXPECT_TRUE(decode_null_bitmap(opened.null_bitmap_bytes) == null_rows(rows));

    roaring::Roaring hits;
    const std::string lower = encode(-1);
    const std::string upper = encode(1);
    assert_ok(opened.reader->range(slice_of(lower), true, slice_of(upper), true, &hits));
    EXPECT_TRUE(hits.isEmpty());
}

// A column with no NULL at all must not emit a null-bitmap sub-file whose
// decode says otherwise; the reader-side has_null path keys off this.
TEST_F(SniiBkdAdapterTest, ColumnWithoutNullsCarriesAnEmptyNullSet) {
    std::vector<Row> rows;
    for (uint32_t i = 0; i < 800; ++i) {
        rows.push_back(Row {false, static_cast<int64_t>(i) - 400});
    }
    const std::string path = test_path("no_null");
    write_segment(path, _meta, rows);

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));
    EXPECT_EQ(opened.reader->point_count(), rows.size());
    EXPECT_TRUE(decode_null_bitmap(opened.null_bitmap_bytes).isEmpty());
}

// The adapter must encode through the INDEX's field type. Driving an INT index
// keeps 4-byte points; if the adapter fell back to a wider stride or another
// coder the values would still round-trip self-consistently but compare in the
// wrong order, which only a semantic oracle catches.
TEST_F(SniiBkdAdapterTest, EncodesThroughTheIndexFieldTypeNotTheWidestOne) {
    io::FileWriterPtr file_writer;
    const std::string path = test_path("int32");
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(path), &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), path, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/301);

    SniiBkdIndexColumnWriter writer(&index_file_writer, &_meta, FieldType::OLAP_FIELD_TYPE_INT);
    assert_ok(writer.init());
    // Straddles zero so the sign-bit flip is the thing under test.
    const std::vector<int32_t> values = {-2147483648, -1, 0, 1, 2147483647, -5, 5};
    assert_ok(writer.add_values("c1", values.data(), values.size()));
    assert_ok(writer.finish());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));
    EXPECT_EQ(opened.reader->header().bytes_per_dim, sizeof(int32_t));
    EXPECT_EQ(opened.reader->header().field_type, FieldType::OLAP_FIELD_TYPE_INT);

    const auto encode_int = [](int32_t v) {
        std::string out;
        get_key_coder(FieldType::OLAP_FIELD_TYPE_INT)->full_encode_ascending(&v, &out);
        return out;
    };
    // Everything strictly below zero: rows 0, 1, 5.
    const std::string upper = encode_int(-1);
    roaring::Roaring hits;
    assert_ok(opened.reader->range(::doris::snii::Slice(), true, slice_of(upper), true, &hits));
    roaring::Roaring expected;
    expected.add(0);
    expected.add(1);
    expected.add(5);
    EXPECT_TRUE(hits == expected);
}

// ---------------------------------------------------------------------------
// READ path (task P3-2b): the same sealed container answered through the Doris
// InvertedIndexReader interface, i.e. what a predicate actually calls.
// ---------------------------------------------------------------------------

// A query context with the searcher cache DISABLED, so each test opens its own
// reader and cannot pass or fail because of another test's cache entry.
class QueryContextFixture {
public:
    QueryContextFixture() {
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        _runtime_state.set_query_options(query_options);
        _context->io_ctx = &_io_ctx;
        _context->stats = &_stats;
        _context->runtime_state = &_runtime_state;
    }
    const IndexQueryContextPtr& context() { return _context; }

private:
    OlapReaderStatistics _stats;
    io::IOContext _io_ctx;
    RuntimeState _runtime_state;
    IndexQueryContextPtr _context = std::make_shared<IndexQueryContext>();
};

std::shared_ptr<IndexFileReader> open_file_reader(const std::string& path) {
    auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), path,
                                                    InvertedIndexStorageFormatPB::SNII);
    EXPECT_TRUE(reader->init().ok());
    return reader;
}

Field int64_field(int64_t value) {
    return Field::create_field<TYPE_BIGINT>(value);
}

// The predicate path: an ordinary comparison over a numeric column reaches the
// SNII BKD through query(), and must answer exactly what a scan would.
TEST_F(SniiBkdAdapterTest, ReaderAnswersComparisonPredicates) {
    const std::vector<Row> rows = sample_rows(3000, 500);
    const std::string path = test_path("read_cmp");
    write_segment(path, _meta, rows);

    QueryContextFixture fixture;
    auto reader = SniiBkdIndexReader::create_shared(&_meta, open_file_reader(path));

    struct Case {
        InvertedIndexQueryType type;
        int64_t value;
    };
    const std::vector<Case> cases = {
            {InvertedIndexQueryType::EQUAL_QUERY, 17},
            {InvertedIndexQueryType::EQUAL_QUERY, -400},
            {InvertedIndexQueryType::LESS_THAN_QUERY, 0},
            {InvertedIndexQueryType::LESS_EQUAL_QUERY, 0},
            {InvertedIndexQueryType::GREATER_THAN_QUERY, 250},
            {InvertedIndexQueryType::GREATER_EQUAL_QUERY, 250},
            {InvertedIndexQueryType::LESS_THAN_QUERY, -600},
            {InvertedIndexQueryType::GREATER_EQUAL_QUERY, 600},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE("type " + std::to_string(static_cast<int>(c.type)) + " value " +
                     std::to_string(c.value));
        auto bitmap = std::make_shared<roaring::Roaring>();
        assert_ok(reader->query(fixture.context(), "c1", int64_field(c.value), c.type, bitmap));

        roaring::Roaring expected;
        for (uint32_t rid = 0; rid < rows.size(); ++rid) {
            if (rows[rid].is_null) {
                continue;
            }
            const int64_t v = rows[rid].value;
            const bool hit = (c.type == InvertedIndexQueryType::EQUAL_QUERY)        ? v == c.value
                             : (c.type == InvertedIndexQueryType::LESS_THAN_QUERY)  ? v < c.value
                             : (c.type == InvertedIndexQueryType::LESS_EQUAL_QUERY) ? v <= c.value
                             : (c.type == InvertedIndexQueryType::GREATER_THAN_QUERY)
                                     ? v > c.value
                                     : v >= c.value;
            if (hit) {
                expected.add(rid);
            }
        }
        EXPECT_TRUE(*bitmap == expected);
    }
}

// A query shape the BKD cannot answer must be REFUSED, not approximated: the
// caller keeps the predicate and evaluates it normally.
TEST_F(SniiBkdAdapterTest, ReaderRefusesUnsupportedQueryTypes) {
    const std::vector<Row> rows = sample_rows(200, 20);
    const std::string path = test_path("read_refuse");
    write_segment(path, _meta, rows);

    QueryContextFixture fixture;
    auto reader = SniiBkdIndexReader::create_shared(&_meta, open_file_reader(path));
    auto bitmap = std::make_shared<roaring::Roaring>();
    const Status status = reader->query(fixture.context(), "c1", int64_field(1),
                                        InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status.to_string();
}

// read_null_bitmap must surface the SNII null POD this segment actually carries;
// the NULL-aware predicate paths subtract it from their result.
TEST_F(SniiBkdAdapterTest, ReaderReadsTheNullBitmap) {
    const std::vector<Row> rows = sample_rows(1500, 40);
    const std::string path = test_path("read_nulls");
    write_segment(path, _meta, rows);

    QueryContextFixture fixture;
    auto reader = SniiBkdIndexReader::create_shared(&_meta, open_file_reader(path));
    InvertedIndexQueryCacheHandle handle;
    assert_ok(reader->read_null_bitmap(fixture.context(), &handle));
    ASSERT_NE(handle.get_bitmap(), nullptr);
    // Without this the test would also pass against a reader that always
    // returned an EMPTY bitmap, which is exactly the failure it exists to catch.
    ASSERT_GT(null_rows(rows).cardinality(), 0U) << "the fixture must contain NULLs";
    EXPECT_TRUE(*handle.get_bitmap() == null_rows(rows));
}

// try_query answers from the leaf directory alone. It is an ESTIMATE, so the
// contract asserted here is the documented error bound, not equality.
TEST_F(SniiBkdAdapterTest, ReaderEstimatesCardinalityWithinTheDocumentedBound) {
    const std::vector<Row> rows = sample_rows(4000, 300);
    const std::string path = test_path("read_estimate");
    write_segment(path, _meta, rows);

    QueryContextFixture fixture;
    auto reader = SniiBkdIndexReader::create_shared(&_meta, open_file_reader(path));

    for (const int64_t bound : {-300, -50, 0, 100, 299}) {
        SCOPED_TRACE("less than " + std::to_string(bound));
        size_t estimate = 0;
        assert_ok(reader->try_query(fixture.context(), "c1", int64_field(bound),
                                    InvertedIndexQueryType::LESS_THAN_QUERY, &estimate));
        uint64_t truth = 0;
        for (const Row& row : rows) {
            if (!row.is_null && row.value < bound) {
                ++truth;
            }
        }
        // A bound whose truth is 0 would be satisfied by any estimate below the
        // slack, so the interesting bounds must actually select something.
        if (bound > -300) {
            ASSERT_GT(truth, 0U) << "bound selects nothing; the assertion below is vacuous";
        }
        // Only the two boundary leaves are guessed at, each at half its count.
        const uint64_t slack = 2 * ((::doris::snii::bkd::kDefaultPointsPerLeaf + 1) / 2);
        EXPECT_LE(estimate, truth + slack);
        EXPECT_GE(estimate + slack, truth);
    }
}

// The reader must report itself as a BKD reader: the predicate layer routes on
// exactly this (comparison_predicate.h / in_list_predicate.h check for a BKD
// reader before pushing a numeric comparison down at all).
TEST_F(SniiBkdAdapterTest, ReaderIdentifiesAsBkd) {
    const std::string path = test_path("read_type");
    write_segment(path, _meta, sample_rows(100, 10));
    auto reader = SniiBkdIndexReader::create_shared(&_meta, open_file_reader(path));
    EXPECT_EQ(reader->type(), InvertedIndexReaderType::BKD);
}

// ---------------------------------------------------------------------------
// The GATES (task P3-2c): before this, both factories refused every non-string
// column on a SNII segment outright, so nothing above could ever be reached in
// production. These pin that the routing now happens -- and that a type NEITHER
// writer can represent is still refused rather than silently dropped.
// ---------------------------------------------------------------------------

TabletColumn numeric_column(FieldType type) {
    TabletColumn column;
    ColumnPB pb;
    pb.set_unique_id(0);
    pb.set_name("c1");
    pb.set_type(TabletColumn::get_string_by_field_type(type));
    pb.set_is_key(false);
    pb.set_is_nullable(true);
    column.init_from_pb(pb);
    return column;
}

TEST_F(SniiBkdAdapterTest, WriterFactoryRoutesNumericColumnsToTheNativeBkd) {
    io::FileWriterPtr file_writer;
    const std::string prefix = test_path("factory");
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/301);

    const TabletColumn column = numeric_column(kFieldType);
    std::unique_ptr<IndexColumnWriter> writer;
    assert_ok(IndexColumnWriter::create(&column, &writer, &index_file_writer, &_meta));
    ASSERT_NE(writer, nullptr);
    EXPECT_NE(dynamic_cast<SniiBkdIndexColumnWriter*>(writer.get()), nullptr)
            << "a numeric SNII column must be routed to the native BKD writer";
}

// A type that is neither text nor numeric (JSONB) has no representation in
// either SNII writer. Opening the gate must not turn that into a silently
// unindexed column.
TEST_F(SniiBkdAdapterTest, WriterFactoryStillRefusesNonIndexableTypes) {
    io::FileWriterPtr file_writer;
    const std::string prefix = test_path("factory_refuse");
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/301);

    const TabletColumn column = numeric_column(FieldType::OLAP_FIELD_TYPE_JSONB);
    std::unique_ptr<IndexColumnWriter> writer;
    const Status status = IndexColumnWriter::create(&column, &writer, &index_file_writer, &_meta);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status.to_string();
}

// ---------------------------------------------------------------------------
// ARRAY<numeric> (task P3-4). One row contributes SEVERAL points, which is why
// the builder keys on (value, doc_id) rather than assuming one value per doc.
//
// The reference for these semantics is the CLucene numeric array branch
// (inverted_index_writer.cpp add_array_values): it walks elements with a
// running counter, skips nested nulls, advances the row id once per row, and
// records NO null for a row whose array is empty. Array-level NULLs come from
// add_array_nulls and nowhere else.
// ---------------------------------------------------------------------------

// Drives the adapter's array entry points directly, the way the segment writer
// does: one add_array_values over the whole block, then add_array_nulls over
// the SAME rows.
struct ArrayBlock {
    std::vector<int64_t> elements;
    std::vector<uint8_t> element_nulls; // nested null map, per element
    std::vector<uint64_t> offsets;      // count+1 entries
    std::vector<uint8_t> row_nulls;     // array-level null map, per row
};

// `base` reproduces the olap_convertor behaviour the CLucene writer warns
// about: offsets accumulate from a base that is NOT necessarily zero, while the
// element and null arrays always start at zero. Indexing elements by offsets[i]
// instead of by a running count silently reads the wrong elements.
ArrayBlock make_array_block(const std::vector<std::vector<int64_t>>& rows, uint64_t base) {
    ArrayBlock block;
    block.offsets.push_back(base);
    for (const std::vector<int64_t>& row : rows) {
        for (const int64_t value : row) {
            block.elements.push_back(value);
            block.element_nulls.push_back(0);
        }
        block.offsets.push_back(block.offsets.back() + row.size());
        block.row_nulls.push_back(0);
    }
    return block;
}

void write_array_segment(const std::string& prefix, const TabletIndex& meta,
                         const ArrayBlock& block) {
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/303);
    SniiBkdIndexColumnWriter writer(&index_file_writer, &meta, kFieldType);
    assert_ok(writer.init());
    const size_t rows = block.offsets.size() - 1;
    assert_ok(writer.add_array_values(
            sizeof(int64_t), block.elements.data(),
            block.element_nulls.empty() ? nullptr : block.element_nulls.data(),
            reinterpret_cast<const uint8_t*>(block.offsets.data()), rows));
    assert_ok(writer.add_array_nulls(block.row_nulls.data(), rows));
    assert_ok(writer.finish());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());
}

// Several values on one row all resolve to that row, and the row is counted
// once. A doc_count that counted points would over-report every array column.
TEST_F(SniiBkdAdapterTest, ArrayRowContributesSeveralPointsUnderOneRowId) {
    const std::vector<std::vector<int64_t>> rows = {{10, 20, 30}, {20}, {40, 50}};
    const std::string path = test_path("array_multi");
    write_array_segment(path, _meta, make_array_block(rows, /*base=*/0));

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));
    EXPECT_EQ(opened.reader->point_count(), 6U);
    EXPECT_EQ(opened.reader->doc_count(), 3U);

    // 20 appears on rows 0 and 1; both must come back.
    const std::string twenty = encode(20);
    roaring::Roaring hits;
    assert_ok(opened.reader->range(slice_of(twenty), true, slice_of(twenty), true, &hits));
    roaring::Roaring expected;
    expected.add(0);
    expected.add(1);
    EXPECT_TRUE(hits == expected);
}

// THE TRAP the CLucene writer documents: offsets accumulate from a base that is
// not zero while the element array starts at zero. Indexing elements by
// offsets[i] reads past the end; only a running count is correct.
TEST_F(SniiBkdAdapterTest, ArrayOffsetsMayStartFromANonZeroBase) {
    const std::vector<std::vector<int64_t>> rows = {{10, 20}, {30}, {40, 50, 60}};
    const std::string path = test_path("array_base");
    write_array_segment(path, _meta, make_array_block(rows, /*base=*/100000));

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));
    EXPECT_EQ(opened.reader->point_count(), 6U);
    EXPECT_EQ(opened.reader->doc_count(), 3U);

    for (const auto& [value, row] : std::vector<std::pair<int64_t, uint32_t>> {
                 {10, 0}, {20, 0}, {30, 1}, {40, 2}, {50, 2}, {60, 2}}) {
        SCOPED_TRACE("value " + std::to_string(value));
        const std::string encoded = encode(value);
        roaring::Roaring hits;
        assert_ok(opened.reader->range(slice_of(encoded), true, slice_of(encoded), true, &hits));
        roaring::Roaring expected;
        expected.add(row);
        EXPECT_TRUE(hits == expected);
    }
}

// An EMPTY array is not NULL. It owns no point -- there is nothing to compare --
// but marking it NULL would make `col IS NULL` true for a row that holds [].
TEST_F(SniiBkdAdapterTest, EmptyArrayOwnsNoPointAndIsNotNull) {
    const std::vector<std::vector<int64_t>> rows = {{10}, {}, {20, 30}};
    const std::string path = test_path("array_empty");
    write_array_segment(path, _meta, make_array_block(rows, /*base=*/0));

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));
    EXPECT_EQ(opened.reader->point_count(), 3U);
    // Rows owning at least one point: rows 0 and 2.
    EXPECT_EQ(opened.reader->doc_count(), 2U);
    EXPECT_TRUE(decode_null_bitmap(opened.null_bitmap_bytes).isEmpty())
            << "an empty array was recorded as NULL; [] is not NULL";
}

// A nested null is skipped as an element, and does not make its ROW null
// either -- [NULL, 7] is a non-null array that happens to contain a null.
TEST_F(SniiBkdAdapterTest, NestedNullsAreSkippedWithoutNullingTheRow) {
    // Row 2 is [NULL, NULL] -- every element null. It produces no point and is
    // STILL not a null array; a version that only tracked "did this row produce
    // a value" gets rows 0 and 1 right and this one wrong.
    ArrayBlock block = make_array_block({{5, 6}, {7, 8}, {9, 11}}, /*base=*/0);
    block.element_nulls = {0, 1, 1, 0, 1, 1}; // [5, NULL], [NULL, 8], [NULL, NULL]
    const std::string path = test_path("array_nested_null");
    write_array_segment(path, _meta, block);

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));
    EXPECT_EQ(opened.reader->point_count(), 2U);
    EXPECT_EQ(opened.reader->doc_count(), 2U);
    EXPECT_TRUE(decode_null_bitmap(opened.null_bitmap_bytes).isEmpty())
            << "a nested NULL nulled its whole row";

    // 6 and 7 were the nulled elements and must be absent.
    for (const int64_t absent : {6, 7, 9, 11}) {
        const std::string encoded = encode(absent);
        roaring::Roaring hits;
        assert_ok(opened.reader->range(slice_of(encoded), true, slice_of(encoded), true, &hits));
        EXPECT_TRUE(hits.isEmpty()) << "value " << absent << " was indexed despite being NULL";
    }
}

// add_array_nulls covers the SAME rows add_array_values just walked, so it must
// record them without advancing the row id again.
TEST_F(SniiBkdAdapterTest, ArrayLevelNullsAreRecordedWithoutAdvancingTheRowId) {
    ArrayBlock block = make_array_block({{1}, {2}, {3}}, /*base=*/0);
    block.row_nulls = {0, 1, 0}; // row 1's ARRAY is NULL
    const std::string path = test_path("array_row_null");
    write_array_segment(path, _meta, block);

    OpenedIndex opened;
    ASSERT_NO_FATAL_FAILURE(open_index(path, &opened));
    roaring::Roaring expected;
    expected.add(1);
    EXPECT_TRUE(decode_null_bitmap(opened.null_bitmap_bytes) == expected);

    // Row 2 still holds 3: a null map that advanced the row id would have
    // shifted every later row by one.
    const std::string three = encode(3);
    roaring::Roaring hits;
    assert_ok(opened.reader->range(slice_of(three), true, slice_of(three), true, &hits));
    roaring::Roaring row2;
    row2.add(2);
    EXPECT_TRUE(hits == row2);
}

} // namespace
} // namespace doris::segment_v2
