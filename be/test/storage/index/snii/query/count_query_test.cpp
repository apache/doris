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

#include "storage/index/snii/query/count_query.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "common/status.h"
#include "roaring/roaring.hh"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

// G02 single-term count-only fast path: the count answered from dict-entry df
// alone must equal the full posting decode on the shared 9000-doc fixture. The
// count_fastpath_hits seam pins that hits are counted exactly when a dict-only
// answer was produced and never on the ordinary decode path.
using namespace doris::snii;
using namespace doris::snii::snii_test;
using doris::snii::query::count_only_term_df;
using doris::snii::query::fabricate_null_disjoint_count_bitmap;
using doris::snii::query::term_query;
namespace qinternal = doris::snii::query::internal;

namespace {

struct Fixture {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
};

void build_fixture(Fixture* f) {
    assert_ok(build_reader(&f->file, &f->segment_reader, &f->index_reader));
}

uint64_t full_decode_term_count(const reader::LogicalIndexReader& idx, const std::string& term) {
    std::vector<uint32_t> docids;
    assert_ok(term_query(idx, term, &docids));
    return docids.size();
}

void reset_query_counters() {
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
}

uint64_t count_hits() {
    return qinternal::query_test_counters().count_fastpath_hits;
}

} // namespace

// Single-term df from the dict equals the full posting decode for every
// posting layout in the fixture (dense/windowed, sparse, tiny, tail) and for
// an absent term (0). The seam counts one hit per dict-only answer and none
// for the decodes.
TEST(SniiCountQuery, TermDfEqualsFullDecode) {
    Fixture plain;
    build_fixture(&plain);

    const std::vector<std::string> terms {"failed",       "driver",    "almost", "sparse_left",
                                          "sparse_right", "needle",    "123",    "trace",
                                          "repeat",       "nosuchterm"};

    reset_query_counters();
    uint64_t expected_hits = 0;
    for (const std::string& term : terms) {
        const uint64_t decode_count = full_decode_term_count(plain.index_reader, term);
        EXPECT_EQ(count_hits(), expected_hits) << "decode path must not touch the seam";

        uint64_t df_count = 0;
        assert_ok(count_only_term_df(plain.index_reader, term, &df_count));
        EXPECT_EQ(df_count, decode_count) << "term: " << term;
        ++expected_hits;
        EXPECT_EQ(count_hits(), expected_hits) << "term: " << term;
    }
    // Spot-pin the fixture dfs so a fixture drift cannot silently weaken the test.
    uint64_t df = 0;
    assert_ok(count_only_term_df(plain.index_reader, "failed", &df));
    EXPECT_EQ(df, 9000U);
    assert_ok(count_only_term_df(plain.index_reader, "nosuchterm", &df));
    EXPECT_EQ(df, 0U);
}

// Single-term df remains valid on a positionless index.
TEST(SniiCountQuery, DocsOnlyIndexTermStillCounts) {
    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 3;
    input.index_suffix = "DocsOnly";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = 100;
    input.terms = {make_term("alpha", docs_with_one_position(0, 60, 0)),
                   make_term("bravo", docs_with_one_position(10, 25, 1))};
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });
    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());

    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    reader::LogicalIndexReader idx;
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &idx));
    ASSERT_FALSE(idx.has_positions());

    reset_query_counters();
    uint64_t df = 0;
    assert_ok(count_only_term_df(idx, "alpha", &df));
    EXPECT_EQ(df, full_decode_term_count(idx, "alpha"));
    EXPECT_EQ(df, 60U);
    assert_ok(count_only_term_df(idx, "bravo", &df));
    EXPECT_EQ(df, 15U);
    EXPECT_EQ(count_hits(), 2U);
}

// --- fabricate_null_disjoint_count_bitmap truth table -----------------------
// The fabricated bitmap must (a) hold exactly `count` ids, (b) be DISJOINT
// from the null bitmap so the caller-side FunctionMatchBase ->
// InvertedIndexResultBitmap::mask_out_null subtraction is a provable no-op,
// and (c) stay inside [0, count + |nulls|), which never exceeds the segment's
// [0, num_rows) row space because df counts only non-null docs.

TEST(SniiCountQuery, FabricateNoNullsIsDenseRange) {
    roaring::Roaring nulls;
    roaring::Roaring out;
    assert_ok(fabricate_null_disjoint_count_bitmap(100, nulls, &out));
    roaring::Roaring expected;
    expected.addRange(0, 100);
    EXPECT_EQ(out, expected);
}

TEST(SniiCountQuery, FabricateNullPrefixShiftsRange) {
    roaring::Roaring nulls;
    nulls.addRange(0, 100);
    roaring::Roaring out;
    assert_ok(fabricate_null_disjoint_count_bitmap(50, nulls, &out));
    roaring::Roaring expected;
    expected.addRange(100, 150);
    EXPECT_EQ(out, expected);
    EXPECT_TRUE((out & nulls).isEmpty());
}

TEST(SniiCountQuery, FabricateInterleavedNullsStaysDisjointAndExact) {
    roaring::Roaring nulls;
    for (uint32_t id = 0; id < 200; id += 2) {
        nulls.add(id); // 100 even nulls
    }
    roaring::Roaring out;
    assert_ok(fabricate_null_disjoint_count_bitmap(70, nulls, &out));
    EXPECT_EQ(out.cardinality(), 70U);
    EXPECT_TRUE((out & nulls).isEmpty());
    // Exactly the first 70 odd (non-null) ids.
    roaring::Roaring expected;
    for (uint32_t id = 1; id < 140; id += 2) {
        expected.add(id);
    }
    EXPECT_EQ(out, expected);
    // The safety property itself: the null-bitmap subtraction the MATCH
    // machinery applies to every index result must not change the count.
    roaring::Roaring masked = out;
    masked -= nulls;
    EXPECT_EQ(masked.cardinality(), 70U);
}

TEST(SniiCountQuery, FabricateNullsBeyondWindowIrrelevant) {
    roaring::Roaring nulls;
    nulls.add(1000000);
    roaring::Roaring out;
    assert_ok(fabricate_null_disjoint_count_bitmap(10, nulls, &out));
    roaring::Roaring expected;
    expected.addRange(0, 10);
    EXPECT_EQ(out, expected);
}

TEST(SniiCountQuery, FabricateConsumesEntireNonNullWindow) {
    // nulls {0..4}, count 5: every non-null id of the window [0, 10) is used.
    roaring::Roaring nulls;
    nulls.addRange(0, 5);
    roaring::Roaring out;
    assert_ok(fabricate_null_disjoint_count_bitmap(5, nulls, &out));
    roaring::Roaring expected;
    expected.addRange(5, 10);
    EXPECT_EQ(out, expected);
}

TEST(SniiCountQuery, FabricateZeroCountIsEmpty) {
    roaring::Roaring nulls;
    nulls.addRange(0, 100);
    roaring::Roaring out;
    out.add(7); // stale content must be overwritten
    assert_ok(fabricate_null_disjoint_count_bitmap(0, nulls, &out));
    EXPECT_TRUE(out.isEmpty());
}

TEST(SniiCountQuery, FabricateRejectsDocidDomainOverflow) {
    // df + null count beyond the uint32 docid domain can only come from a
    // corrupt index: it must surface as an error (the reader glue falls
    // through to the row-accurate decode), never as a silently wrong bitmap.
    roaring::Roaring nulls;
    nulls.add(0);
    roaring::Roaring out;
    const uint64_t count = uint64_t(std::numeric_limits<uint32_t>::max()) + 1; // 2^32
    doris::Status st = fabricate_null_disjoint_count_bitmap(count, nulls, &out);
    EXPECT_FALSE(st.ok());
}

// A segment WITH a null bitmap end to end through the real writer/reader:
// df from the dict is unchanged by nulls (the writer adds NO postings for a
// null doc, so postings -- and df -- can never include null rows), and the
// fabricated bitmap built against the segment's REAL null bitmap keeps
// cardinality df through the caller-side null subtraction. This pins the
// contract that replaced the old "veto whenever a null section exists" guard.
TEST(SniiCountQuery, NullBitmapSegmentDfCountsAndFabricationSurvivesMasking) {
    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 4;
    input.index_suffix = "Nullable";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 100;
    // Writer invariant mirrored by the fixture: null docids carry no postings
    // (scalar add_nulls adds no tokens; a NULL array row is an empty range).
    input.null_docids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 50, 99};
    input.terms = {make_term("alpha", docs_with_one_position(10, 50, 0))};
    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());

    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    reader::LogicalIndexReader idx;
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &idx));
    ASSERT_GT(idx.section_refs().null_bitmap.length, 0U);

    // df is the exact match count despite the null rows.
    uint64_t df = 0;
    assert_ok(count_only_term_df(idx, "alpha", &df));
    EXPECT_EQ(df, full_decode_term_count(idx, "alpha"));
    EXPECT_EQ(df, 40U);

    // Decode the segment's REAL null bitmap the same way the reader glue does.
    const auto& ref = idx.section_refs().null_bitmap;
    std::vector<uint8_t> bytes;
    assert_ok(idx.reader()->read_at(ref.offset, ref.length, &bytes));
    format::NullBitmapReader null_reader;
    assert_ok(format::NullBitmapReader::open(Slice(bytes), &null_reader));
    roaring::Roaring nulls;
    null_reader.copy_to(&nulls);
    ASSERT_EQ(nulls.cardinality(), input.null_docids.size());

    roaring::Roaring fabricated;
    assert_ok(fabricate_null_disjoint_count_bitmap(df, nulls, &fabricated));
    EXPECT_EQ(fabricated.cardinality(), df);
    EXPECT_TRUE((fabricated & nulls).isEmpty());
    roaring::Roaring masked = fabricated;
    masked -= nulls; // FunctionMatchBase::mask_out_null equivalent
    EXPECT_EQ(masked.cardinality(), df);
    // With the 10 leading nulls the first 40 non-null ids are exactly [10, 50):
    // pinned to catch select / removeRange off-by-ones against a real layout.
    roaring::Roaring expected;
    expected.addRange(10, 50);
    EXPECT_EQ(fabricated, expected);
}
