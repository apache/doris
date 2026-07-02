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
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

// G02 count-only fast path: the count answered from dict-entry df alone must
// EQUAL the full posting decode on the shared 9000-doc fixture, and every
// unsafe shape must fall through (*handled = false) so the generic paths keep
// owning the semantics. The count_fastpath_hits seam (BE_TEST) pins that hits
// are counted exactly when a dict-only answer was produced and never on a
// fall-through or on the ordinary decode paths.
using namespace doris::snii;
using namespace doris::snii::snii_test;
using doris::snii::query::count_only_term_df;
using doris::snii::query::count_only_two_term_phrase_bigram_df;
using doris::snii::query::phrase_query;
using doris::snii::query::term_query;
namespace qinternal = doris::snii::query::internal;

namespace {

constexpr uint32_t kPruneThreshold = 64;

struct Fixture {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
};

void build_fixture(Fixture* f, bool include_bigrams, uint32_t bigram_prune_min_df) {
    assert_ok(build_reader(&f->file, &f->segment_reader, &f->index_reader, include_bigrams,
                           bigram_prune_min_df));
}

uint64_t full_decode_term_count(const reader::LogicalIndexReader& idx, const std::string& term) {
    std::vector<uint32_t> docids;
    assert_ok(term_query(idx, term, &docids));
    return docids.size();
}

uint64_t full_decode_phrase_count(const reader::LogicalIndexReader& idx,
                                  const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    assert_ok(phrase_query(idx, terms, &docids));
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
    build_fixture(&plain, /*include_bigrams=*/false, /*threshold=*/0);

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

// 2-term phrase with a SURVIVING bigram: on the G01-pruned segment the hot
// pair (df 9000 >= threshold) is answered from the bigram entry's df, equal to
// the full phrase execution on the pruned segment AND on the unpruned control.
TEST(SniiCountQuery, PhraseHotBigramDfEqualsFullDecode) {
    Fixture control, pruned;
    build_fixture(&control, /*include_bigrams=*/true, /*threshold=*/0);
    build_fixture(&pruned, /*include_bigrams=*/true, kPruneThreshold);

    const uint64_t truth = full_decode_phrase_count(pruned.index_reader, {"repeat", "repeat"});
    EXPECT_EQ(truth, 9000U);
    EXPECT_EQ(full_decode_phrase_count(control.index_reader, {"repeat", "repeat"}), truth);

    reset_query_counters();
    bool handled = false;
    uint64_t count = 0;
    assert_ok(count_only_two_term_phrase_bigram_df(pruned.index_reader, "repeat", "repeat",
                                                   &handled, &count));
    EXPECT_TRUE(handled);
    EXPECT_EQ(count, truth);
    EXPECT_EQ(count_hits(), 1U);

    // A low-df pair materialized on the LEGACY control segment is also a dict
    // HIT there and must equal its positional truth.
    handled = false;
    count = 0;
    assert_ok(count_only_two_term_phrase_bigram_df(control.index_reader, "failed", "order",
                                                   &handled, &count));
    EXPECT_TRUE(handled);
    EXPECT_EQ(count, full_decode_phrase_count(control.index_reader, {"failed", "order"}));
    EXPECT_EQ(count, 3U);
    EXPECT_EQ(count_hits(), 2U);
}

// Guard: a PRUNED bigram (dict miss on a segment whose meta declares a
// threshold) must fall through -- the miss is ambiguous and only the generic
// phrase path may answer it. The seam stays untouched.
TEST(SniiCountQuery, PrunedBigramFallsThrough) {
    Fixture pruned;
    build_fixture(&pruned, /*include_bigrams=*/true, kPruneThreshold);
    ASSERT_EQ(pruned.index_reader.bigram_prune_min_df(), kPruneThreshold);

    reset_query_counters();
    bool handled = true;
    uint64_t count = 42;
    assert_ok(count_only_two_term_phrase_bigram_df(pruned.index_reader, "failed", "order", &handled,
                                                   &count));
    EXPECT_FALSE(handled);
    EXPECT_EQ(count, 0U);
    EXPECT_EQ(count_hits(), 0U);

    // The generic path still owns the correct answer.
    EXPECT_EQ(full_decode_phrase_count(pruned.index_reader, {"failed", "order"}), 3U);
}

// Guards: legacy dict miss (pair never materialized), a bigram-less segment,
// and non-bigram-indexable terms all fall through conservatively.
TEST(SniiCountQuery, LegacyMissAndNonIndexableFallThrough) {
    Fixture control, nobigram;
    build_fixture(&control, /*include_bigrams=*/true, /*threshold=*/0);
    build_fixture(&nobigram, /*include_bigrams=*/false, /*threshold=*/0);

    reset_query_counters();
    bool handled = true;
    uint64_t count = 42;
    // Legacy segment, pair not materialized by the fixture: even though legacy
    // semantics say "miss == empty", the count path leaves that to phrase_query.
    assert_ok(count_only_two_term_phrase_bigram_df(control.index_reader, "driver", "failed",
                                                   &handled, &count));
    EXPECT_FALSE(handled);

    // Segment built without bigrams at all.
    handled = true;
    assert_ok(count_only_two_term_phrase_bigram_df(nobigram.index_reader, "repeat", "repeat",
                                                   &handled, &count));
    EXPECT_FALSE(handled);

    // "123" is not bigram-indexable (non-alpha): no synthetic term to look up.
    handled = true;
    assert_ok(count_only_two_term_phrase_bigram_df(control.index_reader, "123", "order", &handled,
                                                   &count));
    EXPECT_FALSE(handled);

    EXPECT_EQ(count_hits(), 0U);
}

// Guard: a positionless (kDocsOnly) index must fall through on the phrase
// shape -- the normal phrase path errors there and the count path must not
// mask it. The single-term df path stays valid on the same index.
TEST(SniiCountQuery, DocsOnlyIndexPhraseFallsThroughTermStillCounts) {
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
    bool handled = true;
    uint64_t count = 42;
    assert_ok(count_only_two_term_phrase_bigram_df(idx, "alpha", "bravo", &handled, &count));
    EXPECT_FALSE(handled);
    EXPECT_EQ(count_hits(), 0U);

    uint64_t df = 0;
    assert_ok(count_only_term_df(idx, "alpha", &df));
    EXPECT_EQ(df, full_decode_term_count(idx, "alpha"));
    EXPECT_EQ(df, 60U);
    assert_ok(count_only_term_df(idx, "bravo", &df));
    EXPECT_EQ(df, 15U);
    EXPECT_EQ(count_hits(), 2U);
}
