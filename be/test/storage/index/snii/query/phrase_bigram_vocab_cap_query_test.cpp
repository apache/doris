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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

// G04 verification gate: phrase query RESULTS are identical between a
// vocab-capped build (df==1 bigram tail evicted + bloom-dropped at flush) and
// an uncapped control, across every routing:
//   hot   : pair never evicted (df>=2 before any cap pressure) -> answered
//           DIRECTLY from its bigram posting in BOTH builds;
//   warm  : small survivor, direct in both;
//   evicted-reappearing : evicted at df==1, reappears later; the control
//           answers DIRECTLY (complete bigram posting incl. the pre-eviction
//           doc), the capped build has NO dict entry (bloom drop) and takes the
//           G01 COLD FALLBACK to unigram positions -- recovering the SAME doc
//           set, including the docid the evicted posting lost.
// The same token stream (unigrams with real positions + zero-alloc bigram
// adds + the sentinel, mirroring the production column writer) feeds both
// builds through the streaming term_source path.
using namespace doris::snii;
using namespace doris::snii::snii_test;
using doris::snii::query::phrase_query;
namespace qinternal = doris::snii::query::internal;
namespace wtesting = doris::snii::writer::testing;

namespace {

constexpr uint32_t kDocCount = 400;
constexpr uint32_t kThreshold = 1; // >0: prune mode; every final df survives the
                                   // df gate, isolating the BLOOM as the only
                                   // dropper of the evicted-reappearing pair

const std::string kHotTerm = format::make_phrase_bigram_term("hot", "pair");
const std::string kVictimTerm = format::make_phrase_bigram_term("gone", "away");

struct Fixture {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
};

// Feeds one document containing exactly the two adjacent words l r (unigram
// tokens at positions 0/1 + the hidden bigram), mirroring the production
// _add_value_tokens + _add_phrase_bigram_tokens split.
void feed_pair_doc(writer::SpimiTermBuffer* buf, uint32_t docid, std::string_view l,
                   std::string_view r) {
    buf->add_token(l, docid, 0);
    buf->add_token(r, docid, 1);
    buf->add_bigram_token(l, r, docid, 0);
}

std::pair<std::string, std::string> filler_pair(uint32_t i) {
    std::string l = "u";
    l += static_cast<char>('a' + (i / 26) % 26);
    l += static_cast<char>('a' + i % 26);
    l += static_cast<char>('a' + (i / 676) % 26);
    std::string r = "v";
    r += static_cast<char>('a' + i % 26);
    r += static_cast<char>('a' + (i / 26) % 26);
    r += static_cast<char>('a' + (i / 676) % 26);
    return {std::move(l), std::move(r)};
}

// Feeds the IDENTICAL token stream into both buffers. `capped` has the diet +
// cap configured; the filler phase keeps feeding unique tail pairs until the
// capped buffer provably evicted the victim (bounded; asserted by the caller).
void feed_stream(writer::SpimiTermBuffer* capped, writer::SpimiTermBuffer* control) {
    auto both = [&](uint32_t docid, std::string_view l, std::string_view r) {
        feed_pair_doc(capped, docid, l, r);
        feed_pair_doc(control, docid, l, r);
    };
    // Hot + warm pairs reach df==2 before any cap pressure -> never evictable.
    both(0, "hot", "pair");
    both(1, "hot", "pair");
    both(2, "warm", "mild");
    both(3, "warm", "mild");
    // The victim: one doc -> df==1 when the tail pressure arrives.
    both(4, "gone", "away");
    // Unique tail pairs until the victim is evicted (cursor-order sweep; the
    // loop bound fails the test loudly if it never fires). Docids 5..154+.
    uint32_t docid = 5;
    for (uint32_t i = 0; i < 2000 && docid < 190; ++i) {
        if (capped->bigram_dropped_filter() != nullptr &&
            capped->bigram_dropped_filter()->maybe_contains(kVictimTerm)) {
            break;
        }
        const auto [l, r] = filler_pair(i);
        both(docid++, l, r);
    }
    // The victim REAPPEARS: bigram tokens fed back-to-back first (the second
    // add re-establishes df==2 immunity deterministically -- a term is never
    // evicted by its own add's sweep), then the unigrams for those docs.
    for (uint32_t d = 200; d < 220; ++d) {
        capped->add_bigram_token("gone", "away", d, 0);
        control->add_bigram_token("gone", "away", d, 0);
    }
    for (uint32_t d = 200; d < 220; ++d) {
        for (writer::SpimiTermBuffer* buf : {capped, control}) {
            buf->add_token("gone", d, 0);
            buf->add_token("away", d, 1);
        }
    }
    // More hot occurrences after the pressure (docids keep ascending).
    for (uint32_t d = 300; d < 310; ++d) {
        both(d, "hot", "pair");
    }
    // The sentinel, as the production writer feeds it at finish().
    capped->add_token(format::make_phrase_bigram_sentinel_term(), 0, 0);
    control->add_token(format::make_phrase_bigram_sentinel_term(), 0, 0);
}

Status build_fixture(Fixture* f, writer::SpimiTermBuffer* buf,
                     const writer::BigramDropFilter* dropped) {
    writer::SniiIndexInput input;
    input.index_id = 21;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = kDocCount;
    input.bigram_prune_min_df = kThreshold;
    input.term_source = buf;
    input.bigram_ever_dropped = dropped;

    writer::SniiCompoundWriter writer(&f->file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(&f->file, &f->segment_reader));
    return f->segment_reader.open_index(input.index_id, input.index_suffix, &f->index_reader);
}

std::vector<uint32_t> run_phrase(const reader::LogicalIndexReader& idx,
                                 const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    assert_ok(phrase_query(idx, terms, &docids));
    return docids;
}

void reset_query_counters() {
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
}

} // namespace

TEST(SniiPhraseBigramVocabCap, ResultsEqualControlAcrossHotAndEvictedRoutings) {
    writer::SpimiTermBuffer capped_buf(/*has_positions=*/true);
    writer::SpimiTermBuffer control_buf(/*has_positions=*/true);
    capped_buf.configure_bigram_diet(/*vocab_cap_bytes=*/2 * 1024);
    // Control: same diet mode (docs-only bigrams, as production prune mode
    // implies) but UNCAPPED -- no eviction, complete bigram vocabulary.
    control_buf.configure_bigram_diet(/*vocab_cap_bytes=*/0);

    feed_stream(&capped_buf, &control_buf);
    ASSERT_TRUE(capped_buf.status().ok());
    ASSERT_TRUE(control_buf.status().ok());
    // The victim must have been evicted (and is therefore in the bloom); the
    // hot pair must not be.
    ASSERT_NE(capped_buf.bigram_dropped_filter(), nullptr);
    ASSERT_TRUE(capped_buf.bigram_dropped_filter()->maybe_contains(kVictimTerm));
    EXPECT_FALSE(capped_buf.bigram_dropped_filter()->maybe_contains(kHotTerm));

    wtesting::reset_bigram_prune_counters();
    Fixture capped;
    assert_ok(build_fixture(&capped, &capped_buf, capped_buf.bigram_dropped_filter()));
    // Exactly ONE df-surviving bigram was dropped by the BLOOM (the reappeared
    // victim; threshold 1 means the df gate dropped nothing that reached it).
    EXPECT_EQ(wtesting::bigram_bloom_dropped(), 1U);

    Fixture control;
    assert_ok(build_fixture(&control, &control_buf, control_buf.bigram_dropped_filter()));
    EXPECT_EQ(control.index_reader.bigram_prune_min_df(), kThreshold);
    EXPECT_EQ(capped.index_reader.bigram_prune_min_df(), kThreshold);

    // HOT pair: direct bigram hit in BOTH builds, equal results.
    const std::vector<std::string> hot {"hot", "pair"};
    std::vector<uint32_t> want_hot {0, 1};
    for (uint32_t d = 300; d < 310; ++d) {
        want_hot.push_back(d);
    }
    EXPECT_EQ(run_phrase(control.index_reader, hot), want_hot);
    reset_query_counters();
    EXPECT_EQ(run_phrase(capped.index_reader, hot), want_hot);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);

    // WARM small survivor: direct in both, equal.
    const std::vector<std::string> warm {"warm", "mild"};
    const std::vector<uint32_t> want_warm {2, 3};
    EXPECT_EQ(run_phrase(control.index_reader, warm), want_warm);
    reset_query_counters();
    EXPECT_EQ(run_phrase(capped.index_reader, warm), want_warm);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);

    // EVICTED-REAPPEARING pair: the control answers DIRECTLY from its complete
    // bigram posting {4, 200..219}; the capped build's bigram was bloom-dropped
    // (its surviving posting would have LOST doc 4), so it takes the COLD
    // FALLBACK to unigram positions -- and recovers the identical doc set.
    const std::vector<std::string> victim {"gone", "away"};
    std::vector<uint32_t> want_victim {4};
    for (uint32_t d = 200; d < 220; ++d) {
        want_victim.push_back(d);
    }
    reset_query_counters();
    EXPECT_EQ(run_phrase(control.index_reader, victim), want_victim);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);

    reset_query_counters();
    EXPECT_EQ(run_phrase(capped.index_reader, victim), want_victim);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);
}
