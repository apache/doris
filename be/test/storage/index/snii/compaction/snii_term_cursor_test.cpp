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

// T2.3 unit tests: SniiSegmentTermCursor (pull-model full-dictionary scan),
// TermMergeFrontier (k-way merge by term) and SequentialRegionReader (chunked
// posting-region read-ahead). The load-bearing assertions, per the design:
//   - interleaved dictionaries merge into ONE strictly increasing term order,
//     equal terms aggregated across sources in ascending source order;
//   - reserved phrase-bigram / sentinel terms (full 0x1F marker) abort
//     the scan with a distinct error, while user terms merely starting with a
//     raw 0x1F byte pass through (marker classification, not prefix-byte);
//   - all three posting encodings (inline / slim pod_ref / windowed pod_ref)
//     and the kNoTermStats flag are passed through UNINTERPRETED -- the cursor
//     yields exactly the DictEntry lookup() yields;
//   - read-ahead honors chunk boundaries, clamps to the region end, and falls
//     back to exact reads for oversized/backward windows without disturbing
//     the buffered forward stream;
//   - empty and single-source inputs degenerate cleanly.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <map>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/compaction/indexed_winner_tree.h"
#include "storage/index/snii/compaction/region_reader.h"
#include "storage/index/snii/compaction/term_cursor.h"
#include "storage/index/snii/compaction/term_merge_frontier.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace {

using namespace doris::snii;            // NOLINT
using namespace doris::snii::snii_test; // NOLINT
namespace ErrorCode = doris::ErrorCode;
using doris::Status;
using compaction::PostingStream;
using compaction::SequentialRegionReader;
using compaction::SharedAlignedRegionCache;
using compaction::SniiSegmentTermCursor;
using compaction::TermMergeFrontier;
using compaction::big_endian_term_prefix;
using compaction::IndexedWinnerTree;

// One built source segment: the compound bytes plus an opened logical index.
struct SourceFixture {
    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
};

Status build_source(std::vector<writer::TermPostings> terms, uint32_t doc_count, SourceFixture* fx,
                    bool write_freq = true, uint32_t target_dict_block_bytes = 256,
                    reader::LogicalIndexOpenMode open_mode = reader::LogicalIndexOpenMode::kQuery) {
    writer::SniiIndexInput in;
    in.index_id = 7;
    in.index_suffix = "body";
    in.config = format::IndexConfig::kDocsPositions;
    in.doc_count = doc_count;
    in.write_freq = write_freq;
    // Small blocks force a multi-block dictionary so the scan crosses block
    // boundaries (and the frq/prx bases change between blocks).
    in.target_dict_block_bytes = target_dict_block_bytes;
    std::ranges::sort(terms, [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
        return lhs.term < rhs.term;
    });
    in.terms = std::move(terms);

    writer::SniiCompoundWriter cw(&fx->file);
    RETURN_IF_ERROR(cw.add_logical_index(in));
    RETURN_IF_ERROR(cw.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(&fx->file, &fx->segment));
    return fx->segment.open_index(7, "body", &fx->index, open_mode);
}

// Full DictEntry equality: the cursor contract is UNINTERPRETED passthrough,
// so every locator/stat/inline field must match what lookup() returns.
void expect_entry_eq(const format::DictEntry& got, const format::DictEntry& want) {
    EXPECT_EQ(got.term, want.term);
    EXPECT_EQ(got.kind, want.kind);
    EXPECT_EQ(got.enc, want.enc);
    EXPECT_EQ(got.has_sb, want.has_sb);
    EXPECT_EQ(got.df, want.df);
    EXPECT_EQ(got.ttf_delta, want.ttf_delta);
    EXPECT_EQ(got.term_stats_present, want.term_stats_present);
    EXPECT_EQ(got.max_freq, want.max_freq);
    EXPECT_EQ(got.frq_off_delta, want.frq_off_delta);
    EXPECT_EQ(got.frq_len, want.frq_len);
    EXPECT_EQ(got.prelude_len, want.prelude_len);
    EXPECT_EQ(got.frq_docs_len, want.frq_docs_len);
    EXPECT_EQ(got.prx_off_delta, want.prx_off_delta);
    EXPECT_EQ(got.prx_len, want.prx_len);
    EXPECT_EQ(got.inline_dd_disk_len, want.inline_dd_disk_len);
    EXPECT_EQ(got.dd_meta.zstd, want.dd_meta.zstd);
    EXPECT_EQ(got.dd_meta.uncomp_len, want.dd_meta.uncomp_len);
    EXPECT_EQ(got.dd_meta.disk_len, want.dd_meta.disk_len);
    EXPECT_EQ(got.dd_meta.crc, want.dd_meta.crc);
    EXPECT_EQ(got.freq_meta.zstd, want.freq_meta.zstd);
    EXPECT_EQ(got.freq_meta.uncomp_len, want.freq_meta.uncomp_len);
    EXPECT_EQ(got.freq_meta.disk_len, want.freq_meta.disk_len);
    EXPECT_EQ(got.freq_meta.crc, want.freq_meta.crc);
    EXPECT_EQ(got.frq_bytes, want.frq_bytes);
    EXPECT_EQ(got.prx_bytes, want.prx_bytes);
}

// Corpus spanning all three posting encodings:
//   "aa_tiny"     df=1   -> inline (encoded bytes <= inline threshold)
//   "mid_slim"    df=500 -> slim pod_ref (df < 512 but bytes > threshold)
//   "zz_wide"     df=600 -> windowed pod_ref (df >= kSlimDfThreshold)
// plus filler vocabulary so target_dict_block_bytes=256 yields several blocks.
std::vector<writer::TermPostings> three_encoding_terms(uint32_t* doc_count) {
    *doc_count = 600;
    std::vector<writer::TermPostings> terms;
    terms.push_back(make_term("aa_tiny", {{.docid = 5, .positions = {1}}}));
    std::vector<PostingDoc> mid;
    mid.reserve(500);
    for (uint32_t d = 0; d < 500; ++d) {
        uint32_t mixed = d * 2654435761U;
        mixed ^= mixed >> 16;
        const uint32_t frequency = mixed % 97 + 1;
        std::vector<uint32_t> positions(frequency);
        for (uint32_t i = 0; i < frequency; ++i) {
            positions[i] = i * 3;
        }
        mid.push_back({.docid = d, .positions = std::move(positions)});
    }
    terms.push_back(make_term("mid_slim", std::move(mid)));
    terms.push_back(make_term("zz_wide", docs_with_one_position(0, 600, 2)));
    for (int i = 0; i < 24; ++i) {
        terms.push_back(make_term("filler_" + std::string(1, static_cast<char>('a' + i)),
                                  docs_with_one_position(0, 40 + i, 1)));
    }
    return terms;
}

// Drains a cursor, checking each yielded entry against lookup() (locator
// passthrough) and collecting the term sequence.
void drain_and_check(SourceFixture* fx, std::vector<std::string>* terms_out) {
    SniiSegmentTermCursor cursor(&fx->index, /*source_ordinal=*/0);
    bool has_term = false;
    ASSERT_TRUE(cursor.next(&has_term).ok());
    while (has_term) {
        terms_out->push_back(cursor.term());

        bool found = false;
        format::DictEntry want;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(fx->index.lookup(cursor.term(), &found, &want, &frq_base, &prx_base));
        ASSERT_TRUE(found) << cursor.term();
        expect_entry_eq(cursor.entry(), want);
        EXPECT_EQ(cursor.frq_base(), frq_base) << cursor.term();
        EXPECT_EQ(cursor.prx_base(), prx_base) << cursor.term();

        ASSERT_TRUE(cursor.next(&has_term).ok());
    }
}

// ---------------------------------------------------------------------------
// SniiSegmentTermCursor
// ---------------------------------------------------------------------------

TEST(SniiTermCursorTest, FullDictionaryOrderedScanWithLocatorPassthrough) {
    uint32_t doc_count = 0;
    std::vector<writer::TermPostings> terms = three_encoding_terms(&doc_count);
    std::vector<std::string> want_terms;
    want_terms.reserve(terms.size());
    for (const auto& tp : terms) {
        want_terms.push_back(tp.term);
    }
    std::ranges::sort(want_terms);

    SourceFixture fx;
    assert_ok(build_source(std::move(terms), doc_count, &fx));
    // The scan must cross DICT block boundaries to prove per-block base/state
    // tracking; 256-byte target blocks guarantee it for this corpus.
    ASSERT_GT(fx.index.n_dict_blocks(), 1U);

    std::vector<std::string> got_terms;
    drain_and_check(&fx, &got_terms);
    EXPECT_EQ(got_terms, want_terms);

    // Three-encoding coverage: the corpus really exercised inline, slim
    // pod_ref and windowed pod_ref (guards against a corpus regression that
    // would silently weaken the passthrough assertions above).
    auto entry_of = [&](const std::string& term) {
        bool found = false;
        format::DictEntry e;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        EXPECT_TRUE(fx.index.lookup(term, &found, &e, &frq_base, &prx_base).ok());
        EXPECT_TRUE(found) << term;
        return e;
    };
    EXPECT_EQ(entry_of("aa_tiny").kind, format::DictEntryKind::kInline);
    const format::DictEntry slim = entry_of("mid_slim");
    EXPECT_EQ(slim.kind, format::DictEntryKind::kPodRef);
    EXPECT_EQ(slim.enc, format::DictEntryEnc::kSlim);
    const format::DictEntry wide = entry_of("zz_wide");
    EXPECT_EQ(wide.kind, format::DictEntryKind::kPodRef);
    EXPECT_EQ(wide.enc, format::DictEntryEnc::kWindowed);
}

TEST(SniiTermCursorTest, CompactionOpenSkipsResidentQuerySections) {
    SourceFixture query;
    assert_ok(build_source({make_term("alpha", {{.docid = 0, .positions = {0}}})},
                           /*doc_count=*/1, &query));
    const format::SectionRefs refs = query.index.section_refs();
    ASSERT_GT(refs.dict_region.length, 0U);
    ASSERT_GT(refs.bsbf.length, 0U);

    query.file.clear_reads();
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    assert_ok(reader::SniiSegmentReader::open(&query.file, &segment));
    assert_ok(segment.open_index(7, "body", &index, reader::LogicalIndexOpenMode::kCompaction));
    EXPECT_EQ(index.open_mode(), reader::LogicalIndexOpenMode::kCompaction);
    for (const MemoryFile::Read& read : query.file.reads()) {
        EXPECT_FALSE(read.offset == refs.dict_region.offset && read.len == refs.dict_region.length);
        EXPECT_FALSE(read.offset == refs.bsbf.offset && read.len == refs.bsbf.length);
    }

    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index.lookup("alpha", &found, &entry, &frq_base, &prx_base));
    EXPECT_TRUE(found);
}

TEST(SniiTermCursorTest, DictScanMemoryIsReservedAndLimitErrorIsPreserved) {
    SourceFixture source;
    assert_ok(build_source({make_term("alpha", {{.docid = 0, .positions = {0}}})},
                           /*doc_count=*/1, &source, /*write_freq=*/true,
                           /*target_dict_block_bytes=*/256,
                           reader::LogicalIndexOpenMode::kCompaction));

    writer::MemoryReporter reporter;
    {
        SniiSegmentTermCursor cursor(&source.index, /*source_ordinal=*/0, &reporter);
        bool has_term = false;
        assert_ok(cursor.next(&has_term));
        ASSERT_TRUE(has_term);
        EXPECT_GT(reporter.current_bytes(), 0);
    }
    EXPECT_EQ(reporter.current_bytes(), 0);

    writer::MemoryReporter limited_reporter(/*consume_release=*/nullptr, /*cap_bytes=*/1);
    {
        SniiSegmentTermCursor cursor(&source.index, /*source_ordinal=*/0, &limited_reporter);
        bool has_term = false;
        const Status status = cursor.next(&has_term);
        EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
        EXPECT_FALSE(has_term);
        EXPECT_TRUE(cursor.next(&has_term).is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    }
    EXPECT_EQ(limited_reporter.current_bytes(), 0);
}

TEST(SniiTermCursorTest, EmptyIndexYieldsNothing) {
    SourceFixture fx;
    assert_ok(build_source({}, /*doc_count=*/16, &fx));
    EXPECT_EQ(fx.index.n_dict_blocks(), 0U);

    SniiSegmentTermCursor cursor(&fx.index, 0);
    bool has_term = true;
    ASSERT_TRUE(cursor.next(&has_term).ok());
    EXPECT_FALSE(has_term);
    // Exhaustion is stable, not an error.
    has_term = true;
    ASSERT_TRUE(cursor.next(&has_term).ok());
    EXPECT_FALSE(has_term);
}

TEST(SniiTermCursorTest, SyntheticMarkerTermAborts) {
    // A marker term written through the modern writer path (not the frozen
    // image) must abort identically -- the gate is on the term key, not on
    // legacy meta.
    std::vector<writer::TermPostings> terms;
    terms.push_back(make_term(std::string(format::kPhraseBigramTermMarker) + "ab",
                              {{.docid = 0, .positions = {0}}}));
    terms.push_back(make_term("alpha", {{.docid = 1, .positions = {0}}}));

    SourceFixture fx;
    assert_ok(build_source(std::move(terms), 8, &fx));

    SniiSegmentTermCursor cursor(&fx.index, 0);
    bool has_term = false;
    const Status st = cursor.next(&has_term);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << st.to_string();
}

TEST(SniiTermCursorTest, LeadingUnitSeparatorUserTermsPass) {
    // 0x1F-corner ruling: classification uses the FULL marker. A user term
    // that merely begins with a raw 0x1F byte -- including a strict PREFIX of
    // the marker -- is a legitimate dictionary term and must scan through.
    const std::string partial_marker =
            "\x1F"
            "SNII_PHRASE"; // prefix of the marker, not the marker
    const std::string unit_sep_user =
            "\x1F"
            "user";
    std::vector<writer::TermPostings> terms;
    terms.push_back(make_term(partial_marker, {{.docid = 0, .positions = {0}}}));
    terms.push_back(make_term(unit_sep_user, {{.docid = 1, .positions = {0}}}));
    terms.push_back(make_term("alpha", {{.docid = 2, .positions = {0}}}));

    SourceFixture fx;
    assert_ok(build_source(std::move(terms), 8, &fx));

    std::vector<std::string> got;
    drain_and_check(&fx, &got);
    EXPECT_EQ(got, (std::vector<std::string> {partial_marker, unit_sep_user, "alpha"}));
}

TEST(SniiTermCursorTest, NoTermStatsFlagPassthrough) {
    // A freq-dropped index (write_freq=false on a positions config) writes
    // kNoTermStats DICT blocks; the cursor must surface term_stats_present ==
    // false so the downstream pump recomputes ttf/max_freq from the actual
    // freq stream instead of trusting meaningless defaults (invariant 2).
    auto build_terms = [] {
        std::vector<writer::TermPostings> terms;
        terms.push_back(make_term("alpha", docs_with_one_position(0, 20, 0)));
        terms.push_back(make_term("bravo", {{.docid = 3, .positions = {1, 4}}}));
        return terms;
    };

    SourceFixture dropped;
    assert_ok(build_source(build_terms(), 32, &dropped, /*write_freq=*/false));
    SniiSegmentTermCursor cursor(&dropped.index, 0);
    bool has_term = false;
    size_t seen = 0;
    ASSERT_TRUE(cursor.next(&has_term).ok());
    while (has_term) {
        EXPECT_FALSE(cursor.entry().term_stats_present) << cursor.term();
        ++seen;
        ASSERT_TRUE(cursor.next(&has_term).ok());
    }
    EXPECT_EQ(seen, 2U);

    SourceFixture kept;
    assert_ok(build_source(build_terms(), 32, &kept, /*write_freq=*/true));
    SniiSegmentTermCursor kept_cursor(&kept.index, 0);
    ASSERT_TRUE(kept_cursor.next(&has_term).ok());
    ASSERT_TRUE(has_term);
    EXPECT_TRUE(kept_cursor.entry().term_stats_present);
}

// ---------------------------------------------------------------------------
// TermMergeFrontier
// ---------------------------------------------------------------------------

TEST(SniiIndexedWinnerTreeTest, StartsAsValidEmptyTree) {
    auto before = [](size_t lhs, size_t rhs) { return lhs < rhs; };
    IndexedWinnerTree tree(before);
    static_assert(noexcept(tree.empty()));

    EXPECT_TRUE(tree.empty());
    tree.build(0, [](size_t) { return false; });
    EXPECT_TRUE(tree.empty());

    tree.build(3, [](size_t source) { return source != 1; });
    ASSERT_FALSE(tree.empty());
    EXPECT_EQ(tree.winner(), 0);
    EXPECT_EQ(tree.runner_up(), 2);

    tree.update(0, false);
    EXPECT_EQ(tree.winner(), 2);
    EXPECT_EQ(tree.runner_up(), IndexedWinnerTree<decltype(before)>::kNoSource);
    tree.update(1, true);
    EXPECT_EQ(tree.winner(), 1);
    EXPECT_EQ(tree.runner_up(), 2);
    tree.update(1, false);
    tree.update(2, false);
    EXPECT_TRUE(tree.empty());
}

TEST(SniiTermMergeFrontierTest, InterleavedDictionariesMergeInFullOrder) {
    // Three sources with interleaved vocabularies and two shared terms; the
    // merged stream must be strictly increasing, with same-term sources
    // aggregated in ascending source order and per-source df passed through.
    SourceFixture a;
    SourceFixture b;
    SourceFixture c;
    assert_ok(build_source({make_term("apple", docs_with_one_position(0, 3, 0)),
                            make_term("banana", docs_with_one_position(0, 5, 0)),
                            make_term("cherry", docs_with_one_position(0, 2, 0)),
                            make_term("shared", docs_with_one_position(0, 7, 0))},
                           16, &a));
    assert_ok(build_source({make_term("banana", docs_with_one_position(0, 4, 0)),
                            make_term("date", docs_with_one_position(0, 6, 0)),
                            make_term("shared", docs_with_one_position(0, 9, 0))},
                           16, &b));
    assert_ok(build_source({make_term("apple", docs_with_one_position(0, 8, 0)),
                            make_term("elderberry", docs_with_one_position(0, 1, 0))},
                           16, &c));

    SniiSegmentTermCursor ca(&a.index, 0);
    SniiSegmentTermCursor cb(&b.index, 1);
    SniiSegmentTermCursor cc(&c.index, 2);
    TermMergeFrontier frontier;
    assert_ok(frontier.init({&ca, &cb, &cc}));

    struct WantGroup {
        std::string term;
        std::vector<std::pair<uint32_t, uint32_t>> sources; // (ordinal, df)
    };
    const std::vector<WantGroup> want = {
            {"apple", {{0, 3}, {2, 8}}}, {"banana", {{0, 5}, {1, 4}}}, {"cherry", {{0, 2}}},
            {"date", {{1, 6}}},          {"elderberry", {{2, 1}}},     {"shared", {{0, 7}, {1, 9}}},
    };

    std::string prev_term;
    for (const WantGroup& wg : want) {
        ASSERT_FALSE(frontier.empty());
        const std::string group_term = frontier.front()->term();
        EXPECT_EQ(group_term, wg.term);
        EXPECT_TRUE(prev_term < group_term); // strictly increasing across groups
        prev_term = group_term;
        size_t source_index = 0;
        while (!frontier.empty() && frontier.front()->term() == group_term) {
            SniiSegmentTermCursor* cursor = frontier.front();
            ASSERT_LT(source_index, wg.sources.size()) << wg.term;
            EXPECT_EQ(cursor->source_ordinal(), wg.sources[source_index].first) << wg.term;
            format::DictEntry entry = cursor->take_entry();
            EXPECT_EQ(entry.df, wg.sources[source_index].second) << wg.term;
            EXPECT_EQ(entry.term, wg.term);
            ++source_index;
            assert_ok(frontier.advance_front());
        }
        EXPECT_EQ(source_index, wg.sources.size()) << wg.term;
    }
    EXPECT_TRUE(frontier.empty());
    // Exhaustion is stable.
    EXPECT_TRUE(frontier.empty());
}

TEST(SniiTermMergeFrontierTest, SingleSourceDegenerates) {
    uint32_t doc_count = 0;
    std::vector<writer::TermPostings> terms = three_encoding_terms(&doc_count);
    std::vector<std::string> want_terms;
    for (const auto& tp : terms) {
        want_terms.push_back(tp.term);
    }
    std::ranges::sort(want_terms);

    SourceFixture fx;
    assert_ok(build_source(std::move(terms), doc_count, &fx));
    SniiSegmentTermCursor cursor(&fx.index, 5);
    TermMergeFrontier frontier;
    assert_ok(frontier.init({&cursor}));

    std::vector<std::string> got;
    while (!frontier.empty()) {
        SniiSegmentTermCursor* current = frontier.front();
        EXPECT_EQ(current->source_ordinal(), 5U);
        format::DictEntry entry = current->take_entry();
        got.push_back(std::move(entry.term));
        assert_ok(frontier.advance_front());
    }
    EXPECT_EQ(got, want_terms);
}

TEST(SniiTermMergeFrontierTest, EmptyInputsDegenerate) {
    // No sources at all.
    TermMergeFrontier none;
    assert_ok(none.init({}));
    EXPECT_TRUE(none.empty());

    // One source whose dictionary is empty: it never enters the frontier.
    SourceFixture empty;
    assert_ok(build_source({}, 16, &empty));
    SourceFixture full;
    assert_ok(build_source({make_term("only", {{.docid = 0, .positions = {0}}})}, 16, &full));
    SniiSegmentTermCursor ce(&empty.index, 0);
    SniiSegmentTermCursor cf(&full.index, 1);
    TermMergeFrontier frontier;
    assert_ok(frontier.init({&ce, &cf}));
    ASSERT_FALSE(frontier.empty());
    EXPECT_EQ(frontier.front()->term(), "only");
    EXPECT_EQ(frontier.front()->source_ordinal(), 1U);
    static_cast<void>(frontier.front()->take_entry());
    assert_ok(frontier.advance_front());
    EXPECT_TRUE(frontier.empty());
}

TEST(SniiTermMergeFrontierTest, CachedPrefixesPreserveRandomizedStringOrderingAndGrouping) {
    EXPECT_EQ(big_endian_term_prefix(""), 0U);
    EXPECT_EQ(big_endian_term_prefix("a"), 0x6100000000000000ULL);
    EXPECT_EQ(big_endian_term_prefix("abcdefgh"), 0x6162636465666768ULL);
    EXPECT_EQ(big_endian_term_prefix("abcdefgh-tail"), 0x6162636465666768ULL);
    const std::string binary_prefix {static_cast<char>(0xFF), static_cast<char>(0x80), '\0', 'a'};
    EXPECT_EQ(big_endian_term_prefix(binary_prefix), 0xFF80006100000000ULL);

    std::vector<std::string> vocabulary = {
            "",
            "a",
            std::string("a\0", 2),
            std::string("a\0b", 3),
            "abcdefgh",
            "abcdefghx",
            "abcdefghy",
            std::string(1, static_cast<char>(0x80)),
            std::string {static_cast<char>(0x80), '\0', static_cast<char>(0xFF)},
            std::string(1, static_cast<char>(0xFF)),
            std::string {static_cast<char>(0xFF), '\0'},
            std::string(96, 'z'),
            "shared-prefix",
    };
    uint64_t state = 0x9E3779B97F4A7C15ULL;
    for (size_t ordinal = 0; ordinal < 160; ++ordinal) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const size_t suffix_size = 1 + static_cast<size_t>((state >> 32) % 24);
        std::string term = ordinal % 3 == 0 ? "shared-prefix-" : "random-";
        for (size_t i = 0; i < suffix_size; ++i) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            term.push_back(static_cast<char>(state >> 32));
        }
        term.append("-").append(std::to_string(ordinal));
        vocabulary.push_back(std::move(term));
    }

    constexpr size_t kSourceCount = 5;
    std::array<SourceFixture, kSourceCount> fixtures;
    std::array<SniiSegmentTermCursor*, kSourceCount> cursor_ptrs {};
    std::vector<std::unique_ptr<SniiSegmentTermCursor>> cursors;
    cursors.reserve(kSourceCount);
    std::map<std::string, std::vector<uint32_t>> expected;
    for (size_t source = 0; source < kSourceCount; ++source) {
        std::vector<writer::TermPostings> terms;
        for (size_t ordinal = 0; ordinal < vocabulary.size(); ++ordinal) {
            if ((ordinal * 17 + source * 11) % 4 == 0) {
                continue;
            }
            terms.push_back(make_term(vocabulary[ordinal], {{.docid = 0, .positions = {1}}}));
            expected[vocabulary[ordinal]].push_back(static_cast<uint32_t>(source));
        }
        assert_ok(build_source(std::move(terms), 1, &fixtures[source], /*write_freq=*/true,
                               /*target_dict_block_bytes=*/128));
        cursors.push_back(std::make_unique<SniiSegmentTermCursor>(&fixtures[source].index,
                                                                  static_cast<uint32_t>(source)));
        cursor_ptrs[source] = cursors.back().get();
    }

    TermMergeFrontier frontier;
    assert_ok(frontier.init(
            std::vector<SniiSegmentTermCursor*>(cursor_ptrs.begin(), cursor_ptrs.end())));
    for (const auto& [term, source_ordinals] : expected) {
        ASSERT_FALSE(frontier.empty());
        const std::string group_term = frontier.front()->term();
        EXPECT_EQ(group_term, term);
        size_t source_index = 0;
        while (!frontier.empty() && frontier.front()->term() == group_term) {
            SniiSegmentTermCursor* current = frontier.front();
            ASSERT_LT(source_index, source_ordinals.size());
            EXPECT_EQ(current->source_ordinal(), source_ordinals[source_index]);
            format::DictEntry entry = current->take_entry();
            EXPECT_EQ(entry.term, term);
            ++source_index;
            assert_ok(frontier.advance_front());
        }
        EXPECT_EQ(source_index, source_ordinals.size());
    }
    EXPECT_TRUE(frontier.empty());
}

TEST(SniiTermMergeFrontierTest, CursorErrorPropagatesFromInit) {
    // A source with a reserved marker term fails during priming, and the
    // frontier must surface the cursor's distinct error so the caller aborts
    // this column's merge.
    SourceFixture invalid;
    assert_ok(build_source({make_term(std::string(format::kPhraseBigramTermMarker) + "ab",
                                      {{.docid = 0, .positions = {0}}})},
                           8, &invalid));
    SourceFixture normal;
    assert_ok(build_source({make_term("alpha", {{.docid = 0, .positions = {0}}})}, 8, &normal));

    SniiSegmentTermCursor cl(&invalid.index, 0);
    SniiSegmentTermCursor cn(&normal.index, 1);
    TermMergeFrontier frontier;
    const Status st = frontier.init({&cl, &cn});
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << st.to_string();
    // Priming may already have advanced an earlier source. The frontier is
    // terminal after this error: retrying init could otherwise skip a term.
    EXPECT_TRUE(frontier.init({&cl, &cn}).is<ErrorCode::INVALID_ARGUMENT>());
    const Status sticky = frontier.advance_front();
    EXPECT_TRUE(sticky.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << sticky.to_string();
}

TEST(SniiTermMergeFrontierTest, CursorErrorDuringAdvanceIsSticky) {
    std::vector<writer::TermPostings> terms;
    terms.push_back(make_term("", {{.docid = 0, .positions = {0}}}));
    terms.push_back(make_term(std::string(format::kPhraseBigramTermMarker) + "ab",
                              {{.docid = 1, .positions = {0}}}));

    SourceFixture source;
    assert_ok(build_source(std::move(terms), 8, &source));
    SniiSegmentTermCursor cursor(&source.index, 0);
    TermMergeFrontier frontier;
    assert_ok(frontier.init({&cursor}));
    ASSERT_FALSE(frontier.empty());
    EXPECT_TRUE(frontier.front()->term().empty());
    static_cast<void>(frontier.front()->take_entry());

    const Status first = frontier.advance_front();
    ASSERT_FALSE(first.ok());
    EXPECT_TRUE(first.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << first.to_string();
    const Status sticky = frontier.advance_front();
    EXPECT_TRUE(sticky.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << sticky.to_string();
    EXPECT_EQ(sticky.to_string(), first.to_string());
}

TEST(SniiTermMergeFrontierTest, LifecycleGuards) {
    TermMergeFrontier frontier;
    EXPECT_TRUE(frontier.advance_front().is<ErrorCode::INVALID_ARGUMENT>());
    assert_ok(frontier.init({}));
    EXPECT_TRUE(frontier.init({}).is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(frontier.advance_front().is<ErrorCode::INVALID_ARGUMENT>());

    TermMergeFrontier with_null;
    EXPECT_TRUE(with_null.init({nullptr}).is<ErrorCode::INVALID_ARGUMENT>());
}

// ---------------------------------------------------------------------------
// SequentialRegionReader
// ---------------------------------------------------------------------------

// A file of distinct bytes so slices can be verified by value; the region is a
// strict interior window so clamping to the region (not the file) is provable.
void fill_pattern_file(MemoryFile* file, size_t n) {
    std::vector<uint8_t> bytes(n);
    for (size_t i = 0; i < n; ++i) {
        bytes[i] = static_cast<uint8_t>(i * 31 + 7);
    }
    ASSERT_TRUE(file->append(Slice(bytes)).ok());
}

void expect_slice_matches(const MemoryFile& file, const Slice& got, uint64_t abs_off,
                          uint64_t len) {
    ASSERT_EQ(got.size(), len);
    for (uint64_t i = 0; i < len; ++i) {
        ASSERT_EQ(got[i], file.data()[abs_off + i]) << "at " << abs_off + i;
    }
}

TEST(SniiTermCursorRegionReaderTest, SequentialChunkingAndTailClamp) {
    MemoryFile file;
    fill_pattern_file(&file, 200);
    // Region [10, 110), chunk 16: interior on both sides.
    SequentialRegionReader rr(&file, /*region_offset=*/10, /*region_length=*/100,
                              /*chunk_bytes=*/16);
    std::vector<uint8_t> scratch;
    Slice out;

    // First window fills one whole chunk at the window start.
    assert_ok(rr.resolve(10, 8, &scratch, &out));
    expect_slice_matches(file, out, 10, 8);
    ASSERT_EQ(file.reads().size(), 1U);
    EXPECT_EQ(file.reads()[0].offset, 10U);
    EXPECT_EQ(file.reads()[0].len, 16U);

    // Fully-buffered window: zero-copy, NO new read.
    assert_ok(rr.resolve(18, 8, &scratch, &out));
    expect_slice_matches(file, out, 18, 8);
    EXPECT_EQ(file.reads().size(), 1U);
    EXPECT_EQ(rr.buffer_hits(), 1U);

    // Window straddling the chunk end: forward miss -> refill AT the window.
    assert_ok(rr.resolve(24, 8, &scratch, &out));
    expect_slice_matches(file, out, 24, 8);
    ASSERT_EQ(file.reads().size(), 2U);
    EXPECT_EQ(file.reads()[1].offset, 24U);
    EXPECT_EQ(file.reads()[1].len, 16U);

    // Tail window: the refill is clamped to the REGION end (110), not the
    // chunk size and not the file end -- read-ahead never leaves the region.
    assert_ok(rr.resolve(104, 6, &scratch, &out));
    expect_slice_matches(file, out, 104, 6);
    ASSERT_EQ(file.reads().size(), 3U);
    EXPECT_EQ(file.reads()[2].offset, 104U);
    EXPECT_EQ(file.reads()[2].len, 6U);

    // Zero-length window: no read, empty slice.
    assert_ok(rr.resolve(110, 0, &scratch, &out));
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(file.reads().size(), 3U);
    EXPECT_EQ(rr.read_calls(), 3U);
}

TEST(SniiTermCursorRegionReaderTest, OversizedAndBackwardFallThroughToExactReads) {
    MemoryFile file;
    fill_pattern_file(&file, 200);
    SequentialRegionReader rr(&file, 10, 100, /*chunk_bytes=*/16);
    std::vector<uint8_t> scratch;
    Slice out;

    assert_ok(rr.resolve(10, 8, &scratch, &out)); // buffer = [10, 26)
    ASSERT_EQ(file.reads().size(), 1U);

    // Oversized window (> chunk): ONE exact read into scratch; the buffered
    // chunk survives untouched.
    assert_ok(rr.resolve(30, 40, &scratch, &out));
    expect_slice_matches(file, out, 30, 40);
    ASSERT_EQ(file.reads().size(), 2U);
    EXPECT_EQ(file.reads()[1].offset, 30U);
    EXPECT_EQ(file.reads()[1].len, 40U);

    // The old chunk still serves hits (proves the fallback did not evict it).
    assert_ok(rr.resolve(12, 4, &scratch, &out));
    expect_slice_matches(file, out, 12, 4);
    EXPECT_EQ(file.reads().size(), 2U);

    // Advance the stream, then a BACKWARD window: exact read, buffer kept.
    assert_ok(rr.resolve(50, 8, &scratch, &out)); // buffer = [50, 66)
    ASSERT_EQ(file.reads().size(), 3U);
    assert_ok(rr.resolve(10, 4, &scratch, &out));
    expect_slice_matches(file, out, 10, 4);
    ASSERT_EQ(file.reads().size(), 4U);
    EXPECT_EQ(file.reads()[3].offset, 10U);
    EXPECT_EQ(file.reads()[3].len, 4U);
    assert_ok(rr.resolve(52, 4, &scratch, &out)); // still buffered
    expect_slice_matches(file, out, 52, 4);
    EXPECT_EQ(file.reads().size(), 4U);
}

TEST(SniiTermCursorRegionReaderTest, OutOfRegionWindowsRejected) {
    MemoryFile file;
    fill_pattern_file(&file, 200);
    SequentialRegionReader rr(&file, 10, 100, 16);
    std::vector<uint8_t> scratch;
    Slice out;

    EXPECT_TRUE(rr.resolve(9, 4, &scratch, &out).is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_TRUE(rr.resolve(108, 4, &scratch, &out).is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_TRUE(rr.resolve(111, 0, &scratch, &out).is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_EQ(file.reads().size(), 0U);
}

TEST(SniiTermCursorRegionReaderTest, OverflowingRegionRejected) {
    MemoryFile file;
    fill_pattern_file(&file, 32);
    SequentialRegionReader rr(&file, std::numeric_limits<uint64_t>::max() - 3,
                              /*region_length=*/8, /*chunk_bytes=*/16);
    std::vector<uint8_t> scratch;
    Slice out;

    EXPECT_TRUE(rr.resolve(std::numeric_limits<uint64_t>::max() - 2, 1, &scratch, &out)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_TRUE(file.reads().empty());
}

TEST(SniiTermCursorRegionReaderTest, SharedCacheCoalescesLogicalStreamsAndPinsSlices) {
    MemoryFile file;
    fill_pattern_file(&file, 200);
    SharedAlignedRegionCache cache(&file, /*region_offset=*/10, /*region_length=*/100,
                                   /*total_budget_bytes=*/32);
    assert_ok(cache.init());
    std::vector<uint8_t> docs_scratch;
    std::vector<uint8_t> prx_scratch;
    Slice docs;
    Slice prx;

    assert_ok(cache.resolve(PostingStream::kDocs, 12, 4, &docs_scratch, &docs));
    expect_slice_matches(file, docs, 12, 4);
    ASSERT_EQ(file.reads().size(), 1U);
    EXPECT_EQ(file.reads()[0].offset, 10U);
    EXPECT_EQ(file.reads()[0].len, 16U);

    assert_ok(cache.resolve(PostingStream::kPrx, 20, 4, &prx_scratch, &prx));
    expect_slice_matches(file, prx, 20, 4);
    expect_slice_matches(file, docs, 12, 4);
    EXPECT_EQ(file.reads().size(), 1U);
    EXPECT_EQ(cache.buffer_hits(PostingStream::kPrx), 1U);

    assert_ok(cache.resolve(PostingStream::kDocs, 30, 4, &docs_scratch, &docs));
    expect_slice_matches(file, docs, 30, 4);
    expect_slice_matches(file, prx, 20, 4);
    ASSERT_EQ(file.reads().size(), 2U);
    EXPECT_EQ(file.reads()[1].offset, 26U);
    EXPECT_EQ(file.reads()[1].len, 16U);

    assert_ok(cache.resolve(PostingStream::kPrx, 34, 4, &prx_scratch, &prx));
    expect_slice_matches(file, prx, 34, 4);
    expect_slice_matches(file, docs, 30, 4);
    EXPECT_EQ(file.reads().size(), 2U);
    EXPECT_EQ(cache.physical_read_ranges(), 2U);
    EXPECT_EQ(cache.physical_read_bytes(), 32U);
    EXPECT_EQ(cache.read_calls(PostingStream::kDocs), 2U);
    EXPECT_EQ(cache.read_calls(PostingStream::kPrx), 0U);
    EXPECT_LE(cache.resident_capacity_bytes(), 32U);
}

TEST(SniiTermCursorRegionReaderTest, SharedCacheUsesExactCrossBlockAndClampsTail) {
    MemoryFile file;
    fill_pattern_file(&file, 200);
    SharedAlignedRegionCache cache(&file, /*region_offset=*/10, /*region_length=*/100,
                                   /*total_budget_bytes=*/32);
    assert_ok(cache.init());
    std::vector<uint8_t> docs_scratch;
    std::vector<uint8_t> prx_scratch;
    Slice docs;
    Slice prx;

    assert_ok(cache.resolve(PostingStream::kDocs, 20, 10, &docs_scratch, &docs));
    expect_slice_matches(file, docs, 20, 10);
    ASSERT_EQ(file.reads().size(), 1U);
    EXPECT_EQ(file.reads()[0].offset, 20U);
    EXPECT_EQ(file.reads()[0].len, 10U);

    assert_ok(cache.resolve(PostingStream::kPrx, 106, 4, &prx_scratch, &prx));
    expect_slice_matches(file, prx, 106, 4);
    expect_slice_matches(file, docs, 20, 10);
    ASSERT_EQ(file.reads().size(), 2U);
    EXPECT_EQ(file.reads()[1].offset, 106U);
    EXPECT_EQ(file.reads()[1].len, 4U);
    EXPECT_EQ(cache.physical_read_bytes(), 14U);
    EXPECT_EQ(cache.physical_read_ranges(), 2U);
}

class FailBootstrapAppendFile final : public io::FileWriter {
public:
    Status append(Slice data) override {
        ++append_calls_;
        if (append_calls_ == 1) {
            if (!data.empty()) ++bytes_written_;
            return Status::Error<ErrorCode::IO_ERROR, false>("injected bootstrap append failure");
        }
        bytes_written_ += data.size();
        return Status::OK();
    }

    Status finalize() override {
        finalized_ = true;
        return Status::OK();
    }

    uint64_t bytes_written() const override { return bytes_written_; }
    size_t append_calls() const { return append_calls_; }
    bool finalized() const { return finalized_; }

private:
    uint64_t bytes_written_ = 0;
    size_t append_calls_ = 0;
    bool finalized_ = false;
};

TEST(SniiStreamedCompactionWriterTest, BootstrapFailurePoisonsCompound) {
    FailBootstrapAppendFile file;
    writer::SniiCompoundWriter compound(&file);
    writer::SniiIndexInput input;
    input.index_id = 7;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 1;

    writer::SniiStreamedIndexSession* session = nullptr;
    const Status first = compound.begin_streamed_index(std::move(input), &session);
    ASSERT_FALSE(first.ok());
    EXPECT_TRUE(first.is<ErrorCode::IO_ERROR>()) << first.to_string();
    EXPECT_EQ(session, nullptr);
    EXPECT_EQ(file.append_calls(), 1U);

    writer::SniiIndexInput retry;
    retry.index_id = 7;
    retry.index_suffix = "body";
    retry.config = format::IndexConfig::kDocsPositions;
    retry.doc_count = 1;
    const Status second = compound.begin_streamed_index(std::move(retry), &session);
    EXPECT_TRUE(second.is<ErrorCode::IO_ERROR>()) << second.to_string();
    EXPECT_EQ(file.append_calls(), 1U);
    EXPECT_TRUE(compound.finish().is<ErrorCode::IO_ERROR>());
    EXPECT_FALSE(file.finalized());
}

// Integration seam: resolve REAL posting windows (from cursor entries) through
// the region reader over the source's posting region -- the exact wiring the
// merge pump uses. Every pod_ref window must resolve to in-region bytes.
TEST(SniiTermCursorRegionReaderTest, ResolvesRealPostingWindowsSequentially) {
    uint32_t doc_count = 0;
    SourceFixture fx;
    assert_ok(build_source(three_encoding_terms(&doc_count), doc_count, &fx));

    const format::RegionRef& region = fx.index.section_refs().posting_region;
    ASSERT_GT(region.length, 0U);
    SequentialRegionReader rr(&fx.file, region.offset, region.length,
                              /*chunk_bytes=*/static_cast<size_t>(region.length));

    SniiSegmentTermCursor cursor(&fx.index, 0);
    bool has_term = false;
    std::vector<uint8_t> scratch;
    size_t pod_windows = 0;
    ASSERT_TRUE(cursor.next(&has_term).ok());
    while (has_term) {
        if (cursor.entry().kind == format::DictEntryKind::kPodRef) {
            uint64_t abs_off = 0;
            uint64_t len = 0;
            assert_ok(
                    fx.index.resolve_frq_window(cursor.entry(), cursor.frq_base(), &abs_off, &len));
            Slice out;
            assert_ok(rr.resolve(abs_off, len, &scratch, &out));
            EXPECT_EQ(out.size(), len);
            ++pod_windows;
        }
        ASSERT_TRUE(cursor.next(&has_term).ok());
    }
    EXPECT_GT(pod_windows, 0U);
    EXPECT_GT(rr.buffer_hits(), 0U); // sequential walk actually amortized reads
}

} // namespace
