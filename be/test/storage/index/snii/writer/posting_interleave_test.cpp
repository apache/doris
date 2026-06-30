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
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/per_index_meta.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"

// Interleaved posting-region read-back validation (docs/design/frqprx-interleave-
// design.md section 10.2). The former separate .frq POD and .prx POD are merged
// into ONE posting region in which each pod_ref term writes [prx span][frq span]
// contiguously, in term order. These tests assert the writer/reader contract for
// that layout: per-term contiguity (frq_off_delta == prx_off_delta + prx_len when
// has_prx), independent delta resolution, byte-correct sub-ranges, docs-only-prefix
// containment inside the frq span, multi-index isolation, empty-index tier recovery
// from the persisted flag, INLINE-between-pod_ref gaplessness, and the R1 docs-only-
// with-pod_ref tier-recovery regression guard.
namespace {

using namespace doris::snii;         // NOLINT
using namespace doris::snii::format; // NOLINT
using namespace doris::snii::writer; // NOLINT
using doris::snii::reader::DecodedPosting;
using doris::snii::reader::LogicalIndexReader;
using doris::snii::reader::SniiSegmentReader;
using doris::snii::reader::read_windowed_posting;

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_posting_interleave_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

TermPostings MakeTerm(const std::string& term, const std::vector<uint32_t>& docs,
                      bool with_positions, uint32_t positions_per_doc) {
    TermPostings tp;
    tp.term = term;
    tp.docids = docs;
    tp.freqs.assign(docs.size(), positions_per_doc);
    if (with_positions) {
        for (size_t i = 0; i < docs.size(); ++i) {
            for (uint32_t p = 0; p < positions_per_doc; ++p) {
                tp.positions_flat.push_back(p); // deterministic ascending positions
            }
        }
    }
    return tp;
}

// Writes one container with one logical index, returns its path. Keeps the
// SniiIndexInput's terms (the writer reads but does not retain them).
std::string WriteOne(const SniiIndexInput& in) {
    const std::string path = TempPath();
    io::LocalFileWriter w;
    EXPECT_TRUE(w.open(path).ok());
    SniiCompoundWriter cw(&w);
    EXPECT_TRUE(cw.add_logical_index(in).ok());
    EXPECT_TRUE(cw.finish().ok());
    return path;
}

// Enumerates every term's DictEntry + owning block frq/prx bases via prefix_terms
// (empty prefix = all terms, in lexicographic order).
std::vector<LogicalIndexReader::PrefixHit> AllEntries(const LogicalIndexReader& idx) {
    std::vector<LogicalIndexReader::PrefixHit> out;
    EXPECT_TRUE(idx.prefix_terms("", &out).ok());
    return out;
}

SniiIndexInput BaseInput(uint64_t id, std::string suffix, IndexConfig cfg) {
    SniiIndexInput in;
    in.index_id = id;
    in.index_suffix = std::move(suffix);
    in.config = cfg;
    in.target_dict_block_bytes = 256; // many small blocks -> exercise multiple bases
    return in;
}

} // namespace

// Test #1: round-trip with positions (interleave correctness). Slim-pod_ref and
// windowed terms (df below and above kSlimDfThreshold). For each pod_ref term:
//   - frq_off_delta == prx_off_delta + prx_len (gated on has_prx);
//   - resolve_frq_window / resolve_prx_window land inside posting_region;
//   - decoded docids/freqs/positions equal the input.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPostingInterleave, RoundTripWithPositionsContiguous) {
    SniiIndexInput in = BaseInput(1, "body", IndexConfig::kDocsPositions);
    const uint32_t doc_count = 4096;
    in.doc_count = doc_count;

    // windowed term (df=2000 >= kSlimDfThreshold=512), slim pod_ref (df=600 forced
    // large enough to exceed the inline threshold via several positions/doc), and an
    // INLINE tiny term -- all positions-bearing.
    std::vector<uint32_t> wide_docs, slim_docs;
    for (uint32_t d = 0; d < 2000; ++d) {
        wide_docs.push_back(d * 2);
    }
    for (uint32_t d = 0; d < 600; ++d) {
        slim_docs.push_back(d * 3 + 1);
    }
    in.terms.push_back(MakeTerm("aa_wide", wide_docs, /*with_positions=*/true, 3));
    in.terms.push_back(MakeTerm("bb_slim", slim_docs, /*with_positions=*/true, 4));
    in.terms.push_back(MakeTerm("cc_tiny", {7, 19, 33}, /*with_positions=*/true, 1));

    const std::string path = WriteOne(in);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    EXPECT_TRUE(idx.has_positions());

    const RegionRef& region = idx.section_refs().posting_region;
    for (const auto& hit : AllEntries(idx)) {
        const DictEntry& e = hit.entry;
        if (e.kind != DictEntryKind::kPodRef) {
            continue; // INLINE term: nothing in region
        }
        // Contiguity property (gated on has_prx): frq span immediately follows prx span.
        EXPECT_EQ(e.frq_off_delta, e.prx_off_delta + e.prx_len) << e.term;

        uint64_t foff = 0, flen = 0, poff = 0, plen = 0;
        ASSERT_TRUE(idx.resolve_frq_window(e, hit.frq_base, &foff, &flen).ok()) << e.term;
        ASSERT_TRUE(idx.resolve_prx_window(e, hit.prx_base, &poff, &plen).ok()) << e.term;
        // Both windows land inside the single posting region.
        EXPECT_GE(foff, region.offset) << e.term;
        EXPECT_LE(foff + flen, region.offset + region.length) << e.term;
        EXPECT_GE(poff, region.offset) << e.term;
        EXPECT_LE(poff + plen, region.offset + region.length) << e.term;
    }

    // Docids round-trip for every term.
    for (const auto& [term, expect] : std::vector<std::pair<std::string, std::vector<uint32_t>>> {
                 {"aa_wide", wide_docs}, {"bb_slim", slim_docs}, {"cc_tiny", {7, 19, 33}}}) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::term_query(idx, term, &got).ok()) << term;
        EXPECT_EQ(got, expect) << term;
    }

    // Positions round-trip for the windowed term via the full decode path.
    bool found = false;
    DictEntry wide_e;
    uint64_t fb = 0, pb = 0;
    ASSERT_TRUE(idx.lookup("aa_wide", &found, &wide_e, &fb, &pb).ok());
    ASSERT_TRUE(found);
    DecodedPosting dp;
    ASSERT_TRUE(read_windowed_posting(idx, wide_e, fb, pb, /*want_positions=*/true,
                                      /*want_freq=*/true, &dp)
                        .ok());
    ASSERT_EQ(dp.docids.size(), wide_docs.size());
    EXPECT_EQ(dp.docids, wide_docs);
    ASSERT_EQ(dp.positions.size(), wide_docs.size());
    for (const auto& pv : dp.positions) {
        ASSERT_EQ(pv.size(), 3U); // positions_per_doc
        EXPECT_EQ(pv[0], 0U);
        EXPECT_EQ(pv[1], 1U);
        EXPECT_EQ(pv[2], 2U);
    }
    std::remove(path.c_str());
}

// Test #2 / #3: byte-correctness of the combined region. Read the raw posting
// region bytes; assert each term's [prx_off_delta, +prx_len) and
// [frq_off_delta, +frq_len) sub-ranges, and that the docs-only prefix
// [frq_off_delta, +frq_docs_len) stays INSIDE the frq span (never reads prx).
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPostingInterleave, ByteCorrectnessAndDocsPrefixStaysInFrqSpan) {
    SniiIndexInput in = BaseInput(1, "body", IndexConfig::kDocsPositions);
    in.doc_count = 4096;
    std::vector<uint32_t> wide_docs;
    for (uint32_t d = 0; d < 1500; ++d) {
        wide_docs.push_back(d);
    }
    in.terms.push_back(MakeTerm("aa_wide", wide_docs, /*with_positions=*/true, 2));
    std::vector<uint32_t> slim_docs;
    for (uint32_t d = 0; d < 600; ++d) {
        slim_docs.push_back(d + 5);
    }
    in.terms.push_back(MakeTerm("bb_slim", slim_docs, /*with_positions=*/true, 3));

    const std::string path = WriteOne(in);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    const RegionRef& region = idx.section_refs().posting_region;
    std::vector<uint8_t> region_bytes;
    ASSERT_TRUE(local.read_at(region.offset, region.length, &region_bytes).ok());

    for (const auto& hit : AllEntries(idx)) {
        const DictEntry& e = hit.entry;
        if (e.kind != DictEntryKind::kPodRef) {
            continue;
        }
        // prx span precedes frq span; both lie inside the region (relative to base).
        const uint64_t prx_in = hit.prx_base + e.prx_off_delta;
        const uint64_t frq_in = hit.frq_base + e.frq_off_delta;
        ASSERT_LE(prx_in + e.prx_len, region.length) << e.term;
        ASSERT_LE(frq_in + e.frq_len, region.length) << e.term;
        // prx span ends exactly where frq span begins (contiguous, prx first).
        EXPECT_EQ(prx_in + e.prx_len, frq_in) << e.term;

        // The docs-only prefix [frq_off_delta, +frq_docs_len) stays inside the frq span
        // (begins after the prx span and fits within frq_len) -- it never reads prx.
        EXPECT_GE(frq_in, prx_in + e.prx_len) << e.term; // prefix begins after prx
        EXPECT_LE(e.frq_docs_len, e.frq_len) << e.term;  // prefix fits in frq span

        // The absolute frq span stays inside the posting region (reader reads exactly
        // what the writer wrote).
        const uint64_t frq_span_abs = region.offset + frq_in;
        EXPECT_LE(frq_span_abs + e.frq_len, region.offset + region.length) << e.term;
    }
    std::remove(path.c_str());
}

// Test #4: multi-index. One docs-positions index and one docs-only index in one
// container; each posting_region placement is independent (no offset bleed) and
// both resolve correctly.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPostingInterleave, MultiIndexIndependentPlacements) {
    SniiIndexInput a = BaseInput(1, "body", IndexConfig::kDocsPositions);
    a.doc_count = 2048;
    std::vector<uint32_t> a_docs;
    for (uint32_t d = 0; d < 1000; ++d) {
        a_docs.push_back(d);
    }
    a.terms.push_back(MakeTerm("aa_wide", a_docs, /*with_positions=*/true, 2));

    SniiIndexInput b = BaseInput(2, "tag", IndexConfig::kDocsOnly);
    b.doc_count = 2048;
    std::vector<uint32_t> b_docs;
    for (uint32_t d = 0; d < 800; ++d) {
        b_docs.push_back(d * 2);
    }
    b.terms.push_back(MakeTerm("kk_keyword", b_docs, /*with_positions=*/false, 1));

    const std::string path = TempPath();
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(a).ok());
        ASSERT_TRUE(cw.add_logical_index(b).ok());
        ASSERT_TRUE(cw.finish().ok());
    }

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &seg).ok());

    LogicalIndexReader ia, ib;
    ASSERT_TRUE(seg.open_index(1, "body", &ia).ok());
    ASSERT_TRUE(seg.open_index(2, "tag", &ib).ok());
    EXPECT_TRUE(ia.has_positions());
    EXPECT_FALSE(ib.has_positions()); // docs-only recovered from the flag
    EXPECT_EQ(ia.tier(), IndexTier::kT2);
    EXPECT_EQ(ib.tier(), IndexTier::kT1);

    const RegionRef ra = ia.section_refs().posting_region;
    const RegionRef rb = ib.section_refs().posting_region;
    // Non-overlapping regions (independent placement; no cross-index bleed).
    const bool disjoint =
            (ra.offset + ra.length <= rb.offset) || (rb.offset + rb.length <= ra.offset);
    EXPECT_TRUE(disjoint) << "ra=[" << ra.offset << "," << ra.length << "] rb=[" << rb.offset << ","
                          << rb.length << "]";

    std::vector<uint32_t> got_a, got_b;
    ASSERT_TRUE(query::term_query(ia, "aa_wide", &got_a).ok());
    ASSERT_TRUE(query::term_query(ib, "kk_keyword", &got_b).ok());
    EXPECT_EQ(got_a, a_docs);
    EXPECT_EQ(got_b, b_docs);
    std::remove(path.c_str());
}

// Test #5: no-positions index -- the posting region holds only frq spans;
// prx_off_delta / prx_len are unset; the bytes are identical to today's .frq POD
// for the same input (a docs-only index's posting_region IS the frq concatenation,
// since the prx span is always empty). We verify prx spans are absent and the
// posting region tiles exactly the per-term frq spans with no gaps/prx.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPostingInterleave, NoPositionsRegionIsFrqOnly) {
    SniiIndexInput in = BaseInput(1, "tag", IndexConfig::kDocsOnly);
    in.doc_count = 4096;
    std::vector<uint32_t> wide_docs, slim_docs;
    for (uint32_t d = 0; d < 1500; ++d) {
        wide_docs.push_back(d);
    }
    for (uint32_t d = 0; d < 600; ++d) {
        slim_docs.push_back(d + 2);
    }
    in.terms.push_back(MakeTerm("aa_wide", wide_docs, /*with_positions=*/false, 1));
    in.terms.push_back(MakeTerm("bb_slim", slim_docs, /*with_positions=*/false, 1));

    const std::string path = WriteOne(in);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "tag", &idx).ok());
    EXPECT_FALSE(idx.has_positions());
    EXPECT_EQ(idx.tier(), IndexTier::kT1);

    const RegionRef region = idx.section_refs().posting_region;
    uint64_t frq_bytes_sum = 0;
    for (const auto& hit : AllEntries(idx)) {
        const DictEntry& e = hit.entry;
        if (e.kind != DictEntryKind::kPodRef) {
            continue;
        }
        EXPECT_EQ(e.prx_len, 0U) << e.term; // no prx span at all
        EXPECT_EQ(e.prx_off_delta, 0U) << e.term;
        // frq span is the whole per-term posting (no preceding prx span).
        EXPECT_EQ(hit.frq_base + e.frq_off_delta, frq_bytes_sum) << e.term;
        frq_bytes_sum += e.frq_len;
    }
    // The frq spans tile the region exactly: no prx bytes, no gaps.
    EXPECT_EQ(frq_bytes_sum, region.length);

    std::vector<uint32_t> got;
    ASSERT_TRUE(query::term_query(idx, "aa_wide", &got).ok());
    EXPECT_EQ(got, wide_docs);
    std::remove(path.c_str());
}

// Test #6: empty term set. posting_region.length == 0 and dict_region.length == 0;
// the reader opens cleanly and lookups miss. Built BOTH docs-only and docs-positions
// to assert tier is recovered correctly from the flag despite both having a
// zero-length posting region.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPostingInterleave, EmptyTermSetTierFromFlag) {
    for (IndexConfig cfg : {IndexConfig::kDocsOnly, IndexConfig::kDocsPositions}) {
        SniiIndexInput in = BaseInput(1, "body", cfg);
        in.doc_count = 16; // docs exist, but no terms
        const std::string path = WriteOne(in);

        io::LocalFileReader local;
        ASSERT_TRUE(local.open(path).ok());
        SniiSegmentReader seg;
        ASSERT_TRUE(SniiSegmentReader::open(&local, &seg).ok());
        LogicalIndexReader idx;
        ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

        EXPECT_EQ(idx.section_refs().posting_region.length, 0U);
        EXPECT_EQ(idx.section_refs().dict_region.length, 0U);
        const bool want_positions = (cfg != IndexConfig::kDocsOnly);
        EXPECT_EQ(idx.has_positions(), want_positions);
        EXPECT_EQ(idx.tier(), want_positions ? IndexTier::kT2 : IndexTier::kT1);

        std::vector<uint32_t> got;
        ASSERT_TRUE(query::term_query(idx, "anything", &got).ok());
        EXPECT_TRUE(got.empty());
        std::remove(path.c_str());
    }
}

// Test #8: INLINE-between-pod_ref. Mix tiny (INLINE) and large (pod_ref) terms;
// INLINE terms append nothing to the region (region length == sum of pod_ref
// spans) and pod_ref deltas stay contiguous across the interleaving.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPostingInterleave, InlineBetweenPodRefIsGapless) {
    SniiIndexInput in = BaseInput(1, "body", IndexConfig::kDocsPositions);
    in.doc_count = 4096;
    std::vector<uint32_t> wide1, wide2;
    for (uint32_t d = 0; d < 1500; ++d) {
        wide1.push_back(d);
    }
    for (uint32_t d = 0; d < 1200; ++d) {
        wide2.push_back(d + 1);
    }
    // Lexicographic order interleaves tiny INLINE terms between the two windowed
    // pod_ref terms: aa_big < bb_tiny < cc_big < dd_tiny.
    in.terms.push_back(MakeTerm("aa_big", wide1, /*with_positions=*/true, 2));
    in.terms.push_back(MakeTerm("bb_tiny", {3, 8}, /*with_positions=*/true, 1));
    in.terms.push_back(MakeTerm("cc_big", wide2, /*with_positions=*/true, 2));
    in.terms.push_back(MakeTerm("dd_tiny", {1, 99}, /*with_positions=*/true, 1));

    const std::string path = WriteOne(in);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    const RegionRef region = idx.section_refs().posting_region;
    uint64_t pod_span_sum = 0;
    uint64_t cursor = 0; // running region offset of the next pod_ref term's span
    for (const auto& hit : AllEntries(idx)) {
        const DictEntry& e = hit.entry;
        if (e.kind == DictEntryKind::kInline) {
            EXPECT_EQ(e.prx_len, 0U) << e.term; // inline carries bytes in the entry
            EXPECT_EQ(e.frq_len, 0U) << e.term;
            continue; // appends nothing to the region
        }
        // pod_ref term's prx span begins exactly where the previous span ended (no gap
        // introduced by the interleaved INLINE terms).
        const uint64_t prx_in = hit.prx_base + e.prx_off_delta;
        EXPECT_EQ(prx_in, cursor) << e.term;
        const uint64_t term_span = e.prx_len + e.frq_len;
        cursor += term_span;
        pod_span_sum += term_span;
    }
    // Region length is exactly the sum of pod_ref spans (INLINE adds nothing).
    EXPECT_EQ(pod_span_sum, region.length);

    // All terms still resolve.
    for (const auto& [term, expect] : std::vector<std::pair<std::string, std::vector<uint32_t>>> {
                 {"aa_big", wide1}, {"bb_tiny", {3, 8}}, {"cc_big", wide2}, {"dd_tiny", {1, 99}}}) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::term_query(idx, term, &got).ok()) << term;
        EXPECT_EQ(got, expect) << term;
    }
    std::remove(path.c_str());
}

// Test #9: tier recovery for docs-only-with-pod_ref (R1 regression guard). A
// docs-only (T1) index containing a windowed pod_ref term so posting_region.length
// > 0. open_index must recover tier == kT1 / has_positions == false (NOT inferred
// from the non-zero region length), and a lookup + docid decode must succeed with
// NO spurious positions-flag parse (DictBlockReader::open does not InvalidArgument).
TEST(SniiPostingInterleave, DocsOnlyWithPodRefTierRecovery) {
    SniiIndexInput in = BaseInput(1, "tag", IndexConfig::kDocsOnly);
    in.doc_count = 4096;
    std::vector<uint32_t> wide_docs;
    for (uint32_t d = 0; d < 2000; ++d) {
        wide_docs.push_back(d); // windowed pod_ref
    }
    in.terms.push_back(MakeTerm("kk_keyword", wide_docs, /*with_positions=*/false, 1));

    const std::string path = WriteOne(in);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &seg).ok());
    LogicalIndexReader idx;
    // This open would FAIL under the naive posting_region.length > 0 heuristic:
    // has_positions would be wrongly true, DictBlockReader::open -> check_flags would
    // hard-fail with InvalidArgument. With the persisted flag it opens cleanly.
    ASSERT_TRUE(seg.open_index(1, "tag", &idx).ok());
    EXPECT_EQ(idx.tier(), IndexTier::kT1);
    EXPECT_FALSE(idx.has_positions());
    EXPECT_GT(idx.section_refs().posting_region.length, 0U); // non-empty region

    // Lookup + docid decode succeeds (no spurious positions parse).
    bool found = false;
    DictEntry e;
    uint64_t fb = 0, pb = 0;
    ASSERT_TRUE(idx.lookup("kk_keyword", &found, &e, &fb, &pb).ok());
    ASSERT_TRUE(found);
    EXPECT_EQ(e.kind, DictEntryKind::kPodRef);
    EXPECT_EQ(e.prx_len, 0U); // docs-only: no prx span
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::term_query(idx, "kk_keyword", &got).ok());
    EXPECT_EQ(got, wide_docs);
    std::remove(path.c_str());
}

// M1 (code-review blocker): the SLIM pod_ref path is the ONLY write path whose
// physical byte order flipped to [prx][frq], yet no prior test exercised it -- every
// "slim" term elsewhere is df>=512 (windowed) or tiny (inline). Build a genuine
// kSlim/kPodRef term (df<512 so build_slim_entry runs; docids spread so the PFOR'd
// frq exceeds the 256B inline threshold so it is kPodRef not kInline), lock the
// routing, assert the interleave invariant, and round-trip docids (frq span) AND
// positions (prx span, decoded via the slim phrase path).
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPostingInterleave, SlimPodRefInterleaveAndPositionsRoundTrip) {
    SniiIndexInput in = BaseInput(1, "body", IndexConfig::kDocsPositions);
    in.doc_count = 30000;
    std::vector<uint32_t> docs;
    for (uint32_t d = 0; d < 500; ++d) {
        docs.push_back(d * 50 + 1); // df=500 (<512), spread
    }
    in.terms.push_back(MakeTerm("ss_aaa", docs, /*with_positions=*/true, 4));
    in.terms.push_back(MakeTerm("ss_bbb", docs, /*with_positions=*/true, 4));

    const std::string path = WriteOne(in);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    ASSERT_TRUE(idx.has_positions());

    bool found = false;
    DictEntry e;
    uint64_t fb = 0, pb = 0;
    ASSERT_TRUE(idx.lookup("ss_aaa", &found, &e, &fb, &pb).ok());
    ASSERT_TRUE(found);
    // Lock the routing onto the slim pod_ref path (the one whose order flipped).
    ASSERT_EQ(e.kind, DictEntryKind::kPodRef) << "frq must exceed the inline threshold";
    ASSERT_EQ(e.enc, DictEntryEnc::kSlim) << "df must be below kSlimDfThreshold";

    // Interleave invariant + adjacency: the prx span ends exactly where the frq begins.
    EXPECT_EQ(e.frq_off_delta, e.prx_off_delta + e.prx_len);
    const RegionRef& region = idx.section_refs().posting_region;
    uint64_t foff = 0, flen = 0, poff = 0, plen = 0;
    ASSERT_TRUE(idx.resolve_frq_window(e, fb, &foff, &flen).ok());
    ASSERT_TRUE(idx.resolve_prx_window(e, pb, &poff, &plen).ok());
    EXPECT_GE(poff, region.offset);
    EXPECT_LE(foff + flen, region.offset + region.length);
    EXPECT_EQ(poff + plen, foff) << "prx span must abut the frq span ([prx][frq])";

    // Docids round-trip via term_query: decodes dd from the FRQ span -> proves the frq
    // offset lands after the prx in the [prx][frq] layout.
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::term_query(idx, "ss_aaa", &got).ok());
    EXPECT_EQ(got, docs);

    // Positions round-trip via the SLIM read path: both terms sit at positions {0,1,2,3}
    // in every shared doc, so "ss_aaa ss_bbb" matches (ss_aaa@0, ss_bbb@1) iff each
    // term's prx window decodes correctly from its prx span.
    std::vector<uint32_t> phr;
    ASSERT_TRUE(query::phrase_query(idx, {"ss_aaa", "ss_bbb"}, &phr).ok());
    EXPECT_EQ(phr, docs);
    std::remove(path.c_str());
}
