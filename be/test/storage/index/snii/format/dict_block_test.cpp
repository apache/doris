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

#include "storage/index/snii/format/dict_block.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"

using namespace doris::snii;         // NOLINT
using namespace doris::snii::format; // NOLINT
using doris::Status;

namespace {

// Construct a pod_ref slim dict entry. frq/prx off_delta is already relative to the block base.
DictEntry MakePodRef(std::string term, uint32_t df, uint64_t frq_off, uint64_t prx_off = 0) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kPodRef;
    e.enc = DictEntryEnc::kSlim;
    e.df = df;
    e.ttf_delta = df * 2; // written only when tier>=T2
    e.max_freq = 9;       // written only when tier>=T2
    e.frq_off_delta = frq_off;
    e.frq_len = 128;
    e.prx_off_delta = prx_off; // written only when positions are enabled
    e.prx_len = 64;            // written only when positions are enabled
    return e;
}

DictEntry MakeInline(std::string term, uint32_t df) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kInline;
    e.enc = DictEntryEnc::kSlim;
    e.df = df;
    e.frq_bytes = {0x10, 0x20, 0x30};
    return e;
}

void ExpectCommon(const DictEntry& a, const DictEntry& b) {
    EXPECT_EQ(a.term, b.term);
    EXPECT_EQ(a.kind, b.kind);
    EXPECT_EQ(a.enc, b.enc);
    EXPECT_EQ(a.df, b.df);
}

// Serialize a set of entries into one block using the builder; return the byte buffer.
std::vector<uint8_t> BuildBlock(const std::vector<DictEntry>& entries, IndexTier tier,
                                bool has_positions, uint64_t frq_base, uint64_t prx_base,
                                uint32_t anchor_interval) {
    DictBlockBuilder builder(tier, has_positions, frq_base, prx_base, anchor_interval);
    for (const auto& e : entries) {
        builder.add_entry(e);
    }
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

} // namespace

// Empty block: n_entries=0, open should still succeed, find_term on any target returns not-found.
TEST(SniiDictBlock, EmptyBlock) {
    std::vector<uint8_t> bytes =
            BuildBlock({}, IndexTier::kT1, /*has_positions=*/false, 1000, 0, 16);
    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(bytes), IndexTier::kT1,
                                      /*has_positions=*/false, &reader)
                        .ok());
    EXPECT_EQ(reader.n_entries(), 0U);
    EXPECT_EQ(reader.frq_base(), 1000U);

    bool found = true;
    DictEntry out;
    ASSERT_TRUE(reader.find_term("anything", &found, &out).ok());
    EXPECT_FALSE(found);
}

// Single-entry round-trip.
TEST(SniiDictBlock, SingleEntryRoundTrip) {
    DictEntry e = MakePodRef("solo", 7, 0);
    std::vector<uint8_t> bytes = BuildBlock({e}, IndexTier::kT1, false, 4096, 0, 16);
    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader).ok());
    EXPECT_EQ(reader.n_entries(), 1U);
    EXPECT_EQ(reader.frq_base(), 4096U);

    bool found = false;
    DictEntry out;
    ASSERT_TRUE(reader.find_term("solo", &found, &out).ok());
    ASSERT_TRUE(found);
    ExpectCommon(e, out);
    EXPECT_EQ(out.frq_off_delta, e.frq_off_delta);
    EXPECT_EQ(out.frq_len, e.frq_len);
}

// Multi-entry round-trip: all terms found, fields preserved.
TEST(SniiDictBlock, MultiEntryRoundTrip) {
    std::vector<DictEntry> entries = {MakePodRef("alpha", 3, 0), MakePodRef("beta", 5, 100),
                                      MakeInline("gamma", 2), MakePodRef("delta", 9, 300),
                                      MakePodRef("epsilon", 11, 500)};
    // delta < gamma lexicographically, so reorder entries to be sorted.
    entries = {MakePodRef("alpha", 3, 0), MakePodRef("beta", 5, 100), MakePodRef("delta", 9, 300),
               MakePodRef("epsilon", 11, 500), MakeInline("gamma", 2)};
    std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT2, true, 8192, 16384, 16);
    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(bytes), IndexTier::kT2, true, &reader).ok());
    EXPECT_EQ(reader.n_entries(), entries.size());

    for (const auto& e : entries) {
        bool found = false;
        DictEntry out;
        ASSERT_TRUE(reader.find_term(e.term, &found, &out).ok()) << e.term;
        ASSERT_TRUE(found) << e.term;
        ExpectCommon(e, out);
    }
}

TEST(SniiDictBlock, DecodeAll) {
    std::vector<DictEntry> entries = {MakePodRef("alpha", 3, 0), MakePodRef("beta", 5, 100),
                                      MakePodRef("delta", 9, 300), MakePodRef("epsilon", 11, 500),
                                      MakeInline("gamma", 2)};
    std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT2, true, 8192, 16384, 16);
    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(bytes), IndexTier::kT2, true, &reader).ok());

    // Null output is rejected, not dereferenced.
    EXPECT_FALSE(reader.decode_all(nullptr).ok());

    // decode_all returns every entry, in ascending order, matching n_entries.
    std::vector<DictEntry> all;
    ASSERT_TRUE(reader.decode_all(&all).ok());
    ASSERT_EQ(all.size(), entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(all[i].term, entries[i].term) << i;
        ExpectCommon(entries[i], all[i]);
    }
}

// Front-coding across anchors: with a small anchor_interval, terms spanning anchor boundaries are decoded correctly.
TEST(SniiDictBlock, PrefixCompressionAcrossAnchors) {
    std::vector<DictEntry> entries;
    // 21 sorted terms sharing a long common prefix, anchor_interval=4 -> multiple anchors.
    std::vector<std::string> terms = {"interest",  "interested",  "interesting", "interestingly",
                                      "interests", "internal",    "internally",  "international",
                                      "internet",  "internets",   "interplay",   "interpose",
                                      "interpret", "interpreted", "interval",    "intervene",
                                      "interview", "interviewed", "intestine",   "intimate",
                                      "intricate"};
    uint64_t off = 0;
    for (const auto& t : terms) {
        entries.push_back(MakePodRef(t, 4, off));
        off += 50;
    }
    std::vector<uint8_t> bytes =
            BuildBlock(entries, IndexTier::kT1, false, 0, 0, /*anchor_interval=*/4);
    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader).ok());
    EXPECT_EQ(reader.n_entries(), terms.size());

    for (size_t i = 0; i < terms.size(); ++i) {
        bool found = false;
        DictEntry out;
        ASSERT_TRUE(reader.find_term(terms[i], &found, &out).ok()) << terms[i];
        ASSERT_TRUE(found) << terms[i];
        EXPECT_EQ(out.term, terms[i]);
        EXPECT_EQ(out.frq_off_delta, static_cast<uint64_t>(i * 50));
    }
}

// find_term boundaries: less than first term, greater than last term, exactly an anchor term.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiDictBlock, FindTermBoundaries) {
    std::vector<std::string> terms;
    for (int i = 0; i < 40; ++i) {
        char buf[8];
        snprintf(buf, sizeof(buf), "t%02d", i);
        terms.emplace_back(buf);
    }
    std::vector<DictEntry> entries;
    for (size_t i = 0; i < terms.size(); ++i) {
        entries.push_back(MakePodRef(terms[i], 1, i * 10));
    }
    std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 0, 0, 8);
    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader).ok());

    // Less than the first term.
    {
        bool found = true;
        DictEntry out;
        ASSERT_TRUE(reader.find_term("\x01", &found, &out).ok());
        EXPECT_FALSE(found);
    }
    // Greater than the last term.
    {
        bool found = true;
        DictEntry out;
        ASSERT_TRUE(reader.find_term("zzzz", &found, &out).ok());
        EXPECT_FALSE(found);
    }
    // Hit terms that are exactly at anchor positions (anchor_interval=8 -> t08, t16, t24 ...).
    for (const char* t : {"t00", "t08", "t16", "t24", "t32"}) {
        bool found = false;
        DictEntry out;
        ASSERT_TRUE(reader.find_term(t, &found, &out).ok()) << t;
        ASSERT_TRUE(found) << t;
        EXPECT_EQ(out.term, t);
    }
    // Hit terms that are NOT at anchor positions.
    for (const char* t : {"t03", "t11", "t27"}) {
        bool found = false;
        DictEntry out;
        ASSERT_TRUE(reader.find_term(t, &found, &out).ok()) << t;
        ASSERT_TRUE(found) << t;
        EXPECT_EQ(out.term, t);
    }
    // A key within the term range that does not exist (gap).
    {
        bool found = true;
        DictEntry out;
        ASSERT_TRUE(reader.find_term("t05x", &found, &out).ok());
        EXPECT_FALSE(found);
    }
}

// CRC corruption detection: flipping any byte must cause open() to report Corruption.
TEST(SniiDictBlock, CrcCorruptionDetected) {
    std::vector<DictEntry> entries = {MakePodRef("aaa", 1, 0), MakePodRef("bbb", 2, 50),
                                      MakePodRef("ccc", 3, 100)};
    std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT2, true, 100, 200, 16);
    // Flip one byte in the middle of the entries region.
    bytes[bytes.size() / 2] ^= 0xFF;
    DictBlockReader reader;
    Status s = DictBlockReader::open(Slice(bytes), IndexTier::kT2, true, &reader);
    EXPECT_FALSE(s.ok());
    EXPECT_FALSE(s.ok());
}

// A truncated block (shorter than the CRC footer) must report Corruption rather than crash with an out-of-range access.
TEST(SniiDictBlock, TruncatedBlockDetected) {
    std::vector<DictEntry> entries = {MakePodRef("xxx", 1, 0)};
    std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 0, 0, 16);
    bytes.resize(2); // Only a few bytes of the header remain.
    DictBlockReader reader;
    Status s = DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader);
    EXPECT_FALSE(s.ok());
}

// Positions mode: prx_base and prx fields are preserved correctly.
TEST(SniiDictBlock, PositionsPrxRoundTrip) {
    std::vector<DictEntry> entries = {MakePodRef("phrase", 5, 0, 0),
                                      MakePodRef("query", 6, 200, 80)};
    std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT2, true, 7000, 9000, 16);
    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(bytes), IndexTier::kT2, true, &reader).ok());
    EXPECT_EQ(reader.prx_base(), 9000U);

    bool found = false;
    DictEntry out;
    ASSERT_TRUE(reader.find_term("query", &found, &out).ok());
    ASSERT_TRUE(found);
    EXPECT_EQ(out.prx_off_delta, 80U);
    EXPECT_EQ(out.prx_len, 64U);
    EXPECT_EQ(out.ttf_delta, 12U);
}

// estimated_bytes is monotonically non-decreasing: grows after each add_entry, and the actual byte
// count after finish() does not exceed the estimate (the estimate is an upper bound for block-splitting decisions).
TEST(SniiDictBlock, EstimatedBytesMonotonic) {
    DictBlockBuilder builder(IndexTier::kT1, false, 0, 0, 16);
    size_t prev = builder.estimated_bytes();
    std::vector<std::string> terms = {"aa", "bb", "cc", "dd"};
    for (const auto& t : terms) {
        builder.add_entry(MakePodRef(t, 1, 0));
        size_t now = builder.estimated_bytes();
        EXPECT_GE(now, prev);
        prev = now;
    }
    ByteSink sink;
    builder.finish(&sink);
    EXPECT_LE(sink.size(), builder.estimated_bytes());
}

// Security regression: anchor offsets must be strictly increasing with the first
// anchor at the entries start. A tampered anchor table (offsets swapped) whose crc
// is re-stamped must be rejected by open(); otherwise scan_from_anchor would
// underflow seg_end-seg_begin and read out of bounds (found via ASAN/UBSAN review).
TEST(SniiDictBlock, RejectsNonMonotonicAnchorOffsets) {
    std::vector<DictEntry> entries = {MakePodRef("aaa", 1, 0), MakePodRef("bbb", 2, 10),
                                      MakePodRef("ccc", 3, 20)};
    // anchor_interval = 1 -> every entry is an anchor (3 anchors).
    std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 100, 0, 1);
    const size_t n = bytes.size();
    ASSERT_GT(n, 20U);
    // Tail layout: [...][anchor_offsets: 3 * u32][n_anchors u32][crc32c u32].
    const size_t off1 = n - 4 /*crc*/ - 4 /*n_anchors*/ - 4 /*offset[2]*/ - 4 /*offset[1]*/;
    const size_t off2 = off1 + 4;
    for (int k = 0; k < 4; ++k) {
        uint8_t tmp = bytes[off1 + k];
        bytes[off1 + k] = bytes[off2 + k];
        bytes[off2 + k] = tmp;
    }
    // Re-stamp crc32c over the covered region [0, n-4) so corruption is structural.
    uint32_t crc = doris::snii::crc32c(Slice(bytes.data(), n - 4));
    for (int k = 0; k < 4; ++k) {
        bytes[n - 4 + k] = static_cast<uint8_t>(crc >> (8 * k));
    }

    DictBlockReader reader;
    Status s = DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader);
    EXPECT_FALSE(s.ok());
}

// ===========================================================================
// T16: DictBlockBuilder move-entry overload + dead prev_term_ removal.
// The move overload must be byte-for-byte equivalent to the const-ref (copy)
// overload, must actually transfer (not copy) the entry, and removing the dead
// prev_term_ member must leave finish() output unchanged. These live in the
// SniiDictBlockTest suite per the T16 plan.
// ===========================================================================
namespace {

// Inline entry carrying BOTH non-empty frq_bytes and prx_bytes, so the move path
// has real heap-allocated vectors (and a heap term) to transfer -- exercised by
// the byte-equivalence and moved-from tests below.
DictEntry MakeInlineWithPrx(std::string term, uint32_t df) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kInline;
    e.enc = DictEntryEnc::kSlim;
    e.df = df;
    e.ttf_delta = df * 3; // written only when tier>=T2
    e.max_freq = 7;       // written only when tier>=T2
    e.frq_bytes = {0x01, 0x02, 0x03, 0x04, 0x05};
    e.prx_bytes = {0xAA, 0xBB, 0xCC}; // written only when positions are enabled
    return e;
}

// Build a block by MOVING each entry into the builder. The caller's vector stays
// intact because each entry is copied once and only the copy is moved in (so the
// returned bytes can be compared against the const-ref BuildBlock path).
std::vector<uint8_t> BuildBlockMoved(const std::vector<DictEntry>& entries, IndexTier tier,
                                     bool has_positions, uint64_t frq_base, uint64_t prx_base,
                                     uint32_t anchor_interval) {
    DictBlockBuilder builder(tier, has_positions, frq_base, prx_base, anchor_interval);
    for (const auto& e : entries) {
        DictEntry tmp = e; // copy, then move the copy into the builder
        builder.add_entry(std::move(tmp));
    }
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

// A mixed, sorted entry set that crosses the default anchor_interval (16): 18
// entries, inline-with-prx at a few indices, pod_ref elsewhere. Zero-padded term
// names keep lexicographic order == construction order.
std::vector<DictEntry> MakeMixedCrossAnchorEntries() {
    std::vector<DictEntry> entries;
    for (int i = 0; i < 18; ++i) {
        char buf[16];
        snprintf(buf, sizeof(buf), "term%02d", i);
        if (i == 3 || i == 8 || i == 14) {
            entries.push_back(MakeInlineWithPrx(buf, static_cast<uint32_t>(i + 1)));
        } else {
            entries.push_back(MakePodRef(buf, static_cast<uint32_t>(i + 1),
                                         static_cast<uint64_t>(i) * 64,
                                         static_cast<uint64_t>(i) * 32));
        }
    }
    return entries;
}

} // namespace

// FC-1 / RED-1: the move overload produces byte-identical block output to the
// const-ref (copy) overload, and the size estimate matches -- the latter only
// holds if estimate_entry_bytes runs BEFORE the move (estimating a moved-from
// entry would undercount entries_est_ and diverge here).
TEST(SniiDictBlockTest, MoveAddProducesByteIdenticalOutput) {
    const std::vector<DictEntry> entries = MakeMixedCrossAnchorEntries();

    DictBlockBuilder builder_a(IndexTier::kT2, /*has_positions=*/true, 8192, 16384, 16);
    DictBlockBuilder builder_b(IndexTier::kT2, /*has_positions=*/true, 8192, 16384, 16);
    for (const auto& e : entries) {
        builder_a.add_entry(e); // const-ref copy path
        DictEntry tmp = e;
        builder_b.add_entry(std::move(tmp)); // move path
    }

    // estimate-before-move guard: undercounting on the move path would show here.
    EXPECT_EQ(builder_a.estimated_bytes(), builder_b.estimated_bytes());
    EXPECT_EQ(builder_a.n_entries(), builder_b.n_entries());

    ByteSink sink_a;
    ByteSink sink_b;
    builder_a.finish(&sink_a);
    builder_b.finish(&sink_b);
    EXPECT_EQ(sink_a.buffer(), sink_b.buffer()); // on-disk bytes identical
}

// FC-2 / RED-2: add_entry(DictEntry&&) actually moves -- the source entry's term
// and both byte vectors are left empty (moved-from). A heap-sized term (beyond
// SSO) makes "the buffer was stolen" observable regardless of small-string opt.
TEST(SniiDictBlockTest, MoveAddLeavesSourceMovedFrom) {
    DictEntry e = MakeInlineWithPrx("a-deliberately-long-term-well-beyond-sso", 5);
    ASSERT_FALSE(e.term.empty());
    ASSERT_FALSE(e.frq_bytes.empty());
    ASSERT_FALSE(e.prx_bytes.empty());

    DictBlockBuilder builder(IndexTier::kT2, /*has_positions=*/true, 0, 0, 16);
    builder.add_entry(std::move(e));

    EXPECT_TRUE(
            e.term.empty()); // NOLINT(bugprone-use-after-move): intentionally inspects moved-from state
    EXPECT_TRUE(e.frq_bytes.empty()); // NOLINT(bugprone-use-after-move)
    EXPECT_TRUE(e.prx_bytes.empty()); // NOLINT(bugprone-use-after-move)
    EXPECT_EQ(builder.n_entries(), 1U);
}

// FC-3: single-entry block built via the move overload round-trips through
// open()/find_term() (open succeeding also verifies the block CRC).
TEST(SniiDictBlockTest, MoveAddSingleEntryRoundTrip) {
    DictEntry e = MakePodRef("solo", 7, 0);
    const DictEntry expected = e; // pristine copy for comparison

    DictBlockBuilder builder(IndexTier::kT1, /*has_positions=*/false, 4096, 0, 16);
    builder.add_entry(std::move(e));
    ByteSink sink;
    builder.finish(&sink);

    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(sink.buffer()), IndexTier::kT1,
                                      /*has_positions=*/false, &reader)
                        .ok());
    EXPECT_EQ(reader.n_entries(), 1U);
    EXPECT_EQ(reader.frq_base(), 4096U);

    bool found = false;
    DictEntry out;
    ASSERT_TRUE(reader.find_term("solo", &found, &out).ok());
    ASSERT_TRUE(found);
    ExpectCommon(expected, out);
    EXPECT_EQ(out.frq_off_delta, expected.frq_off_delta);
    EXPECT_EQ(out.frq_len, expected.frq_len);
}

// FC-4: empty-block boundary still serializes and opens cleanly (the move
// overload is irrelevant when nothing is added, but the boundary must hold).
TEST(SniiDictBlockTest, MoveAddEmptyBlock) {
    DictBlockBuilder builder(IndexTier::kT1, /*has_positions=*/false, 1000, 0, 16);
    ByteSink sink;
    builder.finish(&sink);

    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(sink.buffer()), IndexTier::kT1,
                                      /*has_positions=*/false, &reader)
                        .ok());
    EXPECT_EQ(reader.n_entries(), 0U);

    bool found = true;
    DictEntry out;
    ASSERT_TRUE(reader.find_term("anything", &found, &out).ok());
    EXPECT_FALSE(found);
}

// FC-5: anchor-boundary readback. anchor_interval default = 16, so 17 move-added
// entries force a second anchor (index 16); decode_all must reset the
// front-coding base at each anchor segment and return every term in order.
TEST(SniiDictBlockTest, MoveAddAnchorBoundaryDecodeAll) {
    std::vector<std::string> terms;
    std::vector<DictEntry> entries;
    for (int i = 0; i < 17; ++i) {
        char buf[16];
        snprintf(buf, sizeof(buf), "key%02d", i);
        terms.emplace_back(buf);
        entries.push_back(
                MakePodRef(buf, static_cast<uint32_t>(i + 1), static_cast<uint64_t>(i) * 50));
    }

    DictBlockBuilder builder(IndexTier::kT1, /*has_positions=*/false, 0, 0, /*anchor_interval=*/16);
    for (const auto& e : entries) {
        DictEntry tmp = e;
        builder.add_entry(std::move(tmp));
    }
    ByteSink sink;
    builder.finish(&sink);

    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(sink.buffer()), IndexTier::kT1,
                                      /*has_positions=*/false, &reader)
                        .ok());
    EXPECT_EQ(reader.n_entries(), 17U);

    std::vector<DictEntry> all;
    ASSERT_TRUE(reader.decode_all(&all).ok());
    ASSERT_EQ(all.size(), entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(all[i].term, terms[i]) << i;
        ExpectCommon(entries[i], all[i]);
        EXPECT_EQ(all[i].frq_off_delta, static_cast<uint64_t>(i) * 50) << i;
    }
}

// RED-3: removing the dead prev_term_ member must not change finish() output.
// A multi-anchor, long-shared-prefix dataset stresses front coding across anchor
// segments -- exactly what finish() rebuilds from its local `prev` (never from
// the removed member). The const-ref path is the pristine golden; the move path
// must reproduce it byte-for-byte, and the block must decode back to the inputs.
TEST(SniiDictBlockTest, FinishUnaffectedByDeadPrevTermRemoval) {
    const std::vector<std::string> terms = {
            "interest",  "interested", "interesting",   "interestingly", "interests",
            "internal",  "internally", "international", "internet",      "internets",
            "interplay", "interpose",  "interpret",     "interpreted",   "interval",
            "intervene", "interview",  "interviewed",   "intestine",     "intimate",
            "intricate"};
    std::vector<DictEntry> entries;
    uint64_t off = 0;
    for (const auto& t : terms) {
        entries.push_back(MakePodRef(t, 4, off));
        off += 50;
    }

    const std::vector<uint8_t> golden = BuildBlock(entries, IndexTier::kT1, /*has_positions=*/false,
                                                   0, 0, /*anchor_interval=*/4);
    const std::vector<uint8_t> moved =
            BuildBlockMoved(entries, IndexTier::kT1, /*has_positions=*/false, 0, 0, 4);
    EXPECT_EQ(golden, moved); // dead-field removal + move path leave bytes identical

    DictBlockReader reader;
    ASSERT_TRUE(DictBlockReader::open(Slice(golden), IndexTier::kT1, false, &reader).ok());
    std::vector<DictEntry> all;
    ASSERT_TRUE(reader.decode_all(&all).ok());
    ASSERT_EQ(all.size(), terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_EQ(all[i].term, terms[i]) << i; // front-coding base rebuilt without prev_term_
    }
}
