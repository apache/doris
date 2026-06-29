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

#include "snii/format/dict_block.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/crc32c.h"
#include "snii/format/dict_entry.h"
#include "snii/format/format_constants.h"

using namespace snii;         // NOLINT
using namespace snii::format; // NOLINT
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
    uint32_t crc = snii::crc32c(Slice(bytes.data(), n - 4));
    for (int k = 0; k < 4; ++k) {
        bytes[n - 4 + k] = static_cast<uint8_t>(crc >> (8 * k));
    }

    DictBlockReader reader;
    Status s = DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader);
    EXPECT_FALSE(s.ok());
}
