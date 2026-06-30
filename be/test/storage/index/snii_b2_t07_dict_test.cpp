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

// T07 -- DICT entry key-first decode (exact find_term + prefix streaming
// early-stop). These tests assert three things the optimization promises:
//   1. byte-identical decode: decode_dict_entry == decode_dict_entry_key +
//      decode_dict_entry_rest, and visit_prefix_range streams the same entries
//      decode_all materializes;
//   2. body-decode op-count drops: find_term materializes 1 (hit) / 0 (miss)
//      bodies instead of every scanned entry; visit_prefix_range materializes
//      only the accepted/emitted bodies (anchor-jump + early-stop);
//   3. results unchanged at the reader/query layer (prefix_terms / prefix_query
//      route through the rewired streaming path).

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii {
namespace {

using namespace doris::snii::format;    // NOLINT(google-build-using-namespace)
using namespace doris::snii::snii_test; // NOLINT(google-build-using-namespace)
using doris::Status;

// ---- entry builders (valid, self-consistent locators so they round-trip) ----

DictEntry MakePodRef(std::string term, uint32_t df, uint64_t frq_off, uint64_t prx_off = 0) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kPodRef;
    e.enc = DictEntryEnc::kSlim;
    e.df = df;
    e.ttf_delta = df * 2;
    e.max_freq = 9;
    e.frq_off_delta = frq_off;
    e.frq_len = 128;
    e.frq_docs_len = 64; // dd region on-disk length (<= frq_len)
    e.dd_meta.uncomp_len = 70;
    e.dd_meta.crc = 0xABCD1234U; // pod_ref regions keep their per-region crc
    e.freq_meta.uncomp_len = 40;
    e.freq_meta.crc = 0x55AA00FFU;
    e.prx_off_delta = prx_off;
    e.prx_len = 64;
    return e;
}

DictEntry MakePodRefWindowed(std::string term, uint32_t df, uint64_t frq_off) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kPodRef;
    e.enc = DictEntryEnc::kWindowed;
    e.df = df;
    e.ttf_delta = df * 2;
    e.max_freq = 5;
    e.frq_off_delta = frq_off;
    e.frq_len = 200;
    e.prelude_len = 10;   // 0 < prelude_len <= frq_docs_len
    e.frq_docs_len = 120; // <= frq_len
    e.prx_off_delta = 0;
    e.prx_len = 50;
    return e;
}

DictEntry MakeInline(std::string term, uint32_t df) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kInline;
    e.enc = DictEntryEnc::kSlim;
    e.df = df;
    e.ttf_delta = df * 3;
    e.max_freq = 7;
    e.frq_bytes = {0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80};
    e.inline_dd_disk_len = 5; // <= frq_bytes.size()
    e.dd_meta.uncomp_len = 12;
    e.freq_meta.uncomp_len = 6;
    e.prx_bytes = {0xAA, 0xBB, 0xCC};
    return e;
}

// Full field comparison of two DECODED entries (used to prove the key-first path
// produces byte-identical output to the full / decode_all path).
void ExpectEntryEq(const DictEntry& a, const DictEntry& b) {
    EXPECT_EQ(a.term, b.term);
    EXPECT_EQ(a.kind, b.kind);
    EXPECT_EQ(a.enc, b.enc);
    EXPECT_EQ(a.has_sb, b.has_sb);
    EXPECT_EQ(a.df, b.df);
    EXPECT_EQ(a.ttf_delta, b.ttf_delta);
    EXPECT_EQ(a.max_freq, b.max_freq);
    EXPECT_EQ(a.frq_off_delta, b.frq_off_delta);
    EXPECT_EQ(a.frq_len, b.frq_len);
    EXPECT_EQ(a.prelude_len, b.prelude_len);
    EXPECT_EQ(a.frq_docs_len, b.frq_docs_len);
    EXPECT_EQ(a.prx_off_delta, b.prx_off_delta);
    EXPECT_EQ(a.prx_len, b.prx_len);
    EXPECT_EQ(a.inline_dd_disk_len, b.inline_dd_disk_len);
    EXPECT_EQ(a.frq_bytes, b.frq_bytes);
    EXPECT_EQ(a.prx_bytes, b.prx_bytes);
    EXPECT_EQ(a.dd_meta.zstd, b.dd_meta.zstd);
    EXPECT_EQ(a.dd_meta.uncomp_len, b.dd_meta.uncomp_len);
    EXPECT_EQ(a.dd_meta.disk_len, b.dd_meta.disk_len);
    EXPECT_EQ(a.dd_meta.crc, b.dd_meta.crc);
    EXPECT_EQ(a.dd_meta.verify_crc, b.dd_meta.verify_crc);
    EXPECT_EQ(a.freq_meta.zstd, b.freq_meta.zstd);
    EXPECT_EQ(a.freq_meta.uncomp_len, b.freq_meta.uncomp_len);
    EXPECT_EQ(a.freq_meta.disk_len, b.freq_meta.disk_len);
    EXPECT_EQ(a.freq_meta.crc, b.freq_meta.crc);
    EXPECT_EQ(a.freq_meta.verify_crc, b.freq_meta.verify_crc);
}

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

// A no-op key predicate (accept all); passing an empty std::function exercises
// the accept-all short-circuit in visit_prefix_range.
const std::function<bool(std::string_view)> kAcceptAll {};

// Collects the streamed entries' terms; never stops early.
std::function<Status(DictEntry&&, bool*)> collect_terms(std::vector<std::string>* out) {
    return [out](DictEntry&& e, bool* stop) -> Status {
        out->push_back(e.term);
        *stop = false;
        return Status::OK();
    };
}

// ---- F-1: key/rest split and decode_all are byte-identical ----

// Entry level: decode_dict_entry (full) vs decode_dict_entry_key + _rest, over a
// front-coded sequence mixing pod_ref(slim/windowed) and inline entries.
TEST(SniiB2T07Dict, KeyRestSplitDecodesByteIdenticalToFull) {
    const IndexTier tier = IndexTier::kT2;
    const std::vector<DictEntry> entries = {
            MakePodRef("alpha", 3, 0, 0),
            MakeInline("alphabet", 5),
            MakePodRefWindowed("beta", 7, 100),
            MakePodRef("betas", 9, 300, 80),
            MakeInline("gamma", 2),
    };

    ByteSink sink;
    std::string prev;
    for (const auto& e : entries) {
        assert_ok(encode_dict_entry(e, prev, tier, &sink));
        prev = e.term;
    }
    const std::vector<uint8_t> buf = sink.buffer();

    // Baseline: full decode.
    std::vector<DictEntry> full;
    {
        ByteSource src {Slice(buf)};
        std::string p;
        for (size_t i = 0; i < entries.size(); ++i) {
            DictEntry e;
            assert_ok(decode_dict_entry(&src, p, tier, &e));
            p = e.term;
            full.push_back(std::move(e));
        }
        EXPECT_TRUE(src.eof());
    }

    // key + rest decode: byte-identical fields, exactly one body decode per entry.
    reset_dict_entry_counters();
    std::vector<DictEntry> split;
    {
        ByteSource src {Slice(buf)};
        std::string p;
        for (size_t i = 0; i < entries.size(); ++i) {
            DictEntry e;
            size_t body_start = 0;
            uint64_t total = 0;
            assert_ok(decode_dict_entry_key(&src, p, &e, &body_start, &total));
            assert_ok(decode_dict_entry_rest(&src, tier, body_start, total, &e));
            p = e.term;
            split.push_back(std::move(e));
        }
        EXPECT_TRUE(src.eof());
    }
    EXPECT_EQ(dict_entry_body_decode_count(), entries.size());

    ASSERT_EQ(full.size(), split.size());
    for (size_t i = 0; i < full.size(); ++i) {
        ExpectEntryEq(full[i], split[i]);
    }
}

// skip_dict_entry_body advances across entries (front coding still rebuilds each
// term) without materializing any body.
TEST(SniiB2T07Dict, SkipDictEntryBodyAdvancesAndCountsNoBody) {
    const IndexTier tier = IndexTier::kT2;
    const std::vector<DictEntry> entries = {MakePodRef("aa", 1, 0, 0), MakeInline("ab", 2),
                                            MakePodRefWindowed("ac", 3, 50)};
    ByteSink sink;
    std::string prev;
    for (const auto& e : entries) {
        assert_ok(encode_dict_entry(e, prev, tier, &sink));
        prev = e.term;
    }
    const std::vector<uint8_t> buf = sink.buffer();

    reset_dict_entry_counters();
    ByteSource src {Slice(buf)};
    std::string p;
    std::vector<std::string> terms;
    for (size_t i = 0; i < entries.size(); ++i) {
        DictEntry e;
        size_t body_start = 0;
        uint64_t total = 0;
        assert_ok(decode_dict_entry_key(&src, p, &e, &body_start, &total));
        terms.push_back(e.term);
        assert_ok(skip_dict_entry_body(&src, body_start, total));
        p = e.term;
    }
    EXPECT_TRUE(src.eof());
    EXPECT_EQ(dict_entry_body_decode_count(), 0U); // keys only
    ASSERT_EQ(terms.size(), 3U);
    EXPECT_EQ(terms[0], "aa");
    EXPECT_EQ(terms[1], "ab");
    EXPECT_EQ(terms[2], "ac");
}

// Block level: visit_prefix_range("") streams every entry, byte-identical to
// decode_all, across multiple anchor segments and mixed entry kinds.
TEST(SniiB2T07Dict, VisitPrefixRangeEmptyPrefixMatchesDecodeAll) {
    std::vector<DictEntry> entries;
    for (int i = 0; i < 40; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "term_%02d", i);
        if (i % 3 == 0) {
            entries.push_back(MakeInline(buf, static_cast<uint32_t>(i + 1)));
        } else if (i % 3 == 1) {
            entries.push_back(MakePodRef(buf, static_cast<uint32_t>(i + 1),
                                         static_cast<uint64_t>(i) * 10,
                                         static_cast<uint64_t>(i) * 4));
        } else {
            entries.push_back(MakePodRefWindowed(buf, static_cast<uint32_t>(i + 1),
                                                 static_cast<uint64_t>(i) * 10));
        }
    }
    const std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT2, true, 8192, 16384, 16);
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT2, true, &reader));

    std::vector<DictEntry> golden;
    assert_ok(reader.decode_all(&golden));

    std::vector<DictEntry> streamed;
    bool exhausted = false;
    assert_ok(reader.visit_prefix_range(
            "", kAcceptAll,
            [&](DictEntry&& e, bool* stop) -> Status {
                streamed.push_back(std::move(e));
                *stop = false;
                return Status::OK();
            },
            &exhausted));
    EXPECT_FALSE(exhausted); // the empty prefix never leaves the range
    ASSERT_EQ(streamed.size(), golden.size());
    for (size_t i = 0; i < golden.size(); ++i) {
        ExpectEntryEq(golden[i], streamed[i]);
    }
}

// ---- F-2 / F-3: find_term materializes only the matched body ----

TEST(SniiB2T07Dict, FindTermDecodesOnlyMatchedBody) {
    std::vector<DictEntry> entries;
    for (int i = 0; i < 16; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%02d", i);
        entries.push_back(
                MakePodRef(buf, static_cast<uint32_t>(i + 1), static_cast<uint64_t>(i) * 8));
    }
    const std::vector<uint8_t> bytes =
            BuildBlock(entries, IndexTier::kT1, false, 0, 0, 16); // 1 anchor
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader));

    // First / middle / last entry: each materializes exactly one body even though
    // the scan walks up to 16 keys.
    for (const char* t : {"k00", "k07", "k15"}) {
        reset_dict_entry_counters();
        bool found = false;
        DictEntry out;
        assert_ok(reader.find_term(t, &found, &out));
        EXPECT_TRUE(found) << t;
        EXPECT_EQ(out.term, t) << t;
        EXPECT_EQ(dict_entry_body_decode_count(), 1U) << t;
    }
}

TEST(SniiB2T07Dict, FindTermMissDecodesNoBody) {
    std::vector<DictEntry> entries; // k00, k02, ... k30 -> gaps between keys
    for (int i = 0; i < 32; i += 2) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%02d", i);
        entries.push_back(MakePodRef(buf, 1, static_cast<uint64_t>(i) * 4));
    }
    ASSERT_EQ(entries.size(), 16U);
    const std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 0, 0, 16);
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader));

    bool found = true;
    DictEntry out;

    // Gap inside the range: stop at the first key past target, no body decode.
    reset_dict_entry_counters();
    assert_ok(reader.find_term("k15", &found, &out));
    EXPECT_FALSE(found);
    EXPECT_EQ(dict_entry_body_decode_count(), 0U);

    // Below the first anchor term: rejected by the anchor search, no scan at all.
    reset_dict_entry_counters();
    found = true;
    assert_ok(reader.find_term("a", &found, &out));
    EXPECT_FALSE(found);
    EXPECT_EQ(dict_entry_body_decode_count(), 0U);

    // Above the last term: scan keys to eof, still no body decode.
    reset_dict_entry_counters();
    found = true;
    assert_ok(reader.find_term("z", &found, &out));
    EXPECT_FALSE(found);
    EXPECT_EQ(dict_entry_body_decode_count(), 0U);
}

// find_term's matched body is byte-identical to the decode_all entry, across
// anchor boundaries (mixed kinds).
TEST(SniiB2T07Dict, FindTermReturnsEntryMatchingDecodeAll) {
    std::vector<DictEntry> entries;
    for (int i = 0; i < 40; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "term_%02d", i);
        if (i % 2 == 0) {
            entries.push_back(MakeInline(buf, static_cast<uint32_t>(i + 1)));
        } else {
            entries.push_back(MakePodRef(buf, static_cast<uint32_t>(i + 1),
                                         static_cast<uint64_t>(i) * 10,
                                         static_cast<uint64_t>(i) * 4));
        }
    }
    const std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT2, true, 4096, 8192, 16);
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT2, true, &reader));

    std::vector<DictEntry> golden;
    assert_ok(reader.decode_all(&golden));
    for (const auto& g : golden) {
        bool found = false;
        DictEntry out;
        assert_ok(reader.find_term(g.term, &found, &out));
        ASSERT_TRUE(found) << g.term;
        ExpectEntryEq(g, out);
    }
}

// ---- F-6 / anchor-jump / accept_key / early-stop on visit_prefix_range ----

TEST(SniiB2T07Dict, VisitPrefixRangeStopsAtBoundaryDecodingOnlyMatches) {
    const std::vector<DictEntry> entries = {
            MakePodRef("aa1", 1, 0),  MakePodRef("aa2", 1, 10), MakePodRef("ab1", 1, 20),
            MakeInline("ab2", 2),     MakePodRef("ab3", 1, 40), MakePodRef("ac1", 1, 50),
            MakePodRef("ac2", 1, 60),
    };
    const std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 0, 0, 16);
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader));

    reset_dict_entry_counters();
    std::vector<std::string> got;
    bool exhausted = false;
    assert_ok(reader.visit_prefix_range("ab", kAcceptAll, collect_terms(&got), &exhausted));
    EXPECT_TRUE(exhausted); // saw "ac1" past the range
    ASSERT_EQ(got.size(), 3U);
    EXPECT_EQ(got[0], "ab1");
    EXPECT_EQ(got[1], "ab2");
    EXPECT_EQ(got[2], "ab3");
    // aa* skipped (key-only), ac* never reached -> exactly the 3 ab* bodies.
    EXPECT_EQ(dict_entry_body_decode_count(), 3U);
}

TEST(SniiB2T07Dict, VisitPrefixRangeAnchorJumpSkipsPrePrefixSegments) {
    std::vector<DictEntry> entries;
    for (int i = 0; i < 32; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "aa%03d", i);
        entries.push_back(MakePodRef(buf, 1, static_cast<uint64_t>(i)));
    }
    for (int i = 0; i < 4; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "zz%03d", i);
        entries.push_back(MakePodRef(buf, 1, 1000 + static_cast<uint64_t>(i)));
    }
    // anchor_interval=8 -> several whole "aa*" anchor segments are jumped, never
    // decoded; the segment straddling the prefix is scanned key-only.
    const std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 0, 0, 8);
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader));

    reset_dict_entry_counters();
    std::vector<std::string> got;
    bool exhausted = false;
    assert_ok(reader.visit_prefix_range("zz", kAcceptAll, collect_terms(&got), &exhausted));
    ASSERT_EQ(got.size(), 4U);
    EXPECT_EQ(got[0], "zz000");
    EXPECT_EQ(got[3], "zz003");
    EXPECT_FALSE(exhausted); // zz003 is the last entry -> block ended in range
    EXPECT_EQ(dict_entry_body_decode_count(), 4U); // only the zz* bodies
}

TEST(SniiB2T07Dict, VisitPrefixRangeAcceptKeySkipsRejectedBodies) {
    std::vector<DictEntry> entries;
    for (int i = 0; i < 10; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "ab%d", i);
        entries.push_back(MakePodRef(buf, 1, static_cast<uint64_t>(i) * 5));
    }
    const std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 0, 0, 16);
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader));

    reset_dict_entry_counters();
    std::vector<std::string> got;
    bool exhausted = false;
    const std::function<bool(std::string_view)> accept_even = [](std::string_view t) {
        return ((t.back() - '0') % 2) == 0;
    };
    assert_ok(reader.visit_prefix_range("ab", accept_even, collect_terms(&got), &exhausted));
    ASSERT_EQ(got.size(), 5U); // ab0, ab2, ab4, ab6, ab8
    EXPECT_EQ(got[0], "ab0");
    EXPECT_EQ(got[4], "ab8");
    // Rejected keys skip their body: 5 bodies, not 10.
    EXPECT_EQ(dict_entry_body_decode_count(), 5U);
}

TEST(SniiB2T07Dict, VisitPrefixRangeEarlyStopAfterK) {
    std::vector<DictEntry> entries;
    for (int i = 0; i < 10; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "ab%d", i);
        entries.push_back(MakePodRef(buf, 1, static_cast<uint64_t>(i) * 5));
    }
    const std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 0, 0, 16);
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader));

    reset_dict_entry_counters();
    std::vector<std::string> got;
    bool exhausted = false;
    int n = 0;
    assert_ok(reader.visit_prefix_range(
            "ab", kAcceptAll,
            [&](DictEntry&& e, bool* stop) -> Status {
                got.push_back(e.term);
                ++n;
                *stop = (n >= 3);
                return Status::OK();
            },
            &exhausted));
    ASSERT_EQ(got.size(), 3U);
    EXPECT_FALSE(exhausted);                       // stopped by the visitor, not a boundary
    EXPECT_EQ(dict_entry_body_decode_count(), 3U); // no bodies past the stop
}

// ---- F-5 / F-7: single-entry block + corrupt entry_len ----

TEST(SniiB2T07Dict, SingleEntryBlockVisitAndFind) {
    const std::vector<DictEntry> entries = {MakePodRef("solo", 7, 0)};
    const std::vector<uint8_t> bytes = BuildBlock(entries, IndexTier::kT1, false, 4096, 0, 16);
    DictBlockReader reader;
    assert_ok(DictBlockReader::open(Slice(bytes), IndexTier::kT1, false, &reader));

    reset_dict_entry_counters();
    bool found = false;
    DictEntry out;
    assert_ok(reader.find_term("solo", &found, &out));
    EXPECT_TRUE(found);
    EXPECT_EQ(out.term, "solo");
    EXPECT_EQ(dict_entry_body_decode_count(), 1U);

    reset_dict_entry_counters();
    std::vector<std::string> got;
    bool exhausted = false;
    assert_ok(reader.visit_prefix_range("so", kAcceptAll, collect_terms(&got), &exhausted));
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0], "solo");
    EXPECT_FALSE(exhausted);
    EXPECT_EQ(dict_entry_body_decode_count(), 1U);
}

TEST(SniiB2T07Dict, DecodeDictEntryKeyRejectsCorruptEntryLen) {
    // entry_len varint decodes to 127 but only 3 bytes remain after it.
    const std::vector<uint8_t> buf = {0x7F, 0x00, 0x00, 0x00};
    ByteSource src {Slice(buf)};
    DictEntry e;
    size_t body_start = 0;
    uint64_t total = 0;
    const Status s = decode_dict_entry_key(&src, "", &e, &body_start, &total);
    EXPECT_FALSE(s.ok());
}

// ---- reader / query level: results unchanged + body-decode bounded ----

// prefix_terms routes through the rewired visit_prefix_range; it returns the same
// ordered hits and materializes only the matched bodies.
TEST(SniiB2T07DictReader, PrefixTermsResultsUnchangedAndStreamBounded) {
    ScopedEnv resident_bsbf("SNII_BSBF_RESIDENT_MAX", "1048576");
    ScopedEnv resident_dict("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    reader::SniiSegmentReader seg;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &seg, &idx));

    std::vector<reader::LogicalIndexReader::PrefixHit> hits;
    reset_dict_entry_counters();
    assert_ok(idx.prefix_terms("ord", &hits, 10));
    ASSERT_EQ(hits.size(), 2U);
    EXPECT_EQ(hits[0].term, "order");
    EXPECT_EQ(hits[1].term, "ordinal");
    // Resident block: only the two prefix matches' bodies are materialized.
    EXPECT_EQ(dict_entry_body_decode_count(), 2U);
}

TEST(SniiB2T07DictReader, PrefixTermsEmptyPrefixEnumeratesAllSorted) {
    ScopedEnv resident_bsbf("SNII_BSBF_RESIDENT_MAX", "1048576");
    ScopedEnv resident_dict("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    reader::SniiSegmentReader seg;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &seg, &idx));

    std::vector<reader::LogicalIndexReader::PrefixHit> hits;
    assert_ok(idx.prefix_terms("", &hits, 0));
    std::vector<std::string> terms;
    terms.reserve(hits.size());
    for (const auto& h : hits) {
        terms.push_back(h.term);
    }
    const std::vector<std::string> expected = {"123",         "almost",       "driver",  "failed",
                                               "needle",      "order",        "ordinal", "repeat",
                                               "sparse_left", "sparse_right", "trace"};
    EXPECT_EQ(terms, expected);
}

TEST(SniiB2T07DictReader, LookupDecodesOnlyMatchedBody) {
    ScopedEnv resident_bsbf("SNII_BSBF_RESIDENT_MAX", "1048576");
    ScopedEnv resident_dict("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    reader::SniiSegmentReader seg;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &seg, &idx));

    bool found = false;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    reset_dict_entry_counters();
    assert_ok(idx.lookup("trace", &found, &entry, &frq_base, &prx_base));
    EXPECT_TRUE(found);
    EXPECT_EQ(entry.term, "trace");
    EXPECT_EQ(dict_entry_body_decode_count(), 1U); // only "trace" body materialized
}

// End-to-end: a prefix query over the rewired streaming path equals the union of
// the expanded terms' postings.
TEST(SniiB2T07DictReader, PrefixQueryMatchesTermUnion) {
    MemoryFile file;
    reader::SniiSegmentReader seg;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &seg, &idx));

    std::vector<uint32_t> prefix_docids;
    assert_ok(query::prefix_query(idx, "ord", &prefix_docids));

    std::vector<uint32_t> order_docids;
    std::vector<uint32_t> ordinal_docids;
    assert_ok(query::term_query(idx, "order", &order_docids));
    assert_ok(query::term_query(idx, "ordinal", &ordinal_docids));

    std::vector<uint32_t> expected;
    std::set_union(order_docids.begin(), order_docids.end(), ordinal_docids.begin(),
                   ordinal_docids.end(), std::back_inserter(expected));
    EXPECT_EQ(prefix_docids, expected);
}

} // namespace
} // namespace doris::snii
