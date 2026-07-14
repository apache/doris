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

#include "storage/index/snii/format/dict_entry.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/format_constants.h"

using namespace doris::snii;         // NOLINT
using namespace doris::snii::format; // NOLINT
using doris::Status;

namespace {

// Construct a pod_ref slim dict entry (tier determines which optional fields
// are present).
DictEntry MakePodRefSlim(std::string term, uint32_t df) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kPodRef;
    e.enc = DictEntryEnc::kSlim;
    e.has_sb = false;
    e.df = df;
    e.ttf_delta = df * 3; // written only when tier>=T2
    e.max_freq = 7;       // written only when tier>=T2
    e.frq_off_delta = 4096;
    e.frq_len = 333;
    e.frq_docs_len = 200;   // slim pod_ref: docs-only prefix (<= frq_len)
    e.prx_off_delta = 8192; // written only when positions are stored
    e.prx_len = 512;        // written only when positions are stored
    return e;
}

DictEntry MakePodRefWindowed(std::string term, uint32_t df) {
    DictEntry e = MakePodRefSlim(std::move(term), df);
    e.enc = DictEntryEnc::kWindowed;
    e.has_sb = true;
    e.prelude_len = 64;   // written only for windowed encoding
    e.frq_docs_len = 240; // windowed docs-only prefix: [prelude][dd-block]
    return e;
}

DictEntry MakeInline(std::string term, uint32_t df) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kInline;
    e.enc = DictEntryEnc::kSlim;
    e.df = df;
    e.ttf_delta = df * 2;
    e.max_freq = 5;
    e.frq_bytes = {0x01, 0x02, 0x03, 0x04};
    e.prx_bytes = {0xAA, 0xBB}; // written only when positions are stored
    return e;
}

// round-trip: encode then decode, assert that fields retained by the given tier
// match.
DictEntry RoundTrip(const DictEntry& in, std::string_view prev, IndexTier tier) {
    ByteSink sink;
    EXPECT_TRUE(encode_dict_entry(in, prev, tier, &sink).ok());
    ByteSource src(sink.view());
    DictEntry out;
    Status s = decode_dict_entry(&src, prev, tier, &out);
    EXPECT_TRUE(s.ok()) << s.to_string();
    EXPECT_EQ(src.remaining(), 0U) << "decode did not consume the entire entry";
    return out;
}

void ExpectCommon(const DictEntry& a, const DictEntry& b) {
    EXPECT_EQ(a.term, b.term);
    EXPECT_EQ(a.kind, b.kind);
    EXPECT_EQ(a.enc, b.enc);
    EXPECT_EQ(a.has_sb, b.has_sb);
    EXPECT_EQ(a.df, b.df);
}

} // namespace

TEST(SniiDictEntry, PodRefSlimTier1RoundTrip) {
    DictEntry in = MakePodRefSlim("apple", 42);
    DictEntry out = RoundTrip(in, "", IndexTier::kT1);
    ExpectCommon(in, out);
    EXPECT_EQ(out.frq_off_delta, in.frq_off_delta);
    EXPECT_EQ(out.frq_len, in.frq_len);
    EXPECT_EQ(out.frq_docs_len, in.frq_docs_len); // slim docs-only prefix preserved
    // tier1: ttf/max_freq/prx are not written; decode restores them to default 0.
    EXPECT_EQ(out.ttf_delta, 0U);
    EXPECT_EQ(out.max_freq, 0U);
    EXPECT_EQ(out.prx_len, 0U);
}

TEST(SniiDictEntry, PodRefSlimTier2RoundTrip) {
    DictEntry in = MakePodRefSlim("banana", 100);
    DictEntry out = RoundTrip(in, "", IndexTier::kT2);
    ExpectCommon(in, out);
    EXPECT_EQ(out.frq_docs_len, in.frq_docs_len);
    EXPECT_EQ(out.ttf_delta, in.ttf_delta);
    EXPECT_EQ(out.max_freq, in.max_freq);
    EXPECT_EQ(out.prx_off_delta, in.prx_off_delta);
    EXPECT_EQ(out.prx_len, in.prx_len);
}

// A slim pod_ref whose frq_docs_len exceeds frq_len is rejected on decode.
TEST(SniiDictEntry, PodRefSlimFrqDocsLenExceedsFrqLenRejected) {
    DictEntry in = MakePodRefSlim("kiwi", 50);
    in.frq_docs_len = in.frq_len + 1; // impossible prefix
    ByteSink sink;
    ASSERT_TRUE(encode_dict_entry(in, "", IndexTier::kT2, &sink).ok());
    ByteSource src(sink.view());
    DictEntry out;
    Status s = decode_dict_entry(&src, "", IndexTier::kT2, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiDictEntry, PodRefSlimTier3RoundTrip) {
    DictEntry in = MakePodRefSlim("cherry", 500);
    DictEntry out = RoundTrip(in, "", IndexTier::kT3);
    ExpectCommon(in, out);
    EXPECT_EQ(out.ttf_delta, in.ttf_delta);
    EXPECT_EQ(out.max_freq, in.max_freq);
    EXPECT_EQ(out.prx_off_delta, in.prx_off_delta);
    EXPECT_EQ(out.prx_len, in.prx_len);
}

TEST(SniiDictEntry, PodRefWindowedTier2RoundTrip) {
    DictEntry in = MakePodRefWindowed("durian", 2000);
    DictEntry out = RoundTrip(in, "", IndexTier::kT2);
    ExpectCommon(in, out);
    EXPECT_EQ(out.enc, DictEntryEnc::kWindowed);
    EXPECT_EQ(out.has_sb, true);
    EXPECT_EQ(out.prelude_len, in.prelude_len);
    EXPECT_EQ(out.frq_docs_len, in.frq_docs_len);
    EXPECT_EQ(out.prx_len, in.prx_len);
}

TEST(SniiDictEntry, PodRefWindowedTier1RoundTrip) {
    DictEntry in = MakePodRefWindowed("elderberry", 1500);
    DictEntry out = RoundTrip(in, "", IndexTier::kT1);
    ExpectCommon(in, out);
    EXPECT_EQ(out.prelude_len, in.prelude_len);
    EXPECT_EQ(out.frq_docs_len, in.frq_docs_len);
    EXPECT_EQ(out.prx_len, 0U); // tier1 has no prx
}

TEST(SniiDictEntry, PodRefWindowedDocsPrefixBoundsRejected) {
    DictEntry in = MakePodRefWindowed("jackfruit", 1500);
    in.frq_docs_len = in.prelude_len - 1;
    ByteSink sink;
    ASSERT_TRUE(encode_dict_entry(in, "", IndexTier::kT2, &sink).ok());
    ByteSource src(sink.view());
    DictEntry out;
    Status s = decode_dict_entry(&src, "", IndexTier::kT2, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());

    in.frq_docs_len = in.frq_len + 1;
    ByteSink sink2;
    ASSERT_TRUE(encode_dict_entry(in, "", IndexTier::kT2, &sink2).ok());
    ByteSource src2(sink2.view());
    s = decode_dict_entry(&src2, "", IndexTier::kT2, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiDictEntry, InlineTier1RoundTrip) {
    DictEntry in = MakeInline("fig", 3);
    DictEntry out = RoundTrip(in, "", IndexTier::kT1);
    ExpectCommon(in, out);
    EXPECT_EQ(out.frq_bytes, in.frq_bytes);
    EXPECT_TRUE(out.prx_bytes.empty()); // tier1 has no prx
}

TEST(SniiDictEntry, InlineTier2RoundTrip) {
    DictEntry in = MakeInline("grape", 8);
    DictEntry out = RoundTrip(in, "", IndexTier::kT2);
    ExpectCommon(in, out);
    EXPECT_EQ(out.frq_bytes, in.frq_bytes);
    EXPECT_EQ(out.prx_bytes, in.prx_bytes);
    EXPECT_EQ(out.ttf_delta, in.ttf_delta);
    EXPECT_EQ(out.max_freq, in.max_freq);
}

TEST(SniiDictEntry, InlineTier3RoundTrip) {
    DictEntry in = MakeInline("honeydew", 12);
    DictEntry out = RoundTrip(in, "", IndexTier::kT3);
    ExpectCommon(in, out);
    EXPECT_EQ(out.frq_bytes, in.frq_bytes);
    EXPECT_EQ(out.prx_bytes, in.prx_bytes);
}

// Format v2: an INLINE entry omits the redundant per-region crc32c bytes
// (dd_crc + freq_crc) -- its region bytes are covered by the dict block crc32c.
// Decoded inline metas carry verify_crc=false so region decode skips the check.
// A slim POD-ref entry KEEPS its crc (regions live in the separately-fetched
// .frq POD), so it is exactly 8 bytes larger for the same df.
TEST(SniiDictEntry, InlineOmitsRedundantRegionCrc) {
    DictEntry inl = MakeInline("melon", 8);
    inl.dd_meta.crc = 0xDEADBEEF;   // would be written by v1; must be omitted now
    inl.freq_meta.crc = 0xCAFEF00D; // ditto
    ByteSink inl_sink;
    ASSERT_TRUE(encode_dict_entry(inl, "", IndexTier::kT2, &inl_sink).ok());

    // Same payload but encoded as a slim POD-ref keeps both crcs (dd + freq =
    // 8B).
    DictEntry ref = inl;
    ref.kind = DictEntryKind::kPodRef;
    ref.frq_len = inl.frq_bytes.size();
    ref.frq_docs_len = inl.inline_dd_disk_len;
    ByteSink ref_sink;
    ASSERT_TRUE(encode_dict_entry(ref, "", IndexTier::kT2, &ref_sink).ok());
    // The POD-ref locator differs structurally; assert the inline encoding does
    // not carry the 8 crc bytes by decoding and checking verify_crc is OFF.
    ByteSource src(inl_sink.view());
    DictEntry out;
    ASSERT_TRUE(decode_dict_entry(&src, "", IndexTier::kT2, &out).ok());
    EXPECT_FALSE(out.dd_meta.verify_crc);
    EXPECT_FALSE(out.freq_meta.verify_crc);
    EXPECT_EQ(out.dd_meta.crc, 0U); // crc not stored -> decoded as 0
    EXPECT_EQ(out.freq_meta.crc, 0U);
}

// A POD-ref slim entry decodes with verify_crc=true (its regions carry a crc).
TEST(SniiDictEntry, PodRefSlimKeepsRegionCrc) {
    DictEntry in = MakePodRefSlim("nectarine", 50);
    in.dd_meta.crc = 0x12345678;
    in.freq_meta.crc = 0x9ABCDEF0;
    DictEntry out = RoundTrip(in, "", IndexTier::kT2);
    EXPECT_TRUE(out.dd_meta.verify_crc);
    EXPECT_TRUE(out.freq_meta.verify_crc);
    EXPECT_EQ(out.dd_meta.crc, 0x12345678U);
    EXPECT_EQ(out.freq_meta.crc, 0x9ABCDEF0U);
}

// Front coding: consecutive encode/decode of sorted terms; suffix stores only
// the differing part.
TEST(SniiDictEntry, PrefixCompressionSharedPrefix) {
    // "interest" and "interesting" share the prefix "interest".
    DictEntry a = MakePodRefSlim("interest", 10);
    DictEntry b = MakePodRefSlim("interesting", 11);

    ByteSink sink;
    ASSERT_TRUE(encode_dict_entry(a, "", IndexTier::kT2, &sink).ok());
    size_t a_end = sink.size();
    ASSERT_TRUE(encode_dict_entry(b, a.term, IndexTier::kT2, &sink).ok());

    ByteSource src(sink.view());
    DictEntry oa;
    ASSERT_TRUE(decode_dict_entry(&src, "", IndexTier::kT2, &oa).ok());
    EXPECT_EQ(src.position(), a_end);
    EXPECT_EQ(oa.term, "interest");

    DictEntry ob;
    ASSERT_TRUE(decode_dict_entry(&src, oa.term, IndexTier::kT2, &ob).ok());
    EXPECT_EQ(ob.term, "interesting");
    EXPECT_TRUE(src.eof());
}

// Encode/decode three sorted terms consecutively to verify the prefix chain.
TEST(SniiDictEntry, PrefixCompressionChain) {
    std::vector<std::string> terms = {"app", "apple", "application"};
    ByteSink sink;
    std::string prev;
    for (const auto& t : terms) {
        DictEntry e = MakePodRefSlim(t, 5);
        ASSERT_TRUE(encode_dict_entry(e, prev, IndexTier::kT1, &sink).ok());
        prev = t;
    }
    ByteSource src(sink.view());
    prev.clear();
    for (const auto& t : terms) {
        DictEntry out;
        ASSERT_TRUE(decode_dict_entry(&src, prev, IndexTier::kT1, &out).ok());
        EXPECT_EQ(out.term, t);
        prev = out.term;
    }
    EXPECT_TRUE(src.eof());
}

// entry_len must allow a reader to skip the entire entry without parsing its
// internal fields.
TEST(SniiDictEntry, EntryLenAllowsSkip) {
    DictEntry a = MakePodRefSlim("first", 9);
    DictEntry b = MakeInline("second", 4);
    ByteSink sink;
    ASSERT_TRUE(encode_dict_entry(a, "", IndexTier::kT2, &sink).ok());
    ASSERT_TRUE(encode_dict_entry(b, a.term, IndexTier::kT2, &sink).ok());

    ByteSource src(sink.view());
    // Skip the first entry using only entry_len.
    ASSERT_TRUE(skip_dict_entry(&src).ok());
    DictEntry out;
    ASSERT_TRUE(decode_dict_entry(&src, a.term, IndexTier::kT2, &out).ok());
    EXPECT_EQ(out.term, "second");
    EXPECT_TRUE(src.eof());
}

// Edge case: empty suffix (term is identical to prev).
TEST(SniiDictEntry, EmptySuffixEqualsPrev) {
    DictEntry in = MakePodRefSlim("same", 1);
    DictEntry out = RoundTrip(in, "same", IndexTier::kT1);
    EXPECT_EQ(out.term, "same");
}

// Edge case: df = 0.
TEST(SniiDictEntry, ZeroDf) {
    DictEntry in = MakePodRefSlim("zero", 0);
    DictEntry out = RoundTrip(in, "", IndexTier::kT2);
    EXPECT_EQ(out.df, 0U);
}

// Edge case: empty term (first entry, prev is empty, suffix is also empty).
TEST(SniiDictEntry, EmptyTerm) {
    DictEntry in = MakePodRefSlim("", 1);
    DictEntry out = RoundTrip(in, "", IndexTier::kT1);
    EXPECT_EQ(out.term, "");
}

// Structural integrity: no CRC at the entry level (CRC is at the DICT block
// level), but entry_len and the actual body length must match; tampering with
// entry_len to make it smaller than the real body -> decode must report
// Corruption.
TEST(SniiDictEntry, EntryLenMismatchDetected) {
    DictEntry in = MakePodRefSlim("payload", 99);
    ByteSink sink;
    ASSERT_TRUE(encode_dict_entry(in, "", IndexTier::kT2, &sink).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes[0], 1U); // single-byte entry_len varint
    bytes[0] -= 1;           // claim the body is 1 byte shorter than it actually is

    Slice corrupt(bytes);
    ByteSource src(corrupt);
    DictEntry out;
    Status s = decode_dict_entry(&src, "", IndexTier::kT2, &out);
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// prefix_len exceeding the length of prev must be rejected (guard against
// malformed input).
TEST(SniiDictEntry, RejectPrefixLongerThanPrev) {
    DictEntry in = MakePodRefSlim("abcdef", 1);
    ByteSink sink;
    ASSERT_TRUE(encode_dict_entry(in, "abc", IndexTier::kT1, &sink).ok());
    // Decode with a shorter prev: prefix_len(=3) > prev.size()(=2) -> Corruption.
    ByteSource src(sink.view());
    DictEntry out;
    Status s = decode_dict_entry(&src, "ab", IndexTier::kT1, &out);
    EXPECT_FALSE(s.ok());
}
