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

#include "storage/index/inverted/spimi/segment_writer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Reusable VInt/VLong/Long byte reader for cross-checking the produced
// segment bytes.
class ByteReader {
public:
    explicit ByteReader(const std::vector<uint8_t>& bytes) : _bytes(bytes) {}

    uint8_t Byte() {
        EXPECT_LT(_pos, _bytes.size());
        return _bytes[_pos++];
    }

    int32_t ReadInt() {
        int32_t v = (Byte() << 24);
        v |= (Byte() << 16);
        v |= (Byte() << 8);
        v |= Byte();
        return v;
    }

    int64_t ReadLong() {
        const auto hi = static_cast<int64_t>(static_cast<uint32_t>(ReadInt()));
        const auto lo = static_cast<int64_t>(static_cast<uint32_t>(ReadInt()));
        return (hi << 32) | lo;
    }

    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = Byte();
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<int32_t>(v);
    }

    int64_t ReadVLong() {
        uint64_t v = 0;
        uint64_t shift = 0;
        while (true) {
            const uint8_t b = Byte();
            v |= static_cast<uint64_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<int64_t>(v);
    }

    size_t remaining() const { return _bytes.size() - _pos; }

private:
    const std::vector<uint8_t>& _bytes;
    size_t _pos = 0;
};

void AppendUtf8(ByteReader& r, std::string& dst, int32_t length) {
    // For ASCII inputs only — each wide-char round-trips as a single byte.
    for (int32_t i = 0; i < length; ++i) {
        dst.push_back(static_cast<char>(r.Byte()));
    }
}

} // namespace

TEST(SegmentWriterTest, EmptyBufferEmitsHeaderAndFooterOnly) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    buffer.Sort();
    const int64_t emitted = w.Emit(buffer, /*field=*/0);
    w.Close();

    EXPECT_EQ(emitted, 0);
    EXPECT_EQ(w.TermCount(), 0);
    // .frq / .prx must remain empty (no docs ⇒ no bytes).
    EXPECT_TRUE(frq.bytes().empty());
    EXPECT_TRUE(prx.bytes().empty());
    // .tis is just header (24 bytes: int + long + int*3) + footer (long
    // size = 0 ⇒ 8 bytes).
    EXPECT_EQ(tis.bytes().size(), 32U);
}

TEST(SegmentWriterTest, SingleTermSingleDocRoundTrip) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    buffer.Append("apple", /*doc=*/3, /*pos=*/0);
    buffer.Sort();

    const int64_t emitted = w.Emit(buffer, 0);
    w.Close();

    EXPECT_EQ(emitted, 1);
    EXPECT_EQ(w.TermCount(), 1);

    // .frq: SLIM kDefault block (df < skip_interval) — NO codec byte, NO
    // VInt(doc_count). Just one doc encoded as ((3 << 1) | 1) = 7 (1 B).
    ByteReader fr(frq.bytes());
    EXPECT_EQ(fr.ReadVInt(), 7);
    EXPECT_EQ(fr.remaining(), 0U) << "df < skip_interval ⇒ slim block, no skip block";

    // .prx: one position delta 0 ⇒ single byte 0x00.
    ByteReader pr(prx.bytes());
    EXPECT_EQ(pr.Byte(), 0x00) << "prox block raw-mode header byte";
    EXPECT_EQ(pr.ReadVInt(), 0);
    EXPECT_EQ(pr.remaining(), 0U);

    // .tis: header (20 bytes) → one entry → footer.
    ByteReader r(tis.bytes());
    r.ReadInt();
    r.ReadLong();
    r.ReadInt();
    r.ReadInt();
    r.ReadInt();
    // Entry: prefix=0, suffix=5, "apple", field=0, df=1, fp_delta=0, pp_delta=0.
    EXPECT_EQ(r.ReadVInt(), 0);
    EXPECT_EQ(r.ReadVInt(), 5);
    std::string suffix;
    AppendUtf8(r, suffix, 5);
    EXPECT_EQ(suffix, "apple");
    EXPECT_EQ(r.ReadVInt(), 0);  // field
    EXPECT_EQ(r.ReadVInt(), 1);  // df
    EXPECT_EQ(r.ReadVLong(), 0); // fp delta
    EXPECT_EQ(r.ReadVLong(), 0); // pp delta
    EXPECT_EQ(r.ReadLong(), 1);  // footer: tis size
}

TEST(SegmentWriterTest, MultipleTermsAreEmittedInSortedOrder) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    // Append out of order to exercise the sort.
    buffer.Append("banana", 1, 0);
    buffer.Append("apple", 0, 0);
    buffer.Append("apple", 2, 1);
    buffer.Append("cherry", 3, 0);
    buffer.Sort();

    const int64_t emitted = w.Emit(buffer, 0);
    w.Close();

    EXPECT_EQ(emitted, 3);

    // .frq: 3 terms emit sequentially as SLIM kDefault blocks (df < skip_interval)
    // — NO codec byte, NO VInt(doc_count). Just per-doc deltas back-to-back.
    //   apple (df=2): doc 0 freq 1 → 1; doc 2 (delta 2) freq 1 → 5.
    //   banana (df=1): doc 1 freq 1 → 3.
    //   cherry (df=1): doc 3 freq 1 → 7.
    ByteReader fr(frq.bytes());
    EXPECT_EQ(fr.ReadVInt(), 1); // apple doc 0
    EXPECT_EQ(fr.ReadVInt(), 5); // apple doc 2
    EXPECT_EQ(fr.ReadVInt(), 3); // banana doc 1
    EXPECT_EQ(fr.ReadVInt(), 7); // cherry doc 3
    EXPECT_EQ(fr.remaining(), 0U);
}

TEST(SegmentWriterTest, MultiPositionDocsPreserveOrder) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    // "the" appears multiple times in doc 5 (positions 0, 4, 9) and once in
    // doc 8 (position 2).
    buffer.Append("the", 5, 0);
    buffer.Append("the", 5, 4);
    buffer.Append("the", 5, 9);
    buffer.Append("the", 8, 2);
    buffer.Sort();

    w.Emit(buffer, 0);
    w.Close();

    // .frq: one term "the" as a SLIM kDefault block (df=2 < skip_interval) —
    // NO codec byte, NO VInt(doc_count). doc 5 freq 3 → (5<<1)=10 then freq=3;
    // doc 8 delta=3 freq 1 → (3<<1)|1 = 7.
    ByteReader fr(frq.bytes());
    EXPECT_EQ(fr.ReadVInt(), 10);
    EXPECT_EQ(fr.ReadVInt(), 3);
    EXPECT_EQ(fr.ReadVInt(), 7);
    EXPECT_EQ(fr.remaining(), 0U);

    // .prx: doc 5 positions 0,4,9 → deltas 0, 4, 5. doc 8 position 2 → delta 2.
    ByteReader pr(prx.bytes());
    EXPECT_EQ(pr.Byte(), 0x00) << "prox block raw-mode header byte";
    EXPECT_EQ(pr.ReadVInt(), 0);
    EXPECT_EQ(pr.ReadVInt(), 4);
    EXPECT_EQ(pr.ReadVInt(), 5);
    EXPECT_EQ(pr.ReadVInt(), 2);
    EXPECT_EQ(pr.remaining(), 0U);
}

TEST(SegmentWriterTest, FrontCodingAcrossTermsUsesTermDict) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    buffer.Append("apple", 0, 0);
    buffer.Append("apply", 1, 0);
    buffer.Sort();

    w.Emit(buffer, 0);
    w.Close();

    ByteReader r(tis.bytes());
    // Skip header.
    r.ReadInt();
    r.ReadLong();
    r.ReadInt();
    r.ReadInt();
    r.ReadInt();
    // "apple" : prefix=0, suffix=5.
    EXPECT_EQ(r.ReadVInt(), 0);
    EXPECT_EQ(r.ReadVInt(), 5);
    std::string s1;
    AppendUtf8(r, s1, 5);
    EXPECT_EQ(s1, "apple");
    EXPECT_EQ(r.ReadVInt(), 0); // field
    EXPECT_EQ(r.ReadVInt(), 1); // df
    EXPECT_EQ(r.ReadVLong(), 0);
    EXPECT_EQ(r.ReadVLong(), 0);
    // "apply" shares "appl" → prefix=4, suffix=1.
    EXPECT_EQ(r.ReadVInt(), 4);
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.Byte(), static_cast<uint8_t>('y'));
    EXPECT_EQ(r.ReadVInt(), 0); // field
    EXPECT_EQ(r.ReadVInt(), 1); // df
    EXPECT_GT(r.ReadVLong(), 0) << "freq pointer delta > 0";
}

TEST(SegmentWriterTest, LargeTermEmitsSkipList) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx,
                    /*index_interval=*/128,
                    /*skip_interval=*/4,
                    /*max_skip_levels=*/2);
    SpimiPostingBuffer buffer;
    // One term, 8 docs (>= skip_interval), each with a single position.
    for (int d = 0; d < 8; ++d) {
        buffer.Append("freq", d, 0);
    }
    buffer.Sort();
    w.Emit(buffer, 0);
    w.Close();

    // The .tis entry for "freq" must contain a non-zero skip_offset. We
    // re-decode the entry to confirm.
    ByteReader r(tis.bytes());
    r.ReadInt();
    r.ReadLong();
    r.ReadInt();
    r.ReadInt();
    r.ReadInt();
    EXPECT_EQ(r.ReadVInt(), 0);
    EXPECT_EQ(r.ReadVInt(), 4); // suffix "freq"
    std::string s;
    AppendUtf8(r, s, 4);
    EXPECT_EQ(s, "freq");
    EXPECT_EQ(r.ReadVInt(), 0); // field
    EXPECT_EQ(r.ReadVInt(), 8); // df
    EXPECT_EQ(r.ReadVLong(), 0);
    EXPECT_EQ(r.ReadVLong(), 0);
    EXPECT_GT(r.ReadVInt(), 0) << "df >= skip_interval ⇒ skip_offset > 0";
}

TEST(SegmentWriterTest, CloseIsIdempotent) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    buffer.Append("x", 0, 0);
    buffer.Sort();
    w.Emit(buffer, 0);
    w.Close();
    const auto bytes_first = tis.bytes();
    w.Close();
    EXPECT_EQ(tis.bytes(), bytes_first);
}

namespace {

// Fills `b` with a compact-mode-triggering, monotonic workload: `docs` docs,
// 16 terms, each term appearing `per_doc` times per doc at increasing
// positions. With docs*16*per_doc >> 512 records and avg occ/term >> 32 the
// buffer enters compact mode; doc/position order is monotonic so the streams
// stay sorted and the direct-emit fast path is eligible. `docs` controls the
// per-term doc frequency, which selects the .frq encoder mode (>=512 ⇒ PFOR).
void BuildMonotonicCompact(SpimiPostingBuffer& b, uint32_t docs, uint32_t per_doc) {
    for (uint32_t doc = 0; doc < docs; ++doc) {
        uint32_t pos = 0;
        for (int t = 0; t < 16; ++t) {
            const std::string term = "term" + std::to_string(t);
            for (uint32_t r = 0; r < per_doc; ++r) {
                b.Append(term, doc, pos++);
            }
        }
    }
}

// Emits `buffer` (already Sort()ed) into fresh memory streams and returns the
// four output byte vectors.
struct SegmentBytes {
    std::vector<uint8_t> tis, tii, frq, prx;
};
SegmentBytes EmitToBytes(const SpimiPostingBuffer& buffer) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    w.Emit(buffer, /*field=*/0);
    w.Close();
    return SegmentBytes {
            .tis = tis.bytes(), .tii = tii.bytes(), .frq = frq.bytes(), .prx = prx.bytes()};
}

// The compact direct-emit path (Sort(allow_direct_emit=true)) must produce
// byte-for-byte identical .tis/.tii/.frq/.prx output to the legacy
// records-materialization path (Sort(false)) on the same input.
void ExpectDirectEmitMatchesRecords(uint32_t docs, uint32_t per_doc) {
    SpimiPostingBuffer direct;
    SpimiPostingBuffer records;
    BuildMonotonicCompact(direct, docs, per_doc);
    BuildMonotonicCompact(records, docs, per_doc);

    direct.Sort(/*allow_direct_emit=*/true);
    records.Sort(/*allow_direct_emit=*/false);

    ASSERT_TRUE(direct.CompactDirectEmitReady())
            << "expected compact direct-emit path for docs=" << docs;
    ASSERT_FALSE(records.CompactDirectEmitReady());

    const SegmentBytes a = EmitToBytes(direct);
    const SegmentBytes b = EmitToBytes(records);
    EXPECT_EQ(a.tis, b.tis);
    EXPECT_EQ(a.tii, b.tii);
    EXPECT_EQ(a.frq, b.frq);
    EXPECT_EQ(a.prx, b.prx);
}

} // namespace

TEST(SegmentWriterTest, CompactDirectEmitMatchesRecordsPathVInt) {
    // df = 100 < skip_interval ⇒ .frq uses the kDefault VInt block mode.
    ExpectDirectEmitMatchesRecords(/*docs=*/100, /*per_doc=*/2);
}

TEST(SegmentWriterTest, CompactDirectEmitMatchesRecordsPathPfor) {
    // df = 600 >= skip_interval (512) ⇒ .frq uses the SPIMI PFOR block mode.
    ExpectDirectEmitMatchesRecords(/*docs=*/600, /*per_doc=*/2);
}

// Independent-oracle test for the StreamVByte block codec. Builds known
// monotonic postings with per-term occurrence counts spanning the
// kPostingBlock(=64) boundary (1, 64, 65, 128, 200) plus a freq>1 term,
// triggers compact mode, then decodes each term's streams via
// DecodeCompactTerm and asserts they equal the appended truth recorded by the
// test itself. The expected values come from the test's own bookkeeping — NOT
// from the records-materialization path (which now shares the same decoder) —
// so a prev-threading or block-boundary decode bug cannot pass unnoticed.
TEST(SegmentWriterTest, CompactStreamDecodeMatchesIndependentTruth) {
    SpimiPostingBuffer buf;
    std::map<std::string, std::vector<std::pair<uint32_t, uint32_t>>> truth;

    // 30 single-occurrence-per-doc terms with counts cycling through the
    // block-boundary cases. 30 terms * ~92 avg > 512 records and avg > 32 ⇒
    // compaction fires; every term is monotonic ⇒ direct-emit is eligible.
    const std::vector<uint32_t> counts = {1, 64, 65, 128, 200};
    for (int t = 0; t < 30; ++t) {
        const std::string term = "term" + std::to_string(t);
        const uint32_t n = counts[static_cast<size_t>(t) % counts.size()];
        for (uint32_t i = 0; i < n; ++i) {
            const uint32_t doc = i;           // one occurrence per doc, ascending
            const uint32_t pos = i * 2U + 1U; // ascending positions
            buf.Append(term, doc, pos);
            truth[term].emplace_back(doc, pos);
        }
    }
    // A freq>1 term: each doc holds three ascending positions (exercises the
    // .prx per-doc reset and multi-position decode across block boundaries).
    for (uint32_t d = 0; d < 40; ++d) {
        for (uint32_t p = 0; p < 3; ++p) {
            buf.Append("multi", d, p);
            truth["multi"].emplace_back(d, p);
        }
    }

    buf.Sort(/*allow_direct_emit=*/true);
    ASSERT_TRUE(buf.CompactDirectEmitReady()) << "expected compact monotonic streams";

    std::vector<uint32_t> docs;
    std::vector<uint32_t> positions;
    size_t verified = 0;
    for (const auto& ref : buf.SortedCompactTerms()) {
        const std::string term(buf.TermAt(ref.text_ref));
        buf.DecodeCompactTerm(ref.term_id, docs, positions);
        const auto& exp = truth[term];
        ASSERT_EQ(docs.size(), exp.size()) << "term=" << term;
        ASSERT_EQ(positions.size(), exp.size()) << "term=" << term;
        for (size_t i = 0; i < exp.size(); ++i) {
            EXPECT_EQ(docs[i], exp[i].first) << "term=" << term << " i=" << i;
            EXPECT_EQ(positions[i], exp[i].second) << "term=" << term << " i=" << i;
        }
        ++verified;
    }
    EXPECT_EQ(verified, truth.size());
}

// Exercises the non-monotonic compact path: the SAME (term, doc, position)
// multiset is appended two ways — sorted (the monotonic oracle, which takes the
// direct-emit fast path) and reversed (non-monotonic, which must set
// _compact_streams_sorted=false and go through DecodeTermToRecords +
// global-sort materialization). Each term has 100 occurrences, so the streams
// cross the kPostingBlock(=64) boundary. Both must emit byte-identical
// segments, since Sort() canonicalizes both to the same (term, doc, pos) order.
TEST(SegmentWriterTest, NonMonotonicCompactMatchesSortedReference) {
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> triples;
    for (int t = 0; t < 20; ++t) {
        const std::string term = "term" + std::to_string(t);
        for (uint32_t d = 0; d < 100; ++d) {
            triples.emplace_back(term, d, d % 7U);
        }
    }
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> sorted = triples;
    std::ranges::sort(sorted);
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> reversed = sorted;
    std::ranges::reverse(reversed);

    SpimiPostingBuffer mono;
    for (const auto& [term, d, p] : sorted) {
        mono.Append(term, d, p);
    }
    SpimiPostingBuffer nonmono;
    for (const auto& [term, d, p] : reversed) {
        nonmono.Append(term, d, p);
    }

    mono.Sort(/*allow_direct_emit=*/true);
    nonmono.Sort(/*allow_direct_emit=*/true);
    EXPECT_TRUE(mono.CompactDirectEmitReady()) << "sorted append should be monotonic";
    EXPECT_FALSE(nonmono.CompactDirectEmitReady())
            << "reversed append must force the materialization path";

    const SegmentBytes a = EmitToBytes(mono);
    const SegmentBytes b = EmitToBytes(nonmono);
    EXPECT_EQ(a.tis, b.tis);
    EXPECT_EQ(a.tii, b.tii);
    EXPECT_EQ(a.frq, b.frq);
    EXPECT_EQ(a.prx, b.prx);
}

} // namespace doris::segment_v2::inverted_index::spimi
