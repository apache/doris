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

// Round-trip differential test: drive randomised (term, doc, pos) input
// through the SPIMI SegmentWriter, then decode the four output files
// (.tis / .tii / .frq / .prx) with a hand-written Lucene 2.x format reader
// embedded in the test, and confirm the reconstructed records equal the
// input. This is the safety net while the SPIMI write path is being
// integrated into InvertedIndexColumnWriter (Phase 7b); a regression in
// the byte format is caught here before it reaches the integration test.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "gen_cpp/segment_v2.pb.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_writer.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"
#include "util/block_compression.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

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

    size_t pos() const { return _pos; }

    // Random byte access without advancing the cursor.
    uint8_t byte_at(size_t i) const { return _bytes[i]; }

    // Seek absolute position.
    void Seek(size_t p) { _pos = p; }

private:
    const std::vector<uint8_t>& _bytes;
    size_t _pos = 0;
};

// Decodes one wide char using the same modified-UTF-8 rule the writer uses
// (ByteOutput::WriteSCharsFromWide).
wchar_t DecodeWideChar(ByteReader& r) {
    const uint8_t b0 = r.Byte();
    if ((b0 & 0x80U) == 0) {
        return static_cast<wchar_t>(b0);
    }
    if ((b0 & 0xE0U) == 0xC0U) {
        const uint8_t b1 = r.Byte();
        return static_cast<wchar_t>(((b0 & 0x1FU) << 6) | (b1 & 0x3FU));
    }
    if ((b0 & 0xF0U) == 0xE0U) {
        const uint8_t b1 = r.Byte();
        const uint8_t b2 = r.Byte();
        return static_cast<wchar_t>(((b0 & 0x0FU) << 12) | ((b1 & 0x3FU) << 6) | (b2 & 0x3FU));
    }
    // 4-byte modified path: leading byte has high bit set.
    const uint8_t b1 = r.Byte();
    const uint8_t b2 = r.Byte();
    const uint8_t b3 = r.Byte();
    return static_cast<wchar_t>(((b0 & 0x07U) << 18) | ((b1 & 0x3FU) << 12) | ((b2 & 0x3FU) << 6) |
                                (b3 & 0x3FU));
}

struct DecodedTermEntry {
    std::wstring term_wide;
    int32_t field_number = 0;
    int32_t doc_freq = 0;
    int64_t freq_pointer = 0; // absolute
    int64_t prox_pointer = 0; // absolute
    int32_t skip_offset = 0;
};

// Walks `.tis` end-to-end (header + entries + footer) and yields one
// DecodedTermEntry per entry. Validates the front-coded encoding.
std::vector<DecodedTermEntry> DecodeTis(const std::vector<uint8_t>& tis_bytes,
                                        int32_t skip_interval) {
    ByteReader r(tis_bytes);
    EXPECT_EQ(r.ReadInt(), TermDictWriter::kFormat);
    EXPECT_EQ(r.ReadLong(), -1);
    (void)r.ReadInt(); // index_interval (varies by test)
    EXPECT_EQ(r.ReadInt(), skip_interval);
    EXPECT_EQ(r.ReadInt(), TermDictWriter::kMaxSkipLevels);

    std::vector<DecodedTermEntry> entries;
    std::wstring prev_term;
    int64_t cum_freq = 0;
    int64_t cum_prox = 0;

    // We can't tell from the format alone where entries end; the footer is
    // a single Long at the very end (term count). Track expected end as
    // `tis_bytes.size() - 8`.
    const size_t footer_offset = tis_bytes.size() - 8;
    while (r.pos() < footer_offset) {
        DecodedTermEntry entry;
        const int32_t prefix = r.ReadVInt();
        const int32_t suffix = r.ReadVInt();
        std::wstring term;
        term.reserve(static_cast<size_t>(prefix + suffix));
        if (prefix > 0) {
            term.append(prev_term, 0, static_cast<size_t>(prefix));
        }
        for (int32_t i = 0; i < suffix; ++i) {
            term.push_back(DecodeWideChar(r));
        }
        entry.term_wide = term;
        entry.field_number = r.ReadVInt();
        entry.doc_freq = r.ReadVInt();
        cum_freq += r.ReadVLong();
        cum_prox += r.ReadVLong();
        entry.freq_pointer = cum_freq;
        entry.prox_pointer = cum_prox;
        if (entry.doc_freq >= skip_interval) {
            entry.skip_offset = r.ReadVInt();
        }
        entries.push_back(entry);
        prev_term = term;
    }
    // Footer: term count.
    const int64_t size = r.ReadLong();
    EXPECT_EQ(size, static_cast<int64_t>(entries.size()));
    return entries;
}

struct DecodedDoc {
    int32_t doc_id;
    std::vector<int32_t> positions;
};

// Decodes `term_count` consecutive docs from `.frq` starting at the term's
// freq_pointer. Each term's `.frq` payload opens with a CodeMode byte:
//   - 0x00 = kDefault: `VInt(doc_count) + per-doc (deltaDoc<<1|freq_bit)`
//   - 0x05 = kSpimiPfor (Phase 35): one or more `SpimiPforEncoder` blocks
//     covering all doc_deltas, followed by (if has_prox) the same number
//     of blocks for freqs.
// Returns the reconstructed (doc_id, freq) pairs.
std::vector<std::pair<int32_t, int32_t>> DecodeFrqDocs(ByteReader& fr, int32_t doc_freq,
                                                       bool is_slim) {
    std::vector<std::pair<int32_t, int32_t>> docs;
    docs.reserve(doc_freq);
    if (is_slim) {
        // SLIM kDefault block (df < skip_interval): NO codec byte, NO doc count.
        // Just per-doc (deltaDoc<<1|freq_bit) VInts.
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            const int32_t code = fr.ReadVInt();
            const auto delta = static_cast<int32_t>(static_cast<uint32_t>(code) >> 1U);
            const int32_t doc_id = last_doc + delta;
            last_doc = doc_id;
            int32_t freq = ((code & 1) != 0) ? 1 : fr.ReadVInt();
            docs.emplace_back(doc_id, freq);
        }
        return docs;
    }
    const uint8_t code_mode = fr.Byte();
    EXPECT_EQ(code_mode, 0x05U) << "non-slim .frq must be PFOR (df >= skip_interval)";
    // Phase 35 PFOR mode: decode N doc-delta sub-blocks then (with
    // prox) N freq sub-blocks. Each sub-block is byte(width) + bitpacked
    // payload (+ optional patch trailer) — same shape SpimiPforEncoder
    // emits. The block does NOT store its value count: blocks are
    // kBlockSize values except the last, so the count is derived here
    // exactly as the production run decoder does.
    auto decode_chunks = [&fr](int32_t total) {
        std::vector<uint32_t> values;
        values.reserve(total);
        int32_t collected = 0;
        while (collected < total) {
            const int32_t n = static_cast<int32_t>(std::min<int64_t>(
                    static_cast<int64_t>(SpimiPforEncoder::kBlockSize), total - collected));
            const size_t mark = fr.pos();
            const uint8_t raw_width = fr.Byte();
            const bool patched = (raw_width & 0x80U) != 0U;
            const uint8_t width = raw_width & 0x3FU;
            const size_t bit_bytes = (static_cast<size_t>(n) * width + 7U) / 8U;
            // Total on-wire span of this sub-block: width byte + bitpacked
            // payload, plus the patch trailer when 0x80 is set.
            size_t span = (fr.pos() - mark) + bit_bytes;
            if (patched) {
                // Trailer = byte k, byte except_width, k position bytes,
                // ceil(k*except_width/8) high-bit bytes.
                const uint8_t k = fr.byte_at(mark + (fr.pos() - mark) + bit_bytes);
                const uint8_t except_width = fr.byte_at(mark + (fr.pos() - mark) + bit_bytes + 1);
                const size_t high_bytes = (static_cast<size_t>(k) * except_width + 7U) / 8U;
                span += 2U + static_cast<size_t>(k) + high_bytes;
            }
            // Slice [mark, mark + span) is one complete sub-block.
            std::vector<uint8_t> slice;
            slice.reserve(span);
            for (size_t i = mark; i < mark + span; ++i) {
                slice.push_back(fr.byte_at(i));
            }
            std::vector<uint32_t> sub;
            SpimiPforDecoder::DecodeBlockFromBytes(slice, static_cast<size_t>(n), &sub);
            for (uint32_t v : sub) {
                values.push_back(v);
            }
            collected += static_cast<int32_t>(sub.size());
            // Advance cursor past everything after the already-consumed
            // width byte.
            for (size_t i = (fr.pos() - mark); i < span; ++i) {
                (void)fr.Byte();
            }
        }
        return values;
    };
    const auto doc_deltas = decode_chunks(doc_freq);
    const auto freqs = decode_chunks(doc_freq);
    int32_t last_doc = 0;
    for (int32_t i = 0; i < doc_freq; ++i) {
        last_doc += static_cast<int32_t>(doc_deltas[i]);
        docs.emplace_back(last_doc, static_cast<int32_t>(freqs[i]));
    }
    return docs;
}

// Decodes the positions for a single doc with `freq` positions, starting at
// `pr`'s current cursor.
std::vector<int32_t> DecodeProxPositions(ByteReader& pr, int32_t freq) {
    std::vector<int32_t> positions;
    positions.reserve(freq);
    int32_t last_pos = 0;
    for (int32_t i = 0; i < freq; ++i) {
        const int32_t delta = pr.ReadVInt();
        last_pos += delta;
        positions.push_back(last_pos);
    }
    return positions;
}

// Reconstructs the per-term doc-and-position set from the four segment
// streams.
struct ReconstructedTerm {
    std::wstring term_wide;
    std::vector<DecodedDoc> docs;
};

// Resolves a term's prox block (which begins with a 1-byte mode header: raw
// VInt deltas, or a ZSTD-compressed payload) into the raw VInt position-delta
// bytes for the term, mirroring SpimiProxReader.
std::vector<uint8_t> ReadTermProxRaw(const std::vector<uint8_t>& prx, int64_t prox_pointer) {
    ByteReader h(prx);
    h.Seek(static_cast<size_t>(prox_pointer));
    const uint8_t mode = h.Byte();
    if (mode == 1) { // ZSTD
        const auto uncomp = static_cast<uint32_t>(h.ReadVInt());
        const auto comp = static_cast<uint32_t>(h.ReadVInt());
        const size_t comp_off = h.pos();
        std::vector<uint8_t> raw(uncomp);
        BlockCompressionCodec* codec = nullptr;
        EXPECT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());
        Slice in(reinterpret_cast<const char*>(prx.data() + comp_off), comp);
        Slice out(reinterpret_cast<char*>(raw.data()), uncomp);
        EXPECT_TRUE(codec->decompress(in, &out).ok());
        return raw;
    }
    EXPECT_EQ(mode, 0) << "unknown prox mode";
    // Raw: return the tail from just past the mode byte; DecodeProxPositions
    // reads only the freq-bounded number of VInts.
    return {prx.begin() + h.pos(), prx.end()};
}

// Resolves a term's .frq block into its raw (uncompressed) bytes, mirroring
// SpimiTermDocsReader: a large block is wrapped in a kCodeModeZstd envelope
// (VInt(uncomp) VInt(comp) ZSTD-payload); a small block is the raw CodeMode
// block verbatim. DecodeFrqDocs then reads the inner kDefault/kSpimiPfor block.
std::vector<uint8_t> ReadTermFrqRaw(const std::vector<uint8_t>& frq, int64_t freq_pointer,
                                    bool is_slim) {
    // A SLIM kDefault block (df < skip_interval) carries no codec byte and is
    // never ZSTD-wrapped, so return its bytes verbatim (DecodeFrqDocs reads only
    // the doc_freq-bounded VInt deltas).
    if (is_slim) {
        return {frq.begin() + static_cast<size_t>(freq_pointer), frq.end()};
    }
    ByteReader h(frq);
    h.Seek(static_cast<size_t>(freq_pointer));
    const uint8_t mode = h.Byte();
    if (mode == FreqProxEncoder::kCodeModeZstd) {
        const auto uncomp = static_cast<uint32_t>(h.ReadVInt());
        const auto comp = static_cast<uint32_t>(h.ReadVInt());
        const size_t comp_off = h.pos();
        std::vector<uint8_t> raw(uncomp);
        BlockCompressionCodec* codec = nullptr;
        EXPECT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());
        Slice in(reinterpret_cast<const char*>(frq.data() + comp_off), comp);
        Slice out(reinterpret_cast<char*>(raw.data()), uncomp);
        EXPECT_TRUE(codec->decompress(in, &out).ok());
        return raw;
    }
    // Raw CodeMode block: return the tail from freq_pointer (mode byte included);
    // DecodeFrqDocs reads only doc_freq-bounded data and ignores trailing skip.
    return {frq.begin() + static_cast<size_t>(freq_pointer), frq.end()};
}

std::vector<ReconstructedTerm> ReconstructSegment(const std::vector<uint8_t>& tis,
                                                  const std::vector<uint8_t>& frq,
                                                  const std::vector<uint8_t>& prx,
                                                  int32_t skip_interval) {
    const std::vector<DecodedTermEntry> entries = DecodeTis(tis, skip_interval);
    std::vector<ReconstructedTerm> result;
    result.reserve(entries.size());
    for (const auto& entry : entries) {
        const bool is_slim = entry.doc_freq < skip_interval;
        const std::vector<uint8_t> term_frq = ReadTermFrqRaw(frq, entry.freq_pointer, is_slim);
        ByteReader fr(term_frq);
        const auto doc_freqs = DecodeFrqDocs(fr, entry.doc_freq, is_slim);
        const std::vector<uint8_t> term_prox = ReadTermProxRaw(prx, entry.prox_pointer);
        ByteReader pr(term_prox);
        ReconstructedTerm rt;
        rt.term_wide = entry.term_wide;
        rt.docs.reserve(doc_freqs.size());
        for (const auto& [doc_id, freq] : doc_freqs) {
            DecodedDoc dd;
            dd.doc_id = doc_id;
            dd.positions = DecodeProxPositions(pr, freq);
            rt.docs.push_back(std::move(dd));
        }
        result.push_back(std::move(rt));
    }
    return result;
}

} // namespace

TEST(SegmentRoundtripTest, ReconstructsHandCraftedInputExactly) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);

    SpimiPostingBuffer buffer;
    buffer.Append("alpha", 0, 0);
    buffer.Append("alpha", 0, 5);
    buffer.Append("alpha", 1, 2);
    buffer.Append("beta", 0, 1);
    buffer.Append("beta", 2, 3);
    buffer.Append("beta", 2, 9);
    buffer.Append("gamma", 1, 0);
    buffer.Append("gamma", 3, 7);
    buffer.Sort();
    w.Emit(buffer, /*field=*/0);
    w.Close();

    const auto reconstructed = ReconstructSegment(tis.bytes(), frq.bytes(), prx.bytes(),
                                                  TermDictWriter::kDefaultSkipInterval);

    ASSERT_EQ(reconstructed.size(), 3U);
    EXPECT_EQ(reconstructed[0].term_wide, std::wstring(L"alpha"));
    ASSERT_EQ(reconstructed[0].docs.size(), 2U);
    EXPECT_EQ(reconstructed[0].docs[0].doc_id, 0);
    EXPECT_EQ(reconstructed[0].docs[0].positions, (std::vector<int32_t> {0, 5}));
    EXPECT_EQ(reconstructed[0].docs[1].doc_id, 1);
    EXPECT_EQ(reconstructed[0].docs[1].positions, (std::vector<int32_t> {2}));

    EXPECT_EQ(reconstructed[1].term_wide, std::wstring(L"beta"));
    ASSERT_EQ(reconstructed[1].docs.size(), 2U);
    EXPECT_EQ(reconstructed[1].docs[0].doc_id, 0);
    EXPECT_EQ(reconstructed[1].docs[0].positions, (std::vector<int32_t> {1}));
    EXPECT_EQ(reconstructed[1].docs[1].doc_id, 2);
    EXPECT_EQ(reconstructed[1].docs[1].positions, (std::vector<int32_t> {3, 9}));

    EXPECT_EQ(reconstructed[2].term_wide, std::wstring(L"gamma"));
    ASSERT_EQ(reconstructed[2].docs.size(), 2U);
}

TEST(SegmentRoundtripTest, RandomisedInputReconstructsBitForBit) {
    // Deterministic seed so failures reproduce.
    std::mt19937 rng(0xA5A5A5A5U);
    std::uniform_int_distribution<int32_t> doc_dist(0, 999);
    std::uniform_int_distribution<int32_t> pos_dist(0, 10000);
    std::uniform_int_distribution<int32_t> term_dist(0, 49);

    // Build a "ground truth" map: term -> doc -> positions[].
    std::map<std::string, std::map<int32_t, std::vector<int32_t>>> truth;

    SpimiPostingBuffer buffer;
    constexpr int kRecordCount = 5000;
    for (int i = 0; i < kRecordCount; ++i) {
        char term[16];
        std::snprintf(term, sizeof(term), "t%04d", term_dist(rng));
        const int32_t doc = doc_dist(rng);
        const int32_t pos = pos_dist(rng);
        buffer.Append(term, static_cast<uint32_t>(doc), static_cast<uint32_t>(pos));
        truth[term][doc].push_back(pos);
    }
    buffer.Sort();

    // Normalise truth: dedupe identical positions per doc and sort.
    for (auto& [term, docs] : truth) {
        for (auto& [doc, positions] : docs) {
            std::sort(positions.begin(), positions.end());
            // We keep duplicates: the SPIMI buffer stores every occurrence,
            // including duplicate positions at the same doc.
        }
    }

    MemoryByteOutput tis, tii, frq, prx;
    // Use a small skip interval to exercise the skip-list code path with
    // realistic doc counts.
    SegmentWriter w(&tis, &tii, &frq, &prx,
                    /*index_interval=*/64,
                    /*skip_interval=*/16,
                    /*max_skip_levels=*/4);
    w.Emit(buffer, 0);
    w.Close();

    const auto reconstructed = ReconstructSegment(tis.bytes(), frq.bytes(), prx.bytes(),
                                                  /*skip_interval=*/16);

    ASSERT_EQ(reconstructed.size(), truth.size());
    size_t i = 0;
    for (const auto& [term, docs] : truth) {
        EXPECT_EQ(reconstructed[i].term_wide, Utf8ToWide(term)) << "term mismatch at index " << i;
        ASSERT_EQ(reconstructed[i].docs.size(), docs.size())
                << "doc-count mismatch for term " << term;
        size_t j = 0;
        for (const auto& [doc, positions] : docs) {
            EXPECT_EQ(reconstructed[i].docs[j].doc_id, doc);
            EXPECT_EQ(reconstructed[i].docs[j].positions, positions)
                    << "positions mismatch for term " << term << " doc " << doc;
            ++j;
        }
        ++i;
    }
}

TEST(SegmentRoundtripTest, FilePointersInTermDictMatchByteOffsets) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    buffer.Append("a", 0, 0);
    buffer.Append("b", 1, 0);
    buffer.Append("c", 2, 0);
    buffer.Sort();
    w.Emit(buffer, 0);
    w.Close();

    const auto entries = DecodeTis(tis.bytes(), TermDictWriter::kDefaultSkipInterval);
    ASSERT_EQ(entries.size(), 3U);
    EXPECT_EQ(entries[0].freq_pointer, 0);
    EXPECT_EQ(entries[0].prox_pointer, 0);

    // Each term's .frq block is now the SLIM kDefault layout: NO codec byte and
    // NO VInt(doc_count), just the single doc-encoding VInt ⇒ 1 byte per term
    // for docFreq=1. Each term's .prx block is `[prox-mode byte 0x00][VInt 0
    // position]` = 2 bytes (the prox block carries a 1-byte raw/ZSTD mode header,
    // then the position deltas) — unchanged by the slim .frq change.
    EXPECT_EQ(entries[1].freq_pointer, 1);
    EXPECT_EQ(entries[2].freq_pointer, 2);

    EXPECT_EQ(entries[1].prox_pointer, 2);
    EXPECT_EQ(entries[2].prox_pointer, 4);
}

// A term whose whole-term .prx block exceeds kProxCompressMin and is highly
// compressible must take the ZSTD envelope (mode byte kProxZstd) AND round-trip
// bit-for-bit through the decompressing reader. Guards the .prx ZSTD-1 path.
TEST(SegmentRoundtripTest, ProxZstdBlockRoundTripsAndUsesZstdMode) {
    // This test exercises the .prx ZSTD-1 envelope specifically, so pin the
    // small-block-ZSTD-skip threshold to 0 (always attempt ZSTD) — the
    // production default (512) would store this ~300B block raw.
    const int64_t saved = config::inverted_index_spimi_zstd_min_window_bytes;
    config::inverted_index_spimi_zstd_min_window_bytes = 0;
    struct Restore {
        int64_t v;
        ~Restore() { config::inverted_index_spimi_zstd_min_window_bytes = v; }
    } restore {saved};
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    // One doc, 300 ascending positions ⇒ 300 VInt deltas (mostly 0x01) — well
    // above kProxCompressMin (48) and trivially compressible.
    constexpr int kPositions = 300;
    std::vector<int32_t> expect_positions;
    expect_positions.reserve(kPositions);
    for (int32_t p = 0; p < kPositions; ++p) {
        buffer.Append("term", /*doc=*/0, /*pos=*/p);
        expect_positions.push_back(p);
    }
    buffer.Sort();
    w.Emit(buffer, /*field=*/0);
    w.Close();

    // The single term's prox block starts at offset 0; assert the writer chose
    // the ZSTD envelope (otherwise this test would silently not exercise it).
    ASSERT_FALSE(prx.bytes().empty());
    EXPECT_EQ(prx.bytes()[0], FreqProxEncoder::kProxZstd) << "large block must use ZSTD mode";

    const auto reconstructed = ReconstructSegment(tis.bytes(), frq.bytes(), prx.bytes(),
                                                  TermDictWriter::kDefaultSkipInterval);
    ASSERT_EQ(reconstructed.size(), 1U);
    ASSERT_EQ(reconstructed[0].docs.size(), 1U);
    EXPECT_EQ(reconstructed[0].docs[0].doc_id, 0);
    EXPECT_EQ(reconstructed[0].docs[0].positions, expect_positions);
}

// A term whose doc frequency is large (513 docs, past the skip_interval) drives
// the on-disk PFOR + skip-list .frq path. (The old in-memory frame-of-reference
// "graduation" was removed; the in-memory postings are now uniformly slice-chain
// VInt and the PFOR/skip encoding happens only at emit.) Round-trip every doc +
// position to verify that high-DF on-disk path.
TEST(SegmentRoundtripTest, ForGraduationPast512DocsRoundTrips) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    constexpr int kDocs = 513; // 512 boundary + 1
    for (int32_t d = 0; d < kDocs; ++d) {
        buffer.Append("t", /*doc=*/d, /*pos=*/d % 7); // a position per doc
    }
    buffer.Sort();
    w.Emit(buffer, /*field=*/0);
    w.Close();

    const auto reconstructed = ReconstructSegment(tis.bytes(), frq.bytes(), prx.bytes(),
                                                  TermDictWriter::kDefaultSkipInterval);
    ASSERT_EQ(reconstructed.size(), 1U);
    ASSERT_EQ(reconstructed[0].docs.size(), static_cast<size_t>(kDocs));
    for (int32_t d = 0; d < kDocs; ++d) {
        EXPECT_EQ(reconstructed[0].docs[d].doc_id, d) << "doc id at " << d;
        EXPECT_EQ(reconstructed[0].docs[d].positions, (std::vector<int32_t> {d % 7}))
                << "positions at doc " << d;
    }
}

// High-DF term (df > skip_interval ⇒ PFOR freq region) whose per-doc freqs
// contain a few large outliers among many freq==1 docs. This drives the
// patched-PFOR (OPT_PFOR_PATCH_FREQS) freq encoding and verifies full .frq
// encode→decode fidelity through the test-local PFOR reconstitution path
// (which now understands the patch trailer, mirroring the two production
// reconstitution loops in term_docs_reader.cpp / posting_decoder.cpp).
TEST(SegmentRoundtripTest, PatchedFreqHighDfRoundTrips) {
    MemoryByteOutput tis, tii, frq, prx;
    SegmentWriter w(&tis, &tii, &frq, &prx);
    SpimiPostingBuffer buffer;
    constexpr int kDocs = 600; // past the 512 skip_interval boundary
    std::vector<int32_t> expected_freq(kDocs, 1);
    for (int32_t d = 0; d < kDocs; ++d) {
        // Most docs have freq 1; a sparse set of docs gets a large freq so a
        // freq sub-block packs at a narrow base width plus a small patch list.
        int32_t freq = 1;
        if (d == 5 || d == 130 || d == 300 || d == 450) {
            freq = 700 + d; // large outlier freq
        }
        expected_freq[d] = freq;
        for (int32_t p = 0; p < freq; ++p) {
            buffer.Append("hot", /*doc=*/d, /*pos=*/p);
        }
    }
    buffer.Sort();
    w.Emit(buffer, /*field=*/0);
    w.Close();

    const auto reconstructed = ReconstructSegment(tis.bytes(), frq.bytes(), prx.bytes(),
                                                  TermDictWriter::kDefaultSkipInterval);
    ASSERT_EQ(reconstructed.size(), 1U);
    ASSERT_EQ(reconstructed[0].docs.size(), static_cast<size_t>(kDocs));
    for (int32_t d = 0; d < kDocs; ++d) {
        EXPECT_EQ(reconstructed[0].docs[d].doc_id, d) << "doc id at " << d;
        EXPECT_EQ(static_cast<int32_t>(reconstructed[0].docs[d].positions.size()), expected_freq[d])
                << "freq (position count) at doc " << d;
    }
}

namespace {

// Wires up a SpimiSegmentSink over the seven required MemoryByteOutput streams
// (EmitSegment DCHECKs all of them non-null). `.nrm` is intentionally left null:
// V4 sets omit_norms=true, so no norm stream is written.
struct EmitStreams {
    MemoryByteOutput tis;
    MemoryByteOutput tii;
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    MemoryByteOutput fnm;
    MemoryByteOutput seg_n;
    MemoryByteOutput seg_gen;

    SpimiSegmentSink Sink() {
        SpimiSegmentSink sink;
        sink.tis = &tis;
        sink.tii = &tii;
        sink.frq = &frq;
        sink.prx = &prx;
        sink.fnm = &fnm;
        sink.nrm = nullptr; // V4 omits norms
        sink.segments_n = &seg_n;
        sink.segments_gen = &seg_gen;
        return sink;
    }
};

// Decodes a DOCS_ONLY slim `.frq` block into its doc-id set. In omit_tfap mode
// the encoder writes each doc as a raw VInt doc-delta — NO low-bit freq flag and
// NO separate freq VInt (FreqProxEncoder::AddDoc omit_tfap branch). So the
// general DecodeFrqDocs (which assumes a freq bit) does not apply; this reads
// `doc_freq` plain delta VInts and accumulates the doc ids.
std::vector<int32_t> DecodeDocsOnlyFrq(const std::vector<uint8_t>& frq, int64_t freq_pointer,
                                       int32_t doc_freq) {
    ByteReader fr(frq);
    fr.Seek(static_cast<size_t>(freq_pointer));
    std::vector<int32_t> doc_ids;
    doc_ids.reserve(static_cast<size_t>(doc_freq));
    int32_t last_doc = 0;
    for (int32_t i = 0; i < doc_freq; ++i) {
        last_doc += fr.ReadVInt();
        doc_ids.push_back(last_doc);
    }
    return doc_ids;
}

} // namespace

// DOCS_ONLY (support_phrase=false) write path: a buffer constructed with
// omit_tfap=true skips the per-term prox slice-chain, and EmitSegment must NOT
// read it. Drives a multi-term, multi-doc, multi-occurrence input (positions
// WOULD be emitted if not omitted) through the V4 (windowed-capable) EmitSegment
// and asserts:
//   1. every term's `.prx` is empty (prox_pointer == 0, .prx stream empty) — no
//      positions were written;
//   2. doc ids reconstruct correctly from `.frq` (raw doc-delta decode) per term;
//   3. emit does not crash and the new buffer<->emit omit DCHECK passes.
// For contrast, the SAME input emitted with omit=false produces a NON-empty
// `.prx` for a multi-occurrence term — proving the assertions actually
// distinguish the omit path.
TEST(SegmentRoundtripTest, DocsOnlyOmitTfapRoundTrips) {
    // Ground truth: term -> sorted distinct doc-id set. Every doc has multiple
    // occurrences (would-be positions) so .prx would be non-empty if not omitted.
    const std::map<std::string, std::vector<int32_t>> truth = {
            {"alpha", {0, 1, 3}},
            {"beta", {0, 2}},
            {"gamma", {1, 2, 4}},
    };

    // Build the omit_tfap=true buffer and append multi-occurrence-per-doc input.
    SpimiPostingBuffer buffer(/*omit_tfap=*/true);
    ASSERT_TRUE(buffer.OmitTfap());
    for (const auto& [term, docs] : truth) {
        for (int32_t doc : docs) {
            // Three occurrences per doc at distinct positions; if positions were
            // NOT omitted these would yield two prox VInt deltas per doc.
            buffer.Append(term, static_cast<uint32_t>(doc), /*pos=*/0);
            buffer.Append(term, static_cast<uint32_t>(doc), /*pos=*/4);
            buffer.Append(term, static_cast<uint32_t>(doc), /*pos=*/9);
        }
    }

    constexpr int32_t kDocCount = 5;

    EmitStreams streams;
    SpimiSegmentSink sink = streams.Sink();
    // V4 index_version -> windowed-capable path; omit_term_freq_and_positions=true
    // exercises DOCS_ONLY. Trailing inline_small_terms defaults to false, so the
    // `.tis` freq/prox pointers stay external offsets into the real streams.
    const int64_t term_count = SpimiFulltextWriter::EmitSegment(
            buffer, sink, /*segment_name=*/"_0", /*field_name=*/"body", kDocCount,
            FieldInfosWriter::kIndexVersionV4, /*omit_term_freq_and_positions=*/true,
            /*omit_norms=*/true, /*out_byte_counts=*/nullptr);
    ASSERT_EQ(term_count, static_cast<int64_t>(truth.size()));

    // Assertion (1): the whole `.prx` stream is empty — DOCS_ONLY never wrote it.
    EXPECT_TRUE(streams.prx.bytes().empty()) << ".prx must be empty in DOCS_ONLY";

    const auto entries = DecodeTis(streams.tis.bytes(), TermDictWriter::kDefaultSkipInterval);
    ASSERT_EQ(entries.size(), truth.size());

    size_t i = 0;
    for (const auto& [term, docs] : truth) {
        const auto& entry = entries[i];
        EXPECT_EQ(entry.term_wide, Utf8ToWide(term)) << "term mismatch at " << i;
        EXPECT_EQ(entry.doc_freq, static_cast<int32_t>(docs.size())) << "doc_freq for " << term;
        // Assertion (1b): every term's prox slice has zero span (pointer stays 0).
        // Don't call ReadTermProxRaw here: it reads a mode byte, which would index
        // past the end of the (correctly) empty `.prx`. The empty-stream check
        // above plus prox_pointer==0 already prove no positions were written.
        EXPECT_EQ(entry.prox_pointer, 0) << "prox_pointer must be 0 for " << term;

        // Assertion (2): doc ids reconstruct from `.frq` (raw doc-delta decode).
        const auto decoded_docs =
                DecodeDocsOnlyFrq(streams.frq.bytes(), entry.freq_pointer, entry.doc_freq);
        EXPECT_EQ(decoded_docs, docs) << "doc set mismatch for " << term;
        ++i;
    }

    // Contrast: the SAME input emitted with omit=false MUST produce a non-empty
    // `.prx` (multi-occurrence docs write position deltas). Proves the omit
    // assertions above actually distinguish the DOCS_ONLY path.
    SpimiPostingBuffer with_prox(/*omit_tfap=*/false);
    ASSERT_FALSE(with_prox.OmitTfap());
    for (const auto& [term, docs] : truth) {
        for (int32_t doc : docs) {
            with_prox.Append(term, static_cast<uint32_t>(doc), /*pos=*/0);
            with_prox.Append(term, static_cast<uint32_t>(doc), /*pos=*/4);
            with_prox.Append(term, static_cast<uint32_t>(doc), /*pos=*/9);
        }
    }
    EmitStreams streams_prox;
    SpimiSegmentSink sink_prox = streams_prox.Sink();
    SpimiFulltextWriter::EmitSegment(with_prox, sink_prox, /*segment_name=*/"_0",
                                     /*field_name=*/"body", kDocCount,
                                     FieldInfosWriter::kIndexVersionV4,
                                     /*omit_term_freq_and_positions=*/false,
                                     /*omit_norms=*/true, /*out_byte_counts=*/nullptr);
    EXPECT_FALSE(streams_prox.prx.bytes().empty())
            << "omit=false must write a non-empty .prx for multi-occurrence terms";
}

} // namespace doris::segment_v2::inverted_index::spimi
