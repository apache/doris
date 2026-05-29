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

#include "storage/index/inverted/spimi/term_docs_reader.h"

// `_CLTHROWA` for byte-parser hard-fail on untrusted .frq bytes.

#include <algorithm>

#include "common/logging.h"
#include "gen_cpp/segment_v2.pb.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"
#include "util/block_compression.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Tiny byte-stream cursor mirroring what `ByteOutput` writes.
// `ByteOutput::WriteVInt` uses 7-bit groups with continuation bit
// 0x80, which matches `IndexInput::readVInt` byte-for-byte.
class ByteStream {
public:
    ByteStream(const uint8_t* data, size_t len) : _data(data), _len(len) {}

    uint8_t ReadByte() {
        if (_pos >= _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq read past end of buffer");
        }
        return _data[_pos++];
    }
    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = ReadByte();
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
            // C++ undefined behaviour if `shift >= 32` and we evaluate
            // `<< shift` on a uint32. A crafted .frq with ≥5 continuation
            // bytes drives shift to 35/42/… — clang lowers `<< 35` to
            // BMI `shlx` which masks the count, silently truncating the
            // decoded value. Hard-fail instead.
            if (shift >= 32U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq VInt: shift overflow on crafted input");
            }
        }
        return static_cast<int32_t>(v);
    }
    void ReadInto(std::vector<uint8_t>* out, size_t n) {
        if (_pos + n > _len || _pos + n < _pos /*overflow*/) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq ReadInto bounds violation");
        }
        out->insert(out->end(), _data + _pos, _data + _pos + n);
        _pos += n;
    }
    size_t pos() const { return _pos; }

private:
    const uint8_t* _data;
    size_t _len;
    size_t _pos = 0;
};

// Decodes consecutive PFOR sub-blocks from `cur` until `count` total
// values have been recovered. Each sub-block is `VInt(n) +
// byte(width) + bitpacked` per `SpimiPforEncoder::EncodeBlock`. The
// reader reconstitutes the on-wire bytes for one sub-block and hands
// them to `SpimiPforDecoder::DecodeBlockFromBytes`, which uses
// Arrow's `BitReader` for the actual bit unpacking — same primitive
// the encoder uses so the bit-width interpretation cannot drift.
std::vector<uint32_t> DecodePforRun(ByteStream& cur, int32_t count) {
    std::vector<uint32_t> values;
    // Cap pre-reserve against a sane upper bound to defeat the
    // crafted-segment "reserve INT32_MAX × 4 B" DoS. 1<<24 (~16M)
    // fits any realistic segment doc count; the actual vector
    // grows organically via push_back if larger.
    constexpr size_t kSafeReserveCap = 1U << 24;
    values.reserve(std::min(static_cast<size_t>(count), kSafeReserveCap));
    int32_t collected = 0;
    while (collected < count) {
        const size_t mark = cur.pos();
        const int32_t n = cur.ReadVInt();
        // Bound `n` against `count` so a malformed PFOR header
        // can't make us loop forever or allocate an astronomical
        // sub-block buffer. count itself is from .tis doc_freq
        // which the upper layer already validated as a sensible
        // bound (≤ max_doc per the segment manifest).
        if (n <= 0 || n > count - collected) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block count out of range");
        }
        const uint8_t width = cur.ReadByte() & 0x3FU;
        // Validate width BEFORE the reserve so a crafted width=63 +
        // n=16M can't allocate ~126 MB before rejection. Encoder
        // emits width ∈ [1, 32]; decoder accepts the same range.
        if (width == 0 || width > 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq invalid PFOR bit width");
        }
        const size_t bit_bytes = (static_cast<size_t>(n) * width + 7U) / 8U;
        std::vector<uint8_t> block;
        block.reserve((cur.pos() - mark) + bit_bytes);
        // Reconstitute the on-wire bytes [mark, cur.pos() + bit_bytes).
        // The VInt(n) + byte(width) bytes have already been consumed by
        // cur above, so re-emit them in order matching what the
        // encoder wrote.
        // VInt encoding of n (1..2 bytes — n ≤ 128 fits in 7 bits +
        // optional continuation byte; we redo the VInt encoding rather
        // than peek-back since ByteStream doesn't have a peek API and
        // adding one would just hide this).
        {
            auto vn = static_cast<uint32_t>(n);
            while ((vn & ~0x7FU) != 0) {
                block.push_back(static_cast<uint8_t>((vn & 0x7FU) | 0x80U));
                vn >>= 7U;
            }
            block.push_back(static_cast<uint8_t>(vn));
        }
        block.push_back(width);
        cur.ReadInto(&block, bit_bytes);
        std::vector<uint32_t> sub;
        SpimiPforDecoder::DecodeBlockFromBytes(block, &sub);
        if (sub.size() != static_cast<size_t>(n)) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block decoded count mismatch");
        }
        values.insert(values.end(), sub.begin(), sub.end());
        collected += n;
    }
    if (collected != count) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq PFOR run total mismatch");
    }
    return values;
}

// Decompresses a whole-term ZSTD `.frq` envelope (the bytes after the
// kCodeModeZstd marker: VInt(uncomp_len) VInt(comp_len) ZSTD-payload) into the
// raw inner block, which itself begins with a kDefault/kPfor mode byte. All
// reads are bounds-checked against the (untrusted) on-disk buffer.
std::vector<uint8_t> DecompressZstdFrqBlock(ByteStream& cur) {
    const auto uncomp = static_cast<uint32_t>(cur.ReadVInt());
    const auto comp = static_cast<uint32_t>(cur.ReadVInt());
    std::vector<uint8_t> packed;
    cur.ReadInto(&packed, comp); // bounds-checked
    std::vector<uint8_t> raw(uncomp);
    BlockCompressionCodec* codec = nullptr;
    if (!get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok() || codec == nullptr)
            [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq: ZSTD codec unavailable");
    }
    Slice in(reinterpret_cast<const char*>(packed.data()), comp);
    Slice slice_out(reinterpret_cast<char*>(raw.data()), uncomp);
    if (!codec->decompress(in, &slice_out).ok() || slice_out.size != uncomp) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq: ZSTD decompress failed");
    }
    return raw;
}

} // namespace

std::vector<SpimiTermDocsReader::DocFreq> SpimiTermDocsReader::ReadTerm(const uint8_t* frq_data,
                                                                        size_t frq_length,
                                                                        int32_t doc_freq,
                                                                        bool has_prox) {
    // Untrusted-byte invariants. `doc_freq` is .tis-validated by the
    // upper layer at this point but defence-in-depth — a negative or
    // zero value on a corrupt segment must not silently produce an
    // empty result. We intentionally do NOT bound `doc_freq` against
    // `frq_length`: PFOR sub-blocks are bit-packed (down to ~0.1 byte
    // per doc at width=1), so doc_freq can legitimately exceed
    // frq_length. The byte-bounds checks inside the decoder
    // (`ByteStream::ReadByte`, `DecodePforRun`) will catch a truly
    // truncated buffer.
    if (doc_freq <= 0 || frq_length == 0U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq ReadTerm: bad doc_freq / buffer length");
    }

    ByteStream cur(frq_data, frq_length);
    const uint8_t mode = cur.ReadByte();
    if (mode == FreqProxEncoder::kCodeModeZstd) {
        // Whole-term .frq was ZSTD-compressed: decompress, then decode the
        // inner block (which begins with its own kDefault/kPfor mode byte).
        const std::vector<uint8_t> raw = DecompressZstdFrqBlock(cur);
        return ReadTerm(raw.data(), raw.size(), doc_freq, has_prox);
    }
    std::vector<DocFreq> out;
    // Cap pre-reserve against the same DoS-bounding upper limit as
    // `DecodePforRun` above. Vector grows organically beyond the
    // cap via push_back if `doc_freq` exceeds 16M.
    constexpr size_t kSafeReserveCap = 1U << 24;
    out.reserve(std::min(static_cast<size_t>(doc_freq), kSafeReserveCap));

    if (mode == FreqProxEncoder::kCodeModeDefault) {
        // Byte-equal to CLucene's kDefault block. The leading
        // `VInt(docCount)` redundantly encodes doc_freq — assert it
        // matches the caller-provided value so a stale .tis entry
        // can't silently corrupt the read.
        const int32_t recorded = cur.ReadVInt();
        if (recorded != doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq kDefault VInt(docCount) disagrees with .tis docFreq");
        }
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            if (has_prox) {
                const auto code = static_cast<uint32_t>(cur.ReadVInt());
                last_doc += static_cast<int32_t>(code >> 1U);
                const int32_t freq = ((code & 1U) != 0) ? 1 : cur.ReadVInt();
                out.emplace_back(last_doc, freq);
            } else {
                last_doc += static_cast<int32_t>(cur.ReadVInt());
                out.emplace_back(last_doc, 1);
            }
        }
        return out;
    }

    if (mode == FreqProxEncoder::kCodeModeSpimiPfor) {
        // Phase 35 PFOR block. Consume doc_freq doc_delta values,
        // then — if has_prox — another doc_freq freqs. The sub-block
        // boundaries are self-describing (each carries VInt(n)) so we
        // simply pull until count is met.
        const auto doc_deltas = DecodePforRun(cur, doc_freq);
        std::vector<uint32_t> freqs;
        if (has_prox) {
            freqs = DecodePforRun(cur, doc_freq);
        }
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            last_doc += static_cast<int32_t>(doc_deltas[static_cast<size_t>(i)]);
            const int32_t f = has_prox ? static_cast<int32_t>(freqs[static_cast<size_t>(i)]) : 1;
            out.emplace_back(last_doc, f);
        }
        return out;
    }

    // Unknown CodeMode byte on a corrupt segment must not crash the
    // BE process. Hard-throw so the search-path catch surfaces it
    // as `INVERTED_INDEX_FILE_CORRUPTED`.
    SPIMI_THROW_CORRUPT("SPIMI .frq unknown CodeMode byte");
}

} // namespace doris::segment_v2::inverted_index::spimi
