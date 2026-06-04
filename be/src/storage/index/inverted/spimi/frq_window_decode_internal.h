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

#pragma once

// Shared internal decode primitives for SPIMI `.frq` term blocks: the
// byte-stream cursor, the PFOR-run decoder, the per-window payload
// inflater, and the whole-term ZSTD-envelope inflater. Extracted from
// `term_docs_reader.cpp` so the lazy window-addressed reader
// (`window_term_reader.cpp`) decodes byte-for-byte identically to the
// eager whole-term `SpimiTermDocsReader::ReadTerm` — there is exactly
// one implementation of each primitive, so the two paths cannot drift.
//
// Every read is bounds-checked against the (untrusted) on-disk buffer
// and hard-fails via `SPIMI_THROW_CORRUPT` on malformed input; the
// PFOR-run loop has a count-match guard so it cannot spin forever.
//
// These are header-only `inline` definitions: both translation units
// that include this header (term_docs_reader.cpp, window_term_reader.cpp)
// get their own inlined copy with no ODR violation, and the compiler is
// free to fold them.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "gen_cpp/segment_v2.pb.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"
#include "util/block_compression.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {
namespace frq_internal {

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
    // Advance the cursor to absolute offset `p` without reading. Used by
    // the lazy reader to jump to a window's payload tuple. Hard-fails if
    // `p` is past the (untrusted) buffer end so a crafted skip-table
    // byte-offset cannot drive an OOB read in the subsequent decode.
    void SeekTo(size_t p) {
        if (p > _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq SeekTo past end of buffer");
        }
        _pos = p;
    }
    size_t pos() const { return _pos; }
    size_t len() const { return _len; }

private:
    const uint8_t* _data;
    size_t _len;
    size_t _pos = 0;
};

// Decodes consecutive PFOR sub-blocks from `cur` until `count` total
// values have been recovered. Each sub-block is `byte(width) +
// bitpacked` (+ optional patch trailer) per `SpimiPforEncoder::
// EncodeBlock`. The block does NOT store its value count: the encoder
// emits kBlockSize values per block except the final one, so the decoder
// derives `n = min(kBlockSize, count - collected)` here. The reader
// reconstitutes the on-wire bytes for one sub-block and hands them (with
// `n`) to `SpimiPforDecoder::DecodeBlockFromBytes`, which uses Arrow's
// `BitReader` for the actual bit unpacking — same primitive the encoder
// uses so the bit-width interpretation cannot drift.
inline std::vector<uint32_t> DecodePforRun(ByteStream& cur, int32_t count) {
    std::vector<uint32_t> values;
    // Cap pre-reserve against a sane upper bound to defeat the
    // crafted-segment "reserve INT32_MAX × 4 B" DoS. 1<<24 (~16M)
    // fits any realistic segment doc count; the actual vector
    // grows organically via push_back if larger.
    constexpr size_t kSafeReserveCap = 1U << 24;
    values.reserve(std::min(static_cast<size_t>(count), kSafeReserveCap));
    int32_t collected = 0;
    while (collected < count) {
        // Block value count is implicit: kBlockSize per block except the
        // last. `count` (window/term doc_count) is from .tis doc_freq, which
        // the upper layer already validated, so this is always in [1, 128].
        const int32_t n = static_cast<int32_t>(
                std::min<int64_t>(SpimiPforEncoder::kBlockSize, count - collected));
        const size_t mark = cur.pos();
        const uint8_t raw_width = cur.ReadByte();
        const bool patched = (raw_width & 0x80U) != 0U; // patched-PFOR signal bit
        const uint8_t width = raw_width & 0x3FU;
        // Validate width BEFORE the reserve. Encoder emits width ∈ [1, 32];
        // decoder accepts the same range.
        if (width == 0 || width > 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq invalid PFOR bit width");
        }
        const size_t bit_bytes = (static_cast<size_t>(n) * width + 7U) / 8U;
        std::vector<uint8_t> block;
        block.reserve((cur.pos() - mark) + bit_bytes);
        // Reconstitute the on-wire bytes [mark, cur.pos() + bit_bytes).
        // Preserve the UNMASKED width byte so DecodeBlockFromBytes sees the
        // patch flag and parses the trailer.
        block.push_back(raw_width);
        cur.ReadInto(&block, bit_bytes);
        if (patched) {
            // Append the patch trailer so the reconstituted sub-block is
            // complete and the next loop iteration starts at the real next
            // sub-block header (not mid-trailer). Trailer = byte k, byte
            // except_width, k position bytes, ceil(k*except_width/8) bytes.
            const uint8_t k = cur.ReadByte();
            const uint8_t except_width = cur.ReadByte();
            if (k == 0 || k > n) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq PFOR patch: num_exceptions out of range");
            }
            if (except_width == 0 || static_cast<uint32_t>(width) + except_width > 32U)
                    [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq PFOR patch: except_width out of range");
            }
            block.push_back(k);
            block.push_back(except_width);
            cur.ReadInto(&block, static_cast<size_t>(k)); // position bytes
            const size_t high_bytes = (static_cast<size_t>(k) * except_width + 7U) / 8U;
            cur.ReadInto(&block, high_bytes);
        }
        std::vector<uint32_t> sub;
        SpimiPforDecoder::DecodeBlockFromBytes(block, static_cast<size_t>(n), &sub);
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
inline std::vector<uint8_t> DecompressZstdFrqBlock(ByteStream& cur) {
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

// Resolves one V4 window payload tuple (win_mode byte, VInt(uncomp), optional
// VInt(comp), bytes) to its inflated inner bytes. Shared shape with the .prx
// reader. All reads are bounds-checked against the untrusted buffer.
inline std::vector<uint8_t> ReadWindowPayload(ByteStream& cur) {
    const uint8_t win_mode = cur.ReadByte();
    if (win_mode == 0 /*raw*/) {
        const auto uncomp = static_cast<uint32_t>(cur.ReadVInt());
        std::vector<uint8_t> raw;
        cur.ReadInto(&raw, uncomp);
        return raw;
    }
    if (win_mode == 1 /*zstd*/) {
        const auto uncomp = static_cast<uint32_t>(cur.ReadVInt());
        const auto comp = static_cast<uint32_t>(cur.ReadVInt());
        std::vector<uint8_t> packed;
        cur.ReadInto(&packed, comp);
        std::vector<uint8_t> raw(uncomp);
        BlockCompressionCodec* codec = nullptr;
        if (!get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok() || codec == nullptr)
                [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq window: ZSTD codec unavailable");
        }
        Slice in(reinterpret_cast<const char*>(packed.data()), comp);
        Slice slice_out(reinterpret_cast<char*>(raw.data()), uncomp);
        if (!codec->decompress(in, &slice_out).ok() || slice_out.size != uncomp) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq window: ZSTD decompress failed");
        }
        return raw;
    }
    SPIMI_THROW_CORRUPT("SPIMI .frq window: unknown win_mode");
}

} // namespace frq_internal
} // namespace doris::segment_v2::inverted_index::spimi
