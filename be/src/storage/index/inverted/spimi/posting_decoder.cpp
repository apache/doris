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

#include "storage/index/inverted/spimi/posting_decoder.h"

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

// Minimal byte-stream cursor (same pattern as term_docs_reader.cpp
// and term_enum.cpp — duplicated to keep each decoder self-contained).
class Cursor {
public:
    Cursor(const uint8_t* data, size_t len) : _data(data), _len(len) {}

    uint8_t ReadByte() {
        if (_pos >= _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder: read past end of buffer");
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
            if (shift >= 32U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("PostingDecoder VInt: shift overflow");
            }
        }
        return static_cast<int32_t>(v);
    }
    void ReadInto(std::vector<uint8_t>* out, size_t n) {
        if (_pos + n > _len || _pos + n < _pos) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder ReadInto: bounds violation");
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

// Decodes consecutive PFOR sub-blocks until `count` values are
// recovered.  Same logic as `DecodePforRun` in term_docs_reader.cpp.
std::vector<uint32_t> DecodePforRun(Cursor& cur, int32_t count) {
    std::vector<uint32_t> values;
    constexpr size_t kSafeReserveCap = 1U << 24;
    values.reserve(std::min(static_cast<size_t>(count), kSafeReserveCap));
    int32_t collected = 0;
    while (collected < count) {
        const size_t mark = cur.pos();
        const int32_t n = cur.ReadVInt();
        if (n <= 0 || n > count - collected) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder PFOR sub-block count out of range");
        }
        const uint8_t raw_width = cur.ReadByte();
        const bool patched = (raw_width & 0x80U) != 0U; // patched-PFOR signal bit
        const uint8_t width = raw_width & 0x3FU;
        if (width == 0 || width > 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder PFOR invalid bit width");
        }
        const size_t bit_bytes = (static_cast<size_t>(n) * width + 7U) / 8U;
        std::vector<uint8_t> block;
        block.reserve((cur.pos() - mark) + bit_bytes);
        // Re-emit the VInt(n) bytes.
        {
            uint32_t vn = static_cast<uint32_t>(n);
            while ((vn & ~0x7FU) != 0) {
                block.push_back(static_cast<uint8_t>((vn & 0x7FU) | 0x80U));
                vn >>= 7U;
            }
            block.push_back(static_cast<uint8_t>(vn));
        }
        // Preserve the UNMASKED width byte so DecodeBlockFromBytes sees the
        // patch flag and parses the trailer.
        block.push_back(raw_width);
        cur.ReadInto(&block, bit_bytes);
        if (patched) {
            // Append the patch trailer (byte k, byte except_width, k
            // position bytes, ceil(k*except_width/8) high-bit bytes) so the
            // reconstituted sub-block is complete and the next iteration
            // starts at the real next sub-block header.
            const uint8_t k = cur.ReadByte();
            const uint8_t except_width = cur.ReadByte();
            if (k == 0 || k > n) [[unlikely]] {
                SPIMI_THROW_CORRUPT("PostingDecoder PFOR patch: num_exceptions out of range");
            }
            if (except_width == 0 || static_cast<uint32_t>(width) + except_width > 32U)
                    [[unlikely]] {
                SPIMI_THROW_CORRUPT("PostingDecoder PFOR patch: except_width out of range");
            }
            block.push_back(k);
            block.push_back(except_width);
            cur.ReadInto(&block, static_cast<size_t>(k)); // position bytes
            const size_t high_bytes = (static_cast<size_t>(k) * except_width + 7U) / 8U;
            cur.ReadInto(&block, high_bytes);
        }
        std::vector<uint32_t> sub;
        SpimiPforDecoder::DecodeBlockFromBytes(block, &sub);
        if (sub.size() != static_cast<size_t>(n)) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder PFOR sub-block decoded count mismatch");
        }
        values.insert(values.end(), sub.begin(), sub.end());
        collected += n;
    }
    if (collected != count) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder PFOR run total mismatch");
    }
    return values;
}

// Resolves jk's whole-term ZSTD envelope on the `.frq` stream. The caller has
// already consumed the leading `kCodeModeZstd` marker; what follows is
// `VInt(uncomp_len) VInt(comp_len) ZSTD-payload`, decompressing to the raw inner
// block (which itself begins with a kDefault/kPfor mode byte). Mirrors
// term_docs_reader.cpp's DecompressZstdFrqBlock; all reads are bounds-checked
// against the (untrusted) input.
std::vector<uint8_t> DecompressZstdFrqBlock(Cursor& cur) {
    const auto uncomp = static_cast<uint32_t>(cur.ReadVInt());
    const auto comp = static_cast<uint32_t>(cur.ReadVInt());
    std::vector<uint8_t> packed;
    cur.ReadInto(&packed, comp); // bounds-checked
    std::vector<uint8_t> raw(uncomp);
    BlockCompressionCodec* codec = nullptr;
    if (!get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok() || codec == nullptr)
            [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder .frq: ZSTD codec unavailable");
    }
    Slice in(reinterpret_cast<const char*>(packed.data()), comp);
    Slice slice_out(reinterpret_cast<char*>(raw.data()), uncomp);
    if (!codec->decompress(in, &slice_out).ok() || slice_out.size != uncomp) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder .frq: ZSTD decompress failed");
    }
    return raw;
}

// Resolves one V4 window payload tuple (win_mode, VInt(uncomp), optional
// VInt(comp), bytes) to its inflated inner bytes.
std::vector<uint8_t> ReadWindowPayload(Cursor& cur) {
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
            SPIMI_THROW_CORRUPT("PostingDecoder window: ZSTD codec unavailable");
        }
        Slice in(reinterpret_cast<const char*>(packed.data()), comp);
        Slice slice_out(reinterpret_cast<char*>(raw.data()), uncomp);
        if (!codec->decompress(in, &slice_out).ok() || slice_out.size != uncomp) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder window: ZSTD decompress failed");
        }
        return raw;
    }
    SPIMI_THROW_CORRUPT("PostingDecoder window: unknown win_mode");
}

// Decodes position deltas from the `.prx` stream for all documents.
// The term's `.prx` block begins with a 1-byte mode header (kProxRaw = raw VInt
// deltas, or kProxZstd = ZSTD-compressed payload behind VInt(uncomp) VInt(comp)).
// After resolving the envelope, the inner format for a term is:
//   for each doc d_i (in ascending order):
//     for each position p_j (in ascending order within d_i):
//       vint  position_delta_j   (delta resets to 0 at every new doc)
//
// Mirrors prox_reader.cpp's envelope handling. `docs` must already have doc_id
// and freq populated.
void DecodePositions(const uint8_t* prx_data, size_t prx_length, std::vector<DecodedDoc>& docs) {
    if (prx_data == nullptr || prx_length == 0) {
        return;
    }
    // Resolve the whole-term prox envelope (kProxRaw / kProxZstd) so the cursor
    // below sees only the inner VInt position-delta stream.
    const uint8_t* data = prx_data;
    size_t len = prx_length;
    std::vector<uint8_t> decompressed;
    const uint8_t mode = prx_data[0];
    if (mode == FreqProxEncoder::kProxZstd) {
        Cursor hdr(prx_data + 1, prx_length - 1);
        const auto uncomp = static_cast<uint32_t>(hdr.ReadVInt());
        const auto comp = static_cast<uint32_t>(hdr.ReadVInt());
        const size_t hpos = 1 + hdr.pos();
        if (hpos + comp > prx_length || hpos + comp < hpos) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .prx: compressed length exceeds block");
        }
        decompressed.resize(uncomp);
        BlockCompressionCodec* codec = nullptr;
        if (!get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok() || codec == nullptr)
                [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .prx: ZSTD codec unavailable");
        }
        Slice in(reinterpret_cast<const char*>(prx_data + hpos), comp);
        Slice slice_out(reinterpret_cast<char*>(decompressed.data()), uncomp);
        if (!codec->decompress(in, &slice_out).ok() || slice_out.size != uncomp) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .prx: ZSTD decompress failed");
        }
        data = decompressed.data();
        len = uncomp;
    } else if (mode == FreqProxEncoder::kProxRaw) {
        data = prx_data + 1;
        len = prx_length - 1;
    } else if (mode == FreqProxEncoder::kProxWindowed) {
        // V4 windowed .prx: byte mode, VInt(W), VInt(num_windows), a per-window
        // skip table (4 VInts/window), then per-window payloads. This eager path
        // is framing-agnostic — concatenating the inflated per-window PART_POS
        // bytes reproduces the term's whole contiguous VInt position stream — so
        // it only STEPS OVER the skip table (mirrors the .frq skip-table skip).
        Cursor wc(prx_data + 1, prx_length - 1);
        (void)wc.ReadVInt(); // W
        const int32_t num_windows = wc.ReadVInt();
        if (num_windows <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .prx windowed: num_windows out of range");
        }
        // Skip the skip table: doc_count, byte_offset, min_docid, max_docid_delta.
        for (int32_t w = 0; w < num_windows; ++w) {
            for (int s = 0; s < 4; ++s) {
                (void)wc.ReadVInt();
            }
        }
        for (int32_t w = 0; w < num_windows; ++w) {
            const std::vector<uint8_t> inner = ReadWindowPayload(wc);
            decompressed.insert(decompressed.end(), inner.begin(), inner.end());
        }
        data = decompressed.data();
        len = decompressed.size();
    } else [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder .prx: unknown prox block mode");
    }

    Cursor prx(data, len);
    for (auto& doc : docs) {
        doc.positions.reserve(static_cast<size_t>(doc.freq));
        int32_t last_pos = 0;
        for (int32_t j = 0; j < doc.freq; ++j) {
            const int32_t delta = prx.ReadVInt();
            last_pos += delta;
            doc.positions.push_back(last_pos);
        }
    }
}

} // namespace

std::vector<DecodedDoc> PostingDecoder::Decode(const uint8_t* frq_data, size_t frq_length,
                                               const uint8_t* prx_data, size_t prx_length,
                                               int32_t doc_freq, bool has_prox) {
    if (doc_freq <= 0 || frq_length == 0U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder: bad doc_freq / buffer length");
    }

    // Decode the .frq stream (resolving any whole-term kCodeModeZstd envelope)
    // into per-doc {doc_id, freq}, then attach positions from the .prx stream.
    std::vector<DecodedDoc> docs = DecodeInner(frq_data, frq_length, doc_freq, has_prox);
    if (has_prox) {
        DecodePositions(prx_data, prx_length, docs);
    }
    return docs;
}

std::vector<DecodedDoc> PostingDecoder::DecodeInner(const uint8_t* frq_data, size_t frq_length,
                                                    int32_t doc_freq, bool has_prox) {
    Cursor cur(frq_data, frq_length);
    const uint8_t mode = cur.ReadByte();

    std::vector<DecodedDoc> docs;
    constexpr size_t kSafeReserveCap = 1U << 24;
    docs.reserve(std::min(static_cast<size_t>(doc_freq), kSafeReserveCap));

    if (mode == FreqProxEncoder::kCodeModeZstd) {
        const std::vector<uint8_t> raw = DecompressZstdFrqBlock(cur);
        return DecodeInner(raw.data(), raw.size(), doc_freq, has_prox);
    }
    if (mode == FreqProxEncoder::kCodeModeSpimiWindowed) {
        const uint8_t inner_mode = cur.ReadByte();
        (void)cur.ReadVInt(); // W
        const int32_t num_windows = cur.ReadVInt();
        if (num_windows <= 0 || num_windows > doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .frq windowed: num_windows out of range");
        }
        std::vector<int32_t> win_doc_count(static_cast<size_t>(num_windows));
        int64_t total = 0;
        for (int32_t w = 0; w < num_windows; ++w) {
            win_doc_count[static_cast<size_t>(w)] = cur.ReadVInt();
            (void)cur.ReadVInt(); // win_byte_offset
            (void)cur.ReadVInt(); // win_min_docid
            (void)cur.ReadVInt(); // win_max_docid_delta
            const int32_t c = win_doc_count[static_cast<size_t>(w)];
            if (c <= 0 || c > doc_freq) [[unlikely]] {
                SPIMI_THROW_CORRUPT("PostingDecoder .frq windowed: win_doc_count out of range");
            }
            total += c;
        }
        if (total != doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .frq windowed: window counts disagree");
        }
        int32_t last_doc = 0;
        for (int32_t w = 0; w < num_windows; ++w) {
            const std::vector<uint8_t> inner = ReadWindowPayload(cur);
            const int32_t wc = win_doc_count[static_cast<size_t>(w)];
            Cursor wcur(inner.data(), inner.size());
            if (inner_mode == FreqProxEncoder::kCodeModeSpimiPfor) {
                const auto dd = DecodePforRun(wcur, wc);
                std::vector<uint32_t> fq;
                if (has_prox) {
                    fq = DecodePforRun(wcur, wc);
                }
                for (int32_t i = 0; i < wc; ++i) {
                    DecodedDoc d;
                    last_doc += static_cast<int32_t>(dd[static_cast<size_t>(i)]);
                    d.doc_id = last_doc;
                    d.freq = has_prox ? static_cast<int32_t>(fq[static_cast<size_t>(i)]) : 1;
                    docs.push_back(std::move(d));
                }
            } else if (inner_mode == FreqProxEncoder::kCodeModeDefault) {
                for (int32_t i = 0; i < wc; ++i) {
                    DecodedDoc d;
                    if (has_prox) {
                        const auto code = static_cast<uint32_t>(wcur.ReadVInt());
                        last_doc += static_cast<int32_t>(code >> 1U);
                        d.freq = ((code & 1U) != 0) ? 1 : wcur.ReadVInt();
                    } else {
                        last_doc += static_cast<int32_t>(wcur.ReadVInt());
                        d.freq = 1;
                    }
                    d.doc_id = last_doc;
                    docs.push_back(std::move(d));
                }
            } else [[unlikely]] {
                SPIMI_THROW_CORRUPT("PostingDecoder .frq windowed: unknown inner_mode");
            }
        }
        return docs;
    }
    if (mode == FreqProxEncoder::kCodeModeDefault) {
        const int32_t recorded = cur.ReadVInt();
        if (recorded != doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .frq kDefault docCount disagrees with .tis");
        }
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            DecodedDoc d;
            if (has_prox) {
                const uint32_t code = static_cast<uint32_t>(cur.ReadVInt());
                last_doc += static_cast<int32_t>(code >> 1U);
                d.freq = ((code & 1U) != 0) ? 1 : cur.ReadVInt();
            } else {
                last_doc += static_cast<int32_t>(cur.ReadVInt());
                d.freq = 1;
            }
            d.doc_id = last_doc;
            docs.push_back(std::move(d));
        }
    } else if (mode == FreqProxEncoder::kCodeModeSpimiPfor) {
        const auto doc_deltas = DecodePforRun(cur, doc_freq);
        std::vector<uint32_t> freqs;
        if (has_prox) {
            freqs = DecodePforRun(cur, doc_freq);
        }
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            DecodedDoc d;
            last_doc += static_cast<int32_t>(doc_deltas[static_cast<size_t>(i)]);
            d.doc_id = last_doc;
            d.freq = has_prox ? static_cast<int32_t>(freqs[static_cast<size_t>(i)]) : 1;
            docs.push_back(std::move(d));
        }
    } else {
        SPIMI_THROW_CORRUPT("PostingDecoder: unknown .frq CodeMode byte");
    }

    return docs;
}

} // namespace doris::segment_v2::inverted_index::spimi
