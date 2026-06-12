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
#include <limits>

#include "common/logging.h"
#include "gen_cpp/segment_v2.pb.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"
#include "util/block_compression.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// 所有解码消费统一走 ForwardByteSource（替代旧的本地 Cursor）：内存块包
// MemoryByteSource（窗口即整段，热路径与旧 Cursor 同为内联指针算术 + 边界
// 检查），spill 文件走滑窗游标 —— 同一份信封/codec 解析同时服务两种背书。

// Decodes consecutive PFOR sub-blocks until `count` values are
// recovered.  Same logic as `DecodePforRun` in frq_window_decode_internal.h.
// The block value count is implicit (kBlockSize per block except the last),
// so it is derived here rather than read from the stream.
std::vector<uint32_t> DecodePforRun(ForwardByteSource& cur, int32_t count) {
    std::vector<uint32_t> values;
    constexpr size_t kSafeReserveCap = 1U << 24;
    values.reserve(std::min(static_cast<size_t>(count), kSafeReserveCap));
    int32_t collected = 0;
    while (collected < count) {
        const int32_t n = static_cast<int32_t>(
                std::min<int64_t>(SpimiPforEncoder::kBlockSize, count - collected));
        const uint8_t raw_width = cur.ReadByte();
        // 常数块（b=0）：整字节 0x00 标记 + VInt(常数)，零比特 payload。逐字节
        // 重组该子块（标记 + 最多 5 字节 VInt）后仍交给 DecodeBlockFromBytes 统一
        // 解释（与 frq_window_decode_internal.h 的副本保持一致）。0x80 不进此分
        // 支，落到下方宽度校验并硬失败。
        if (raw_width == 0x00U) {
            std::vector<uint8_t> block;
            block.reserve(6);
            block.push_back(raw_width);
            size_t vint_bytes = 0;
            uint8_t b = 0;
            do {
                b = cur.ReadByte();
                block.push_back(b);
                if (++vint_bytes > 5U) [[unlikely]] {
                    SPIMI_THROW_CORRUPT("PostingDecoder PFOR const block: constant VInt overlong");
                }
            } while ((b & 0x80U) != 0U);
            std::vector<uint32_t> sub;
            SpimiPforDecoder::DecodeBlockFromBytes(block, static_cast<size_t>(n), &sub);
            if (sub.size() != static_cast<size_t>(n)) [[unlikely]] {
                SPIMI_THROW_CORRUPT("PostingDecoder PFOR const block decoded count mismatch");
            }
            values.insert(values.end(), sub.begin(), sub.end());
            collected += n;
            continue;
        }
        const bool patched = (raw_width & 0x80U) != 0U; // patched-PFOR signal bit
        const uint8_t width = raw_width & 0x3FU;
        if (width == 0 || width > 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder PFOR invalid bit width");
        }
        const size_t bit_bytes = (static_cast<size_t>(n) * width + 7U) / 8U;
        std::vector<uint8_t> block;
        block.reserve(1U + bit_bytes);
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
        SpimiPforDecoder::DecodeBlockFromBytes(block, static_cast<size_t>(n), &sub);
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
// `VInt(uncomp_len) VInt(comp_len) ZSTD-payload`, decompressing to the raw
// inner block (which itself begins with a kDefault/kPfor mode byte). Mirrors
// term_docs_reader.cpp's DecompressZstdFrqBlock; all reads are bounds-checked
// against the (untrusted) input.
std::vector<uint8_t> DecompressZstdFrqBlock(ForwardByteSource& cur) {
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
std::vector<uint8_t> ReadWindowPayload(ForwardByteSource& cur) {
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

// Flat `.frq` core: appends exactly `doc_freq` RAW doc-deltas (as stored — the
// first one is the delta from an implicit 0, i.e. the absolute first doc id)
// to `dd`, and `doc_freq` freqs to `fq` when has_prox. Shared by Decode and
// DecodeFlat so there is exactly ONE envelope/codec dispatch implementation.
// 消费严格前向且由 doc_freq 自定界：返回时 `cur` 恰好停在本 term .frq 块的
// 内容尾（流式游标据此免预知块长）。
void DecodeFrqFlat(ForwardByteSource& cur, int32_t doc_freq, bool has_prox, bool is_slim,
                   std::vector<uint32_t>& dd, std::vector<uint32_t>& fq) {
    constexpr size_t kSafeReserveCap = 1U << 24;
    dd.reserve(dd.size() + std::min(static_cast<size_t>(doc_freq), kSafeReserveCap));
    if (has_prox) {
        fq.reserve(fq.size() + std::min(static_cast<size_t>(doc_freq), kSafeReserveCap));
    }

    if (is_slim) {
        // SLIM kDefault block (df < skip_interval): NO codec byte, NO
        // VInt(doc_count). Read exactly `doc_freq` per-doc VInt deltas; doc_freq
        // is authoritative from .tis so the loop never over-reads.
        for (int32_t i = 0; i < doc_freq; ++i) {
            if (has_prox) {
                const auto code = static_cast<uint32_t>(cur.ReadVInt());
                dd.push_back(code >> 1U);
                fq.push_back(((code & 1U) != 0) ? 1U : static_cast<uint32_t>(cur.ReadVInt()));
            } else {
                dd.push_back(static_cast<uint32_t>(cur.ReadVInt()));
            }
        }
        return;
    }

    const uint8_t mode = cur.ReadByte();

    if (mode == FreqProxEncoder::kCodeModeZstd) {
        const std::vector<uint8_t> raw = DecompressZstdFrqBlock(cur);
        // Only PFOR blocks are ZSTD-wrapped; the inner block keeps its codec
        // byte, so is_slim stays false through the recursion.
        MemoryByteSource inner(raw.data(), raw.size());
        DecodeFrqFlat(inner, doc_freq, has_prox, /*is_slim=*/false, dd, fq);
        return;
    }
    if (mode == FreqProxEncoder::kCodeModeSpimiWindowed) {
        const uint8_t inner_mode = cur.ReadByte();
        const int32_t W = cur.ReadVInt(); // window doc-width; derives win_doc_count
        if (W <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .frq windowed: non-positive W");
        }
        const int32_t num_windows = cur.ReadVInt();
        if (num_windows <= 0 || num_windows > doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .frq windowed: num_windows out of range");
        }
        // SLIM skip table (3 VInts/window): win_doc_count is DERIVED as
        // min(W, remaining) (every non-last window is exactly W docs; only the
        // term's last window may be a partial unit). The delta-coded offset /
        // min_docid are stepped over for whole-term sequential decode.
        std::vector<int32_t> win_doc_count(static_cast<size_t>(num_windows));
        int64_t total = 0;
        for (int32_t w = 0; w < num_windows; ++w) {
            (void)cur.ReadVInt(); // win_byte_offset_delta
            (void)cur.ReadVInt(); // win_min_docid_delta
            (void)cur.ReadVInt(); // win_max_docid_delta
            const int32_t c = static_cast<int32_t>(std::min<int64_t>(W, doc_freq - total));
            if (c <= 0 || c > doc_freq) [[unlikely]] {
                SPIMI_THROW_CORRUPT(
                        "PostingDecoder .frq windowed: derived win_doc_count out of range");
            }
            win_doc_count[static_cast<size_t>(w)] = c;
            total += c;
        }
        if (total != doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .frq windowed: window counts disagree");
        }
        for (int32_t w = 0; w < num_windows; ++w) {
            const std::vector<uint8_t> inner = ReadWindowPayload(cur);
            const int32_t wc = win_doc_count[static_cast<size_t>(w)];
            MemoryByteSource wcur(inner.data(), inner.size());
            if (inner_mode == FreqProxEncoder::kCodeModeSpimiPfor) {
                const auto run = DecodePforRun(wcur, wc);
                dd.insert(dd.end(), run.begin(), run.end());
                if (has_prox) {
                    const auto fr = DecodePforRun(wcur, wc);
                    fq.insert(fq.end(), fr.begin(), fr.end());
                }
            } else if (inner_mode == FreqProxEncoder::kCodeModeDefault) {
                for (int32_t i = 0; i < wc; ++i) {
                    if (has_prox) {
                        const auto code = static_cast<uint32_t>(wcur.ReadVInt());
                        dd.push_back(code >> 1U);
                        fq.push_back(((code & 1U) != 0) ? 1U
                                                        : static_cast<uint32_t>(wcur.ReadVInt()));
                    } else {
                        dd.push_back(static_cast<uint32_t>(wcur.ReadVInt()));
                    }
                }
            } else [[unlikely]] {
                SPIMI_THROW_CORRUPT("PostingDecoder .frq windowed: unknown inner_mode");
            }
        }
        return;
    }
    // A SLIM kDefault top-level block (df < skip_interval) carries NO codec byte
    // and is handled by the is_slim fast path above; it never reaches this
    // codec-byte dispatch. Only PFOR / windowed / ZSTD blocks remain here.
    if (mode == FreqProxEncoder::kCodeModeSpimiPfor) {
        const auto run = DecodePforRun(cur, doc_freq);
        dd.insert(dd.end(), run.begin(), run.end());
        if (has_prox) {
            const auto fr = DecodePforRun(cur, doc_freq);
            fq.insert(fq.end(), fr.begin(), fr.end());
        }
        return;
    }
    SPIMI_THROW_CORRUPT("PostingDecoder: unknown .frq CodeMode byte");
}

// 把已就位的整段 position-delta VInt 流（pd, plen）按 per-doc freq 跳读建
// 偏移并 verbatim 追加进 out（旧 DecodeFlat 的扫描循环原样下沉）。
void ScanPosBytes(const uint8_t* pd, size_t plen, int32_t doc_freq, size_t dd_base,
                  PostingDecoder::FlatPostings* out) {
    const size_t base = out->pos_vint.size();
    size_t pb = 0;
    for (int32_t i = 0; i < doc_freq; ++i) {
        if (base + pb > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder flat: position stream exceeds u32 offsets");
        }
        out->pos_offsets.push_back(static_cast<uint32_t>(base + pb));
        const uint32_t freq = out->freqs[dd_base + static_cast<size_t>(i)];
        for (uint32_t f = 0; f < freq; ++f) {
            // Skip one VInt: continuation bytes then the terminator.
            while (true) {
                if (pb >= plen) [[unlikely]] {
                    SPIMI_THROW_CORRUPT("PostingDecoder flat: .prx stream truncated");
                }
                const bool cont = (pd[pb] & 0x80U) != 0U;
                ++pb;
                if (!cont) {
                    break;
                }
            }
        }
    }
    out->pos_vint.insert(out->pos_vint.end(), pd, pd + pb);
}

// kProxRaw 的流式扫描：逐字节从源读 position VInt（由 Σfreq 自定界），边读
// 边 verbatim 追加 —— 不借用、不超读（消费恰好停在本 term 的 raw payload
// 尾）。slim term 才走 raw 信封，载荷天然有界。
void ScanPosBytesStreaming(ForwardByteSource& src, int32_t doc_freq, size_t dd_base,
                           PostingDecoder::FlatPostings* out) {
    for (int32_t i = 0; i < doc_freq; ++i) {
        if (out->pos_vint.size() > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder flat: position stream exceeds u32 offsets");
        }
        out->pos_offsets.push_back(static_cast<uint32_t>(out->pos_vint.size()));
        const uint32_t freq = out->freqs[dd_base + static_cast<size_t>(i)];
        for (uint32_t f = 0; f < freq; ++f) {
            while (true) {
                const uint8_t b = src.ReadByte();
                out->pos_vint.push_back(b);
                if ((b & 0x80U) == 0U) {
                    break;
                }
            }
        }
    }
}

// 解析一个 term 的整段 `.prx` 信封（kProxRaw / kProxZstd / kProxWindowed）
// 并把 position 字节 + per-doc 偏移追加进 out。raw 模式直接从源流式扫描；
// ZSTD / windowed 模式先 inflate 出该 term 的精确内层流再扫描（与旧
// ResolvePrxInner + 扫描的字节行为逐一相同）。
void DecodePrxFlat(ForwardByteSource& src, int32_t doc_freq, size_t dd_base,
                   PostingDecoder::FlatPostings* out) {
    const uint8_t mode = src.ReadByte();
    if (mode == FreqProxEncoder::kProxRaw) {
        ScanPosBytesStreaming(src, doc_freq, dd_base, out);
        return;
    }
    if (mode == FreqProxEncoder::kProxZstd) {
        const auto uncomp = static_cast<uint32_t>(src.ReadVInt());
        const auto comp = static_cast<uint32_t>(src.ReadVInt());
        std::vector<uint8_t> packed;
        src.ReadInto(&packed, comp); // bounds-checked
        std::vector<uint8_t> owned(uncomp);
        BlockCompressionCodec* codec = nullptr;
        if (!get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok() || codec == nullptr)
                [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .prx: ZSTD codec unavailable");
        }
        Slice in(reinterpret_cast<const char*>(packed.data()), comp);
        Slice slice_out(reinterpret_cast<char*>(owned.data()), uncomp);
        if (!codec->decompress(in, &slice_out).ok() || slice_out.size != uncomp) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .prx: ZSTD decompress failed");
        }
        ScanPosBytes(owned.data(), owned.size(), doc_freq, dd_base, out);
        return;
    }
    if (mode == FreqProxEncoder::kProxWindowed) {
        // V4 windowed .prx: byte mode, VInt(W), VInt(num_windows), a per-window
        // skip table (4 VInts/window), then per-window payloads. This eager path
        // is framing-agnostic — concatenating the inflated per-window PART_POS
        // bytes reproduces the term's whole contiguous VInt position stream — so
        // it only STEPS OVER the skip table (mirrors the .frq skip-table skip).
        (void)src.ReadVInt(); // W
        const int32_t num_windows = src.ReadVInt();
        if (num_windows <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .prx windowed: num_windows out of range");
        }
        // Skip the skip table: doc_count, byte_offset, min_docid, max_docid_delta.
        for (int32_t w = 0; w < num_windows; ++w) {
            for (int s = 0; s < 4; ++s) {
                (void)src.ReadVInt();
            }
        }
        std::vector<uint8_t> owned;
        for (int32_t w = 0; w < num_windows; ++w) {
            const std::vector<uint8_t> inner = ReadWindowPayload(src);
            owned.insert(owned.end(), inner.begin(), inner.end());
        }
        ScanPosBytes(owned.data(), owned.size(), doc_freq, dd_base, out);
        return;
    }
    SPIMI_THROW_CORRUPT("PostingDecoder .prx: unknown prox block mode");
}

} // namespace

std::vector<DecodedDoc> PostingDecoder::Decode(const uint8_t* frq_data, size_t frq_length,
                                               const uint8_t* prx_data, size_t prx_length,
                                               int32_t doc_freq, bool has_prox, bool is_slim) {
    if (doc_freq <= 0 || frq_length == 0U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder: bad doc_freq / buffer length");
    }

    // Positions are attached only when the caller actually supplied a `.prx`
    // block (legacy tolerance: a null/empty block yields docs with empty
    // position lists).
    const bool want_pos = has_prox && prx_data != nullptr && prx_length > 0;

    FlatPostings flat;
    if (want_pos) {
        DecodeFlat(frq_data, frq_length, prx_data, prx_length, doc_freq, has_prox, is_slim, &flat);
    } else {
        MemoryByteSource frq(frq_data, frq_length);
        DecodeFrqFlat(frq, doc_freq, has_prox, is_slim, flat.doc_deltas, flat.freqs);
    }

    std::vector<DecodedDoc> docs;
    constexpr size_t kSafeReserveCap = 1U << 24;
    docs.reserve(std::min(static_cast<size_t>(doc_freq), kSafeReserveCap));
    MemoryByteSource prx(flat.pos_vint.data(), flat.pos_vint.size());
    int64_t doc = 0;
    for (int32_t i = 0; i < doc_freq; ++i) {
        doc += flat.doc_deltas[static_cast<size_t>(i)];
        DecodedDoc d;
        d.doc_id = static_cast<int32_t>(doc);
        d.freq = has_prox ? static_cast<int32_t>(flat.freqs[static_cast<size_t>(i)]) : 1;
        if (want_pos) {
            d.positions.reserve(static_cast<size_t>(d.freq));
            int32_t last_pos = 0;
            for (int32_t j = 0; j < d.freq; ++j) {
                last_pos += prx.ReadVInt();
                d.positions.push_back(last_pos);
            }
        }
        docs.push_back(std::move(d));
    }
    return docs;
}

void PostingDecoder::DecodeFlat(const uint8_t* frq_data, size_t frq_length, const uint8_t* prx_data,
                                size_t prx_length, int32_t doc_freq, bool has_prox, bool is_slim,
                                FlatPostings* out) {
    MemoryByteSource frq(frq_data, frq_length);
    if (has_prox) {
        if (prx_data == nullptr || prx_length == 0U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder flat: missing .prx block for a phrase-on term");
        }
        MemoryByteSource prx(prx_data, prx_length);
        DecodeFlat(frq, &prx, doc_freq, has_prox, is_slim, out);
    } else {
        DecodeFlat(frq, nullptr, doc_freq, has_prox, is_slim, out);
    }
}

void PostingDecoder::DecodeFlat(ForwardByteSource& frq_src, ForwardByteSource* prx_src,
                                int32_t doc_freq, bool has_prox, bool is_slim, FlatPostings* out) {
    if (doc_freq <= 0 || frq_src.Remaining() <= 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder: bad doc_freq / buffer length");
    }

    const size_t dd_base = out->doc_deltas.size();
    DecodeFrqFlat(frq_src, doc_freq, has_prox, is_slim, out->doc_deltas, out->freqs);
    if (out->doc_deltas.size() != dd_base + static_cast<size_t>(doc_freq)) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder flat: decoded doc count mismatch");
    }

    // Re-base this run's first delta. As stored, it is the delta from an
    // implicit 0 (== the absolute first doc id, because FreqProxEncoder::
    // StartTerm resets _last_doc to 0 per segment). Appended after a previous
    // run, it must become the delta from that run's last doc so the chain
    // reads as ONE term written in doc order. Inputs never overlap (spills are
    // successive slices of the same monotonically increasing _rid stream), so
    // the re-based delta is strictly positive; anything else is corruption.
    if (dd_base > 0) {
        const auto abs_first = static_cast<int64_t>(out->doc_deltas[dd_base]);
        if (abs_first <= out->last_doc) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder flat: appended run overlaps the previous run");
        }
        out->doc_deltas[dd_base] = static_cast<uint32_t>(abs_first - out->last_doc);
    }
    int64_t last = out->last_doc;
    for (size_t i = dd_base; i < out->doc_deltas.size(); ++i) {
        last += out->doc_deltas[i];
    }
    out->last_doc = last;

    if (!has_prox) {
        return;
    }
    if (prx_src == nullptr || prx_src->Remaining() <= 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder flat: missing .prx block for a phrase-on term");
    }
    DecodePrxFlat(*prx_src, doc_freq, dd_base, out);
}

} // namespace doris::segment_v2::inverted_index::spimi
