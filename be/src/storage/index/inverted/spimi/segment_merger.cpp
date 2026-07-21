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

#include "storage/index/inverted/spimi/segment_merger.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <queue>
#include <utility>

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/posting_decoder.h"
#include "storage/index/inverted/spimi/segment_infos_writer.h"
#include "storage/index/inverted/spimi/segment_writer.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"
#include "storage/index/inverted/spimi/term_enum.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// 观测计数器与测试钩子（见 segment_merger.h 的 MergeStats 注释）。
// thread_local：一次 Merge 在单线程内完成，生产逻辑从不读取。
thread_local SegmentMerger::MergeStats g_merge_stats;
thread_local bool g_force_slim_reencode_for_test = false;

// 把一条输入流整段拷到输出：内存源走 BorrowStable 零拷贝 WriteBytes（与旧
// 的 vector 直拷等价），文件游标按 1MiB 分块流拷 —— 任一情形下输出字节与
// 全量载回后整段 WriteBytes 逐一相同。
void CopyWholeStream(ForwardByteSource& src, ByteOutput* dst) {
    const int64_t total = src.Remaining();
    if (total <= 0) {
        return;
    }
    if (const uint8_t* p = src.BorrowStable(static_cast<size_t>(total))) {
        dst->WriteBytes(p, static_cast<size_t>(total));
        return;
    }
    constexpr size_t kCopyChunk = 1U << 20;
    std::vector<uint8_t> chunk;
    int64_t left = total;
    while (left > 0) {
        const size_t n =
                static_cast<size_t>(std::min<int64_t>(static_cast<int64_t>(kCopyChunk), left));
        chunk.clear();
        src.ReadInto(&chunk, n);
        dst->WriteBytes(chunk.data(), n);
        left -= static_cast<int64_t>(n);
    }
}

// Single-input fast path: copies posting bytes (.tis/.tii/.frq/.prx)
// directly from the input to the output without decode/re-encode,
// then rebuilds metadata (.fnm, segments_N, segments.gen) with the
// caller's parameters.
//
// Safe when the single input's encoding matches the output format:
//   - doc_offset is 0 (always true for the first/only input)
//   - omit_norms is true (spill always omits norms)
// The spill's omit_term_freq_and_positions flag moves in lockstep with the
// final segment (SpillManager passes the field's flag to EmitSegment), so the
// spill .frq/.prx are already in exactly the format this output advertises —
// whether positions are present (.prx populated, has_prox=true) or omitted
// (.prx empty, has_prox=false). The byte-copy is correct for BOTH: it copies
// the .prx verbatim (empty in omit mode) and rebuilds .fnm with
// has_prox = !omit. This covers the V4 (pure SPIMI) path including DOCS_ONLY.
int64_t MergeSingleInput(SegmentMerger::StreamInput& input, const SpimiSegmentSink& sink,
                         const std::string& segment_name, const std::string& field_name,
                         int32_t total_doc_count, int32_t index_version,
                         bool omit_term_freq_and_positions, bool omit_norms) {
    // The byte-copy is valid when the single input's on-disk encoding matches
    // the output flags. The spill is written in lockstep with these flags, so
    // only omit_norms must hold here; both positions-present and omit work (the
    // .prx is copied verbatim and .fnm is rebuilt with has_prox = !omit). The
    // dispatch guard in Merge() enforces this; the DCHECK makes it crash-loud
    // in debug for any future caller that bypasses the guard.
    DCHECK(omit_norms);

    // .tis 头/尾校验 + footer term 数（不动游标的随机小读）—— 与旧实现经
    // TermEnum ctor 做的 FORMAT / 长度 / 条目数校验同口径。
    if (input.tis->Length() < 32) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: buffer too short");
    }
    uint8_t hdr[4];
    input.tis->ReadAt(0, hdr, sizeof(hdr));
    const auto format = static_cast<int32_t>((static_cast<uint32_t>(hdr[0]) << 24) |
                                             (static_cast<uint32_t>(hdr[1]) << 16) |
                                             (static_cast<uint32_t>(hdr[2]) << 8) | hdr[3]);
    if (format != TermDictWriter::kFormat && format != TermDictWriter::kFormatInline) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: FORMAT mismatch");
    }
    uint8_t footer[8];
    input.tis->ReadAt(input.tis->Length() - 8, footer, sizeof(footer));
    uint64_t total_entries = 0;
    for (const uint8_t b : footer) {
        total_entries = (total_entries << 8) | b;
    }
    if (static_cast<int64_t>(total_entries) < 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .tis TermEnum: negative entry count");
    }

    // Copy all posting bytes directly — no decode/re-encode cycle.
    // The single input's doc_offset is 0, so TermInfo pointers in
    // .tis remain valid and posting data needs no adjustment.
    CopyWholeStream(*input.tis, sink.tis);
    CopyWholeStream(*input.tii, sink.tii);
    CopyWholeStream(*input.frq, sink.frq);
    CopyWholeStream(*input.prx, sink.prx);

    // Rebuild .fnm with the caller's index_version and field flags.
    // The spill's .fnm may have used kIndexVersionV1; the final
    // output may need V0.  Only .fnm needs rewriting because the
    // index_version tag lives there, not in the posting bytes.
    {
        FieldInfoEntry fi;
        fi.name = field_name;
        fi.is_indexed = true;
        fi.has_prox = !omit_term_freq_and_positions;
        fi.omit_norms = omit_norms;
        fi.index_version = index_version;
        fi.flags = 0;
        FieldInfosWriter(sink.fnm).Write({fi});
    }

    // Rebuild segments_N and segments.gen with the correct segment
    // name and total doc count.
    {
        SegmentInfoEntry seg;
        seg.name = segment_name;
        seg.doc_count = total_doc_count;
        seg.del_gen = -1;
        seg.doc_store_offset = -1;
        seg.has_single_norm_file = true;
        seg.is_compound_file = -1;
        SegmentInfosWriter manifest_writer;
        manifest_writer.WriteSegmentsN(sink.segments_n, /*version=*/1, /*counter=*/1, {seg});
        manifest_writer.WriteSegmentsGen(sink.segments_gen, /*generation=*/1);
    }

    ++g_merge_stats.single_input_segments;

    // Return the term count from the input's .tis footer.
    return static_cast<int64_t>(total_entries);
}

// Heap entry: the current term from one input segment.
struct HeapEntry {
    int32_t field_number;
    std::string term_utf8;
    int32_t input_index;

    // Min-heap: smallest (field, term) wins; input_index breaks ties
    // so inputs are processed in spill order.
    bool operator>(const HeapEntry& o) const {
        if (field_number != o.field_number) {
            return field_number > o.field_number;
        }
        if (term_utf8 != o.term_utf8) {
            return term_utf8 > o.term_utf8;
        }
        return input_index > o.input_index;
    }
};

// LEB128 VInt append (same encoding ByteOutput::WriteVInt produces for the
// same 32-bit pattern) — used to rebuild a merged SLIM term's .frq block from
// the flat arrays.
inline void AppendVInt(std::vector<uint8_t>& buf, uint32_t v) {
    while (v & ~0x7FU) {
        buf.push_back(static_cast<uint8_t>((v & 0x7FU) | 0x80U));
        v >>= 7U;
    }
    buf.push_back(static_cast<uint8_t>(v));
}

} // namespace

const SegmentMerger::MergeStats& SegmentMerger::Stats() {
    return g_merge_stats;
}

void SegmentMerger::ResetStats() {
    g_merge_stats = MergeStats {};
}

void SegmentMerger::SetForceSlimReencodeForTest(bool v) {
    g_force_slim_reencode_for_test = v;
}

SegmentMerger::StreamInput SegmentMerger::WrapOwnedInput(Input&& in) {
    StreamInput s;
    s.tis = std::make_unique<MemoryByteSource>(std::move(in.tis_bytes));
    s.tii = std::make_unique<MemoryByteSource>(std::move(in.tii_bytes));
    s.frq = std::make_unique<MemoryByteSource>(std::move(in.frq_bytes));
    s.prx = std::make_unique<MemoryByteSource>(std::move(in.prx_bytes));
    s.doc_count = in.doc_count;
    return s;
}

int64_t SegmentMerger::Merge(const std::vector<Input>& inputs, const SpimiSegmentSink& sink,
                             const std::string& segment_name, const std::string& field_name,
                             int32_t total_doc_count, int32_t index_version,
                             bool omit_term_freq_and_positions, bool omit_norms) {
    // 薄包装：对每路内存 Input 构造借用型游标（字节仍归调用方所有），转发
    // 给核心（游标）重载 —— 两条入口的输出逐字节相同。
    std::vector<StreamInput> streams;
    streams.reserve(inputs.size());
    for (const Input& in : inputs) {
        StreamInput s;
        s.tis = std::make_unique<MemoryByteSource>(in.tis_bytes.data(), in.tis_bytes.size());
        s.tii = std::make_unique<MemoryByteSource>(in.tii_bytes.data(), in.tii_bytes.size());
        s.frq = std::make_unique<MemoryByteSource>(in.frq_bytes.data(), in.frq_bytes.size());
        s.prx = std::make_unique<MemoryByteSource>(in.prx_bytes.data(), in.prx_bytes.size());
        s.doc_count = in.doc_count;
        streams.push_back(std::move(s));
    }
    return Merge(streams, sink, segment_name, field_name, total_doc_count, index_version,
                 omit_term_freq_and_positions, omit_norms);
}

int64_t SegmentMerger::Merge(std::vector<StreamInput>& inputs, const SpimiSegmentSink& sink,
                             const std::string& segment_name, const std::string& field_name,
                             int32_t total_doc_count, int32_t index_version,
                             bool omit_term_freq_and_positions, bool omit_norms) {
    if (inputs.empty()) {
        return 0;
    }

    // Single-input fast path: when there is exactly one input and norms are
    // omitted (always true for V4 spills), copy posting bytes directly and
    // rebuild only metadata, eliminating the decode/re-encode cycle for the
    // common case where the buffer was flushed exactly once before finish. The
    // spill's omit_term_freq_and_positions flag is in lockstep with the output,
    // so both phrase-on (positions present) AND DOCS_ONLY (omit, empty .prx)
    // single spills byte-copy correctly — MergeSingleInput copies the .prx
    // verbatim and rebuilds .fnm with has_prox = !omit.
    if (inputs.size() == 1 && omit_norms) {
        return MergeSingleInput(inputs[0], sink, segment_name, field_name, total_doc_count,
                                index_version, omit_term_freq_and_positions, omit_norms);
    }

    // Spill segments are successive slices of the SAME monotonically increasing
    // _rid stream, so every input already carries GLOBAL absolute doc_ids that
    // never overlap across inputs (FreqProxEncoder::StartTerm resets _last_doc
    // to 0, so a segment's first doc delta IS its absolute id). The k-way merge
    // therefore concatenates the already-ordered runs verbatim; applying a
    // per-segment offset here would double-shift every doc after the first spill
    // and push doc_ids past total_doc_count.

    // Create TermEnums for each input (流式 .tis 游标解析；内存输入经包装
    // 后同样走游标接口)。
    std::vector<std::unique_ptr<TermEnum>> enums;
    enums.reserve(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        enums.push_back(std::make_unique<TermEnum>(inputs[i].tis.get()));
    }

    // Seed the min-heap with the first term from each input.
    using Heap = std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>>;
    Heap heap;
    for (size_t i = 0; i < inputs.size(); ++i) {
        if (enums[i]->Next()) {
            const auto& e = enums[i]->Current();
            heap.push({e.field_number, e.term_utf8, static_cast<int32_t>(i)});
        }
    }

    // V4 windowed segments inline small terms' full posting bytes into the
    // .tis (zero extra GET on read). Gate on the same version the final output
    // is tagged with; legacy (<V4) merges keep the external-pointer format.
    const bool use_windowed = index_version >= FieldInfosWriter::kIndexVersionV4;
    const bool inline_small_terms = use_windowed;
    const uint32_t inline_threshold = SegmentWriter::kInlineMaxBytes;

    // Open the output writers.
    TermDictWriter dict(sink.tis, sink.tii, TermDictWriter::kDefaultIndexInterval,
                        TermDictWriter::kDefaultSkipInterval, inline_small_terms);
    FreqProxEncoder encoder(sink.frq, sink.prx, TermDictWriter::kDefaultSkipInterval,
                            TermDictWriter::kMaxSkipLevels, omit_term_freq_and_positions,
                            use_windowed, /*inline_capable=*/inline_small_terms);

    int64_t term_count = 0;

    // Per-term scratch, reused across the whole merge (capacity persists,
    // contents cleared per term) — the merged term's working set is FLAT:
    // doc-delta/freq arrays + the raw position VInt bytes + per-doc offsets.
    // No per-doc vector<DecodedDoc> materialization, no per-doc heap blocks.
    struct TermSource {
        int32_t input_index;
        // Copied: Current() is invalidated by the enum's Next(). 流式 .tis
        // 游标下 enum 的 inline span 只活到它的下一次 Next()，take() 把 span
        // 字节拷入本结构的 inline_bytes（≤ inline 上限的有界拷贝，V7 的
        // ≤257B stage 合约不变）并把 info 的指针重指向自有缓冲。
        TermInfo info;
        std::vector<uint8_t> inline_bytes;
    };
    std::vector<TermSource> sources;
    PostingDecoder::FlatPostings flat;
    std::vector<uint8_t> slim_frq; // merged SLIM .frq block (df < skip_interval)
    std::vector<uint8_t> slim_prx; // merged SLIM term's raw position payload (concat path)
    const std::vector<uint32_t> kNoU32;
    const std::vector<uint8_t> kNoBytes;
    const int32_t skip_interval = TermDictWriter::kDefaultSkipInterval;
    const bool has_prox = !omit_term_freq_and_positions;

    while (!heap.empty()) {
        // Pop the smallest (field, term). Collect all inputs that
        // share this exact (field, term).
        const auto top = heap.top();
        heap.pop();

        const int32_t cur_field = top.field_number;
        const std::string& cur_term = top.term_utf8;

        // ---- Phase 1: collect every input holding this exact (field, term),
        // in input (spill) order, advancing the enums. The df of every run is
        // known BEFORE any posting byte is touched (df lives in the .tis entry
        // ahead of the posting pointers), so Σdf — the merged doc frequency —
        // drives the slim/windowed/inline tier decision below exactly as it
        // would have driven a direct write of the same data. Heap op count is
        // unchanged versus the old drain (one pop + one push per entry).
        sources.clear();
        auto take = [&](int32_t idx) {
            TermSource src;
            src.input_index = idx;
            src.info = enums[idx]->Current().info;
            if (src.info.inlined) {
                // inline span 即拷即走（frq 在前、prx 紧随），随后才允许该
                // enum Next()（流式游标的滑窗会作废借用指针）。指针在两段
                // 都追加完成后再取，避免扩容搬家。
                const size_t fn = src.info.inline_frq_len;
                const size_t pn = src.info.inline_prx_len;
                src.inline_bytes.reserve(fn + pn);
                if (fn > 0) {
                    src.inline_bytes.insert(src.inline_bytes.end(), src.info.inline_frq,
                                            src.info.inline_frq + fn);
                }
                if (pn > 0) {
                    src.inline_bytes.insert(src.inline_bytes.end(), src.info.inline_prx,
                                            src.info.inline_prx + pn);
                }
                src.info.inline_frq = fn > 0 ? src.inline_bytes.data() : nullptr;
                src.info.inline_prx = pn > 0 ? src.inline_bytes.data() + fn : nullptr;
            }
            sources.push_back(std::move(src));
            if (enums[idx]->Next()) {
                const auto& ne = enums[idx]->Current();
                heap.push({ne.field_number, ne.term_utf8, idx});
            }
        };
        take(top.input_index);
        while (!heap.empty() && heap.top().field_number == cur_field &&
               heap.top().term_utf8 == cur_term) {
            const int32_t idx = heap.top().input_index;
            heap.pop();
            take(idx);
        }

        int64_t sum_df = 0;
        for (const auto& s : sources) {
            sum_df += s.info.doc_freq;
        }
        if (sum_df <= 0 || sum_df > std::numeric_limits<int32_t>::max()) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SegmentMerger: merged doc_freq out of range");
        }
        const auto df = static_cast<int32_t>(sum_df);

        FreqProxEncoder::FinishedTerm ft;
        bool staged = false;

        if (df < skip_interval && !g_force_slim_reencode_for_test) {
            // ---- 批 4 快路径（k>1 slim 字节级拼接）：Σdf < skip_interval ⇒
            // 合并结果仍是 slim term，且每个输入 run 必然也是 slim（df_i ≤
            // Σdf < skip_interval —— is_slim 纯由 df 推导），于是不再平面化
            // 解码 + 逐值重编，而是按 spill 序做字节级拼接：每输入仅首
            // docCode 重基，其余 .frq 字节 verbatim；.prx 信封解析为 raw
            // payload 拼接（kProxZstd 输入先 inflate），合并块的 mode/ZSTD
            // 决策由 EmitSlimTermPreEncoded 对拼接后的 payload 统一重跑。
            // 输出与下方平面化重编路径逐字节相同（交叉断言见
            // spill_merge_slim_concat_test）—— 这是 MergeSingleInput 整段
            // 字节拷贝在 k>1 的 term 级推广。
            slim_frq.clear();
            slim_prx.clear();
            int64_t concat_last_doc = 0;
            bool first_run = true;
            for (const auto& s : sources) {
                DCHECK(s.info.is_slim) << "df_i <= Σdf < skip_interval implies a slim run";
                if (s.info.inlined) {
                    // inline run：posting 字节在 .tis 条目里（take() 已拷入
                    // TermSource 自有缓冲），包内存源走同一拼接核心。
                    MemoryByteSource frq_mem(s.info.inline_frq, s.info.inline_frq_len);
                    MemoryByteSource prx_mem(s.info.inline_prx, s.info.inline_prx_len);
                    concat_last_doc = PostingDecoder::ConcatSlimRun(
                            frq_mem, has_prox ? &prx_mem : nullptr, s.info.doc_freq, has_prox,
                            first_run, concat_last_doc, &slim_frq, has_prox ? &slim_prx : nullptr);
                } else {
                    // 外置 run：游标前跳到 posting 指针后流式拼接（消费自定
                    // 界，与 DecodeFlat 同约定，停在块尾）。
                    auto& input = inputs[s.input_index];
                    input.frq->SkipForwardTo(s.info.freq_pointer);
                    ForwardByteSource* prx_src = nullptr;
                    if (has_prox) {
                        input.prx->SkipForwardTo(s.info.prox_pointer);
                        prx_src = input.prx.get();
                    }
                    concat_last_doc = PostingDecoder::ConcatSlimRun(
                            *input.frq, prx_src, s.info.doc_freq, has_prox, first_run,
                            concat_last_doc, &slim_frq, has_prox ? &slim_prx : nullptr);
                }
                first_run = false;
            }
            ft = encoder.EmitSlimTermPreEncoded(df, slim_frq, has_prox ? slim_prx : kNoBytes);
            staged = true;
            ++g_merge_stats.slim_concat_terms;
        } else {
            // ---- Phase 2: flat-decode each input's run in spill order. Spill
            // segments already carry GLOBAL absolute doc_ids in non-overlapping
            // ascending ranges (see the contract above), so the runs concatenate
            // verbatim — DecodeFlat re-bases only each subsequent run's FIRST
            // doc-delta (from "delta vs implicit 0" to "delta vs previous run's
            // last doc") and splices the position bytes untouched (within-doc
            // position deltas are self-anchored per doc). No doc-level merge, no
            // sort: the result is the exact flat input a direct write of the
            // merged term would have staged.
            flat.Clear();
            for (const auto& s : sources) {
                if (s.info.inlined) {
                    // Inline input term: the posting bytes live in the .tis entry
                    // (take() 已拷入 TermSource 自有缓冲)。Decode them directly —
                    // no .frq/.prx offset arithmetic. V4 spills are written
                    // inlined (lockstep with the final segment), so multi-spill
                    // merges hit this branch for every small term.
                    PostingDecoder::DecodeFlat(s.info.inline_frq, s.info.inline_frq_len,
                                               s.info.inline_prx, s.info.inline_prx_len,
                                               s.info.doc_freq, has_prox, s.info.is_slim, &flat);
                } else {
                    // 外置 term：把该输入的 .frq/.prx 游标前跳到本 term 的
                    // posting 指针（指针随 term 序单调，跳表等间隙字节顺带跳过），
                    // 流式解码自定界（doc_freq / 信封驱动），无需预知块尾。
                    auto& input = inputs[s.input_index];
                    input.frq->SkipForwardTo(s.info.freq_pointer);
                    ForwardByteSource* prx_src = nullptr;
                    if (has_prox) {
                        input.prx->SkipForwardTo(s.info.prox_pointer);
                        prx_src = input.prx.get();
                    }
                    PostingDecoder::DecodeFlat(*input.frq, prx_src, s.info.doc_freq, has_prox,
                                               s.info.is_slim, &flat);
                }
            }

            // ---- Phase 3: re-emit through the Σdf tier — the SAME dispatch a
            // direct write performs in FreqProxEncoder::StartTerm (slim below the
            // skip interval, windowed/PFOR at-or-above), fed pre-decoded flat
            // input, so the output bytes are identical to the direct write's.
            if (df < skip_interval) {
                // SLIM merged term（批 4 后仅测试强制重编时到达，作为拼接快
                // 路径的交叉对照）: rebuild the per-doc docCode VInts from the
                // flat arrays (cheap — df < skip_interval) and hand the spliced
                // raw position bytes to the slim pre-encoded emit, which applies
                // the exact FlushProxBlock mode-byte + ZSTD policy to the MERGED
                // payload (any input-side .prx envelope was already resolved by
                // DecodeFlat).
                slim_frq.clear();
                if (has_prox) {
                    for (int32_t i = 0; i < df; ++i) {
                        const uint32_t code = flat.doc_deltas[static_cast<size_t>(i)] << 1U;
                        const uint32_t freq = flat.freqs[static_cast<size_t>(i)];
                        if (freq == 1) {
                            AppendVInt(slim_frq, code | 1U);
                        } else {
                            AppendVInt(slim_frq, code);
                            AppendVInt(slim_frq, freq);
                        }
                    }
                } else {
                    for (int32_t i = 0; i < df; ++i) {
                        AppendVInt(slim_frq, flat.doc_deltas[static_cast<size_t>(i)]);
                    }
                }
                ft = encoder.EmitSlimTermPreEncoded(df, slim_frq,
                                                    has_prox ? flat.pos_vint : kNoBytes);
                staged = true;
            } else if (use_windowed) {
                // V4 windowed merged term (phrase-on or DOCS_ONLY): the flat
                // arrays are exactly the pre-decoded shape the windowed emit
                // consumes; WindowFrameEncoder receives the same input a direct
                // write would have buffered, so framing/W-selection/ZSTD reproduce
                // byte-for-byte.
                ft = encoder.EmitWindowedTermPreDecoded(
                        df, flat.doc_deltas, has_prox ? flat.freqs : kNoU32,
                        has_prox ? flat.pos_vint : kNoBytes, has_prox ? flat.pos_offsets : kNoU32);
                staged = true;
            } else {
                // Legacy (pre-V4) PFOR re-encode: replay the flat arrays through
                // the streaming encoder. Positions are prefix-summed straight off
                // the flat VInt bytes — still no per-doc heap blocks.
                encoder.StartTerm(df);
                int64_t doc = 0;
                size_t pb = 0;
                const uint8_t* pv = flat.pos_vint.data();
                for (int32_t i = 0; i < df; ++i) {
                    doc += flat.doc_deltas[static_cast<size_t>(i)];
                    const auto freq =
                            has_prox ? static_cast<int32_t>(flat.freqs[static_cast<size_t>(i)]) : 1;
                    encoder.StartDoc(static_cast<int32_t>(doc), freq);
                    if (has_prox) {
                        int32_t pos = 0;
                        for (int32_t f = 0; f < freq; ++f) {
                            // One LEB128 VInt (bounds enforced by DecodeFlat's scan).
                            uint32_t v = 0;
                            uint32_t shift = 0;
                            while (true) {
                                const uint8_t b = pv[pb++];
                                v |= static_cast<uint32_t>(b & 0x7FU) << shift;
                                if ((b & 0x80U) == 0) {
                                    break;
                                }
                                shift += 7;
                            }
                            pos += static_cast<int32_t>(v);
                            encoder.AddPosition(pos);
                        }
                    }
                    encoder.FinishDoc();
                }
                if (inline_small_terms) {
                    ft = encoder.FinishTermStaged();
                    staged = true;
                } else {
                    dict.Add(cur_field, cur_term, encoder.FinishTerm());
                }
            }
            ++g_merge_stats.reencode_terms;
        }

        if (staged && inline_small_terms) {
            // Stage the merged term's block; inline it if small, else flush
            // externally — mirroring SegmentWriter::AddStagedTerm.
            const size_t frq_n = ft.frq != nullptr ? ft.frq->size() : 0;
            const size_t prx_n = ft.prx != nullptr ? ft.prx->size() : 0;
            const size_t total = frq_n + prx_n;
            const bool can_inline = total <= static_cast<size_t>(inline_threshold) &&
                                    frq_n <= TermDictWriter::kInlineHardCapBytes &&
                                    prx_n <= TermDictWriter::kInlineHardCapBytes;
            if (can_inline) {
                dict.AddInline(cur_field, cur_term, ft.info, frq_n > 0 ? ft.frq->data() : nullptr,
                               static_cast<uint32_t>(frq_n), prx_n > 0 ? ft.prx->data() : nullptr,
                               static_cast<uint32_t>(prx_n));
            } else {
                if (frq_n > 0) {
                    sink.frq->WriteBytes(ft.frq->data(), frq_n);
                }
                if (prx_n > 0) {
                    sink.prx->WriteBytes(ft.prx->data(), prx_n);
                }
                dict.Add(cur_field, cur_term, ft.info);
            }
        } else if (staged) {
            // Non-inline mode: the pre-encoded emits wrote the block straight
            // to the real outputs; just record the external .tis entry.
            dict.Add(cur_field, cur_term, ft.info);
        }
        ++term_count;
    }

    dict.Close();

    // Write .fnm.
    {
        FieldInfoEntry fi;
        fi.name = field_name;
        fi.is_indexed = true;
        fi.has_prox = !omit_term_freq_and_positions;
        fi.omit_norms = omit_norms;
        fi.index_version = index_version;
        fi.flags = 0;
        FieldInfosWriter(sink.fnm).Write({fi});
    }

    // Write segments_N and segments.gen.
    {
        SegmentInfoEntry seg;
        seg.name = segment_name;
        seg.doc_count = total_doc_count;
        seg.del_gen = -1;
        seg.doc_store_offset = -1;
        seg.has_single_norm_file = true;
        seg.is_compound_file = -1;
        SegmentInfosWriter manifest_writer;
        manifest_writer.WriteSegmentsN(sink.segments_n, /*version=*/1, /*counter=*/1, {seg});
        manifest_writer.WriteSegmentsGen(sink.segments_gen, /*generation=*/1);
    }

    return term_count;
}

} // namespace doris::segment_v2::inverted_index::spimi
