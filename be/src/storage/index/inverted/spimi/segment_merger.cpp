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
int64_t MergeSingleInput(const SegmentMerger::Input& input, const SpimiSegmentSink& sink,
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

    // Copy all posting bytes directly — no decode/re-encode cycle.
    // The single input's doc_offset is 0, so TermInfo pointers in
    // .tis remain valid and posting data needs no adjustment.
    sink.tis->WriteBytes(input.tis_bytes.data(), input.tis_bytes.size());
    sink.tii->WriteBytes(input.tii_bytes.data(), input.tii_bytes.size());
    sink.frq->WriteBytes(input.frq_bytes.data(), input.frq_bytes.size());
    sink.prx->WriteBytes(input.prx_bytes.data(), input.prx_bytes.size());

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

    // Return the term count from the input's .tis footer.
    TermEnum tenum(input.tis_bytes);
    return tenum.TotalEntries();
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

// Helper: compute the .prx byte range for one term.
// `prox_start` is the term's prox_pointer (absolute in .prx).
// `prox_end` is the next term's prox_pointer, or prx_bytes.size()
// for the last term.
std::pair<const uint8_t*, size_t> PrxRange(const std::vector<uint8_t>& prx_bytes,
                                           int64_t prox_start, int64_t prox_end) {
    if (prox_start < 0 || prox_start >= static_cast<int64_t>(prx_bytes.size())) {
        return {nullptr, 0};
    }
    if (prox_end <= prox_start) {
        return {nullptr, 0};
    }
    return {prx_bytes.data() + prox_start, static_cast<size_t>(prox_end - prox_start)};
}

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

int64_t SegmentMerger::Merge(const std::vector<Input>& inputs, const SpimiSegmentSink& sink,
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

    // Create TermEnums for each input.
    std::vector<std::unique_ptr<TermEnum>> enums;
    enums.reserve(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        enums.push_back(std::make_unique<TermEnum>(inputs[i].tis_bytes));
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
        // Copied: Current() is invalidated by the enum's Next(). The inline
        // spans inside borrow the input's .tis buffer, which outlives the merge.
        TermInfo info;
    };
    std::vector<TermSource> sources;
    PostingDecoder::FlatPostings flat;
    std::vector<uint8_t> slim_frq; // rebuilt merged SLIM .frq block (df < skip_interval)
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
            sources.push_back({idx, enums[idx]->Current().info});
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
            const auto& input = inputs[s.input_index];
            const uint8_t* frq_ptr = nullptr;
            size_t frq_len = 0;
            const uint8_t* prx_ptr = nullptr;
            size_t prx_len = 0;
            if (s.info.inlined) {
                // Inline input term: the posting bytes live in the .tis entry
                // (TermEnum recorded spans into the input's .tis buffer).
                // Decode them directly — no .frq/.prx offset arithmetic. V4
                // spills are written inlined (lockstep with the final
                // segment), so multi-spill merges hit this branch for every
                // small term.
                frq_ptr = s.info.inline_frq;
                frq_len = s.info.inline_frq_len;
                prx_ptr = s.info.inline_prx;
                prx_len = s.info.inline_prx_len;
            } else {
                // .frq byte range: we don't easily know the end without
                // peeking ahead, so use the full remaining buffer — the
                // decoder stops after doc_freq docs anyway.
                const int64_t frq_start = s.info.freq_pointer;
                frq_ptr = input.frq_bytes.data() + frq_start;
                frq_len = static_cast<size_t>(input.frq_bytes.size()) -
                          static_cast<size_t>(frq_start);

                const int64_t prx_start = s.info.prox_pointer;
                const int64_t prx_end = static_cast<int64_t>(input.prx_bytes.size());
                auto range = PrxRange(input.prx_bytes, prx_start, prx_end);
                prx_ptr = range.first;
                prx_len = range.second;
            }
            PostingDecoder::DecodeFlat(frq_ptr, frq_len, prx_ptr, prx_len, s.info.doc_freq,
                                       has_prox, s.info.is_slim, &flat);
        }

        // ---- Phase 3: re-emit through the Σdf tier — the SAME dispatch a
        // direct write performs in FreqProxEncoder::StartTerm (slim below the
        // skip interval, windowed/PFOR at-or-above), fed pre-decoded flat
        // input, so the output bytes are identical to the direct write's.
        FreqProxEncoder::FinishedTerm ft;
        bool staged = false;
        if (df < skip_interval) {
            // SLIM merged term: rebuild the per-doc docCode VInts from the
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
            ft = encoder.EmitSlimTermPreEncoded(df, slim_frq, has_prox ? flat.pos_vint : kNoBytes);
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
