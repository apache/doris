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
#include <memory>
#include <queue>
#include <utility>

#include "common/logging.h"
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

    while (!heap.empty()) {
        // Pop the smallest (field, term). Collect all inputs that
        // share this exact (field, term).
        const auto top = heap.top();
        heap.pop();

        const int32_t cur_field = top.field_number;
        const std::string& cur_term = top.term_utf8;

        // Gather decoded docs from all inputs that have this term.
        std::vector<DecodedDoc> merged_docs;

        // Process the first (smallest) input for this term.
        auto process_input = [&](int32_t idx) {
            const auto& input = inputs[idx];
            const auto& entry = enums[idx]->Current();

            const uint8_t* frq_ptr = nullptr;
            size_t frq_len = 0;
            const uint8_t* prx_ptr = nullptr;
            size_t prx_len = 0;
            if (entry.info.inlined) {
                // Inline input term: the posting bytes live in the .tis entry
                // (TermEnum recorded spans into the input's .tis buffer).
                // Decode them directly — no .frq/.prx offset arithmetic. V4
                // spills are now written inlined (lockstep with the final
                // segment), so multi-spill merges hit this branch for every
                // small term.
                frq_ptr = entry.info.inline_frq;
                frq_len = entry.info.inline_frq_len;
                prx_ptr = entry.info.inline_prx;
                prx_len = entry.info.inline_prx_len;
            } else {
                // Compute the .frq byte range for this term.
                // Use the next entry's freq_pointer or frq_bytes.size().
                const int64_t frq_start = entry.info.freq_pointer;
                // We don't easily know the end without peeking ahead.
                // Use the full remaining buffer — PostingDecoder stops
                // after doc_freq docs anyway.
                frq_ptr = input.frq_bytes.data() + frq_start;
                frq_len = static_cast<size_t>(input.frq_bytes.size()) -
                          static_cast<size_t>(frq_start);

                // Compute the .prx byte range.
                const int64_t prx_start = entry.info.prox_pointer;
                const int64_t prx_end = static_cast<int64_t>(input.prx_bytes.size());
                auto range = PrxRange(input.prx_bytes, prx_start, prx_end);
                prx_ptr = range.first;
                prx_len = range.second;
            }

            auto docs =
                    PostingDecoder::Decode(frq_ptr, frq_len, prx_ptr, prx_len, entry.info.doc_freq,
                                           !omit_term_freq_and_positions, entry.info.is_slim);

            // Spill segments already carry global absolute doc_ids, so append
            // the decoded run verbatim (no per-segment offset).
            merged_docs.insert(merged_docs.end(), std::make_move_iterator(docs.begin()),
                               std::make_move_iterator(docs.end()));
        };

        process_input(top.input_index);

        // Advance this input's enum; push back if more terms remain.
        if (enums[top.input_index]->Next()) {
            const auto& ne = enums[top.input_index]->Current();
            heap.push({ne.field_number, ne.term_utf8, top.input_index});
        }

        // Drain any other inputs with the same (field, term).
        while (!heap.empty() && heap.top().field_number == cur_field &&
               heap.top().term_utf8 == cur_term) {
            const auto dup = heap.top();
            heap.pop();
            process_input(dup.input_index);
            if (enums[dup.input_index]->Next()) {
                const auto& ne = enums[dup.input_index]->Current();
                heap.push({ne.field_number, ne.term_utf8, dup.input_index});
            }
        }

        // Sort merged docs by doc_id. Each input's run is already sorted and the
        // inputs' absolute-id ranges don't overlap, so this is a no-op merge of
        // already-ordered runs — but we sort defensively.
        std::stable_sort(
                merged_docs.begin(), merged_docs.end(),
                [](const DecodedDoc& a, const DecodedDoc& b) { return a.doc_id < b.doc_id; });

        // In the normal case every doc_id is unique across inputs (the _rid
        // stream is strictly increasing and spills never overlap), so there is
        // nothing to deduplicate.

        // Re-encode through FreqProxEncoder.
        const auto df = static_cast<int32_t>(merged_docs.size());
        encoder.StartTerm(df);
        for (const auto& doc : merged_docs) {
            encoder.StartDoc(doc.doc_id, doc.freq);
            for (int32_t pos : doc.positions) {
                encoder.AddPosition(pos);
            }
            encoder.FinishDoc();
        }
        if (inline_small_terms) {
            // Stage the merged term's block; inline it if small, else flush
            // externally — mirroring SegmentWriter::FinishAndAddTerm.
            const FreqProxEncoder::FinishedTerm ft = encoder.FinishTermStaged();
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
        } else {
            const TermInfo info = encoder.FinishTerm();
            dict.Add(cur_field, cur_term, info);
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
