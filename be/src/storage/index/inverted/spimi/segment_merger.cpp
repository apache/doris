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
//   - omit_term_freq_and_positions is false (spill always has positions)
//   - omit_norms is true (spill always omits norms)
// This covers the V4 (pure SPIMI) path.
int64_t MergeSingleInput(const SegmentMerger::Input& input, const SpimiSegmentSink& sink,
                         const std::string& segment_name, const std::string& field_name,
                         int32_t total_doc_count, int32_t index_version,
                         bool omit_term_freq_and_positions, bool omit_norms) {
    // The byte-copy is only valid when the single input's on-disk
    // encoding matches these output flags (positions present, norms
    // omitted).  The dispatch guard in Merge() enforces this; the
    // DCHECK makes the coupling crash-loud in debug for any future
    // caller that bypasses the guard.
    DCHECK(omit_norms && !omit_term_freq_and_positions);

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

// One cursor walking a single input segment's TermEnum.
struct MergeCursor {
    int32_t input_index = 0;
    int32_t doc_offset = 0; // added to each decoded doc_id
    TermEnum* tenum = nullptr;
};

// Heap entry: the current term from one input segment.
struct HeapEntry {
    int32_t field_number;
    std::string term_utf8;
    int32_t input_index;
    int32_t doc_offset;

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

    // Single-input fast path: when there is exactly one input and
    // the output format matches the spill format (has positions,
    // omits norms — always true for V4), copy posting bytes
    // directly and rebuild only metadata.  This eliminates the
    // decode/re-encode cycle for the common case where the buffer
    // was flushed exactly once before finish.
    if (inputs.size() == 1 && !omit_term_freq_and_positions && omit_norms) {
        return MergeSingleInput(inputs[0], sink, segment_name, field_name, total_doc_count,
                                index_version, omit_term_freq_and_positions, omit_norms);
    }

    // Compute per-input doc_id offsets.
    std::vector<int32_t> doc_offsets(inputs.size(), 0);
    {
        int32_t running = 0;
        for (size_t i = 0; i < inputs.size(); ++i) {
            doc_offsets[i] = running;
            running += inputs[i].doc_count;
        }
    }

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
            heap.push({e.field_number, e.term_utf8, static_cast<int32_t>(i), doc_offsets[i]});
        }
    }

    // Open the output writers.
    TermDictWriter dict(sink.tis, sink.tii, TermDictWriter::kDefaultIndexInterval,
                        TermDictWriter::kDefaultSkipInterval);
    FreqProxEncoder encoder(sink.frq, sink.prx, TermDictWriter::kDefaultSkipInterval,
                            TermDictWriter::kMaxSkipLevels, omit_term_freq_and_positions);

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
        auto process_input = [&](int32_t idx, int32_t offset) {
            const auto& input = inputs[idx];
            const auto& entry = enums[idx]->Current();

            // Compute the .frq byte range for this term.
            // Use the next entry's freq_pointer or frq_bytes.size().
            const int64_t frq_start = entry.info.freq_pointer;
            // We don't easily know the end without peeking ahead.
            // Use the full remaining buffer — PostingDecoder stops
            // after doc_freq docs anyway.
            const size_t frq_len =
                    static_cast<size_t>(input.frq_bytes.size()) - static_cast<size_t>(frq_start);

            // Compute the .prx byte range.
            const int64_t prx_start = entry.info.prox_pointer;
            const int64_t prx_end = static_cast<int64_t>(input.prx_bytes.size());
            auto [prx_ptr, prx_len] = PrxRange(input.prx_bytes, prx_start, prx_end);

            auto docs = PostingDecoder::Decode(input.frq_bytes.data() + frq_start, frq_len, prx_ptr,
                                               prx_len, entry.info.doc_freq,
                                               !omit_term_freq_and_positions);

            // Apply doc_id offset and append.
            for (auto& d : docs) {
                d.doc_id += offset;
            }
            merged_docs.insert(merged_docs.end(), std::make_move_iterator(docs.begin()),
                               std::make_move_iterator(docs.end()));
        };

        process_input(top.input_index, top.doc_offset);

        // Advance this input's enum; push back if more terms remain.
        if (enums[top.input_index]->Next()) {
            const auto& ne = enums[top.input_index]->Current();
            heap.push({ne.field_number, ne.term_utf8, top.input_index, top.doc_offset});
        }

        // Drain any other inputs with the same (field, term).
        while (!heap.empty() && heap.top().field_number == cur_field &&
               heap.top().term_utf8 == cur_term) {
            const auto dup = heap.top();
            heap.pop();
            process_input(dup.input_index, dup.doc_offset);
            if (enums[dup.input_index]->Next()) {
                const auto& ne = enums[dup.input_index]->Current();
                heap.push({ne.field_number, ne.term_utf8, dup.input_index, dup.doc_offset});
            }
        }

        // Sort merged docs by doc_id (should already be sorted
        // within each input, and inputs are in order, but a stable
        // sort ensures correctness even with overlapping ranges).
        // In practice the offsets guarantee strict ordering so
        // this is a no-op merge of already-sorted runs — but we
        // sort defensively.
        std::stable_sort(
                merged_docs.begin(), merged_docs.end(),
                [](const DecodedDoc& a, const DecodedDoc& b) { return a.doc_id < b.doc_id; });

        // Deduplicate: if two inputs happen contain the same doc_id
        // (shouldn't happen with correct offsets, but guard anyway),
        // merge their positions.
        // In the normal case every doc_id is unique.

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
        const TermInfo info = encoder.FinishTerm();
        dict.Add(cur_field, cur_term, info);
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
