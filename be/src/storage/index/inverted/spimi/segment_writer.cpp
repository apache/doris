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

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

SegmentWriter::SegmentWriter(ByteOutput* tis_out, ByteOutput* tii_out, ByteOutput* frq_out,
                             ByteOutput* prx_out, int32_t index_interval, int32_t skip_interval,
                             int32_t max_skip_levels, bool omit_term_freq_and_positions,
                             bool use_windowed, bool inline_small_terms, uint32_t inline_threshold)
        : _frq_out(frq_out),
          _prx_out(prx_out),
          _inline_small_terms(inline_small_terms),
          _inline_threshold(inline_threshold),
          _omit_tfap(omit_term_freq_and_positions),
          _dict(tis_out, tii_out, index_interval, skip_interval, inline_small_terms),
          _encoder(frq_out, prx_out, skip_interval, max_skip_levels, omit_term_freq_and_positions,
                   use_windowed, /*inline_capable=*/inline_small_terms) {}

void SegmentWriter::FinishAndAddTerm(int32_t field_number, std::string_view term_text) {
    if (!_inline_small_terms) {
        // Non-inline path: the encoder wrote the block straight to the real
        // .frq/.prx outputs; just record the external .tis entry.
        const TermInfo info = _encoder.FinishTerm();
        _dict.Add(field_number, term_text, info);
        return;
    }

    // Inline path: the encoder STAGED the term block. Decide inline-vs-flush.
    const FreqProxEncoder::FinishedTerm ft = _encoder.FinishTermStaged();
    const size_t frq_n = ft.frq != nullptr ? ft.frq->size() : 0;
    const size_t prx_n = ft.prx != nullptr ? ft.prx->size() : 0;
    const size_t total = frq_n + prx_n;
    const bool can_inline = total <= static_cast<size_t>(_inline_threshold) &&
                            frq_n <= TermDictWriter::kInlineHardCapBytes &&
                            prx_n <= TermDictWriter::kInlineHardCapBytes;

    if (can_inline) {
        // Store the block bytes IN the .tis entry; the real .frq/.prx outputs
        // are NOT advanced for this term (its external bytes simply never
        // exist), and the dict writer leaves the running pointer chain
        // unchanged so the next external term's delta stays consistent.
        _dict.AddInline(field_number, term_text, ft.info, frq_n > 0 ? ft.frq->data() : nullptr,
                        static_cast<uint32_t>(frq_n), prx_n > 0 ? ft.prx->data() : nullptr,
                        static_cast<uint32_t>(prx_n));
    } else {
        // Flush the block externally. ft.info.freq_pointer / prox_pointer were
        // captured from the real outputs' FilePointer at StartTerm, so writing
        // the staged bytes here lands them exactly at those offsets and the
        // NEXT term's StartTerm captures the advanced FilePointer.
        if (frq_n > 0) {
            _frq_out->WriteBytes(ft.frq->data(), frq_n);
        }
        if (prx_n > 0) {
            _prx_out->WriteBytes(ft.prx->data(), prx_n);
        }
        _dict.Add(field_number, term_text, ft.info);
    }
}

int64_t SegmentWriter::Emit(const SpimiPostingBuffer& buffer, int32_t field_number) {
    DCHECK(!_closed) << "SegmentWriter::Emit called after Close()";
    if (buffer.CompactDirectEmitReady()) {
        return EmitFromCompactDirect(buffer, field_number);
    }
    return EmitFromRecords(buffer, field_number);
}

int64_t SegmentWriter::EmitFromRecords(const SpimiPostingBuffer& buffer, int32_t field_number) {
    const auto& records = buffer.records();
    if (records.empty()) {
        return 0;
    }

    size_t i = 0;
    int64_t emitted = 0;
    while (i < records.size()) {
        const uint32_t text_ref = records[i].text_ref;

        // Find the end of this term's run (records sharing the text_ref).
        // Sort() groups them contiguously.
        size_t j = i + 1;
        while (j < records.size() && records[j].text_ref == text_ref) {
            ++j;
        }

        // Count distinct docs in [i, j).
        int32_t doc_freq = 1;
        for (size_t k = i + 1; k < j; ++k) {
            if (records[k].doc_id != records[k - 1].doc_id) {
                ++doc_freq;
            }
        }

        _encoder.StartTerm(doc_freq);

        size_t doc_start = i;
        while (doc_start < j) {
            const uint32_t doc_id = records[doc_start].doc_id;
            size_t doc_end = doc_start + 1;
            while (doc_end < j && records[doc_end].doc_id == doc_id) {
                ++doc_end;
            }
            const auto freq = static_cast<int32_t>(doc_end - doc_start);
            _encoder.StartDoc(static_cast<int32_t>(doc_id), freq);
            // DOCS_ONLY: records carry position=0 (DecodeTermToRecords) and the
            // encoder no-ops AddPosition, so skip the loop entirely. Byte-neutral
            // (no .prx is written either way); just avoids the no-op calls.
            if (!_omit_tfap) {
                for (size_t k = doc_start; k < doc_end; ++k) {
                    _encoder.AddPosition(static_cast<int32_t>(records[k].position));
                }
            }
            _encoder.FinishDoc();
            doc_start = doc_end;
        }

        const std::string_view term_text = buffer.TermAt(text_ref);
        FinishAndAddTerm(field_number, term_text);

        ++_term_count;
        ++emitted;
        i = j;
    }

    return emitted;
}

// A single streaming pass that decodes the VInt prefix and FOR suffix, regroups
// occurrences per doc and drives FreqProxEncoder term-by-term; the chunked
// decode and per-doc grouping share cursors/accumulators that would have to be
// threaded through helpers, so it is kept as one loop. Covered by the
// segment-roundtrip and segment-writer unit tests.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
int64_t SegmentWriter::EmitFromCompactDirect(const SpimiPostingBuffer& buffer,
                                             int32_t field_number) {
    // Iterates the compact per-term arrays in the text-sorted order Sort()
    // prepared, driving the encoder with exactly the same StartTerm / StartDoc
    // / AddPosition / FinishDoc / FinishTerm sequence as EmitFromRecords —
    // so the emitted .frq / .prx / .tis / .tii bytes are identical. The only
    // difference is the source: per-term (docs, positions) arrays instead of
    // a materialized SpimiRecord run.
    const auto& terms = buffer.SortedCompactTerms();
    int64_t emitted = 0;
    // Reused per-doc position buffer. Streaming decode reads the term's two
    // VInt slice chains occurrence-by-occurrence and only buffers ONE doc's
    // positions at a time (bounded by the max term-frequency within a single
    // doc — tiny), so a term holding a large share of all occurrences no longer
    // forces a multi-MB docs/positions materialization. This is the finish-
    // phase peak fix; the StartTerm / StartDoc / AddPosition / FinishDoc /
    // FinishTerm sequence is unchanged, so .frq/.prx/.tis/.tii stay identical.
    for (const auto& term : terms) {
        const auto st = buffer.CompactStreamsFor(term.term_id);
        const uint32_t n = st.occ_count;
        // Every interned term has at least one occurrence.
        DCHECK_GT(n, 0U);
        _encoder.StartTerm(static_cast<int32_t>(st.doc_count));
        // The freq chain is already grouped PER DOC (docCode = doc_delta<<1, low
        // bit = freq==1, else a trailing VInt(freq)); the prox chain holds the
        // within-doc position DELTAS per occurrence (reset to 0 each doc). Replay
        // doc-by-doc, prefix-summing the deltas back to absolute positions for
        // the encoder — no per-occurrence regrouping needed. The StartDoc/
        // AddPosition/FinishDoc sequence (hence .frq/.prx output) is byte-identical.
        ByteSliceReader freq_reader(buffer.Pool(), st.doc_start, st.doc_end);
        uint32_t prev_doc = 0;
        uint32_t emitted_occ = 0;
        if (_omit_tfap) {
            // DOCS_ONLY: the buffer never wrote a prox chain (st.pos_start ==
            // st.pos_end == 0), so do NOT construct a prox reader and skip the
            // AddPosition loop. The StartDoc(prev_doc, freq) call STAYS — the
            // encoder ignores freq in omit mode and writes only the doc-delta
            // VInt, so .frq is byte-identical. `freq` still advances emitted_occ.
            while (emitted_occ < n) {
                const uint64_t code = freq_reader.ReadVInt64();
                prev_doc += static_cast<uint32_t>(code >> 1U);
                const uint32_t freq = (code & 1U) ? 1U : freq_reader.ReadVInt();
                _encoder.StartDoc(static_cast<int32_t>(prev_doc), static_cast<int32_t>(freq));
                _encoder.FinishDoc();
                emitted_occ += freq;
            }
        } else {
            ByteSliceReader pos_reader(buffer.Pool(), st.pos_start, st.pos_end);
            while (emitted_occ < n) {
                const uint64_t code = freq_reader.ReadVInt64();
                prev_doc += static_cast<uint32_t>(code >> 1U);
                const uint32_t freq = (code & 1U) ? 1U : freq_reader.ReadVInt();
                _encoder.StartDoc(static_cast<int32_t>(prev_doc), static_cast<int32_t>(freq));
                // Positions are stored as within-doc deltas (reset to 0 per doc);
                // prefix-sum them back to absolute before re-encoding so the .prx
                // is byte-identical. Modular add round-trips any order.
                uint32_t pos = 0;
                for (uint32_t k = 0; k < freq; ++k) {
                    pos += pos_reader.ReadVInt();
                    _encoder.AddPosition(static_cast<int32_t>(pos));
                }
                _encoder.FinishDoc();
                emitted_occ += freq;
            }
        }

        const std::string_view term_text = buffer.TermAt(term.text_ref);
        FinishAndAddTerm(field_number, term_text);

        ++_term_count;
        ++emitted;
    }
    return emitted;
}

void SegmentWriter::Close() {
    if (_closed) {
        return;
    }
    _closed = true;
    _dict.Close();
}

} // namespace doris::segment_v2::inverted_index::spimi
