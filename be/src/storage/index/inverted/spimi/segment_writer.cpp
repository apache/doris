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
          _skip_interval(skip_interval),
          _use_windowed(use_windowed),
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
    AddStagedTerm(field_number, term_text, _encoder.FinishTermStaged());
}

void SegmentWriter::AddStagedTerm(int32_t field_number, std::string_view term_text,
                                  const FreqProxEncoder::FinishedTerm& ft) {
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

        // SLIM fast path: the buffer's freq chain bytes ARE the on-disk slim
        // .frq block — phrase-on stores per-doc docCode VInts (same values in
        // the same order; VInt64/VInt of the same value encode identically and
        // direct-emit input is monotonic), DOCS_ONLY stores the bare doc-delta
        // VInts the omit format wants. Phrase-on additionally copies the prox
        // chain, whose within-doc position deltas ARE the raw .prx payload
        // FlushProxBlock would have built. One memcpy per slice replaces the
        // per-occurrence VInt decode + re-encode through StartDoc/AddPosition
        // for the dominant vocabulary tail.
        //
        // The chain FORMAT follows the BUFFER's omit flag, the on-disk format
        // follows the WRITER's — the copy is only legal when they AGREE. The
        // one legal mixed combination (omit writer over a phrase buffer, which
        // tests use to emit DOCS_ONLY from a generic buffer) has a docCode
        // chain but wants bare deltas, so it falls through to the replay
        // below, which decodes and re-encodes.
        if (st.doc_count < static_cast<uint32_t>(_skip_interval) &&
            buffer.OmitTfap() == _omit_tfap) {
            _slim_frq_scratch.clear();
            ByteSliceReader(buffer.Pool(), st.doc_start, st.doc_end)
                    .AppendRemainingTo(_slim_frq_scratch);
            _slim_prx_scratch.clear();
            if (!_omit_tfap) {
                ByteSliceReader(buffer.Pool(), st.pos_start, st.pos_end)
                        .AppendRemainingTo(_slim_prx_scratch);
            }
            const FreqProxEncoder::FinishedTerm ft = _encoder.EmitSlimTermPreEncoded(
                    static_cast<int32_t>(st.doc_count), _slim_frq_scratch, _slim_prx_scratch);
            const std::string_view term_text = buffer.TermAt(term.text_ref);
            if (_inline_small_terms) {
                AddStagedTerm(field_number, term_text, ft);
            } else {
                // Non-inline mode: the block already went straight to the real
                // outputs; just record the external .tis entry.
                _dict.Add(field_number, term_text, ft.info);
            }
            ++_term_count;
            ++emitted;
            continue;
        }

        // WINDOWED fast path (V4 phrase-on, df >= skip_interval): the prox
        // chain's within-doc position deltas are byte-identical to what the
        // replay's AddPosition would rebuild into the windowed position
        // buffer, so copy the whole chain once instead of decoding and
        // re-encoding every occurrence. The docCode chain is decoded into the
        // per-doc delta/freq vectors (df entries — cheap relative to
        // occurrences), and each doc's position byte offset is found by
        // skipping freq VInt ends in the copied buffer (a byte scan, no value
        // decode). Gated on the buffer being phrase-on (the chain must carry
        // positions) — the omit-writer-over-phrase-buffer mixed combination
        // never reaches here (_omit_tfap is false on this branch).
        if (!_omit_tfap && !buffer.OmitTfap() && _use_windowed &&
            st.doc_count >= static_cast<uint32_t>(_skip_interval)) {
            const uint32_t dc = st.doc_count;
            _win_dd_scratch.clear();
            _win_fq_scratch.clear();
            _win_off_scratch.clear();
            _slim_prx_scratch.clear();
            ByteSliceReader(buffer.Pool(), st.pos_start, st.pos_end)
                    .AppendRemainingTo(_slim_prx_scratch);
            ByteSliceReader code_reader(buffer.Pool(), st.doc_start, st.doc_end);
            const uint8_t* pv = _slim_prx_scratch.data();
            size_t pos_byte = 0;
            for (uint32_t k = 0; k < dc; ++k) {
                const uint64_t code = code_reader.ReadVInt64();
                const auto delta = static_cast<uint32_t>(code >> 1U);
                const uint32_t freq = (code & 1U) ? 1U : code_reader.ReadVInt();
                _win_dd_scratch.push_back(delta);
                _win_fq_scratch.push_back(freq);
                _win_off_scratch.push_back(static_cast<uint32_t>(pos_byte));
                for (uint32_t f = 0; f < freq; ++f) {
                    // Skip one position VInt: continuation bytes then the
                    // terminator.
                    while ((pv[pos_byte] & 0x80U) != 0) {
                        ++pos_byte;
                    }
                    ++pos_byte;
                }
            }
            DCHECK_EQ(pos_byte, _slim_prx_scratch.size())
                    << "prox chain length must match the per-doc freq walk";
            const FreqProxEncoder::FinishedTerm ft = _encoder.EmitWindowedTermPreDecoded(
                    static_cast<int32_t>(dc), _win_dd_scratch, _win_fq_scratch, _slim_prx_scratch,
                    _win_off_scratch);
            const std::string_view term_text = buffer.TermAt(term.text_ref);
            if (_inline_small_terms) {
                AddStagedTerm(field_number, term_text, ft);
            } else {
                _dict.Add(field_number, term_text, ft.info);
            }
            ++_term_count;
            ++emitted;
            continue;
        }

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
        if (_omit_tfap && buffer.OmitTfap()) {
            // DOCS_ONLY buffer: the chain holds one bare doc-delta VInt per doc
            // (no freq codes, no prox chain), so replay doc_count entries. This
            // branch only runs for PFOR terms (df >= skip_interval — slim omit
            // terms took the chain-copy fast path above); the encoder ignores
            // freq in omit mode, so StartDoc(_, 1) reproduces the same bytes.
            const uint32_t dc = st.doc_count;
            for (uint32_t k = 0; k < dc; ++k) {
                prev_doc += freq_reader.ReadVInt();
                _encoder.StartDoc(static_cast<int32_t>(prev_doc), /*freq=*/1);
                _encoder.FinishDoc();
            }
        } else if (_omit_tfap) {
            // Omit WRITER over a PHRASE buffer (the test-supported mixed
            // combination): decode the docCode chain, ignore freqs/positions,
            // and emit bare doc deltas. The buffer's prox chain exists but is
            // never read.
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
