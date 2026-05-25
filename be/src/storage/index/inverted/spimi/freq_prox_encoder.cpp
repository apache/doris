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

#include "storage/index/inverted/spimi/freq_prox_encoder.h"

#include <algorithm>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

FreqProxEncoder::FreqProxEncoder(ByteOutput* frq_out, ByteOutput* prx_out,
                                 int32_t skip_interval, int32_t max_skip_levels,
                                 bool omit_term_freq_and_positions)
        : _frq_out(frq_out),
          _prx_out(prx_out),
          _skip_interval(skip_interval),
          _omit_tfap(omit_term_freq_and_positions),
          _skip_list_writer(skip_interval, max_skip_levels) {
    DCHECK(_frq_out != nullptr);
    // _prx_out may be nullptr ONLY when omit_tfap is true. In that mode the
    // encoder never touches the prox stream so callers can pass nullptr to
    // make the intent explicit (or pass a real stream and rely on the
    // branching below — either works).
    DCHECK(_omit_tfap || _prx_out != nullptr);
    DCHECK_GT(_skip_interval, 0);
}

void FreqProxEncoder::StartTerm(int32_t expected_doc_freq) {
    DCHECK(!_term_open) << "StartTerm called while a term was already open";
    DCHECK(!_doc_open);
    DCHECK_GT(expected_doc_freq, 0) << "Lucene format requires df > 0 per term";
    _term_open = true;
    _expected_doc_freq = expected_doc_freq;
    _term_freq_start = _frq_out->FilePointer();
    // In omit_tfap mode the prox pointer is always 0 (CLucene's contract:
    // every term's proxPointer is 0 because the prox stream is empty for
    // this field). Hardcoding 0 avoids touching `_prx_out` when it may be
    // nullptr.
    _term_prox_start = _omit_tfap ? 0 : _prx_out->FilePointer();
    _doc_freq = 0;
    _last_doc = 0;

    // Phase 35 — choose the block encoding based on docFreq. When df is
    // below the skip-interval, the kDefault per-doc VInt encoding is
    // byte-equal to what CLucene's SDocumentWriter emits for the same
    // term, so the SPIMI shadow segment passes the byte-equality audit.
    // When df is at-or-above the skip-interval, CLucene switches to
    // PFOR blocks (SDocumentWriter.cpp:1257). SPIMI mirrors the switch
    // by emitting a `kCodeModeSpimiPfor` header byte and buffering
    // (doc_delta, freq) into the PFOR vectors below; FinishTerm flushes
    // the buffers via `SpimiPforEncoder` once the full term is seen.
    _use_pfor = (expected_doc_freq >= _skip_interval);
    _pfor_doc_deltas.clear();
    _pfor_freqs.clear();

    if (_use_pfor) {
        // PFOR mode: header byte only; sub-blocks (each carrying its
        // own VInt(count) + byte(width) + bitpacked payload) follow at
        // FinishTerm via `SpimiPforEncoder::EncodeBlock`.
        _frq_out->WriteByte(kCodeModeSpimiPfor);
    } else {
        // kDefault mode (byte-equal to CLucene's
        // `SDocumentWriter::appendPostings:1330` for the same term):
        // codec byte + VInt(doc_count) + per-doc encoding.
        _frq_out->WriteByte(kCodeModeDefault);
        _frq_out->WriteVInt(expected_doc_freq);
    }

    _skip_list_writer.Reset(expected_doc_freq, _term_freq_start, _term_prox_start);
}

void FreqProxEncoder::StartDoc(int32_t doc_id, int32_t freq) {
    DCHECK(_term_open);
    DCHECK(!_doc_open);
    DCHECK_GE(doc_id, 0);
    DCHECK_GT(freq, 0);
    DCHECK(_doc_freq == 0 || doc_id > _last_doc)
            << "Documents must be added in strictly ascending order";

    // Skip-list emission: every skip_interval-th doc boundary (after the
    // boundary doc was already emitted), record skip data for the *previous*
    // doc + pointers, then write the next-skip-block delta encoding. In
    // omit_tfap mode the prox pointer is always 0 (no prox stream).
    if (_doc_freq > 0 && (_doc_freq % _skip_interval) == 0) {
        const int64_t prox_ptr = _omit_tfap ? 0 : _prx_out->FilePointer();
        _skip_list_writer.SetSkipData(_last_doc, _frq_out->FilePointer(), prox_ptr);
        _skip_list_writer.BufferSkip(_doc_freq);
    }

    const uint32_t doc_delta = static_cast<uint32_t>(doc_id - _last_doc);
    if (_use_pfor) {
        // Phase 35 PFOR mode — buffer the (doc_delta, freq) pair. The
        // PFOR sub-blocks are emitted in `FinishTerm` once the full
        // term is seen, because `SpimiPforEncoder` chooses the per-
        // block bit-width based on the max value in the block, which
        // we can only know once buffering is complete. Freq stays out
        // of the buffer when omit_tfap is true (matches `kDefault`'s
        // branching).
        _pfor_doc_deltas.push_back(doc_delta);
        if (!_omit_tfap) {
            _pfor_freqs.push_back(static_cast<uint32_t>(freq));
        }
    } else if (_omit_tfap) {
        // omit_tfap=true: .frq stores the raw doc-id delta as a VInt, with
        // NO low-bit freq flag and NO separate freq VInt. Matches CLucene's
        // `SDocumentsWriter::flush` no-prox-no-freq branch and is the format
        // the reader expects when the field's omitTermFreqAndPositions
        // metadata bit is set. `freq` is implicitly 1; the caller is
        // expected to pass freq=1 but we don't enforce it because the value
        // is simply discarded here.
        _frq_out->WriteVInt(static_cast<int32_t>(doc_delta));
    } else {
        const uint32_t code = doc_delta << 1U;
        if (freq == 1) {
            _frq_out->WriteVInt(static_cast<int32_t>(code | 1U));
        } else {
            _frq_out->WriteVInt(static_cast<int32_t>(code));
            _frq_out->WriteVInt(freq);
        }
    }

    _last_doc = doc_id;
    _current_freq = freq;
    _positions_remaining = freq;
    _last_position_in_doc = 0;
    _doc_open = true;
}

void FreqProxEncoder::AddPosition(int32_t position) {
    DCHECK(_doc_open);
    DCHECK_GE(position, _last_position_in_doc);

    if (_omit_tfap) {
        // omit_tfap=true: positions are silently dropped. The encoder API
        // still accepts AddPosition calls so the caller (SegmentWriter::
        // Emit) doesn't need to branch on omit_tfap.
        return;
    }
    DCHECK_GT(_positions_remaining, 0);
    _prx_out->WriteVInt(position - _last_position_in_doc);
    _last_position_in_doc = position;
    --_positions_remaining;
}

void FreqProxEncoder::FinishDoc() {
    DCHECK(_doc_open);
    // In omit_tfap mode we silently drop positions, so _positions_remaining
    // never decrements. Only check the invariant when positions are tracked.
    DCHECK(_omit_tfap || _positions_remaining == 0)
            << "FinishDoc called before all `freq` positions were emitted";
    _doc_open = false;
    ++_doc_freq;
}

TermInfo FreqProxEncoder::FinishTerm() {
    DCHECK(_term_open);
    DCHECK(!_doc_open);
    DCHECK_EQ(_doc_freq, _expected_doc_freq)
            << "Expected doc frequency did not match the actual count";

    if (_use_pfor) {
        // Flush the buffered doc-deltas (and freqs, when has_prox) as
        // contiguous PFOR sub-blocks. SpimiPforEncoder's kBlockSize
        // bounds each sub-block; for skip_interval = 512 that is 4
        // sub-blocks per skip window, matching Lucene 9.x PForUtil's
        // BLOCK_SIZE = 128 nested inside CLucene's PFOR_BLOCK_SIZE =
        // skip_interval. The reader recovers `doc_freq` from the .tis
        // term-info and consumes sub-blocks in order until that count
        // is reached, then (if has_prox) consumes another `doc_freq`
        // worth of sub-blocks for freqs.
        auto emit_chunks = [this](std::vector<uint32_t>& values) {
            const size_t total = values.size();
            for (size_t pos = 0; pos < total; pos += SpimiPforEncoder::kBlockSize) {
                const size_t chunk = std::min(SpimiPforEncoder::kBlockSize, total - pos);
                SpimiPforEncoder::EncodeBlock(values.data() + pos, chunk, _frq_out);
            }
        };
        emit_chunks(_pfor_doc_deltas);
        if (!_omit_tfap) {
            emit_chunks(_pfor_freqs);
        }
    }

    const int64_t skip_pointer = _skip_list_writer.WriteSkip(_frq_out);

    TermInfo info;
    info.doc_freq = _doc_freq;
    info.freq_pointer = _term_freq_start;
    info.prox_pointer = _term_prox_start;
    info.skip_offset = (_doc_freq >= _skip_interval)
                               ? static_cast<int32_t>(skip_pointer - _term_freq_start)
                               : 0;

    _term_open = false;
    return info;
}

} // namespace doris::segment_v2::inverted_index::spimi
