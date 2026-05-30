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

#include <zstd.h>

#include <algorithm>

#include "common/logging.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"
#include "util/faststring.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {
// Whole-term .frq/.prx compression uses ZSTD level 1. An isolated benchmark
// (V4-vs-V2 interleaved write-time ratio — drift-immune — over the real
// httplogs corpus at 1M and 3M rows/segment) showed level 1 matches level 3's
// on-disk size while restoring V4's write-CPU edge over V2 (V4/V2 ratio 0.96
// vs level 3's 1.00): level 3's per-term compress was eating the entire
// write-CPU win for no size gain at this data's redundancy.
//
// We call ZSTD_compress directly because get_block_compression_codec(ZSTD)
// hard-codes ZSTD_CLEVEL_DEFAULT (3) and exposes no level knob. Decompression
// is level-independent, so the term-docs / prox readers still decode this via
// the registry ZSTD codec unchanged (verified by the SPIMI roundtrip UT).
//
// Compress `data[0..n)` into `comp`; return true only when the result beats raw
// by more than the 10-byte envelope header (mode byte + two VInt lengths), else
// false so the caller emits the block raw. `comp` MUST be a caller-owned local
// faststring (fresh per call, never a reused member) to avoid the
// assign_copy-on-poisoned-buffer ASAN hazard.
//
// `cctx` is the encoder's single reused compression context. ZSTD_compressCCtx
// reuses its internal workspace across calls; the previous ZSTD_compress one-shot
// allocated+initialised+freed a fresh CCtx on EVERY term, which on a large-vocab
// segment is hundreds of thousands of malloc/init/free cycles — pure write-CPU
// burned for no benefit. Reusing one context is the dominant ZSTD CPU saving and
// produces byte-identical output (level + input fully determine the result).
inline bool TryCompressBlock(ZSTD_CCtx* cctx, const uint8_t* data, size_t n, faststring* comp) {
    const size_t bound = ZSTD_compressBound(n);
    comp->resize(bound);
    const size_t csize = ZSTD_compressCCtx(cctx, comp->data(), bound, data, n, /*level=*/1);
    if (ZSTD_isError(csize) || csize + 10 >= n) {
        return false;
    }
    comp->resize(csize);
    return true;
}

inline void AppendVInt(std::vector<uint8_t>& buf, uint32_t v) {
    while (v & ~0x7FU) {
        buf.push_back(static_cast<uint8_t>((v & 0x7FU) | 0x80U));
        v >>= 7U;
    }
    buf.push_back(static_cast<uint8_t>(v));
}
} // namespace

FreqProxEncoder::FreqProxEncoder(ByteOutput* frq_out, ByteOutput* prx_out, int32_t skip_interval,
                                 int32_t max_skip_levels, bool omit_term_freq_and_positions)
        : _frq_out(nullptr),
          _frq_real(frq_out),
          _prx_out(prx_out),
          _skip_interval(skip_interval),
          _omit_tfap(omit_term_freq_and_positions),
          _skip_list_writer(skip_interval, max_skip_levels) {
    DCHECK(_frq_real != nullptr);
    // All per-term .frq writes go to the staging buffer; FinishTerm flushes it
    // (raw or ZSTD) to the real output.
    _frq_out = &_frq_term_buf;
    // _prx_out may be nullptr ONLY when omit_tfap is true. In that mode the
    // encoder never touches the prox stream so callers can pass nullptr to
    // make the intent explicit (or pass a real stream and rely on the
    // branching below — either works).
    DCHECK(_omit_tfap || _prx_out != nullptr);
    DCHECK_GT(_skip_interval, 0);
    // One compression context reused for every term's .frq/.prx block (see
    // TryCompressBlock). Created once here, freed in the destructor.
    _cctx = ZSTD_createCCtx();
    DCHECK(_cctx != nullptr);
}

FreqProxEncoder::~FreqProxEncoder() {
    ZSTD_freeCCtx(_cctx); // null-safe per the ZSTD contract
}

void FreqProxEncoder::StartTerm(int32_t expected_doc_freq) {
    DCHECK(!_term_open) << "StartTerm called while a term was already open";
    DCHECK(!_doc_open);
    DCHECK_GT(expected_doc_freq, 0) << "Lucene format requires df > 0 per term";
    _term_open = true;
    _expected_doc_freq = expected_doc_freq;
    _frq_term_buf.Clear();                       // stage this term's .frq
    _term_freq_start = _frq_real->FilePointer(); // where the (flushed) block lands
    // In omit_tfap mode the prox pointer is always 0 (CLucene's contract:
    // every term's proxPointer is 0 because the prox stream is empty for
    // this field). Hardcoding 0 avoids touching `_prx_out` when it may be
    // nullptr.
    _term_prox_start = _omit_tfap ? 0 : _prx_out->FilePointer();
    _prox_raw.clear(); // stage this term's position-deltas
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
    _pfor_freq_blocks.Clear();

    if (_use_pfor) {
        // PFOR mode: header byte only; sub-blocks (each carrying its
        // own VInt(count) + byte(width) + bitpacked payload) follow at
        // FinishTerm via `SpimiPforEncoder::EncodeBlock`.
        _frq_out->WriteByte(kCodeModeSpimiPfor);
        // Anchor for PFOR skip entries, in REAL .frq coordinates (the space the
        // skip list's pointers live in): _term_freq_start is where this term's
        // staged block will land in _frq_real, and _frq_out->FilePointer() is
        // the post-header offset within the staging buffer (== 1, just past the
        // codec byte). Their sum is the real file offset just past the header.
        //
        // This MUST be real-space: SkipListWriter::Reset() seeds
        // _last_skip_freq_pointer with _term_freq_start (also real), and
        // WriteSkipEntry stores `_cur_freq_pointer - _last_skip_freq_pointer`.
        // The previous code captured only the staging FilePointer (~1), so for
        // every term after the first (where _term_freq_start has advanced) the
        // delta went negative — silently writing a wrapped VInt into the skip
        // list in release builds (corrupt skip data; only large-df PFOR terms
        // emit skips, so small-df tests never tripped it), and aborting the
        // ASAN DCHECK_GE(freq_delta, 0) in SkipListWriter.
        _pfor_frq_anchor = _term_freq_start + _frq_out->FilePointer();
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
        // Skip entries only occur in PFOR mode (skip_interval ≤ df). The .frq
        // pointer is pinned to the post-header anchor so incremental doc-delta
        // flushing does not shift the emitted skip bytes (byte-identical).
        const int64_t frq_ptr = _use_pfor ? _pfor_frq_anchor : _frq_out->FilePointer();
        _skip_list_writer.SetSkipData(_last_doc, frq_ptr, prox_ptr);
        _skip_list_writer.BufferSkip(_doc_freq);
    }

    const auto doc_delta = static_cast<uint32_t>(doc_id - _last_doc);
    if (_use_pfor) {
        // Phase 35 PFOR mode — buffer the (doc_delta, freq) pair. The
        // PFOR sub-blocks are emitted in `FinishTerm` once the full
        // term is seen, because `SpimiPforEncoder` chooses the per-
        // block bit-width based on the max value in the block, which
        // we can only know once buffering is complete. Freq stays out
        // of the buffer when omit_tfap is true (matches `kDefault`'s
        // branching).
        _pfor_doc_deltas.push_back(doc_delta);
        if (_pfor_doc_deltas.size() == SpimiPforEncoder::kBlockSize) {
            // Flush a full doc-delta sub-block immediately; bytes are identical
            // to flushing it at FinishTerm (each block encodes independently).
            SpimiPforEncoder::EncodeBlock(_pfor_doc_deltas.data(), _pfor_doc_deltas.size(),
                                          _frq_out);
            _pfor_doc_deltas.clear();
        }
        if (!_omit_tfap) {
            _pfor_freqs.push_back(static_cast<uint32_t>(freq));
            if (_pfor_freqs.size() == SpimiPforEncoder::kBlockSize) {
                // Pack a full freq sub-block into the temp buffer (bytes
                // identical to emitting it directly into the freq region).
                SpimiPforEncoder::EncodeBlock(_pfor_freqs.data(), _pfor_freqs.size(),
                                              &_pfor_freq_blocks);
                _pfor_freqs.clear();
            }
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
    // Stage the VInt position-delta for the whole term; FlushProxBlock() writes
    // (and optionally ZSTD-compresses) the term's prox block at FinishTerm.
    AppendVInt(_prox_raw, static_cast<uint32_t>(position - _last_position_in_doc));
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
        // Full doc-delta sub-blocks were already streamed in StartDoc; flush
        // only the trailing partial block here, then the freq region.
        if (!_pfor_doc_deltas.empty()) {
            SpimiPforEncoder::EncodeBlock(_pfor_doc_deltas.data(), _pfor_doc_deltas.size(),
                                          _frq_out);
        }
        if (!_omit_tfap) {
            // Flush the trailing partial freq block, then bulk-append the whole
            // packed freq region (same bytes as emitting each sub-block here).
            if (!_pfor_freqs.empty()) {
                SpimiPforEncoder::EncodeBlock(_pfor_freqs.data(), _pfor_freqs.size(),
                                              &_pfor_freq_blocks);
            }
            const auto& fr = _pfor_freq_blocks.bytes();
            if (!fr.empty()) {
                _frq_out->WriteBytes(fr.data(), fr.size());
            }
        }
    }

    // Write the whole-term prox block (mode header + raw or ZSTD payload) at
    // _term_prox_start (nothing else wrote to _prx_out during the term).
    FlushProxBlock();

    // Skip data is written into the per-term buffer; its offset is relative to
    // the term's .frq start (buffer offset 0). The term-docs reader reads the
    // term whole and decodes sequentially, so it never consumes skip_offset —
    // it is recorded relative to the (uncompressed) term .frq for completeness.
    const int64_t skip_pointer = _skip_list_writer.WriteSkip(_frq_out);

    TermInfo info;
    info.doc_freq = _doc_freq;
    info.freq_pointer = _term_freq_start;
    info.prox_pointer = _term_prox_start;
    info.skip_offset = (_doc_freq >= _skip_interval) ? static_cast<int32_t>(skip_pointer) : 0;

    // Flush the whole term's staged .frq (raw, or ZSTD behind a kCodeModeZstd
    // envelope) to the real output at _term_freq_start.
    FlushFrqBlock();

    _term_open = false;
    return info;
}

void FreqProxEncoder::FlushFrqBlock() {
    const auto& buf = _frq_term_buf.bytes();
    const size_t n = buf.size();
    faststring comp;
    if (n >= kProxCompressMin && TryCompressBlock(_cctx, buf.data(), n, &comp)) {
        // A single term's .frq stays far below 2 GB (arena byte cap), so the
        // VInt length casts below never lose bits.
        DCHECK_LE(n, static_cast<size_t>(INT32_MAX));
        _frq_real->WriteByte(kCodeModeZstd);
        _frq_real->WriteVInt(static_cast<int32_t>(n));
        _frq_real->WriteVInt(static_cast<int32_t>(comp.size()));
        _frq_real->WriteBytes(comp.data(), comp.size());
        return;
    }
    // Raw fallback: emit the term .frq verbatim (its first byte is the inner
    // kCodeModeDefault/kCodeModeSpimiPfor mode, never kCodeModeZstd).
    if (n > 0) {
        _frq_real->WriteBytes(buf.data(), n);
    }
}

void FreqProxEncoder::FlushProxBlock() {
    if (_omit_tfap) {
        return; // no prox stream in this mode
    }
    const size_t n = _prox_raw.size();
    faststring comp;
    // Only keep the compressed form if it actually wins after the mode-byte + two
    // VInt length headers (~≤10 B) — TryCompressBlock enforces that margin.
    if (n >= kProxCompressMin && TryCompressBlock(_cctx, _prox_raw.data(), n, &comp)) {
        DCHECK_LE(n, static_cast<size_t>(INT32_MAX)); // single-term .prx << 2 GB
        _prx_out->WriteByte(kProxZstd);
        _prx_out->WriteVInt(static_cast<int32_t>(n));
        _prx_out->WriteVInt(static_cast<int32_t>(comp.size()));
        _prx_out->WriteBytes(comp.data(), comp.size());
        return;
    }
    // Raw fallback: mode byte then the VInt position-deltas. The reader knows
    // the position count from the per-doc freqs, so no length prefix is needed.
    _prx_out->WriteByte(kProxRaw);
    if (n > 0) {
        _prx_out->WriteBytes(_prox_raw.data(), n);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
