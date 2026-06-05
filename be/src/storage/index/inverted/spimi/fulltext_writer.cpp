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

#include "storage/index/inverted/spimi/fulltext_writer.h"

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/Directory.h>

#include <limits>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"

namespace doris::segment_v2::inverted_index::spimi {

SpimiFulltextWriter::SpimiFulltextWriter(SpimiSegmentSink sink, std::string segment_name,
                                         std::string field_name, int32_t index_version)
        : _sink(sink),
          _segment_name(std::move(segment_name)),
          _field_name(std::move(field_name)),
          _index_version(index_version) {
    DCHECK(_sink.tis != nullptr);
    DCHECK(_sink.tii != nullptr);
    DCHECK(_sink.frq != nullptr);
    DCHECK(_sink.prx != nullptr);
    DCHECK(_sink.fnm != nullptr);
    DCHECK(_sink.segments_n != nullptr);
    DCHECK(_sink.segments_gen != nullptr);
}

void SpimiFulltextWriter::AddOccurrence(uint32_t doc_id, std::string_view term, uint32_t position) {
    DCHECK(!_finished) << "AddOccurrence after Finish()";
    _buffer.Append(term, doc_id, position);
    NoteDocId(doc_id);
}

void SpimiFulltextWriter::NoteDocId(uint32_t doc_id) {
    // The CLucene 2.x segments_N format encodes doc_count as int32. Above
    // INT32_MAX the `static_cast<int32_t>(doc_id) + 1` below would overflow
    // (UB on signed addition) and the emitted segment would advertise a
    // wildly wrong doc count. Doris segments never approach this in
    // practice, but per the project coding standard ("upon discovering
    // errors or unexpected situations, report errors or crash — never
    // allow the process to continue") use `DORIS_CHECK` so the bound is
    // enforced in release as well as debug.
    DORIS_CHECK(doc_id < static_cast<uint32_t>(std::numeric_limits<int32_t>::max()));
    const auto next = static_cast<int32_t>(doc_id) + 1;
    if (next > _doc_count) {
        _doc_count = next;
    }
}

int64_t SpimiFulltextWriter::Finish() {
    DCHECK(!_finished);
    _finished = true;
    // Direct single-flush path: this IS the final segment (no spill/merge), so
    // inline small terms (V4 only; EmitSegment gates on use_windowed). The spill
    // path now ALSO inlines its transient segments, in lockstep with the final
    // segment, so a single-spill byte-copy merge yields an inlined result.
    _term_count =
            EmitSegment(_buffer, _sink, _segment_name, _field_name, _doc_count, _index_version,
                        /*omit_term_freq_and_positions=*/false, /*omit_norms=*/false,
                        /*out_byte_counts=*/nullptr, /*inline_small_terms=*/true);
    return _term_count;
}

namespace {

// Mirrors CLucene's `int_to_byte4` (be/../contrib/clucene/.../Similarity.cpp
// line 231). For lengths below NUM_FREE_VALUES (≈ 220) the encoded byte
// is the length itself — the common case for fulltext docs which rarely
// have > 200 tokens.
constexpr int32_t kNumFreeNormValues = 220;
uint8_t EncodeLengthNorm(int32_t length) {
    if (length < 0) {
        return 0;
    }
    if (length < kNumFreeNormValues) {
        return static_cast<uint8_t>(length);
    }
    auto i = static_cast<uint64_t>(length - kNumFreeNormValues);
    int32_t num_bits = 0;
    {
        uint64_t v = i;
        while (v != 0) {
            ++num_bits;
            v >>= 1U;
        }
    }
    uint32_t encoded;
    if (num_bits < 4) {
        encoded = static_cast<uint32_t>(i);
    } else {
        const int32_t shift = num_bits - 4;
        encoded = (static_cast<uint32_t>(i >> shift) & 0x07U) |
                  static_cast<uint32_t>((shift + 1) << 3);
    }
    return static_cast<uint8_t>(kNumFreeNormValues + encoded);
}

constexpr uint8_t kNormsHeader[] = {'N', 'R', 'M', 0xFF};

std::vector<int32_t> ComputeDocLengths(const SpimiPostingBuffer& buffer, int32_t doc_count) {
    std::vector<int32_t> lengths(static_cast<size_t>(std::max(0, doc_count)), 0);
    for (const auto& rec : buffer.records()) {
        if (rec.doc_id < static_cast<uint32_t>(doc_count)) {
            ++lengths[rec.doc_id];
        }
    }
    return lengths;
}

// CLucene's `defaultNorm = encodeNorm(1) = 1` for slots that
// `BufferedNorms::fill` writes when a doc didn't contribute to the
// field (null docs / docs missing the field).
constexpr uint8_t kDefaultNorm = 1;

void WriteNormsForField(ByteOutput* out, const SpimiPostingBuffer& buffer, int32_t doc_count) {
    out->WriteBytes(kNormsHeader, sizeof(kNormsHeader));
    const std::vector<int32_t> lengths = ComputeDocLengths(buffer, doc_count);
    // CLucene emits `writeLong(total_term_count)` between the
    // NORMS_HEADER and per-doc bytes (see `SDocumentWriter.cpp:988`).
    // The total IS used by the reader's `total_term_count_` accounting
    // for averageFieldLength in BM25-style similarities. SPIMI must
    // mirror this so shadow byte-equality tests against CLucene's
    // emit hold.
    int64_t total = 0;
    for (int32_t len : lengths) {
        total += len;
    }
    out->WriteLong(total);
    for (int32_t len : lengths) {
        out->WriteByte(len == 0 ? kDefaultNorm : EncodeLengthNorm(len));
    }
}

} // namespace

// The segment-emit entry point wires up the four output streams, the term dict,
// field infos and (optional) norms in one linear sequence; the steps are
// sequential setup with no reusable sub-unit, so inlining keeps the emit order
// obvious. Exercised by the SPIMI unit tests.
// NOLINTNEXTLINE(readability-function-size)
int64_t SpimiFulltextWriter::EmitSegment(SpimiPostingBuffer& buffer, const SpimiSegmentSink& sink,
                                         const std::string& segment_name,
                                         const std::string& field_name, int32_t doc_count,
                                         int32_t index_version, bool omit_term_freq_and_positions,
                                         bool omit_norms, EmittedSegmentByteCounts* out_byte_counts,
                                         bool inline_small_terms) {
    DCHECK(sink.tis != nullptr);
    DCHECK(sink.tii != nullptr);
    DCHECK(sink.frq != nullptr);
    DCHECK(sink.prx != nullptr);
    DCHECK(sink.fnm != nullptr);
    DCHECK(sink.segments_n != nullptr);
    DCHECK(sink.segments_gen != nullptr);

    // Belt-and-suspenders against a prox-chain desync. Only the READ direction is unsafe: when emit
    // reads the prox slice-chain (omit=false) the buffer MUST have written it (buffer omit=false),
    // else the SegmentWriter reads a chain that was never written. The reverse is safe — an omit=true
    // emit never touches the prox chain, so a non-omit buffer (whose positions we simply ignore) is
    // fine; tests rely on this to emit a DOCS_ONLY segment from a generic buffer. Production callers
    // single-source both flags from the field's support_phrase, so they always agree.
    DCHECK(omit_term_freq_and_positions || !buffer.OmitTfap())
            << "SPIMI omit_tfap desync: emit reads prox but buffer omitted it (buffer.OmitTfap()="
            << buffer.OmitTfap() << " emit_omit=" << omit_term_freq_and_positions << ")";

    // Norms (.nrm) are derived from per-doc token lengths via
    // ComputeDocLengths(), which iterates buffer.records(). When norms will
    // be written we must keep the materialized records, so disable the
    // compact direct-emit fast path in that case. V4 (omit_norms=true) and
    // any sink without a norm stream take the direct-emit path.
    const bool need_norms = !omit_norms && sink.nrm != nullptr;
    buffer.Sort(/*allow_direct_emit=*/!need_norms);
    // V4 (and later) emit the windowed `.frq`/`.prx` posting format; V0..V3
    // keep the legacy streaming PFOR/VInt path. The persisted per-field
    // index_version in `.fnm` is the durable read-side gate.
    const bool use_windowed = index_version >= FieldInfosWriter::kIndexVersionV4;
    // Inlining is a V4-only feature; only honour the caller's request when this
    // segment is actually windowed (the read path gates inline decode on -5).
    const bool inline_terms = use_windowed && inline_small_terms;
    SegmentWriter segment_writer(
            sink.tis, sink.tii, sink.frq, sink.prx, TermDictWriter::kDefaultIndexInterval,
            TermDictWriter::kDefaultSkipInterval, TermDictWriter::kMaxSkipLevels,
            omit_term_freq_and_positions, use_windowed, inline_terms);
    const int64_t term_count = segment_writer.Emit(buffer, /*field_number=*/0);
    segment_writer.Close();

    FieldInfoEntry fi;
    fi.name = field_name;
    fi.is_indexed = true;
    // .fnm `has_prox` must mirror the encoder's actual prox-emit behavior
    // so the production reader picks the matching no-prox / with-prox
    // decoder path.
    fi.has_prox = !omit_term_freq_and_positions;
    // `omit_norms` is parameterized:
    //  - V4 (pure SPIMI) sets it true: scoring not supported,
    //    reader's `norms()` is a synthesizer that ignores `.nrm`.
    //    CLucene's `IndexReader::norms()` short-circuits to null
    //    when `getFieldInfos()->fieldInfo(field)->omitNorms == 1`.
    //  - Shadow / debug modes (running SPIMI alongside CLucene for
    //    byte-equality validation) leave it false so the SPIMI
    //    `.fnm` flag bits match CLucene's emit.
    fi.omit_norms = omit_norms;
    fi.index_version = index_version;
    fi.flags = 0;
    FieldInfosWriter(sink.fnm).Write({fi});

    // Emit `.nrm` only when norms are NOT omitted AND the sink
    // provided one. Honest empty: if the caller wants norms but
    // didn't open a stream, that's the caller's choice (test
    // fixtures legitimately skip this).
    if (!fi.omit_norms && sink.nrm != nullptr) {
        WriteNormsForField(sink.nrm, buffer, doc_count);
    }

    SegmentInfoEntry seg;
    seg.name = segment_name;
    seg.doc_count = doc_count;
    seg.del_gen = -1;
    seg.doc_store_offset = -1;
    seg.has_single_norm_file = true;
    // We emit the segment as a set of individual files (no compound
    // packing). The integration layer re-packs them into the Doris .idx
    // compound file via IndexFileWriter.
    seg.is_compound_file = -1;
    SegmentInfosWriter manifest_writer;
    manifest_writer.WriteSegmentsN(sink.segments_n, /*version=*/1, /*counter=*/1, {seg});
    manifest_writer.WriteSegmentsGen(sink.segments_gen, /*generation=*/1);

    // Capture per-stream byte counts at the moment EmitSegment finished
    // writing. `FilePointer()` is the running count of bytes fed through
    // each ByteOutput; the caller can re-query the Directory for the
    // on-disk file length after close() and compare via
    // `ValidateClosedSegmentByteCounts` to catch async partial flushes.
    // .nrm is left at 0 when omit_norms=true so the validator skips it.
    if (out_byte_counts != nullptr) {
        out_byte_counts->tis = sink.tis->FilePointer();
        out_byte_counts->tii = sink.tii->FilePointer();
        out_byte_counts->frq = sink.frq->FilePointer();
        out_byte_counts->prx = sink.prx->FilePointer();
        out_byte_counts->fnm = sink.fnm->FilePointer();
        out_byte_counts->nrm = (sink.nrm != nullptr && !omit_norms) ? sink.nrm->FilePointer() : 0;
        out_byte_counts->segments_n = sink.segments_n->FilePointer();
        out_byte_counts->segments_gen = sink.segments_gen->FilePointer();
    }
    return term_count;
}

namespace {

void CheckLengthOrThrow(lucene::store::Directory* dir, const char* name, int64_t expected) {
    if (name == nullptr || expected <= 0) {
        return;
    }
    if (!dir->fileExists(name)) {
        throw doris::Exception(
                doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "SPIMI segment file '{}' missing after close (expected {} bytes)", name,
                        expected));
    }
    const int64_t actual = dir->fileLength(name);
    if (actual != expected) {
        throw doris::Exception(
                doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "SPIMI segment file '{}' size mismatch: wrote {} bytes, on-disk {} bytes "
                        "(partial flush or truncation suspected)",
                        name, expected, actual));
    }
}

} // namespace

void ValidateClosedSegmentByteCounts(lucene::store::Directory* dir,
                                     const SpimiSegmentFileNames& names,
                                     const EmittedSegmentByteCounts& expected) {
    DCHECK(dir != nullptr);
    CheckLengthOrThrow(dir, names.tis, expected.tis);
    CheckLengthOrThrow(dir, names.tii, expected.tii);
    CheckLengthOrThrow(dir, names.frq, expected.frq);
    CheckLengthOrThrow(dir, names.prx, expected.prx);
    CheckLengthOrThrow(dir, names.fnm, expected.fnm);
    // .nrm is conditional — only check when EmitSegment recorded a non-zero
    // byte count (omit_norms=false path).
    if (expected.nrm > 0) {
        CheckLengthOrThrow(dir, names.nrm, expected.nrm);
    }
    CheckLengthOrThrow(dir, names.segments_n, expected.segments_n);
    CheckLengthOrThrow(dir, names.segments_gen, expected.segments_gen);
}

} // namespace doris::segment_v2::inverted_index::spimi
