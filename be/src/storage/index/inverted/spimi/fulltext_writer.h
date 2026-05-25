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

#pragma once

#include <cstdint>
#include <string>

#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/lucene_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_infos_writer.h"
#include "storage/index/inverted/spimi/segment_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// Bundle of LuceneOutputs that together form one Lucene 2.x fulltext
// segment as the existing CLucene reader expects. The seven streams are
// the minimum set for a SPIMI segment with `omit_norms=true` and no
// compound packing — Doris's IndexFileWriter handles the optional .cfs
// bundling separately.
struct SpimiSegmentSink {
    LuceneOutput* tis = nullptr;          // term dictionary
    LuceneOutput* tii = nullptr;          // term dictionary sparse index
    LuceneOutput* frq = nullptr;          // doc-and-freq stream
    LuceneOutput* prx = nullptr;          // position stream
    LuceneOutput* fnm = nullptr;          // field info
    LuceneOutput* nrm = nullptr;          // norms (optional; only written
                                          // when fi.omit_norms = false)
    LuceneOutput* segments_n = nullptr;   // segments manifest (segments_<N>)
    LuceneOutput* segments_gen = nullptr; // redundancy generation pointer
};

// High-level facade orchestrating the SPIMI write path. Owns the in-memory
// SpimiPostingBuffer and drives all the per-stream writers (term dict +
// freq/prox + field-infos + segments). The user feeds token occurrences
// via AddOccurrence(), then calls Finish(...) to emit the full segment.
//
// Memory: the only sizeable buffer is the SpimiPostingBuffer (12 bytes
// per token occurrence + term arena + intern map). All other writers are
// streaming.
class SpimiFulltextWriter {
public:
    // Sink ownership stays with the caller. `field_name` is the name
    // written into the segment's `.fnm`. `segment_name` is the per-segment
    // prefix (e.g. "_0"); it appears in the segments_N manifest.
    SpimiFulltextWriter(SpimiSegmentSink sink, std::string segment_name, std::string field_name,
                        int32_t index_version = FieldInfosWriter::kIndexVersionV0);

    SpimiFulltextWriter(const SpimiFulltextWriter&) = delete;
    SpimiFulltextWriter& operator=(const SpimiFulltextWriter&) = delete;

    // Emits a complete SPIMI segment from an externally-owned buffer. Used
    // by the integration layer (InvertedIndexColumnWriter), which keeps a
    // long-lived SpimiPostingBuffer for the column's write lifetime and
    // only opens the seven sinks at finish() time. Sorts `buffer` in
    // place. `doc_count` is recorded into the segments_N manifest as the
    // segment's docCount.
    // `omit_norms`: when true, writes `.fnm`'s field-info with
    // omitNorms=1 and skips emitting `.nrm` entirely. V4 sets this
    // (pure-SPIMI, scoring not yet supported, reader's `norms()` is
    // a synthesizer that ignores `.nrm` anyway). Shadow / debug
    // modes that compare byte-for-byte against CLucene leave it
    // false so the SPIMI `.fnm` flag bits match CLucene's emit.
    static int64_t EmitSegment(SpimiPostingBuffer& buffer, const SpimiSegmentSink& sink,
                               const std::string& segment_name, const std::string& field_name,
                               int32_t doc_count,
                               int32_t index_version = FieldInfosWriter::kIndexVersionV0,
                               bool omit_term_freq_and_positions = false,
                               bool omit_norms = false);

    // Records one token occurrence. `doc_id` must be non-decreasing across
    // calls; `position` must be non-decreasing within the same doc_id.
    // The buffer dedupes identical (term, doc, position) triples at sort
    // time so the segment encoder only sees the canonical sequence.
    void AddOccurrence(uint32_t doc_id, std::string_view term, uint32_t position);

    // Marks `doc_id` as the largest doc id present in this segment. Drives
    // the segment manifest's `doc_count` (max doc_id + 1).
    void NoteDocId(uint32_t doc_id);

    // Sorts the buffer and emits the full segment (.tis/.tii/.frq/.prx/
    // .fnm/segments_N/segments.gen). Returns the number of distinct terms
    // written. After Finish(), AddOccurrence() must not be called again.
    int64_t Finish();

    int64_t TermCount() const { return _term_count; }
    int32_t DocCount() const { return _doc_count; }
    size_t BufferMemoryUsage() const { return _buffer.MemoryUsage(); }

private:
    SpimiSegmentSink _sink;
    std::string _segment_name;
    std::string _field_name;
    int32_t _index_version;
    SpimiPostingBuffer _buffer;
    int64_t _term_count = 0;
    int32_t _doc_count = 0;
    bool _finished = false;
};

} // namespace doris::segment_v2::inverted_index::spimi
