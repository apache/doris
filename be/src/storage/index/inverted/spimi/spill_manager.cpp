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

#include "storage/index/inverted/spimi/spill_manager.h"

#include <utility>

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

SpillManager::SpillManager(std::string field_name) : _field_name(std::move(field_name)) {}

int64_t SpillManager::FlushBuffer(SpimiPostingBuffer& buffer, int32_t doc_count) {
    // Skip empty buffers — no point emitting a zero-term segment.
    // Use RecordCount() (not records().empty()) because compact mode
    // frees _records after migrating occurrences into per-term streams;
    // records().empty() would incorrectly return true for a non-empty
    // compact-mode buffer, causing silent data loss.
    if (buffer.RecordCount() == 0) {
        buffer.Reset();
        buffer.ResetFlushNeeded();
        return 0;
    }

    SpillSegment seg;
    seg.segment_name = "_spill_" + std::to_string(_spill_counter++);
    seg.doc_count = doc_count;

    // In-memory sinks for the eight segment streams.
    MemoryByteOutput m_tis;
    MemoryByteOutput m_tii;
    MemoryByteOutput m_frq;
    MemoryByteOutput m_prx;
    MemoryByteOutput m_fnm;
    MemoryByteOutput m_nrm; // V4 always omits norms, so this stays empty
    MemoryByteOutput m_segments_n;
    MemoryByteOutput m_segments_gen;

    SpimiSegmentSink sink {
            .tis = &m_tis,
            .tii = &m_tii,
            .frq = &m_frq,
            .prx = &m_prx,
            .fnm = &m_fnm,
            .nrm = &m_nrm,
            .segments_n = &m_segments_n,
            .segments_gen = &m_segments_gen,
    };

    // EmitSegment sorts the buffer and writes the full segment.
    // V4 spill segments always use omit_norms=true (scoring not
    // supported in V4) and index_version V1 (matching the finish()
    // path in inverted_index_writer.cpp).
    seg.term_count = SpimiFulltextWriter::EmitSegment(buffer, sink, seg.segment_name, _field_name,
                                                      doc_count, FieldInfosWriter::kIndexVersionV1,
                                                      /*omit_term_freq_and_positions=*/false,
                                                      /*omit_norms=*/true);

    // Move the byte vectors into the spill segment.
    seg.tis_bytes = std::move(m_tis.mutable_bytes());
    seg.tii_bytes = std::move(m_tii.mutable_bytes());
    seg.frq_bytes = std::move(m_frq.mutable_bytes());
    seg.prx_bytes = std::move(m_prx.mutable_bytes());
    seg.fnm_bytes = std::move(m_fnm.mutable_bytes());
    seg.segments_n_bytes = std::move(m_segments_n.mutable_bytes());
    seg.segments_gen_bytes = std::move(m_segments_gen.mutable_bytes());

    _spills.push_back(std::move(seg));

    // Reset the buffer for the next batch of Appends.
    buffer.Reset();
    buffer.ResetFlushNeeded();

    return _spills.back().term_count;
}

void SpillManager::CleanupSpillFiles() {
    _spills.clear();
}

size_t SpillManager::TotalSpillBytes() const {
    size_t total = 0;
    for (const auto& seg : _spills) {
        total += seg.tis_bytes.size() + seg.tii_bytes.size() + seg.frq_bytes.size() +
                 seg.prx_bytes.size() + seg.fnm_bytes.size() + seg.segments_n_bytes.size() +
                 seg.segments_gen_bytes.size();
    }
    return total;
}

} // namespace doris::segment_v2::inverted_index::spimi
