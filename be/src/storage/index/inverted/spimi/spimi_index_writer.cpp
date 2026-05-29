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

#include "storage/index/inverted/spimi/spimi_index_writer.h"

#include <algorithm>
#include <utility>

#include "common/logging.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/inverted/spimi/index_output_byte_output.h"
#include "storage/index/inverted/spimi/segment_merger.h"

namespace doris::segment_v2::inverted_index::spimi {

// V4 (pure SPIMI) uses plain "_0.*" names; shadow/debug mode uses
// "_spimi_0.*" to avoid clashing with the CLucene-emitted segment.
SpimiIndexWriter::FileNames SpimiIndexWriter::GetFileNames(bool is_v4) {
    if (is_v4) {
        return {"_0.tis", "_0.tii", "_0.frq", "_0.prx",
                "_0.fnm", "_0.nrm", "segments_1", "segments.gen"};
    }
    return {"_spimi_0.tis", "_spimi_0.tii", "_spimi_0.frq", "_spimi_0.prx",
            "_spimi_0.fnm", "_spimi_0.nrm", "segments_spimi_1", "segments_spimi.gen"};
}

SpimiIndexWriter::OutputStreams SpimiIndexWriter::CreateOutputStreams(DorisFSDirectory* dir,
                                                                       const FileNames& names) {
    OutputStreams s;
    s.tis.reset(dir->createOutput(names.tis));
    s.tii.reset(dir->createOutput(names.tii));
    s.frq.reset(dir->createOutput(names.frq));
    s.prx.reset(dir->createOutput(names.prx));
    s.fnm.reset(dir->createOutput(names.fnm));
    s.nrm.reset(dir->createOutput(names.nrm));
    s.seg_n.reset(dir->createOutput(names.seg_n));
    s.seg_gen.reset(dir->createOutput(names.seg_gen));
    return s;
}

SpimiIndexWriter::SpimiIndexWriter(std::string field_name)
        : _field_name(std::move(field_name)),
          _buffer(std::make_unique<SpimiPostingBuffer>()),
          _spill_manager(std::make_unique<SpillManager>(_field_name)) {}

void SpimiIndexWriter::AppendToken(std::string_view term, uint32_t doc_id, uint32_t position) {
    DCHECK(_buffer != nullptr);
    _buffer->Append(term, doc_id, position);
}

bool SpimiIndexWriter::Saturated() const {
    DCHECK(_buffer != nullptr);
    return _buffer->Saturated();
}

bool SpimiIndexWriter::ShouldFlush() const {
    DCHECK(_buffer != nullptr);
    return _buffer->ShouldFlush();
}

void SpimiIndexWriter::FlushPending(int32_t doc_count) {
    DCHECK(_buffer != nullptr);
    _spill_manager->FlushBuffer(*_buffer, doc_count);
}

int64_t SpimiIndexWriter::MemoryUsage() const {
    int64_t buf = _buffer ? static_cast<int64_t>(_buffer->MemoryUsage()) : 0;
    int64_t spill = _spill_manager ? static_cast<int64_t>(_spill_manager->TotalSpillBytes()) : 0;
    return buf + spill;
}

void SpimiIndexWriter::Cleanup() {
    if (_spill_manager) {
        _spill_manager->CleanupSpillFiles();
    }
    _buffer.reset();
}

EmittedSegmentByteCounts SpimiIndexWriter::EmitDirect(const OutputStreams& streams,
                                                       const SpimiFinishConfig& config) {
    IndexOutputByteOutput tis_bo(streams.tis.get());
    IndexOutputByteOutput tii_bo(streams.tii.get());
    IndexOutputByteOutput frq_bo(streams.frq.get());
    IndexOutputByteOutput prx_bo(streams.prx.get());
    IndexOutputByteOutput fnm_bo(streams.fnm.get());
    IndexOutputByteOutput nrm_bo(streams.nrm.get());
    IndexOutputByteOutput seg_n_bo(streams.seg_n.get());
    IndexOutputByteOutput seg_gen_bo(streams.seg_gen.get());

    SpimiSegmentSink sink;
    sink.tis = &tis_bo;
    sink.tii = &tii_bo;
    sink.frq = &frq_bo;
    sink.prx = &prx_bo;
    sink.fnm = &fnm_bo;
    sink.nrm = &nrm_bo;
    sink.segments_n = &seg_n_bo;
    sink.segments_gen = &seg_gen_bo;

    const bool omit_norms = config.is_v4;
    EmittedSegmentByteCounts byte_counts;
    SpimiFulltextWriter::EmitSegment(*_buffer, sink, /*segment_name=*/"_0",
                                     config.field_name_utf8, config.doc_count,
                                     FieldInfosWriter::kIndexVersionV0,
                                     config.omit_term_freq_and_positions, omit_norms,
                                     &byte_counts);
    return byte_counts;
}

void SpimiIndexWriter::EmitMerged(const OutputStreams& streams,
                                   const SpimiFinishConfig& config) {
    // Flush remaining buffer contents as one more spill segment.
    if (_buffer->ShouldFlush() || _buffer->RecordCount() > 0) {
        _spill_manager->FlushBuffer(*_buffer, config.doc_count);
    }

    IndexOutputByteOutput tis_bo(streams.tis.get());
    IndexOutputByteOutput tii_bo(streams.tii.get());
    IndexOutputByteOutput frq_bo(streams.frq.get());
    IndexOutputByteOutput prx_bo(streams.prx.get());
    IndexOutputByteOutput fnm_bo(streams.fnm.get());
    IndexOutputByteOutput nrm_bo(streams.nrm.get());
    IndexOutputByteOutput seg_n_bo(streams.seg_n.get());
    IndexOutputByteOutput seg_gen_bo(streams.seg_gen.get());

    SpimiSegmentSink sink;
    sink.tis = &tis_bo;
    sink.tii = &tii_bo;
    sink.frq = &frq_bo;
    sink.prx = &prx_bo;
    sink.fnm = &fnm_bo;
    sink.nrm = &nrm_bo;
    sink.segments_n = &seg_n_bo;
    sink.segments_gen = &seg_gen_bo;

    const auto& spills = _spill_manager->Spills();
    std::vector<SegmentMerger::Input> inputs;
    inputs.reserve(spills.size());
    for (const auto& spill : spills) {
        SegmentMerger::Input inp;
        inp.tis_bytes = spill.tis_bytes;
        inp.tii_bytes = spill.tii_bytes;
        inp.frq_bytes = spill.frq_bytes;
        inp.prx_bytes = spill.prx_bytes;
        inp.doc_count = spill.doc_count;
        inputs.push_back(std::move(inp));
    }

    const bool omit_norms = config.is_v4;
    SegmentMerger::Merge(inputs, sink, /*segment_name=*/"_0", config.field_name_utf8,
                         config.doc_count, FieldInfosWriter::kIndexVersionV0,
                         config.omit_term_freq_and_positions, omit_norms);
}

void SpimiIndexWriter::Finish(DorisFSDirectory* dir, const SpimiFinishConfig& config) {
    // Nothing to emit if the buffer was never created or already cleaned up.
    if (!HasBuffer()) {
        return;
    }
    // If the buffer is saturated with zero terms we still need to emit
    // an empty segment so the reader can open it. But if it's truly
    // empty and not saturated there's no data at all.
    if (_buffer->RecordCount() == 0 && !_buffer->Saturated() &&
        _spill_manager->SpillCount() == 0) {
        _buffer.reset();
        return;
    }

    const FileNames names = GetFileNames(config.is_v4);
    OutputStreams streams;

    // Decide the emission path: if spill segments already exist (or
    // the buffer needs flushing), go through the k-way merge path.
    // Otherwise emit the buffer directly as a single segment.
    const bool has_spills = _spill_manager->SpillCount() > 0 || _buffer->ShouldFlush();

    // ErrorContext + FINALLY_CLOSE manages the eight IndexOutput streams.
    // CreateOutputStreams is inside the try block so that if any
    // createOutput throws, the partially-created streams are still
    // closed by FINALLY_CLOSE (finally_close handles null pointers).
    ErrorContext error_context;
    EmittedSegmentByteCounts byte_counts;

    try {
        streams = CreateOutputStreams(dir, names);
        if (has_spills) {
            EmitMerged(streams, config);
        } else {
            byte_counts = EmitDirect(streams, config);
        }
    } catch (CLuceneError& e) {
        error_context.eptr = std::current_exception();
        error_context.err_msg.append("SpimiIndexWriter::Finish CLuceneError: ");
        error_context.err_msg.append(e.what());
        LOG(ERROR) << error_context.err_msg;
    } catch (const doris::Exception& e) {
        error_context.eptr = std::current_exception();
        error_context.err_msg.append("SpimiIndexWriter::Finish doris::Exception: ");
        error_context.err_msg.append(e.what());
        LOG(ERROR) << error_context.err_msg;
    } catch (...) {
        // Catch-all: any unexpected exception type (e.g. std::bad_alloc)
        // must still flow through FINALLY_CLOSE to avoid leaking the
        // eight IndexOutput streams.
        error_context.eptr = std::current_exception();
        error_context.err_msg.append("SpimiIndexWriter::Finish unknown exception");
        LOG(ERROR) << error_context.err_msg;
    }

    // Always close all eight IndexOutput streams regardless of errors.
    FINALLY_CLOSE(streams.tis);
    FINALLY_CLOSE(streams.tii);
    FINALLY_CLOSE(streams.frq);
    FINALLY_CLOSE(streams.prx);
    FINALLY_CLOSE(streams.fnm);
    FINALLY_CLOSE(streams.nrm);
    FINALLY_CLOSE(streams.seg_n);
    FINALLY_CLOSE(streams.seg_gen);

    // Validate on-disk byte counts only for the direct-emit path where
    // we have accurate per-stream counts.
    if (!has_spills && !error_context.eptr) {
        SpimiSegmentFileNames seg_names;
        seg_names.tis = names.tis;
        seg_names.tii = names.tii;
        seg_names.frq = names.frq;
        seg_names.prx = names.prx;
        seg_names.fnm = names.fnm;
        seg_names.nrm = names.nrm;
        seg_names.segments_n = names.seg_n;
        seg_names.segments_gen = names.seg_gen;
        ValidateClosedSegmentByteCounts(dir, seg_names, byte_counts);
    }

    // Release in-memory data now that the segment is on disk.
    _buffer.reset();
    if (_spill_manager) {
        _spill_manager->CleanupSpillFiles();
    }

    if (error_context.eptr) {
        std::rethrow_exception(error_context.eptr);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
