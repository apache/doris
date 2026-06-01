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

#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {
// Process-wide counter giving each SpillManager a unique tmp subdir, so
// concurrent writers (e.g. parallel column writers) never collide.
std::atomic<uint64_t> g_spill_dir_counter {0};

// Resolves the base directory for spill tmp files. Prefers an explicit override
// ($DORIS_SPILL_TMP), else /tmp. tmp files are always BE-local (never S3).
std::string ResolveBaseTmpDir() {
    if (const char* env = std::getenv("DORIS_SPILL_TMP"); env != nullptr && env[0] != '\0') {
        return std::string(env);
    }
    return "/tmp";
}
} // namespace

SpillManager::SpillManager(std::string field_name, bool is_v4, std::string tmp_dir)
        : _field_name(std::move(field_name)), _is_v4(is_v4), _tmp_dir(std::move(tmp_dir)) {
    if (_tmp_dir.empty()) {
        // Process-unique subdir: <base>/spimi_spill_<pid>_<counter>.
        const uint64_t uniq = g_spill_dir_counter.fetch_add(1, std::memory_order_relaxed);
        _tmp_dir = ResolveBaseTmpDir() + "/spimi_spill_" + std::to_string(::getpid()) + "_" +
                   std::to_string(uniq);
    }
}

Status SpillManager::EnsureTmpDir() {
    if (_tmp_dir_ready) {
        return Status::OK();
    }
    RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_tmp_dir,
                                                                    /*failed_if_exists=*/false));
    _tmp_dir_ready = true;
    return Status::OK();
}

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

    // In-memory sinks for the eight segment streams. These are TRANSIENT: they
    // hold the encoded bytes only long enough to stream them out to the tmp
    // file, then free at scope exit so zero spill bytes stay resident.
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
    // V4 spill segments always use omit_norms=true (scoring not supported in
    // V4). Index version moves in LOCKSTEP with the final segment: V4 spills are
    // written kIndexVersionV4 (windowed-capable + inline) so that
    // SegmentMerger::MergeSingleInput can byte-copy the spill's .frq/.prx under a
    // final .fnm that advertises V4. Adaptive per-term windowing keeps small-df
    // terms legacy, so a V4 spill is byte-for-byte what a V4 final segment emits.
    // Non-V4 (legacy shadow/debug) spills keep V1 as before. The encoder is
    // UNTOUCHED, so the final .idx remains byte-identical.
    const int32_t index_version =
            _is_v4 ? FieldInfosWriter::kIndexVersionV4 : FieldInfosWriter::kIndexVersionV1;
    seg.term_count = SpimiFulltextWriter::EmitSegment(buffer, sink, seg.segment_name, _field_name,
                                                      doc_count, index_version,
                                                      /*omit_term_freq_and_positions=*/false,
                                                      /*omit_norms=*/true);

    // Stream the eight encoded streams out to one node-local tmp file and free
    // the in-memory vectors. IO errors are thrown as doris::Exception so they
    // flow through SpimiIndexWriter::Finish's try/catch + FINALLY_CLOSE.
    if (Status st = EnsureTmpDir(); !st.ok()) {
        throw doris::Exception(st);
    }

    seg.spill_path = _tmp_dir + "/" + seg.segment_name;
    io::FileWriterPtr writer;
    if (Status st = io::global_local_filesystem()->create_file(seg.spill_path, &writer); !st.ok()) {
        throw doris::Exception(st);
    }

    // Register the segment (carrying its spill_path) BEFORE the first append, so
    // that if append() (e.g. ENOSPC mid-merge) or close() throws after the tmp
    // file already exists, CleanupSpillFiles() still finds and deletes it. On the
    // throw path Finish/add_values go straight to cleanup without ever calling
    // LoadSpill, so a registered-but-half-written segment is never read back.
    _spills.push_back(std::move(seg));
    SpillSegment& reg = _spills.back();

    int64_t offset = 0;
    // Appends one stream's bytes, records its [offset,length) range, advances.
    auto append_stream = [&](const std::vector<uint8_t>& bytes, SpillSegment::Range& range) {
        range.offset = offset;
        range.length = static_cast<int64_t>(bytes.size());
        if (!bytes.empty()) {
            if (Status st = writer->append(Slice(bytes.data(), bytes.size())); !st.ok()) {
                throw doris::Exception(st);
            }
        }
        offset += range.length;
    };

    append_stream(m_tis.bytes(), reg.tis);
    append_stream(m_tii.bytes(), reg.tii);
    append_stream(m_frq.bytes(), reg.frq);
    append_stream(m_prx.bytes(), reg.prx);
    append_stream(m_fnm.bytes(), reg.fnm);
    append_stream(m_segments_n.bytes(), reg.segments_n);
    append_stream(m_segments_gen.bytes(), reg.segments_gen);

    if (Status st = writer->close(); !st.ok()) {
        throw doris::Exception(st);
    }

    const int64_t term_count = reg.term_count;

    // The MemoryByteOutput vectors free here (scope exit) => zero resident
    // spill bytes; only the tmp file holds the encoded streams.

    // Reset the buffer for the next batch of Appends.
    buffer.Reset();
    buffer.ResetFlushNeeded();

    return term_count;
}

Status SpillManager::LoadSpill(size_t i, SegmentMerger::Input& out) const {
    if (i >= _spills.size()) {
        return Status::InternalError("SpillManager::LoadSpill index {} out of range ({})", i,
                                     _spills.size());
    }
    const SpillSegment& seg = _spills[i];

    io::FileReaderSPtr reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(seg.spill_path, &reader));

    // Reads one stream's [offset,length) range into `dst`.
    auto read_stream = [&](const SpillSegment::Range& range, std::vector<uint8_t>& dst) -> Status {
        dst.resize(static_cast<size_t>(range.length));
        if (range.length == 0) {
            return Status::OK();
        }
        size_t bytes_read = 0;
        RETURN_IF_ERROR(reader->read_at(static_cast<size_t>(range.offset),
                                        Slice(dst.data(), dst.size()), &bytes_read));
        if (bytes_read != static_cast<size_t>(range.length)) {
            return Status::InternalError(
                    "SpillManager::LoadSpill short read on {} (got {}, want {})", seg.spill_path,
                    bytes_read, range.length);
        }
        return Status::OK();
    };

    // Only .tis/.tii/.frq/.prx are consumed by the merge; .fnm/segments_* are
    // rebuilt from scratch by the merger and need not be read back.
    RETURN_IF_ERROR(read_stream(seg.tis, out.tis_bytes));
    RETURN_IF_ERROR(read_stream(seg.tii, out.tii_bytes));
    RETURN_IF_ERROR(read_stream(seg.frq, out.frq_bytes));
    RETURN_IF_ERROR(read_stream(seg.prx, out.prx_bytes));
    out.doc_count = seg.doc_count;

    static_cast<void>(reader->close());
    return Status::OK();
}

void SpillManager::CleanupSpillFiles() {
    // Best-effort delete each spill tmp file. delete_file returns OK when the
    // file is already gone, so this is idempotent and leak-safe on the error
    // path as well.
    for (const auto& seg : _spills) {
        if (seg.spill_path.empty()) {
            continue;
        }
        if (Status st = io::global_local_filesystem()->delete_file(seg.spill_path); !st.ok()) {
            LOG(WARNING) << "SpimiIndexWriter: failed to delete spill tmp file " << seg.spill_path
                         << ": " << st;
        }
    }
    _spills.clear();

    // Also remove the now-empty per-instance spill subdir so it does not leak in
    // /tmp on either the success or the exception path. delete_directory returns
    // OK when the dir is already gone, so this is idempotent. Reset the flag so a
    // subsequent flush recreates the dir via EnsureTmpDir().
    if (_tmp_dir_ready && !_tmp_dir.empty()) {
        if (Status st = io::global_local_filesystem()->delete_directory(_tmp_dir); !st.ok()) {
            LOG(WARNING) << "SpimiIndexWriter: failed to delete spill tmp dir " << _tmp_dir << ": "
                         << st;
        }
        _tmp_dir_ready = false;
    }
}

SpillManager::~SpillManager() {
    // Defensive: an abandoned manager (one that never had CleanupSpillFiles()
    // called, e.g. an early return before Finish) still cleans up its tmp files
    // and subdir so nothing leaks in /tmp. CleanupSpillFiles is idempotent.
    if (_tmp_dir_ready || !_spills.empty()) {
        CleanupSpillFiles();
    }
}

size_t SpillManager::TotalSpillBytes() const {
    // Spilled bytes now live on disk, not in RAM. Account only the small
    // per-spill metadata so SpimiIndexWriter::MemoryUsage reflects true
    // resident RAM against the 256 MiB budget.
    size_t total = 0;
    for (const auto& seg : _spills) {
        total += sizeof(SpillSegment) + seg.segment_name.size() + seg.spill_path.size();
    }
    return total;
}

} // namespace doris::segment_v2::inverted_index::spimi
