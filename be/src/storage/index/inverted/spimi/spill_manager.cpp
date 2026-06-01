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
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/file_byte_output.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {
// Process-wide counter giving each SpillManager a unique tmp subdir, so
// concurrent writers (e.g. parallel column writers) never collide.
std::atomic<uint64_t> g_spill_dir_counter {0};

// Resolves the base directory for spill tmp files. tmp files are always BE-local
// (never S3). Resolution order:
//   1. config::inverted_index_spimi_spill_path (if non-empty)
//   2. config::spill_storage_root_path         (if non-empty)
//   3. $DORIS_SPILL_TMP                         (if set/non-empty)
//   4. "/tmp"
std::string ResolveBaseTmpDir() {
    if (!config::inverted_index_spimi_spill_path.empty()) {
        return config::inverted_index_spimi_spill_path;
    }
    if (!config::spill_storage_root_path.empty()) {
        return config::spill_storage_root_path;
    }
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

    // Resolve + lazily create the tmp dir BEFORE creating any per-stream file.
    // IO errors are thrown as doris::Exception so they flow through
    // SpimiIndexWriter::Finish's try/catch + FINALLY_CLOSE.
    if (Status st = EnsureTmpDir(); !st.ok()) {
        throw doris::Exception(st);
    }
    seg.spill_path = _tmp_dir + "/" + seg.segment_name;

    // Register the segment (carrying its spill_path stem) BEFORE creating any
    // file, so that if create_file/append/close throws after a tmp file already
    // exists, CleanupSpillFiles() still finds and deletes every per-stream file.
    // On the throw path Finish/add_values go straight to cleanup without ever
    // calling LoadSpill, so a registered-but-half-written segment is never read.
    _spills.push_back(std::move(seg));
    SpillSegment& reg = _spills.back();

    // Create one node-local tmp file per stream and wrap each in a streaming
    // FileByteOutput. The streams are INTERLEAVED during EmitSegment (the
    // encoder writes a little .tis, then .frq/.prx, etc.), so they cannot share
    // one sequential file — hence one file per stream. The .nrm stream is unused
    // (V4 omits norms), so it is given a MemoryByteOutput that stays empty and
    // is never persisted.
    struct StreamFile {
        const char* ext;
        io::FileWriterPtr writer;
        std::unique_ptr<FileByteOutput> out;
    };

    auto open_stream = [&](const char* ext) -> StreamFile {
        StreamFile sf;
        sf.ext = ext;
        const std::string path = reg.StreamPath(ext);
        if (Status st = io::global_local_filesystem()->create_file(path, &sf.writer); !st.ok()) {
            throw doris::Exception(st);
        }
        sf.out = std::make_unique<FileByteOutput>(sf.writer.get());
        return sf;
    };

    StreamFile s_tis = open_stream("tis");
    StreamFile s_tii = open_stream("tii");
    StreamFile s_frq = open_stream("frq");
    StreamFile s_prx = open_stream("prx");
    StreamFile s_fnm = open_stream("fnm");
    StreamFile s_segments_n = open_stream("segments_n");
    StreamFile s_segments_gen = open_stream("segments_gen");

    // V4 always omits norms; the .nrm sink stays empty and is not persisted.
    MemoryByteOutput m_nrm;

    SpimiSegmentSink sink {
            .tis = s_tis.out.get(),
            .tii = s_tii.out.get(),
            .frq = s_frq.out.get(),
            .prx = s_prx.out.get(),
            .fnm = s_fnm.out.get(),
            .nrm = &m_nrm,
            .segments_n = s_segments_n.out.get(),
            .segments_gen = s_segments_gen.out.get(),
    };

    // EmitSegment sorts the buffer and writes the full segment, streaming each
    // stream straight to its tmp file. V4 spill segments always use
    // omit_norms=true (scoring not supported in V4). Index version moves in
    // LOCKSTEP with the final segment: V4 spills are written kIndexVersionV4
    // (windowed-capable + inline) so that SegmentMerger::MergeSingleInput can
    // byte-copy the spill's .frq/.prx under a final .fnm that advertises V4.
    // Adaptive per-term windowing keeps small-df terms legacy, so a V4 spill is
    // byte-for-byte what a V4 final segment emits. Non-V4 (legacy shadow/debug)
    // spills keep V1 as before. The encoder is UNTOUCHED, and FileByteOutput's
    // FilePointer() returns the same running byte total MemoryByteOutput would,
    // so the final .idx remains byte-identical.
    const int32_t index_version =
            _is_v4 ? FieldInfosWriter::kIndexVersionV4 : FieldInfosWriter::kIndexVersionV1;
    reg.term_count = SpimiFulltextWriter::EmitSegment(buffer, sink, reg.segment_name, _field_name,
                                                      doc_count, index_version,
                                                      /*omit_term_freq_and_positions=*/false,
                                                      /*omit_norms=*/true);

    // Flush each stream's residual staging buffer (FileByteOutput::Finish
    // returns the first IO error seen), record its final length, and close the
    // writer. Any IO error -> doris::Exception.
    auto finalize_stream = [&](StreamFile& sf, SpillSegment::Range& range) {
        if (Status st = sf.out->Finish(); !st.ok()) {
            throw doris::Exception(st);
        }
        range.offset = 0;
        range.length = sf.out->FilePointer();
        if (Status st = sf.writer->close(); !st.ok()) {
            throw doris::Exception(st);
        }
    };

    finalize_stream(s_tis, reg.tis);
    finalize_stream(s_tii, reg.tii);
    finalize_stream(s_frq, reg.frq);
    finalize_stream(s_prx, reg.prx);
    finalize_stream(s_fnm, reg.fnm);
    finalize_stream(s_segments_n, reg.segments_n);
    finalize_stream(s_segments_gen, reg.segments_gen);

    const int64_t term_count = reg.term_count;

    // Only the small per-stream staging buffers were ever resident; the encoded
    // streams live entirely in the per-stream tmp files now.

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

    // Reads one stream's per-stream tmp file (`<spill_path>.<ext>`) into `dst`.
    auto read_stream = [&](const char* ext, const SpillSegment::Range& range,
                           std::vector<uint8_t>& dst) -> Status {
        dst.resize(static_cast<size_t>(range.length));
        if (range.length == 0) {
            return Status::OK();
        }
        const std::string path = seg.StreamPath(ext);
        io::FileReaderSPtr reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(path, &reader));
        size_t bytes_read = 0;
        RETURN_IF_ERROR(reader->read_at(0, Slice(dst.data(), dst.size()), &bytes_read));
        if (bytes_read != static_cast<size_t>(range.length)) {
            return Status::InternalError(
                    "SpillManager::LoadSpill short read on {} (got {}, want {})", path, bytes_read,
                    range.length);
        }
        static_cast<void>(reader->close());
        return Status::OK();
    };

    // Only .tis/.tii/.frq/.prx are consumed by the merge; .fnm/segments_* are
    // rebuilt from scratch by the merger and need not be read back.
    RETURN_IF_ERROR(read_stream("tis", seg.tis, out.tis_bytes));
    RETURN_IF_ERROR(read_stream("tii", seg.tii, out.tii_bytes));
    RETURN_IF_ERROR(read_stream("frq", seg.frq, out.frq_bytes));
    RETURN_IF_ERROR(read_stream("prx", seg.prx, out.prx_bytes));
    out.doc_count = seg.doc_count;

    return Status::OK();
}

void SpillManager::CleanupSpillFiles() {
    // Best-effort delete every per-stream tmp file of each spill. delete_file
    // returns OK when the file is already gone, so this is idempotent and
    // leak-safe on the error path as well (half-written spills are cleaned up).
    static constexpr const char* kStreamExts[] = {"tis", "tii",        "frq",          "prx",
                                                  "fnm", "segments_n", "segments_gen", "nrm"};
    for (const auto& seg : _spills) {
        if (seg.spill_path.empty()) {
            continue;
        }
        for (const char* ext : kStreamExts) {
            const std::string path = seg.StreamPath(ext);
            if (Status st = io::global_local_filesystem()->delete_file(path); !st.ok()) {
                LOG(WARNING) << "SpimiIndexWriter: failed to delete spill tmp file " << path << ": "
                             << st;
            }
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
