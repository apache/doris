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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/writer/spimi_term_buffer.h"

namespace snii::writer {

// On-disk SPIMI "run" codec for the spill / k-way-merge out-of-core build path.
//
// A RUN is a self-describing file holding a sequence of terms keyed by TERM-ID,
// each followed by its postings, in this exact wire layout. The file is produced
// and consumed by THIS module only (a private temp file -- the on-disk INDEX is
// unaffected), so the format is chosen for cheap I/O: docids, freqs and positions
// are ALL RAW fixed-width little-endian u32 BLOCKS (bulk memcpy on both ends,
// ~10x cheaper than per-value varint -- which cost ~1.5s of encode CPU over the
// 5M build's ~60M docids and compressed those streams poorly anyway). Decode
// still validates every length against the file size.
//
//   run := record*                       (term-ids ordered by vocab string,
//                                          strictly ascending within a run)
//   record :=
//     VInt term_id                       (index into the shared vocabulary; the
//                                          string is NOT stored -- smaller runs,
//                                          no per-record string IO)
//     VInt n_docs
//     u32  docid   * n_docs              (RAW LE block, memcpy; ABSOLUTE ascending
//                                          docids -- the merge concatenates across
//                                          runs and re-deltas at index encode time)
//     u32  freq    * n_docs              (RAW LE block, memcpy; each >= 1)
//     VInt n_pos                         (== sum(freqs) when has_positions, else 0)
//     u32  position * n_pos              (RAW LE block, document-order, partitioned
//                                          by freqs)
//
// Decode is fully STREAMED: a RunReader reads a small fixed buffer at a time and
// materializes only the CURRENT term's postings, never the whole run. The k-way
// merge keeps one heap slot per run (each holding only its current term-id +
// that term's postings), so peak memory is bounded by the widest single term
// summed across the runs that contain it -- not by total postings. The merge
// orders runs by the term-id's VOCAB STRING (resolved via the shared vocabulary)
// so the merged stream is lexicographic.

// Writes a sorted sequence of terms (by id) to one run file. Term-ids must be
// handed to write_term in vocab-string ascending order (the spill caller sorts
// before spilling). RAII: the file is flushed and closed on close(); the partial
// file is left for the owning SpimiTermBuffer to delete on its temp-path list.
class RunWriter {
public:
    RunWriter() = default;
    ~RunWriter();

    RunWriter(const RunWriter&) = delete;
    RunWriter& operator=(const RunWriter&) = delete;

    // Opens `path` for writing (truncating). Returns IoError on failure.
    doris::Status open(const std::string& path);

    // Appends one term's postings under `term_id`. `tp.positions_flat` must be empty
    // iff !has_positions (and otherwise hold sum(freqs) entries in doc order).
    // Caller guarantees ascending docids and parallel docids/freqs lengths.
    doris::Status write_term(uint32_t term_id, const TermPostings& tp);

    // Flushes the buffer and closes the file. Safe to call once; idempotent.
    doris::Status close();

private:
    doris::Status flush();

    int fd_ = -1;
    std::vector<uint8_t> buf_; // staging buffer; flushed in fixed-size chunks
};

// Streamed reader over one run file. After open() the first term is loaded;
// current()/current_id() expose it; advance() loads the next (or marks
// exhausted). Only the current term's postings live in memory at a time. The
// current record's `term` string is left EMPTY -- runs store only the id; the
// owner resolves the string via the shared vocabulary.
//
// LAZY POSITIONS (peak-RSS optimization for the widest merged term): advance()
// loads term_id / docids / freqs and the position-block COUNT, but does NOT read
// the position bytes -- it leaves the decode window cursor parked at the start of
// the position block. The owner then chooses, per term:
//   * materialize_positions(): bulk-reads the block into current().positions_flat
//     (the default; behaves exactly as the old eager reader).
//   * stream_positions(dst, n): pulls the next n positions straight from the
//     window in 64 KiB chunks, never materializing the whole block -- used by the
//     k-way merge's wide-term position pump so the widest term's tens-of-MiB
//     positions buffer is never resident.
// advance() drains any positions left unread from the previous term before the
// next record, so a partly-streamed (or skipped) term still lands at the right
// record boundary. The yielded byte sequence is identical either way.
class RunReader {
public:
    RunReader() = default;
    ~RunReader();

    RunReader(const RunReader&) = delete;
    RunReader& operator=(const RunReader&) = delete;

    // Opens `path`, loading the first record (if any). has_positions must match
    // the writer's setting so n_pos is interpreted consistently.
    doris::Status open(const std::string& path, bool has_positions);

    bool exhausted() const { return exhausted_; }
    const TermPostings& current() const { return current_; }
    uint32_t current_id() const { return current_id_; }

    // Number of positions in the current term's (lazily-loaded) position block.
    uint64_t current_pos_count() const { return pos_count_; }
    // True once the current term's positions have been materialized OR fully
    // streamed (i.e. nothing remains to read before advance()).
    bool positions_drained() const { return pos_remaining_ == 0; }

    // Materializes the current term's position block into current().positions_flat
    // (bulk read). Idempotent within a term: a no-op once positions are drained.
    doris::Status materialize_positions();
    // Streams the next `n` positions of the current term into dst[0..n) directly
    // from the decode window (64 KiB chunks topped up on demand). Caller must not
    // request more than positions_remaining(); each call advances the cursor.
    doris::Status stream_positions(uint32_t* dst, size_t n);
    uint64_t positions_remaining() const { return pos_remaining_; }

    // Loads the next record into current(); sets exhausted() at end of file. Any
    // positions of the current term left unread are skipped first.
    doris::Status advance();

private:
    size_t available() const;        // buffered bytes from pos_ to window end
    doris::Status fill();                   // tops up the decode window from disk
    doris::Status ensure(size_t n);         // guarantees >= n buffered bytes (or eof)
    doris::Status read_varint(uint64_t* v); // bounds-checked streamed varint
    // Bulk-reads `count` RAW little-endian u32s from the window into `out` (resized
    // to count). Bounds-checked against the run's true length (Corruption on EOF).
    doris::Status read_raw_u32(size_t count, std::vector<uint32_t>* out);
    // Streams `count` raw u32s from the window into dst (caller-owned, sized by the
    // caller); shared by read_raw_u32 (into a vector) and stream_positions.
    doris::Status pull_raw_u32(uint8_t* dst, size_t count);
    // Drains (and discards) any remaining positions of the current term so the
    // window cursor lands at the next record boundary.
    doris::Status skip_remaining_positions();

    int fd_ = -1;
    bool has_positions_ = false;
    bool exhausted_ = false;
    uint64_t file_size_ = 0;      // total run byte size (fstat at open); bounds lengths
    std::vector<uint8_t> window_; // sliding decode window
    size_t pos_ = 0;              // consumed offset within window_
    bool eof_ = false;            // no more bytes on disk
    uint32_t current_id_ = 0;     // current record's term-id
    uint64_t pos_count_ = 0;      // current term's total position count (from n_pos)
    uint64_t pos_remaining_ = 0;  // positions still unread in the current block
    TermPostings current_;
};

// K-way merges the given run files into a single term stream ordered by the
// term-id's VOCAB STRING (lexicographic), invoking `fn` once per distinct
// term-id with its postings concatenated across all runs that contain it (in
// run order -> docids stay ascending) and its `term` resolved from `vocab`.
// Only one merged term is materialized at a time. Returns IoError/Corruption on
// bad run data. has_positions must match how the runs were written. `vocab`
// maps term-id -> string and is borrowed.
//
// allow_stream_positions (peak-RSS optimization): when true (the streaming-writer
// path), a WIDE merged term's positions are NOT materialized into positions_flat;
// instead the TermPostings carries a pos_pump that streams positions in document
// order straight from the run readers (which stay parked at this term's blocks
// for the duration of the fn() call). `fn` MUST therefore consume each term
// SYNCHRONOUSLY and must NOT retain the TermPostings past the call (the pump
// references live readers freed when the merge advances). Callers that retain the
// term (e.g. finalize_sorted) MUST pass false, so positions are always fully
// materialized. The produced bytes are identical either way.
doris::Status MergeRuns(const std::vector<std::string>& run_paths, const std::vector<std::string>& vocab,
                 bool has_positions, const std::function<void(TermPostings&&)>& fn,
                 bool allow_stream_positions = true);

} // namespace snii::writer
