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
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::writer {

// Raw-u32 RUN compatibility codec for explicit fixtures and diagnostics. The
// production spill/merge path uses EncodedRunWriter and StreamingRunReader
// (encoded_spill_run.h); it never materializes an entire term in these classes.
//
// The former private format remains readable by the bounded merge path:
//   run := record* (term ids ordered by vocabulary string rank)
//   record := VInt term_id, VInt shape, VInt n_docs, u32 docid[n_docs]
//   shape 0: docs only; no frequencies or positions
//   shape 1: u32 frequency[n_docs]
//   shape 2: u32 frequency[n_docs], VInt n_positions, u32 position[n_positions]
// Fixed-width integers are little-endian. Positions are partitioned by positive
// frequencies, whose sum equals n_positions. RunReader intentionally exposes
// whole doc/frequency arrays for callers that request this materialized API.

// Writes a sorted sequence of terms (by id) to one run file. Term-ids must be
// handed to write_term in vocab-string ascending order (the spill caller sorts
// before spilling). RAII: the file is flushed and closed on close(); the partial
// file is left for the owning SpimiTermBuffer to delete on its temp-path list.
class RunWriter {
public:
    explicit RunWriter(MemoryReporter* memory_reporter = nullptr,
                       size_t buffer_limit = 4 * 1024 * 1024);
    ~RunWriter();

    RunWriter(const RunWriter&) = delete;
    RunWriter& operator=(const RunWriter&) = delete;

    // Opens `path` for writing, truncating unless append is requested.
    Status open(const std::string& path, bool append = false);

    // Appends one term's postings under `term_id`. Empty freqs denotes the
    // docs-only-statless shape; otherwise freqs parallels docids. Positioned
    // postings additionally hold sum(freqs) positions in document order.
    Status write_term(uint32_t term_id, const TermPostings& tp);

    // Flushes the buffer and closes the file. Safe to call once; idempotent.
    Status close();

private:
    friend class EncodedRunWriter;
    Status flush();
    Status append_bytes(const uint8_t* data, size_t size);
    Status append_varint(uint64_t value);
    Status append_raw_u32(const uint32_t* values, size_t count);
    void release_buffer();

    MemoryReporter* memory_reporter_ = nullptr;
    MemoryReporter::Reservation buffer_reservation_;
    size_t buffer_limit_;
    uint64_t file_bytes_ = 0;
    int fd_ = -1;
    std::vector<uint8_t> buf_; // bounded staging buffer; flushed in fixed-size chunks
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
//     explicit streamed-position callers to avoid a whole positions array.
// advance() drains any positions left unread from the previous term before the
// next record, so a partly-streamed (or skipped) term still lands at the right
// record boundary. The yielded byte sequence is identical either way.
class RunReader {
public:
    explicit RunReader(MemoryReporter* memory_reporter = nullptr);
    ~RunReader();

    RunReader(const RunReader&) = delete;
    RunReader& operator=(const RunReader&) = delete;

    // Opens `path`, loading the first record (if any). has_positions declares
    // whether this run may contain positioned or mixed statless records.
    Status open(const std::string& path, bool has_positions);

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
    Status materialize_positions();
    // Streams the next `n` positions of the current term into dst[0..n) directly
    // from the decode window (64 KiB chunks topped up on demand). Caller must not
    // request more than positions_remaining(); each call advances the cursor.
    Status stream_positions(uint32_t* dst, size_t n);
    uint64_t positions_remaining() const { return pos_remaining_; }

    // Loads the next record into current(); sets exhausted() at end of file. Any
    // positions of the current term left unread are skipped first.
    Status advance();

private:
    size_t available() const;        // buffered bytes from pos_ to window end
    Status fill();                   // tops up the decode window from disk
    Status ensure(size_t n);         // guarantees >= n buffered bytes (or eof)
    Status read_varint(uint64_t* v); // bounds-checked streamed varint
    // Bulk-reads `count` RAW little-endian u32s from the window into `out` (resized
    // to count). Bounds-checked against the run's true length (Corruption on EOF).
    Status read_raw_u32(size_t count, std::vector<uint32_t>* out,
                        MemoryReporter::Reservation* reservation);
    // Streams `count` raw u32s from the window into dst (caller-owned, sized by the
    // caller); shared by read_raw_u32 (into a vector) and stream_positions.
    Status pull_raw_u32(uint8_t* dst, size_t count);
    // Drains (and discards) any remaining positions of the current term so the
    // window cursor lands at the next record boundary.
    Status skip_remaining_positions();

    MemoryReporter* memory_reporter_ = nullptr;
    // Reservations precede their vectors so allocations are destroyed first.
    MemoryReporter::Reservation window_reservation_;
    MemoryReporter::Reservation docids_reservation_;
    MemoryReporter::Reservation freqs_reservation_;
    MemoryReporter::Reservation positions_reservation_;
    int fd_ = -1;
    bool has_positions_ = false;
    bool exhausted_ = false;
    uint64_t file_size_ = 0;      // total run byte size (fstat at open); bounds lengths
    uint64_t bytes_read_ = 0;     // bytes pulled from fd; never exceeds file_size_
    std::vector<uint8_t> window_; // sliding decode window
    size_t pos_ = 0;              // consumed offset within window_
    bool eof_ = false;            // no more bytes on disk
    uint32_t current_id_ = 0;     // current record's term-id
    uint64_t pos_count_ = 0;      // current term's total position count (from n_pos)
    uint64_t pos_remaining_ = 0;  // positions still unread in the current block
    TermPostings current_;
};

// K-way merges the given run files into a single term stream ordered by a
// PRECOMPUTED integer string-rank (string_rank[term_id] == the term-id's
// lexicographic rank over the dense vocabulary), invoking `fn` once per distinct
// term-id with its postings concatenated across all runs that contain it (in run
// order -> docids stay ascending) and its `term` resolved from `vocab` once.
// Because a dense vocab maps each id to a distinct string, the rank is a
// lexicographic bijection: ordering by the dense 4 B rank array (an integer
// compare) reproduces the EXACT order a vocab-string compare would -- but never
// reads a vocab string in the inner heap/gather loops. Only one bounded posting
// window is materialized at a time. Returns IoError/Corruption on bad run data, or
// InternalError when string_rank.size() != vocab.size(). has_positions must match
// how the runs were written. `vocab` (term-id -> string) and `string_rank`
// (term-id -> rank) are both borrowed and MUST be sized to the vocabulary.
//
// The source callback is synchronous: matching run readers remain parked on the
// current term until the callback returns. The source fills writer-owned windows
// directly and coalesces equal docids at run boundaries. A successful callback
// must exhaust the source. Legacy raw fixtures require allow_legacy=true; production
// requires the encoded header and seal, including for an empty run.
Status merge_run_sources(const std::vector<std::string>& run_paths,
                         const std::vector<std::string>& vocab,
                         const std::vector<uint32_t>& string_rank, bool has_positions,
                         const StreamedTermConsumer& fn, MemoryReporter* memory_reporter = nullptr,
                         bool allow_legacy = false);

// Production input consists of independently sealed ranges in one append-only
// spool. `ends` stores {u64 end_offset, u32 CRC} records and shares the posting
// workspace; no per-run path or offset array is materialized. A nonzero fan-in
// limit adds the historical run-file knob to the budget/fd constraints.
Status merge_spooled_run_sources(const std::string& spool, PostingByteBuffer* ends, size_t count,
                                 const std::vector<std::string>& vocab,
                                 const std::vector<uint32_t>& string_rank, bool has_positions,
                                 const StreamedTermConsumer& fn, MemoryReporter* reporter,
                                 size_t fan_in_limit);

// Diagnostic/compaction interface: merges `run_paths` into one new run file at
// `out_path`, keyed and ordered exactly like merge_run_sources (heap on
// string_rank[term_id]; per-term postings concatenated across runs in run
// order, boundary docs coalesced -- the same concat the final merge applies,
// so compact-then-merge emits the identical term stream as merging the
// originals). Compact fragments are copied with bounded buffers. Multi-pass
// groups preserve chronological run order; the final consumer coalesces boundary
// documents while decoding. The output uses the current private encoded format.
// Every record's term-id must index string_rank (else Corruption). On error
// `out_path` may hold a partial file the caller must delete; the input runs
// are never modified. Active input fds are bounded by the shared workspace and
// process fd limit; excess inputs are reduced through contiguous merge passes.
Status compact_runs(const std::vector<std::string>& run_paths,
                    const std::vector<uint32_t>& string_rank, bool has_positions,
                    const std::string& out_path, MemoryReporter* memory_reporter = nullptr,
                    bool allow_legacy = false);

} // namespace doris::snii::writer
