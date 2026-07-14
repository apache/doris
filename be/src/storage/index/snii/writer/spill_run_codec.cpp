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

#include "storage/index/snii/writer/spill_run_codec.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>
#include <queue>
#include <stdexcept>
#include <utility>

#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::writer {

namespace {

// Flush staging once it grows past this. A LARGE write buffer (4 MiB) collapses
// the per-flush write() syscall count by ~64x: at 64 KiB the 5M build issued
// ~8800 write()s to ext4 (~9s of syscall overhead) for ~553 MiB of runs, versus
// a raw dd of the same bytes taking ~1.2s. Runs are PRIVATE temp files, so the
// on-disk index is unaffected; the only cost is a slightly larger transient
// RunWriter staging buffer (4 MiB, bounded, freed at close()).
constexpr size_t kWriteFlushBytes = 1u << 22; // 4 MiB
// RunReader reads this much per disk fill; the window slides so a single record
// never needs the whole run in RAM (only the current term's encoded span). KEEP
// this small (64 KiB): a large read chunk x many open runs would inflate the
// merge-phase peak RSS at low spill thresholds (each reader holds a window).
constexpr size_t kReadChunkBytes = 1u << 16; // 64 KiB

void AppendVarint(std::vector<uint8_t>* buf, uint64_t v) {
    uint8_t tmp[10];
    const size_t n = encode_varint64(v, tmp);
    buf->insert(buf->end(), tmp, tmp + n);
}

// Appends a block of `count` uint32 values as RAW little-endian fixed-width bytes
// (memcpy from contiguous source). Runs are private temp files; the on-disk index
// is unaffected. Raw blocks make encode/decode ~10x cheaper than per-value varint
// for the freqs/positions streams (which compress poorly as varints anyway), at
// the cost of a modestly larger temp run. Empty source is a no-op.
void AppendRawU32(std::vector<uint8_t>* buf, const uint32_t* src, size_t count) {
    if (count == 0) return;
    const auto* bytes = reinterpret_cast<const uint8_t*>(src);
    buf->insert(buf->end(), bytes, bytes + count * sizeof(uint32_t));
}

// Writes the full byte range [data, data+len) to fd, looping over short writes.
Status WriteAll(int fd, const uint8_t* data, size_t len) {
    size_t off = 0;
    while (off < len) {
        const ssize_t n = ::write(fd, data + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return Status::Error<ErrorCode::IO_ERROR, false>(std::string("run write failed: ") +
                                                             std::strerror(errno));
        }
        off += static_cast<size_t>(n);
    }
    return Status::OK();
}

} // namespace

// ---------------------------------------------------------------------------
// RunWriter
// ---------------------------------------------------------------------------

RunWriter::~RunWriter() {
    if (fd_ >= 0) ::close(fd_);
}

Status RunWriter::open(const std::string& path) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd_ < 0) {
        return Status::Error<ErrorCode::IO_ERROR, false>("run open(" + path +
                                                         "): " + std::strerror(errno));
    }
    buf_.clear();
    return Status::OK();
}

Status RunWriter::flush() {
    if (buf_.empty()) return Status::OK();
    RETURN_IF_ERROR(WriteAll(fd_, buf_.data(), buf_.size()));
    buf_.clear();
    return Status::OK();
}

Status RunWriter::write_term(uint32_t term_id, const TermPostings& tp) {
    AppendVarint(&buf_, term_id);
    AppendVarint(&buf_, tp.docids.size());
    // Docids are a RAW fixed-width u32 block (bulk memcpy), NOT per-value VInt.
    // Per-value varint over ~60M docids cost ~1.5s of encode CPU on the spill feed
    // side; raw is a single memcpy and the decode side becomes a memcpy too. Runs
    // are PRIVATE temp files written then read back from page cache, so the modestly
    // larger run (no delta packing) costs ~0 extra real I/O. Absolute docids are
    // stored (the merge concatenates per-term across runs and re-deltas at encode).
    AppendRawU32(&buf_, tp.docids.data(), tp.docids.size());
    // Freqs + positions are RAW fixed-width u32 blocks (bulk memcpy). The decoder
    // reads them back the same way; n_pos == positions_flat.size() is recoverable
    // from sum(freqs), but is written explicitly so a reader can size the block.
    AppendRawU32(&buf_, tp.freqs.data(), tp.freqs.size());
    const uint64_t n_pos = tp.positions_flat.size();
    AppendVarint(&buf_, n_pos);
    AppendRawU32(&buf_, tp.positions_flat.data(), tp.positions_flat.size());
    if (buf_.size() >= kWriteFlushBytes) RETURN_IF_ERROR(flush());
    return Status::OK();
}

Status RunWriter::close() {
    if (fd_ < 0) return Status::OK();
    RETURN_IF_ERROR(flush());
    const int fd = fd_;
    fd_ = -1;
    if (::close(fd) != 0) {
        return Status::Error<ErrorCode::IO_ERROR, false>(std::string("run close: ") +
                                                         std::strerror(errno));
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// RunReader
// ---------------------------------------------------------------------------

RunReader::~RunReader() {
    if (fd_ >= 0) ::close(fd_);
}

Status RunReader::open(const std::string& path, bool has_positions) {
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0) {
        return Status::Error<ErrorCode::IO_ERROR, false>("run reopen(" + path +
                                                         "): " + std::strerror(errno));
    }
    // Record the run's byte size so every length decoded from the stream can be
    // bounded against it before allocating (no record holds more u32s than the whole
    // file). Honors the header's "lengths validated against the file size" contract,
    // turning a corrupt/truncated length into Status::Corruption rather than an
    // uncaught std::bad_alloc from a giant resize().
    struct stat st {};
    if (::fstat(fd_, &st) != 0) {
        return Status::Error<ErrorCode::IO_ERROR, false>(std::string("run fstat: ") +
                                                         std::strerror(errno));
    }
    file_size_ = static_cast<uint64_t>(st.st_size);
    has_positions_ = has_positions;
    exhausted_ = false;
    eof_ = false;
    pos_ = 0;
    pos_count_ = 0;
    pos_remaining_ = 0;
    window_.clear();
    return advance();
}

// Slides consumed bytes out of the window, then appends one disk chunk.
Status RunReader::fill() {
    if (pos_ > 0) {
        window_.erase(window_.begin(), window_.begin() + pos_);
        pos_ = 0;
    }
    if (eof_) return Status::OK();
    const size_t base = window_.size();
    window_.resize(base + kReadChunkBytes);
    ssize_t n;
    do {
        n = ::read(fd_, window_.data() + base, kReadChunkBytes);
    } while (n < 0 && errno == EINTR);
    if (n < 0)
        return Status::Error<ErrorCode::IO_ERROR, false>(std::string("run read: ") +
                                                         std::strerror(errno));
    window_.resize(base + static_cast<size_t>(n));
    if (n == 0) eof_ = true;
    return Status::OK();
}

// Buffered bytes available to the decoder right now (from pos_ to window end).
// fill() may slide the window (erasing consumed bytes), so callers must compare
// THIS quantity -- not window_.size() -- to decide whether more data arrived.
size_t RunReader::available() const {
    return window_.size() - pos_;
}

Status RunReader::ensure(size_t n) {
    while (available() < n) {
        const size_t had = available();
        RETURN_IF_ERROR(fill());
        if (available() == had && eof_) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "run truncated: needed more bytes than available");
        }
    }
    return Status::OK();
}

// Streamed varint: decode from the current window; if it straddles the buffered
// boundary, top up from disk and retry. A varint is at most 10 bytes, so this
// loops at most a couple of times. Bounds-safe: decode_varint64 never reads past
// `end`, and a partial varint at true eof is reported as corruption.
Status RunReader::read_varint(uint64_t* v) {
    while (true) {
        const uint8_t* p = window_.data() + pos_;
        const uint8_t* end = window_.data() + window_.size();
        const uint8_t* next = nullptr;
        Status s = decode_varint64(p, end, v, &next);
        if (s.ok()) {
            pos_ += static_cast<size_t>(next - p);
            return Status::OK();
        }
        if (eof_)
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "run truncated: incomplete varint");
        const size_t had = available();
        RETURN_IF_ERROR(fill());
        if (available() == had && eof_) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "run truncated: incomplete varint at eof");
        }
    }
}

// Streams `count` raw little-endian u32s from the window into `dst` (caller-owned
// storage of at least count*4 bytes), topping up the window from disk as needed.
// Copies whatever is buffered each pass (the window may hold only part of a large
// block), so a high-df term's freqs/positions stream through in 64 KiB chunks
// without ever needing the whole block resident at once.
Status RunReader::pull_raw_u32(uint8_t* dst, size_t count) {
    if (count == 0) return Status::OK();
    size_t need = count * sizeof(uint32_t);
    size_t written = 0;
    while (need > 0) {
        if (available() == 0) {
            const size_t had = available();
            RETURN_IF_ERROR(fill());
            if (available() == had && eof_) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "run truncated: needed more raw bytes than available");
            }
        }
        const size_t take = std::min(need, available());
        std::memcpy(dst + written, window_.data() + pos_, take);
        pos_ += take;
        written += take;
        need -= take;
    }
    return Status::OK();
}

// Bulk-decodes `count` raw u32s into `out` (resized to count).
Status RunReader::read_raw_u32(size_t count, std::vector<uint32_t>* out) {
    // Bound `count` against the run's byte size BEFORE resize(): a record can never
    // hold more u32s than the whole file. Rejects a corrupt/truncated length varint
    // (which is otherwise an unbounded resize -> uncaught std::bad_alloc).
    if (count > file_size_ / sizeof(uint32_t)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: raw u32 count exceeds file size");
    }
    out->resize(count);
    if (count == 0) return Status::OK();
    return pull_raw_u32(reinterpret_cast<uint8_t*>(out->data()), count);
}

// Materializes the current term's deferred position block into positions_flat.
// A no-op once the positions are already drained (idempotent within a term).
Status RunReader::materialize_positions() {
    if (pos_remaining_ == 0) {
        current_.positions_flat.clear();
        return Status::OK();
    }
    const size_t n = static_cast<size_t>(pos_remaining_);
    if (has_positions_) {
        RETURN_IF_ERROR(read_raw_u32(n, &current_.positions_flat));
    } else {
        // No-positions runs should carry n_pos == 0; tolerate (skip) a stray block.
        std::vector<uint32_t> skip;
        RETURN_IF_ERROR(read_raw_u32(n, &skip));
        current_.positions_flat.clear();
    }
    pos_remaining_ = 0;
    return Status::OK();
}

// Streams the next `n` positions of the current term straight from the window.
Status RunReader::stream_positions(uint32_t* dst, size_t n) {
    if (n == 0) return Status::OK();
    if (n > pos_remaining_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: stream_positions past block end");
    }
    RETURN_IF_ERROR(pull_raw_u32(reinterpret_cast<uint8_t*>(dst), n));
    pos_remaining_ -= n;
    return Status::OK();
}

// Discards any positions of the current term left unread, so the window cursor
// lands at the next record boundary before advance() reads the next term.
Status RunReader::skip_remaining_positions() {
    if (pos_remaining_ == 0) return Status::OK();
    const size_t n = static_cast<size_t>(pos_remaining_);
    std::vector<uint32_t> skip;
    RETURN_IF_ERROR(read_raw_u32(n, &skip));
    pos_remaining_ = 0;
    return Status::OK();
}

Status RunReader::advance() {
    // Drain any positions the owner left unread for the previous term so the window
    // cursor lands at the next record boundary.
    RETURN_IF_ERROR(skip_remaining_positions());
    // End-of-run detection: at a record boundary, if no bytes remain we are done.
    if (available() == 0) {
        RETURN_IF_ERROR(fill());
        if (available() == 0 && eof_) {
            exhausted_ = true;
            return Status::OK();
        }
    }
    uint64_t term_id = 0;
    RETURN_IF_ERROR(read_varint(&term_id));
    if (term_id > UINT32_MAX)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run term_id exceeds uint32");
    current_id_ = static_cast<uint32_t>(term_id);
    current_.term.clear(); // runs store only the id; owner resolves the string

    uint64_t n_docs = 0;
    RETURN_IF_ERROR(read_varint(&n_docs));
    // Docids: RAW absolute u32 block (bulk read), matching the writer's AppendRawU32.
    RETURN_IF_ERROR(read_raw_u32(static_cast<size_t>(n_docs), &current_.docids));
    // Freqs: RAW u32 block (bulk read), matching the writer's AppendRawU32.
    RETURN_IF_ERROR(read_raw_u32(static_cast<size_t>(n_docs), &current_.freqs));
    uint64_t n_pos = 0;
    RETURN_IF_ERROR(read_varint(&n_pos));
    // Positions are LAZY: record the block count and leave the window cursor parked
    // at the block start. The owner picks materialize_positions() (default) or
    // stream_positions() (wide-term merge pump). The widest term's tens-of-MiB
    // position block is thus never resident unless the owner asks for it whole.
    current_.positions_flat.clear();
    pos_count_ = n_pos;
    pos_remaining_ = n_pos;
    return Status::OK();
}

// ---------------------------------------------------------------------------
// K-way merge
// ---------------------------------------------------------------------------

namespace {

// Min-heap entry: orders by the run's current term-id's PRECOMPUTED integer
// string-rank (rank[term_id] == its lexicographic rank over the dense vocabulary),
// tie-broken by run index so equal terms are gathered run-order (keeping
// concatenated docids ascending). The rank is a lexicographic bijection on a dense
// vocab, so ordering by the dense 4 B rank array reproduces the exact dictionary
// order a vocab-string compare would -- with an integer compare and zero random
// vocab string access in the inner loop.
struct HeapItem {
    uint32_t term_id;
    size_t run;
};
struct HeapGreater {
    const std::vector<uint32_t>* rank;
    bool operator()(const HeapItem& a, const HeapItem& b) const {
        const uint32_t ra = (*rank)[a.term_id];
        const uint32_t rb = (*rank)[b.term_id];
        if (ra != rb) {
            return ra > rb;
        }                     // smaller rank first (lexicographic min-heap)
        return a.run > b.run; // same term across runs: run-order tie-break
    }
};

// Appends src's postings onto dst (run order). Later runs only cover docids
// >= dst's last, so docids stay ascending. COALESCE the boundary doc: if a spill
// fell BETWEEN two tokens of the same doc, that doc ends one run and begins the
// next with the SAME docid -- merge them (sum freqs, splice positions) so the
// merged term has exactly one entry per docid (matching the in-memory build).
//
// Positions are FLAT: doc order, partitioned by freqs. Because both dst and src
// already store doc-ordered flat positions, the common (no-boundary-overlap) case
// is a single bulk append. The boundary-overlap case must INSERT src's first
// doc's positions right after dst's last doc's positions so flat order stays
// consistent with the merged (coalesced) freqs.
void Concat(TermPostings* dst, const TermPostings& src, bool has_positions) {
    if (src.docids.empty()) return;
    // PER-TERM positions presence: a G04 position-suppressed bigram term is
    // written with an EMPTY position block (n_pos == 0) even in a has_positions
    // run. Its freqs are still real (> 0), so indexing positions_flat by freqs
    // would read out of range -- gate every positions splice on the term
    // actually carrying position bytes. Suppression is a property of the TERM
    // (its marker prefix), so it is consistent across every run that holds it.
    const bool src_has_pos = has_positions && !src.positions_flat.empty();
    size_t start = 0;
    size_t src_pos_start = 0; // flat offset of src positions to append after splice
    if (!dst->docids.empty() && dst->docids.back() == src.docids.front()) {
        const uint32_t head_fc = src.freqs.front();
        if (src_has_pos && head_fc != 0) {
            // Splice src's first-doc positions in right after dst's last-doc positions.
            // dst's last doc owns dst->freqs.back() entries at the tail of positions_flat
            // BEFORE we bump that freq, so insert at end() (last doc is the tail run).
            auto& flat = dst->positions_flat;
            flat.insert(flat.end(), src.positions_flat.begin(),
                        src.positions_flat.begin() + head_fc);
        }
        dst->freqs.back() += head_fc;
        src_pos_start = head_fc;
        start = 1; // boundary doc folded in; append the rest
    }
    dst->docids.insert(dst->docids.end(), src.docids.begin() + start, src.docids.end());
    dst->freqs.insert(dst->freqs.end(), src.freqs.begin() + start, src.freqs.end());
    if (src_has_pos) {
        dst->positions_flat.insert(dst->positions_flat.end(),
                                   src.positions_flat.begin() + src_pos_start,
                                   src.positions_flat.end());
    }
}

// Coalesces ONLY docids/freqs (no positions). Used by the WIDE-term path, whose
// positions are streamed via a pos_pump instead of materialized. The boundary-doc
// freq merge (dst->freqs.back() += head_fc) is identical to Concat's, so the
// merged df / freqs / ttf are bit-for-bit the same; positions are emitted in pure
// run-order concatenation by the pump (the same byte stream Concat would build).
void ConcatDocsFreqs(TermPostings* dst, const TermPostings& src) {
    if (src.docids.empty()) return;
    size_t start = 0;
    if (!dst->docids.empty() && dst->docids.back() == src.docids.front()) {
        dst->freqs.back() += src.freqs.front();
        start = 1; // boundary doc folded in; append the rest
    }
    dst->docids.insert(dst->docids.end(), src.docids.begin() + start, src.docids.end());
    dst->freqs.insert(dst->freqs.end(), src.freqs.begin() + start, src.freqs.end());
}

// A merged term is emitted with a STREAMED position pump (instead of a
// materialized positions_flat) when it is wide enough that its full flat
// positions would dominate the merge-phase peak RSS. The writer routes any term
// with df >= kSlimDfThreshold through the windowed path (build_windowed_entry),
// which is the only path that consumes pos_pump; a slim term reads positions_flat
// directly, so it must always be materialized. Gating on the same df threshold
// the writer uses keeps the two in lockstep and is conservative: only the few
// genuinely-wide terms (led by the single widest, the merge-phase peak driver)
// take the streamed path. total_pos is also required so a degenerate wide term
// with no positions still has something to stream.
bool ShouldStreamPositions(uint64_t total_docs, uint64_t total_pos, bool has_positions) {
    return has_positions && total_pos != 0 && total_docs >= format::kSlimDfThreshold;
}

} // namespace

Status MergeRuns(const std::vector<std::string>& run_paths, const std::vector<std::string>& vocab,
                 const std::vector<uint32_t>& string_rank, bool has_positions,
                 const std::function<void(TermPostings&&)>& fn, bool allow_stream_positions) {
    // The heap/gather key is string_rank[term_id]; require it sized to the vocab so
    // every in-vocab-range term-id (enforced by the current_id() < vocab.size() guards
    // below) also indexes string_rank in bounds. A mismatch is a caller wiring bug.
    if (string_rank.size() != vocab.size()) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "MergeRuns: string_rank/vocab size mismatch");
    }
    std::vector<std::unique_ptr<RunReader>> readers;
    readers.reserve(run_paths.size());
    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapGreater> heap(
            HeapGreater {&string_rank});
    for (size_t i = 0; i < run_paths.size(); ++i) {
        auto r = std::make_unique<RunReader>();
        RETURN_IF_ERROR(r->open(run_paths[i], has_positions));
        if (!r->exhausted()) {
            if (r->current_id() >= vocab.size()) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "run term_id out of vocab range");
            }
            heap.push({r->current_id(), i});
        }
        readers.push_back(std::move(r));
    }

    std::vector<size_t> matching; // run indices contributing the current term
    while (!heap.empty()) {
        const uint32_t id = heap.top().term_id;
        TermPostings merged;
        merged.term = vocab[id]; // resolve the id -> dictionary string once per term
        // Gather every run whose head is THIS term-id (integer equality -- the heap's
        // run tie-break keeps them in run order, so concatenated docids stay ascending).
        // A dense vocab maps each id to a distinct string, so the same term across runs
        // shares one id; comparing ids avoids any vocab string access in this loop. The
        // matching runs' current slices are already loaded in their readers (they were
        // read to seed the heap), so summing their sizes here costs nothing extra in RAM.
        matching.clear();
        uint64_t total_docs = 0, total_pos = 0;
        while (!heap.empty() && heap.top().term_id == id) {
            const size_t ri = heap.top().run;
            heap.pop();
            const RunReader* r = readers[ri].get();
            total_docs += r->current().docids.size();
            total_pos += r->current_pos_count(); // positions are LAZY: use the count
            matching.push_back(ri);
        }
        // Reserve EXACTLY the summed sizes (an upper bound -- boundary-doc coalescing
        // only shrinks the final size). This eliminates std::vector's geometric
        // over-allocation, which left ~32 MiB of dead capacity on the widest term (df
        // in the millions split across spills) -- a dominant merge-phase peak-RSS
        // overhang at 5M. The reserved-but-unwritten pages are not faulted in, so the
        // empty reservation itself does not raise RSS; only the actual data does.
        merged.docids.reserve(static_cast<size_t>(total_docs));
        merged.freqs.reserve(static_cast<size_t>(total_docs));

        bool stream = allow_stream_positions &&
                      ShouldStreamPositions(total_docs, total_pos, has_positions);
        if (!stream && has_positions) {
            merged.positions_flat.reserve(static_cast<size_t>(total_pos));
        }
        // Coalesce docids/freqs from every matching run (always materialized -- a few
        // u32 vectors). For the non-wide case, also coalesce positions here. For the
        // wide case, leave positions for the streamed pump and keep the readers PARKED
        // at their position blocks until fn() drains the pump.
        for (size_t ri : matching) {
            RunReader* r = readers[ri].get();
            if (stream) {
                ConcatDocsFreqs(&merged, r->current());
            } else {
                if (has_positions) RETURN_IF_ERROR(r->materialize_positions());
                Concat(&merged, r->current(), has_positions);
            }
        }

        // The stream gate keyed on PRE-coalesce total_docs, but the writer's slim vs
        // windowed dispatch keys on the POST-coalesce df (merged.docids.size()).
        // Boundary-doc coalescing across spill seams can drop df below kSlimDfThreshold
        // while total_docs stayed above it; that term routes to build_slim_entry, which
        // reads positions_flat directly and ignores pos_pump. Materialize positions now
        // from the still-parked readers (mirrors drain_sorted()'s slim fallback).
        if (stream && merged.docids.size() < format::kSlimDfThreshold) {
            merged.positions_flat.reserve(static_cast<size_t>(total_pos));
            for (size_t ri : matching) {
                RunReader* r = readers[ri].get();
                RETURN_IF_ERROR(r->materialize_positions());
                const std::vector<uint32_t>& pf = r->current().positions_flat;
                merged.positions_flat.insert(merged.positions_flat.end(), pf.begin(), pf.end());
            }
            stream = false;
        }

        if (stream) {
            // WIDE term: STREAM positions via a pump that walks the matching readers in
            // run order (pure flat concatenation == the coalesced positions_flat,
            // byte-for-byte). positions_flat stays empty -- the widest term's tens-of-MiB
            // position buffer is never resident; only one ~64 KiB window per pull is. The
            // readers are still parked at this term's blocks, so the pump pulls from them
            // synchronously while fn() runs (fn consumes synchronously -- the windowed
            // writer does). After fn(), advance the readers past the (now-drained) blocks.
            merged.pos_total = total_pos;
            size_t cursor = 0; // index into `matching` for the run currently being drained
            Status pump_status = Status::OK();
            std::vector<std::unique_ptr<RunReader>>* rd = &readers;
            const std::vector<size_t>* match = &matching;
            // Self-contained liveness guard. The pump captures references into THIS stack
            // frame (&cursor, &pump_status) and the parked run readers (rd/match), valid
            // ONLY while fn() runs synchronously -- after fn() the readers advance past the
            // drained blocks. `pump_alive` is heap-owned and captured BY VALUE, so a
            // stored/deferred pos_pump fails loudly (throws) instead of dereferencing
            // dangling state. See the contract on TermPostings::pos_pump.
            auto pump_alive = std::make_shared<bool>(true);
            merged.pos_pump = [rd, match, &cursor, &pump_status, pump_alive](uint32_t* dst,
                                                                             size_t n) {
                if (!*pump_alive) {
                    throw std::logic_error(
                            "TermPostings::pos_pump invoked after its producing merge scope ended; "
                            "the streamed TermPostings must be consumed synchronously inside fn() "
                            "and never stored for later use");
                }
                size_t off = 0;
                while (off < n) {
                    // Advance to the next run that still has positions to yield.
                    while (cursor < match->size() &&
                           (*rd)[(*match)[cursor]]->positions_remaining() == 0) {
                        ++cursor;
                    }
                    if (cursor >= match->size()) break; // defensive: pump over-pulled
                    RunReader* r = (*rd)[(*match)[cursor]].get();
                    const size_t take =
                            std::min(n - off, static_cast<size_t>(r->positions_remaining()));
                    Status s = r->stream_positions(dst + off, take);
                    if (!s.ok()) {
                        // Mid-stream I/O / corruption: zero-fill the UNFILLED tail before
                        // returning. fn() has the pump and will consume dst BEFORE pump_status
                        // is surfaced after fn(); never hand it uninitialized bytes (the
                        // failed stream_positions wrote nothing into dst[off..]). The error is
                        // still latched and surfaced after fn(), so the build aborts -- the
                        // zero fill only guarantees deterministic, defined bytes meanwhile.
                        std::memset(dst + off, 0, (n - off) * sizeof(uint32_t));
                        if (pump_status.ok()) pump_status = std::move(s);
                        return;
                    }
                    off += take;
                }
                // Short-fill on over-pull (cursor ran past the matching runs without an
                // error status): the readers held fewer positions than n. Zero-fill the
                // unfilled tail so the writer never reads uninitialized storage. With
                // valid runs n == pos_total == sum(positions_remaining), so off == n and
                // this memset spans zero bytes -- the produced .idx is unchanged.
                if (off < n) std::memset(dst + off, 0, (n - off) * sizeof(uint32_t));
            };
            fn(std::move(merged));
            *pump_alive = false;          // any later pos_pump call now throws instead of UAF
            RETURN_IF_ERROR(pump_status); // surface a streamed-read I/O error
        } else {
            fn(std::move(merged));
        }

        // Advance every matching reader to its next term and re-seed the heap. For the
        // wide path this also skips any positions the pump did not pull (none, when fn
        // drained the whole stream); for the non-wide path positions were already
        // materialized so nothing remains.
        for (size_t ri : matching) {
            RunReader* r = readers[ri].get();
            RETURN_IF_ERROR(r->advance()); // frees this run's slice, loads next term
            if (!r->exhausted()) {
                if (r->current_id() >= vocab.size()) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                            "run term_id out of vocab range");
                }
                heap.push({r->current_id(), ri});
            }
        }
    }
    return Status::OK();
}

Status CompactRuns(const std::vector<std::string>& run_paths,
                   const std::vector<uint32_t>& string_rank, bool has_positions,
                   const std::string& out_path) {
    // Same heap machinery as MergeRuns, but the output is a RUN (records keyed
    // by term-id, ordered by string rank -- the exact invariant every run file
    // carries), not a resolved term stream: no vocab strings are needed, and
    // positions are always materialized because the run codec serializes
    // positions_flat directly.
    std::vector<std::unique_ptr<RunReader>> readers;
    readers.reserve(run_paths.size());
    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapGreater> heap(
            HeapGreater {&string_rank});
    for (size_t i = 0; i < run_paths.size(); ++i) {
        auto r = std::make_unique<RunReader>();
        RETURN_IF_ERROR(r->open(run_paths[i], has_positions));
        if (!r->exhausted()) {
            if (r->current_id() >= string_rank.size()) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "run term_id out of rank range");
            }
            heap.push({r->current_id(), i});
        }
        readers.push_back(std::move(r));
    }

    RunWriter w;
    RETURN_IF_ERROR(w.open(out_path));
    std::vector<size_t> matching; // run indices contributing the current term
    while (!heap.empty()) {
        const uint32_t id = heap.top().term_id;
        TermPostings merged;
        matching.clear();
        uint64_t total_docs = 0;
        uint64_t total_pos = 0;
        while (!heap.empty() && heap.top().term_id == id) {
            const size_t ri = heap.top().run;
            heap.pop();
            const RunReader* r = readers[ri].get();
            total_docs += r->current().docids.size();
            total_pos += r->current_pos_count();
            matching.push_back(ri);
        }
        merged.docids.reserve(static_cast<size_t>(total_docs));
        merged.freqs.reserve(static_cast<size_t>(total_docs));
        if (has_positions) {
            merged.positions_flat.reserve(static_cast<size_t>(total_pos));
        }
        // Concat (WITH boundary-doc coalescing) is deliberately the SAME
        // append the final merge applies: coalescing the seam between two
        // adjacent input runs here yields exactly what the final merge would
        // have produced from the uncompacted pair, so compaction is invisible
        // in the emitted term stream. A G04 position-suppressed term carries
        // an empty position block in every run (a per-TERM property), so its
        // compacted record is re-written with n_pos == 0.
        for (size_t ri : matching) {
            RunReader* r = readers[ri].get();
            if (has_positions) {
                RETURN_IF_ERROR(r->materialize_positions());
            }
            Concat(&merged, r->current(), has_positions);
        }
        RETURN_IF_ERROR(w.write_term(id, merged));
        for (size_t ri : matching) {
            RunReader* r = readers[ri].get();
            RETURN_IF_ERROR(r->advance());
            if (!r->exhausted()) {
                if (r->current_id() >= string_rank.size()) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                            "run term_id out of rank range");
                }
                heap.push({r->current_id(), ri});
            }
        }
    }
    return w.close();
}

} // namespace doris::snii::writer
