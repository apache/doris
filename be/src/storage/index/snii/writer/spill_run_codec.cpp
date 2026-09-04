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
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <memory>
#include <queue>
#include <utility>

#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::writer {

namespace {

// Flush staging at this exact bound. A large write buffer (4 MiB) collapses the
// per-flush write() syscall count by ~64x: at 64 KiB the 5M build issued
// ~8800 write()s to ext4 (~9s of syscall overhead) for ~553 MiB of runs, versus
// a raw dd of the same bytes taking ~1.2s. Wide terms are appended and flushed
// in chunks, so the staging allocation never grows with term width.
constexpr size_t kWriteFlushBytes = 1u << 22; // 4 MiB
// RunReader reads this much per disk fill; the window slides so a single record
// never needs the whole run in RAM (only the current term's encoded span). KEEP
// this small (64 KiB): a large read chunk x many open runs would inflate the
// merge-phase peak RSS at low spill thresholds (each reader holds a window).
constexpr size_t kReadChunkBytes = 1u << 16; // 64 KiB

enum class RunPostingShape : uint8_t {
    kDocsOnlyStatless = 0,
    kDocsAndFreqs = 1,
    kPositioned = 2,
};

RunPostingShape posting_shape(const TermPostings& tp) {
    if (tp.retain_positions) {
        return RunPostingShape::kPositioned;
    }
    return tp.freqs.empty() ? RunPostingShape::kDocsOnlyStatless : RunPostingShape::kDocsAndFreqs;
}

// Writes the full byte range [data, data+len) to fd, looping over short writes.
Status write_all(int fd, const uint8_t* data, size_t len) {
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

template <typename T>
Status reserve_vector_for_size(std::vector<T>* values, size_t target,
                               MemoryReporter* memory_reporter,
                               MemoryReporter::Reservation* reservation) {
    if (target <= values->capacity()) {
        return Status::OK();
    }
    if (target > std::numeric_limits<uint64_t>::max() / sizeof(T)) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "run reader: vector byte capacity overflow");
    }
    if (memory_reporter == nullptr) {
        values->reserve(target);
        return Status::OK();
    }
    MemoryReporter::Reservation replacement;
    const uint64_t target_bytes = static_cast<uint64_t>(target) * sizeof(T);
    RETURN_IF_ERROR(reservation->prepare_replacement(target_bytes, &replacement));
    values->reserve(target);
    DCHECK_EQ(values->capacity(), target);
    *reservation = std::move(replacement);
    return Status::OK();
}

Status reserve_write_buffer_for_append(std::vector<uint8_t>* buffer, size_t target,
                                       MemoryReporter* memory_reporter,
                                       MemoryReporter::Reservation* reservation) {
    if (target <= buffer->capacity()) {
        return Status::OK();
    }
    if (target > kWriteFlushBytes) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "run writer: staging buffer exceeds flush bound");
    }

    size_t capacity = std::max<size_t>(buffer->capacity(), 1);
    while (capacity < target) {
        capacity = capacity > kWriteFlushBytes / 2 ? kWriteFlushBytes : capacity * 2;
    }
    return reserve_vector_for_size(buffer, capacity, memory_reporter, reservation);
}

} // namespace

// ---------------------------------------------------------------------------
// RunWriter
// ---------------------------------------------------------------------------

RunWriter::RunWriter(MemoryReporter* memory_reporter)
        : memory_reporter_(memory_reporter),
          buffer_reservation_(memory_reporter == nullptr ? MemoryReporter::Reservation()
                                                         : memory_reporter->make_reservation()) {}

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
    RETURN_IF_ERROR(write_all(fd_, buf_.data(), buf_.size()));
    buf_.clear();
    return Status::OK();
}

Status RunWriter::append_bytes(const uint8_t* data, size_t size) {
    while (size != 0) {
        if (buf_.size() == kWriteFlushBytes) {
            RETURN_IF_ERROR(flush());
        }
        const size_t count = std::min(size, kWriteFlushBytes - buf_.size());
        const size_t target = buf_.size() + count;
        RETURN_IF_ERROR(reserve_write_buffer_for_append(&buf_, target, memory_reporter_,
                                                        &buffer_reservation_));
        buf_.insert(buf_.end(), data, data + count);
        data += count;
        size -= count;
    }
    return Status::OK();
}

Status RunWriter::append_varint(uint64_t value) {
    uint8_t bytes[10];
    const size_t size = encode_varint64(value, bytes);
    return append_bytes(bytes, size);
}

Status RunWriter::append_raw_u32(const uint32_t* values, size_t count) {
    if (count > std::numeric_limits<size_t>::max() / sizeof(uint32_t)) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "run writer: raw u32 byte count overflows size_t");
    }
    return append_bytes(reinterpret_cast<const uint8_t*>(values), count * sizeof(uint32_t));
}

void RunWriter::release_buffer() {
    std::vector<uint8_t>().swap(buf_);
    buffer_reservation_.reset();
}

Status RunWriter::write_term(uint32_t term_id, const TermPostings& tp) {
    DCHECK(tp.retain_positions || tp.positions_flat.empty());
    const RunPostingShape shape = posting_shape(tp);
    const size_t doc_count = tp.document_count();
    if (shape != RunPostingShape::kDocsOnlyStatless) {
        DCHECK_EQ(tp.docids.size(), tp.freqs.size());
    }
    RETURN_IF_ERROR(append_varint(term_id));
    RETURN_IF_ERROR(append_varint(static_cast<uint8_t>(shape)));
    RETURN_IF_ERROR(append_varint(doc_count));
    // Docids are a RAW fixed-width u32 block (bulk memcpy), NOT per-value VInt.
    // Per-value varint over ~60M docids cost ~1.5s of encode CPU on the spill feed
    // side; raw is a single memcpy and the decode side becomes a memcpy too. Runs
    // are PRIVATE temp files written then read back from page cache, so the modestly
    // larger run (no delta packing) costs ~0 extra real I/O. Absolute docids are
    // stored (the merge concatenates per-term across runs and re-deltas at encode).
    RETURN_IF_ERROR(append_raw_u32(tp.docids.data(), tp.docids.size()));
    if (shape != RunPostingShape::kDocsOnlyStatless) {
        RETURN_IF_ERROR(append_raw_u32(tp.freqs.data(), tp.freqs.size()));
    }
    if (shape == RunPostingShape::kPositioned) {
        const uint64_t n_pos = tp.positions_flat.size();
        RETURN_IF_ERROR(append_varint(n_pos));
        RETURN_IF_ERROR(append_raw_u32(tp.positions_flat.data(), tp.positions_flat.size()));
    }
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
    release_buffer();
    return Status::OK();
}

// ---------------------------------------------------------------------------
// RunReader
// ---------------------------------------------------------------------------

RunReader::RunReader(MemoryReporter* memory_reporter)
        : memory_reporter_(memory_reporter),
          window_reservation_(memory_reporter == nullptr ? MemoryReporter::Reservation()
                                                         : memory_reporter->make_reservation()),
          docids_reservation_(memory_reporter == nullptr ? MemoryReporter::Reservation()
                                                         : memory_reporter->make_reservation()),
          freqs_reservation_(memory_reporter == nullptr ? MemoryReporter::Reservation()
                                                        : memory_reporter->make_reservation()),
          positions_reservation_(memory_reporter == nullptr ? MemoryReporter::Reservation()
                                                            : memory_reporter->make_reservation()) {
}

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
    bytes_read_ = 0;
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
    if (bytes_read_ > file_size_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run reader: bytes read exceed file size");
    }
    const uint64_t remaining = file_size_ - bytes_read_;
    if (remaining == 0) {
        eof_ = true;
        return Status::OK();
    }
    const size_t read_size = static_cast<size_t>(
            std::min<uint64_t>(remaining, static_cast<uint64_t>(kReadChunkBytes)));
    const size_t base = window_.size();
    if (base > std::numeric_limits<size_t>::max() - read_size) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "run reader: decode window capacity overflow");
    }
    RETURN_IF_ERROR(reserve_vector_for_size(&window_, base + read_size, memory_reporter_,
                                            &window_reservation_));
    window_.resize(base + read_size);
    ssize_t n;
    do {
        n = ::read(fd_, window_.data() + base, read_size);
    } while (n < 0 && errno == EINTR);
    if (n < 0)
        return Status::Error<ErrorCode::IO_ERROR, false>(std::string("run read: ") +
                                                         std::strerror(errno));
    window_.resize(base + static_cast<size_t>(n));
    bytes_read_ += static_cast<uint64_t>(n);
    if (n == 0 || bytes_read_ == file_size_) eof_ = true;
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
    if (count > std::numeric_limits<size_t>::max() / sizeof(uint32_t)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: raw u32 byte count overflows size_t");
    }
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
Status RunReader::read_raw_u32(size_t count, std::vector<uint32_t>* out,
                               MemoryReporter::Reservation* reservation) {
    // Bound `count` against the run's byte size BEFORE resize(): a record can never
    // hold more u32s than the whole file. Rejects a corrupt/truncated length varint
    // (which is otherwise an unbounded resize -> uncaught std::bad_alloc).
    if (count > file_size_ / sizeof(uint32_t)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: raw u32 count exceeds file size");
    }
    RETURN_IF_ERROR(reserve_vector_for_size(out, count, memory_reporter_, reservation));
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
    if (pos_remaining_ > std::numeric_limits<size_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: position count exceeds addressable memory");
    }
    const size_t n = static_cast<size_t>(pos_remaining_);
    RETURN_IF_ERROR(read_raw_u32(n, &current_.positions_flat, &positions_reservation_));
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
    std::array<uint32_t, 1024> scratch;
    while (pos_remaining_ != 0) {
        const size_t count = static_cast<size_t>(
                std::min<uint64_t>(pos_remaining_, static_cast<uint64_t>(scratch.size())));
        RETURN_IF_ERROR(pull_raw_u32(reinterpret_cast<uint8_t*>(scratch.data()), count));
        pos_remaining_ -= count;
    }
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

    uint64_t encoded_shape = 0;
    RETURN_IF_ERROR(read_varint(&encoded_shape));
    if (encoded_shape > static_cast<uint8_t>(RunPostingShape::kPositioned)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: unknown posting shape");
    }
    const auto shape = static_cast<RunPostingShape>(encoded_shape);
    if (shape == RunPostingShape::kPositioned && !has_positions_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: positioned record in docs-only run");
    }
    if (shape == RunPostingShape::kDocsOnlyStatless && !has_positions_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: statless record requires a positioned mixed-shape run");
    }

    uint64_t n_docs = 0;
    RETURN_IF_ERROR(read_varint(&n_docs));
    if (n_docs > file_size_ / sizeof(uint32_t) || n_docs > std::numeric_limits<size_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: document count exceeds file size or addressable memory");
    }
    // Docids: RAW absolute u32 block (bulk read), matching the writer's AppendRawU32.
    RETURN_IF_ERROR(
            read_raw_u32(static_cast<size_t>(n_docs), &current_.docids, &docids_reservation_));
    for (size_t i = 1; i < current_.docids.size(); ++i) {
        if (current_.docids[i] <= current_.docids[i - 1]) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "run: docids must be strictly ascending within one record");
        }
    }
    current_.freqs.clear();
    current_.positions_flat.clear();
    pos_count_ = 0;
    pos_remaining_ = 0;
    if (shape == RunPostingShape::kDocsOnlyStatless) {
        current_.retain_positions = false;
        return Status::OK();
    }

    // Freqs: RAW u32 block (bulk read), matching the writer's AppendRawU32.
    RETURN_IF_ERROR(
            read_raw_u32(static_cast<size_t>(n_docs), &current_.freqs, &freqs_reservation_));
    uint64_t total_freq = 0;
    for (uint32_t freq : current_.freqs) {
        if (freq == 0) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "run: frequency must be positive");
        }
        if (freq > UINT64_MAX - total_freq) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "run: frequency sum overflows uint64");
        }
        total_freq += freq;
    }
    if (shape == RunPostingShape::kDocsAndFreqs) {
        current_.retain_positions = false;
        return Status::OK();
    }

    uint64_t n_pos = 0;
    RETURN_IF_ERROR(read_varint(&n_pos));
    if (n_pos != total_freq) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: position count does not match frequency sum");
    }
    if (n_pos > file_size_ / sizeof(uint32_t)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: position count exceeds file size");
    }
    // Positions are LAZY: record the block count and leave the window cursor parked
    // at the block start. The owner picks materialize_positions() for explicit
    // materialization or stream_positions() for bounded writer-owned windows.
    current_.retain_positions = true;
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
void concat(TermPostings* dst, const TermPostings& src, RunPostingShape shape) {
    if (src.docids.empty()) return;
    const bool has_positions = shape == RunPostingShape::kPositioned;
    const bool statless = shape == RunPostingShape::kDocsOnlyStatless;
    DCHECK(posting_shape(src) == shape);
    size_t start = 0;
    size_t src_pos_start = 0; // flat offset of src positions to append after splice
    if (!dst->docids.empty() && dst->docids.back() == src.docids.front()) {
        const uint32_t head_fc = statless ? 0 : src.freqs.front();
        if (has_positions && head_fc != 0) {
            // Splice src's first-doc positions in right after dst's last-doc positions.
            // dst's last doc owns dst->freqs.back() entries at the tail of positions_flat
            // BEFORE we bump that freq, so insert at end() (last doc is the tail run).
            auto& flat = dst->positions_flat;
            flat.insert(flat.end(), src.positions_flat.begin(),
                        src.positions_flat.begin() + head_fc);
        }
        if (!statless) {
            dst->freqs.back() += head_fc;
        }
        src_pos_start = head_fc;
        start = 1; // boundary doc folded in; append the rest
    }
    dst->docids.insert(dst->docids.end(), src.docids.begin() + start, src.docids.end());
    if (!statless) {
        dst->freqs.insert(dst->freqs.end(), src.freqs.begin() + start, src.freqs.end());
    }
    if (has_positions) {
        dst->positions_flat.insert(dst->positions_flat.end(),
                                   src.positions_flat.begin() + src_pos_start,
                                   src.positions_flat.end());
    }
}

class RunTermPostingSource final : public TermPostingSource {
public:
    RunTermPostingSource(std::vector<std::unique_ptr<RunReader>>* readers,
                         const std::vector<size_t>* matching, RunPostingShape shape)
            : readers_(readers), matching_(matching), shape_(shape) {}

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        if (out == nullptr || exhausted == nullptr || target_docs == 0 || !out->empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "run posting source: invalid fill arguments");
        }

        Cursor planned = cursor_;
        size_t document_count = 0;
        size_t position_count = 0;
        while (document_count < target_docs) {
            normalize(&planned);
            if (planned.run == matching_->size()) {
                break;
            }
            const uint32_t docid = current_docid(planned);
            uint64_t frequency = 0;
            do {
                const TermPostings& postings = current_postings(planned);
                if (shape_ != RunPostingShape::kDocsOnlyStatless) {
                    frequency += postings.freqs[planned.doc];
                    if (frequency > std::numeric_limits<uint32_t>::max()) {
                        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                                "run: coalesced frequency exceeds uint32");
                    }
                }
                advance(&planned);
                normalize(&planned);
                if (planned.run == matching_->size()) {
                    break;
                }
                const uint32_t next_docid = current_docid(planned);
                if (next_docid < docid) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                            "run: docids overlap across spill runs");
                }
                if (next_docid != docid) {
                    break;
                }
            } while (true);
            if (shape_ == RunPostingShape::kPositioned) {
                if (frequency > std::numeric_limits<size_t>::max() - position_count) {
                    return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                            "run posting source: position window exceeds size_t");
                }
                position_count += static_cast<size_t>(frequency);
            }
            ++document_count;
        }

        MutableTermPostingSpan destination;
        const bool has_freqs = shape_ != RunPostingShape::kDocsOnlyStatless;
        RETURN_IF_ERROR(
                out->grow_uninitialized(document_count, has_freqs, position_count, &destination));
        size_t position_offset = 0;
        for (size_t output = 0; output < document_count; ++output) {
            normalize(&cursor_);
            DCHECK_LT(cursor_.run, matching_->size());
            const uint32_t docid = current_docid(cursor_);
            uint64_t frequency = 0;
            do {
                const TermPostings& postings = current_postings(cursor_);
                const uint32_t run_frequency = has_freqs ? postings.freqs[cursor_.doc] : 0;
                if (shape_ == RunPostingShape::kPositioned) {
                    RunReader* reader = (*readers_)[(*matching_)[cursor_.run]].get();
                    RETURN_IF_ERROR(reader->stream_positions(
                            destination.positions_flat.data() + position_offset, run_frequency));
                    position_offset += run_frequency;
                }
                frequency += run_frequency;
                advance(&cursor_);
                normalize(&cursor_);
                if (cursor_.run == matching_->size() || current_docid(cursor_) != docid) {
                    break;
                }
            } while (true);
            destination.docids[output] = docid;
            if (has_freqs) {
                destination.freqs[output] = static_cast<uint32_t>(frequency);
            }
        }
        DCHECK_EQ(position_offset, destination.positions_flat.size());
        normalize(&cursor_);
        *exhausted = cursor_.run == matching_->size();
        return Status::OK();
    }

    bool exhausted() {
        normalize(&cursor_);
        return cursor_.run == matching_->size();
    }

private:
    struct Cursor {
        size_t run = 0;
        size_t doc = 0;
    };

    const TermPostings& current_postings(const Cursor& cursor) const {
        return (*readers_)[(*matching_)[cursor.run]]->current();
    }

    uint32_t current_docid(const Cursor& cursor) const {
        return current_postings(cursor).docids[cursor.doc];
    }

    void normalize(Cursor* cursor) const {
        while (cursor->run < matching_->size() &&
               cursor->doc == current_postings(*cursor).docids.size()) {
            ++cursor->run;
            cursor->doc = 0;
        }
    }

    static void advance(Cursor* cursor) { ++cursor->doc; }

    std::vector<std::unique_ptr<RunReader>>* readers_;
    const std::vector<size_t>* matching_;
    RunPostingShape shape_;
    Cursor cursor_;
};

} // namespace

Status merge_run_sources(const std::vector<std::string>& run_paths,
                         const std::vector<std::string>& vocab,
                         const std::vector<uint32_t>& string_rank, bool has_positions,
                         const StreamedTermConsumer& fn, TermKeyMaterializer materialize_term_key,
                         MemoryReporter* memory_reporter) {
    if (string_rank.size() != vocab.size()) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "merge_run_sources: string_rank/vocab size mismatch");
    }
    std::vector<std::unique_ptr<RunReader>> readers;
    readers.reserve(run_paths.size());
    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapGreater> heap(
            HeapGreater {&string_rank});
    for (size_t i = 0; i < run_paths.size(); ++i) {
        auto reader = std::make_unique<RunReader>(memory_reporter);
        RETURN_IF_ERROR(reader->open(run_paths[i], has_positions));
        if (!reader->exhausted()) {
            if (reader->current_id() >= vocab.size()) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "run term_id out of vocab range");
            }
            heap.push({reader->current_id(), i});
        }
        readers.push_back(std::move(reader));
    }

    std::vector<size_t> matching;
    while (!heap.empty()) {
        const uint32_t id = heap.top().term_id;
        matching.clear();
        while (!heap.empty() && heap.top().term_id == id) {
            matching.push_back(heap.top().run);
            heap.pop();
        }
        DCHECK(!matching.empty());
        const RunPostingShape shape = posting_shape(readers[matching.front()]->current());
        for (size_t run : matching) {
            if (posting_shape(readers[run]->current()) != shape) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "run: posting shape differs across matching terms");
            }
        }

        RunTermPostingSource source(&readers, &matching, shape);
        StreamedTermPostings postings {.term = materialize_term_key
                                                       ? materialize_term_key(vocab[id])
                                                       : std::string(vocab[id]),
                                       .retain_positions = shape == RunPostingShape::kPositioned,
                                       .source = &source};
        RETURN_IF_ERROR(fn(std::move(postings)));
        if (!source.exhausted()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "run posting source: consumer returned before term exhaustion");
        }

        for (size_t run : matching) {
            RunReader* reader = readers[run].get();
            RETURN_IF_ERROR(reader->advance());
            if (!reader->exhausted()) {
                if (reader->current_id() >= vocab.size()) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                            "run term_id out of vocab range");
                }
                heap.push({reader->current_id(), run});
            }
        }
    }
    return Status::OK();
}

Status compact_runs(const std::vector<std::string>& run_paths,
                    const std::vector<uint32_t>& string_rank, bool has_positions,
                    const std::string& out_path, MemoryReporter* memory_reporter) {
    // Same heap machinery as merge_run_sources, but the output is a RUN (records keyed
    // by term-id, ordered by string rank -- the exact invariant every run file
    // carries), not a resolved term stream: no vocab strings are needed, and
    // positions are always materialized because the run codec serializes
    // positions_flat directly.
    std::vector<std::unique_ptr<RunReader>> readers;
    readers.reserve(run_paths.size());
    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapGreater> heap(
            HeapGreater {&string_rank});
    for (size_t i = 0; i < run_paths.size(); ++i) {
        auto r = std::make_unique<RunReader>(memory_reporter);
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

    RunWriter w(memory_reporter);
    RETURN_IF_ERROR(w.open(out_path));
    std::vector<size_t> matching; // run indices contributing the current term
    while (!heap.empty()) {
        const uint32_t id = heap.top().term_id;
        MemoryReporter::Reservation merged_docids_reservation =
                memory_reporter == nullptr ? MemoryReporter::Reservation()
                                           : memory_reporter->make_reservation();
        MemoryReporter::Reservation merged_freqs_reservation =
                memory_reporter == nullptr ? MemoryReporter::Reservation()
                                           : memory_reporter->make_reservation();
        MemoryReporter::Reservation merged_positions_reservation =
                memory_reporter == nullptr ? MemoryReporter::Reservation()
                                           : memory_reporter->make_reservation();
        TermPostings merged;
        matching.clear();
        uint64_t total_docs = 0;
        uint64_t total_pos = 0;
        while (!heap.empty() && heap.top().term_id == id) {
            const size_t ri = heap.top().run;
            heap.pop();
            const RunReader* r = readers[ri].get();
            const uint64_t run_docs = r->current().docids.size();
            const uint64_t run_positions = r->current_pos_count();
            if (run_docs > std::numeric_limits<uint64_t>::max() - total_docs ||
                run_positions > std::numeric_limits<uint64_t>::max() - total_pos) {
                return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                        "run compaction: merged posting size overflows uint64");
            }
            total_docs += run_docs;
            total_pos += run_positions;
            matching.push_back(ri);
        }
        DCHECK(!matching.empty());
        const RunPostingShape shape = posting_shape(readers[matching.front()]->current());
        for (size_t ri : matching) {
            if (posting_shape(readers[ri]->current()) != shape) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "run: posting shape differs across matching terms");
            }
        }
        const bool term_has_positions = shape == RunPostingShape::kPositioned;
        const bool statless = shape == RunPostingShape::kDocsOnlyStatless;
        merged.retain_positions = term_has_positions;
        if (total_docs > std::numeric_limits<size_t>::max() ||
            total_pos > std::numeric_limits<size_t>::max()) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "run compaction: merged posting exceeds addressable memory");
        }
        RETURN_IF_ERROR(reserve_vector_for_size(&merged.docids, static_cast<size_t>(total_docs),
                                                memory_reporter, &merged_docids_reservation));
        if (!statless) {
            RETURN_IF_ERROR(reserve_vector_for_size(&merged.freqs, static_cast<size_t>(total_docs),
                                                    memory_reporter, &merged_freqs_reservation));
        }
        if (term_has_positions) {
            RETURN_IF_ERROR(reserve_vector_for_size(&merged.positions_flat,
                                                    static_cast<size_t>(total_pos), memory_reporter,
                                                    &merged_positions_reservation));
        }
        // concat (WITH boundary-doc coalescing) is deliberately the SAME
        // append the final merge applies: coalescing the seam between two
        // adjacent input runs here yields exactly what the final merge would
        // have produced from the uncompacted pair, so compaction is invisible
        // in the emitted term stream.
        for (size_t ri : matching) {
            RunReader* r = readers[ri].get();
            if (term_has_positions) {
                RETURN_IF_ERROR(r->materialize_positions());
            }
            concat(&merged, r->current(), shape);
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
