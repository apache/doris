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

// 形状值 0 曾是 CommonGrams 的无频次 docs-only 记录，已删除；run 文件是构建期临时文件，
// 读到 0 一律按损坏处理。
enum class RunPostingShape : uint8_t {
    kDocsAndFreqs = 1,
    kPositioned = 2,
};

RunPostingShape posting_shape(const TermPostings& tp) {
    DCHECK_EQ(tp.docids.size(), tp.freqs.size());
    return tp.retain_positions ? RunPostingShape::kPositioned : RunPostingShape::kDocsAndFreqs;
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

RunWriter::RunWriter(MemoryReporter* memory_reporter, size_t buffer_limit)
        : memory_reporter_(memory_reporter),
          buffer_reservation_(memory_reporter == nullptr
                                      ? MemoryReporter::Reservation()
                                      : memory_reporter->make_postings_reservation()),
          buffer_limit_(buffer_limit) {
    DORIS_CHECK(buffer_limit != 0 && buffer_limit <= kWriteFlushBytes);
}

RunWriter::~RunWriter() {
    if (fd_ >= 0) ::close(fd_);
}

Status RunWriter::open(const std::string& path, bool append) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC | (append ? O_APPEND : O_TRUNC),
                 0600);
    if (fd_ < 0) {
        return Status::Error<ErrorCode::IO_ERROR, false>("run open(" + path +
                                                         "): " + std::strerror(errno));
    }
    const off_t offset = ::lseek(fd_, 0, SEEK_END);
    if (offset < 0) {
        return Status::Error<ErrorCode::IO_ERROR, false>("run seek failed: {}",
                                                         std::strerror(errno));
    }
    file_bytes_ = static_cast<uint64_t>(offset);
    buf_.clear();
    return Status::OK();
}

Status RunWriter::flush() {
    if (buf_.empty()) return Status::OK();
    RETURN_IF_ERROR(write_all(fd_, buf_.data(), buf_.size()));
    file_bytes_ += buf_.size();
    if (memory_reporter_ != nullptr) {
        memory_reporter_->record_postings_io(0, buf_.size());
    }
    buf_.clear();
    return Status::OK();
}

Status RunWriter::append_bytes(const uint8_t* data, size_t size) {
    while (size != 0) {
        if (buf_.size() == buffer_limit_) {
            RETURN_IF_ERROR(flush());
        }
        const size_t count = std::min(size, buffer_limit_ - buf_.size());
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
    RETURN_IF_ERROR(append_raw_u32(tp.freqs.data(), tp.freqs.size()));
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
    if (encoded_shape < static_cast<uint8_t>(RunPostingShape::kDocsAndFreqs) ||
        encoded_shape > static_cast<uint8_t>(RunPostingShape::kPositioned)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: unknown posting shape");
    }
    const auto shape = static_cast<RunPostingShape>(encoded_shape);
    if (shape == RunPostingShape::kPositioned && !has_positions_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "run: positioned record in docs-only run");
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

} // namespace doris::snii::writer
