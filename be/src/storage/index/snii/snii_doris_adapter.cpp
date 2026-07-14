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

#include "storage/index/snii/snii_doris_adapter.h"

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <vector>

#include "common/cast_set.h"
#include "runtime/exec_env.h"
#include "storage/index/snii/common/uninitialized_buffer.h"
#include "util/countdown_latch.h"
#include "util/threadpool.h"

namespace doris::segment_v2::snii_doris {
namespace {
// Per-call cap on concurrently dispatched physical segment reads. A coalesced
// batch of at most this many segments is served as a single concurrent round
// (mirrors ::doris::snii::io::MeteredFileReader's "at most one serial round" contract and
// the S3 standalone reader's 16-way fan-out).
constexpr size_t kMaxConcurrentReads = 16;
} // namespace

thread_local const io::IOContext* DorisSniiFileReader::_scoped_io_ctx = nullptr;
ThreadPool* DorisSniiFileReader::_io_pool_for_test = nullptr;

Status DorisSniiFileWriter::append(::doris::snii::Slice data) {
    if (_writer == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("doris writer is null");
    }
    return _writer->append(Slice(reinterpret_cast<const char*>(data.data()), data.size()));
}

Status DorisSniiFileWriter::finalize() {
    if (_writer == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("doris writer is null");
    }
    return Status::OK();
}

uint64_t DorisSniiFileWriter::bytes_written() const {
    return _writer == nullptr ? 0 : _writer->bytes_appended();
}

DorisSniiFileReader::DorisSniiFileReader(io::FileReaderSPtr reader, const io::IOContext* io_ctx)
        : _reader(std::move(reader)), _default_io_ctx(_make_index_io_context(io_ctx)) {}

io::IOContext DorisSniiFileReader::_make_index_io_context(const io::IOContext* io_ctx) {
    io::IOContext index_io_ctx;
    if (io_ctx != nullptr) {
        index_io_ctx = *io_ctx;
    }
    index_io_ctx.is_inverted_index = true;
    // is_index_data is inherited from io_ctx: META scopes set it true at the source
    // (index_file_reader), non-meta reads default to false.
    return index_io_ctx;
}

DorisSniiFileReader::ScopedIOContext::ScopedIOContext(const io::IOContext* io_ctx)
        : _previous(_scoped_io_ctx), _io_ctx(DorisSniiFileReader::_make_index_io_context(io_ctx)) {
    _scoped_io_ctx = &_io_ctx;
}

DorisSniiFileReader::ScopedIOContext::~ScopedIOContext() {
    _scoped_io_ctx = _previous;
}

Status DorisSniiFileReader::read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) {
    RETURN_IF_ERROR(_check_read_range(offset, len));
    RETURN_IF_ERROR(_read_at(offset, len, out, _current_io_ctx()));
    if (len > 0) {
        _record_read_stats(cast_set<int64_t>(len), cast_set<int64_t>(len), 1, 1);
    }
    return Status::OK();
}

// NOLINTNEXTLINE(readability-non-const-parameter): out is the SNII read output buffer.
Status DorisSniiFileReader::_read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out,
                                     const io::IOContext* io_ctx) const {
    if (_reader == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("doris reader is null");
    }
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("output buffer is null");
    }
    RETURN_IF_ERROR(_check_read_range(offset, len));
    if (len == 0) {
        out->clear();
        return Status::OK();
    }
    out->resize(len);
    size_t bytes_read = 0;
    auto status = _reader->read_at(offset, Slice(out->data(), len), &bytes_read, io_ctx);
    if (!status.ok()) {
        return status;
    }
    if (bytes_read != len) {
        return Status::Error<ErrorCode::IO_ERROR, false>(
                fmt::format("short read at offset {}, expect {}, got {}", offset, len, bytes_read));
    }
    return Status::OK();
}

// NOLINTBEGIN(readability-non-const-parameter): outs is the SNII batch read output buffer.
Status DorisSniiFileReader::read_batch(const std::vector<::doris::snii::io::Range>& ranges,
                                       std::vector<std::vector<uint8_t>>* outs) {
    if (outs == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("output buffers is null");
    }
    outs->clear();
    outs->resize(ranges.size());
    if (ranges.empty()) {
        return Status::OK();
    }

    // ----- Phase 1: plan (serial, lock-free) -----
    // No section-classification lock exists on this reader, so the whole plan is
    // a plain in-memory scan; the NO-IO-UNDER-LOCK red line holds trivially (no
    // lock is taken anywhere in read_batch).
    struct IndexedRange {
        uint64_t offset = 0;
        size_t len = 0;
        size_t index = 0;
    };
    int64_t request_bytes = 0;
    std::vector<IndexedRange> sorted;
    sorted.reserve(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
        RETURN_IF_ERROR(_check_read_range(ranges[i].offset, ranges[i].len));
        request_bytes += cast_set<int64_t>(ranges[i].len);
        if (ranges[i].len == 0) {
            continue;
        }
        sorted.push_back({ranges[i].offset, ranges[i].len, i});
    }
    if (sorted.empty()) {
        return Status::OK();
    }
    // F27: callers (BatchRangeFetcher::fetch) already pass offset-sorted ranges;
    // only pay for a sort when the input is actually out of order.
    auto by_offset = [](const IndexedRange& lhs, const IndexedRange& rhs) {
        return lhs.offset < rhs.offset;
    };
    if (!std::ranges::is_sorted(sorted, by_offset)) {
        std::ranges::sort(sorted, by_offset);
    }

    // Coalesce the sorted ranges into disjoint physical segments.
    struct Seg {
        uint64_t offset = 0;
        size_t len = 0;
        size_t begin = 0; // first index into `sorted` covered by this segment
        size_t end = 0;   // one-past-last index into `sorted`
        bool single = false;
    };
    constexpr uint64_t max_coalesced_gap = 4096;
    constexpr uint64_t max_coalesced_read = 1ULL << 20;
    std::vector<Seg> segs;
    for (size_t begin = 0; begin < sorted.size();) {
        uint64_t read_offset = sorted[begin].offset;
        uint64_t read_end = sorted[begin].offset + sorted[begin].len;
        size_t end = begin + 1;
        while (end < sorted.size()) {
            const uint64_t next_end = sorted[end].offset + sorted[end].len;
            if ((sorted[end].offset > read_end &&
                 sorted[end].offset - read_end > max_coalesced_gap) ||
                next_end - read_offset > max_coalesced_read) {
                break;
            }
            read_end = std::max(read_end, next_end);
            ++end;
        }
        Seg seg;
        seg.offset = read_offset;
        seg.len = cast_set<size_t>(read_end - read_offset);
        seg.begin = begin;
        seg.end = end;
        // A single-range group exactly covers its segment, so it can be read
        // straight into the caller's output slot with no temp + no second copy.
        seg.single = (end == begin + 1);
        segs.push_back(seg);
        begin = end;
    }

    // Resolve per-segment target buffers, io contexts and the shared sink on the
    // calling thread: workers (which run on tracker-less pool threads) must not
    // allocate, and per-segment private cache-stat slots keep disjoint physical
    // reads from racing on the shared FileCacheStatistics.
    const size_t num_segs = segs.size();
    const io::IOContext* base_io_ctx = _current_io_ctx();
    std::vector<std::vector<uint8_t>> tmp_bufs(num_segs);
    std::vector<std::vector<uint8_t>*> targets(num_segs);
    std::vector<io::FileCacheStatistics> seg_stats(num_segs);
    std::vector<io::IOContext> seg_io_ctx(num_segs);
    std::vector<Status> seg_status(num_segs);
    int64_t read_bytes = 0;
    for (size_t s = 0; s < num_segs; ++s) {
        const Seg& seg = segs[s];
        std::vector<uint8_t>* target =
                seg.single ? &(*outs)[sorted[seg.begin].index] : &tmp_bufs[s];
        ::doris::snii::resize_uninitialized(*target, seg.len);
        targets[s] = target;
        seg_io_ctx[s] = *base_io_ctx;
        seg_io_ctx[s].file_cache_stats =
                base_io_ctx->file_cache_stats != nullptr ? &seg_stats[s] : nullptr;
        read_bytes += cast_set<int64_t>(seg.len);
    }

    // ----- Phase 2: physical reads (lock-free; concurrent when a pool exists) -----
    auto run_segment = [&](size_t s) {
        seg_status[s] = _read_at(segs[s].offset, segs[s].len, targets[s], &seg_io_ctx[s]);
    };
    ThreadPool* pool = _select_io_pool();
    if (pool != nullptr && num_segs > 1) {
        for (size_t base = 0; base < num_segs; base += kMaxConcurrentReads) {
            const size_t wave_end = std::min(base + kMaxConcurrentReads, num_segs);
            ::doris::CountDownLatch latch(cast_set<int>(wave_end - base));
            for (size_t s = base; s < wave_end; ++s) {
                Status submit_st = pool->submit_func([&run_segment, &latch, s]() {
                    run_segment(s);
                    latch.count_down();
                });
                if (!submit_st.ok()) {
                    // Pool full/shut down: read this segment inline; never skip
                    // the count_down or the latch would not drain.
                    run_segment(s);
                    latch.count_down();
                }
            }
            latch.wait();
        }
    } else {
        // Serial fallback: no executor (e.g. tools without ExecEnv) or a single
        // segment (avoids micro-batch scheduling overhead).
        for (size_t s = 0; s < num_segs; ++s) {
            run_segment(s);
        }
    }

    // ----- Phase 3: first-error, scatter, merge stats, account (serial) -----
    for (size_t s = 0; s < num_segs; ++s) {
        RETURN_IF_ERROR(seg_status[s]);
    }
    for (size_t s = 0; s < num_segs; ++s) {
        const Seg& seg = segs[s];
        if (seg.single) {
            continue; // already read in place
        }
        const std::vector<uint8_t>& bytes = tmp_bufs[s];
        for (size_t i = seg.begin; i < seg.end; ++i) {
            const uint64_t pos = sorted[i].offset - seg.offset;
            auto& out = (*outs)[sorted[i].index];
            out.assign(bytes.begin() + cast_set<ptrdiff_t>(pos),
                       bytes.begin() + cast_set<ptrdiff_t>(pos + sorted[i].len));
        }
    }
    if (base_io_ctx->file_cache_stats != nullptr) {
        for (size_t s = 0; s < num_segs; ++s) {
            _merge_file_cache_statistics(base_io_ctx->file_cache_stats, seg_stats[s]);
        }
    }
    _record_read_stats(request_bytes, read_bytes, cast_set<int64_t>(num_segs),
                       cast_set<int64_t>(_compute_num_waves(num_segs)));
    return Status::OK();
}
// NOLINTEND(readability-non-const-parameter)

uint64_t DorisSniiFileReader::size() const {
    return _reader == nullptr ? 0 : _reader->size();
}

const io::IOContext* DorisSniiFileReader::_current_io_ctx() const {
    return _scoped_io_ctx != nullptr ? _scoped_io_ctx : &_default_io_ctx;
}

void DorisSniiFileReader::_record_read_stats(int64_t request_bytes, int64_t read_bytes,
                                             int64_t range_read_count,
                                             int64_t serial_read_rounds) const {
    const auto* io_ctx = _current_io_ctx();
    if (io_ctx->file_cache_stats == nullptr) {
        return;
    }
    auto* stats = io_ctx->file_cache_stats;
    stats->inverted_index_request_bytes += request_bytes;
    stats->inverted_index_read_bytes += read_bytes;
    stats->inverted_index_range_read_count += range_read_count;
    stats->inverted_index_serial_read_rounds += serial_read_rounds;
}

void DorisSniiFileReader::set_io_thread_pool_for_test(ThreadPool* pool) {
    _io_pool_for_test = pool;
}

ThreadPool* DorisSniiFileReader::_select_io_pool() {
    if (_io_pool_for_test != nullptr) {
        return _io_pool_for_test;
    }
    if (ExecEnv::ready()) {
        return ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool();
    }
    return nullptr;
}

size_t DorisSniiFileReader::_compute_num_waves(size_t seg_count) {
    if (seg_count == 0) {
        return 0;
    }
    return (seg_count + kMaxConcurrentReads - 1) / kMaxConcurrentReads;
}

void DorisSniiFileReader::_merge_file_cache_statistics(io::FileCacheStatistics* dst,
                                                       const io::FileCacheStatistics& src) {
    if (dst == nullptr) {
        return;
    }
    dst->num_local_io_total += src.num_local_io_total;
    dst->num_remote_io_total += src.num_remote_io_total;
    dst->num_peer_io_total += src.num_peer_io_total;
    dst->local_io_timer += src.local_io_timer;
    dst->bytes_read_from_local += src.bytes_read_from_local;
    dst->bytes_read_from_remote += src.bytes_read_from_remote;
    dst->bytes_read_from_peer += src.bytes_read_from_peer;
    dst->remote_io_timer += src.remote_io_timer;
    dst->peer_io_timer += src.peer_io_timer;
    dst->remote_wait_timer += src.remote_wait_timer;
    dst->write_cache_io_timer += src.write_cache_io_timer;
    dst->bytes_write_into_cache += src.bytes_write_into_cache;
    dst->num_skip_cache_io_total += src.num_skip_cache_io_total;
    dst->read_cache_file_directly_timer += src.read_cache_file_directly_timer;
    dst->cache_get_or_set_timer += src.cache_get_or_set_timer;
    dst->lock_wait_timer += src.lock_wait_timer;
    dst->get_timer += src.get_timer;
    dst->set_timer += src.set_timer;
    dst->inverted_index_num_local_io_total += src.inverted_index_num_local_io_total;
    dst->inverted_index_num_remote_io_total += src.inverted_index_num_remote_io_total;
    dst->inverted_index_num_peer_io_total += src.inverted_index_num_peer_io_total;
    dst->inverted_index_bytes_read_from_local += src.inverted_index_bytes_read_from_local;
    dst->inverted_index_bytes_read_from_remote += src.inverted_index_bytes_read_from_remote;
    dst->inverted_index_bytes_read_from_peer += src.inverted_index_bytes_read_from_peer;
    dst->inverted_index_remote_physical_read_bytes += src.inverted_index_remote_physical_read_bytes;
    dst->inverted_index_bytes_write_into_cache += src.inverted_index_bytes_write_into_cache;
    dst->inverted_index_local_io_timer += src.inverted_index_local_io_timer;
    dst->inverted_index_remote_io_timer += src.inverted_index_remote_io_timer;
    dst->inverted_index_peer_io_timer += src.inverted_index_peer_io_timer;
    dst->inverted_index_io_timer += src.inverted_index_io_timer;
    dst->inverted_index_request_bytes += src.inverted_index_request_bytes;
    dst->inverted_index_read_bytes += src.inverted_index_read_bytes;
    dst->inverted_index_range_read_count += src.inverted_index_range_read_count;
    dst->inverted_index_serial_read_rounds += src.inverted_index_serial_read_rounds;
}

Status DorisSniiFileReader::_check_read_range(uint64_t offset, size_t len) const {
    if (_reader == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("doris reader is null");
    }
    if (offset > std::numeric_limits<uint64_t>::max() - len) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                fmt::format("read range overflows: offset {}, len {}", offset, len));
    }
    const uint64_t end = offset + len;
    if (end > _reader->size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                fmt::format("read range exceeds file size: offset {}, len {}, file size {}", offset,
                            len, _reader->size()));
    }
    return Status::OK();
}

} // namespace doris::segment_v2::snii_doris
