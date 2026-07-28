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
#include "cpp/sync_point.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "storage/index/snii/common/uninitialized_buffer.h"
#include "util/countdown_latch.h"
#include "util/threadpool.h"

namespace doris::segment_v2::snii_doris {
namespace {
// Per-call cap on concurrently dispatched physical segment reads. A coalesced
// batch of at most this many segments is served as a single concurrent round
// (mirrors the "at most one serial round" contract that the test-side
// MeteredFileReader measures, and the S3 standalone reader's 16-way fan-out).
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

DorisSniiFileReader::DorisSniiFileReader(io::FileReaderSPtr reader, const io::IOContext* io_ctx,
                                         bool direct_remote_io)
        : _reader(std::move(reader)),
          _direct_remote_io(direct_remote_io),
          _default_io_ctx(_make_index_io_context(io_ctx)) {}

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
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("output buffer is null");
    }
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
    TEST_SYNC_POINT_RETURN_WITH_VALUE("DorisSniiFileReader::_read_at",
                                      Status::IOError("injected SNII read failure"), offset, len);
    DCHECK(_reader != nullptr);
    DCHECK(out != nullptr);
    DCHECK(_check_read_range(offset, len).ok());
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
        // Carried onto the pool threads below. They are "Orphan" threads with no MemTrackerLimiter
        // of their own (buffered_reader.cpp:426), and the read does allocate down there: in cloud
        // mode _read_at reaches CachedRemoteFileReader::read_at_impl ->
        // _read_from_indirect_cache -> _read_remote_blocks_into_cache -> _execute_remote_read ->
        // _execute_s3_fallback, which does `new char[span_size]`. Without this the span is charged
        // to no tracker at all, which memory_orphan_check() (on by default) treats as a bug.
        // Same shape as the hedged-read path in cached_remote_file_reader.cpp:500-505.
        const std::shared_ptr<ResourceContext> parent_resource_ctx =
                thread_context()->resource_ctx();
        for (size_t base = 0; base < num_segs; base += kMaxConcurrentReads) {
            const size_t wave_end = std::min(base + kMaxConcurrentReads, num_segs);
            ::doris::CountDownLatch latch(cast_set<int>(wave_end - base));
            for (size_t s = base; s < wave_end; ++s) {
                Status submit_st =
                        pool->submit_func([&run_segment, &latch, s, parent_resource_ctx]() {
                            std::unique_ptr<AttachTask> attach_task;
                            if (parent_resource_ctx != nullptr) {
                                attach_task = std::make_unique<AttachTask>(parent_resource_ctx);
                            }
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

    // ----- Phase 3: merge stats, first-error, scatter, account (serial) -----
    // Fold every segment's private stats back FIRST: physical IO that already
    // happened (including partial work inside a segment that then failed) must
    // reach the query profile even when another segment of this batch errors.
    if (base_io_ctx->file_cache_stats != nullptr) {
        for (size_t s = 0; s < num_segs; ++s) {
            _merge_file_cache_statistics(base_io_ctx->file_cache_stats, seg_stats[s]);
        }
    }
    Status first_error = Status::OK();
    int64_t completed_request_bytes = 0;
    int64_t completed_read_bytes = 0;
    for (size_t s = 0; s < num_segs; ++s) {
        if (!seg_status[s].ok()) {
            if (first_error.ok()) {
                first_error = seg_status[s];
            }
            continue;
        }
        completed_read_bytes += cast_set<int64_t>(segs[s].len);
        for (size_t i = segs[s].begin; i < segs[s].end; ++i) {
            completed_request_bytes += cast_set<int64_t>(sorted[i].len);
        }
    }
    if (!first_error.ok()) {
        // Record only what actually completed so the logical counters stay
        // truthful for the failed batch; ranges/rounds reflect what was issued.
        _record_read_stats(completed_request_bytes, completed_read_bytes,
                           cast_set<int64_t>(num_segs),
                           cast_set<int64_t>(_compute_num_waves(num_segs)));
        return first_error;
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
    if (_direct_remote_io) {
        // No CachedRemoteFileReader below us: every byte this reader fetched was a
        // direct remote GET, so account it as physical remote IO here.
        stats->inverted_index_remote_physical_read_bytes += read_bytes;
    }
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
    // Delegate to the canonical field list so per-wave private stats can never
    // silently drop fields the general layer adds (e.g. write_cache_io_timer,
    // remote_only_on_miss_*), which a hand-rolled copy here used to do.
    dst->merge_from(src);
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
