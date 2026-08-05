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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "common/status.h"
#include "util/slice.h"

namespace doris {

class MemTrackerLimiter;

namespace segment_v2 {

struct PageSliceDescriptor {
    uint32_t page_index = 0;
    uint64_t page_offset = 0;
    uint32_t page_size = 0;
    size_t buffer_offset = 0;
};

struct CompleteBlockSlice {
    uint64_t block_offset = 0;
    size_t valid_size = 0;
    size_t buffer_offset = 0;
    std::vector<uint32_t> source_page_indexes;
};

struct PageFetchRangeSpec {
    uint64_t offset = 0;
    size_t size = 0;
    size_t requested_page_bytes = 0;
    size_t coalesced_gap_bytes = 0;
    size_t block_fill_bytes = 0;
    std::vector<PageSliceDescriptor> pages;
    std::vector<CompleteBlockSlice> complete_blocks;
};

struct RangeReadStats {
    size_t cache_or_inflight_bytes = 0;
    size_t remote_bytes = 0;
    int64_t remote_io_time_ns = 0;
    size_t self_heal_count = 0;
};

struct PagePrefetchBudgetLimits {
    size_t max_ranges = 1;
    size_t max_bytes = 1;
};

enum class PagePrefetchRejectReason : uint8_t {
    NONE,
    DISABLED,
    QUERY_CANCELLED,
    QUERY_RANGE_LIMIT,
    QUERY_BYTE_LIMIT,
    GLOBAL_RANGE_LIMIT,
    GLOBAL_BYTE_LIMIT,
    ALLOC_FAILED,
    THREAD_POOL_REJECTED,
    SHUTTING_DOWN,
};

class PrefetchRange;
class PagePrefetchReservation;

class PagePrefetchGlobalBudget {
public:
    explicit PagePrefetchGlobalBudget(PagePrefetchBudgetLimits limits);

    size_t inflight_ranges() const { return _inflight_ranges.load(std::memory_order_relaxed); }
    size_t resident_bytes() const { return _resident_bytes.load(std::memory_order_relaxed); }

private:
    friend class PagePrefetchReservation;

    PagePrefetchRejectReason _try_reserve(size_t bytes, bool is_range);
    void _release(size_t bytes, bool release_range);

    const PagePrefetchBudgetLimits _limits;
    std::atomic<size_t> _resident_bytes {0};
    std::atomic<size_t> _inflight_ranges {0};
};

class PagePrefetchQueryContext {
public:
    explicit PagePrefetchQueryContext(PagePrefetchBudgetLimits limits);

    void register_range(const std::shared_ptr<PrefetchRange>& range);
    void cancel();
    bool cancelled() const { return _cancelled.load(std::memory_order_acquire); }

    size_t inflight_ranges() const { return _inflight_ranges.load(std::memory_order_relaxed); }
    size_t resident_bytes() const { return _resident_bytes.load(std::memory_order_relaxed); }

private:
    friend class PagePrefetchReservation;

    PagePrefetchRejectReason _try_reserve(size_t bytes, bool is_range);
    void _release(size_t bytes, bool release_range);

    const PagePrefetchBudgetLimits _limits;
    std::atomic<size_t> _resident_bytes {0};
    std::atomic<size_t> _inflight_ranges {0};
    std::atomic<bool> _cancelled {false};
    std::mutex _ranges_mutex;
    std::vector<std::weak_ptr<PrefetchRange>> _ranges;
};

class PagePrefetchReservation {
public:
    PagePrefetchReservation(const PagePrefetchReservation&) = delete;
    PagePrefetchReservation& operator=(const PagePrefetchReservation&) = delete;
    PagePrefetchReservation(PagePrefetchReservation&& other) noexcept;
    PagePrefetchReservation& operator=(PagePrefetchReservation&& other) noexcept;
    ~PagePrefetchReservation();

    static std::optional<PagePrefetchReservation> try_reserve_range(
            std::shared_ptr<PagePrefetchQueryContext> query,
            std::shared_ptr<PagePrefetchGlobalBudget> global_budget, size_t bytes,
            PagePrefetchRejectReason* reject_reason);
    static std::optional<PagePrefetchReservation> try_reserve_writeback(
            std::shared_ptr<PagePrefetchQueryContext> query,
            std::shared_ptr<PagePrefetchGlobalBudget> global_budget, size_t bytes,
            PagePrefetchRejectReason* reject_reason);

    bool valid() const { return _query_reserved && _global_reserved; }
    size_t bytes() const { return _bytes; }
    void release_range_slot();

private:
    PagePrefetchReservation(std::shared_ptr<PagePrefetchQueryContext> query,
                            std::shared_ptr<PagePrefetchGlobalBudget> global_budget, size_t bytes,
                            bool is_range);

    static std::optional<PagePrefetchReservation> _try_reserve(
            std::shared_ptr<PagePrefetchQueryContext> query,
            std::shared_ptr<PagePrefetchGlobalBudget> global_budget, size_t bytes, bool is_range,
            PagePrefetchRejectReason* reject_reason);
    void _reset();

    std::shared_ptr<PagePrefetchQueryContext> _query;
    std::shared_ptr<PagePrefetchGlobalBudget> _global_budget;
    size_t _bytes = 0;
    bool _is_range = false;
    bool _range_slot_held = false;
    bool _query_reserved = false;
    bool _global_reserved = false;
};

class PagePrefetchBuffer {
public:
    ~PagePrefetchBuffer();

    PagePrefetchBuffer(const PagePrefetchBuffer&) = delete;
    PagePrefetchBuffer& operator=(const PagePrefetchBuffer&) = delete;

    static Status create(size_t size, std::shared_ptr<MemTrackerLimiter> tracker,
                         PagePrefetchReservation reservation,
                         std::shared_ptr<PagePrefetchBuffer>* buffer);

    char* data() { return _data; }
    const char* data() const { return _data; }
    size_t size() const { return _size; }

private:
    friend class PrefetchRange;

    PagePrefetchBuffer(size_t size, std::shared_ptr<MemTrackerLimiter> tracker,
                       PagePrefetchReservation reservation);
    void _release_range_slot() { _reservation.release_range_slot(); }

    char* _data = nullptr;
    size_t _size = 0;
    std::shared_ptr<MemTrackerLimiter> _tracker;
    PagePrefetchReservation _reservation;
};

class PrefetchRange {
public:
    enum class State : uint8_t {
        CREATED,
        QUEUED,
        RUNNING,
        READY,
        FAILED,
        CANCELLED,
        REJECTED,
    };

    PrefetchRange(PageFetchRangeSpec spec, std::shared_ptr<PagePrefetchBuffer> buffer);

    void mark_queued();
    bool mark_running();
    void publish_ready(RangeReadStats read_stats);
    void publish_failed(Status status, RangeReadStats read_stats);
    void publish_cancelled(RangeReadStats read_stats = {});
    void mark_rejected(Status status);

    Status wait_for_consume();
    void request_cancel();
    State state() const;
    bool cancel_requested() const;
    Slice page_slice(size_t descriptor_index) const;
    const PageFetchRangeSpec& spec() const { return _spec; }
    std::shared_ptr<PagePrefetchBuffer> buffer() const { return _buffer; }
    RangeReadStats read_stats() const;
    bool take_read_stats_once(RangeReadStats* read_stats);

private:
    static bool _is_terminal(State state);
    void _publish_from_running(State state, Status status, RangeReadStats read_stats);

    const PageFetchRangeSpec _spec;
    const std::shared_ptr<PagePrefetchBuffer> _buffer;
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    State _state = State::CREATED;
    Status _status;
    RangeReadStats _read_stats;
    bool _cancel_requested = false;
    bool _stats_merged = false;
};

} // namespace segment_v2
} // namespace doris
