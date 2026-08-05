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

#include "storage/segment/page_prefetch_io_service.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <type_traits>
#include <utility>

#include "core/allocator.h"
#include "cpp/sync_point.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/remote_scan_cache_write_limiter.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::segment_v2 {
namespace {

using PagePrefetchAllocator = Allocator<false, false, false, DefaultMemoryAllocator, true>;

bool try_add_with_limit(std::atomic<size_t>* current, size_t amount, size_t limit) {
    DORIS_CHECK(current != nullptr);
    DORIS_CHECK(amount > 0);
    size_t observed = current->load(std::memory_order_relaxed);
    while (true) {
        DORIS_CHECK(observed <= limit);
        if (amount > limit - observed) {
            return false;
        }
        if (current->compare_exchange_weak(observed, observed + amount,
                                           std::memory_order_relaxed)) {
            return true;
        }
    }
}

void release_counter(std::atomic<size_t>* current, size_t amount) {
    DORIS_CHECK(current != nullptr);
    DORIS_CHECK(amount > 0);
    const size_t previous = current->fetch_sub(amount, std::memory_order_relaxed);
    DORIS_CHECK(previous >= amount);
}

template <typename T>
bool weak_ptr_has_owner(const std::weak_ptr<T>& pointer) {
    const std::weak_ptr<T> empty;
    return pointer.owner_before(empty) || empty.owner_before(pointer);
}

} // namespace

PagePrefetchGlobalBudget::PagePrefetchGlobalBudget(PagePrefetchBudgetLimits limits)
        : _limits(limits) {
    DORIS_CHECK(_limits.max_ranges > 0);
    DORIS_CHECK(_limits.max_bytes > 0);
}

PagePrefetchRejectReason PagePrefetchGlobalBudget::_try_reserve(size_t bytes, bool is_range) {
    if (is_range && !try_add_with_limit(&_inflight_ranges, 1, _limits.max_ranges)) {
        return PagePrefetchRejectReason::GLOBAL_RANGE_LIMIT;
    }
    if (!try_add_with_limit(&_resident_bytes, bytes, _limits.max_bytes)) {
        if (is_range) {
            release_counter(&_inflight_ranges, 1);
        }
        return PagePrefetchRejectReason::GLOBAL_BYTE_LIMIT;
    }
    return PagePrefetchRejectReason::NONE;
}

void PagePrefetchGlobalBudget::_release(size_t bytes, bool release_range) {
    DORIS_CHECK(bytes > 0 || release_range);
    if (bytes > 0) {
        release_counter(&_resident_bytes, bytes);
    }
    if (release_range) {
        release_counter(&_inflight_ranges, 1);
    }
}

PagePrefetchQueryContext::PagePrefetchQueryContext(PagePrefetchBudgetLimits limits)
        : _query_id(), _query_ctx(), _tracks_runtime_query(false), _limits(limits) {
    DORIS_CHECK(_limits.max_ranges > 0);
    DORIS_CHECK(_limits.max_bytes > 0);
}

PagePrefetchQueryContext::PagePrefetchQueryContext(TUniqueId query_id,
                                                   std::weak_ptr<QueryContext> query_ctx,
                                                   PagePrefetchBudgetLimits limits)
        : _query_id(query_id),
          _query_ctx(std::move(query_ctx)),
          _tracks_runtime_query(weak_ptr_has_owner(_query_ctx)),
          _limits(limits) {
    DORIS_CHECK(_limits.max_ranges > 0);
    DORIS_CHECK(_limits.max_bytes > 0);
}

bool PagePrefetchQueryContext::cancelled() const {
    return _cancelled.load(std::memory_order_acquire) || _runtime_query_cancelled();
}

bool PagePrefetchQueryContext::_runtime_query_cancelled() const {
    if (!_tracks_runtime_query) {
        return false;
    }
    auto query_ctx = _query_ctx.lock();
    return query_ctx == nullptr || query_ctx->is_cancelled();
}

PagePrefetchRejectReason PagePrefetchQueryContext::_try_reserve(size_t bytes, bool is_range) {
    if (cancelled()) {
        return PagePrefetchRejectReason::QUERY_CANCELLED;
    }
    if (is_range && !try_add_with_limit(&_inflight_ranges, 1, _limits.max_ranges)) {
        return PagePrefetchRejectReason::QUERY_RANGE_LIMIT;
    }
    if (!try_add_with_limit(&_resident_bytes, bytes, _limits.max_bytes)) {
        if (is_range) {
            release_counter(&_inflight_ranges, 1);
        }
        return PagePrefetchRejectReason::QUERY_BYTE_LIMIT;
    }
    if (cancelled()) {
        _release(bytes, is_range);
        return PagePrefetchRejectReason::QUERY_CANCELLED;
    }
    return PagePrefetchRejectReason::NONE;
}

void PagePrefetchQueryContext::_release(size_t bytes, bool release_range) {
    DORIS_CHECK(bytes > 0 || release_range);
    if (bytes > 0) {
        release_counter(&_resident_bytes, bytes);
    }
    if (release_range) {
        release_counter(&_inflight_ranges, 1);
    }
}

void PagePrefetchQueryContext::register_range(const std::shared_ptr<PrefetchRange>& range) {
    DORIS_CHECK(range != nullptr);
    bool cancel_range = false;
    {
        std::lock_guard lock(_ranges_mutex);
        _ranges.erase(std::remove_if(_ranges.begin(), _ranges.end(),
                                     [](const auto& weak_range) { return weak_range.expired(); }),
                      _ranges.end());
        if (cancelled()) {
            cancel_range = true;
        } else {
            _ranges.emplace_back(range);
        }
    }
    if (cancel_range) {
        range->request_cancel();
    }
}

void PagePrefetchQueryContext::cancel() {
    std::vector<std::shared_ptr<PrefetchRange>> ranges;
    {
        std::lock_guard lock(_ranges_mutex);
        _cancelled.store(true, std::memory_order_release);
        ranges.reserve(_ranges.size());
        for (auto& weak_range : _ranges) {
            if (auto range = weak_range.lock()) {
                ranges.emplace_back(std::move(range));
            }
        }
        _ranges.clear();
    }
    for (const auto& range : ranges) {
        range->request_cancel();
    }
}

PagePrefetchReservation::PagePrefetchReservation(
        std::shared_ptr<PagePrefetchQueryContext> query,
        std::shared_ptr<PagePrefetchGlobalBudget> global_budget, size_t bytes, bool is_range)
        : _query(std::move(query)),
          _global_budget(std::move(global_budget)),
          _bytes(bytes),
          _is_range(is_range),
          _range_slot_held(is_range) {
    DORIS_CHECK(_query != nullptr);
    DORIS_CHECK(_global_budget != nullptr);
    DORIS_CHECK(_bytes > 0);
}

PagePrefetchReservation::PagePrefetchReservation(PagePrefetchReservation&& other) noexcept
        : _query(std::move(other._query)),
          _global_budget(std::move(other._global_budget)),
          _bytes(std::exchange(other._bytes, 0)),
          _is_range(std::exchange(other._is_range, false)),
          _range_slot_held(std::exchange(other._range_slot_held, false)),
          _query_reserved(std::exchange(other._query_reserved, false)),
          _global_reserved(std::exchange(other._global_reserved, false)) {}

PagePrefetchReservation& PagePrefetchReservation::operator=(
        PagePrefetchReservation&& other) noexcept {
    if (this != &other) {
        _reset();
        _query = std::move(other._query);
        _global_budget = std::move(other._global_budget);
        _bytes = std::exchange(other._bytes, 0);
        _is_range = std::exchange(other._is_range, false);
        _range_slot_held = std::exchange(other._range_slot_held, false);
        _query_reserved = std::exchange(other._query_reserved, false);
        _global_reserved = std::exchange(other._global_reserved, false);
    }
    return *this;
}

PagePrefetchReservation::~PagePrefetchReservation() {
    _reset();
}

std::optional<PagePrefetchReservation> PagePrefetchReservation::try_reserve_range(
        std::shared_ptr<PagePrefetchQueryContext> query,
        std::shared_ptr<PagePrefetchGlobalBudget> global_budget, size_t bytes,
        PagePrefetchRejectReason* reject_reason) {
    return _try_reserve(std::move(query), std::move(global_budget), bytes, true, reject_reason);
}

std::optional<PagePrefetchReservation> PagePrefetchReservation::try_reserve_writeback(
        std::shared_ptr<PagePrefetchQueryContext> query,
        std::shared_ptr<PagePrefetchGlobalBudget> global_budget, size_t bytes,
        PagePrefetchRejectReason* reject_reason) {
    return _try_reserve(std::move(query), std::move(global_budget), bytes, false, reject_reason);
}

std::optional<PagePrefetchReservation> PagePrefetchReservation::_try_reserve(
        std::shared_ptr<PagePrefetchQueryContext> query,
        std::shared_ptr<PagePrefetchGlobalBudget> global_budget, size_t bytes, bool is_range,
        PagePrefetchRejectReason* reject_reason) {
    DORIS_CHECK(query != nullptr);
    DORIS_CHECK(global_budget != nullptr);
    DORIS_CHECK(bytes > 0);
    DORIS_CHECK(reject_reason != nullptr);
    *reject_reason = PagePrefetchRejectReason::NONE;

    PagePrefetchReservation reservation(std::move(query), std::move(global_budget), bytes,
                                        is_range);
    *reject_reason = reservation._query->_try_reserve(bytes, is_range);
    if (*reject_reason != PagePrefetchRejectReason::NONE) {
        return std::nullopt;
    }
    reservation._query_reserved = true;

    *reject_reason = reservation._global_budget->_try_reserve(bytes, is_range);
    if (*reject_reason != PagePrefetchRejectReason::NONE) {
        return std::nullopt;
    }
    reservation._global_reserved = true;

    if (reservation._query->cancelled()) {
        *reject_reason = PagePrefetchRejectReason::QUERY_CANCELLED;
        return std::nullopt;
    }
    return reservation;
}

void PagePrefetchReservation::release_range_slot() {
    DORIS_CHECK(valid());
    DORIS_CHECK(_is_range);
    DORIS_CHECK(_range_slot_held);
    _global_budget->_release(0, true);
    _query->_release(0, true);
    _range_slot_held = false;
}

void PagePrefetchReservation::_reset() {
    if (_global_reserved) {
        _global_budget->_release(_bytes, _range_slot_held);
    }
    if (_query_reserved) {
        _query->_release(_bytes, _range_slot_held);
    }
    _query.reset();
    _global_budget.reset();
    _bytes = 0;
    _is_range = false;
    _range_slot_held = false;
    _query_reserved = false;
    _global_reserved = false;
}

PagePrefetchBuffer::PagePrefetchBuffer(size_t size, std::shared_ptr<MemTrackerLimiter> tracker,
                                       PagePrefetchReservation reservation)
        : _size(size), _tracker(std::move(tracker)), _reservation(std::move(reservation)) {
    DORIS_CHECK(_tracker != nullptr);
    DORIS_CHECK(_size > 0);
    DORIS_CHECK(_reservation.valid());
    DORIS_CHECK(_reservation.bytes() == _size);
    PagePrefetchAllocator allocator;
    _data = reinterpret_cast<char*>(allocator.alloc(_size));
}

PagePrefetchBuffer::~PagePrefetchBuffer() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_tracker);
    PagePrefetchAllocator allocator;
    allocator.free(_data, _size);
}

Status PagePrefetchBuffer::create(size_t size, std::shared_ptr<MemTrackerLimiter> tracker,
                                  PagePrefetchReservation reservation,
                                  std::shared_ptr<PagePrefetchBuffer>* buffer) {
    DORIS_CHECK(size > 0);
    DORIS_CHECK(tracker != nullptr);
    DORIS_CHECK(reservation.valid());
    DORIS_CHECK(reservation.bytes() == size);
    DORIS_CHECK(buffer != nullptr);
    Status injected_status;
    TEST_SYNC_POINT_CALLBACK("PagePrefetchBuffer::create:inject_failure", &injected_status);
    if (!injected_status.ok()) {
        return injected_status;
    }

    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
    try {
        *buffer = std::shared_ptr<PagePrefetchBuffer>(
                new PagePrefetchBuffer(size, std::move(tracker), std::move(reservation)));
    } catch (const std::exception& error) {
        return Status::MemoryAllocFailed("allocate page prefetch buffer failed: {}", error.what());
    }
    return Status::OK();
}

PrefetchRange::PrefetchRange(PageFetchRangeSpec spec, std::shared_ptr<PagePrefetchBuffer> buffer)
        : _spec(std::move(spec)), _buffer(std::move(buffer)) {
    DORIS_CHECK(_buffer != nullptr);
    DORIS_CHECK(_spec.size > 0);
    DORIS_CHECK(_spec.size == _buffer->size());
    DORIS_CHECK(_spec.size <= std::numeric_limits<uint64_t>::max() - _spec.offset);
    DORIS_CHECK(_spec.requested_page_bytes <= _spec.size);
    DORIS_CHECK(_spec.block_fill_bytes <= _spec.size - _spec.requested_page_bytes);
    DORIS_CHECK(_spec.coalesced_gap_bytes ==
                _spec.size - _spec.requested_page_bytes - _spec.block_fill_bytes);
    for (const auto& page : _spec.pages) {
        DORIS_CHECK(page.page_offset >= _spec.offset);
        DORIS_CHECK(page.buffer_offset == page.page_offset - _spec.offset);
        DORIS_CHECK(page.buffer_offset <= _spec.size);
        DORIS_CHECK(page.page_size <= _spec.size - page.buffer_offset);
    }
    for (const auto& block : _spec.complete_blocks) {
        DORIS_CHECK(block.buffer_offset <= _spec.size);
        DORIS_CHECK(block.valid_size <= _spec.size - block.buffer_offset);
    }
}

void PrefetchRange::mark_queued() {
    std::lock_guard lock(_mutex);
    DORIS_CHECK(_state == State::CREATED);
    _state = State::QUEUED;
}

bool PrefetchRange::mark_running() {
    bool cancelled = false;
    {
        std::lock_guard lock(_mutex);
        DORIS_CHECK(_state == State::QUEUED);
        if (_cancel_requested) {
            _state = State::CANCELLED;
            _status = Status::Cancelled("page prefetch range cancelled before execution");
            cancelled = true;
        } else {
            _state = State::RUNNING;
        }
    }
    if (cancelled) {
        _buffer->_release_range_slot();
        _cv.notify_all();
    }
    return !cancelled;
}

void PrefetchRange::publish_ready(RangeReadStats read_stats) {
    _publish_from_running(State::READY, Status::OK(), std::move(read_stats));
}

void PrefetchRange::publish_failed(Status status, RangeReadStats read_stats) {
    DORIS_CHECK(!status.ok());
    _publish_from_running(State::FAILED, std::move(status), std::move(read_stats));
}

void PrefetchRange::publish_cancelled(RangeReadStats read_stats) {
    _publish_from_running(State::CANCELLED, Status::Cancelled("page prefetch range cancelled"),
                          std::move(read_stats));
}

void PrefetchRange::mark_rejected(Status status) {
    DORIS_CHECK(!status.ok());
    {
        std::lock_guard lock(_mutex);
        DORIS_CHECK(_state == State::CREATED || _state == State::QUEUED);
        _state = State::REJECTED;
        _status = std::move(status);
    }
    _buffer->_release_range_slot();
    _cv.notify_all();
}

Status PrefetchRange::wait_for_consume() {
    std::unique_lock lock(_mutex);
    _cv.wait(lock, [this]() { return _is_terminal(_state) || _cancel_requested; });
    if (_cancel_requested && !_is_terminal(_state)) {
        return Status::Cancelled("page prefetch range cancelled while waiting");
    }
    return _status;
}

void PrefetchRange::request_cancel() {
    bool notify = false;
    {
        std::lock_guard lock(_mutex);
        if (!_is_terminal(_state) && !_cancel_requested) {
            _cancel_requested = true;
            notify = true;
        }
    }
    if (notify) {
        _cv.notify_all();
    }
}

PrefetchRange::State PrefetchRange::state() const {
    std::lock_guard lock(_mutex);
    return _state;
}

bool PrefetchRange::cancel_requested() const {
    std::lock_guard lock(_mutex);
    return _cancel_requested;
}

Slice PrefetchRange::page_slice(size_t descriptor_index) const {
    std::lock_guard lock(_mutex);
    DORIS_CHECK(_state == State::READY);
    DORIS_CHECK(descriptor_index < _spec.pages.size());
    const auto& descriptor = _spec.pages[descriptor_index];
    DORIS_CHECK(descriptor.buffer_offset <= _buffer->size());
    DORIS_CHECK(descriptor.page_size <= _buffer->size() - descriptor.buffer_offset);
    return Slice(_buffer->data() + descriptor.buffer_offset, descriptor.page_size);
}

RangeReadStats PrefetchRange::read_stats() const {
    std::lock_guard lock(_mutex);
    DORIS_CHECK(_is_terminal(_state));
    return _read_stats;
}

bool PrefetchRange::take_read_stats_once(RangeReadStats* read_stats) {
    DORIS_CHECK(read_stats != nullptr);
    std::lock_guard lock(_mutex);
    DORIS_CHECK(_is_terminal(_state));
    if (_stats_merged) {
        return false;
    }
    _stats_merged = true;
    *read_stats = _read_stats;
    return true;
}

bool PrefetchRange::_is_terminal(State state) {
    return state == State::READY || state == State::FAILED || state == State::CANCELLED ||
           state == State::REJECTED;
}

void PrefetchRange::_publish_from_running(State state, Status status, RangeReadStats read_stats) {
    DORIS_CHECK(state == State::READY || state == State::FAILED || state == State::CANCELLED);
    {
        std::lock_guard lock(_mutex);
        DORIS_CHECK(_state == State::RUNNING);
        if (_cancel_requested) {
            _state = State::CANCELLED;
            _status = Status::Cancelled("page prefetch range cancelled during execution");
        } else {
            _state = state;
            _status = std::move(status);
        }
        _read_stats = std::move(read_stats);
    }
    _buffer->_release_range_slot();
    _cv.notify_all();
}

PagePrefetchSafeIOContext::PagePrefetchSafeIOContext(const PagePrefetchSafeIOContext& other)
        : io_ctx(other.io_ctx),
          query_id_value(other.query_id_value),
          admission_ctx(other.admission_ctx),
          remote_only_on_miss(other.remote_only_on_miss) {
    _rebind_query_id();
}

PagePrefetchSafeIOContext& PagePrefetchSafeIOContext::operator=(
        const PagePrefetchSafeIOContext& other) {
    if (this != &other) {
        io_ctx = other.io_ctx;
        query_id_value = other.query_id_value;
        admission_ctx = other.admission_ctx;
        remote_only_on_miss = other.remote_only_on_miss;
        _rebind_query_id();
    }
    return *this;
}

PagePrefetchSafeIOContext::PagePrefetchSafeIOContext(PagePrefetchSafeIOContext&& other) noexcept
        : io_ctx(std::move(other.io_ctx)),
          query_id_value(std::move(other.query_id_value)),
          admission_ctx(std::move(other.admission_ctx)),
          remote_only_on_miss(other.remote_only_on_miss) {
    _rebind_query_id();
    other.io_ctx.query_id = nullptr;
}

PagePrefetchSafeIOContext& PagePrefetchSafeIOContext::operator=(
        PagePrefetchSafeIOContext&& other) noexcept {
    if (this != &other) {
        io_ctx = std::move(other.io_ctx);
        query_id_value = std::move(other.query_id_value);
        admission_ctx = std::move(other.admission_ctx);
        remote_only_on_miss = other.remote_only_on_miss;
        _rebind_query_id();
        other.io_ctx.query_id = nullptr;
    }
    return *this;
}

PagePrefetchSafeIOContext PagePrefetchSafeIOContext::from_query_thread(const io::IOContext& source,
                                                                       int64_t tablet_id) {
    PagePrefetchSafeIOContext result;
    result.io_ctx = source;
    if (source.query_id != nullptr) {
        result.query_id_value = *source.query_id;
    }

    io::CacheContext cache_context(&source);
    result.admission_ctx = {
            .query_id = cache_context.query_id,
            .cache_type = cache_context.cache_type,
            .expiration_time = cache_context.expiration_time,
            .tablet_id = tablet_id,
            .is_warmup = false,
    };
    result.remote_only_on_miss =
            source.file_cache_miss_policy == io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS ||
            (source.remote_scan_cache_write_limiter != nullptr &&
             source.remote_scan_cache_write_limiter->remote_only_on_miss());

    result.io_ctx.file_cache_stats = nullptr;
    result.io_ctx.file_reader_stats = nullptr;
    result.io_ctx.remote_scan_cache_write_limiter = nullptr;
    result.io_ctx.is_index_data = false;
    result.io_ctx.is_inverted_index = false;
    result.io_ctx.is_dryrun = false;
    result.io_ctx.is_warmup = false;
    result.io_ctx.condition_cache_filtered_rows = 0;
    result.io_ctx.predicate_filtered_rows = 0;
    result.io_ctx.bypass_peer_read = true;
    result.io_ctx.cache_align_mode_override = io::CacheAlignMode::UNALIGNED;
    result.io_ctx.cache_write_mode_override = io::CacheWriteMode::NO_WRITE;
    if (result.remote_only_on_miss) {
        result.io_ctx.file_cache_miss_policy = io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS;
    }
    result._rebind_query_id();
    return result;
}

void PagePrefetchSafeIOContext::_rebind_query_id() {
    io_ctx.query_id = query_id_value.has_value() ? &*query_id_value : nullptr;
}

PagePrefetchIOService::PagePrefetchIOService(ThreadPool* pool, PagePrefetchIOServiceOptions options)
        : _pool(pool),
          _options(options),
          _global_budget(std::make_shared<PagePrefetchGlobalBudget>(options.global_limits)),
          _mem_tracker(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE,
                                                        "PagePrefetchBuffer")) {
    DORIS_CHECK(_pool != nullptr);
    DORIS_CHECK(_options.query_limits.max_ranges > 0);
    DORIS_CHECK(_options.query_limits.max_bytes > 0);
}

PagePrefetchIOService::~PagePrefetchIOService() {
    shutdown();
}

std::shared_ptr<PagePrefetchQueryContext> PagePrefetchIOService::get_or_create_query_context(
        const TUniqueId& query_id, std::weak_ptr<QueryContext> query_ctx) {
    DORIS_CHECK(weak_ptr_has_owner(query_ctx));
    std::lock_guard lock(_query_contexts_mutex);
    auto existing = _query_contexts.find(query_id);
    if (existing != _query_contexts.end()) {
        if (auto context = existing->second.lock()) {
            return context;
        }
        _query_contexts.erase(existing);
    }

    auto context = std::make_shared<PagePrefetchQueryContext>(query_id, std::move(query_ctx),
                                                              _options.query_limits);
    if (accepting()) {
        _query_contexts.emplace(query_id, context);
    } else {
        context->cancel();
    }
    return context;
}

PagePrefetchSubmitResult PagePrefetchIOService::try_submit(
        PageFetchRangeSpec spec, std::shared_ptr<io::CachedRemoteFileReader> reader,
        PagePrefetchSafeIOContext io_ctx, std::shared_ptr<PagePrefetchQueryContext> query_ctx) {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(query_ctx != nullptr);
    DORIS_CHECK(spec.size > 0);
    DORIS_CHECK(spec.offset < reader->size());
    DORIS_CHECK(spec.size <= reader->size() - spec.offset);

    PagePrefetchSubmitResult result;
    if (!_begin_submit()) {
        result.reject_reason = PagePrefetchRejectReason::SHUTTING_DOWN;
        return result;
    }
    Defer finish_submit {[this]() { _finish_submit(); }};
    if (query_ctx->cancelled()) {
        result.reject_reason = PagePrefetchRejectReason::QUERY_CANCELLED;
        return result;
    }

    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto reservation = PagePrefetchReservation::try_reserve_range(query_ctx, _global_budget,
                                                                  spec.size, &reject_reason);
    if (!reservation.has_value()) {
        result.reject_reason = reject_reason;
        return result;
    }

    std::shared_ptr<PagePrefetchBuffer> buffer;
    Status allocation_status =
            PagePrefetchBuffer::create(spec.size, _mem_tracker, std::move(*reservation), &buffer);
    if (!allocation_status.ok()) {
        result.reject_reason = PagePrefetchRejectReason::ALLOC_FAILED;
        return result;
    }

    auto range = std::make_shared<PrefetchRange>(std::move(spec), std::move(buffer));
    _register_query_context(query_ctx);
    query_ctx->register_range(range);
    range->mark_queued();
    if (!_reserve_outstanding_task()) {
        range->mark_rejected(Status::Cancelled("page prefetch service is shutting down"));
        result.reject_reason = PagePrefetchRejectReason::SHUTTING_DOWN;
        return result;
    }

    Status submit_status = _pool->submit_func([this, range, reader = std::move(reader),
                                               io_ctx = std::move(io_ctx), query_ctx]() mutable {
        Defer finish_task {[this]() { _finish_outstanding_task(); }};
        _execute_range(range, reader, std::move(io_ctx), query_ctx);
    });
    if (!submit_status.ok()) {
        range->mark_rejected(std::move(submit_status));
        _finish_outstanding_task();
        result.reject_reason = PagePrefetchRejectReason::THREAD_POOL_REJECTED;
        return result;
    }

    result.range = std::move(range);
    return result;
}

void PagePrefetchIOService::shutdown() {
    {
        std::unique_lock lock(_lifecycle_mutex);
        _accepting.store(false, std::memory_order_release);
        _lifecycle_cv.wait(lock, [this]() { return _active_submitters == 0; });
    }

    std::vector<std::shared_ptr<PagePrefetchQueryContext>> query_contexts;
    {
        std::lock_guard lock(_query_contexts_mutex);
        query_contexts.reserve(_query_contexts.size());
        for (auto& [query_id, weak_context] : _query_contexts) {
            static_cast<void>(query_id);
            if (auto context = weak_context.lock()) {
                query_contexts.emplace_back(std::move(context));
            }
        }
        _query_contexts.clear();
    }
    for (const auto& query_context : query_contexts) {
        query_context->cancel();
    }

    std::unique_lock lock(_lifecycle_mutex);
    _lifecycle_cv.wait(lock, [this]() { return _outstanding_tasks == 0; });
}

size_t PagePrefetchIOService::outstanding_tasks() const {
    std::lock_guard lock(_lifecycle_mutex);
    return _outstanding_tasks;
}

bool PagePrefetchIOService::_begin_submit() {
    std::lock_guard lock(_lifecycle_mutex);
    if (!_accepting.load(std::memory_order_acquire)) {
        return false;
    }
    ++_active_submitters;
    return true;
}

void PagePrefetchIOService::_finish_submit() {
    std::lock_guard lock(_lifecycle_mutex);
    DORIS_CHECK(_active_submitters > 0);
    --_active_submitters;
    if (_active_submitters == 0) {
        _lifecycle_cv.notify_all();
    }
}

bool PagePrefetchIOService::_reserve_outstanding_task() {
    std::lock_guard lock(_lifecycle_mutex);
    if (!_accepting.load(std::memory_order_acquire)) {
        return false;
    }
    ++_outstanding_tasks;
    return true;
}

void PagePrefetchIOService::_finish_outstanding_task() {
    std::lock_guard lock(_lifecycle_mutex);
    DORIS_CHECK(_outstanding_tasks > 0);
    --_outstanding_tasks;
    if (_outstanding_tasks == 0) {
        _lifecycle_cv.notify_all();
    }
}

void PagePrefetchIOService::_register_query_context(
        const std::shared_ptr<PagePrefetchQueryContext>& query_ctx) {
    DORIS_CHECK(query_ctx != nullptr);
    std::lock_guard lock(_query_contexts_mutex);
    auto [iterator, inserted] = _query_contexts.emplace(query_ctx->query_id(), query_ctx);
    if (!inserted) {
        auto existing = iterator->second.lock();
        DORIS_CHECK(existing == nullptr || existing == query_ctx);
        iterator->second = query_ctx;
    }
}

void PagePrefetchIOService::_execute_range(
        const std::shared_ptr<PrefetchRange>& range,
        const std::shared_ptr<io::CachedRemoteFileReader>& reader, PagePrefetchSafeIOContext io_ctx,
        const std::shared_ptr<PagePrefetchQueryContext>& query_ctx) {
    DORIS_CHECK(range != nullptr);
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(query_ctx != nullptr);
    if (!range->mark_running()) {
        return;
    }
    if (!accepting() || query_ctx->cancelled()) {
        range->publish_cancelled();
        return;
    }

    io::FileCacheStatistics file_cache_stats;
    io::FileReaderStats file_reader_stats;
    io_ctx.io_ctx.file_cache_stats = &file_cache_stats;
    io_ctx.io_ctx.file_reader_stats = &file_reader_stats;
    io_ctx.io_ctx.remote_scan_cache_write_limiter = nullptr;
    size_t bytes_read = 0;
    auto buffer = range->buffer();
    Status status = reader->read_at(range->spec().offset, Slice(buffer->data(), buffer->size()),
                                    &bytes_read, &io_ctx.io_ctx);
    if (status.ok()) {
        DORIS_CHECK(bytes_read == buffer->size());
    }
    DORIS_CHECK(file_cache_stats.bytes_read_from_local >= 0);
    DORIS_CHECK(file_cache_stats.bytes_read_from_remote >= 0);
    RangeReadStats read_stats {
            .cache_or_inflight_bytes = static_cast<size_t>(file_cache_stats.bytes_read_from_local),
            .remote_bytes = static_cast<size_t>(file_cache_stats.bytes_read_from_remote),
            .remote_io_time_ns = file_cache_stats.remote_io_timer,
            .self_heal_count = 0,
    };

    if (!accepting() || query_ctx->cancelled()) {
        range->publish_cancelled(std::move(read_stats));
    } else if (!status.ok()) {
        range->publish_failed(std::move(status), std::move(read_stats));
    } else {
        range->publish_ready(std::move(read_stats));
    }
}

static_assert(std::is_nothrow_move_constructible_v<PagePrefetchReservation>);
static_assert(std::is_nothrow_move_assignable_v<PagePrefetchReservation>);

} // namespace doris::segment_v2
