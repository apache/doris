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

#include "exec/sink/writer/paimon/paimon_sink_memory_allocator.h"

#include <algorithm>
#include <limits>

#include "common/check.h"
#include "common/config.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_state.h"
#include "util/pretty_printer.h"

namespace doris {

Status PaimonSinkMemoryAllocator::create(RuntimeState* state,
                                         std::shared_ptr<PaimonSinkMemoryAllocator>* allocator) {
    DORIS_CHECK(state != nullptr);
    DORIS_CHECK(allocator != nullptr);
    if (state->query_mem_tracker() == nullptr) {
        return Status::InternalError(
                "Paimon sink cannot size its shared memory without a query tracker");
    }

    const int64_t writer_count = std::max<int64_t>(1, state->num_local_sink());
    const int64_t configured_page_limit = config::paimon_jni_writer_memory_pool_limit_bytes;
    const int64_t arrow_headroom = config::paimon_jni_writer_arrow_memory_limit_bytes;
    const int64_t configured_writer_budget =
            configured_page_limit > std::numeric_limits<int64_t>::max() - arrow_headroom
                    ? std::numeric_limits<int64_t>::max()
                    : configured_page_limit + arrow_headroom;
    const int64_t configured_total_budget =
            configured_writer_budget > std::numeric_limits<int64_t>::max() / writer_count
                    ? std::numeric_limits<int64_t>::max()
                    : configured_writer_budget * writer_count;
    const int64_t query_limit = state->query_mem_tracker()->limit();
    const int64_t total_budget = query_limit > 0 ? std::min(query_limit, configured_total_budget)
                                                 : configured_total_budget;
    const int64_t writer_budget = std::min(configured_writer_budget, total_budget);
    const int64_t page_memory_limit = writer_budget - arrow_headroom;
    if (page_memory_limit <= 0) {
        return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                "Paimon sink has insufficient shared memory: query_limit={}, writer_count={}, "
                "writer_budget={}, arrow_headroom={}",
                PrettyPrinter::print_bytes(query_limit), writer_count,
                PrettyPrinter::print_bytes(writer_budget),
                PrettyPrinter::print_bytes(arrow_headroom));
    }

    *allocator = std::shared_ptr<PaimonSinkMemoryAllocator>(
            new PaimonSinkMemoryAllocator(total_budget, writer_budget, page_memory_limit));
    return Status::OK();
}

Status PaimonSinkMemoryAllocator::register_writer(const DependencySPtr& dependency,
                                                  std::shared_ptr<PaimonWriterMemoryLease>* lease) {
    DORIS_CHECK(dependency != nullptr);
    DORIS_CHECK(lease != nullptr);

    bool granted = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_status.ok()) {
            return _status;
        }

        const uint64_t writer_id = _next_writer_id++;
        auto new_lease = std::shared_ptr<PaimonWriterMemoryLease>(new PaimonWriterMemoryLease(
                shared_from_this(), writer_id, _page_memory_limit, dependency));
        if (_leased_bytes <= _total_budget - _writer_budget) {
            _leased_bytes += _writer_budget;
            _active_leases.emplace(writer_id, new_lease);
            new_lease->_grant();
            granted = true;
        } else {
            dependency->block();
            _waiters.push_back({writer_id, new_lease});
        }
        *lease = std::move(new_lease);
    }

    if (granted) {
        dependency->set_ready();
    }
    return Status::OK();
}

Status PaimonSinkMemoryAllocator::_check_lease(uint64_t writer_id) const {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_status.ok()) {
        return _status;
    }
    if (!_active_leases.contains(writer_id)) {
        return Status::InternalError("Paimon writer memory lease {} is not active", writer_id);
    }
    return Status::OK();
}

void PaimonSinkMemoryAllocator::_release_lease(uint64_t writer_id) {
    std::vector<DependencySPtr> ready_dependencies;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto active_it = _active_leases.find(writer_id);
        if (active_it != _active_leases.end()) {
            _active_leases.erase(active_it);
            _leased_bytes -= _writer_budget;
        } else {
            std::erase_if(_waiters,
                          [&](const Waiter& waiter) { return waiter.writer_id == writer_id; });
        }

        while (_status.ok() && !_waiters.empty() &&
               _leased_bytes <= _total_budget - _writer_budget) {
            Waiter waiter = _waiters.front();
            _waiters.pop_front();
            auto next_lease = waiter.lease.lock();
            if (next_lease == nullptr || next_lease->_released.load(std::memory_order_acquire)) {
                continue;
            }
            _leased_bytes += _writer_budget;
            _active_leases.emplace(waiter.writer_id, next_lease);
            next_lease->_grant();
            ready_dependencies.push_back(next_lease->_dependency);
        }
    }

    for (const auto& dependency : ready_dependencies) {
        dependency->set_ready();
    }
}

void PaimonSinkMemoryAllocator::poison(const Status& status) {
    DORIS_CHECK(!status.ok());
    std::vector<DependencySPtr> ready_dependencies;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_status.ok()) {
            return;
        }
        _status = status;
        for (const auto& waiter : _waiters) {
            if (auto lease = waiter.lease.lock()) {
                ready_dependencies.push_back(lease->_dependency);
            }
        }
        _waiters.clear();
    }
    for (const auto& dependency : ready_dependencies) {
        dependency->set_ready();
    }
}

PaimonWriterMemoryLease::~PaimonWriterMemoryLease() {
    release();
}

Status PaimonWriterMemoryLease::check_ready() const {
    RETURN_IF_ERROR(_allocator->_check_lease(_writer_id));
    if (!granted()) {
        return Status::InternalError("Paimon writer memory lease is not ready");
    }
    return Status::OK();
}

void PaimonWriterMemoryLease::release() {
    if (!_released.exchange(true, std::memory_order_acq_rel)) {
        _allocator->_release_lease(_writer_id);
    }
}

void PaimonWriterMemoryLease::poison(const Status& status) {
    _allocator->poison(status);
}

} // namespace doris
