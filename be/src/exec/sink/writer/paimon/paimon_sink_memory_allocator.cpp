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
#include "runtime/workload_management/task_controller.h"
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

Status PaimonSinkMemoryAllocator::create_lease(std::unique_ptr<PaimonWriterMemoryLease>* lease) {
    DORIS_CHECK(lease != nullptr);

    std::lock_guard<std::mutex> lock(_mutex);
    if (!_status.ok()) {
        return _status;
    }

    const uint64_t writer_id = _next_writer_id++;
    auto new_lease = std::unique_ptr<PaimonWriterMemoryLease>(
            new PaimonWriterMemoryLease(shared_from_this(), writer_id, _page_memory_limit));
    *lease = std::move(new_lease);
    return Status::OK();
}

Status PaimonSinkMemoryAllocator::_acquire_lease(PaimonWriterMemoryLease* lease,
                                                 TaskController* task_controller) {
    DORIS_CHECK(lease != nullptr);
    DORIS_CHECK(task_controller != nullptr);
    std::unique_lock<std::mutex> lock(_mutex);
    if (lease->_acquired) {
        return Status::OK();
    }
    if (!lease->_waiting) {
        lease->_waiting = true;
        _waiters.push_back(lease->_writer_id);
    }

    while (true) {
        if (!_status.ok()) {
            std::erase(_waiters, lease->_writer_id);
            lease->_waiting = false;
            return _status;
        }
        if (task_controller->is_cancelled()) {
            std::erase(_waiters, lease->_writer_id);
            lease->_waiting = false;
            _cv.notify_all();
            return Status::Cancelled(
                    "Paimon writer memory allocation stopped because the query was cancelled");
        }
        if (lease->_acquired) {
            return Status::OK();
        }

        if (!_waiters.empty() && _waiters.front() == lease->_writer_id &&
            _leased_bytes <= _total_budget - _writer_budget) {
            _waiters.pop_front();
            lease->_waiting = false;
            lease->_acquired = true;
            _leased_bytes += _writer_budget;
            // Wake another callback of the same writer, if Java issued concurrent page requests.
            _cv.notify_all();
            return Status::OK();
        }
        _cv.wait(lock);
    }
}

void PaimonSinkMemoryAllocator::_release_lease(PaimonWriterMemoryLease* lease) {
    DORIS_CHECK(lease != nullptr);
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (lease->_acquired) {
            _leased_bytes -= _writer_budget;
            lease->_acquired = false;
        }
        if (lease->_waiting) {
            std::erase(_waiters, lease->_writer_id);
            lease->_waiting = false;
        }
    }
    _cv.notify_all();
}

void PaimonSinkMemoryAllocator::poison(const Status& status) {
    DORIS_CHECK(!status.ok());
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_status.ok()) {
            return;
        }
        _status = status;
    }
    _cv.notify_all();
}

PaimonWriterMemoryLease::~PaimonWriterMemoryLease() {
    _allocator->_release_lease(this);
}

Status PaimonWriterMemoryLease::acquire(TaskController* task_controller) {
    return _allocator->_acquire_lease(this, task_controller);
}

void PaimonWriterMemoryLease::poison(const Status& status) {
    _allocator->poison(status);
}

} // namespace doris
