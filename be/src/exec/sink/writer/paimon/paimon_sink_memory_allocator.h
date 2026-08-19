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

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <utility>

#include "common/status.h"

namespace doris {

class RuntimeState;
class TaskController;
class PaimonWriterMemoryLease;

/// Operator-scoped admission controller for Paimon writer memory.
///
/// The first JNI page allocation synchronously acquires the writer's complete
/// Paimon-page budget plus Arrow headroom. A writer that cannot acquire the
/// complete budget waits without holding any Paimon native pages, avoiding
/// page-level resource cycles between concurrent writers.
class PaimonSinkMemoryAllocator : public std::enable_shared_from_this<PaimonSinkMemoryAllocator> {
public:
    static Status create(RuntimeState* state,
                         std::shared_ptr<PaimonSinkMemoryAllocator>* allocator);

    Status create_lease(std::unique_ptr<PaimonWriterMemoryLease>* lease);

    /// Prevent new writers from entering and wake current waiters with error.
    void poison(const Status& status);

private:
    friend class PaimonWriterMemoryLease;

    PaimonSinkMemoryAllocator(int64_t total_budget, int64_t writer_budget,
                              int64_t page_memory_limit)
            : _total_budget(total_budget),
              _writer_budget(writer_budget),
              _page_memory_limit(page_memory_limit) {}

    Status _acquire_lease(PaimonWriterMemoryLease* lease, TaskController* task_controller);
    void _release_lease(PaimonWriterMemoryLease* lease);

    const int64_t _total_budget;
    const int64_t _writer_budget;
    const int64_t _page_memory_limit;

    std::mutex _mutex;
    std::condition_variable _cv;
    uint64_t _next_writer_id = 1;
    // Logical admission budget, not a Doris MemTracker reservation. Actual pages are still
    // allocated and accounted by PaimonJniMemoryManager after the lease is granted.
    int64_t _leased_bytes = 0;
    Status _status;
    std::deque<uint64_t> _waiters;
};

/// One local-state writer's claim on the shared operator memory budget.
class PaimonWriterMemoryLease {
public:
    ~PaimonWriterMemoryLease();

    int64_t memory_limit() const { return _memory_limit; }
    Status acquire(TaskController* task_controller);

    void poison(const Status& status);

private:
    friend class PaimonSinkMemoryAllocator;

    PaimonWriterMemoryLease(std::shared_ptr<PaimonSinkMemoryAllocator> allocator,
                            uint64_t writer_id, int64_t memory_limit)
            : _allocator(std::move(allocator)),
              _writer_id(writer_id),
              _memory_limit(memory_limit) {}

    std::shared_ptr<PaimonSinkMemoryAllocator> _allocator;
    const uint64_t _writer_id;
    const int64_t _memory_limit;
    // Both fields are protected by the allocator mutex.
    bool _waiting = false;
    bool _acquired = false;
};

} // namespace doris
