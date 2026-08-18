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
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec/pipeline/dependency.h"

namespace doris {

class RuntimeState;
class PaimonWriterMemoryLease;

/// Operator-scoped admission controller for Paimon writer memory.
///
/// A writer receives its complete Paimon-page budget plus Arrow headroom before
/// it enters the synchronous JNI writer. This avoids page-level resource
/// cycles: an admitted writer never waits for another writer while already
/// holding part of its Paimon pool.
class PaimonSinkMemoryAllocator : public std::enable_shared_from_this<PaimonSinkMemoryAllocator> {
public:
    static Status create(RuntimeState* state,
                         std::shared_ptr<PaimonSinkMemoryAllocator>* allocator);

    Status register_writer(const DependencySPtr& dependency,
                           std::shared_ptr<PaimonWriterMemoryLease>* lease);

    /// Prevent new writers from entering and wake current waiters with error.
    void poison(const Status& status);

private:
    friend class PaimonWriterMemoryLease;

    struct Waiter {
        uint64_t writer_id;
        std::weak_ptr<PaimonWriterMemoryLease> lease;
    };

    PaimonSinkMemoryAllocator(int64_t total_budget, int64_t writer_budget,
                              int64_t page_memory_limit)
            : _total_budget(total_budget),
              _writer_budget(writer_budget),
              _page_memory_limit(page_memory_limit) {}

    Status _check_lease(uint64_t writer_id) const;
    void _release_lease(uint64_t writer_id);

    const int64_t _total_budget;
    const int64_t _writer_budget;
    const int64_t _page_memory_limit;

    mutable std::mutex _mutex;
    uint64_t _next_writer_id = 1;
    // Logical admission budget, not a Doris MemTracker reservation. Actual pages are still
    // allocated and accounted by PaimonJniMemoryManager after the lease is granted.
    int64_t _leased_bytes = 0;
    Status _status;
    std::unordered_map<uint64_t, std::weak_ptr<PaimonWriterMemoryLease>> _active_leases;
    std::deque<Waiter> _waiters;
};

/// One local-state writer's claim on the shared operator memory budget.
class PaimonWriterMemoryLease {
public:
    ~PaimonWriterMemoryLease();

    int64_t memory_limit() const { return _memory_limit; }
    bool granted() const { return _granted.load(std::memory_order_acquire); }
    Status check_ready() const;

    /// Idempotently return this writer's complete lease to the operator pool.
    void release();
    void poison(const Status& status);

private:
    friend class PaimonSinkMemoryAllocator;

    PaimonWriterMemoryLease(std::shared_ptr<PaimonSinkMemoryAllocator> allocator,
                            uint64_t writer_id, int64_t memory_limit, DependencySPtr dependency)
            : _allocator(std::move(allocator)),
              _writer_id(writer_id),
              _memory_limit(memory_limit),
              _dependency(std::move(dependency)) {}

    void _grant() { _granted.store(true, std::memory_order_release); }

    std::shared_ptr<PaimonSinkMemoryAllocator> _allocator;
    const uint64_t _writer_id;
    const int64_t _memory_limit;
    DependencySPtr _dependency;
    std::atomic<bool> _granted = false;
    std::atomic<bool> _released = false;
};

} // namespace doris
