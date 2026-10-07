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

#include <cstdint>
#include <memory>
#include <string>

#include "common/atomic_shared_ptr.h"

namespace doris {

class Status;

// Limit hierarchy, not an aggregation of malloc counters. Each node keeps its
// existing accounting: task counters, WG snapshots/growth, or process RSS/reserved.
class MemoryLimit {
public:
    enum class Scope { TASK = 1, WORKLOAD_GROUP = 2, PROCESS = 4 };
    enum class CheckScope {
        NONE = 0,
        CHECK_TASK = 1,
        CHECK_WORKLOAD_GROUP = 2,
        CHECK_TASK_AND_WORKLOAD_GROUP = 3,
        CHECK_PROCESS = 4,
        CHECK_TASK_AND_PROCESS = 5,
        CHECK_WORKLOAD_GROUP_AND_PROCESS = 6,
        CHECK_TASK_AND_WORKLOAD_GROUP_AND_PROCESS = 7,
    };

    virtual ~MemoryLimit() = default;

    Scope memory_limit_scope() const { return _scope; }
    std::shared_ptr<MemoryLimit> memory_limit_parent() const;
    void set_memory_limit_parent(std::shared_ptr<MemoryLimit> parent);

    static const std::shared_ptr<MemoryLimit>& process_memory_limit();

    // Skipping a node's check does not skip checks on its ancestors.
    Status check_memory_limit(
            int64_t bytes,
            CheckScope checker = CheckScope::CHECK_TASK_AND_WORKLOAD_GROUP_AND_PROCESS);
    // Allocation/GC polling needs a verdict without allocating an error message.
    bool exceeds_memory_limit(int64_t bytes, CheckScope checker);

    // Every node accounts the reservation, even if its check is disabled. On
    // failure each successful descendant rolls back its own reservation.
    // A temporary thread limiter can use the task's WG without reparenting a
    // shared cache/global tracker. parent_override applies only to this node.
    Status try_reserve_memory(int64_t bytes, CheckScope checker,
                              MemoryLimit* parent_override = nullptr);

    std::string memory_limit_tree_string() const;

protected:
    MemoryLimit(Scope scope, std::shared_ptr<MemoryLimit> parent);
    virtual bool exceeds_local_memory_limit(int64_t bytes) = 0;
    virtual Status check_local_memory_limit(int64_t bytes) = 0;
    virtual Status reserve_local_memory(int64_t bytes, bool check_limit) = 0;
    virtual void rollback_local_reservation(int64_t bytes) = 0;
    virtual std::string local_memory_limit_string() const = 0;

private:
    const Scope _scope;
    // WG binding can overlap diagnostics.
    atomic_shared_ptr<MemoryLimit> _parent;
};

} // namespace doris
