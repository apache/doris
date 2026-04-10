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

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <utility>

#include "exec/partitioner/partitioner.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"

namespace doris {

// Default spill partitioner for initial partitioning (level-0). Repartition
// paths may use different channel-id policies (e.g. raw-hash mode).
using SpillPartitionerType = Crc32HashPartitioner<SpillPartitionChannelIds>;

// Repartition partitioner: keeps raw hash (no final modulo) so SpillRepartitioner
// can apply level-aware hash mixing and channel mapping.
using SpillRePartitionerType = Crc32HashPartitioner<SpillRePartitionChannelIds>;

struct SpillContext {
    std::atomic_int running_tasks_count;
    TUniqueId query_id;
    std::function<void(SpillContext*)> all_tasks_finished_callback;

    SpillContext(int running_tasks_count_, TUniqueId query_id_,
                 std::function<void(SpillContext*)> all_tasks_finished_callback_)
            : running_tasks_count(running_tasks_count_),
              query_id(std::move(query_id_)),
              all_tasks_finished_callback(std::move(all_tasks_finished_callback_)) {}

    ~SpillContext() {
        if (running_tasks_count.load() != 0) {
            LOG(WARNING) << "Query: " << print_id(query_id)
                         << " not all spill tasks finished, remaining tasks: "
                         << running_tasks_count.load();
        }
    }

    void on_task_finished() {
        auto count = running_tasks_count.fetch_sub(1);
        if (count == 1) {
            all_tasks_finished_callback(this);
        }
    }
};

// helper to execute a spill function synchronously.  The old code used
// SpillRunnable/SpillSinkRunnable/SpillRecoverRunnable wrappers to track
// counters and optionally notify a SpillContext.  Since spill operations are
// now performed synchronously and external code already maintains any
// necessary counters, those wrappers are no longer necessary.  We keep a
// small utility to run the provided callbacks and forward cancellation.
inline Status run_spill_task(RuntimeState* state, std::function<Status()> exec_func,
                             std::function<Status()> fin_cb = {}) {
    RETURN_IF_ERROR(exec_func());
    if (fin_cb) {
        RETURN_IF_ERROR(fin_cb());
    }
    return Status::OK();
}

template <bool accumulating>
inline void update_profile_from_inner_profile(const std::string& name,
                                              RuntimeProfile* runtime_profile,
                                              RuntimeProfile* inner_profile) {
    auto* inner_counter = inner_profile->get_counter(name);
    DCHECK(inner_counter != nullptr) << "inner counter " << name << " not found";
    if (inner_counter == nullptr) [[unlikely]] {
        return;
    }
    auto* counter = runtime_profile->get_counter(name);
    if (counter == nullptr) [[unlikely]] {
        counter = runtime_profile->add_counter(name, inner_counter->type(), "",
                                               inner_counter->level());
    }
    if constexpr (accumulating) {
        // Memory usage should not be accumulated.
        if (counter->type() == TUnit::BYTES) {
            counter->set(inner_counter->value());
        } else {
            counter->update(inner_counter->value());
        }
    } else {
        counter->set(inner_counter->value());
    }
}

} // namespace doris
