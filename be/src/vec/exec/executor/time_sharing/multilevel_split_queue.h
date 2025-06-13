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
#include <array>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include "common/factory_creator.h"
#include "vec/exec/executor/time_sharing/prioritized_split_runner.h"
#include "vec/exec/executor/time_sharing/priority.h"

namespace doris {
namespace vectorized {

struct SplitRunnerComparator {
    bool operator()(const std::shared_ptr<PrioritizedSplitRunner>& a,
                    const std::shared_ptr<PrioritizedSplitRunner>& b) const {
        const auto a_priority = a->priority().level_priority();
        const auto b_priority = b->priority().level_priority();
        if (a_priority != b_priority) {
            return a_priority > b_priority;
        }

        return a->worker_id() > b->worker_id();
    }
};

class MultilevelSplitQueue {
    ENABLE_FACTORY_CREATOR(MultilevelSplitQueue);

public:
    static constexpr std::array<int, 5> LEVEL_THRESHOLD_SECONDS = {0, 1, 10, 60, 300};
    static constexpr int64_t LEVEL_CONTRIBUTION_CAP = 30 * 1000000000LL; // 30 seconds in nanos

    explicit MultilevelSplitQueue(double level_time_multiplier);

    static int compute_level(int64_t scheduled_nanos);

    Priority update_priority(const Priority& old_priority, int64_t quanta_nanos,
                             int64_t scheduled_nanos);
    int64_t get_level_min_priority(int level, int64_t scheduled_nanos);
    void offer(std::shared_ptr<PrioritizedSplitRunner> split);
    std::shared_ptr<PrioritizedSplitRunner> take();
    size_t size() const;
    void remove(std::shared_ptr<PrioritizedSplitRunner> split);
    void remove_all(const std::vector<std::shared_ptr<PrioritizedSplitRunner>>& splits);
    void interrupt();

    int64_t level_scheduled_time(int level) const { return _level_scheduled_time[level].load(); }

private:
    int64_t _get_level0_target_time(std::unique_lock<std::mutex>& lock);
    std::shared_ptr<PrioritizedSplitRunner> _poll_split(std::unique_lock<std::mutex>& lock);
    void _offer_locked(std::shared_ptr<PrioritizedSplitRunner> split, int level,
                       std::unique_lock<std::mutex>& lock);

    const double _level_time_multiplier;

    std::array<std::priority_queue<std::shared_ptr<PrioritizedSplitRunner>,
                                   std::vector<std::shared_ptr<PrioritizedSplitRunner>>,
                                   SplitRunnerComparator>,
               LEVEL_THRESHOLD_SECONDS.size()>
            _level_waiting_splits;

    std::array<std::atomic<int64_t>, LEVEL_THRESHOLD_SECONDS.size()> _level_scheduled_time;
    std::array<std::atomic<int64_t>, LEVEL_THRESHOLD_SECONDS.size()> _level_min_priority;

    std::atomic<bool> _interrupted {false};
    mutable std::mutex _mutex;
    std::condition_variable _not_empty;
};

} // namespace vectorized
} // namespace doris
