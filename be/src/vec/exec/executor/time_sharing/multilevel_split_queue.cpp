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

#include "vec/exec/executor/time_sharing/multilevel_split_queue.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <unordered_set>

namespace doris {
namespace vectorized {

MultilevelSplitQueue::MultilevelSplitQueue(double level_time_multiplier)
        : _level_time_multiplier(level_time_multiplier), _level_waiting_splits() {
    for (size_t i = 0; i < LEVEL_THRESHOLD_SECONDS.size(); ++i) {
        _level_scheduled_time[i].store(0);
        _level_min_priority[i].store(-1);
    }
}

int MultilevelSplitQueue::compute_level(int64_t scheduled_nanos) {
    double scheduled_seconds = static_cast<double>(scheduled_nanos) / 1000000000.0;

    for (int level = 0; level < (LEVEL_THRESHOLD_SECONDS.size() - 1); level++) {
        if (scheduled_seconds < LEVEL_THRESHOLD_SECONDS[level + 1]) {
            return level;
        }
    }

    return LEVEL_THRESHOLD_SECONDS.size() - 1;
}

/**
 * 'Charges' the quanta run time to the task <i>and</i> the level it belongs to in
 * an effort to maintain the target thread utilization ratios between levels and to
 * maintain fairness within a level.
 * <p>
 * Consider an example split where a read hung for several minutes. This is either a bug
 * or a failing dependency. In either case we do not want to charge the task too much,
 * and we especially do not want to charge the level too much - i.e. cause other queries
 * in this level to starve.
 *
 * @return the new priority for the task
 */
Priority MultilevelSplitQueue::update_priority(const Priority& old_priority, int64_t quanta_nanos,
                                               int64_t scheduled_nanos) {
    int old_level = old_priority.level();
    int new_level = compute_level(scheduled_nanos);

    int64_t level_contribution = std::min(quanta_nanos, LEVEL_CONTRIBUTION_CAP);

    if (old_level == new_level) {
        _level_scheduled_time[old_level].fetch_add(level_contribution);
        return Priority(old_level, old_priority.level_priority() + quanta_nanos);
    }

    int64_t remaining_level_contribution = level_contribution;
    int64_t remaining_task_time = quanta_nanos;

    for (int current_level = old_level; current_level < new_level; ++current_level) {
        int64_t level_time_threshold =
                static_cast<int64_t>((LEVEL_THRESHOLD_SECONDS[current_level + 1] -
                                      LEVEL_THRESHOLD_SECONDS[current_level]) *
                                     1e9);

        int64_t time_accrued_to_level =
                std::min(level_time_threshold, remaining_level_contribution);
        _level_scheduled_time[current_level].fetch_add(time_accrued_to_level);
        remaining_level_contribution -= time_accrued_to_level;
        remaining_task_time -= time_accrued_to_level;
    }

    _level_scheduled_time[new_level].fetch_add(remaining_level_contribution);
    int64_t new_level_min_priority = get_level_min_priority(new_level, scheduled_nanos);
    return Priority(new_level, new_level_min_priority + remaining_task_time);
}

int64_t MultilevelSplitQueue::get_level_min_priority(int level, int64_t task_thread_usage_nanos) {
    int64_t expected = -1;
    _level_min_priority[level].compare_exchange_strong(expected, task_thread_usage_nanos);
    return _level_min_priority[level].load();
}

void MultilevelSplitQueue::offer(std::shared_ptr<PrioritizedSplitRunner> split) {
    split->set_ready();
    int level = split->priority().level();
    _do_offer(split, level);
}

void MultilevelSplitQueue::_do_offer(std::shared_ptr<PrioritizedSplitRunner> split, int level) {
    if (_level_waiting_splits[level].empty()) {
        int64_t level0_time = _get_level0_target_time();
        int64_t level_expected_time =
                static_cast<int64_t>(level0_time / std::pow(_level_time_multiplier, level));
        int64_t delta = level_expected_time - _level_scheduled_time[level].load();
        _level_scheduled_time[level].fetch_add(delta);
    }
    _level_waiting_splits[level].push(split);
}

std::shared_ptr<PrioritizedSplitRunner> MultilevelSplitQueue::take() {
    auto split = _poll_split();
    if (split) {
        if (split->update_level_priority()) {
            _do_offer(split, split->priority().level());
            return take();
        }
        int selected_level = split->priority().level();
        _level_min_priority[selected_level].store(split->priority().level_priority());
        return split;
    }
    return nullptr;
}

/**
 * Attempts to give each level a target amount of scheduled time, which is configurable
 * using levelTimeMultiplier.
 * <p>
 * This function selects the level that has the lowest ratio of actual to the target time
 * with the objective of minimizing deviation from the target scheduled time. From this level,
 * we pick the split with the lowest priority.
 */
std::shared_ptr<PrioritizedSplitRunner> MultilevelSplitQueue::_poll_split() {
    int64_t target_scheduled_time = _get_level0_target_time();
    double worst_ratio = 1.0;
    int selected_level = -1;

    for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.size(); ++level) {
        if (!_level_waiting_splits[level].empty()) {
            int64_t level_time = _level_scheduled_time[level].load();
            double ratio = (level_time == 0) ? 0
                                             : static_cast<double>(target_scheduled_time) /
                                                       (1.0 * level_time);

            if (selected_level == -1 || ratio > worst_ratio) {
                worst_ratio = ratio;
                selected_level = level;
            }
        }
        target_scheduled_time =
                static_cast<int64_t>(std::round(target_scheduled_time / _level_time_multiplier));
    }

    if (selected_level == -1) return nullptr;

    auto result = _level_waiting_splits[selected_level].top();
    _level_waiting_splits[selected_level].pop();
    return result;
}

void MultilevelSplitQueue::remove(std::shared_ptr<PrioritizedSplitRunner> split) {
    for (auto& level_queue : _level_waiting_splits) {
        std::priority_queue<std::shared_ptr<PrioritizedSplitRunner>,
                            std::vector<std::shared_ptr<PrioritizedSplitRunner>>,
                            SplitRunnerComparator>
                new_queue;
        while (!level_queue.empty()) {
            auto current = level_queue.top();
            level_queue.pop();
            if (current != split) {
                new_queue.emplace(std::move(current));
            }
        }
        level_queue.swap(new_queue);
    }
}

void MultilevelSplitQueue::remove_all(
        const std::vector<std::shared_ptr<PrioritizedSplitRunner>>& splits) {
    std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>> to_remove(splits.begin(),
                                                                          splits.end());

    for (auto& level_queue : _level_waiting_splits) {
        std::priority_queue<std::shared_ptr<PrioritizedSplitRunner>,
                            std::vector<std::shared_ptr<PrioritizedSplitRunner>>,
                            SplitRunnerComparator>
                new_queue;
        while (!level_queue.empty()) {
            auto current = level_queue.top();
            level_queue.pop();
            if (!to_remove.count(current)) {
                new_queue.emplace(std::move(current));
            }
        }
        level_queue.swap(new_queue);
    }
}

size_t MultilevelSplitQueue::size() const {
    return std::accumulate(_level_waiting_splits.begin(), _level_waiting_splits.end(), size_t(0),
                           [](size_t sum, const auto& queue) { return sum + queue.size(); });
}

int64_t MultilevelSplitQueue::_get_level0_target_time() {
    int64_t level0_target_time = _level_scheduled_time[0].load();
    double current_multiplier = _level_time_multiplier;

    for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.size(); ++level) {
        current_multiplier /= _level_time_multiplier;
        int64_t level_time = _level_scheduled_time[level].load();
        level0_target_time =
                std::max(level0_target_time, static_cast<int64_t>(level_time / current_multiplier));
    }
    return level0_target_time;
}

void MultilevelSplitQueue::clear() {
    for (auto& queue : _level_waiting_splits) {
        while (!queue.empty()) {
            queue.pop();
        }
    }
}

} // namespace vectorized
} // namespace doris
