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

#include "storage/active_tablet_stats.h"

#include <algorithm>
#include <cstddef>

#include "common/config.h"
#include "storage/tablet/base_tablet.h"
#include "util/time.h"

namespace doris {

void ActiveTabletCollector::start() {
    _now_ms = UnixMillis();
}

void ActiveTabletCollector::collect(const std::shared_ptr<BaseTablet>& tablet) {
    if (!tablet) {
        return;
    }
    const int64_t scan_cur = tablet->query_scan_count->value();
    const int64_t flush_cur = tablet->flush_finish_count->value();
    _pending.push_back({tablet, scan_cur, flush_cur});

    const int64_t prev_ms = tablet->last_reported_time_ms.load(std::memory_order_relaxed);
    if (prev_ms == 0) {
        // First time we see this tablet: this round only establishes the baseline.
        return;
    }
    const int64_t scan_delta =
            scan_cur - tablet->last_reported_scan_count.load(std::memory_order_relaxed);
    const int64_t load_delta =
            flush_cur - tablet->last_reported_flush_count.load(std::memory_order_relaxed);
    const int64_t win_ms = std::max<int64_t>(1, _now_ms - prev_ms);
    const int64_t active_window_ms =
            static_cast<int64_t>(config::report_active_tablet_window_second) * 1000;

    const int64_t last_q = tablet->last_query_scan_time_ms.load(std::memory_order_relaxed);
    const int64_t last_l = tablet->last_load_flush_time_ms.load(std::memory_order_relaxed);
    if (scan_delta > 0 && _now_ms - last_q <= active_window_ms) {
        _query_cands.push_back({tablet->tablet_id(), scan_delta, win_ms, last_q});
    }
    if (load_delta > 0 && _now_ms - last_l <= active_window_ms) {
        _load_cands.push_back({tablet->tablet_id(), load_delta, win_ms, last_l});
    }
    // A tablet hot on both dimensions appears once in each list; FE merges by tablet_id.
}

void ActiveTabletCollector::take_top_n() {
    const auto truncate_to_cap = [this](std::vector<ActiveTabletCandidate>& candidates) {
        // A negative cap would wrap to SIZE_MAX and silently disable truncation, i.e. ship the
        // whole candidate list in the report; clamp instead.
        const auto cap = static_cast<std::size_t>(
                std::max(0, static_cast<int>(config::report_active_tablet_max_num)));
        if (candidates.size() > cap) {
            _truncated = true;
            std::nth_element(
                    candidates.begin(), candidates.begin() + cap, candidates.end(),
                    [](const auto& lhs, const auto& rhs) { return lhs.rate() > rhs.rate(); });
            candidates.resize(cap);
        }
    };
    truncate_to_cap(_query_cands);
    truncate_to_cap(_load_cands);
}

void ActiveTabletCollector::commit() {
    for (auto& pending : _pending) {
        if (auto tablet = pending.tablet.lock()) {
            tablet->last_reported_scan_count.store(pending.scan_count, std::memory_order_relaxed);
            tablet->last_reported_flush_count.store(pending.flush_count, std::memory_order_relaxed);
            tablet->last_reported_time_ms.store(_now_ms, std::memory_order_relaxed);
        }
    }
}

void ActiveTabletCollector::clear() {
    _query_cands.clear();
    _load_cands.clear();
    _pending.clear();
    _truncated = false;
}

} // namespace doris
