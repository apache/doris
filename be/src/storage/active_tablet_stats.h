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
#include <vector>

namespace doris {
class BaseTablet;

// One entry of a per-dimension top-N list. Kept thrift-free on purpose so this header
// does not drag gen_cpp into the storage layer; conversion lives in task_worker_pool.cpp.
struct ActiveTabletCandidate {
    int64_t tablet_id = 0;
    int64_t delta = 0;
    int64_t window_ms = 1;
    int64_t last_time_ms = 0;

    // Rate, not raw delta: a tablet whose baseline was not committed last round carries a
    // delta spanning several intervals, so raw deltas are not comparable. See handoff §9.2d.
    double rate() const {
        return static_cast<double>(delta) * 1000.0 / static_cast<double>(window_ms);
    }
};

// Collected inside the EXISTING report-tablet walk (no extra traversal). Collection is
// strictly read-only on the tablet baselines; commit() is called only after
// handle_report() returned true, so a retried or dropped report loses nothing.
class ActiveTabletCollector {
public:
    // Called at the top of each build_all_report_tablets_info() pass.
    void start();
    // Called once per tablet during the walk.
    void collect(const std::shared_ptr<BaseTablet>& tablet);
    // Per-dimension nth_element by rate. No cross-dimension merging/weighting here --
    // that is FE's job (handoff §9.2b).
    void take_top_n();
    // Advance the baselines. ONLY after handle_report() succeeded.
    void commit();
    // Reset everything; called on each retry of the report loop.
    void clear();

    const std::vector<ActiveTabletCandidate>& query_candidates() const { return _query_cands; }
    const std::vector<ActiveTabletCandidate>& load_candidates() const { return _load_cands; }
    bool truncated() const { return _truncated; }

private:
    struct Pending {
        std::weak_ptr<BaseTablet> tablet;
        int64_t scan_count = 0;
        int64_t flush_count = 0;
    };
    std::vector<ActiveTabletCandidate> _query_cands;
    std::vector<ActiveTabletCandidate> _load_cands;
    std::vector<Pending> _pending;
    int64_t _now_ms = 0;
    bool _truncated = false;
};

} // namespace doris
