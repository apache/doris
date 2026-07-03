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

#include "storage/index/snii/writer/global_memory_limiter.h"

#include <algorithm>
#include <utility>
#include <vector>

namespace doris::snii::writer {

GlobalMemoryLimiter* GlobalMemoryLimiter::instance() {
    // Intentionally leaked (never destroyed): buffers un-register from their
    // destructors, which may run during static teardown at process exit -- a
    // destroyed registry there would be use-after-free. A leaked singleton
    // makes the "limiter outlives every attached buffer" contract
    // unconditional.
    static auto* g_instance = new GlobalMemoryLimiter();
    return g_instance;
}

void GlobalMemoryLimiter::register_buffer(std::atomic<bool>* spill_flag, int64_t resident_bytes) {
    std::lock_guard<std::mutex> guard(mutex_);
    int64_t& slot = entries_[spill_flag]; // 0 on first insert
    total_bytes_locked_ += resident_bytes - slot;
    slot = resident_bytes;
}

void GlobalMemoryLimiter::report(std::atomic<bool>* spill_flag, int64_t resident_bytes) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = entries_.find(spill_flag);
    if (it == entries_.end()) {
        // Not registered (or already unregistered): ignore rather than
        // resurrect an entry nobody will remove.
        return;
    }
    total_bytes_locked_ += resident_bytes - it->second;
    it->second = resident_bytes;
    request_spills_locked();
}

void GlobalMemoryLimiter::unregister_buffer(std::atomic<bool>* spill_flag) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = entries_.find(spill_flag);
    if (it == entries_.end()) {
        return;
    }
    total_bytes_locked_ -= it->second;
    entries_.erase(it);
}

int64_t GlobalMemoryLimiter::total_bytes() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return total_bytes_locked_;
}

size_t GlobalMemoryLimiter::registered_count() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return entries_.size();
}

void GlobalMemoryLimiter::request_spills_locked() {
    const int64_t budget = budget_bytes_.load(std::memory_order_relaxed);
    if (budget <= 0 || total_bytes_locked_ <= budget) {
        return;
    }
    const int64_t overage = total_bytes_locked_ - budget;
    // Largest consumers first. n is the live writer count of the process (at
    // most a few hundred), and this only runs while over budget, so the sort
    // under the mutex is bounded, allocation-light work.
    std::vector<std::pair<int64_t, std::atomic<bool>*>> by_size;
    by_size.reserve(entries_.size());
    for (const auto& [flag, bytes] : entries_) {
        if (bytes > 0) {
            by_size.emplace_back(bytes, flag);
        }
    }
    std::sort(by_size.begin(), by_size.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    int64_t covered = 0;
    for (const auto& [bytes, flag] : by_size) {
        if (covered >= overage) {
            break;
        }
        // An ALREADY-pending flag (set by an earlier report, owner not yet at
        // its next token) counts toward the covered sum without a fresh store:
        // re-flagging it would be a no-op, and skipping the store avoids
        // dirtying the owner's cache line every over-budget report.
        if (!flag->load(std::memory_order_relaxed)) {
            flag->store(true, std::memory_order_relaxed);
        }
        covered += bytes;
    }
}

} // namespace doris::snii::writer
