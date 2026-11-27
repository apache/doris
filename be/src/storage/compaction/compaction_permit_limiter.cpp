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

#include "storage/compaction/compaction_permit_limiter.h"

#include <bvar/reducer.h>
#include <bvar/status.h>

#include <algorithm>
#include <chrono>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"

namespace doris {

CompactionPermitLimiter::CompactionPermitLimiter() : _used_permits(0) {}

bool CompactionPermitLimiter::try_request(int64_t permits) {
    const int64_t total_permits = config::total_permits_for_compaction_score;
    std::unique_lock<std::mutex> lock(_permits_mutex);
    if (_used_permits + permits > total_permits) {
        return false;
    }

    _used_permits += permits;
    DorisMetrics::instance()->compaction_used_permits->set_value(_used_permits);
    return true;
}

void CompactionPermitLimiter::request(int64_t permits) {
    DorisMetrics::instance()->compaction_waitting_permits->set_value(permits);
    // 1. config::total_permits_for_compaction_score = 20000
    // 2. Thread-B requests permits 11000， used_permits = 11000
    // 3. Thread-A requests permits 12000，wait for used_permits + 12000 <= 20000
    // 4. adjust config::total_permits_for_compaction_score = 10000
    // 5. Thread-B releases permits，used_permits = 0，notify Thread-A，used_permits + 12000 <= 10000
    // we need to initialize total_permits instead of using the config.
    int64_t total_permits = config::total_permits_for_compaction_score;
    if (permits > total_permits) {
        // when tablet's compaction score is larger than "config::total_permits_for_compaction_score",
        // it's necessary to do compaction for this tablet because this tablet will not get "permits"
        // anyway. otherwise, compaction task for this tablet will not be executed forever.
        std::unique_lock<std::mutex> lock(_permits_mutex);
        _permits_cv.wait(lock, [permits, total_permits, this] {
            return _used_permits == 0 || _used_permits + permits <= total_permits;
        });
    } else {
        if (_used_permits + permits > total_permits) {
            std::unique_lock<std::mutex> lock(_permits_mutex);
            _permits_cv.wait(lock, [permits, total_permits, this] {
                return _used_permits + permits <= total_permits;
            });
        }
    }
    _used_permits += permits;
    DorisMetrics::instance()->compaction_waitting_permits->set_value(0);
    DorisMetrics::instance()->compaction_used_permits->set_value(_used_permits);
}

void CompactionPermitLimiter::release(int64_t permits) {
    std::unique_lock<std::mutex> lock(_permits_mutex);
    _used_permits -= permits;
    _permits_cv.notify_one();
    DorisMetrics::instance()->compaction_used_permits->set_value(_used_permits);
}

namespace {
struct CompactionAdmissionMetrics {
    bvar::Status<int64_t> cumulative_permits_used {"compaction_cumulative_permits_used", 0};
    bvar::Status<int64_t> other_permits_used {"compaction_other_permits_used", 0};
    bvar::Status<int64_t> cumulative_permits_limit {"compaction_cumulative_permits_limit", 0};
    bvar::Status<int64_t> other_permits_limit {"compaction_other_permits_limit", 0};
    bvar::Adder<int64_t> cumulative_permit_rejections {"compaction_cumulative_permit_rejections"};
    bvar::Adder<int64_t> other_permit_rejections {"compaction_other_permit_rejections"};
    bvar::Adder<int64_t> cumulative_disk_rejections {"compaction_cumulative_disk_rejections"};
    bvar::Adder<int64_t> other_disk_rejections {"compaction_other_disk_rejections"};
};

CompactionAdmissionMetrics& admission_metrics() {
    // The disabled path shouldn't publish isolation metrics with misleading zero limits.
    static CompactionAdmissionMetrics metrics;
    return metrics;
}
} // namespace

CompactionAdmission::CompactionAdmission(int64_t reserved_permits)
        : _reserved_permits(reserved_permits) {
    CHECK_GT(_reserved_permits, 0);
}

CompactionAdmission::Lease::~Lease() {
    _owner._release(_dir, _type, _charge);
}

bool CompactionAdmission::_has_disk_slot(const DataDir* dir, CompactionType type, int limit) const {
    auto it = _disk_usage.find(dir);
    const DiskUsage usage = it == _disk_usage.end() ? DiskUsage {} : it->second;
    if (type == CompactionType::CHANGELOG_COMPACTION) {
        // Changelog has its own disk concurrency limit, not the base/cumulative reserve.
        return usage.changelog < limit;
    }
    DCHECK_GE(limit, 2);
    int own_usage = type == CompactionType::CUMULATIVE_COMPACTION ? usage.cumulative : usage.base;
    return usage.base + usage.cumulative < limit && own_usage < limit - 1;
}

bool CompactionAdmission::has_disk_slot(const DataDir* dir, CompactionType type, int disk_limit) {
    std::lock_guard lock(_mutex);
    return _has_disk_slot(dir, type, disk_limit);
}

Status CompactionAdmission::acquire(const DataDir* dir, CompactionType type, int disk_limit,
                                    int64_t score, bool wait, std::unique_ptr<Lease>& lease) {
    DCHECK_GE(score, 0);
    DCHECK(!lease);
    const bool cumulative = type == CompactionType::CUMULATIVE_COMPACTION;
    std::unique_lock lock(_mutex);
    auto& used = cumulative ? _cumulative_used : _other_used;
    auto& metrics = admission_metrics();
    while (!_closed) {
        int64_t total;
        {
            std::lock_guard config_lock(*config::get_mutable_string_config_lock());
            total = config::total_permits_for_compaction_score;
        }
        const int64_t other_limit = total - _reserved_permits;
        metrics.cumulative_permits_limit.set_value(_reserved_permits);
        metrics.other_permits_limit.set_value(other_limit);
        const int64_t limit = cumulative ? _reserved_permits : other_limit;
        // Charge no more than the lane budget. E.g. a score of 1500 still runs in a
        // 1000-permit lane once the lane drains, without consuming the other lane's reserve.
        const int64_t charge = std::min(score, limit);
        const bool disk_available = _has_disk_slot(dir, type, disk_limit);
        // A force request skips permits even if a live budget reduction leaves existing
        // charged tasks temporarily over budget. It must still fit on its target disk.
        const bool permits_available =
                score == 0 ||
                (used <= limit - charge && _cumulative_used + _other_used <= total - charge);
        if (disk_available && permits_available) {
            auto& disk_usage = _disk_usage[dir];
            lease.reset(new Lease(*this, dir, type, charge));
            if (type == CompactionType::CHANGELOG_COMPACTION) {
                ++disk_usage.changelog;
            } else if (cumulative) {
                ++disk_usage.cumulative;
            } else {
                ++disk_usage.base;
            }
            used += charge;
            _update_usage_metrics();
            return Status::OK();
        }
        if (!wait) {
            // An automatic producer must leave this round rather than wait behind a slow base.
            if (disk_available) {
                (cumulative ? metrics.cumulative_permit_rejections
                            : metrics.other_permit_rejections)
                        << 1;
                return Status::TooManyTasks("Insufficient compaction permits");
            }
            (cumulative ? metrics.cumulative_disk_rejections : metrics.other_disk_rejections) << 1;
            return Status::TooManyTasks("Insufficient compaction disk slots");
        }
        // Manual requests retain blocking semantics. Periodically reread the mutable total
        // even if no concurrent task completes after a configuration change.
        _cv.wait_for(lock, std::chrono::seconds(1));
    }
    return Status::Cancelled("Compaction admission is closed");
}

void CompactionAdmission::_release(const DataDir* dir, CompactionType type, int64_t charge) {
    {
        std::lock_guard lock(_mutex);
        auto it = _disk_usage.find(dir);
        DCHECK(it != _disk_usage.end());
        auto& usage = it->second;
        if (type == CompactionType::CUMULATIVE_COMPACTION) {
            --usage.cumulative;
            _cumulative_used -= charge;
        } else {
            _other_used -= charge;
            if (type == CompactionType::CHANGELOG_COMPACTION) {
                --usage.changelog;
            } else {
                --usage.base;
            }
        }
        if (usage.base == 0 && usage.cumulative == 0 && usage.changelog == 0) {
            _disk_usage.erase(it);
        }
        _update_usage_metrics();
    }
    _cv.notify_all();
}

void CompactionAdmission::_update_usage_metrics() {
    DorisMetrics::instance()->compaction_used_permits->set_value(_cumulative_used + _other_used);
    auto& metrics = admission_metrics();
    metrics.cumulative_permits_used.set_value(_cumulative_used);
    metrics.other_permits_used.set_value(_other_used);
}

void CompactionAdmission::close() {
    {
        std::lock_guard lock(_mutex);
        _closed = true;
    }
    _cv.notify_all();
}
} // namespace doris
