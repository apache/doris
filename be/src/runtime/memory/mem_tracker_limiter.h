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
#include <stdint.h>

#include <atomic>
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
#include <list>
#include <memory>
#include <ostream>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "runtime/memory/mem_tracker.h"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris {

class RuntimeProfile;

constexpr size_t MEM_TRACKER_GROUP_NUM = 1000;

struct TrackerLimiterGroup {
    // Note! in order to enable ExecEnv::mem_tracker_limiter_pool support resize,
    // the copy construction of TrackerLimiterGroup is disabled.
    // so cannot copy TrackerLimiterGroup anywhere, should use reference.
    TrackerLimiterGroup() = default;
    TrackerLimiterGroup(TrackerLimiterGroup&&) noexcept {}
    TrackerLimiterGroup(const TrackerLimiterGroup&) {}
    TrackerLimiterGroup& operator=(const TrackerLimiterGroup&) { return *this; }

    std::list<std::weak_ptr<MemTrackerLimiter>> trackers;
    std::mutex group_lock;
};

// Track and limit the memory usage of process and query.
// Contains an limit, arranged into a tree structure.
//
// Automatically track every once malloc/free of the system memory allocator (Currently, based on TCMlloc hook).
// Put Query MemTrackerLimiter into SCOPED_ATTACH_TASK when the thread starts,all memory used by this thread
// will be recorded on this Query, otherwise it will be recorded in Orphan Tracker by default.
class MemTrackerLimiter final : public MemTracker {
public:
    // TODO There are more and more GC codes and there should be a separate manager class.
    enum class GCType { PROCESS = 0, WORK_LOAD_GROUP = 1 };

    inline static std::unordered_map<Type, std::shared_ptr<MemCounter>> TypeMemSum = {
            {Type::GLOBAL, std::make_shared<MemCounter>()},
            {Type::QUERY, std::make_shared<MemCounter>()},
            {Type::LOAD, std::make_shared<MemCounter>()},
            {Type::COMPACTION, std::make_shared<MemCounter>()},
            {Type::SCHEMA_CHANGE, std::make_shared<MemCounter>()},
            {Type::OTHER, std::make_shared<MemCounter>()}};

public:
    static std::shared_ptr<MemTrackerLimiter> create_shared(
            MemTrackerLimiter::Type type, const std::string& label = std::string(),
            int64_t byte_limit = -1);
    // byte_limit equal to -1 means no consumption limit, only participate in process memory statistics.
    MemTrackerLimiter(Type type, const std::string& label, int64_t byte_limit);

    ~MemTrackerLimiter() override;

    static std::string gc_type_string(GCType type) {
        switch (type) {
        case GCType::PROCESS:
            return "process";
        case GCType::WORK_LOAD_GROUP:
            return "work load group";
        default:
            LOG(FATAL) << "not match gc type:" << static_cast<int>(type);
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }

    void set_consumption() { LOG(FATAL) << "MemTrackerLimiter set_consumption not supported"; }
    int64_t group_num() const { return _group_num; }
    bool has_limit() const { return _limit >= 0; }
    int64_t limit() const { return _limit; }
    bool limit_exceeded() const { return _limit >= 0 && _limit < consumption(); }

    bool try_consume(int64_t bytes) const {
        if (UNLIKELY(bytes == 0)) {
            return true;
        }
        bool st = true;
        if (is_overcommit_tracker() && !config::enable_query_memory_overcommit) {
            st = _consumption->try_add(bytes, _limit);
        } else {
            _consumption->add(bytes);
        }
        if (st && _query_statistics) {
            _query_statistics->set_max_peak_memory_bytes(_consumption->peak_value());
            _query_statistics->set_current_used_memory_bytes(_consumption->current_value());
        }
        return st;
    }

    Status check_limit(int64_t bytes = 0);
    bool is_overcommit_tracker() const { return type() == Type::QUERY || type() == Type::LOAD; }

    bool is_query_cancelled() { return _is_query_cancelled; }

    void set_is_query_cancelled(bool is_cancelled) { _is_query_cancelled.store(is_cancelled); }

public:
    // Transfer 'bytes' of consumption from this tracker to 'dst'.
    void transfer_to(int64_t size, MemTrackerLimiter* dst) {
        if (label() == dst->label()) {
            return;
        }
        cache_consume(-size);
        dst->cache_consume(size);
    }

    static void refresh_global_counter();
    static void clean_tracker_limiter_group();

    Snapshot make_snapshot() const override;
    // Returns a list of all the valid tracker snapshots.
    static void make_process_snapshots(std::vector<MemTracker::Snapshot>* snapshots);
    static void make_type_snapshots(std::vector<MemTracker::Snapshot>* snapshots, Type type);
    static void make_all_trackers_snapshots(std::vector<MemTracker::Snapshot>* snapshots);
    static void make_all_memory_state_snapshots(std::vector<MemTracker::Snapshot>* snapshots);
    static void make_top_consumption_snapshots(std::vector<MemTracker::Snapshot>* snapshots,
                                               int top_num);

    static std::string log_usage(MemTracker::Snapshot snapshot);
    std::string log_usage() const { return log_usage(make_snapshot()); }
    static std::string type_log_usage(MemTracker::Snapshot snapshot);
    static std::string type_detail_usage(const std::string& msg, Type type);
    void print_log_usage(const std::string& msg);
    void enable_print_log_usage() { _enable_print_log_usage = true; }
    // process memory changes more than 256M, or the GC ends
    static void enable_print_log_process_usage() { _enable_print_log_process_usage = true; }
    static std::string log_process_usage_str();
    static void print_log_process_usage();

    // Start canceling from the query with the largest memory usage until the memory of min_free_mem size is freed.
    // cancel_reason recorded when gc is triggered, for log printing.
    static int64_t free_top_memory_query(int64_t min_free_mem, const std::string& cancel_reason,
                                         RuntimeProfile* profile, Type type = Type::QUERY);

    static int64_t free_top_memory_query(
            int64_t min_free_mem, Type type, std::vector<TrackerLimiterGroup>& tracker_groups,
            const std::function<std::string(int64_t, const std::string&)>& cancel_msg,
            RuntimeProfile* profile, GCType gctype);

    static int64_t free_top_memory_load(int64_t min_free_mem, const std::string& cancel_reason,
                                        RuntimeProfile* profile) {
        return free_top_memory_query(min_free_mem, cancel_reason, profile, Type::LOAD);
    }
    // Start canceling from the query with the largest memory overcommit ratio until the memory
    // of min_free_mem size is freed.
    static int64_t free_top_overcommit_query(int64_t min_free_mem, const std::string& cancel_reason,
                                             RuntimeProfile* profile, Type type = Type::QUERY);

    static int64_t free_top_overcommit_query(
            int64_t min_free_mem, Type type, std::vector<TrackerLimiterGroup>& tracker_groups,
            const std::function<std::string(int64_t, const std::string&)>& cancel_msg,
            RuntimeProfile* profile, GCType gctype);

    static int64_t free_top_overcommit_load(int64_t min_free_mem, const std::string& cancel_reason,
                                            RuntimeProfile* profile) {
        return free_top_overcommit_query(min_free_mem, cancel_reason, profile, Type::LOAD);
    }

    // only for Type::QUERY or Type::LOAD.
    static TUniqueId label_to_queryid(const std::string& label) {
        if (label.find("#Id=") == std::string::npos) {
            return {};
        }
        auto queryid = split(label, "#Id=")[1];
        TUniqueId querytid;
        parse_id(queryid, &querytid);
        return querytid;
    }

    // Log the memory usage when memory limit is exceeded.
    std::string tracker_limit_exceeded_str();

#ifndef NDEBUG
    void add_address_sanitizers(void* buf, size_t size);
    void remove_address_sanitizers(void* buf, size_t size);
    std::string print_address_sanitizers();
#endif

    std::string debug_string() override {
        std::stringstream msg;
        msg << "limit: " << _limit << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "label: " << _label << "; "
            << "type: " << type_string(_type) << "; ";
        return msg.str();
    }

    // Iterator into mem_tracker_limiter_pool for this object. Stored to have O(1) remove.
    std::list<std::weak_ptr<MemTrackerLimiter>>::iterator tg_tracker_limiter_group_it;

private:
    friend class ThreadMemTrackerMgr;

    // If need to consume the tracker frequently, use it
    void cache_consume(int64_t bytes);

    // When the accumulated untracked memory value exceeds the upper limit,
    // the current value is returned and set to 0.
    // Thread safety.
    int64_t add_untracked_mem(int64_t bytes);

private:
    // Limit on memory consumption, in bytes.
    int64_t _limit;

    // Group number in mem_tracker_limiter_pool and mem_tracker_pool, generated by the timestamp.
    int64_t _group_num;

    // Consume size smaller than mem_tracker_consume_min_size_bytes will continue to accumulate
    // to avoid frequent calls to consume/release of MemTracker.
    std::atomic<int64_t> _untracked_mem = 0;

    // query or load
    std::atomic<bool> _is_query_cancelled = false;

    // Avoid frequent printing.
    bool _enable_print_log_usage = false;
    static std::atomic<bool> _enable_print_log_process_usage;

#ifndef NDEBUG
    struct AddressSanitizer {
        size_t size;
        std::string stack_trace;
    };

    std::mutex _address_sanitizers_mtx;
    std::unordered_map<void*, AddressSanitizer> _address_sanitizers;
#endif
};

inline int64_t MemTrackerLimiter::add_untracked_mem(int64_t bytes) {
    _untracked_mem += bytes;
    if (std::abs(_untracked_mem) >= config::mem_tracker_consume_min_size_bytes) {
        return _untracked_mem.exchange(0);
    }
    return 0;
}

inline void MemTrackerLimiter::cache_consume(int64_t bytes) {
    if (bytes == 0) return;
    int64_t consume_bytes = add_untracked_mem(bytes);
    consume(consume_bytes);
}

inline Status MemTrackerLimiter::check_limit(int64_t bytes) {
    if (bytes <= 0 || (is_overcommit_tracker() && config::enable_query_memory_overcommit)) {
        return Status::OK();
    }
    if (_limit > 0 && _consumption->current_value() + bytes > _limit) {
        return Status::MemoryLimitExceeded(fmt::format(
                "failed alloc size {}, {}", print_bytes(bytes), tracker_limit_exceeded_str()));
    }
    return Status::OK();
}

} // namespace doris
