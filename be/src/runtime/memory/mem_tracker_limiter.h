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
#include <cstdint>
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
#include <list>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "runtime/memory/mem_counter.h"
#include "runtime/query_statistics.h"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris {

class RuntimeProfile;
class MemTrackerLimiter;

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

/*
 * Track and limit the memory usage of process and query.
 *
 * Usually, put Query MemTrackerLimiter into SCOPED_ATTACH_TASK when the thread starts,
 * all memory used by this thread will be recorded on this Query.
 *
 * This class is thread-safe.
*/
class MemTrackerLimiter final {
public:
    /*
    * Part 1, Type definition
    */

    // TODO There are more and more GC codes and there should be a separate manager class.
    enum class GCType { PROCESS = 0, WORK_LOAD_GROUP = 1 };

    enum class Type {
        GLOBAL = 0,        // Life cycle is the same as the process, e.g. Cache and default Orphan
        QUERY = 1,         // Count the memory consumption of all Query tasks.
        LOAD = 2,          // Count the memory consumption of all Load tasks.
        COMPACTION = 3,    // Count the memory consumption of all Base and Cumulative tasks.
        SCHEMA_CHANGE = 4, // Count the memory consumption of all SchemaChange tasks.
        OTHER = 5,
    };

    static std::string type_string(Type type) {
        switch (type) {
        case Type::GLOBAL:
            return "global";
        case Type::QUERY:
            return "query";
        case Type::LOAD:
            return "load";
        case Type::COMPACTION:
            return "compaction";
        case Type::SCHEMA_CHANGE:
            return "schema_change";
        case Type::OTHER:
            return "other";
        default:
            LOG(FATAL) << "not match type of mem tracker limiter :" << static_cast<int>(type);
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }

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

    /*
    * Part 2, Constructors and property methods
    */

    static std::shared_ptr<MemTrackerLimiter> create_shared(MemTrackerLimiter::Type type,
                                                            const std::string& label,
                                                            int64_t byte_limit = -1);
    // byte_limit equal to -1 means no consumption limit, only participate in process memory statistics.
    MemTrackerLimiter(Type type, const std::string& label, int64_t byte_limit);

    ~MemTrackerLimiter();

    Type type() const { return _type; }
    const std::string& label() const { return _label; }
    std::shared_ptr<QueryStatistics> get_query_statistics() { return _query_statistics; }
    int64_t group_num() const { return _group_num; }
    bool has_limit() const { return _limit >= 0; }
    int64_t limit() const { return _limit; }
    bool limit_exceeded() const { return _limit >= 0 && _limit < consumption(); }
    Status check_limit(int64_t bytes = 0);
    // Log the memory usage when memory limit is exceeded.
    std::string tracker_limit_exceeded_str();
    bool is_overcommit_tracker() const { return type() == Type::QUERY || type() == Type::LOAD; }
    bool is_query_cancelled() { return _is_query_cancelled; }
    void set_is_query_cancelled(bool is_cancelled) { _is_query_cancelled.store(is_cancelled); }

    static void clean_tracker_limiter_group();

    /*
    * Part 3, Memory tracking method (use carefully!)
    *
    * Note: Only memory not allocated by Doris Allocator can be tracked by manually calling consume() and release().
    *       Memory allocated by Doris Allocator needs to be tracked using SCOPED_ATTACH_TASK or
    *       SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER.
    */

    int64_t consumption() const { return _mem_counter.current_value(); }
    int64_t peak_consumption() const { return _mem_counter.peak_value(); }

    void consume(int64_t bytes) {
        _mem_counter.add(bytes);
        if (_query_statistics) {
            _query_statistics->set_max_peak_memory_bytes(peak_consumption());
            _query_statistics->set_current_used_memory_bytes(consumption());
        }
    }

    void consume_no_update_peak(int64_t bytes) { _mem_counter.add_no_update_peak(bytes); }

    void release(int64_t bytes) { _mem_counter.sub(bytes); }

    bool try_consume(int64_t bytes) {
        if (UNLIKELY(bytes == 0)) {
            return true;
        }
        bool rt = true;
        if (is_overcommit_tracker() && !config::enable_query_memory_overcommit) {
            rt = _mem_counter.try_add(bytes, _limit);
        } else {
            _mem_counter.add(bytes);
        }
        if (rt && _query_statistics) {
            _query_statistics->set_max_peak_memory_bytes(peak_consumption());
            _query_statistics->set_current_used_memory_bytes(consumption());
        }
        return rt;
    }

    void set_consumption(int64_t bytes) { _mem_counter.set(bytes); }

    // Transfer 'bytes' of consumption from this tracker to 'dst'.
    void transfer_to(int64_t size, MemTrackerLimiter* dst) {
        if (label() == dst->label()) {
            return;
        }
        cache_consume(-size);
        dst->cache_consume(size);
    }

    // If need to consume the tracker frequently, use it
    void cache_consume(int64_t bytes);

    /*
    * Part 4, Reserved memory tracking method
    */

    int64_t reserved_consumption() const { return _reserved_counter.current_value(); }
    int64_t reserved_peak_consumption() const { return _reserved_counter.peak_value(); }

    bool try_reserve(int64_t bytes) {
        bool rt = try_consume(bytes);
        if (rt) {
            _reserved_counter.add(bytes);
        }
        return rt;
    }

    void release_reserved(int64_t bytes) {
        _reserved_counter.sub(bytes);
        DCHECK(reserved_consumption() >= 0);
    }

    /*
    * Part 4, Memory profile and log method
    */
    RuntimeProfile* make_profile(RuntimeProfile* profile) const;
    std::string make_profile_str() const;
    static void make_type_trackers_profile(RuntimeProfile* profile, MemTrackerLimiter::Type type);
    static std::string make_type_trackers_profile_str(MemTrackerLimiter::Type type);
    static void make_top_consumption_tasks_tracker_profile(RuntimeProfile* profile, int top_num);
    static void make_all_tasks_tracker_profile(RuntimeProfile* profile);

    void print_log_usage(const std::string& msg);
    void enable_print_log_usage() { _enable_print_log_usage = true; }

    /*
    * Part 5, Memory GC method
    */

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

    /*
    * Part 6, Memory debug method
    */

    void add_address_sanitizers(void* buf, size_t size);
    void remove_address_sanitizers(void* buf, size_t size);
    bool is_group_commit_load {false};

private:
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

    // When the accumulated untracked memory value exceeds the upper limit,
    // the current value is returned and set to 0.
    // Thread safety.
    int64_t add_untracked_mem(int64_t bytes);

    /*
    * Part 8, Property definition
    */

    Type _type;

    // label used in the make snapshot, not guaranteed unique.
    std::string _label;
    // For generate runtime profile, profile name must be unique.
    UniqueId _uid;

    MemCounter _mem_counter;
    MemCounter _reserved_counter;

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

    std::shared_ptr<QueryStatistics> _query_statistics = nullptr;

    struct AddressSanitizer {
        size_t size;
        std::string stack_trace;
    };

    std::string print_address_sanitizers();
    bool open_memory_tracker_inaccurate_detect();
    std::mutex _address_sanitizers_mtx;
    std::unordered_map<void*, AddressSanitizer> _address_sanitizers;
    std::vector<std::string> _error_address_sanitizers;
};

inline int64_t MemTrackerLimiter::add_untracked_mem(int64_t bytes) {
    _untracked_mem += bytes;
    if (std::abs(_untracked_mem) >= config::mem_tracker_consume_min_size_bytes) {
        return _untracked_mem.exchange(0);
    }
    return 0;
}

inline void MemTrackerLimiter::cache_consume(int64_t bytes) {
    if (bytes == 0) {
        return;
    }
    int64_t consume_bytes = add_untracked_mem(bytes);
    consume(consume_bytes);
}

inline Status MemTrackerLimiter::check_limit(int64_t bytes) {
    if (bytes <= 0 || (is_overcommit_tracker() && config::enable_query_memory_overcommit)) {
        return Status::OK();
    }
    if (_limit > 0 && consumption() + bytes > _limit) {
        return Status::MemoryLimitExceeded(fmt::format("failed alloc size {}, {}",
                                                       MemCounter::print_bytes(bytes),
                                                       tracker_limit_exceeded_str()));
    }
    return Status::OK();
}

} // namespace doris
