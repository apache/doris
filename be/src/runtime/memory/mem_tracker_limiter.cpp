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

#include "runtime/memory/mem_tracker_limiter.h"

#include <fmt/format.h>
#include <gen_cpp/types.pb.h>

#include <functional>
#include <mutex>
#include <queue>
#include <utility>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "service/backend_options.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"

static bvar::Adder<int64_t> memory_memtrackerlimiter_cnt("memory_memtrackerlimiter_cnt");

std::atomic<long> mem_tracker_limiter_group_counter(0);

MemTrackerLimiter::MemTrackerLimiter(Type type, const std::string& label, int64_t byte_limit) {
    DCHECK_GE(byte_limit, -1);
    _type = type;
    _label = label;
    _limit = byte_limit;
    _uid = UniqueId::gen_uid();
    if (_type == Type::GLOBAL) {
        _group_num = 0;
    } else if (_type == Type::METADATA) {
        _group_num = 1;
    } else if (_type == Type::CACHE) {
        _group_num = 2;
    } else {
        _group_num =
                mem_tracker_limiter_group_counter.fetch_add(1) % (MEM_TRACKER_GROUP_NUM - 3) + 3;
    }
    memory_memtrackerlimiter_cnt << 1;
}

std::shared_ptr<MemTrackerLimiter> MemTrackerLimiter::create_shared(MemTrackerLimiter::Type type,
                                                                    const std::string& label,
                                                                    int64_t byte_limit) {
    auto tracker = std::make_shared<MemTrackerLimiter>(type, label, byte_limit);
    // Write tracker is only used to tracker the size, memtable has a separate logic to deal with memory flush,
    // so limit == -1, so that should not check the limit in memtracker.
    auto write_tracker = std::make_shared<MemTrackerLimiter>(type, "Memtable#" + label, -1);
    tracker->_write_tracker.swap(write_tracker);
#ifndef BE_TEST
    DCHECK(ExecEnv::tracking_memory());
    std::lock_guard<std::mutex> l(
            ExecEnv::GetInstance()->mem_tracker_limiter_pool[tracker->group_num()].group_lock);
    ExecEnv::GetInstance()->mem_tracker_limiter_pool[tracker->group_num()].trackers.insert(
            ExecEnv::GetInstance()->mem_tracker_limiter_pool[tracker->group_num()].trackers.end(),
            tracker);
#endif
    return tracker;
}

bool MemTrackerLimiter::open_memory_tracker_inaccurate_detect() {
    return doris::config::crash_in_memory_tracker_inaccurate &&
           (_type == Type::COMPACTION || _type == Type::SCHEMA_CHANGE || _type == Type::QUERY ||
            (_type == Type::LOAD && !is_group_commit_load));
}

MemTrackerLimiter::~MemTrackerLimiter() {
    consume(_untracked_mem);
    static std::string mem_tracker_inaccurate_msg =
            "mem tracker not equal to 0 when mem tracker destruct, this usually means that "
            "memory tracking is inaccurate and SCOPED_ATTACH_TASK and "
            "SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER are not used correctly. "
            "If the log is truncated, search for `Address Sanitizer` in the be.INFO log to see "
            "more information."
            "1. For query and load, memory leaks may have occurred, it is expected that the query "
            "mem tracker will be bound to the thread context using SCOPED_ATTACH_TASK and "
            "SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER before all memory alloc and free. "
            "2. If a memory alloc is recorded by this tracker, it is expected that be "
            "recorded in this tracker when memory is freed. "
            "3. Merge the remaining memory tracking value by "
            "this tracker into Orphan, if you observe that Orphan is not equal to 0 in the mem "
            "tracker web or log, this indicates that there may be a memory leak. "
            "4. If you need to "
            "transfer memory tracking value between two trackers, can use transfer_to.";
    if (consumption() != 0) {
        if (open_memory_tracker_inaccurate_detect()) {
            std::string err_msg = fmt::format(
                    "mem tracker label: {}, consumption: {}, peak consumption: {}, {}.", label(),
                    consumption(), peak_consumption(), mem_tracker_inaccurate_msg);
            LOG(FATAL) << err_msg << print_address_sanitizers();
        }
        if (ExecEnv::tracking_memory()) {
            ExecEnv::GetInstance()->orphan_mem_tracker()->consume(consumption());
        }
        _mem_counter.set(0);
    } else if (open_memory_tracker_inaccurate_detect() && !_address_sanitizers.empty()) {
        LOG(FATAL) << "[Address Sanitizer] consumption is 0, but address sanitizers not empty. "
                   << ", mem tracker label: " << _label
                   << ", peak consumption: " << peak_consumption() << print_address_sanitizers();
    }
    DCHECK_EQ(reserved_consumption(), 0);
    memory_memtrackerlimiter_cnt << -1;
}

void MemTrackerLimiter::add_address_sanitizers(void* buf, size_t size) {
    if (open_memory_tracker_inaccurate_detect()) {
        std::lock_guard<std::mutex> l(_address_sanitizers_mtx);
        auto it = _address_sanitizers.find(buf);
        if (it != _address_sanitizers.end()) {
            _error_address_sanitizers.emplace_back(
                    fmt::format("[Address Sanitizer] memory buf repeat add, mem tracker label: {}, "
                                "consumption: {}, peak consumption: {}, buf: {}, size: {}, old "
                                "buf: {}, old size: {}, new stack_trace: {}, old stack_trace: {}.",
                                _label, consumption(), peak_consumption(), buf, size, it->first,
                                it->second.size, get_stack_trace(1, "FULL_WITH_INLINE"),
                                it->second.stack_trace));
        }

        // if alignment not equal to 0, maybe usable_size > size.
        AddressSanitizer as = {size, doris::config::enable_address_sanitizers_with_stack_trace
                                             ? get_stack_trace(1, "DISABLED")
                                             : ""};
        _address_sanitizers.emplace(buf, as);
    }
}

void MemTrackerLimiter::remove_address_sanitizers(void* buf, size_t size) {
    if (open_memory_tracker_inaccurate_detect()) {
        std::lock_guard<std::mutex> l(_address_sanitizers_mtx);
        auto it = _address_sanitizers.find(buf);
        if (it != _address_sanitizers.end()) {
            if (it->second.size != size) {
                _error_address_sanitizers.emplace_back(fmt::format(
                        "[Address Sanitizer] free memory buf size inaccurate, mem tracker label: "
                        "{}, consumption: {}, peak consumption: {}, buf: {}, size: {}, old buf: "
                        "{}, old size: {}, new stack_trace: {}, old stack_trace: {}.",
                        _label, consumption(), peak_consumption(), buf, size, it->first,
                        it->second.size, get_stack_trace(1, "FULL_WITH_INLINE"),
                        it->second.stack_trace));
            }
            _address_sanitizers.erase(buf);
        } else {
            _error_address_sanitizers.emplace_back(fmt::format(
                    "[Address Sanitizer] memory buf not exist, mem tracker label: {}, consumption: "
                    "{}, peak consumption: {}, buf: {}, size: {}, stack_trace: {}.",
                    _label, consumption(), peak_consumption(), buf, size,
                    get_stack_trace(1, "FULL_WITH_INLINE")));
        }
    }
}

std::string MemTrackerLimiter::print_address_sanitizers() {
    std::lock_guard<std::mutex> l(_address_sanitizers_mtx);
    std::string detail = "[Address Sanitizer]:";
    detail += "\n memory not be freed:";
    for (const auto& it : _address_sanitizers) {
        auto msg = fmt::format(
                "\n    [Address Sanitizer] buf not be freed, mem tracker label: {}, consumption: "
                "{}, peak consumption: {}, buf: {}, size {}, strack trace: {}",
                _label, consumption(), peak_consumption(), it.first, it.second.size,
                it.second.stack_trace);
        LOG(INFO) << msg;
        detail += msg;
    }
    detail += "\n incorrect memory alloc and free:";
    for (const auto& err_msg : _error_address_sanitizers) {
        LOG(INFO) << err_msg;
        detail += fmt::format("\n    {}", err_msg);
    }
    return detail;
}

RuntimeProfile* MemTrackerLimiter::make_profile(RuntimeProfile* profile) const {
    RuntimeProfile* profile_snapshot = profile->create_child(
            fmt::format("{}@{}@id={}", _label, type_string(_type), _uid.to_string()), true, false);
    RuntimeProfile::HighWaterMarkCounter* usage_counter =
            profile_snapshot->AddHighWaterMarkCounter("Memory", TUnit::BYTES);
    COUNTER_SET(usage_counter, peak_consumption());
    COUNTER_SET(usage_counter, consumption());
    if (limit() >= 0) {
        RuntimeProfile::Counter* limit_counter =
                ADD_COUNTER(profile_snapshot, "Limit", TUnit::BYTES);
        COUNTER_SET(limit_counter, _limit);
    }
    if (reserved_peak_consumption() != 0) {
        RuntimeProfile::HighWaterMarkCounter* reserved_counter =
                profile_snapshot->AddHighWaterMarkCounter("ReservedMemory", TUnit::BYTES);
        COUNTER_SET(reserved_counter, reserved_peak_consumption());
        COUNTER_SET(reserved_counter, reserved_consumption());
    }
    return profile_snapshot;
}

std::string MemTrackerLimiter::make_profile_str() const {
    std::unique_ptr<RuntimeProfile> profile_snapshot =
            std::make_unique<RuntimeProfile>("MemTrackerSnapshot");
    make_profile(profile_snapshot.get());
    std::stringstream ss;
    profile_snapshot->pretty_print(&ss);
    return ss.str();
}

void MemTrackerLimiter::clean_tracker_limiter_group() {
#ifndef BE_TEST
    if (ExecEnv::tracking_memory()) {
        for (auto& group : ExecEnv::GetInstance()->mem_tracker_limiter_pool) {
            std::lock_guard<std::mutex> l(group.group_lock);
            auto it = group.trackers.begin();
            while (it != group.trackers.end()) {
                if ((*it).expired()) {
                    it = group.trackers.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
#endif
}

void MemTrackerLimiter::make_type_trackers_profile(RuntimeProfile* profile,
                                                   MemTrackerLimiter::Type type) {
    if (type == Type::GLOBAL) {
        std::lock_guard<std::mutex> l(
                ExecEnv::GetInstance()->mem_tracker_limiter_pool[0].group_lock);
        for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[0].trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                tracker->make_profile(profile);
            }
        }
    } else if (type == Type::METADATA) {
        std::lock_guard<std::mutex> l(
                ExecEnv::GetInstance()->mem_tracker_limiter_pool[1].group_lock);
        for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[1].trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                tracker->make_profile(profile);
            }
        }
    } else if (type == Type::CACHE) {
        std::lock_guard<std::mutex> l(
                ExecEnv::GetInstance()->mem_tracker_limiter_pool[2].group_lock);
        for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[2].trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                tracker->make_profile(profile);
            }
        }
    } else {
        for (unsigned i = 3; i < ExecEnv::GetInstance()->mem_tracker_limiter_pool.size(); ++i) {
            std::lock_guard<std::mutex> l(
                    ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].group_lock);
            for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].trackers) {
                auto tracker = trackerWptr.lock();
                if (tracker != nullptr && tracker->type() == type) {
                    tracker->make_profile(profile);
                }
            }
        }
    }
}

std::string MemTrackerLimiter::make_type_trackers_profile_str(MemTrackerLimiter::Type type) {
    std::unique_ptr<RuntimeProfile> profile_snapshot =
            std::make_unique<RuntimeProfile>("TypeMemTrackersSnapshot");
    make_type_trackers_profile(profile_snapshot.get(), type);
    std::stringstream ss;
    profile_snapshot->pretty_print(&ss);
    return ss.str();
}

void MemTrackerLimiter::make_top_consumption_tasks_tracker_profile(RuntimeProfile* profile,
                                                                   int top_num) {
    std::unique_ptr<RuntimeProfile> tmp_profile_snapshot =
            std::make_unique<RuntimeProfile>("tmpSnapshot");
    std::priority_queue<std::pair<int64_t, RuntimeProfile*>> max_pq;
    // start from 3, not include global/metadata/cache type.
    for (unsigned i = 3; i < ExecEnv::GetInstance()->mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(
                ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].group_lock);
        for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                auto* profile_snapshot = tracker->make_profile(tmp_profile_snapshot.get());
                max_pq.emplace(tracker->consumption(), profile_snapshot);
            }
        }
    }

    while (!max_pq.empty() && top_num > 0) {
        RuntimeProfile* profile_snapshot =
                profile->create_child(max_pq.top().second->name(), true, false);
        profile_snapshot->merge(max_pq.top().second);
        top_num--;
        max_pq.pop();
    }
}

void MemTrackerLimiter::make_all_tasks_tracker_profile(RuntimeProfile* profile) {
    std::unordered_map<Type, RuntimeProfile*> types_profile;
    types_profile[Type::QUERY] = profile->create_child("QueryTasks", true, false);
    types_profile[Type::LOAD] = profile->create_child("LoadTasks", true, false);
    types_profile[Type::COMPACTION] = profile->create_child("CompactionTasks", true, false);
    types_profile[Type::SCHEMA_CHANGE] = profile->create_child("SchemaChangeTasks", true, false);
    types_profile[Type::OTHER] = profile->create_child("OtherTasks", true, false);

    // start from 3, not include global/metadata/cache type.
    for (unsigned i = 3; i < ExecEnv::GetInstance()->mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(
                ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].group_lock);
        for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                // ResultBlockBufferBase will continue to exist for 5 minutes after the query ends, even if the
                // result buffer is empty, and will not be shown in the profile. of course, this code is tricky.
                if (tracker->consumption() == 0 &&
                    tracker->label().starts_with("ResultBlockBuffer")) {
                    continue;
                }
                tracker->make_profile(types_profile[tracker->type()]);
            }
        }
    }
}

void MemTrackerLimiter::print_log_usage(const std::string& msg) {
    if (_enable_print_log_usage) {
        _enable_print_log_usage = false;
        std::string detail = msg;
        detail += "\nProcess Memory Summary: " + GlobalMemoryArbitrator::process_mem_log_str();
        detail += "\n" + make_profile_str();
        LOG(WARNING) << detail;
    }
}

std::string MemTrackerLimiter::tracker_limit_exceeded_str() {
    std::string err_msg = fmt::format(
            "memory tracker limit exceeded, tracker label:{}, type:{}, limit "
            "{}, peak used {}, current used {}. backend {}, {}.",
            label(), type_string(_type), PrettyPrinter::print_bytes(limit()),
            PrettyPrinter::print_bytes(peak_consumption()),
            PrettyPrinter::print_bytes(consumption()), BackendOptions::get_localhost(),
            GlobalMemoryArbitrator::process_memory_used_str());
    if (_type == Type::QUERY || _type == Type::LOAD) {
        err_msg += fmt::format(
                " exec node:<{}>, can `set exec_mem_limit` to change limit, details see be.INFO.",
                doris::thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label());
    } else if (_type == Type::SCHEMA_CHANGE) {
        err_msg += fmt::format(
                " can modify `memory_limitation_per_thread_for_schema_change_bytes` in be.conf to "
                "change limit, details see be.INFO.");
    }
    return err_msg;
}

#include "common/compile_check_end.h"
} // namespace doris
