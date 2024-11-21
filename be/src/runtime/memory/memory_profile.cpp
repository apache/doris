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

#include "runtime/memory/memory_profile.h"

#include "bvar/reducer.h"
#include "runtime/exec_env.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"

namespace doris {

static bvar::Adder<int64_t> memory_all_tracked_sum_bytes("memory_all_tracked_sum_bytes");
static bvar::Adder<int64_t> memory_global_trackers_sum_bytes("memory_global_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_query_trackers_sum_bytes("memory_query_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_load_trackers_sum_bytes("memory_load_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_compaction_trackers_sum_bytes(
        "memory_compaction_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_schema_change_trackers_sum_bytes(
        "memory_schema_change_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_other_trackers_sum_bytes("memory_other_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_reserved_memory_bytes("memory_reserved_memory_bytes");
static bvar::Adder<int64_t> memory_all_tasks_memory_bytes("memory_all_tasks_memory_bytes");
static bvar::Adder<int64_t> memory_untracked_memory_bytes("memory_untracked_memory_bytes");

MemoryProfile::MemoryProfile() {
    _memory_overview_profile.set(std::make_unique<RuntimeProfile>("MemoryOverviewSnapshot"));
    _global_memory_profile.set(std::make_unique<RuntimeProfile>("GlobalMemorySnapshot"));
    _top_memory_tasks_profile.set(std::make_unique<RuntimeProfile>("TopMemoryTasksSnapshot"));
    _tasks_memory_profile.set(std::make_unique<RuntimeProfile>("TasksMemorySnapshot"));
}

void MemoryProfile::refresh_memory_overview_profile() {
#ifdef ADDRESS_SANITIZER
    std::unique_ptr<RuntimeProfile> memory_overview_profile =
            std::make_unique<RuntimeProfile>("[ASAN]MemoryOverviewSnapshot");
#else
    std::unique_ptr<RuntimeProfile> memory_overview_profile =
            std::make_unique<RuntimeProfile>("MemoryOverviewSnapshot");
#endif
    std::unique_ptr<RuntimeProfile> global_memory_profile =
            std::make_unique<RuntimeProfile>("GlobalMemorySnapshot");
    std::unique_ptr<RuntimeProfile> top_memory_tasks_profile =
            std::make_unique<RuntimeProfile>("TopMemoryTasksSnapshot");

    // 1. create profile
    RuntimeProfile* untracked_memory_profile =
            memory_overview_profile->create_child("UntrackedMemory", true, false);
    RuntimeProfile* tracked_memory_profile =
            memory_overview_profile->create_child("TrackedMemory", true, false);
    RuntimeProfile* tasks_memory_overview_profile =
            tracked_memory_profile->create_child("TasksMemory", true, false);
    RuntimeProfile* tasks_memory_overview_details_profile =
            tasks_memory_overview_profile->create_child("Details", true, false);
    RuntimeProfile* global_memory_overview_profile =
            tracked_memory_profile->create_child("GlobalMemory", true, false);
    RuntimeProfile* jemalloc_memory_profile =
            tracked_memory_profile->create_child("JemallocMemory", true, false);
    RuntimeProfile* jemalloc_memory_details_profile =
            jemalloc_memory_profile->create_child("Details", true, false);

    // 2. add counter
    // 2.1 add process memory counter
    RuntimeProfile::Counter* process_physical_memory_current_usage_counter =
            ADD_COUNTER(memory_overview_profile, "PhysicalMemory(VmRSS)", TUnit::BYTES);
    RuntimeProfile::Counter* process_physical_memory_peak_usage_counter =
            memory_overview_profile->AddHighWaterMarkCounter("PhysicalMemoryPeak", TUnit::BYTES);
    RuntimeProfile::Counter* process_virtual_memory_current_usage_counter =
            ADD_COUNTER(memory_overview_profile, "VirtualMemory(VmSize)", TUnit::BYTES);
    RuntimeProfile::Counter* process_virtual_memory_peak_usage_counter =
            memory_overview_profile->AddHighWaterMarkCounter("VirtualMemoryPeak", TUnit::BYTES);

    // 2.2 add untracked memory counter
    RuntimeProfile::Counter* untracked_memory_current_usage_counter =
            ADD_COUNTER(untracked_memory_profile, "CurrentUsage", TUnit::BYTES);
    RuntimeProfile::Counter* untracked_memory_peak_usage_counter =
            untracked_memory_profile->AddHighWaterMarkCounter("PeakUsage", TUnit::BYTES);

    // 2.3 add tracked memory counter
    RuntimeProfile::Counter* tracked_memory_current_usage_counter =
            ADD_COUNTER(tracked_memory_profile, "CurrentUsage", TUnit::BYTES);
    RuntimeProfile::Counter* tracked_memory_peak_usage_counter =
            tracked_memory_profile->AddHighWaterMarkCounter("PeakUsage", TUnit::BYTES);

    // 2.4 add jemalloc memory counter
    RuntimeProfile::Counter* jemalloc_memory_current_usage_counter =
            ADD_COUNTER(jemalloc_memory_profile, "CurrentUsage", TUnit::BYTES);
    RuntimeProfile::Counter* jemalloc_memory_peak_usage_counter =
            jemalloc_memory_profile->AddHighWaterMarkCounter("PeakUsage", TUnit::BYTES);
    RuntimeProfile::Counter* jemalloc_cache_current_usage_counter =
            ADD_COUNTER(jemalloc_memory_details_profile, "Cache", TUnit::BYTES);
    RuntimeProfile::Counter* jemalloc_cache_peak_usage_counter =
            jemalloc_memory_details_profile->AddHighWaterMarkCounter("CachePeak", TUnit::BYTES);
    RuntimeProfile::Counter* jemalloc_metadata_current_usage_counter =
            ADD_COUNTER(jemalloc_memory_details_profile, "Metadata", TUnit::BYTES);
    RuntimeProfile::Counter* jemalloc_metadata_peak_usage_counter =
            jemalloc_memory_details_profile->AddHighWaterMarkCounter("MetadataPeak", TUnit::BYTES);

    // 2.5 add global memory counter
    RuntimeProfile::Counter* global_current_usage_counter =
            ADD_COUNTER(global_memory_overview_profile, "CurrentUsage", TUnit::BYTES);
    RuntimeProfile::Counter* global_peak_usage_counter =
            global_memory_overview_profile->AddHighWaterMarkCounter("PeakUsage", TUnit::BYTES);

    // 2.6 add tasks memory counter
    RuntimeProfile::Counter* tasks_memory_current_usage_counter =
            ADD_COUNTER_WITH_LEVEL(tasks_memory_overview_profile, "CurrentUsage", TUnit::BYTES, 1);
    // Reserved memory is the sum of all task reserved memory, is duplicated with all task memory counter.
    RuntimeProfile::Counter* reserved_memory_current_usage_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            tasks_memory_overview_profile, "ReservedMemory", TUnit::BYTES, "CurrentUsage", 1);
    RuntimeProfile::Counter* reserved_memory_peak_usage_counter =
            tasks_memory_overview_profile->AddHighWaterMarkCounter("ReservedMemoryPeak",
                                                                   TUnit::BYTES, "CurrentUsage", 1);
    RuntimeProfile::Counter* tasks_memory_peak_usage_counter =
            tasks_memory_overview_profile->AddHighWaterMarkCounter("PeakUsage", TUnit::BYTES);
    RuntimeProfile::Counter* query_current_usage_counter =
            ADD_COUNTER_WITH_LEVEL(tasks_memory_overview_details_profile, "Query", TUnit::BYTES, 1);
    RuntimeProfile::Counter* query_peak_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter(
                    "QueryPeak", TUnit::BYTES, "Query", 1);
    RuntimeProfile::Counter* load_current_usage_counter =
            ADD_COUNTER_WITH_LEVEL(tasks_memory_overview_details_profile, "Load", TUnit::BYTES, 1);
    RuntimeProfile::Counter* load_peak_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter("LoadPeak", TUnit::BYTES,
                                                                           "Load", 1);
    RuntimeProfile::Counter* load_all_memtables_current_usage_counter =
            ADD_CHILD_COUNTER_WITH_LEVEL(tasks_memory_overview_details_profile,
                                         "AllMemTablesMemory", TUnit::BYTES, "Load", 1);
    RuntimeProfile::Counter* load_all_memtables_peak_usage_counter =
            ADD_CHILD_COUNTER_WITH_LEVEL(tasks_memory_overview_details_profile,
                                         "AllMemTablesMemoryPeak", TUnit::BYTES, "Load", 1);
    RuntimeProfile::Counter* compaction_current_usage_counter = ADD_COUNTER_WITH_LEVEL(
            tasks_memory_overview_details_profile, "Compaction", TUnit::BYTES, 1);
    RuntimeProfile::Counter* compaction_peak_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter(
                    "CompactionPeak", TUnit::BYTES, "Compaction", 1);
    RuntimeProfile::Counter* schema_change_current_usage_counter = ADD_COUNTER_WITH_LEVEL(
            tasks_memory_overview_details_profile, "SchemaChange", TUnit::BYTES, 1);
    RuntimeProfile::Counter* schema_change_peak_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter(
                    "SchemaChangePeak", TUnit::BYTES, "SchemaChange", 1);
    RuntimeProfile::Counter* other_current_usage_counter =
            ADD_COUNTER_WITH_LEVEL(tasks_memory_overview_details_profile, "Other", TUnit::BYTES, 1);
    RuntimeProfile::Counter* other_peak_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter(
                    "OtherPeak", TUnit::BYTES, "Other", 1);
    // 3. refresh counter
    // 3.1 refresh process memory counter
    COUNTER_SET(process_physical_memory_current_usage_counter,
                PerfCounters::get_vm_rss()); // from /proc VmRSS VmHWM
    COUNTER_SET(process_physical_memory_peak_usage_counter, PerfCounters::get_vm_hwm());
    COUNTER_SET(process_virtual_memory_current_usage_counter,
                PerfCounters::get_vm_size()); // from /proc VmSize VmPeak
    COUNTER_SET(process_virtual_memory_peak_usage_counter, PerfCounters::get_vm_peak());

    // 3.2 refresh tracked memory counter
    std::unordered_map<MemTrackerLimiter::Type, int64_t> type_mem_sum = {
            {MemTrackerLimiter::Type::GLOBAL, 0},        {MemTrackerLimiter::Type::QUERY, 0},
            {MemTrackerLimiter::Type::LOAD, 0},          {MemTrackerLimiter::Type::COMPACTION, 0},
            {MemTrackerLimiter::Type::SCHEMA_CHANGE, 0}, {MemTrackerLimiter::Type::OTHER, 0}};
    // always ExecEnv::ready(), because Daemon::_stop_background_threads_latch
    for (auto& group : ExecEnv::GetInstance()->mem_tracker_limiter_pool) {
        std::lock_guard<std::mutex> l(group.group_lock);
        for (auto trackerWptr : group.trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                type_mem_sum[tracker->type()] += tracker->consumption();
            }
        }
    }

    int64_t all_tracked_mem_sum = 0;
    int64_t tasks_trackers_mem_sum = 0;
    for (auto it : type_mem_sum) {
        all_tracked_mem_sum += it.second;
        switch (it.first) {
        case MemTrackerLimiter::Type::GLOBAL:
            COUNTER_SET(global_current_usage_counter, it.second);
            COUNTER_SET(global_peak_usage_counter, it.second);
            memory_global_trackers_sum_bytes
                    << it.second - memory_global_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::QUERY:
            COUNTER_SET(query_current_usage_counter, it.second);
            COUNTER_SET(query_peak_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_query_trackers_sum_bytes
                    << it.second - memory_query_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::LOAD:
            COUNTER_SET(load_current_usage_counter, it.second);
            COUNTER_SET(load_peak_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_load_trackers_sum_bytes
                    << it.second - memory_load_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::COMPACTION:
            COUNTER_SET(compaction_current_usage_counter, it.second);
            COUNTER_SET(compaction_peak_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_compaction_trackers_sum_bytes
                    << it.second - memory_compaction_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::SCHEMA_CHANGE:
            COUNTER_SET(schema_change_current_usage_counter, it.second);
            COUNTER_SET(schema_change_peak_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_schema_change_trackers_sum_bytes
                    << it.second - memory_schema_change_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::OTHER:
            COUNTER_SET(other_current_usage_counter, it.second);
            COUNTER_SET(other_peak_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_other_trackers_sum_bytes
                    << it.second - memory_other_trackers_sum_bytes.get_value();
        }
    }

    MemTrackerLimiter::make_type_trackers_profile(global_memory_profile.get(),
                                                  MemTrackerLimiter::Type::GLOBAL);

    MemTrackerLimiter::make_top_consumption_tasks_tracker_profile(top_memory_tasks_profile.get(),
                                                                  15);

    COUNTER_SET(tasks_memory_current_usage_counter, tasks_trackers_mem_sum);
    COUNTER_SET(tasks_memory_peak_usage_counter, tasks_trackers_mem_sum);
    memory_all_tasks_memory_bytes << tasks_trackers_mem_sum -
                                             memory_all_tasks_memory_bytes.get_value();

    COUNTER_SET(reserved_memory_current_usage_counter,
                GlobalMemoryArbitrator::process_reserved_memory());
    COUNTER_SET(reserved_memory_peak_usage_counter,
                GlobalMemoryArbitrator::process_reserved_memory());
    memory_reserved_memory_bytes << GlobalMemoryArbitrator::process_reserved_memory() -
                                            memory_reserved_memory_bytes.get_value();

    all_tracked_mem_sum += MemInfo::allocator_cache_mem();
    COUNTER_SET(jemalloc_cache_current_usage_counter,
                static_cast<int64_t>(MemInfo::allocator_cache_mem()));
    COUNTER_SET(jemalloc_cache_peak_usage_counter,
                static_cast<int64_t>(MemInfo::allocator_cache_mem()));
    all_tracked_mem_sum += MemInfo::allocator_metadata_mem();
    COUNTER_SET(jemalloc_metadata_current_usage_counter,
                static_cast<int64_t>(MemInfo::allocator_metadata_mem()));
    COUNTER_SET(jemalloc_metadata_peak_usage_counter,
                static_cast<int64_t>(MemInfo::allocator_metadata_mem()));
    COUNTER_SET(jemalloc_memory_current_usage_counter,
                jemalloc_cache_current_usage_counter->value() +
                        jemalloc_metadata_current_usage_counter->value());
    COUNTER_SET(jemalloc_memory_peak_usage_counter,
                jemalloc_cache_current_usage_counter->value() +
                        jemalloc_metadata_current_usage_counter->value());

    COUNTER_SET(tracked_memory_current_usage_counter, all_tracked_mem_sum);
    COUNTER_SET(tracked_memory_peak_usage_counter, all_tracked_mem_sum);
    memory_all_tracked_sum_bytes << all_tracked_mem_sum - memory_all_tracked_sum_bytes.get_value();

    // 3.3 refresh untracked memory counter
    int64_t untracked_memory =
            process_physical_memory_current_usage_counter->value() - all_tracked_mem_sum;
    COUNTER_SET(untracked_memory_current_usage_counter, untracked_memory);
    COUNTER_SET(untracked_memory_peak_usage_counter, untracked_memory);
    memory_untracked_memory_bytes << untracked_memory - memory_untracked_memory_bytes.get_value();

    // 3.4 refresh additional tracker printed when memory exceeds limit.
    COUNTER_SET(load_all_memtables_current_usage_counter,
                ExecEnv::GetInstance()->memtable_memory_limiter()->mem_tracker()->consumption());
    COUNTER_SET(
            load_all_memtables_peak_usage_counter,
            ExecEnv::GetInstance()->memtable_memory_limiter()->mem_tracker()->peak_consumption());

    // 4. reset profile
    _memory_overview_profile.set(std::move(memory_overview_profile));
    _global_memory_profile.set(std::move(global_memory_profile));
    _top_memory_tasks_profile.set(std::move(top_memory_tasks_profile));
}

void MemoryProfile::refresh_tasks_memory_profile() {
    std::unique_ptr<RuntimeProfile> tasks_memory_profile =
            std::make_unique<RuntimeProfile>("AllTasksMemorySnapshot");
    MemTrackerLimiter::make_all_tasks_tracker_profile(tasks_memory_profile.get());
    _tasks_memory_profile.set(std::move(tasks_memory_profile));
}

void MemoryProfile::make_memory_profile(RuntimeProfile* profile) const {
    RuntimeProfile* memory_profile_snapshot = profile->create_child("MemoryProfile", true, false);

    auto memory_overview_version_ptr = _memory_overview_profile.get();
    RuntimeProfile* memory_overview_profile =
            memory_profile_snapshot->create_child(memory_overview_version_ptr->name(), true, false);
    memory_overview_profile->merge(const_cast<RuntimeProfile*>(memory_overview_version_ptr.get()));

    auto global_memory_version_ptr = _global_memory_profile.get();
    RuntimeProfile* global_memory_profile =
            memory_profile_snapshot->create_child(global_memory_version_ptr->name(), true, false);
    global_memory_profile->merge(const_cast<RuntimeProfile*>(global_memory_version_ptr.get()));

    auto top_memory_tasks_version_ptr = _top_memory_tasks_profile.get();
    RuntimeProfile* top_memory_tasks_profile = memory_profile_snapshot->create_child(
            top_memory_tasks_version_ptr->name(), true, false);
    top_memory_tasks_profile->merge(
            const_cast<RuntimeProfile*>(top_memory_tasks_version_ptr.get()));

    auto tasks_memory_version_ptr = _tasks_memory_profile.get();
    RuntimeProfile* tasks_memory_profile =
            memory_profile_snapshot->create_child(tasks_memory_version_ptr->name(), true, false);
    tasks_memory_profile->merge(const_cast<RuntimeProfile*>(tasks_memory_version_ptr.get()));
}

int64_t MemoryProfile::query_current_usage() {
    return memory_query_trackers_sum_bytes.get_value();
}
int64_t MemoryProfile::load_current_usage() {
    return memory_load_trackers_sum_bytes.get_value();
}
int64_t MemoryProfile::compaction_current_usage() {
    return memory_compaction_trackers_sum_bytes.get_value();
}
int64_t MemoryProfile::schema_change_current_usage() {
    return memory_schema_change_trackers_sum_bytes.get_value();
}
int64_t MemoryProfile::other_current_usage() {
    return memory_other_trackers_sum_bytes.get_value();
}

void MemoryProfile::print_log_process_usage() {
    if (_enable_print_log_process_usage) {
        _enable_print_log_process_usage = false;
        LOG(WARNING) << "Process Memory Summary: " + GlobalMemoryArbitrator::process_mem_log_str();
        LOG(WARNING) << "\n" << print_memory_overview_profile();
        LOG(WARNING) << "\n" << print_global_memory_profile();
        LOG(WARNING) << "\n" << print_top_memory_tasks_profile();
    }
}

} // namespace doris
