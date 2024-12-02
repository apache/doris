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
#include "olap/metadata_adder.h"
#include "olap/schema_cache.h"
#include "olap/tablet_schema_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"

namespace doris {

static bvar::Adder<int64_t> memory_all_tracked_sum_bytes("memory_all_tracked_sum_bytes");
static bvar::Adder<int64_t> memory_global_trackers_sum_bytes("memory_global_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_metadata_trackers_sum_bytes(
        "memory_metadata_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_cache_trackers_sum_bytes("memory_cache_trackers_sum_bytes");
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
#ifdef ADDRESS_SANITIZER
    _memory_overview_profile = std::make_unique<RuntimeProfile>("[ASAN]MemoryOverviewSnapshot");
#else
    _memory_overview_profile = std::make_unique<RuntimeProfile>("MemoryOverviewSnapshot");
#endif
    _global_memory_profile.set(std::make_unique<RuntimeProfile>("GlobalMemorySnapshot"));
    _metadata_memory_profile.set(std::make_unique<RuntimeProfile>("MetadataMemorySnapshot"));
    _cache_memory_profile.set(std::make_unique<RuntimeProfile>("CacheMemorySnapshot"));
    _top_memory_tasks_profile.set(std::make_unique<RuntimeProfile>("TopMemoryTasksSnapshot"));
    _tasks_memory_profile.set(std::make_unique<RuntimeProfile>("TasksMemorySnapshot"));
    init_memory_overview_counter();
}

void MemoryProfile::init_memory_overview_counter() {
    RuntimeProfile* untracked_memory_profile =
            _memory_overview_profile->create_child("UntrackedMemory", true, false);
    RuntimeProfile* tracked_memory_profile =
            _memory_overview_profile->create_child("TrackedMemory", true, false);
    RuntimeProfile* tasks_memory_overview_profile =
            tracked_memory_profile->create_child("TasksMemory", true, false);
    RuntimeProfile* tasks_memory_overview_details_profile =
            tasks_memory_overview_profile->create_child("Details", true, false);
    RuntimeProfile* global_memory_overview_profile =
            tracked_memory_profile->create_child("GlobalMemory", true, false);
    RuntimeProfile* metadata_memory_overview_profile =
            tracked_memory_profile->create_child("MetadataMemory", true, false);
    RuntimeProfile* cache_memory_overview_profile =
            tracked_memory_profile->create_child("CacheMemory", true, false);
    RuntimeProfile* jemalloc_memory_profile =
            tracked_memory_profile->create_child("JemallocMemory", true, false);
    RuntimeProfile* jemalloc_memory_details_profile =
            jemalloc_memory_profile->create_child("Details", true, false);

    // 1 add process memory counter
    _process_physical_memory_usage_counter = _memory_overview_profile->AddHighWaterMarkCounter(
            "PhysicalMemory(VmRSS)", TUnit::BYTES);
    _process_virtual_memory_usage_counter = _memory_overview_profile->AddHighWaterMarkCounter(
            "VirtualMemory(VmSize)", TUnit::BYTES);

    // 2 add untracked/tracked memory counter
    _untracked_memory_usage_counter =
            untracked_memory_profile->AddHighWaterMarkCounter("Memory", TUnit::BYTES);
    _tracked_memory_usage_counter =
            tracked_memory_profile->AddHighWaterMarkCounter("Memory", TUnit::BYTES);

    // 3 add Jemalloc memory counter
    _jemalloc_memory_usage_counter =
            jemalloc_memory_profile->AddHighWaterMarkCounter("Memory", TUnit::BYTES);
    _jemalloc_cache_usage_counter =
            jemalloc_memory_details_profile->AddHighWaterMarkCounter("Cache", TUnit::BYTES);
    _jemalloc_metadata_usage_counter =
            jemalloc_memory_details_profile->AddHighWaterMarkCounter("Metadata", TUnit::BYTES);

    // 4 add global/metadata/cache memory counter
    _global_usage_counter =
            global_memory_overview_profile->AddHighWaterMarkCounter("Memory", TUnit::BYTES);
    _metadata_usage_counter =
            metadata_memory_overview_profile->AddHighWaterMarkCounter("Memory", TUnit::BYTES);
    _cache_usage_counter =
            cache_memory_overview_profile->AddHighWaterMarkCounter("Memory", TUnit::BYTES);

    // 5 add tasks memory counter
    _tasks_memory_usage_counter =
            tasks_memory_overview_profile->AddHighWaterMarkCounter("Memory", TUnit::BYTES);
    // Reserved memory is the sum of all task reserved memory, is duplicated with all task memory counter.
    _reserved_memory_usage_counter = tasks_memory_overview_profile->AddHighWaterMarkCounter(
            "ReservedMemory", TUnit::BYTES, "Memory", 1);
    _query_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter("Query", TUnit::BYTES);
    _load_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter("Load", TUnit::BYTES);
    _load_all_memtables_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter("AllMemTablesMemory",
                                                                           TUnit::BYTES, "Load", 1);
    _compaction_usage_counter = tasks_memory_overview_details_profile->AddHighWaterMarkCounter(
            "Compaction", TUnit::BYTES);
    _schema_change_usage_counter = tasks_memory_overview_details_profile->AddHighWaterMarkCounter(
            "SchemaChange", TUnit::BYTES);
    _other_usage_counter =
            tasks_memory_overview_details_profile->AddHighWaterMarkCounter("Other", TUnit::BYTES);
}

void MemoryProfile::refresh_memory_overview_profile() {
    // 1 create profile
    std::unique_ptr<RuntimeProfile> global_memory_profile =
            std::make_unique<RuntimeProfile>("GlobalMemorySnapshot");
    std::unique_ptr<RuntimeProfile> metadata_memory_profile =
            std::make_unique<RuntimeProfile>("MetadataMemorySnapshot");
    std::unique_ptr<RuntimeProfile> cache_memory_profile =
            std::make_unique<RuntimeProfile>("CacheMemorySnapshot");
    std::unique_ptr<RuntimeProfile> top_memory_tasks_profile =
            std::make_unique<RuntimeProfile>("TopMemoryTasksSnapshot");

    // 2 refresh process memory counter
    COUNTER_SET(_process_physical_memory_usage_counter,
                PerfCounters::get_vm_rss()); // from /proc VmRSS VmHWM
    COUNTER_SET(_process_virtual_memory_usage_counter,
                PerfCounters::get_vm_size()); // from /proc VmSize VmPeak

    // 2 refresh metadata memory tracker
    ExecEnv::GetInstance()->tablets_no_cache_mem_tracker()->set_consumption(
            MetadataAdder<TabletMeta>::get_all_tablets_size() -
            TabletSchemaCache::instance()->value_mem_consumption() -
            SchemaCache::instance()->value_mem_consumption());
    ExecEnv::GetInstance()->rowsets_no_cache_mem_tracker()->set_consumption(
            MetadataAdder<RowsetMeta>::get_all_rowsets_size());
    ExecEnv::GetInstance()->segments_no_cache_mem_tracker()->set_consumption(
            MetadataAdder<segment_v2::Segment>::get_all_segments_estimate_size() -
            SegmentLoader::instance()->cache_mem_usage());

    // 4 refresh tracked memory counter
    std::unordered_map<MemTrackerLimiter::Type, int64_t> type_mem_sum = {
            {MemTrackerLimiter::Type::GLOBAL, 0},        {MemTrackerLimiter::Type::QUERY, 0},
            {MemTrackerLimiter::Type::LOAD, 0},          {MemTrackerLimiter::Type::COMPACTION, 0},
            {MemTrackerLimiter::Type::SCHEMA_CHANGE, 0}, {MemTrackerLimiter::Type::METADATA, 0},
            {MemTrackerLimiter::Type::CACHE, 0},         {MemTrackerLimiter::Type::OTHER, 0}};
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
            COUNTER_SET(_global_usage_counter, it.second);
            memory_global_trackers_sum_bytes
                    << it.second - memory_global_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::QUERY:
            COUNTER_SET(_query_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_query_trackers_sum_bytes
                    << it.second - memory_query_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::LOAD:
            COUNTER_SET(_load_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_load_trackers_sum_bytes
                    << it.second - memory_load_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::COMPACTION:
            COUNTER_SET(_compaction_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_compaction_trackers_sum_bytes
                    << it.second - memory_compaction_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::SCHEMA_CHANGE:
            COUNTER_SET(_schema_change_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_schema_change_trackers_sum_bytes
                    << it.second - memory_schema_change_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::METADATA:
            COUNTER_SET(_metadata_usage_counter, it.second);
            memory_metadata_trackers_sum_bytes
                    << it.second - memory_metadata_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::CACHE:
            COUNTER_SET(_cache_usage_counter, it.second);
            memory_cache_trackers_sum_bytes
                    << it.second - memory_cache_trackers_sum_bytes.get_value();
            break;
        case MemTrackerLimiter::Type::OTHER:
            COUNTER_SET(_other_usage_counter, it.second);
            tasks_trackers_mem_sum += it.second;
            memory_other_trackers_sum_bytes
                    << it.second - memory_other_trackers_sum_bytes.get_value();
        }
    }

    MemTrackerLimiter::make_type_trackers_profile(global_memory_profile.get(),
                                                  MemTrackerLimiter::Type::GLOBAL);
    MemTrackerLimiter::make_type_trackers_profile(metadata_memory_profile.get(),
                                                  MemTrackerLimiter::Type::METADATA);
    MemTrackerLimiter::make_type_trackers_profile(cache_memory_profile.get(),
                                                  MemTrackerLimiter::Type::CACHE);

    MemTrackerLimiter::make_top_consumption_tasks_tracker_profile(top_memory_tasks_profile.get(),
                                                                  15);

    COUNTER_SET(_tasks_memory_usage_counter, tasks_trackers_mem_sum);
    memory_all_tasks_memory_bytes << tasks_trackers_mem_sum -
                                             memory_all_tasks_memory_bytes.get_value();

    COUNTER_SET(_reserved_memory_usage_counter, GlobalMemoryArbitrator::process_reserved_memory());
    memory_reserved_memory_bytes << GlobalMemoryArbitrator::process_reserved_memory() -
                                            memory_reserved_memory_bytes.get_value();

    all_tracked_mem_sum += MemInfo::allocator_cache_mem();
    COUNTER_SET(_jemalloc_cache_usage_counter,
                static_cast<int64_t>(MemInfo::allocator_cache_mem()));
    all_tracked_mem_sum += MemInfo::allocator_metadata_mem();
    COUNTER_SET(_jemalloc_metadata_usage_counter,
                static_cast<int64_t>(MemInfo::allocator_metadata_mem()));
    COUNTER_SET(_jemalloc_memory_usage_counter,
                _jemalloc_cache_usage_counter->current_value() +
                        _jemalloc_metadata_usage_counter->current_value());

    COUNTER_SET(_tracked_memory_usage_counter, all_tracked_mem_sum);
    memory_all_tracked_sum_bytes << all_tracked_mem_sum - memory_all_tracked_sum_bytes.get_value();

    // 5 refresh untracked memory counter
    int64_t untracked_memory =
            _process_physical_memory_usage_counter->current_value() - all_tracked_mem_sum;
    COUNTER_SET(_untracked_memory_usage_counter, untracked_memory);
    memory_untracked_memory_bytes << untracked_memory - memory_untracked_memory_bytes.get_value();

    // 6 refresh additional tracker printed when memory exceeds limit.
    COUNTER_SET(
            _load_all_memtables_usage_counter,
            ExecEnv::GetInstance()->memtable_memory_limiter()->mem_tracker()->peak_consumption());
    COUNTER_SET(_load_all_memtables_usage_counter,
                ExecEnv::GetInstance()->memtable_memory_limiter()->mem_tracker()->consumption());

    // 7. reset profile
    _global_memory_profile.set(std::move(global_memory_profile));
    _metadata_memory_profile.set(std::move(metadata_memory_profile));
    _cache_memory_profile.set(std::move(cache_memory_profile));
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

    RuntimeProfile* memory_overview_profile =
            memory_profile_snapshot->create_child(_memory_overview_profile->name(), true, false);
    memory_overview_profile->merge(const_cast<RuntimeProfile*>(_memory_overview_profile.get()));

    auto global_memory_version_ptr = _global_memory_profile.get();
    RuntimeProfile* global_memory_profile =
            memory_profile_snapshot->create_child(global_memory_version_ptr->name(), true, false);
    global_memory_profile->merge(const_cast<RuntimeProfile*>(global_memory_version_ptr.get()));

    auto metadata_memory_version_ptr = _metadata_memory_profile.get();
    RuntimeProfile* metadata_memory_profile =
            memory_profile_snapshot->create_child(metadata_memory_version_ptr->name(), true, false);
    metadata_memory_profile->merge(const_cast<RuntimeProfile*>(metadata_memory_version_ptr.get()));

    auto cache_memory_version_ptr = _cache_memory_profile.get();
    RuntimeProfile* cache_memory_profile =
            memory_profile_snapshot->create_child(cache_memory_version_ptr->name(), true, false);
    cache_memory_profile->merge(const_cast<RuntimeProfile*>(cache_memory_version_ptr.get()));

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
        LOG(WARNING) << "\n" << print_metadata_memory_profile();
        LOG(WARNING) << "\n" << print_cache_memory_profile();
        LOG(WARNING) << "\n" << print_top_memory_tasks_profile();
    }
}

} // namespace doris
