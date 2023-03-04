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

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "olap/olap_common.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"

namespace doris {
namespace io {

struct AtomicStatistics {
    std::atomic<int64_t> num_io_total = 0;
    std::atomic<int64_t> num_io_hit_cache = 0;
    std::atomic<int64_t> num_io_bytes_read_total = 0;
    std::atomic<int64_t> num_io_bytes_read_from_file_cache = 0;
    std::atomic<int64_t> num_io_bytes_read_from_write_cache = 0;
    std::atomic<int64_t> num_io_written_in_file_cache = 0;
    std::atomic<int64_t> num_io_bytes_written_in_file_cache = 0;
};

struct FileCacheProfile;

struct FileCacheMetric {
    FileCacheMetric(int64_t table_id, FileCacheProfile* profile)
            : profile(profile), table_id(table_id) {}

    FileCacheMetric(int64_t table_id, int64_t partition_id, FileCacheProfile* profile)
            : profile(profile), table_id(table_id), partition_id(partition_id) {}

    void register_entity();
    void deregister_entity() const {
        DorisMetrics::instance()->metric_registry()->deregister_entity(entity);
    }
    void update_table_metrics() const;
    void update_partition_metrics() const;

    FileCacheMetric& operator=(const FileCacheMetric&) = delete;
    FileCacheMetric(const FileCacheMetric&) = delete;
    FileCacheProfile* profile = nullptr;
    int64_t table_id = -1;
    int64_t partition_id = -1;
    std::shared_ptr<MetricEntity> entity;
    IntAtomicCounter* file_cache_num_io_total = nullptr;
    IntAtomicCounter* file_cache_num_io_hit_cache = nullptr;
    IntAtomicCounter* file_cache_num_io_bytes_read_total = nullptr;
    IntAtomicCounter* file_cache_num_io_bytes_read_from_file_cache = nullptr;
    IntAtomicCounter* file_cache_num_io_bytes_read_from_write_cache = nullptr;
    IntAtomicCounter* file_cache_num_io_written_in_file_cache = nullptr;
    IntAtomicCounter* file_cache_num_io_bytes_written_in_file_cache = nullptr;
};

struct FileCacheProfile {
    static FileCacheProfile& instance() {
        static FileCacheProfile s_profile;
        return s_profile;
    }

    FileCacheProfile() {
        OlapReaderStatistics stats;
        update(0, 0, &stats);
    }

    // avoid performance impact, use https to control
    inline static std::atomic<bool> s_enable_profile = true;

    static void set_enable_profile(bool flag) {
        // if enable_profile = false originally, set true, it will clear the count
        if (!s_enable_profile && flag) {
            std::lock_guard lock(instance()._mtx);
            instance()._profile.clear();
        }
        s_enable_profile.store(flag, std::memory_order_release);
    }

    void update(int64_t table_id, int64_t partition_id, OlapReaderStatistics* stats);

    void deregister_metric(int64_t table_id, int64_t partition_id);
    std::mutex _mtx;
    // use shared_ptr for concurrent
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::shared_ptr<AtomicStatistics>>>
            _profile;
    std::unordered_map<int64_t, std::shared_ptr<FileCacheMetric>> _table_metrics;
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::shared_ptr<FileCacheMetric>>>
            _partition_metrics;
    FileCacheStatistics report(int64_t table_id);
    FileCacheStatistics report(int64_t table_id, int64_t partition_id);
};

struct FileCacheProfileReporter {
    RuntimeProfile::Counter* num_io_total;
    RuntimeProfile::Counter* num_io_hit_cache;
    RuntimeProfile::Counter* num_io_bytes_read_total;
    RuntimeProfile::Counter* num_io_bytes_read_from_file_cache;
    RuntimeProfile::Counter* num_io_bytes_read_from_write_cache;
    RuntimeProfile::Counter* num_io_written_in_file_cache;
    RuntimeProfile::Counter* num_io_bytes_written_in_file_cache;
    RuntimeProfile::Counter* num_io_bytes_skip_cache;

    FileCacheProfileReporter(RuntimeProfile* profile) {
        static const char* cache_profile = "FileCache";
        ADD_TIMER(profile, cache_profile);
        num_io_total = ADD_CHILD_COUNTER(profile, "IOTotalNum", TUnit::UNIT, cache_profile);
        num_io_hit_cache = ADD_CHILD_COUNTER(profile, "IOHitCacheNum", TUnit::UNIT, cache_profile);
        num_io_bytes_read_total =
                ADD_CHILD_COUNTER(profile, "ReadTotalBytes", TUnit::BYTES, cache_profile);
        num_io_bytes_read_from_file_cache =
                ADD_CHILD_COUNTER(profile, "ReadFromFileCacheBytes", TUnit::BYTES, cache_profile);
        num_io_bytes_read_from_write_cache =
                ADD_CHILD_COUNTER(profile, "ReadFromWriteCacheBytes", TUnit::BYTES, cache_profile);
        num_io_written_in_file_cache =
                ADD_CHILD_COUNTER(profile, "WriteInFileCacheNum", TUnit::UNIT, cache_profile);
        num_io_bytes_written_in_file_cache =
                ADD_CHILD_COUNTER(profile, "WriteInFileCacheBytes", TUnit::BYTES, cache_profile);
        num_io_bytes_skip_cache =
                ADD_CHILD_COUNTER(profile, "SkipCacheBytes", TUnit::BYTES, cache_profile);
    }

    void update(FileCacheStatistics* statistics) {
        COUNTER_UPDATE(num_io_total, statistics->num_io_total);
        COUNTER_UPDATE(num_io_hit_cache, statistics->num_io_hit_cache);
        COUNTER_UPDATE(num_io_bytes_read_total, statistics->num_io_bytes_read_total);
        COUNTER_UPDATE(num_io_bytes_read_from_file_cache,
                       statistics->num_io_bytes_read_from_file_cache);
        COUNTER_UPDATE(num_io_bytes_read_from_write_cache,
                       statistics->num_io_bytes_read_from_write_cache);
        COUNTER_UPDATE(num_io_written_in_file_cache, statistics->num_io_written_in_file_cache);
        COUNTER_UPDATE(num_io_bytes_written_in_file_cache,
                       statistics->num_io_bytes_written_in_file_cache);
        COUNTER_UPDATE(num_io_bytes_skip_cache, statistics->num_io_bytes_skip_cache);
    }
};

} // namespace io
} // namespace doris