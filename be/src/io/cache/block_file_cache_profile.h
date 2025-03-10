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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "io/io_common.h"
#include "olap/olap_common.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"

namespace doris {
namespace io {

struct AtomicStatistics {
    std::atomic<int64_t> num_io_bytes_read_from_cache = 0;
    std::atomic<int64_t> num_io_bytes_read_from_remote = 0;
};

struct FileCacheProfile;

struct FileCacheMetric {
    FileCacheMetric(FileCacheProfile* profile) : profile(profile) {}

    void register_entity();
    void update_table_metrics() const;

    FileCacheMetric& operator=(const FileCacheMetric&) = delete;
    FileCacheMetric(const FileCacheMetric&) = delete;
    FileCacheProfile* profile = nullptr;
};

struct FileCacheProfile {
    static FileCacheProfile& instance() {
        static FileCacheProfile s_profile;
        return s_profile;
    }

    FileCacheProfile() {
        FileCacheStatistics stats;
        update(&stats);
    }

    void update(FileCacheStatistics* stats);

    std::mutex _mtx;
    // use shared_ptr for concurrent
    std::shared_ptr<AtomicStatistics> _profile;
    std::shared_ptr<FileCacheMetric> _file_cache_metric;
    std::shared_ptr<AtomicStatistics> report();
};

struct FileCacheProfileReporter {
    RuntimeProfile::Counter* num_local_io_total = nullptr;
    RuntimeProfile::Counter* num_remote_io_total = nullptr;
    RuntimeProfile::Counter* num_inverted_index_remote_io_total = nullptr;
    RuntimeProfile::Counter* local_io_timer = nullptr;
    RuntimeProfile::Counter* bytes_scanned_from_cache = nullptr;
    RuntimeProfile::Counter* bytes_scanned_from_remote = nullptr;
    RuntimeProfile::Counter* remote_io_timer = nullptr;
    RuntimeProfile::Counter* write_cache_io_timer = nullptr;
    RuntimeProfile::Counter* bytes_write_into_cache = nullptr;
    RuntimeProfile::Counter* num_skip_cache_io_total = nullptr;
    RuntimeProfile::Counter* read_cache_file_directly_timer = nullptr;
    RuntimeProfile::Counter* cache_get_or_set_timer = nullptr;
    RuntimeProfile::Counter* lock_wait_timer = nullptr;
    RuntimeProfile::Counter* get_timer = nullptr;
    RuntimeProfile::Counter* set_timer = nullptr;

    FileCacheProfileReporter(RuntimeProfile* profile) {
        static const char* cache_profile = "FileCache";
        ADD_TIMER_WITH_LEVEL(profile, cache_profile, 1);
        num_local_io_total = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NumLocalIOTotal", TUnit::UNIT,
                                                          cache_profile, 1);
        num_remote_io_total = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NumRemoteIOTotal", TUnit::UNIT,
                                                           cache_profile, 1);
        num_inverted_index_remote_io_total = ADD_CHILD_COUNTER_WITH_LEVEL(
                profile, "NumInvertedIndexRemoteIOTotal", TUnit::UNIT, cache_profile, 1);
        local_io_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "LocalIOUseTimer", cache_profile, 1);
        remote_io_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "RemoteIOUseTimer", cache_profile, 1);
        write_cache_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(profile, "WriteCacheIOUseTimer", cache_profile, 1);
        bytes_write_into_cache = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "BytesWriteIntoCache",
                                                              TUnit::BYTES, cache_profile, 1);
        num_skip_cache_io_total = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NumSkipCacheIOTotal",
                                                               TUnit::UNIT, cache_profile, 1);
        bytes_scanned_from_cache = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "BytesScannedFromCache",
                                                                TUnit::BYTES, cache_profile, 1);
        bytes_scanned_from_remote = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "BytesScannedFromRemote",
                                                                 TUnit::BYTES, cache_profile, 1);
        read_cache_file_directly_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(profile, "ReadCacheFileDirectlyTimer", cache_profile, 1);
        cache_get_or_set_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(profile, "CacheGetOrSetTimer", cache_profile, 1);
        lock_wait_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "LockWaitTimer", cache_profile, 1);
        get_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "GetTimer", cache_profile, 1);
        set_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "SetTimer", cache_profile, 1);
    }

    void update(const FileCacheStatistics* statistics) const {
        COUNTER_UPDATE(num_local_io_total, statistics->num_local_io_total);
        COUNTER_UPDATE(num_remote_io_total, statistics->num_remote_io_total);
        COUNTER_UPDATE(num_inverted_index_remote_io_total,
                       statistics->num_inverted_index_remote_io_total);
        COUNTER_UPDATE(local_io_timer, statistics->local_io_timer);
        COUNTER_UPDATE(remote_io_timer, statistics->remote_io_timer);
        COUNTER_UPDATE(write_cache_io_timer, statistics->write_cache_io_timer);
        COUNTER_UPDATE(bytes_write_into_cache, statistics->bytes_write_into_cache);
        COUNTER_UPDATE(num_skip_cache_io_total, statistics->num_skip_cache_io_total);
        COUNTER_UPDATE(bytes_scanned_from_cache, statistics->bytes_read_from_local);
        COUNTER_UPDATE(bytes_scanned_from_remote, statistics->bytes_read_from_remote);
        COUNTER_UPDATE(read_cache_file_directly_timer, statistics->read_cache_file_directly_timer);
        COUNTER_UPDATE(cache_get_or_set_timer, statistics->cache_get_or_set_timer);
        COUNTER_UPDATE(lock_wait_timer, statistics->lock_wait_timer);
        COUNTER_UPDATE(get_timer, statistics->get_timer);
        COUNTER_UPDATE(set_timer, statistics->set_timer);
    }
};

} // namespace io
} // namespace doris