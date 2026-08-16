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

#include <gen_cpp/BackendService_types.h>

#include <algorithm>
#include <atomic>
#include <mutex>

#include "common/config.h"
#include "common/logging.h"
#include "exprs/function/dictionary.h"
#include "util/debug_points.h"
#include "util/time.h"

namespace doris {
class MemTrackerLimiter;
}
namespace doris {

class DictionaryFactory : private boost::noncopyable {
public:
    DictionaryFactory();
    ~DictionaryFactory();

    // Returns nullptr if failed
    std::shared_ptr<const IDictionary> get(int64_t dict_id, int64_t version_id) {
        // simulate slow query holding old version_id
        DBUG_EXECUTE_IF("dict_get_delay", {
            int sleep_sec = dp->param<int>("sleep_sec", 10);
            LOG(INFO) << "debug point dict_get_delay: sleeping " << sleep_sec
                      << "s before get dict_id=" << dict_id << " version_id=" << version_id;
            sleep(sleep_sec);
        });
        std::shared_lock lc(_mutex);
        auto it = _dict_id_to_versioned_map.find(dict_id);
        if (it != _dict_id_to_versioned_map.end()) {
            auto vit = it->second.find(version_id);
            if (vit != it->second.end()) {
                return vit->second;
            }
        }
        // fallback to staging: version may have been increased by FE but not yet committed
        auto rit = _refreshing_dict_map.find(dict_id);
        if (rit != _refreshing_dict_map.end() && rit->second.first == version_id) {
            LOG_WARNING(
                    "DictionaryFactory version not found in committed map, falling back to staging")
                    .tag("dict_id", dict_id)
                    .tag("version_id", version_id);
            return rit->second.second;
        }
        return nullptr;
    }

    Status refresh_dict(int64_t dict_id, int64_t version_id, DictionaryPtr dict) {
        VLOG_DEBUG << "DictionaryFactory refresh dictionary"
                   << " dict_id: " << dict_id << " version_id: " << version_id
                   << " dict name: " << dict->dict_name();
        std::unique_lock lc(_mutex);
        dict->_mem_tracker = _mem_tracker;
        _refreshing_dict_map[dict_id] = std::make_pair(version_id, dict);
        // Set the mem tracker for the dictionary
        return Status::OK();
    }

    Status abort_refresh_dict(int64_t dict_id, int64_t version_id) {
        VLOG_DEBUG << "DictionaryFactory abort refresh dictionary"
                   << " dict_id: " << dict_id << " version_id: " << version_id;
        std::unique_lock lc(_mutex);
        if (!_refreshing_dict_map.contains(dict_id)) {
            // FE will abort all, including succeed and failed.
            return Status::OK();
        }
        auto [refresh_version_id, dict] = _refreshing_dict_map[dict_id];
        if (version_id != refresh_version_id) {
            return Status::InvalidArgument(
                    "Version ID is not equal to the refreshing version ID. {} : {}", version_id,
                    refresh_version_id);
        }
        _refreshing_dict_map.erase(dict_id);
        return Status::OK();
    }

    Status commit_refresh_dict(int64_t dict_id, int64_t version_id) {
        VLOG_DEBUG << "DictionaryFactory commit refresh dictionary"
                   << " dict_id: " << dict_id << " version_id: " << version_id;
        std::unique_lock lc(_mutex);
        if (!_refreshing_dict_map.contains(dict_id)) {
            return Status::InvalidArgument("Dictionary is not refreshing dict_id: {}", dict_id);
        }
        auto [refresh_version_id, dict] = _refreshing_dict_map[dict_id];
        if (version_id != refresh_version_id) {
            return Status::InvalidArgument(
                    "Version ID is not equal to the refreshing version ID. {} : {}", version_id,
                    refresh_version_id);
        }
        auto& versioned_map = _dict_id_to_versioned_map[dict_id];
        if (!versioned_map.empty()) {
            int64_t latest = versioned_map.rbegin()->first;
            if (version_id <= latest) {
                LOG_WARNING(
                        "DictionaryFactory Failed to commit dictionary because version ID "
                        "is not greater than the existing version ID")
                        .tag("dict_id", dict_id)
                        .tag("version_id", version_id)
                        .tag("dict name", dict->dict_name())
                        .tag("existing version ID", latest);
                return Status::InvalidArgument(
                        "Version ID is not greater than the existing version ID for the "
                        "dictionary. {} : {}",
                        version_id, latest);
            }
        }
        LOG_INFO("DictionaryFactory Successfully commit dictionary")
                .tag("dict_id", dict_id)
                .tag("version_id", version_id)
                .tag("dict name", dict->dict_name());
        dict->set_commit_time_ms(UnixMillis());
        versioned_map[version_id] = dict;
        _refreshing_dict_map.erase(dict_id);
        lc.unlock();
        gc_if_needed();
        return Status::OK();
    }

    Status delete_dict(int64_t dict_id) {
        VLOG_DEBUG << "DictionaryFactory delete dictionary, dict_id: " << dict_id;
        std::unique_lock lc(_mutex);
        auto it = _dict_id_to_versioned_map.find(dict_id);
        if (it == _dict_id_to_versioned_map.end()) {
            return Status::OK();
        }
        if (it->second.empty()) {
            LOG_WARNING("DictionaryFactory delete dictionary with empty version map")
                    .tag("dict_id", dict_id);
        } else {
            auto latest_it = it->second.rbegin();
            LOG_INFO("DictionaryFactory Successfully delete dictionary")
                    .tag("dict_id", dict_id)
                    .tag("dict name", latest_it->second->dict_name())
                    .tag("latest version_id", latest_it->first);
        }
        _dict_id_to_versioned_map.erase(it);
        return Status::OK();
    }

    std::shared_ptr<MemTrackerLimiter> mem_tracker() const { return _mem_tracker; }

    // unified GC entry: count-based + ttl-based, with interval protection
    void gc_if_needed() {
        int64_t gc_interval_ms = std::max(1, config::dictionary_gc_interval_seconds) * 1000LL;
        int64_t now = UnixMillis();
        if (now - _last_gc_time_ms.load(std::memory_order_relaxed) < gc_interval_ms) {
            return;
        }
        std::unique_lock<std::shared_mutex> lc(_mutex);
        // re-check under lock to avoid duplicate GC
        if (now - _last_gc_time_ms.load(std::memory_order_relaxed) < gc_interval_ms) {
            return;
        }
        _last_gc_time_ms.store(now, std::memory_order_relaxed);
        _gc_all_no_lock(now);
    }

    void get_dictionary_status(std::vector<TDictionaryStatus>& result,
                               std::vector<int64_t> dict_ids);

private:
    // GC all dicts: first count-based, then ttl-based. Always keeps the latest version.
    void _gc_all_no_lock(int64_t now) {
        int32_t max_versions = std::max(1, config::dictionary_max_versions);
        int64_t ttl_ms = static_cast<int64_t>(config::dictionary_version_ttl_seconds) * 1000;
        int64_t threshold_ms = ttl_ms > 0 ? now - ttl_ms : 0;
        for (auto& [dict_id, versioned_map] : _dict_id_to_versioned_map) {
            if (versioned_map.size() <= 1) {
                continue;
            }
            // count-based: drop oldest while exceeding max_versions
            while (versioned_map.size() > static_cast<size_t>(max_versions)) {
                auto it = versioned_map.begin();
                LOG_INFO("DictionaryFactory GC old version by count")
                        .tag("dict_id", dict_id)
                        .tag("version_id", it->first)
                        .tag("dict name", it->second->dict_name());
                versioned_map.erase(it);
            }
            // ttl-based: drop non-latest versions older than ttl
            while (ttl_ms > 0 && versioned_map.size() > 1) {
                auto it = versioned_map.begin();
                if (it->second->commit_time_ms() >= threshold_ms) {
                    break;
                }
                LOG_INFO("DictionaryFactory GC old version by ttl")
                        .tag("dict_id", dict_id)
                        .tag("version_id", it->first)
                        .tag("age_sec", (now - it->second->commit_time_ms()) / 1000);
                versioned_map.erase(it);
            }
        }
    }

    // dict_id -> (version_id -> dict)
    std::map<int64_t, std::map<int64_t, DictionaryPtr>> _dict_id_to_versioned_map;

    std::map<int64_t, std::pair<int64_t, DictionaryPtr>>
            _refreshing_dict_map; // dict_id -> (version_id, dict)

    std::shared_mutex _mutex;

    std::atomic<int64_t> _last_gc_time_ms {0};

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
};

} // namespace doris
