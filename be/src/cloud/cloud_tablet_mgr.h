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

#include <functional>
#include <memory>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"

namespace doris {

class CloudTablet;
class CloudStorageEngine;
class LRUCachePolicy;
class CountDownLatch;

class CloudTabletMgr {
public:
    CloudTabletMgr(CloudStorageEngine& engine);
    ~CloudTabletMgr();

    // If the tablet is in cache, return this tablet directly; otherwise will get tablet meta first,
    // sync rowsets after, and download segment data in background if `warmup_data` is true.
    Result<std::shared_ptr<CloudTablet>> get_tablet(int64_t tablet_id, bool warmup_data = false);

    void erase_tablet(int64_t tablet_id);

    void vacuum_stale_rowsets(const CountDownLatch& stop_latch);

    // Return weak ptr of all cached tablets.
    // We return weak ptr to avoid extend lifetime of tablets that are no longer cached.
    std::vector<std::weak_ptr<CloudTablet>> get_weak_tablets();

    void sync_tablets(const CountDownLatch& stop_latch);

    /**
     * Gets top N tablets that are considered to be compacted first
     *
     * @param n max number of tablets to get, all of them are comapction enabled
     * @param filter_out a filter takes a tablet and return bool to check
     *                   whether skipping the tablet, true for skip
     * @param tablets output param
     * @param max_score output param, max score of existed tablets
     * @return status of this call
     */
    Status get_topn_tablets_to_compact(int n, CompactionType compaction_type,
                                       const std::function<bool(CloudTablet*)>& filter_out,
                                       std::vector<std::shared_ptr<CloudTablet>>* tablets,
                                       int64_t* max_score);

private:
    CloudStorageEngine& _engine;

    class TabletMap;
    std::unique_ptr<TabletMap> _tablet_map;
    std::unique_ptr<LRUCachePolicy> _cache;
};

} // namespace doris
