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
// This file is copied from

#include "runtime/memory/mem_tracker.h"

#include <bvar/reducer.h>

namespace doris {

constexpr size_t MEM_TRACKERS_GROUP_NUM = 1000;
std::atomic<long> mem_tracker_group_counter(0);
bvar::Adder<int64_t> g_memtracker_cnt("memtracker_cnt");

std::vector<MemTracker::TrackersGroup> MemTracker::mem_tracker_pool(MEM_TRACKERS_GROUP_NUM);

MemTracker::MemTracker(const std::string& label) {
    _label = label;
    _group_num = mem_tracker_group_counter.fetch_add(1) % MEM_TRACKERS_GROUP_NUM;
    {
        std::lock_guard<std::mutex> l(mem_tracker_pool[_group_num].group_lock);
        _trackers_group_it = mem_tracker_pool[_group_num].trackers.insert(
                mem_tracker_pool[_group_num].trackers.end(), this);
    }
    g_memtracker_cnt << 1;
}

MemTracker::~MemTracker() {
    if (_group_num != -1) {
        std::lock_guard<std::mutex> l(mem_tracker_pool[_group_num].group_lock);
        if (_trackers_group_it != mem_tracker_pool[_group_num].trackers.end()) {
            mem_tracker_pool[_group_num].trackers.erase(_trackers_group_it);
            _trackers_group_it = mem_tracker_pool[_group_num].trackers.end();
        }
        g_memtracker_cnt << -1;
    }
}

} // namespace doris