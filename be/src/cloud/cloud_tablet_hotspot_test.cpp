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

#include "cloud/cloud_tablet_hotspot.h"

#include <gtest/gtest.h>

namespace doris::cloud {

TEST(TabletHotSpot, base) {
    HotspotCounter counter(1, 1, 1);
    int N = HotspotCounter::week_counters_size + 1;
    std::deque<uint64_t> day_qps_queue;
    uint64_t day_qps = 0;
    std::deque<uint64_t> week_qps_queue;
    uint64_t week_qps = 0;
    EXPECT_EQ(counter.qpd(), day_qps);
    EXPECT_EQ(counter.qpw(), week_qps);
    for (int i = 0; i < N; i++) {
        uint64_t i_1 = i + 1;
        counter.cur_counter.fetch_add(i_1);
        if (day_qps_queue.size() == HotspotCounter::day_counters_size + 1) {
            day_qps -= day_qps_queue.front();
            day_qps_queue.pop_front();
        }
        day_qps += i_1;
        day_qps_queue.push_back(i_1);
        if (week_qps_queue.size() == HotspotCounter::week_counters_size + 1) {
            week_qps -= week_qps_queue.front();
            week_qps_queue.pop_front();
        }
        week_qps += i_1;
        week_qps_queue.push_back(i_1);
        EXPECT_EQ(counter.qpd(), day_qps);
        EXPECT_EQ(counter.qpw(), week_qps);
        counter.make_dot_point();
    }
}

} // namespace doris::cloud