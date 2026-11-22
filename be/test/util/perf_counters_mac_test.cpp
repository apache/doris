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

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "util/perf_counters.h"

namespace doris {

// refresh_proc_status() stores what it reads in process-wide counters that other tests expect to
// stay unrefreshed, so this test reads the kernel's values through get_proc_status() instead.
TEST(PerfCountersMacTest, ProcStatusReportsResidentMemory) {
    PerfCounters::ProcStatus before;
    PerfCounters::get_proc_status(&before);
    EXPECT_GT(before.vm_rss, 0);
    EXPECT_GE(before.vm_hwm, before.vm_rss);
    EXPECT_GE(before.vm_size, before.vm_rss);

    // Writing 64 MiB makes those pages resident, so the resident size has to grow with them.
    constexpr size_t TOUCHED_BYTES = 64UL * 1024 * 1024;
    std::vector<char> buffer(TOUCHED_BYTES);
    std::memset(buffer.data(), 1, buffer.size());
    PerfCounters::ProcStatus after;
    PerfCounters::get_proc_status(&after);
    EXPECT_EQ(buffer.back(), 1);
    EXPECT_GE(after.vm_rss - before.vm_rss, static_cast<int64_t>(TOUCHED_BYTES / 2));
    EXPECT_GE(after.vm_hwm, after.vm_rss);
}

} // namespace doris
