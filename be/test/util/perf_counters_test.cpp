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

#include "util/perf_counters.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

using namespace std;

namespace impala {

TEST(PerfCounterTest, Basic) {
    PerfCounters counters;
    EXPECT_TRUE(counters.AddDefaultCounters());

    counters.Snapshot("Before");

    double result = 0;

    for (int i = 0; i < 1000000; i++) {
        double d1 = rand() / (double)RAND_MAX;
        double d2 = rand() / (double)RAND_MAX;
        result = d1 * d1 + d2 * d2;
    }

    counters.Snapshot("After");

    for (int i = 0; i < 1000000; i++) {
        double d1 = rand() / (double)RAND_MAX;
        double d2 = rand() / (double)RAND_MAX;
        result = d1 * d1 + d2 * d2;
    }

    counters.Snapshot("After2");
    counters.PrettyPrint(&cout);
}

TEST(CpuInfoTest, Basic) {
    cout << CpuInfo::DebugString();
}

TEST(DiskInfoTest, Basic) {
    cout << DiskInfo::DebugString();
    cout << "Device name for disk 0: " << DiskInfo::device_name(0) << std::endl;

    int disk_id_home_dir = DiskInfo::disk_id("/home");
    cout << "Device name for '/home': " << DiskInfo::device_name(disk_id_home_dir) << std::endl;
}

} // namespace impala

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    impala::CpuInfo::Init();
    impala::DiskInfo::Init();
    impala::MemInfo::Init();
    return RUN_ALL_TESTS();
}
