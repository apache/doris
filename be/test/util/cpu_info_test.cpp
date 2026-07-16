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

#include "util/cpu_info.h"

#include <gtest/gtest.h>

#include "common/config.h"

namespace doris {
namespace {

class NumCoresConfigGuard {
public:
    NumCoresConfigGuard() : _num_cores(config::num_cores) {}

    ~NumCoresConfigGuard() { config::num_cores = _num_cores; }

private:
    int _num_cores;
};

TEST(CpuInfoTest, NumCoresAppliesLatestConfiguredLimit) {
    NumCoresConfigGuard guard;

    config::num_cores = 2;
    EXPECT_EQ(2, CpuInfo::num_cores());

    config::num_cores = 4;
    EXPECT_EQ(4, CpuInfo::num_cores());
}

} // namespace
} // namespace doris
