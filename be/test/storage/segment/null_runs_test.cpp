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

#include "storage/segment/null_runs.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <tuple>
#include <vector>

#include "common/status.h"

namespace doris::segment_v2 {

namespace {

using NullRun = std::tuple<bool, size_t, size_t>;

std::vector<NullRun> runs_of(const std::vector<uint8_t>& null_map) {
    std::vector<NullRun> runs;
    const Status st = for_each_null_run(null_map.data(), null_map.size(),
                                        [&](bool is_null, size_t start, size_t len) {
                                            runs.emplace_back(is_null, start, len);
                                            return Status::OK();
                                        });
    EXPECT_TRUE(st.ok()) << st;
    return runs;
}

} // namespace

TEST(NullRunsTest, WalksTheLongestRunsInOrder) {
    EXPECT_EQ(runs_of({0, 0, 1, 1, 1, 0, 1}),
              (std::vector<NullRun> {{false, 0, 2}, {true, 2, 3}, {false, 5, 1}, {true, 6, 1}}));
    EXPECT_EQ(runs_of({1, 1}), (std::vector<NullRun> {{true, 0, 2}}));
    EXPECT_EQ(runs_of({0}), (std::vector<NullRun> {{false, 0, 1}}));
    EXPECT_TRUE(runs_of({}).empty());
}

TEST(NullRunsTest, StopsAtTheFirstError) {
    const std::vector<uint8_t> null_map {0, 1, 0};
    size_t calls = 0;
    const Status st = for_each_null_run(null_map.data(), null_map.size(),
                                        [&](bool /*is_null*/, size_t /*start*/, size_t /*len*/) {
                                            ++calls;
                                            return Status::InternalError("stop");
                                        });
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(calls, 1);
}

} // namespace doris::segment_v2
