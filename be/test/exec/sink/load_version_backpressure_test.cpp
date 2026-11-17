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

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

#include "common/config.h"
#include "exec/sink/load_stream_stub.h"
#include "exec/sink/writer/vtablet_writer.h"
#include "util/defer_op.h"

namespace doris {

TEST(LoadVersionBackpressureTest, SenderWaitMatchesPercentageAndLimit) {
    const auto previous_threshold = config::load_back_pressure_version_threshold;
    const auto previous_max_wait = config::max_load_back_pressure_version_wait_time_ms;
    Defer restore {[&] {
        config::load_back_pressure_version_threshold = previous_threshold;
        config::max_load_back_pressure_version_wait_time_ms = previous_max_wait;
    }};

    TDataSink sink;
    VExprContextSPtrs output_exprs;
    VTabletWriter parent(sink, output_exprs, nullptr, nullptr);
    IndexChannel index(&parent, 1, nullptr);
    VNodeChannel node(&parent, &index, 1);
    LoadStreamStub stream(PUniqueId {}, 1, nullptr, nullptr);

    auto expect_wait = [&](const auto& infos, int64_t expected_wait_ms) {
        node._load_back_pressure_version_wait_time_ms.store(0);
        node._refresh_back_pressure_version_wait_time(infos);
        stream._refresh_back_pressure_version_wait_time(infos);
        EXPECT_EQ(node._load_back_pressure_version_wait_time_ms.load(), expected_wait_ms);
        EXPECT_EQ(stream.get_and_reset_load_back_pressure_version_wait_time_ms(), expected_wait_ms);
    };

    struct TestCase {
        int64_t threshold;
        int32_t max_versions;
        int32_t current_versions;
        int64_t max_wait_ms;
        int64_t expected_wait_ms;
    };
    constexpr auto max_versions = std::numeric_limits<int32_t>::max();
    const TestCase cases[] = {
            {1, 2000, 21, 3000, 1001},
            {50, 2000, 1001, 3000, 1001},
            {80, 2000, 1601, 3000, 1001},
            {100, 2000, 2001, 3000, 1001},
            // Fractional high load points are rounded down after multiplication.
            {80, 1999, 1600, 3000, 1001},
            {1, 1, 1, 3000, 1001},
            // Calculate in int64_t: INT32_MAX * 80 / 100 = 1717986917.
            {80, max_versions, 1717986918, 3000, 1001},
            {100, max_versions - 1, max_versions, 3000, 1001},
            // Adding the base wait must also preserve values above INT32_MAX.
            {1, 1, max_versions, std::numeric_limits<int64_t>::max(),
             static_cast<int64_t>(max_versions) + 1000},
            {80, 2000, 1800, 1100, 1100},
            {80, 2000, 1601, 500, 500},
            // Feedback may be at or below the sender's threshold when BE configs differ.
            {80, 2000, 1600, 3000, 0},
            {80, 2000, 1500, 3000, 0},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(::testing::Message() << "threshold=" << test_case.threshold
                                          << ", max_versions=" << test_case.max_versions
                                          << ", current_versions=" << test_case.current_versions
                                          << ", max_wait_ms=" << test_case.max_wait_ms);
        config::load_back_pressure_version_threshold = test_case.threshold;
        config::max_load_back_pressure_version_wait_time_ms = test_case.max_wait_ms;
        google::protobuf::RepeatedPtrField<PTabletLoadRowsetInfo> infos;
        auto* info = infos.Add();
        info->set_max_config_rowset_nums(test_case.max_versions);
        info->set_current_rowset_nums(test_case.current_versions);
        expect_wait(infos, test_case.expected_wait_ms);
    }

    // The largest excess across tablets determines the wait, using each tablet's own limit.
    config::load_back_pressure_version_threshold = 80;
    config::max_load_back_pressure_version_wait_time_ms = 3000;
    google::protobuf::RepeatedPtrField<PTabletLoadRowsetInfo> infos;
    auto* first = infos.Add();
    first->set_max_config_rowset_nums(2000);
    first->set_current_rowset_nums(1601);
    auto* second = infos.Add();
    second->set_max_config_rowset_nums(1000);
    second->set_current_rowset_nums(1000);
    expect_wait(infos, 1200);
    infos.Clear();
    expect_wait(infos, 0);
}

} // namespace doris
