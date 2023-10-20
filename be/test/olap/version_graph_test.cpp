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

#include "olap/version_graph.h"

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <filesystem>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class VersionGraphTest : public testing::Test {
public:
    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(VersionGraphTest, consistency_version) {
    TimestampedVersionTracker version_tracker;
    Version version(0, 1);
    version_tracker.add_version(version);
    for (int i = 1; i <= 518; i++) {
        Version version(i, i);
        version_tracker.add_version(version);
    }

    Version spec_version(0, 507);
    std::vector<Version> version_graph;
    auto status = version_tracker.capture_consistent_versions(spec_version, &version_graph);
    EXPECT_TRUE(status.OK());
}

} // namespace doris
