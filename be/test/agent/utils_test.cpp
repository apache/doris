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

#include "agent/utils.h"

#include <algorithm>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "service/backend_options.h"
#include "util/logging.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

TEST(AgentUtilsTest, Test) {
    std::string host_name = BackendOptions::get_localhost();
    int cnt = std::count(host_name.begin(), host_name.end(), '.');
    EXPECT_EQ(3, cnt);
}

} // namespace doris
