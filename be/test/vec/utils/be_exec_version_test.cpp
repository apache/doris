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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "agent/be_exec_version_manager.h"

namespace doris::vectorized {
TEST(be_exec_version_test, empty_buckets) {
    // YOU MUST NOT CHANGE THIS TEST !
    // if you want to change be_exec_version, you should know what you are doing
    EXPECT_EQ(BeExecVersionManager::get_newest_version(), 7);
    EXPECT_EQ(BeExecVersionManager::get_min_version(), 0);
}

} // namespace doris::vectorized
