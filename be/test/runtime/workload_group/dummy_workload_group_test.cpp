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

#include <gen_cpp/BackendService_types.h>
#include <gtest/gtest.h>

#include "runtime/exec_env.h"

namespace doris {

class DummyWorkloadGroupTest : public testing::Test {
    DummyWorkloadGroupTest() = default;
    ~DummyWorkloadGroupTest() override = default;
};

TEST_F(DummyWorkloadGroupTest, dummy_wg_basic_test) {
    ExecEnv env;
    Status ret = env.init_dummy_workload_group();
    ASSERT_TRUE(ret.ok());

    ASSERT_TRUE(env.dummy_workload_group()->id() == 0);
    ASSERT_TRUE(env.dummy_workload_group()->name() == "_dummpy_workload_group");
}

} // namespace doris