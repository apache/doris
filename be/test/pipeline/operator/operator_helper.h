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
#pragma once
#include "pipeline/exec/operator.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
namespace doris::pipeline {

struct OperatorContext {
    OperatorContext() : profile("test") {};

    RuntimeProfile profile;
    MockRuntimeState state;

    ObjectPool pool;
};

struct OperatorHelper {
    static void init_local_state(OperatorContext& ctx, auto& op) {
        ctx.state.resize_op_id_to_local_state(-100);
        LocalStateInfo info {&ctx.profile, {}, 0, {}, 0};
        EXPECT_TRUE(op.setup_local_state(&ctx.state, info).ok());
    }

    static void init_local_state(OperatorContext& ctx, auto& op,
                                 const std::vector<TScanRangeParams>& scan_ranges) {
        ctx.state.resize_op_id_to_local_state(-100);
        LocalStateInfo info {&ctx.profile, scan_ranges, 0, {}, 0};
        EXPECT_TRUE(op.setup_local_state(&ctx.state, info).ok());
    }

    static bool is_block(std::vector<Dependency*> deps) {
        for (auto* dep : deps) {
            if (!dep->ready()) {
                return true;
            }
        }
        return false;
    }

    static bool is_ready(std::vector<Dependency*> deps) {
        for (auto* dep : deps) {
            if (!dep->ready()) {
                return false;
            }
        }
        return true;
    }
};

} // namespace doris::pipeline