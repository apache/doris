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

#include "join_test_helper.h"

#include "testutil/creators.h"

namespace doris::pipeline {

void JoinTestHelper::SetUp() {
    runtime_state = std::make_unique<MockRuntimeState>();
    obj_pool = std::make_unique<ObjectPool>();

    runtime_profile = std::make_shared<RuntimeProfile>("test");

    query_ctx = generate_one_query();

    runtime_state->_query_ctx = query_ctx.get();
    runtime_state->_query_id = query_ctx->query_id();
    runtime_state->resize_op_id_to_local_state(-100);
    runtime_state->set_max_operator_id(-100);

    ADD_TIMER(runtime_profile.get(), "ExecTime");
    runtime_profile->AddHighWaterMarkCounter("MemoryUsed", TUnit::BYTES, "", 0);
}

void JoinTestHelper::TearDown() {}

} // namespace doris::pipeline