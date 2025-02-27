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

#include "pipeline/exec/empty_set_operator.h"

#include <gtest/gtest.h>

#include "pipeline/operator/operator_helper.h"
#include "vec/core/block.h"
namespace doris::pipeline {

TEST(EmptySetSourceOperatorTest, test) {
    OperatorContext ctx;
    EmptySetSourceOperatorX op;
    OperatorHelper::init_local_state(ctx, op);

    bool eos = false;
    vectorized::Block block;
    EXPECT_TRUE(op.get_block(&ctx.state, &block, &eos).ok());
    EXPECT_EQ(eos, true);
    EXPECT_EQ(block.empty(), true);
}

} // namespace doris::pipeline
