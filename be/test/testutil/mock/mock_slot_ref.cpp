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

#include "mock_slot_ref.h"

#include <gtest/gtest.h>

#include "testutil/mock/mock_runtime_state.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VExprContextSPtrs MockSlotRef::create_mock_contexts(DataTypePtr data_type) {
    auto ctx = VExprContext::create_shared(std::make_shared<MockSlotRef>(0, data_type));
    ctx->_prepared = true;
    ctx->_opened = true;
    return VExprContextSPtrs {ctx};
}

VExprContextSPtrs MockSlotRef::create_mock_contexts(int column_id, DataTypePtr data_type) {
    auto ctx = VExprContext::create_shared(std::make_shared<MockSlotRef>(column_id, data_type));
    ctx->_prepared = true;
    ctx->_opened = true;
    return VExprContextSPtrs {ctx};
}

VExprContextSPtr MockSlotRef::create_mock_context(int column_id, DataTypePtr data_type) {
    auto ctx = VExprContext::create_shared(std::make_shared<MockSlotRef>(column_id, data_type));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtrs MockSlotRef::create_mock_contexts(DataTypes data_types) {
    VExprContextSPtrs ctxs;
    for (int i = 0; i < data_types.size(); i++) {
        auto ctx = VExprContext::create_shared(std::make_shared<MockSlotRef>(i, data_types[i]));
        ctx->_prepared = true;
        ctx->_opened = true;
        ctxs.push_back(ctx);
    }
    return ctxs;
}

TEST(MockSlotRefTest, test) {
    auto old_ctx = MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

    for (auto ctx : old_ctx) {
        EXPECT_TRUE(ctx->root()->is_slot_ref());
    }

    VExprContextSPtrs new_ctx;
    new_ctx.resize(old_ctx.size());
    MockRuntimeState state;
    for (int i = 0; i < old_ctx.size(); i++) {
        auto st = old_ctx[i]->clone(&state, new_ctx[i]);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    for (auto ctx : new_ctx) {
        EXPECT_TRUE(ctx->root()->is_slot_ref());
    }
}

} // namespace doris::vectorized
