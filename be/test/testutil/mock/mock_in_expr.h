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
#include <memory>
#include <string>

#include "common/status.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vin_predicate.h"

namespace doris::vectorized {
class Block;
class VExprContext;
// use to mock a slot ref expr
class MockInExpr final : public VInPredicate {
public:
    MockInExpr() = default;

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        return Status::OK();
    }
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;

    static std::shared_ptr<MockInExpr> create() { return std::make_shared<MockInExpr>(); }

    static VExprContextSPtr create_with_ctx(ColumnPtr column, bool is_not_in = false);
};

} // namespace doris::vectorized
