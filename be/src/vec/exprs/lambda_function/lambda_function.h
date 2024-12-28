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

#include <runtime/runtime_state.h>

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
class VExpr;
class LambdaFunction {
public:
    virtual ~LambdaFunction() = default;

    virtual std::string get_name() const = 0;

    virtual doris::Status prepare(RuntimeState* state) {
        batch_size = state->batch_size();
        return Status::OK();
    }

    virtual doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                                  int* result_column_id, const DataTypePtr& result_type,
                                  const VExprSPtrs& children) = 0;

    int batch_size;
};

using LambdaFunctionPtr = std::shared_ptr<LambdaFunction>;

} // namespace doris::vectorized
