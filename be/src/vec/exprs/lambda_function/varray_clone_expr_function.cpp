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

#include <fmt/core.h>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

class ArrayCloneExprFunction : public LambdaFunction {
public:
    ~ArrayCloneExprFunction() override = default;

    static constexpr auto name = "array_clone_expr";

    static LambdaFunctionPtr create() { return std::make_shared<ArrayCloneExprFunction>(); }

    std::string get_name() const override { return name; }

    doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                          int* result_column_id, DataTypePtr result_type,
                          const std::vector<VExpr*>& children) override {
        ///* array_clone_expr(expr) *///

        doris::vectorized::ColumnNumbers arguments(children.size());
        RETURN_IF_ERROR(children[0]->execute(context, block, result_column_id));
        return Status::OK();
    }
};

void register_function_array_clone_expr(doris::vectorized::LambdaFunctionFactory& factory) {
    factory.register_function<ArrayCloneExprFunction>();
}
} // namespace doris::vectorized
