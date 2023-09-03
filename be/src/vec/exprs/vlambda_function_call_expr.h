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

#include "common/status.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vlambda_function_expr.h"

namespace doris::vectorized {

class VLambdaFunctionCallExpr : public VExpr {
    ENABLE_FACTORY_CREATOR(VLambdaFunctionCallExpr);

public:
    VLambdaFunctionCallExpr(const TExprNode& node) : VExpr(node) {}
    ~VLambdaFunctionCallExpr() override = default;

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

        std::vector<std::string_view> child_expr_name;
        for (auto child : _children) {
            child_expr_name.emplace_back(child->expr_name());
        }
        _expr_name = fmt::format("{}({})", _fn.name.function_name, child_expr_name);

        _lambda_function = LambdaFunctionFactory::instance().get_function(_fn.name.function_name);
        if (_lambda_function == nullptr) {
            return Status::InternalError("Lambda Function {} is not implemented.",
                                         _fn.name.function_name);
        }
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        return _lambda_function->execute(context, block, result_column_id, _data_type, _children);
    }

    std::string debug_string() const override {
        std::stringstream out;
        out << "VLambdaFunctionCallExpr[";
        out << _expr_name;
        out << "]{";
        bool first = true;
        for (auto& input_expr : children()) {
            if (first) {
                first = false;
            } else {
                out << ",";
            }
            out << "\n" << input_expr->debug_string();
        }
        out << "}";
        return out.str();
    }

private:
    std::string _expr_name;
    LambdaFunctionPtr _lambda_function;
};
} // namespace doris::vectorized
