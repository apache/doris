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
#include "testutil/column_helper.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
class SlotDescriptor;
class RowDescriptor;
class RuntimeState;
class TExprNode;

namespace vectorized {
class Block;
class VExprContext;

class MockLiteral final : public VLiteral {
public:
    MockLiteral(ColumnWithTypeAndName data) {
        _data_type = data.type;
        _column_ptr = data.column;
        _expr_name = data.name;
        if (!check_and_get_column<ColumnConst>(data.column.get())) {
            _mock_const_expr_col =
                    std::make_shared<ColumnPtrWrapper>(ColumnConst::create(data.column, 1));
        } else {
            _mock_const_expr_col = std::make_shared<ColumnPtrWrapper>(data.column);
        }
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        _prepare_finished = true;
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        _open_finished = true;
        return Status::OK();
    }
    const std::string& expr_name() const override { return _name; }

    template <typename DataType>
    static VExprContextSPtr create(const DataType::FieldType& value) {
        auto ctx = VExprContext::create_shared(std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataType>({value})));
        ctx->_prepared = true;
        ctx->_opened = true;
        return ctx;
    }

    template <typename DataType>
    static VExprContextSPtrs create(const std::vector<typename DataType::FieldType>& values) {
        VExprContextSPtrs ctxs;
        for (const auto& value : values) {
            ctxs.push_back(create<DataType>(value));
        }
        return ctxs;
    }

    template <typename DataType>
    static VExprContextSPtr create_const(typename DataType::FieldType value, size_t size) {
        ColumnPtr column_const =
                ColumnConst::create(ColumnHelper::create_column<DataType>({value}), size);

        ColumnWithTypeAndName data;
        data.type = std::make_shared<DataType>();
        data.column = column_const;
        data.name = "MockLiteral Const";

        auto ctx = VExprContext::create_shared(std::make_shared<MockLiteral>(data));
        ctx->_prepared = true;
        ctx->_opened = true;
        return ctx;
    }

    template <typename DataType>
    static VExprContextSPtrs create_const(const std::vector<typename DataType::FieldType>& values,
                                          size_t size) {
        VExprContextSPtrs ctxs;
        for (const auto& value : values) {
            ctxs.push_back(create_const<DataType>(value, size));
        }
        return ctxs;
    }

    Status get_const_col(VExprContext* context,
                         std::shared_ptr<ColumnPtrWrapper>* column_wrapper) override {
        *column_wrapper = _mock_const_expr_col;
        return Status::OK();
    }

private:
    const std::string _name = "MockLiteral";
    std::shared_ptr<ColumnPtrWrapper> _mock_const_expr_col;
};

} // namespace vectorized
} // namespace doris
