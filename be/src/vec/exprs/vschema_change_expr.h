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
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type_object.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

// Sepecial type of expression which only acts on dynmiac column(ColumnObject)
// it sends schema change rpc to Frontend to add new generately columns
// from it's type and name.It contains an inner slot which indicated it's variant
// column.
class VSchemaChangeExpr : public VExpr {
public:
    VSchemaChangeExpr(const TExprNode& node) : VExpr(node), _tnode(node) {}
    ~VSchemaChangeExpr() = default;
    Status execute(VExprContext* context, doris::vectorized::Block* block,
                   int* result_column_id) override;
    Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                   VExprContext* context) override;
    Status open(doris::RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(doris::RuntimeState* state, VExprContext* context,
               FunctionContext::FunctionStateScope scope) override;
    VExpr* clone(doris::ObjectPool* pool) const override {
        return pool->add(new VSchemaChangeExpr(*this));
    }
    const std::string& expr_name() const override;
    std::string debug_string() const override;

private:
    std::string _expr_name;
    int32_t _table_id;
    int _slot_id;
    int _column_id;
    TExprNode _tnode;
    static const constexpr char* function_name = "SCHMA_CHANGE";
};

} // namespace doris::vectorized
