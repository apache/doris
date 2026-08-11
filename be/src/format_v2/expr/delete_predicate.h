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

#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/vexpr.h"
#include "roaring/roaring64map.hh"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;
class Block;
class VExprContext;
} // namespace doris

namespace doris::format {

class DeletePredicate final : public VExpr {
    ENABLE_FACTORY_CREATOR(DeletePredicate);

public:
    DeletePredicate(const std::vector<int64_t>& deleted_rows);
    DeletePredicate(const roaring::Roaring64Map& deletion_vector);
    ~DeletePredicate() override = default;
    Status execute(VExprContext* context, Block* block, int* result_column_id) const override;
    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        return Status::InternalError("Not implement DeletePredicate::execute_column_impl");
    }
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    std::string debug_string() const override;
    uint64_t get_digest(uint64_t seed) const override { return 0; }
    const std::string& expr_name() const override { return _expr_name; }

private:
    std::string _expr_name;
    const std::vector<int64_t>* _deleted_rows = nullptr;
    const roaring::Roaring64Map* _deletion_vector = nullptr;
};
} // namespace doris::format
