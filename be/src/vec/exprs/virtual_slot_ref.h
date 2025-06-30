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

#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

class VirtualSlotRef MOCK_REMOVE(final) : public VExpr {
    ENABLE_FACTORY_CREATOR(VirtualSlotRef);

public:
    VirtualSlotRef(const TExprNode& node);
    VirtualSlotRef(const SlotDescriptor* desc);

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    Status execute(VExprContext* context, Block* block, int* result_column_id) override;
    const std::string& expr_name() const override;
    std::string expr_label() override;
    std::string debug_string() const override;
    bool is_constant() const override { return false; }
    int column_id() const { return _column_id; }
    int slot_id() const { return _slot_id; }
    bool equals(const VExpr& other) override;
    size_t estimate_memory(const size_t rows) override { return 0; }
    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }
    std::shared_ptr<VExpr> get_virtual_column_expr() const { return _virtual_column_expr; }

    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override {
        return _virtual_column_expr->evaluate_inverted_index(context, segment_num_rows);
    }

    bool is_score_expr() const;

private:
    int _column_id;
    int _slot_id;
    const std::string* _column_name;
    const std::string _column_label;
    std::shared_ptr<VExpr> _virtual_column_expr;
    DataTypePtr _column_data_type;
};

} // namespace doris::vectorized