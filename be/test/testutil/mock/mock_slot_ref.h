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
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
class SlotDescriptor;
class RowDescriptor;
class RuntimeState;
class TExprNode;

namespace vectorized {
class Block;
class VExprContext;

// use to mock a slot ref expr
class MockSlotRef final : public VSlotRef {
public:
    MockSlotRef(int column_id) {
        _node_type = TExprNodeType::SLOT_REF;
        _column_id = column_id;
    }

    MockSlotRef(int column_id, DataTypePtr data_type) {
        _node_type = TExprNodeType::SLOT_REF;
        _column_id = column_id;
        _data_type = data_type;
    }

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        *result_column_id = _column_id;
        return Status::OK();
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

    void set_expr_name(String s) {
        _name = s;
    }

    static VExprContextSPtrs create_mock_contexts(DataTypePtr data_type);

    static VExprContextSPtrs create_mock_contexts(int column_id, DataTypePtr data_type);

    static VExprContextSPtr create_mock_context(int column_id, DataTypePtr data_type);

    static VExprContextSPtrs create_mock_contexts(DataTypes data_types);

private:
    std::string _name = "MockSlotRef";
};

} // namespace vectorized
} // namespace doris
