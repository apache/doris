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
#include "vec/exprs/vexpr.h"

namespace doris {
class SlotDescriptor;
class RowDescriptor;
class RuntimeState;
class TExprNode;

namespace vectorized {
class Block;
class VExprContext;

class VSlotRef final : public VExpr {
    ENABLE_FACTORY_CREATOR(VSlotRef);

public:
    VSlotRef(const TExprNode& node);
    VSlotRef(const SlotDescriptor* desc);
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

private:
    int _slot_id;
    int _column_id;
    const std::string* _column_name = nullptr;
    const std::string _column_label;
};
} // namespace vectorized
} // namespace doris
