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

class MockSlotRef final : public VExpr {
public:
    MockSlotRef(int column_id) : _column_id(column_id) {};
    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        *result_column_id = _column_id;
        return Status::OK();
    }
    const std::string& expr_name() const override { return _name; }

private:
    int _column_id;
    const std::string _name = "MockSlotRef";
};
} // namespace vectorized
} // namespace doris
