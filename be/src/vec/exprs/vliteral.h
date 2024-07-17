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

#include "common/object_pool.h"
#include "common/status.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"

namespace doris {
class TExprNode;

namespace vectorized {
class Block;
class VExprContext;

class VLiteral : public VExpr {
    ENABLE_FACTORY_CREATOR(VLiteral);

public:
    VLiteral(const TExprNode& node, bool should_init = true)
            : VExpr(node), _expr_name(_data_type->get_name()) {
        if (should_init) {
            init(node);
        }
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status execute(VExprContext* context, Block* block, int* result_column_id) override;

    const std::string& expr_name() const override { return _expr_name; }
    std::string debug_string() const override;

    std::string value() const;

    const ColumnPtr& get_column_ptr() const { return _column_ptr; }

    bool is_literal() const override { return true; }

protected:
    ColumnPtr _column_ptr;
    std::string _expr_name;

private:
    void init(const TExprNode& node);
};
} // namespace vectorized

} // namespace doris
