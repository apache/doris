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

#include <cstdint>

#include "cctz/time_zone.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

class VDefaultExpr final : public VExpr {
    ENABLE_FACTORY_CREATOR(VDefaultExpr);

public:
    VDefaultExpr(const TExprNode& node) : VExpr(node) {}
    ~VDefaultExpr() override = default;

    Status execute_column(VExprContext* context, const Block* block, size_t count,
                          ColumnPtr& result_column) const override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;
    std::string debug_string() const override;

private:
    template <PrimitiveType PType>
    Status write_current_time_value(const int64_t timestamp_ms, const int32_t nano_seconds,
                                    const cctz::time_zone& timezone_obj, const int precision);
    std::string _expr_name;
    SlotId _slot_id {-1};
    bool _is_nullable {true};
    bool _has_default_value {false};
    std::string _default_value;
    ColumnPtr _cached_column;
};

} // namespace doris::vectorized
