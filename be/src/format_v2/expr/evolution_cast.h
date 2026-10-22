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

#include "format_v2/expr/cast.h"

namespace doris::format {

// File-to-table conversions own external schema evolution validation; SQL casts do not.
class EvolutionCast final : public Cast {
    ENABLE_FACTORY_CREATOR(EvolutionCast);

public:
    explicit EvolutionCast(const DataTypePtr& type, bool truncate_datetimev2_precision = false)
            : Cast(type), _truncate_datetimev2_precision(truncate_datetimev2_precision) {}

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override;
    std::string debug_string() const override;
    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = EvolutionCast::create_shared(_data_type, _truncate_datetimev2_precision);
        return Status::OK();
    }

private:
    bool _truncate_datetimev2_precision = false;
};

} // namespace doris::format
