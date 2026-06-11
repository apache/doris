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

#include <compare>
#include <optional>
#include <vector>

#include "common/check.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "exprs/vexpr_fwd.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"

namespace doris {
class HybridSetBase;
class RuntimeState;
} // namespace doris

namespace doris::expr_zonemap {

struct SlotLiteral {
    int slot_index;
    DataTypePtr slot_type;
    Field literal;
    DataTypePtr literal_type;
    bool literal_on_left;
};

std::optional<SlotLiteral> extract_slot_and_literal(const VExprSPtrs& args);

inline bool field_types_compatible(PrimitiveType lhs, PrimitiveType rhs) {
    return lhs == rhs || (is_string_type(lhs) && is_string_type(rhs));
}

inline bool data_types_compatible(const DataTypePtr& lhs, const DataTypePtr& rhs) {
    if (lhs == nullptr || rhs == nullptr) {
        return false;
    }
    const auto lhs_type = remove_nullable(lhs)->get_primitive_type();
    const auto rhs_type = remove_nullable(rhs)->get_primitive_type();
    if (field_types_compatible(lhs_type, rhs_type)) {
        return true;
    }
    return remove_nullable(lhs)->equals(*remove_nullable(rhs));
}

inline DataTypePtr fetch_compatible_slot_type(const ZoneMapEvalContext& ctx, int slot_index,
                                              const DataTypePtr& expr_slot_type) {
    const auto* slot_type = ctx.data_type(slot_index);
    if (slot_type == nullptr || *slot_type == nullptr) {
        return nullptr;
    }
    DORIS_CHECK(data_types_compatible(*slot_type, expr_slot_type));
    return *slot_type;
}

inline bool field_less(const Field& lhs, const Field& rhs) {
    return (lhs <=> rhs) == std::strong_ordering::less;
}

inline bool field_less_equal(const Field& lhs, const Field& rhs) {
    const auto cmp = lhs <=> rhs;
    return cmp == std::strong_ordering::less || cmp == std::strong_ordering::equal;
}

inline bool field_greater(const Field& lhs, const Field& rhs) {
    return (lhs <=> rhs) == std::strong_ordering::greater;
}

inline bool field_greater_equal(const Field& lhs, const Field& rhs) {
    const auto cmp = lhs <=> rhs;
    return cmp == std::strong_ordering::greater || cmp == std::strong_ordering::equal;
}

bool range_stats_usable_for_zonemap(const segment_v2::ZoneMap& zone_map,
                                    const DataTypePtr& data_type);

ZoneMapFilterResult eval_null_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtrs& arguments,
                                      bool is_null);

bool can_eval_null_zonemap(const VExprSPtrs& arguments);

ZoneMapFilterResult eval_in_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtr& slot_expr,
                                    bool is_not_in, const std::vector<Field>& values,
                                    const Field& min_value, const Field& max_value);

bool can_eval_in_zonemap(const VExprSPtr& slot_expr, const std::vector<Field>& values,
                         const Field& min_value, const Field& max_value);

bool is_expr_zonemap_filter_enabled(const RuntimeState* state);

} // namespace doris::expr_zonemap
