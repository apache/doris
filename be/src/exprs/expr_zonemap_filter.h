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
class TExprNode;
} // namespace doris

namespace doris::expr_zonemap {

struct InZonemapMaterializedSet {
    bool contains_null = false;
    std::vector<Field> values;
    Field min_value;
    Field max_value;
};

struct SlotLiteral {
    // Slot ordinal in the current expression binding. It is also the key used to look up the
    // corresponding reader-schema type and zone map from ZoneMapEvalContext.
    int slot_index;
    // Type carried by the slot expression. It is kept separately because evaluation first has to
    // verify that the expression binding is compatible with the reader-schema type stored in the
    // ZoneMapEvalContext before using schema-indexed zone-map statistics.
    DataTypePtr slot_type;
    // Constant literal value paired with the slot, already materialized as a Field for zone-map
    // comparisons.
    Field literal;
    // Type carried by the literal expression. It is needed with slot_type so capability checks can
    // reject incompatible slot/literal comparisons before runtime evaluation instead of evaluating
    // zone-map ranges with mismatched Field types.
    DataTypePtr literal_type;
    // Whether the original expression shape is literal <op> slot instead of slot <op> literal. The
    // comparison operator is directional, so evaluators need this flag to normalize cases like
    // `5 < slot` into the equivalent slot-side comparison before checking the zone map. Non-symmetric
    // function evaluators also use it to reject unsupported shapes such as `starts_with(literal,
    // slot)`, because only `starts_with(slot, literal)` can be mapped to a safe zone-map range.
    bool literal_on_left;
};

std::optional<SlotLiteral> extract_slot_and_literal(const VExprSPtrs& args);

TExprNode create_texpr_node_from_hybrid_set_value(const void* data, const PrimitiveType& type,
                                                  int precision, int scale);

Status materialize_hybrid_set_for_zonemap_filter(HybridSetBase& set, const DataTypePtr& data_type,
                                                 InZonemapMaterializedSet* result);

inline bool field_types_compatible(PrimitiveType lhs, PrimitiveType rhs) {
    return lhs == rhs || (is_string_type(lhs) && is_string_type(rhs));
}

inline bool data_types_compatible(const DataTypePtr& lhs, const DataTypePtr& rhs) {
    if (lhs == nullptr || rhs == nullptr) {
        return false;
    }
    const auto lhs_type = remove_nullable(lhs);
    const auto rhs_type = remove_nullable(rhs);
    const auto lhs_primitive_type = lhs_type->get_primitive_type();
    const auto rhs_primitive_type = rhs_type->get_primitive_type();
    if (is_string_type(lhs_primitive_type) && is_string_type(rhs_primitive_type)) {
        return true;
    }
    return lhs_type->equals(*rhs_type);
}

inline DataTypePtr fetch_compatible_slot_type(const ZoneMapEvalContext& ctx, int slot_index,
                                              const DataTypePtr& expr_slot_type) {
    auto slot_type = ctx.data_type(slot_index);
    if (slot_type == nullptr) {
        return nullptr;
    }
    DORIS_CHECK(data_types_compatible(slot_type, expr_slot_type));
    return slot_type;
}

bool range_stats_usable_for_zonemap(const segment_v2::ZoneMap& zone_map,
                                    const DataTypePtr& data_type);

ZoneMapFilterResult eval_null_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtrs& arguments,
                                      bool is_null);

ZoneMapFilterResult eval_in_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtr& slot_expr,
                                    bool is_not_in, const std::vector<Field>& values,
                                    const Field& min_value, const Field& max_value);

// Return the only slot ordinal referenced by a zonemap-evaluable expression in its current
// binding. Expressions that are unsupported by zonemap pruning, reference multiple slots, or use an
// invalid negative slot ordinal return a negative value.
int single_slot_zonemap_index(const VExprContextSPtr& ctx);

bool is_expr_zonemap_filter_enabled(const RuntimeState* state);

} // namespace doris::expr_zonemap
