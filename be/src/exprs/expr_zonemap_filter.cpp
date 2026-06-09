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

#include "exprs/expr_zonemap_filter.h"

#include <algorithm>

#include "common/check.h"
#include "common/logging.h"
#include "core/column/column.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/hybrid_set.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vexpr.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_state.h"

namespace doris::expr_zonemap {
namespace {

constexpr size_t kInZoneMapPointCheckThreshold = 64;

VExprSPtr unwrap_identity_cast(const VExprSPtr& expr) {
    if (expr->node_type() != TExprNodeType::CAST_EXPR || expr->get_num_children() != 1) {
        return expr;
    }
    auto child = expr->get_child(0);
    if (!remove_nullable(expr->data_type())->equals(*remove_nullable(child->data_type()))) {
        return expr;
    }
    return unwrap_identity_cast(child);
}

std::optional<Field> field_from_literal_expr(const VExprSPtr& expr, DataTypePtr* literal_type) {
    auto unwrapped = unwrap_identity_cast(expr);
    auto literal = std::dynamic_pointer_cast<VLiteral>(unwrapped);
    if (literal == nullptr) {
        return std::nullopt;
    }
    Field field;
    literal->get_column_ptr()->get(0, field);
    *literal_type = literal->get_data_type();
    return field;
}

struct SlotExpr {
    int index;
    DataTypePtr data_type;
};

std::optional<SlotExpr> slot_from_expr(const VExprSPtr& expr) {
    auto unwrapped = unwrap_identity_cast(expr);
    auto slot = std::dynamic_pointer_cast<VSlotRef>(unwrapped);
    if (slot == nullptr) {
        return std::nullopt;
    }
    return SlotExpr {.index = slot->column_id(), .data_type = slot->data_type()};
}

bool value_in_range(const Field& value, const Field& min_value, const Field& max_value) {
    return field_greater_equal(value, min_value) && field_less_equal(value, max_value);
}

} // namespace

std::optional<SlotLiteral> extract_slot_and_literal(const VExprSPtrs& args) {
    if (args.size() != 2) {
        return std::nullopt;
    }

    if (auto slot = slot_from_expr(args[0]); slot.has_value()) {
        DataTypePtr literal_type;
        auto literal = field_from_literal_expr(args[1], &literal_type);
        if (!literal.has_value()) {
            return std::nullopt;
        }
        return SlotLiteral {.slot_index = slot->index,
                            .slot_type = std::move(slot->data_type),
                            .literal = std::move(*literal),
                            .literal_type = std::move(literal_type),
                            .literal_on_left = false};
    }

    if (auto slot = slot_from_expr(args[1]); slot.has_value()) {
        DataTypePtr literal_type;
        auto literal = field_from_literal_expr(args[0], &literal_type);
        if (!literal.has_value()) {
            return std::nullopt;
        }
        return SlotLiteral {.slot_index = slot->index,
                            .slot_type = std::move(slot->data_type),
                            .literal = std::move(*literal),
                            .literal_type = std::move(literal_type),
                            .literal_on_left = true};
    }

    return std::nullopt;
}

const segment_v2::ZoneMap* fetch_zone_map(const ZoneMapEvalContext& ctx, int slot_index) {
    const auto* zone_map = ctx.zone_map(slot_index);
    if (zone_map == nullptr) {
        record_unsupported_zonemap_filter(ctx);
    }
    return zone_map;
}

bool range_stats_usable_for_zonemap(const segment_v2::ZoneMap& zone_map,
                                    const DataTypePtr& data_type) {
    if (zone_map.pass_all || zone_map.has_nan || zone_map.has_positive_inf ||
        zone_map.has_negative_inf) {
        return false;
    }
    DORIS_CHECK(data_type != nullptr);
    auto primitive_type = remove_nullable(data_type)->get_primitive_type();
    DORIS_CHECK(field_types_compatible(zone_map.min_value.get_type(), primitive_type));
    DORIS_CHECK(field_types_compatible(zone_map.max_value.get_type(), primitive_type));
    return true;
}

ZoneMapFilterResult eval_null_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtrs& arguments,
                                      bool is_null) {
    DORIS_CHECK(arguments.size() == 1);
    auto slot = slot_from_expr(arguments[0]);
    DORIS_CHECK(slot.has_value());
    const auto* zone_map_ref = fetch_zone_map(ctx, slot->index);
    if (zone_map_ref == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    const auto& zone_map = *zone_map_ref;
    if (is_null) {
        return zone_map.has_null ? ZoneMapFilterResult::kMayMatch : ZoneMapFilterResult::kNoMatch;
    }
    return zone_map.has_not_null ? ZoneMapFilterResult::kMayMatch : ZoneMapFilterResult::kNoMatch;
}

bool can_eval_null_zonemap(const VExprSPtrs& arguments) {
    if (arguments.size() != 1) {
        return false;
    }
    return slot_from_expr(arguments[0]).has_value();
}

ZoneMapFilterResult eval_in_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtr& slot_expr,
                                    bool is_not_in, const std::vector<Field>& values,
                                    const Field& min_value, const Field& max_value) {
    auto slot = slot_from_expr(slot_expr);
    DORIS_CHECK(slot.has_value());
    if (values.empty()) {
        return is_not_in ? ZoneMapFilterResult::kMayMatch : ZoneMapFilterResult::kNoMatch;
    }

    DORIS_CHECK(!min_value.is_null());
    DORIS_CHECK(!max_value.is_null());
    auto data_type = remove_nullable(slot->data_type);
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(field_types_compatible(min_value.get_type(), data_type->get_primitive_type()));
    DORIS_CHECK(field_types_compatible(max_value.get_type(), data_type->get_primitive_type()));

    auto slot_type = fetch_compatible_slot_type(ctx, slot->index, slot->data_type);
    if (slot_type == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    const auto* zone_map_ref = fetch_zone_map(ctx, slot->index);
    if (zone_map_ref == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    const auto& zone_map = *zone_map_ref;
    if (!zone_map.has_not_null) {
        return ZoneMapFilterResult::kNoMatch;
    }

    if (!range_stats_usable_for_zonemap(zone_map, slot_type)) {
        return unsupported_zonemap_filter(ctx);
    }

    if (is_not_in) {
        if (zone_map.min_value == zone_map.max_value) {
            const bool only_value_is_filtered = std::ranges::any_of(
                    values, [&](const Field& value) { return value == zone_map.min_value; });
            return only_value_is_filtered ? ZoneMapFilterResult::kNoMatch
                                          : ZoneMapFilterResult::kMayMatch;
        }
        return ZoneMapFilterResult::kMayMatch;
    }

    if (field_less(zone_map.max_value, min_value) || field_greater(zone_map.min_value, max_value)) {
        return ZoneMapFilterResult::kNoMatch;
    }

    if (values.size() > kInZoneMapPointCheckThreshold) {
        ++ctx.stats.in_zonemap_range_only_count;
        return ZoneMapFilterResult::kMayMatch;
    }

    ++ctx.stats.in_zonemap_point_check_count;
    for (const auto& value : values) {
        if (value_in_range(value, zone_map.min_value, zone_map.max_value)) {
            return ZoneMapFilterResult::kMayMatch;
        }
    }
    return ZoneMapFilterResult::kNoMatch;
}

bool can_eval_in_zonemap(const VExprSPtr& slot_expr, const std::vector<Field>&, const Field&,
                         const Field&) {
    return slot_from_expr(slot_expr).has_value();
}

bool is_expr_zonemap_filter_enabled(const RuntimeState* state) {
    if (state == nullptr) {
        return true;
    }
    const auto& query_options = state->query_options();
    return !query_options.__isset.enable_expr_zonemap_filter ||
           query_options.enable_expr_zonemap_filter;
}

} // namespace doris::expr_zonemap
