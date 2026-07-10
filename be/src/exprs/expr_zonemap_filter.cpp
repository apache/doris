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
#include <set>
#include <utility>

#include "common/check.h"
#include "common/logging.h"
#include "core/column/column.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_ref.h"
#include "exprs/hybrid_set.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_state.h"
#include "storage/index/bloom_filter/bloom_filter.h"

namespace doris::expr_zonemap {
namespace {

constexpr size_t kInZoneMapPointCheckThreshold = 64;

std::optional<std::pair<Field, DataTypePtr>> field_from_literal_expr(const VExprSPtr& expr) {
    auto literal = std::dynamic_pointer_cast<VLiteral>(expr);
    if (literal == nullptr) {
        return std::nullopt;
    }
    Field field;
    literal->get_column_ptr()->get(0, field);
    return std::make_pair(std::move(field), literal->get_data_type());
}

bool value_in_range(const Field& value, const Field& min_value, const Field& max_value) {
    return value >= min_value && value <= max_value;
}

bool dictionary_contains(const DictionaryEvalContext::SlotDictionary& dictionary,
                         const Field& value) {
    return std::ranges::any_of(dictionary.values, [&](const Field& dictionary_value) {
        return dictionary_value == value;
    });
}

bool bloom_filter_may_contain(const BloomFilterEvalContext::SlotBloomFilter& slot_filter,
                              const Field& value) {
    DORIS_CHECK(slot_filter.data_type != nullptr);
    DORIS_CHECK(slot_filter.bloom_filter != nullptr);
    const auto data_type = remove_nullable(slot_filter.data_type);
    DORIS_CHECK(data_type != nullptr);
    switch (data_type->get_primitive_type()) {
    case TYPE_BOOLEAN: {
        const bool typed_value = value.get<TYPE_BOOLEAN>();
        return slot_filter.bloom_filter->test_bytes(reinterpret_cast<const char*>(&typed_value),
                                                    sizeof(typed_value));
    }
    case TYPE_INT: {
        const int32_t typed_value = value.get<TYPE_INT>();
        return slot_filter.bloom_filter->test_bytes(reinterpret_cast<const char*>(&typed_value),
                                                    sizeof(typed_value));
    }
    case TYPE_BIGINT: {
        const int64_t typed_value = value.get<TYPE_BIGINT>();
        return slot_filter.bloom_filter->test_bytes(reinterpret_cast<const char*>(&typed_value),
                                                    sizeof(typed_value));
    }
    case TYPE_FLOAT: {
        const float typed_value = value.get<TYPE_FLOAT>();
        return slot_filter.bloom_filter->test_bytes(reinterpret_cast<const char*>(&typed_value),
                                                    sizeof(typed_value));
    }
    case TYPE_DOUBLE: {
        const double typed_value = value.get<TYPE_DOUBLE>();
        return slot_filter.bloom_filter->test_bytes(reinterpret_cast<const char*>(&typed_value),
                                                    sizeof(typed_value));
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const auto& typed_value = value.get<TYPE_STRING>();
        return slot_filter.bloom_filter->test_bytes(typed_value.data(), typed_value.size());
    }
    default:
        return true;
    }
}

template <typename Capability>
int single_slot_index(const VExprContextSPtr& ctx, Capability capability) {
    DORIS_CHECK(ctx != nullptr);
    const auto& root = ctx->root();
    DORIS_CHECK(root != nullptr);
    if (!capability(root)) {
        return -1;
    }

    std::set<int> slot_indexes;
    root->collect_slot_column_ids(slot_indexes);
    if (slot_indexes.size() != 1) {
        return -1;
    }

    return *slot_indexes.begin();
}

} // namespace

const DictionaryEvalContext::SlotDictionary* DictionaryEvalContext::slot(int slot_index) const {
    auto it = slots.find(slot_index);
    return it == slots.end() ? nullptr : &it->second;
}

const BloomFilterEvalContext::SlotBloomFilter* BloomFilterEvalContext::slot(int slot_index) const {
    auto it = slots.find(slot_index);
    return it == slots.end() ? nullptr : &it->second;
}

TExprNode create_texpr_node_from_hybrid_set_value(const void* data, const PrimitiveType& type,
                                                  int precision, int scale) {
    if (is_string_type(type)) {
        const auto* value = reinterpret_cast<const StringRef*>(data);
        auto field = Field::create_field<TYPE_STRING>(String(value->data, value->size));
        return create_texpr_node_from(field, type, precision, scale);
    }
    return create_texpr_node_from(data, type, precision, scale);
}

Status materialize_hybrid_set_for_zonemap_filter(HybridSetBase& set, const DataTypePtr& data_type,
                                                 InZonemapMaterializedSet* result) {
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(data_type != nullptr);
    const auto value_type = remove_nullable(data_type);
    DORIS_CHECK(value_type != nullptr);

    result->contains_null = set.contain_null();
    result->values.clear();
    result->min_value = Field();
    result->max_value = Field();

    auto* iterator = set.begin();
    while (iterator->has_next()) {
        const void* value = iterator->get_value();
        if (value != nullptr) {
            TExprNode literal_node = create_texpr_node_from_hybrid_set_value(
                    value, value_type->get_primitive_type(), value_type->get_precision(),
                    value_type->get_scale());
            auto literal = VLiteral::create_shared(literal_node);
            Field field;
            literal->get_column_ptr()->get(0, field);
            result->values.emplace_back(std::move(field));
        }
        iterator->next();
    }

    if (!result->values.empty()) {
        auto minmax = std::ranges::minmax_element(
                result->values, [](const Field& lhs, const Field& rhs) { return lhs < rhs; });
        result->min_value = *minmax.min;
        result->max_value = *minmax.max;
    }
    return Status::OK();
}

std::optional<SlotLiteral> extract_slot_and_literal(const VExprSPtrs& args) {
    if (args.size() != 2) {
        return std::nullopt;
    }

    if (auto slot = std::dynamic_pointer_cast<VSlotRef>(args[0]); slot) {
        auto literal = field_from_literal_expr(args[1]);
        if (!literal.has_value()) {
            return std::nullopt;
        }
        auto [literal_value, literal_type] = std::move(*literal);
        return SlotLiteral {.slot_index = slot->column_id(),
                            .slot_type = slot->data_type(),
                            .literal = std::move(literal_value),
                            .literal_type = std::move(literal_type),
                            .literal_on_left = false};
    }

    if (auto slot = std::dynamic_pointer_cast<VSlotRef>(args[1]); slot) {
        auto literal = field_from_literal_expr(args[0]);
        if (!literal.has_value()) {
            return std::nullopt;
        }
        auto [literal_value, literal_type] = std::move(*literal);
        return SlotLiteral {.slot_index = slot->column_id(),
                            .slot_type = slot->data_type(),
                            .literal = std::move(literal_value),
                            .literal_type = std::move(literal_type),
                            .literal_on_left = true};
    }

    return std::nullopt;
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
    auto slot = std::dynamic_pointer_cast<VSlotRef>(arguments[0]);
    DORIS_CHECK(slot != nullptr);
    auto zone_map_ptr = ctx.zone_map(slot->column_id());
    if (zone_map_ptr == nullptr) {
        return unsupported_zonemap_filter(ctx);
    }
    const auto& zone_map = *zone_map_ptr;
    if (is_null) {
        return zone_map.has_null ? ZoneMapFilterResult::kMayMatch : ZoneMapFilterResult::kNoMatch;
    }
    return zone_map.has_not_null ? ZoneMapFilterResult::kMayMatch : ZoneMapFilterResult::kNoMatch;
}

ZoneMapFilterResult eval_in_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtr& slot_expr,
                                    bool is_not_in, const std::vector<Field>& values,
                                    const Field& min_value, const Field& max_value) {
    auto slot = std::dynamic_pointer_cast<VSlotRef>(slot_expr);
    DORIS_CHECK(slot != nullptr);
    // Empty IN has no candidate values, while NOT IN with an empty set cannot filter anything.
    if (values.empty()) {
        return is_not_in ? ZoneMapFilterResult::kMayMatch : ZoneMapFilterResult::kNoMatch;
    }

    // The caller has materialized the IN set and precomputed its non-null min/max. They must match
    // the expression slot type before being compared with storage zone-map statistics.
    DORIS_CHECK(!min_value.is_null());
    DORIS_CHECK(!max_value.is_null());
    auto data_type = remove_nullable(slot->data_type());
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(field_types_compatible(min_value.get_type(), data_type->get_primitive_type()));
    DORIS_CHECK(field_types_compatible(max_value.get_type(), data_type->get_primitive_type()));

    // Re-check against the reader-schema type and the available zone map. Missing or unsupported
    // metadata must conservatively fall back to may-match.
    auto slot_type = fetch_compatible_slot_type(ctx, slot->column_id(), slot->data_type());
    if (slot_type == nullptr) {
        return unsupported_zonemap_filter(ctx);
    }
    auto zone_map_ptr = ctx.zone_map(slot->column_id());
    if (zone_map_ptr == nullptr) {
        return unsupported_zonemap_filter(ctx);
    }
    const auto& zone_map = *zone_map_ptr;
    // IN values are all non-null here, so an all-null zone cannot match.
    if (!zone_map.has_not_null) {
        return ZoneMapFilterResult::kNoMatch;
    }

    if (!range_stats_usable_for_zonemap(zone_map, slot_type)) {
        return unsupported_zonemap_filter(ctx);
    }

    if (is_not_in) {
        // NOT IN can only prune when the whole zone contains exactly one non-null value and that
        // value is excluded by the set. Wider ranges may contain values that are not filtered.
        if (zone_map.min_value == zone_map.max_value) {
            const bool only_value_is_filtered = std::ranges::any_of(
                    values, [&](const Field& value) { return value == zone_map.min_value; });
            return only_value_is_filtered ? ZoneMapFilterResult::kNoMatch
                                          : ZoneMapFilterResult::kMayMatch;
        }
        return ZoneMapFilterResult::kMayMatch;
    }

    // First use the materialized IN-set min/max to rule out disjoint zone-map ranges.
    if (zone_map.max_value < min_value || zone_map.min_value > max_value) {
        return ZoneMapFilterResult::kNoMatch;
    }

    // For large IN sets, avoid checking every point on the scan hot path. The range overlap above
    // is only a coarse may-match signal.
    if (values.size() > kInZoneMapPointCheckThreshold) {
        ++ctx.stats.in_zonemap_range_only_count;
        return ZoneMapFilterResult::kMayMatch;
    }

    // For small IN sets, verify whether any candidate value can fall into the zone-map range.
    ++ctx.stats.in_zonemap_point_check_count;
    for (const auto& value : values) {
        if (value_in_range(value, zone_map.min_value, zone_map.max_value)) {
            return ZoneMapFilterResult::kMayMatch;
        }
    }
    return ZoneMapFilterResult::kNoMatch;
}

ZoneMapFilterResult eval_eq_dictionary(const DictionaryEvalContext& ctx,
                                       const SlotLiteral& slot_literal) {
    auto dictionary = ctx.slot(slot_literal.slot_index);
    if (dictionary == nullptr || dictionary->data_type == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    DORIS_CHECK(data_types_compatible(dictionary->data_type, slot_literal.slot_type));
    if (slot_literal.literal.is_null()) {
        return ZoneMapFilterResult::kUnsupported;
    }
    return dictionary_contains(*dictionary, slot_literal.literal) ? ZoneMapFilterResult::kMayMatch
                                                                  : ZoneMapFilterResult::kNoMatch;
}

ZoneMapFilterResult eval_in_dictionary(const DictionaryEvalContext& ctx, const VExprSPtr& slot_expr,
                                       bool is_not_in, const std::vector<Field>& values) {
    if (is_not_in) {
        return ZoneMapFilterResult::kUnsupported;
    }
    auto slot = std::dynamic_pointer_cast<VSlotRef>(slot_expr);
    DORIS_CHECK(slot != nullptr);
    auto dictionary = ctx.slot(slot->column_id());
    if (dictionary == nullptr || dictionary->data_type == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    DORIS_CHECK(data_types_compatible(dictionary->data_type, slot->data_type()));
    if (values.empty()) {
        return ZoneMapFilterResult::kNoMatch;
    }
    for (const auto& value : values) {
        if (!value.is_null() && dictionary_contains(*dictionary, value)) {
            return ZoneMapFilterResult::kMayMatch;
        }
    }
    return ZoneMapFilterResult::kNoMatch;
}

ZoneMapFilterResult eval_eq_bloom_filter(const BloomFilterEvalContext& ctx,
                                         const SlotLiteral& slot_literal) {
    auto slot_filter = ctx.slot(slot_literal.slot_index);
    if (slot_filter == nullptr || slot_filter->data_type == nullptr ||
        slot_filter->bloom_filter == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    DORIS_CHECK(data_types_compatible(slot_filter->data_type, slot_literal.slot_type));
    if (slot_literal.literal.is_null()) {
        return ZoneMapFilterResult::kUnsupported;
    }
    return bloom_filter_may_contain(*slot_filter, slot_literal.literal)
                   ? ZoneMapFilterResult::kMayMatch
                   : ZoneMapFilterResult::kNoMatch;
}

ZoneMapFilterResult eval_in_bloom_filter(const BloomFilterEvalContext& ctx,
                                         const VExprSPtr& slot_expr, bool is_not_in,
                                         const std::vector<Field>& values) {
    if (is_not_in) {
        return ZoneMapFilterResult::kUnsupported;
    }
    auto slot = std::dynamic_pointer_cast<VSlotRef>(slot_expr);
    DORIS_CHECK(slot != nullptr);
    auto slot_filter = ctx.slot(slot->column_id());
    if (slot_filter == nullptr || slot_filter->data_type == nullptr ||
        slot_filter->bloom_filter == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    DORIS_CHECK(data_types_compatible(slot_filter->data_type, slot->data_type()));
    if (values.empty()) {
        return ZoneMapFilterResult::kNoMatch;
    }
    for (const auto& value : values) {
        if (!value.is_null() && bloom_filter_may_contain(*slot_filter, value)) {
            return ZoneMapFilterResult::kMayMatch;
        }
    }
    return ZoneMapFilterResult::kNoMatch;
}

// Return the only slot ordinal referenced by a zonemap-evaluable expression. A negative result is
// the conservative fallback marker for unsupported expressions, multi-slot expressions, or invalid
// slot ordinals, so callers can skip schema-indexed zonemap pruning safely.
int single_slot_zonemap_index(const VExprContextSPtr& ctx) {
    return single_slot_index(
            ctx, [](const VExprSPtr& expr) { return expr->can_evaluate_zonemap_filter(); });
}

int single_slot_dictionary_index(const VExprContextSPtr& ctx) {
    return single_slot_index(
            ctx, [](const VExprSPtr& expr) { return expr->can_evaluate_dictionary_filter(); });
}

int single_slot_bloom_filter_index(const VExprContextSPtr& ctx) {
    return single_slot_index(
            ctx, [](const VExprSPtr& expr) { return expr->can_evaluate_bloom_filter(); });
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
