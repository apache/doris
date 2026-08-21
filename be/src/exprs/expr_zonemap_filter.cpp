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
#include <cmath>
#include <limits>
#include <set>
#include <type_traits>
#include <utility>

#include "common/check.h"
#include "common/config.h"
#include "common/logging.h"
#include "core/column/column.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_ref.h"
#include "exprs/hybrid_set.h"
#include "exprs/hybrid_set_min_max.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_state.h"
#include "storage/index/bloom_filter/bloom_filter.h"

namespace doris::expr_zonemap {
namespace {

std::optional<std::pair<Field, DataTypePtr>> field_from_literal_expr(const VExprSPtr& expr) {
    auto literal = std::dynamic_pointer_cast<VLiteral>(expr);
    if (literal == nullptr) {
        return std::nullopt;
    }
    Field field;
    literal->get_column_ptr()->get(0, field);
    return std::make_pair(std::move(field), literal->get_data_type());
}

std::optional<int32_t> struct_field_ordinal(const Field& field) {
    int64_t ordinal = -1;
    switch (field.get_type()) {
    case TYPE_BOOLEAN:
        ordinal = field.get<TYPE_BOOLEAN>();
        break;
    case TYPE_TINYINT:
        ordinal = field.get<TYPE_TINYINT>();
        break;
    case TYPE_SMALLINT:
        ordinal = field.get<TYPE_SMALLINT>();
        break;
    case TYPE_INT:
        ordinal = field.get<TYPE_INT>();
        break;
    case TYPE_BIGINT:
        ordinal = field.get<TYPE_BIGINT>();
        break;
    default:
        return std::nullopt;
    }
    if (ordinal <= 0 || ordinal > std::numeric_limits<int32_t>::max()) {
        return std::nullopt;
    }
    return static_cast<int32_t>(ordinal - 1);
}

bool dictionary_contains(const DictionaryEvalContext::SlotDictionary& dictionary,
                         const Field& value) {
    return std::ranges::any_of(dictionary.values, [&](const Field& dictionary_value) {
        return dictionary_value == value;
    });
}

template <typename T>
bool floating_point_bloom_filter_may_contain(const segment_v2::BloomFilter& bloom_filter, T value) {
    static_assert(std::is_floating_point_v<T>);
    // Doris equality collapses NaN payloads and signed zeros, while Parquet Bloom hashes physical
    // bytes. A negative probe is safe only after covering the entire Doris-equivalent class.
    if (std::isnan(value)) {
        return true;
    }
    const auto test_value = [&](T candidate) {
        return bloom_filter.test_bytes(reinterpret_cast<const char*>(&candidate),
                                       sizeof(candidate));
    };
    if (test_value(value)) {
        return true;
    }
    return value == T {0} && test_value(-value);
}

bool bloom_filter_probes_equal(const BloomFilterProbe& lhs, const BloomFilterProbe& rhs) {
    return lhs.slot_index == rhs.slot_index && lhs.path == rhs.path &&
           data_types_compatible(lhs.value_type, rhs.value_type);
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
        return floating_point_bloom_filter_may_contain(*slot_filter.bloom_filter, typed_value);
    }
    case TYPE_DOUBLE: {
        const double typed_value = value.get<TYPE_DOUBLE>();
        return floating_point_bloom_filter_may_contain(*slot_filter.bloom_filter, typed_value);
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

void get_hybrid_set_min_max_for_zonemap_filter(const std::shared_ptr<HybridSetBase>& set,
                                               const DataTypePtr& data_type,
                                               InZonemapMinMax& result) {
    DORIS_CHECK(set != nullptr);
    DORIS_CHECK(data_type != nullptr);
    const auto value_type = remove_nullable(data_type);
    DORIS_CHECK(value_type != nullptr);

    set->get_min_max(result.min_value, result.max_value, result.contains_nan);
    const auto value_count = set->size();
    DORIS_CHECK_EQ(result.min_value.is_null(), result.max_value.is_null());
    if (result.min_value.is_null()) {
        if (value_count != 0) {
            DORIS_CHECK(result.contains_nan);
            DORIS_CHECK(is_float_or_double(value_type->get_primitive_type()));
        }
        return;
    }
    DORIS_CHECK_NE(value_count, 0);
    DORIS_CHECK(
            field_types_compatible(result.min_value.get_type(), value_type->get_primitive_type()));
    DORIS_CHECK(
            field_types_compatible(result.max_value.get_type(), value_type->get_primitive_type()));
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

std::optional<BloomFilterProbe> extract_bloom_filter_probe(const VExprSPtr& expr) {
    if (expr == nullptr || expr->data_type() == nullptr) {
        return std::nullopt;
    }
    if (auto slot = std::dynamic_pointer_cast<VSlotRef>(expr); slot) {
        return BloomFilterProbe {
                .slot_index = slot->column_id(), .value_type = slot->data_type(), .path = {}};
    }
    if ((expr->fn().name.function_name != "element_at" &&
         expr->fn().name.function_name != "struct_element") ||
        expr->get_num_children() != 2) {
        return std::nullopt;
    }

    auto probe = extract_bloom_filter_probe(expr->get_child(0));
    auto selector = field_from_literal_expr(expr->get_child(1));
    if (!probe.has_value() || !selector.has_value() || selector->first.is_null()) {
        return std::nullopt;
    }
    const auto parent_type = remove_nullable(expr->get_child(0)->data_type());
    if (parent_type == nullptr) {
        return std::nullopt;
    }

    BloomFilterPathElement path_element;
    switch (parent_type->get_primitive_type()) {
    case TYPE_STRUCT: {
        path_element.kind = BloomFilterPathKind::STRUCT_FIELD;
        const auto selector_type = remove_nullable(selector->second);
        if (selector_type == nullptr) {
            return std::nullopt;
        }
        if (is_string_type(selector_type->get_primitive_type())) {
            path_element.field_name = selector->first.get<TYPE_STRING>();
        } else {
            auto ordinal = struct_field_ordinal(selector->first);
            if (!ordinal.has_value()) {
                return std::nullopt;
            }
            path_element.field_ordinal = *ordinal;
        }
        break;
    }
    case TYPE_ARRAY:
        // Array element positions share one repeated Parquet leaf; membership in that leaf is a
        // necessary condition for any element_at(array, constant) equality to match.
        path_element.kind = BloomFilterPathKind::LIST_ELEMENT;
        break;
    default:
        return std::nullopt;
    }
    probe->value_type = expr->data_type();
    probe->path.push_back(std::move(path_element));
    return probe;
}

bool collect_unique_bloom_filter_probe(const VExprSPtr& expr,
                                       std::optional<BloomFilterProbe>* result) {
    DORIS_CHECK(result != nullptr);
    if (auto probe = extract_bloom_filter_probe(expr); probe.has_value()) {
        if (result->has_value() && !bloom_filter_probes_equal(**result, *probe)) {
            return false;
        }
        *result = std::move(probe);
        return true;
    }
    if (expr == nullptr) {
        return true;
    }
    for (uint16_t child_idx = 0; child_idx < expr->get_num_children(); ++child_idx) {
        const auto& child = expr->get_child(child_idx);
        if (child == nullptr || child->is_literal()) {
            continue;
        }
        // Every Bloom-capable branch must bind to the same leaf; a conflicting subtree cannot be
        // treated like a branch without a probe because the compound evaluator would use it.
        if (!collect_unique_bloom_filter_probe(child, result)) {
            return false;
        }
    }
    return true;
}

std::optional<BloomFilterProbe> extract_bloom_filter_predicate_probe(const VExprSPtr& expr) {
    std::optional<BloomFilterProbe> result;
    if (!collect_unique_bloom_filter_probe(expr, &result)) {
        return std::nullopt;
    }
    return result;
}

std::optional<SlotLiteral> extract_bloom_filter_slot_and_literal(const VExprSPtrs& args) {
    if (args.size() != 2) {
        return std::nullopt;
    }
    for (size_t probe_idx = 0; probe_idx < args.size(); ++probe_idx) {
        auto probe = extract_bloom_filter_probe(args[probe_idx]);
        auto literal = field_from_literal_expr(args[1 - probe_idx]);
        if (!probe.has_value() || !literal.has_value()) {
            continue;
        }
        auto [literal_value, literal_type] = std::move(*literal);
        return SlotLiteral {.slot_index = probe->slot_index,
                            .slot_type = probe->value_type,
                            .literal = std::move(literal_value),
                            .literal_type = std::move(literal_type),
                            .literal_on_left = probe_idx == 1};
    }
    return std::nullopt;
}

bool can_evaluate_bloom_filter_equality(const VExprSPtrs& args) {
    auto slot_literal = extract_bloom_filter_slot_and_literal(args);
    // Parquet Bloom hashes physical bytes, so it cannot disprove Doris NaN equality across
    // different NaN payloads even when the probe targets a nested leaf.
    return slot_literal.has_value() && !slot_literal->literal.is_null() &&
           !slot_literal->literal.is_nan() &&
           data_types_compatible(slot_literal->slot_type, slot_literal->literal_type);
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
        if (!zone_map.has_null) {
            return ZoneMapFilterResult::kNoMatch; // no NULL row here
        }
        if (!zone_map.has_not_null) {
            return ZoneMapFilterResult::kAllMatch; // every row is NULL
        }
        return ZoneMapFilterResult::kMayMatch;
    }
    if (!zone_map.has_not_null) {
        return ZoneMapFilterResult::kNoMatch; // every row is NULL
    }
    if (!zone_map.has_null) {
        return ZoneMapFilterResult::kAllMatch; // no NULL row here
    }
    return ZoneMapFilterResult::kMayMatch;
}

// Keep the conservative fallback checks together so their evaluation order remains explicit.
// NOLINTNEXTLINE(readability-function-size)
ZoneMapFilterResult eval_in_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtr& slot_expr,
                                    bool is_not_in, const InZonemapMinMax& values,
                                    const HybridSetBase& set) {
    auto slot = std::dynamic_pointer_cast<VSlotRef>(slot_expr);
    DORIS_CHECK(slot != nullptr);
    // NOT IN with a NULL literal is UNKNOWN for every non-null value. Zone maps do not retain
    // enough row-level information to recover a match in that case.
    if (is_not_in && set.contain_null()) {
        return ZoneMapFilterResult::kNoMatch;
    }
    // Empty IN has no candidate values, while NOT IN with an empty set cannot filter anything.
    if (set.size() == 0) { // NOLINT(readability-container-size-empty)
        return is_not_in ? ZoneMapFilterResult::kMayMatch : ZoneMapFilterResult::kNoMatch;
    }

    auto data_type = remove_nullable(slot->data_type());
    DORIS_CHECK(data_type != nullptr);

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

    if (ctx.floating_nan_count_unknown(slot->column_id()) &&
        ((!is_not_in && values.contains_nan) || (is_not_in && !values.contains_nan))) {
        // Hidden Parquet NaNs can satisfy IN only when queried, and NOT IN only when omitted.
        return unsupported_zonemap_filter(ctx);
    }

    if (!range_stats_usable_for_zonemap(zone_map, slot_type)) {
        return unsupported_zonemap_filter(ctx);
    }

    // A non-empty set without ordered bounds contains only NaN values. Once the reader proves the
    // data has no hidden NaNs, such an IN cannot match while NOT IN remains conservative.
    if (values.min_value.is_null() || values.max_value.is_null()) {
        DORIS_CHECK(values.contains_nan);
        DORIS_CHECK(values.min_value.is_null());
        DORIS_CHECK(values.max_value.is_null());
        return is_not_in ? ZoneMapFilterResult::kMayMatch : ZoneMapFilterResult::kNoMatch;
    }

    // The caller has precomputed the IN set's owning non-NaN min/max. They must match the expression
    // slot type before being compared with storage zone-map statistics.
    DORIS_CHECK(
            field_types_compatible(values.min_value.get_type(), data_type->get_primitive_type()));
    DORIS_CHECK(
            field_types_compatible(values.max_value.get_type(), data_type->get_primitive_type()));

    // The zone range does not reach the range of the listed values, so nothing in this zone can be
    // one of them.
    const bool no_row_is_listed =
            zone_map.max_value < values.min_value || zone_map.min_value > values.max_value;
    // A NULL row satisfies neither IN nor NOT IN, and a hidden Parquet NaN could satisfy either,
    // so both stop the zone from matching completely.
    const bool can_match_all =
            !zone_map.has_null && !ctx.floating_nan_count_unknown(slot->column_id());

    if (is_not_in) {
        if (no_row_is_listed) {
            return can_match_all ? ZoneMapFilterResult::kAllMatch : ZoneMapFilterResult::kMayMatch;
        }
        // NOT IN can only prune when the whole zone contains exactly one non-null value and that
        // value is excluded by the set. Wider ranges may contain values that are not filtered.
        if (zone_map.min_value == zone_map.max_value) {
            const bool only_value_is_filtered = set.find(zone_map.min_value);
            return only_value_is_filtered ? ZoneMapFilterResult::kNoMatch
                                          : ZoneMapFilterResult::kMayMatch;
        }
        return ZoneMapFilterResult::kMayMatch;
    }

    if (no_row_is_listed) {
        return ZoneMapFilterResult::kNoMatch;
    }
    // Equal bounds mean every row in the zone holds the same value, so probing the set once
    // answers for all of them. Wider ranges cannot be answered this way because the listed values
    // leave gaps between the set's own min and max.
    if (can_match_all && zone_map.min_value == zone_map.max_value && set.find(zone_map.min_value)) {
        return ZoneMapFilterResult::kAllMatch;
    }

    // For large IN sets and dense-domain containers, avoid exact checks on the scan hot path.
    if (set.size() > config::in_zonemap_point_check_threshold) {
        ++ctx.stats.in_zonemap_range_only_count;
        return ZoneMapFilterResult::kMayMatch;
    }

    // Convert the two zone-map bounds to the HybridSet's native type once, then compare them
    // directly with the typed set values without retaining a Field copy of every IN candidate.
    ++ctx.stats.in_zonemap_point_check_count;
    return set.contains_any_in_range(zone_map.min_value, zone_map.max_value)
                   ? ZoneMapFilterResult::kMayMatch
                   : ZoneMapFilterResult::kNoMatch;
}

ZoneMapFilterResult eval_eq_dictionary(const DictionaryEvalContext& ctx,
                                       const SlotLiteral& slot_literal) {
    const auto* dictionary = ctx.slot(slot_literal.slot_index);
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
                                       bool is_not_in, const HybridSetBase& values) {
    if (is_not_in) {
        return ZoneMapFilterResult::kUnsupported;
    }
    auto slot = std::dynamic_pointer_cast<VSlotRef>(slot_expr);
    DORIS_CHECK(slot != nullptr);
    const auto* dictionary = ctx.slot(slot->column_id());
    if (dictionary == nullptr || dictionary->data_type == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    DORIS_CHECK(data_types_compatible(dictionary->data_type, slot->data_type()));
    // HybridSetBase::empty() also treats a NULL literal as non-empty, but dictionary pruning needs
    // to know whether there are any non-NULL candidates.
    if (values.size() == 0) { // NOLINT(readability-container-size-empty)
        return ZoneMapFilterResult::kNoMatch;
    }
    for (const auto& value : dictionary->values) {
        if (!value.is_null() && values.find(value)) {
            return ZoneMapFilterResult::kMayMatch;
        }
    }
    return ZoneMapFilterResult::kNoMatch;
}

ZoneMapFilterResult eval_eq_bloom_filter(const BloomFilterEvalContext& ctx,
                                         const SlotLiteral& slot_literal) {
    const auto* slot_filter = ctx.slot(slot_literal.slot_index);
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
                                         const HybridSetBase& values) {
    if (is_not_in) {
        return ZoneMapFilterResult::kUnsupported;
    }
    auto probe = extract_bloom_filter_probe(slot_expr);
    DORIS_CHECK(probe.has_value());
    const auto* slot_filter = ctx.slot(probe->slot_index);
    if (slot_filter == nullptr || slot_filter->data_type == nullptr ||
        slot_filter->bloom_filter == nullptr) {
        return ZoneMapFilterResult::kUnsupported;
    }
    DORIS_CHECK(data_types_compatible(slot_filter->data_type, probe->value_type));
    if (values.size() == 0) { // NOLINT(readability-container-size-empty)
        return ZoneMapFilterResult::kNoMatch;
    }
    const auto value_type = remove_nullable(slot_filter->data_type)->get_primitive_type();
    switch (value_type) {
    case TYPE_BOOLEAN:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return values.any_match_raw(value_type,
                                    [slot_filter](const char* data, size_t size) {
                                        return slot_filter->bloom_filter->test_bytes(data, size);
                                    })
                       ? ZoneMapFilterResult::kMayMatch
                       : ZoneMapFilterResult::kNoMatch;
    case TYPE_FLOAT:
        return values.any_match_raw(value_type,
                                    [slot_filter](const char* data, size_t size) {
                                        DORIS_CHECK_EQ(size, sizeof(float));
                                        const auto value = *reinterpret_cast<const float*>(data);
                                        return floating_point_bloom_filter_may_contain(
                                                *slot_filter->bloom_filter, value);
                                    })
                       ? ZoneMapFilterResult::kMayMatch
                       : ZoneMapFilterResult::kNoMatch;
    case TYPE_DOUBLE:
        return values.any_match_raw(value_type,
                                    [slot_filter](const char* data, size_t size) {
                                        DORIS_CHECK_EQ(size, sizeof(double));
                                        const auto value = *reinterpret_cast<const double*>(data);
                                        return floating_point_bloom_filter_may_contain(
                                                *slot_filter->bloom_filter, value);
                                    })
                       ? ZoneMapFilterResult::kMayMatch
                       : ZoneMapFilterResult::kNoMatch;
    default:
        return ZoneMapFilterResult::kMayMatch;
    }
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
