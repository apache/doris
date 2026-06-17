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

#include "exec/runtime_filter/runtime_filter_partition_pruner.h"

#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/field.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/hybrid_set.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"

namespace doris {

namespace {

template <PrimitiveType PT>
bool bloom_may_match_fixed_values(const ColumnValueRange<PT>& cvr,
                                  BloomFilterFuncBase* bloom_filter) {
    if (cvr.get_fixed_value_size() == 0) {
        return false;
    }

    using CppType = typename PrimitiveTypeTraits<PT>::CppType;
    using ColumnType = typename PrimitiveTypeTraits<PT>::ColumnType;

    MutableColumnPtr values_column;
    if constexpr (IsDecimalNumber<CppType>) {
        values_column = ColumnType::create(0, cvr.scale());
    } else {
        values_column = ColumnType::create();
    }
    auto* typed_column = static_cast<ColumnType*>(values_column.get());
    for (const auto& value : cvr.get_fixed_value_set()) {
        typed_column->insert_value(value);
    }
    const size_t row_count = values_column->size();

    std::vector<uint8_t> results(row_count, 0);
    ColumnPtr values_column_ptr = std::move(values_column);
    bloom_filter->find_fixed_len(values_column_ptr, results.data());
    return std::any_of(results.begin(), results.end(),
                       [](uint8_t matched) { return matched != 0; });
}

} // namespace

// NOLINTBEGIN(readability-function-cognitive-complexity,readability-function-size)
// Complexity is inflated by macro expansion for each PrimitiveType case.
Status ParsedPartitionBoundaries::parse(
        const std::vector<TPartitionBoundary>& boundaries,
        const phmap::flat_hash_map<int, SlotDescriptor*>& slot_descs) {
    for (const auto& tb : boundaries) {
        DORIS_CHECK(tb.__isset.partition_id);
        DORIS_CHECK(tb.__isset.slot_id);
        SlotId slot_id = tb.slot_id;

        auto slot_it = slot_descs.find(slot_id);
        DORIS_CHECK(slot_it != slot_descs.end());
        SlotDescriptor* slot = slot_it->second;
        // Reuse the slot's pre-built DataType: walking through VLiteral here
        // would cost a `DataTypeFactory::create_data_type(node)` heap allocation
        // and a one-row `ColumnConst` allocation per boundary endpoint. With
        // thousands of partitions that dominates BuildTasksTime.
        const DataTypePtr& slot_type = slot->type();
        PrimitiveType ptype = slot_type->get_primitive_type();
        int precision = cast_set<int>(slot_type->get_precision());
        int scale = cast_set<int>(slot_type->get_scale());
        bool is_nullable = slot->is_nullable();

        // Store slot data type for potential projection use
        _slot_data_types[slot_id] = slot_type;

        ParsedBoundary boundary;
        boundary.partition_id = tb.partition_id;
        boundary.slot_id = slot_id;
        boundary.is_nullable = is_nullable;

        bool parsed_ok = false;

#define BUILD_BOUNDARY_CVR(NAME)                                                                  \
    case TYPE_##NAME: {                                                                           \
        using CppType = typename PrimitiveTypeTraits<TYPE_##NAME>::CppType;                       \
        bool is_list = tb.__isset.list_values && !tb.list_values.empty();                         \
        bool is_range = tb.__isset.range_start || tb.__isset.range_end;                           \
        DORIS_CHECK(is_list || is_range);                                                         \
        boundary.is_list_boundary = is_list;                                                      \
        ColumnValueRange<TYPE_##NAME> cvr(slot->col_name(), is_nullable, precision, scale);       \
        /* Returns nullopt if `node` is a NULL literal; the caller then sets contain_null  */     \
        /* on the CVR instead of trying to extract a typed value (which would dereference  */     \
        /* a null data pointer for the non-string branch).                                 */     \
        auto parse_texpr_node = [&](const TExprNode& node) -> std::optional<CppType> {            \
            if (node.node_type == TExprNodeType::NULL_LITERAL) {                                  \
                return std::nullopt;                                                              \
            }                                                                                     \
            /* `Field` value is copied into the CVR by `add_fixed_value` /             */         \
            /* `add_range` (both take CppType by const-ref / by value), so the         */         \
            /* temporary `Field`'s lifetime ending at this expression's full-statement */         \
            /* boundary is safe -- including for `String` payloads.                    */         \
            Field field = slot_type->get_field(node);                                             \
            return std::make_optional<CppType>(field.get<TYPE_##NAME>());                         \
        };                                                                                        \
        if (is_list) {                                                                            \
            auto empty_cvr = ColumnValueRange<TYPE_##NAME>::create_empty_column_value_range(      \
                    is_nullable, precision, scale);                                               \
            bool list_has_null = false;                                                           \
            bool list_has_value = false;                                                          \
            for (const auto& node : tb.list_values) {                                             \
                auto parsed = parse_texpr_node(node);                                             \
                if (!parsed) {                                                                    \
                    list_has_null = true;                                                         \
                    continue;                                                                     \
                }                                                                                 \
                static_cast<void>(empty_cvr.add_fixed_value(*parsed));                            \
                list_has_value = true;                                                            \
            }                                                                                     \
            if (list_has_value) {                                                                 \
                cvr.intersection(empty_cvr);                                                      \
            }                                                                                     \
            if (list_has_null) {                                                                  \
                DORIS_CHECK(is_nullable);                                                         \
                /* Track NULL membership on ParsedBoundary; calling          */                   \
                /* cvr.set_contain_null(true) here would invoke              */                   \
                /* set_empty_value_range() and discard the concrete fixed    */                   \
                /* values we just inserted, turning {NULL, v} into a         */                   \
                /* NULL-only boundary.                                       */                   \
                boundary.contains_null = true;                                                    \
                if (!list_has_value) {                                                            \
                    boundary.only_null = true;                                                    \
                }                                                                                 \
            }                                                                                     \
        } else {                                                                                  \
            bool range_has_null = false;                                                          \
            if (is_nullable && !tb.__isset.range_start) {                                         \
                range_has_null = true;                                                            \
            }                                                                                     \
            if (tb.__isset.range_start) {                                                         \
                auto parsed = parse_texpr_node(tb.range_start);                                   \
                if (parsed) {                                                                     \
                    static_cast<void>(cvr.add_range(FILTER_LARGER_OR_EQUAL, *parsed));            \
                } else {                                                                          \
                    DORIS_CHECK(is_nullable);                                                     \
                    range_has_null = true;                                                        \
                }                                                                                 \
            }                                                                                     \
            if (tb.__isset.range_end) {                                                           \
                auto parsed = parse_texpr_node(tb.range_end);                                     \
                DORIS_CHECK(parsed.has_value());                                                  \
                /* Multi-column RANGE projection emits a CLOSED upper bound (see       */         \
                /* TPartitionBoundary.range_end_inclusive comment); single-column RANGE */        \
                /* keeps the natural OPEN upper bound matching Doris semantics.         */        \
                SQLFilterOp upper_op = (tb.__isset.range_end_inclusive && tb.range_end_inclusive) \
                                               ? FILTER_LESS_OR_EQUAL                             \
                                               : FILTER_LESS;                                     \
                static_cast<void>(cvr.add_range(upper_op, *parsed));                              \
            }                                                                                     \
            if (range_has_null) {                                                                 \
                boundary.contains_null = true;                                                    \
            }                                                                                     \
        }                                                                                         \
        boundary.boundary_cvr = std::move(cvr);                                                   \
        parsed_ok = true;                                                                         \
        break;                                                                                    \
    }

        switch (ptype) {
            BUILD_BOUNDARY_CVR(TINYINT)
            BUILD_BOUNDARY_CVR(SMALLINT)
            BUILD_BOUNDARY_CVR(INT)
            BUILD_BOUNDARY_CVR(BIGINT)
            BUILD_BOUNDARY_CVR(LARGEINT)
            BUILD_BOUNDARY_CVR(FLOAT)
            BUILD_BOUNDARY_CVR(DOUBLE)
            BUILD_BOUNDARY_CVR(CHAR)
            BUILD_BOUNDARY_CVR(DATE)
            BUILD_BOUNDARY_CVR(DATETIME)
            BUILD_BOUNDARY_CVR(DATEV2)
            BUILD_BOUNDARY_CVR(DATETIMEV2)
            BUILD_BOUNDARY_CVR(TIMESTAMPTZ)
            BUILD_BOUNDARY_CVR(VARCHAR)
            BUILD_BOUNDARY_CVR(STRING)
            BUILD_BOUNDARY_CVR(DECIMAL32)
            BUILD_BOUNDARY_CVR(DECIMAL64)
            BUILD_BOUNDARY_CVR(DECIMAL128I)
            BUILD_BOUNDARY_CVR(DECIMAL256)
            BUILD_BOUNDARY_CVR(DECIMALV2)
            BUILD_BOUNDARY_CVR(BOOLEAN)
            BUILD_BOUNDARY_CVR(IPV4)
            BUILD_BOUNDARY_CVR(IPV6)
        default:
            break;
        }
#undef BUILD_BOUNDARY_CVR

        DORIS_CHECK(parsed_ok);
        _slot_to_boundaries[slot_id].push_back(std::move(boundary));
    }

    // Count distinct partition IDs across all boundaries.
    if (!_slot_to_boundaries.empty()) {
        phmap::flat_hash_set<int64_t> all_partition_ids;
        for (const auto& [_, slot_boundaries] : _slot_to_boundaries) {
            for (const auto& pb : slot_boundaries) {
                all_partition_ids.insert(pb.partition_id);
            }
        }
        _total_partition_count = static_cast<int64_t>(all_partition_ids.size());
    }
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity,readability-function-size)

static bool find_unique_slot_ref_impl(const VExpr* expr, const VSlotRef** found) {
    DORIS_CHECK(expr != nullptr);
    if (expr->is_slot_ref()) {
        const auto* slot = assert_cast<const VSlotRef*>(expr);
        if (*found == nullptr) {
            *found = slot;
            return true;
        }
        if ((*found)->slot_id() != slot->slot_id()) {
            return false;
        }
        DORIS_CHECK((*found)->column_id() == slot->column_id());
        return true;
    }
    return std::ranges::all_of(expr->children(), [&](const auto& child) {
        return find_unique_slot_ref_impl(child.get(), found);
    });
}

static const VSlotRef* find_unique_slot_ref(const VExpr* expr) {
    const VSlotRef* found = nullptr;
    if (!find_unique_slot_ref_impl(expr, &found)) {
        return nullptr;
    }
    return found;
}

// NOLINTBEGIN(readability-function-cognitive-complexity,readability-function-size)
Status ParsedPartitionBoundaries::get_or_compute_projected_boundaries(
        int filter_id, const VExprSPtr& target_expr, SlotId leaf_slot_id, int leaf_column_id,
        const std::unordered_map<int64_t, TTargetExprMonotonicity::type>& partition_directions,
        VExprContext* ctx, std::shared_ptr<const std::vector<ParsedBoundary>>* output) const {
    {
        std::lock_guard<std::mutex> lock(_projected_boundaries_mutex);
        auto it = _projected_boundaries_by_filter_id.find(filter_id);
        if (it != _projected_boundaries_by_filter_id.end()) {
            *output = it->second;
            return Status::OK();
        }
    }

    std::vector<ParsedBoundary> projected;
    auto store_projected_boundaries = [&](std::vector<ParsedBoundary>&& boundaries) {
        auto cached = std::make_shared<const std::vector<ParsedBoundary>>(std::move(boundaries));
        std::lock_guard<std::mutex> lock(_projected_boundaries_mutex);
        auto [it, inserted] = _projected_boundaries_by_filter_id.emplace(filter_id, cached);
        *output = inserted ? cached : it->second;
        return Status::OK();
    };

    auto slot_boundaries_it = _slot_to_boundaries.find(leaf_slot_id);
    if (slot_boundaries_it == _slot_to_boundaries.end()) {
        return store_projected_boundaries(std::move(projected));
    }

    const auto& orig_boundaries = slot_boundaries_it->second;
    if (orig_boundaries.empty()) {
        return store_projected_boundaries(std::move(projected));
    }

    std::unordered_set<int64_t> boundary_partition_ids;
    boundary_partition_ids.reserve(orig_boundaries.size());
    for (const auto& boundary : orig_boundaries) {
        boundary_partition_ids.insert(boundary.partition_id);
    }
    for (const auto& [partition_id, direction] : partition_directions) {
        DORIS_CHECK(direction != TTargetExprMonotonicity::NON_MONOTONIC);
        DORIS_CHECK(boundary_partition_ids.contains(partition_id));
    }

    if (target_expr->is_slot_ref()) {
        auto* slot_ref = assert_cast<VSlotRef*>(target_expr.get());
        DORIS_CHECK(slot_ref->slot_id() == leaf_slot_id);
        std::vector<ParsedBoundary> slot_boundaries;
        slot_boundaries.reserve(orig_boundaries.size());
        for (const auto& boundary : orig_boundaries) {
            auto direction_it = partition_directions.find(boundary.partition_id);
            if (direction_it == partition_directions.end()) {
                continue;
            }
            DORIS_CHECK(direction_it->second == TTargetExprMonotonicity::MONOTONIC_INCREASING);
            slot_boundaries.emplace_back(boundary);
        }
        return store_projected_boundaries(std::move(slot_boundaries));
    }

    std::vector<std::pair<size_t, TTargetExprMonotonicity::type>> selected_boundaries;
    selected_boundaries.reserve(orig_boundaries.size());
    for (size_t i = 0; i < orig_boundaries.size(); ++i) {
        auto it = partition_directions.find(orig_boundaries[i].partition_id);
        if (it != partition_directions.end() &&
            it->second != TTargetExprMonotonicity::NON_MONOTONIC) {
            selected_boundaries.emplace_back(i, it->second);
        }
    }
    if (selected_boundaries.empty()) {
        return store_projected_boundaries(std::move(projected));
    }

    std::vector<std::pair<size_t, TTargetExprMonotonicity::type>> projectable_boundaries;
    projectable_boundaries.reserve(selected_boundaries.size());
    for (const auto& selected_boundary : selected_boundaries) {
        const auto& boundary = orig_boundaries[selected_boundary.first];
        if (!boundary.only_null && !boundary.contains_null) {
            projectable_boundaries.emplace_back(selected_boundary);
        }
    }
    selected_boundaries.swap(projectable_boundaries);
    if (selected_boundaries.empty()) {
        return store_projected_boundaries(std::move(projected));
    }

    auto slot_type_it = _slot_data_types.find(leaf_slot_id);
    DORIS_CHECK(slot_type_it != _slot_data_types.end());

    const DataTypePtr& input_type = slot_type_it->second;
    bool input_is_nullable = input_type->is_nullable();
    DataTypePtr inner_type = input_is_nullable ? remove_nullable(input_type) : input_type;
    PrimitiveType input_ptype = input_type->get_primitive_type();

    size_t N = selected_boundaries.size();
    std::vector<bool> lo_open(N, false);
    std::vector<bool> hi_open(N, false);
    std::vector<bool> boundary_is_list(N, false);
    std::vector<size_t> lo_result_row(N, 0);
    std::vector<size_t> hi_result_row(N, 0);
    std::vector<size_t> list_result_begin(N, 0);
    std::vector<size_t> list_result_end(N, 0);
    size_t lo_row_count = 0;
    size_t hi_row_count = 0;
    size_t list_row_count = 0;

    // Build input blocks for RANGE endpoints and LIST values.
    Block lo_block;
    Block hi_block;
    Block list_block;

    // Pad columns 0..leaf_column_id-1 with placeholder columns so the leaf
    // VSlotRef (whose column_id is leaf_column_id) reads our typed column.
    auto add_dummy_columns = [&](Block& block, size_t row_count) {
        for (int col_idx = 0; col_idx < leaf_column_id; ++col_idx) {
            auto col = ColumnUInt8::create(row_count, 0);
            block.insert({std::move(col), std::make_shared<DataTypeUInt8>(),
                          fmt::format("dummy_{}", col_idx)});
        }
    };

    // Macro-dispatch on input PrimitiveType to build finite RANGE endpoint
    // columns and LIST value columns. Unbounded RANGE endpoints are not executed
    // through target_expr; they stay unbounded after projection and swap sides
    // for monotonic decreasing targets.
    bool input_built = false;
#define BUILD_INPUT_COLUMNS(INPUT_PT)                                                            \
    case TYPE_##INPUT_PT: {                                                                      \
        using InCol = typename PrimitiveTypeTraits<TYPE_##INPUT_PT>::ColumnType;                 \
        MutableColumnPtr lo_inner_base = inner_type->create_column();                            \
        MutableColumnPtr hi_inner_base = inner_type->create_column();                            \
        MutableColumnPtr list_inner_base = inner_type->create_column();                          \
        auto* lo_inner = assert_cast<InCol*>(lo_inner_base.get());                               \
        auto* hi_inner = assert_cast<InCol*>(hi_inner_base.get());                               \
        auto* list_inner = assert_cast<InCol*>(list_inner_base.get());                           \
        lo_inner->reserve(N);                                                                    \
        hi_inner->reserve(N);                                                                    \
        list_inner->reserve(N);                                                                  \
        auto lo_nulls = ColumnUInt8::create();                                                   \
        auto hi_nulls = ColumnUInt8::create();                                                   \
        auto list_nulls = ColumnUInt8::create();                                                 \
        lo_nulls->reserve(N);                                                                    \
        hi_nulls->reserve(N);                                                                    \
        list_nulls->reserve(N);                                                                  \
        for (size_t i = 0; i < N; ++i) {                                                         \
            const auto& boundary = orig_boundaries[selected_boundaries[i].first];                \
            const auto* cvr =                                                                    \
                    std::get_if<ColumnValueRange<TYPE_##INPUT_PT>>(&boundary.boundary_cvr);      \
            DCHECK(cvr != nullptr);                                                              \
            DCHECK(!boundary.only_null);                                                         \
            DCHECK(!boundary.contains_null);                                                     \
            boundary_is_list[i] = boundary.is_list_boundary;                                     \
            if (boundary_is_list[i]) {                                                           \
                DORIS_CHECK(cvr->is_fixed_value_range());                                        \
                list_result_begin[i] = list_row_count;                                           \
                for (const auto& value : cvr->get_fixed_value_set()) {                           \
                    list_inner->insert_value(value);                                             \
                    list_nulls->get_data().push_back(0);                                         \
                    ++list_row_count;                                                            \
                }                                                                                \
                list_result_end[i] = list_row_count;                                             \
                continue;                                                                        \
            }                                                                                    \
            lo_open[i] = cvr->is_low_value_minimum();                                            \
            hi_open[i] = cvr->is_high_value_maximum();                                           \
            if (!lo_open[i]) {                                                                   \
                lo_result_row[i] = lo_row_count++;                                               \
                lo_inner->insert_value(cvr->get_range_min_value());                              \
                lo_nulls->get_data().push_back(0);                                               \
            }                                                                                    \
            if (!hi_open[i]) {                                                                   \
                hi_result_row[i] = hi_row_count++;                                               \
                hi_inner->insert_value(cvr->get_range_max_value());                              \
                hi_nulls->get_data().push_back(0);                                               \
            }                                                                                    \
        }                                                                                        \
        add_dummy_columns(lo_block, lo_row_count);                                               \
        add_dummy_columns(hi_block, hi_row_count);                                               \
        add_dummy_columns(list_block, list_row_count);                                           \
        if (input_is_nullable) {                                                                 \
            auto lo_col = ColumnNullable::create(std::move(lo_inner_base), std::move(lo_nulls)); \
            auto hi_col = ColumnNullable::create(std::move(hi_inner_base), std::move(hi_nulls)); \
            auto list_col =                                                                      \
                    ColumnNullable::create(std::move(list_inner_base), std::move(list_nulls));   \
            lo_block.insert({std::move(lo_col), input_type, "leaf_slot"});                       \
            hi_block.insert({std::move(hi_col), input_type, "leaf_slot"});                       \
            list_block.insert({std::move(list_col), input_type, "leaf_slot"});                   \
        } else {                                                                                 \
            lo_block.insert({std::move(lo_inner_base), input_type, "leaf_slot"});                \
            hi_block.insert({std::move(hi_inner_base), input_type, "leaf_slot"});                \
            list_block.insert({std::move(list_inner_base), input_type, "leaf_slot"});            \
        }                                                                                        \
        input_built = true;                                                                      \
        break;                                                                                   \
    }

    switch (input_ptype) {
        BUILD_INPUT_COLUMNS(TINYINT)
        BUILD_INPUT_COLUMNS(SMALLINT)
        BUILD_INPUT_COLUMNS(INT)
        BUILD_INPUT_COLUMNS(BIGINT)
        BUILD_INPUT_COLUMNS(LARGEINT)
        BUILD_INPUT_COLUMNS(FLOAT)
        BUILD_INPUT_COLUMNS(DOUBLE)
        BUILD_INPUT_COLUMNS(CHAR)
        BUILD_INPUT_COLUMNS(DATE)
        BUILD_INPUT_COLUMNS(DATETIME)
        BUILD_INPUT_COLUMNS(DATEV2)
        BUILD_INPUT_COLUMNS(DATETIMEV2)
        BUILD_INPUT_COLUMNS(TIMESTAMPTZ)
        BUILD_INPUT_COLUMNS(VARCHAR)
        BUILD_INPUT_COLUMNS(STRING)
        BUILD_INPUT_COLUMNS(DECIMAL32)
        BUILD_INPUT_COLUMNS(DECIMAL64)
        BUILD_INPUT_COLUMNS(DECIMAL128I)
        BUILD_INPUT_COLUMNS(DECIMAL256)
        BUILD_INPUT_COLUMNS(DECIMALV2)
        BUILD_INPUT_COLUMNS(BOOLEAN)
        BUILD_INPUT_COLUMNS(IPV4)
        BUILD_INPUT_COLUMNS(IPV6)
    default:
        break;
    }
#undef BUILD_INPUT_COLUMNS

    DORIS_CHECK(input_built);

    int lo_result_id = -1;
    int hi_result_id = -1;
    int list_result_id = -1;
    ColumnPtr lo_result_col;
    ColumnPtr hi_result_col;
    ColumnPtr list_result_col;
    if (lo_row_count > 0) {
        RETURN_IF_ERROR(target_expr->execute(ctx, &lo_block, &lo_result_id));
        DORIS_CHECK(lo_result_id >= 0);
        lo_result_col = lo_block.get_by_position(lo_result_id).column;
    }
    if (hi_row_count > 0) {
        RETURN_IF_ERROR(target_expr->execute(ctx, &hi_block, &hi_result_id));
        DORIS_CHECK(hi_result_id >= 0);
        hi_result_col = hi_block.get_by_position(hi_result_id).column;
    }
    if (list_row_count > 0) {
        RETURN_IF_ERROR(target_expr->execute(ctx, &list_block, &list_result_id));
        DORIS_CHECK(list_result_id >= 0);
        list_result_col = list_block.get_by_position(list_result_id).column;
    }
    int out_precision = cast_set<int>(target_expr->data_type()->get_precision());
    int out_scale = cast_set<int>(target_expr->data_type()->get_scale());
    bool out_nullable = target_expr->data_type()->is_nullable();
    PrimitiveType output_ptype = target_expr->data_type()->get_primitive_type();

    projected.reserve(N);

    bool output_built = false;
#define BUILD_PROJECTED_CVR(OUTPUT_PT)                                                          \
    case TYPE_##OUTPUT_PT: {                                                                    \
        using OutputCppType = typename PrimitiveTypeTraits<TYPE_##OUTPUT_PT>::CppType;          \
        auto projected_value = [](const ColumnPtr& column, size_t row) -> OutputCppType {       \
            Field field = (*column)[row];                                                       \
            return field.get<TYPE_##OUTPUT_PT>();                                               \
        };                                                                                      \
        for (size_t i = 0; i < N; ++i) {                                                        \
            const auto& orig_boundary = orig_boundaries[selected_boundaries[i].first];          \
            TTargetExprMonotonicity::type direction = selected_boundaries[i].second;            \
            if (boundary_is_list[i]) {                                                          \
                auto cvr = ColumnValueRange<TYPE_##OUTPUT_PT>::create_empty_column_value_range( \
                        out_nullable, out_precision, out_scale);                                \
                bool list_has_value = false;                                                    \
                bool list_has_null = false;                                                     \
                for (size_t row = list_result_begin[i]; row < list_result_end[i]; ++row) {      \
                    if (list_result_col->is_null_at(row)) {                                     \
                        list_has_null = true;                                                   \
                        continue;                                                               \
                    }                                                                           \
                    auto projected_list_value = projected_value(list_result_col, row);          \
                    static_cast<void>(cvr.add_fixed_value(projected_list_value));               \
                    list_has_value = true;                                                      \
                }                                                                               \
                if (!list_has_value && !list_has_null) {                                        \
                    continue;                                                                   \
                }                                                                               \
                ParsedBoundary projected_boundary;                                              \
                projected_boundary.partition_id = orig_boundary.partition_id;                   \
                projected_boundary.slot_id = leaf_slot_id;                                      \
                projected_boundary.is_nullable = out_nullable;                                  \
                projected_boundary.is_list_boundary = true;                                     \
                projected_boundary.contains_null = list_has_null;                               \
                projected_boundary.only_null = list_has_null && !list_has_value;                \
                projected_boundary.boundary_cvr = std::move(cvr);                               \
                projected.emplace_back(std::move(projected_boundary));                          \
                continue;                                                                       \
            }                                                                                   \
            if ((!lo_open[i] && lo_result_col->is_null_at(lo_result_row[i])) ||                 \
                (!hi_open[i] && hi_result_col->is_null_at(hi_result_row[i]))) {                 \
                continue;                                                                       \
            }                                                                                   \
            ColumnValueRange<TYPE_##OUTPUT_PT> cvr("", out_nullable, out_precision, out_scale); \
            /* Use closed projected finite bounds as a conservative envelope for */             \
            /* non-strict monotonic expressions such as date ceil/floor/cast.    */             \
            if (direction == TTargetExprMonotonicity::MONOTONIC_DECREASING) {                   \
                if (!hi_open[i]) {                                                              \
                    auto hi_projected = projected_value(hi_result_col, hi_result_row[i]);       \
                    static_cast<void>(cvr.add_range(FILTER_LARGER_OR_EQUAL, hi_projected));     \
                }                                                                               \
                if (!lo_open[i]) {                                                              \
                    auto lo_projected = projected_value(lo_result_col, lo_result_row[i]);       \
                    static_cast<void>(cvr.add_range(FILTER_LESS_OR_EQUAL, lo_projected));       \
                }                                                                               \
            } else {                                                                            \
                if (!lo_open[i]) {                                                              \
                    auto lo_projected = projected_value(lo_result_col, lo_result_row[i]);       \
                    static_cast<void>(cvr.add_range(FILTER_LARGER_OR_EQUAL, lo_projected));     \
                }                                                                               \
                if (!hi_open[i]) {                                                              \
                    auto hi_projected = projected_value(hi_result_col, hi_result_row[i]);       \
                    static_cast<void>(cvr.add_range(FILTER_LESS_OR_EQUAL, hi_projected));       \
                }                                                                               \
            }                                                                                   \
            ParsedBoundary projected_boundary;                                                  \
            projected_boundary.partition_id = orig_boundary.partition_id;                       \
            projected_boundary.slot_id = leaf_slot_id;                                          \
            projected_boundary.is_nullable = out_nullable;                                      \
            projected_boundary.is_list_boundary = false;                                        \
            projected_boundary.only_null = orig_boundary.only_null;                             \
            projected_boundary.contains_null = orig_boundary.contains_null;                     \
            projected_boundary.boundary_cvr = std::move(cvr);                                   \
            projected.emplace_back(std::move(projected_boundary));                              \
        }                                                                                       \
        output_built = true;                                                                    \
        break;                                                                                  \
    }

    switch (output_ptype) {
        BUILD_PROJECTED_CVR(TINYINT)
        BUILD_PROJECTED_CVR(SMALLINT)
        BUILD_PROJECTED_CVR(INT)
        BUILD_PROJECTED_CVR(BIGINT)
        BUILD_PROJECTED_CVR(LARGEINT)
        BUILD_PROJECTED_CVR(FLOAT)
        BUILD_PROJECTED_CVR(DOUBLE)
        BUILD_PROJECTED_CVR(CHAR)
        BUILD_PROJECTED_CVR(DATE)
        BUILD_PROJECTED_CVR(DATETIME)
        BUILD_PROJECTED_CVR(DATEV2)
        BUILD_PROJECTED_CVR(DATETIMEV2)
        BUILD_PROJECTED_CVR(TIMESTAMPTZ)
        BUILD_PROJECTED_CVR(VARCHAR)
        BUILD_PROJECTED_CVR(STRING)
        BUILD_PROJECTED_CVR(DECIMAL32)
        BUILD_PROJECTED_CVR(DECIMAL64)
        BUILD_PROJECTED_CVR(DECIMAL128I)
        BUILD_PROJECTED_CVR(DECIMAL256)
        BUILD_PROJECTED_CVR(DECIMALV2)
        BUILD_PROJECTED_CVR(BOOLEAN)
        BUILD_PROJECTED_CVR(IPV4)
        BUILD_PROJECTED_CVR(IPV6)
    default:
        break;
    }

#undef BUILD_PROJECTED_CVR

    DORIS_CHECK(output_built);

    return store_projected_boundaries(std::move(projected));
}
// NOLINTEND(readability-function-cognitive-complexity,readability-function-size)

static std::optional<SQLFilterOp> convert_opcode_to_filter_op(TExprOpcode::type op) {
    switch (op) {
    case TExprOpcode::LE:
        return FILTER_LESS_OR_EQUAL;
    case TExprOpcode::LT:
        return FILTER_LESS;
    case TExprOpcode::GE:
        return FILTER_LARGER_OR_EQUAL;
    case TExprOpcode::GT:
        return FILTER_LARGER;
    default:
        return std::nullopt;
    }
}

// NOLINTBEGIN(readability-function-cognitive-complexity,readability-function-size)
void RuntimeFilterPartitionPruner::_try_prune_by_single_rf(
        const std::vector<ParsedBoundary>& boundaries, const VExprSPtr& impl,
        phmap::flat_hash_set<int64_t>& newly_pruned) {
    // Pre-compute whether the RF "matches NULL" -- i.e. whether the RF would
    // accept a row whose probe value is NULL. The signal differs by RF impl:
    //   * IN filter      → HybridSet::contain_null()
    //   * Bloom filter   → BloomFilterFuncBase::contain_null()
    //   * MinMax filter  → encoded via the BinaryPredicate node_type:
    //                      NULL_AWARE_BINARY_PRED means the build side had a
    //                      NULL key and a null-safe equal join asked NULL to
    //                      match NULL; plain BINARY_PRED never matches NULL.
    // FilterBase::contain_null() already folds in `_null_aware`, so we only
    // get a true result when the build side is actually null-aware AND
    // produced a NULL value.
    if (impl->node_type() == TExprNodeType::BLOOM_PRED) {
        auto bloom = impl->get_bloom_filter_func();
        DORIS_CHECK(bloom != nullptr);
        bool rf_contains_null = bloom->contain_null();

        for (const auto& pb : boundaries) {
            if (_pruned_partition_ids.contains(pb.partition_id) ||
                newly_pruned.contains(pb.partition_id)) {
                continue;
            }

            if (pb.only_null) {
                if (!rf_contains_null) {
                    newly_pruned.insert(pb.partition_id);
                }
                continue;
            }
            if (pb.contains_null && rf_contains_null) {
                continue;
            }
            if (!pb.is_list_boundary) {
                continue;
            }

            bool may_match = true;
            std::visit(
                    [&](const auto& boundary_cvr) {
                        if (!boundary_cvr.is_fixed_value_range()) {
                            return;
                        }
                        may_match = bloom_may_match_fixed_values(boundary_cvr, bloom.get());
                    },
                    pb.boundary_cvr);
            if (!may_match) {
                newly_pruned.insert(pb.partition_id);
            }
        }
        return;
    }

    bool rf_contains_null = false;
    if (auto hybrid_set = impl->get_set_func()) {
        rf_contains_null = hybrid_set->contain_null();
    } else if (impl->node_type() == TExprNodeType::NULL_AWARE_BINARY_PRED) {
        // Min/Max RF built on a null-safe equal join. The literal child holds
        // the min or max bound; the NULL semantic is conveyed by the node
        // type itself (see create_vbin_predicate in runtime_filter/utils.cpp).
        rf_contains_null = true;
    }

    std::optional<ColumnValueRangeType> rf_cvr;
    if (!boundaries.empty()) {
        std::visit(
                [&](const auto& boundary_cvr) {
                    using CvrType = std::decay_t<decltype(boundary_cvr)>;
                    using CppType = typename CvrType::CppType;

                    auto hybrid_set = impl->get_set_func();
                    if (hybrid_set) {
                        auto typed_rf_cvr = CvrType::create_empty_column_value_range(
                                boundaries.front().is_nullable, boundary_cvr.precision(),
                                boundary_cvr.scale());
                        auto* iter = hybrid_set->begin();
                        while (iter->has_next()) {
                            const void* value = iter->get_value();
                            if (value) {
                                if constexpr (std::is_same_v<CppType, String>) {
                                    // String HybridSets store StringRef*, but
                                    // ColumnValueRange<String>::CppType is
                                    // std::string. Construct from the bytes.
                                    const auto* str_val = reinterpret_cast<const StringRef*>(value);
                                    static_cast<void>(typed_rf_cvr.add_fixed_value(
                                            CppType(str_val->data, str_val->size)));
                                } else {
                                    static_cast<void>(typed_rf_cvr.add_fixed_value(
                                            *reinterpret_cast<const CppType*>(value)));
                                }
                            }
                            iter->next();
                        }
                        rf_cvr.emplace(std::move(typed_rf_cvr));
                    } else if ((impl->node_type() == TExprNodeType::BINARY_PRED ||
                                impl->node_type() == TExprNodeType::NULL_AWARE_BINARY_PRED) &&
                               impl->children().size() == 2 && impl->children()[1]->is_literal()) {
                        // MinMax filter: binary pred with literal bound
                        auto* literal = assert_cast<VLiteral*>(impl->children()[1].get());
                        auto col_ptr = literal->get_column_ptr();
                        auto data = col_ptr->get_data_at(0);
                        CppType val {};
                        if constexpr (std::is_same_v<CppType, String>) {
                            // get_data_at returns a StringRef pointing at the
                            // raw character bytes; ColumnValueRange<String>'s
                            // CppType is std::string, so construct one from
                            // the bytes rather than dereferencing as String.
                            val = CppType(data.data, data.size);
                        } else {
                            val = *reinterpret_cast<const CppType*>(data.data);
                        }

                        auto op = convert_opcode_to_filter_op(impl->op());
                        DORIS_CHECK(op.has_value());

                        CvrType typed_rf_cvr(boundary_cvr.column_name(),
                                             boundaries.front().is_nullable,
                                             boundary_cvr.precision(), boundary_cvr.scale());
                        static_cast<void>(typed_rf_cvr.add_range(*op, val));
                        rf_cvr.emplace(std::move(typed_rf_cvr));
                    }
                },
                boundaries.front().boundary_cvr);
    }
    if (!rf_cvr.has_value()) {
        return;
    }

    for (const auto& pb : boundaries) {
        if (_pruned_partition_ids.contains(pb.partition_id) ||
            newly_pruned.contains(pb.partition_id)) {
            continue;
        }

        // NULL handling:
        //   A partition row whose key is NULL matches the RF iff `rf_contains_null`.
        //   - only_null partition (rows are exclusively NULL): prunable iff !rf_contains_null.
        //   - mixed (NULL + concrete values): if rf_contains_null, NULL rows alone
        //     prevent pruning. Otherwise NULL rows can never match, so we ignore
        //     the NULL portion and let the regular non-NULL intersection decide.
        if (pb.only_null) {
            if (!rf_contains_null) {
                newly_pruned.insert(pb.partition_id);
            }
            continue;
        }
        if (pb.contains_null && rf_contains_null) {
            continue;
        }

        std::visit(
                [&](const auto& boundary_cvr) {
                    using CvrType = std::decay_t<decltype(boundary_cvr)>;

                    const auto* typed_rf_cvr = std::get_if<CvrType>(&rf_cvr.value());
                    DORIS_CHECK(typed_rf_cvr != nullptr);

                    auto boundary_copy = boundary_cvr;
                    auto rf_cvr_copy = *typed_rf_cvr;
                    boundary_copy.intersection(rf_cvr_copy);
                    if (boundary_copy.is_empty_value_range()) {
                        newly_pruned.insert(pb.partition_id);
                    }
                },
                pb.boundary_cvr);
    }
}

Status RuntimeFilterPartitionPruner::prune_by_runtime_filters(
        const ParsedPartitionBoundaries& parsed, const VExprContextSPtrs& conjuncts,
        const std::vector<TRuntimeFilterDesc>& rf_descs, int scan_node_id,
        int64_t* newly_pruned_count) {
    *newly_pruned_count = 0;
    if (parsed.empty()) {
        return Status::OK();
    }
    const auto& slot_to_boundaries = parsed.slot_to_boundaries();

    // Build filter_id -> per-partition monotonicity maps.
    std::unordered_map<int, std::unordered_map<int64_t, TTargetExprMonotonicity::type>>
            filter_id_to_partition_monotonicity;
    for (const auto& desc : rf_descs) {
        if (desc.__isset.planId_to_partition_target_monotonicity) {
            auto it = desc.planId_to_partition_target_monotonicity.find(scan_node_id);
            if (it != desc.planId_to_partition_target_monotonicity.end()) {
                auto& partition_monotonicity = filter_id_to_partition_monotonicity[desc.filter_id];
                for (const auto& partition_entry : it->second) {
                    DORIS_CHECK(partition_entry.__isset.partition_id);
                    DORIS_CHECK(partition_entry.__isset.monotonicity);
                    DORIS_CHECK(partition_entry.monotonicity !=
                                TTargetExprMonotonicity::NON_MONOTONIC);
                    auto [_, inserted] = partition_monotonicity.emplace(
                            partition_entry.partition_id, partition_entry.monotonicity);
                    DORIS_CHECK(inserted);
                }
            }
        }
    }

    // This function is serialized by _conjuncts_lock in the caller, so our reads
    // of _pruned_partition_ids never race with our writes below. The only concurrent
    // readers are is_partition_pruned() calls (under shared_lock), which are
    // properly synchronized by the unique_lock we take when inserting.
    phmap::flat_hash_set<int64_t> newly_pruned;

    for (const auto& conjunct_ctx : conjuncts) {
        VExprSPtr root = conjunct_ctx->root();
        if (!root->is_rf_wrapper()) {
            continue;
        }

        VExprSPtr impl = root->get_impl();
        if (!impl) {
            continue;
        }

        if (impl->children().empty()) {
            continue;
        }

        auto* wrapper_root = assert_cast<RuntimeFilterExpr*>(root.get());
        int filter_id = wrapper_root->filter_id();

        VExprSPtr target_subtree = impl->children()[0];

        auto partition_mono_it = filter_id_to_partition_monotonicity.find(filter_id);
        // For LIST partition targets, this metadata is also the per-partition eligibility map;
        // BE projects every finite LIST value, so the direction itself is neutral there.
        bool has_partition_mono = partition_mono_it != filter_id_to_partition_monotonicity.end() &&
                                  !partition_mono_it->second.empty();
        if (!has_partition_mono) {
            continue;
        }

        const VSlotRef* leaf_slot = find_unique_slot_ref(target_subtree.get());
        DORIS_CHECK(leaf_slot != nullptr);

        SlotId leaf_slot_id = leaf_slot->slot_id();
        DORIS_CHECK(slot_to_boundaries.contains(leaf_slot_id));

        int leaf_column_id = leaf_slot->column_id();

        std::shared_ptr<const std::vector<ParsedBoundary>> projected;
        RETURN_IF_ERROR(parsed.get_or_compute_projected_boundaries(
                filter_id, target_subtree, leaf_slot_id, leaf_column_id, partition_mono_it->second,
                conjunct_ctx.get(), &projected));

        if (projected == nullptr || projected->empty()) {
            continue;
        }

        _try_prune_by_single_rf(*projected, impl, newly_pruned);
    }

    auto count = static_cast<int64_t>(newly_pruned.size());
    if (count > 0) {
        std::unique_lock lock(_prune_mutex);
        for (int64_t pid : newly_pruned) {
            _pruned_partition_ids.insert(pid);
        }
    }
    *newly_pruned_count = count;
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity,readability-function-size)

bool RuntimeFilterPartitionPruner::is_partition_pruned(int64_t partition_id) const {
    std::shared_lock lock(_prune_mutex);
    return _pruned_partition_ids.contains(partition_id);
}

int64_t RuntimeFilterPartitionPruner::pruned_partition_count() const {
    std::shared_lock lock(_prune_mutex);
    return static_cast<int64_t>(_pruned_partition_ids.size());
}

} // namespace doris
