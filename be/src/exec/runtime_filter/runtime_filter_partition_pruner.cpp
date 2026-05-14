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

#include <optional>
#include <utility>

#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/field.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/hybrid_set.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vruntimefilter_wrapper.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"

namespace doris {

// NOLINTBEGIN(readability-function-cognitive-complexity,readability-function-size)
// Complexity is inflated by macro expansion for each PrimitiveType case.
void ParsedPartitionBoundaries::parse(
        const std::vector<TPartitionBoundary>& boundaries,
        const phmap::flat_hash_map<int, SlotDescriptor*>& slot_descs) {
    for (const auto& tb : boundaries) {
        if (!tb.__isset.partition_id || !tb.__isset.slot_id) {
            continue;
        }
        SlotId slot_id = tb.slot_id;

        auto slot_it = slot_descs.find(slot_id);
        if (slot_it == slot_descs.end()) {
            continue;
        }
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

#define BUILD_BOUNDARY_CVR(NAME)                                                               \
    case TYPE_##NAME: {                                                                        \
        using CppType = typename PrimitiveTypeTraits<TYPE_##NAME>::CppType;                    \
        bool is_list = tb.__isset.list_values && !tb.list_values.empty();                      \
        bool is_range = tb.__isset.range_start || tb.__isset.range_end;                        \
        if (!is_list && !is_range) break;                                                      \
        ColumnValueRange<TYPE_##NAME> cvr(slot->col_name(), is_nullable, precision, scale);    \
        /* Returns nullopt if `node` is a NULL literal; the caller then sets contain_null  */  \
        /* on the CVR instead of trying to extract a typed value (which would dereference  */  \
        /* a null data pointer for the non-string branch).                                 */  \
        auto parse_texpr_node = [&](const TExprNode& node) -> std::optional<CppType> {         \
            if (node.node_type == TExprNodeType::NULL_LITERAL) {                               \
                return std::nullopt;                                                           \
            }                                                                                  \
            /* `Field` value is copied into the CVR by `add_fixed_value` /             */      \
            /* `add_range` (both take CppType by const-ref / by value), so the         */      \
            /* temporary `Field`'s lifetime ending at this expression's full-statement */      \
            /* boundary is safe -- including for `String` payloads.                    */      \
            Field field = slot_type->get_field(node);                                          \
            return std::make_optional<CppType>(field.get<TYPE_##NAME>());                      \
        };                                                                                     \
        if (is_list) {                                                                         \
            auto empty_cvr = ColumnValueRange<TYPE_##NAME>::create_empty_column_value_range(   \
                    is_nullable, precision, scale);                                            \
            bool list_has_null = false;                                                        \
            bool list_has_value = false;                                                       \
            for (const auto& node : tb.list_values) {                                          \
                auto parsed = parse_texpr_node(node);                                          \
                if (!parsed) {                                                                 \
                    list_has_null = true;                                                      \
                    continue;                                                                  \
                }                                                                              \
                static_cast<void>(empty_cvr.add_fixed_value(*parsed));                         \
                list_has_value = true;                                                         \
            }                                                                                  \
            if (list_has_value) {                                                              \
                cvr.intersection(empty_cvr);                                                   \
            }                                                                                  \
            if (list_has_null && is_nullable) {                                                \
                /* Track NULL membership on ParsedBoundary; calling          */                \
                /* cvr.set_contain_null(true) here would invoke              */                \
                /* set_empty_value_range() and discard the concrete fixed    */                \
                /* values we just inserted, turning {NULL, v} into a         */                \
                /* NULL-only boundary.                                       */                \
                boundary.contains_null = true;                                                 \
                if (!list_has_value) {                                                         \
                    boundary.only_null = true;                                                 \
                }                                                                              \
            }                                                                                  \
        } else {                                                                               \
            if (tb.__isset.range_start) {                                                      \
                auto parsed = parse_texpr_node(tb.range_start);                                \
                if (parsed) {                                                                  \
                    static_cast<void>(cvr.add_range(FILTER_LARGER_OR_EQUAL, *parsed));         \
                }                                                                              \
            }                                                                                  \
            if (tb.__isset.range_end) {                                                        \
                auto parsed = parse_texpr_node(tb.range_end);                                  \
                if (parsed) {                                                                  \
                    /* Multi-column RANGE projection emits a CLOSED upper bound (see       */  \
                    /* TPartitionBoundary.range_end_inclusive comment); single-column RANGE */ \
                    /* keeps the natural OPEN upper bound matching Doris semantics.         */ \
                    SQLFilterOp upper_op =                                                     \
                            (tb.__isset.range_end_inclusive && tb.range_end_inclusive)         \
                                    ? FILTER_LESS_OR_EQUAL                                     \
                                    : FILTER_LESS;                                             \
                    static_cast<void>(cvr.add_range(upper_op, *parsed));                       \
                }                                                                              \
            }                                                                                  \
        }                                                                                      \
        boundary.boundary_cvr = std::move(cvr);                                                \
        parsed_ok = true;                                                                      \
        break;                                                                                 \
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

        if (parsed_ok) {
            _slot_to_boundaries[slot_id].push_back(std::move(boundary));
        }
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
}
// NOLINTEND(readability-function-cognitive-complexity,readability-function-size)

static const VSlotRef* find_unique_slot_ref(const VExpr* expr) {
    if (!expr) {
        return nullptr;
    }
    if (expr->is_slot_ref()) {
        return assert_cast<const VSlotRef*>(expr);
    }
    const VSlotRef* found = nullptr;
    for (const auto& child : expr->children()) {
        const VSlotRef* c = find_unique_slot_ref(child.get());
        if (c) {
            if (found) {
                return nullptr; // multiple slot refs, can't handle
            }
            found = c;
        }
    }
    return found;
}

// NOLINTBEGIN(readability-function-cognitive-complexity,readability-function-size)
std::shared_ptr<const std::vector<ParsedBoundary>>
ParsedPartitionBoundaries::get_or_compute_projected_boundaries(
        int filter_id, const VExprSPtr& target_expr, SlotId leaf_slot_id, int leaf_column_id,
        TTargetExprMonotonicity::type global_direction,
        const std::unordered_map<int64_t, TTargetExprMonotonicity::type>* partition_directions,
        VExprContext* ctx) const {
    {
        std::lock_guard<std::mutex> lock(_projected_boundaries_mutex);
        auto it = _projected_boundaries_by_filter_id.find(filter_id);
        if (it != _projected_boundaries_by_filter_id.end()) {
            return it->second;
        }
    }

    std::vector<ParsedBoundary> projected;
    auto store_projected_boundaries = [&](std::vector<ParsedBoundary>&& boundaries) {
        auto cached = std::make_shared<const std::vector<ParsedBoundary>>(std::move(boundaries));
        std::lock_guard<std::mutex> lock(_projected_boundaries_mutex);
        auto [it, inserted] = _projected_boundaries_by_filter_id.emplace(filter_id, cached);
        return inserted ? cached : it->second;
    };

    auto slot_boundaries_it = _slot_to_boundaries.find(leaf_slot_id);
    if (slot_boundaries_it == _slot_to_boundaries.end()) {
        return store_projected_boundaries(std::move(projected));
    }

    const auto& orig_boundaries = slot_boundaries_it->second;
    if (orig_boundaries.empty()) {
        return store_projected_boundaries(std::move(projected));
    }

    std::vector<std::pair<size_t, TTargetExprMonotonicity::type>> selected_boundaries;
    selected_boundaries.reserve(orig_boundaries.size());
    if (partition_directions != nullptr) {
        for (size_t i = 0; i < orig_boundaries.size(); ++i) {
            auto it = partition_directions->find(orig_boundaries[i].partition_id);
            if (it != partition_directions->end() &&
                it->second != TTargetExprMonotonicity::NON_MONOTONIC) {
                selected_boundaries.emplace_back(i, it->second);
            }
        }
    } else if (global_direction != TTargetExprMonotonicity::NON_MONOTONIC) {
        for (size_t i = 0; i < orig_boundaries.size(); ++i) {
            selected_boundaries.emplace_back(i, global_direction);
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
    if (slot_type_it == _slot_data_types.end()) {
        return store_projected_boundaries(std::move(projected));
    }

    // LIST partitions: TODO(rf-partition-prune) -- projecting through a
    // monotonic function preserves set membership but requires re-grouping
    // the projected values back per-partition. Skip for now (caller treats
    // an empty result as "no projection available").
    for (const auto& selected_boundary : selected_boundaries) {
        bool is_list = false;
        std::visit([&](const auto& cvr) { is_list = cvr.is_fixed_value_range(); },
                   orig_boundaries[selected_boundary.first].boundary_cvr);
        if (is_list) {
            return store_projected_boundaries(std::move(projected));
        }
    }

    const DataTypePtr& input_type = slot_type_it->second;
    bool input_is_nullable = input_type->is_nullable();
    DataTypePtr inner_type = input_is_nullable ? remove_nullable(input_type) : input_type;
    PrimitiveType input_ptype = input_type->get_primitive_type();

    size_t N = selected_boundaries.size();
    std::vector<bool> lo_open(N, false);
    std::vector<bool> hi_open(N, false);
    std::vector<size_t> lo_result_row(N, 0);
    std::vector<size_t> hi_result_row(N, 0);
    size_t lo_row_count = 0;
    size_t hi_row_count = 0;

    // Build input blocks for lo and hi
    Block lo_block;
    Block hi_block;

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
    // columns. Open endpoints are not executed through target_expr; they stay
    // open after projection and swap sides for monotonic decreasing targets.
    bool input_built = false;
#define BUILD_INPUT_COLUMNS(INPUT_PT)                                                            \
    case TYPE_##INPUT_PT: {                                                                      \
        using InCol = typename PrimitiveTypeTraits<TYPE_##INPUT_PT>::ColumnType;                 \
        MutableColumnPtr lo_inner_base = inner_type->create_column();                            \
        MutableColumnPtr hi_inner_base = inner_type->create_column();                            \
        auto* lo_inner = assert_cast<InCol*>(lo_inner_base.get());                               \
        auto* hi_inner = assert_cast<InCol*>(hi_inner_base.get());                               \
        lo_inner->reserve(N);                                                                    \
        hi_inner->reserve(N);                                                                    \
        auto lo_nulls = ColumnUInt8::create();                                                   \
        auto hi_nulls = ColumnUInt8::create();                                                   \
        lo_nulls->reserve(N);                                                                    \
        hi_nulls->reserve(N);                                                                    \
        for (size_t i = 0; i < N; ++i) {                                                         \
            const auto& boundary = orig_boundaries[selected_boundaries[i].first];                \
            const auto* cvr =                                                                    \
                    std::get_if<ColumnValueRange<TYPE_##INPUT_PT>>(&boundary.boundary_cvr);      \
            DCHECK(cvr != nullptr);                                                              \
            DCHECK(!boundary.only_null);                                                         \
            DCHECK(!boundary.contains_null);                                                     \
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
        if (input_is_nullable) {                                                                 \
            auto lo_col = ColumnNullable::create(std::move(lo_inner_base), std::move(lo_nulls)); \
            auto hi_col = ColumnNullable::create(std::move(hi_inner_base), std::move(hi_nulls)); \
            lo_block.insert({std::move(lo_col), input_type, "leaf_slot"});                       \
            hi_block.insert({std::move(hi_col), input_type, "leaf_slot"});                       \
        } else {                                                                                 \
            lo_block.insert({std::move(lo_inner_base), input_type, "leaf_slot"});                \
            hi_block.insert({std::move(hi_inner_base), input_type, "leaf_slot"});                \
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

    if (!input_built) {
        return store_projected_boundaries(std::move(projected));
    }

    int lo_result_id = -1;
    int hi_result_id = -1;
    ColumnPtr lo_result_col;
    ColumnPtr hi_result_col;
    if (lo_row_count > 0) {
        auto status_lo = target_expr->execute(ctx, &lo_block, &lo_result_id);
        if (!status_lo.ok() || lo_result_id < 0) {
            return store_projected_boundaries(std::move(projected));
        }
        lo_result_col = lo_block.get_by_position(lo_result_id).column;
    }
    if (hi_row_count > 0) {
        auto status_hi = target_expr->execute(ctx, &hi_block, &hi_result_id);
        if (!status_hi.ok() || hi_result_id < 0) {
            return store_projected_boundaries(std::move(projected));
        }
        hi_result_col = hi_block.get_by_position(hi_result_id).column;
    }
    if (lo_row_count == 0 && hi_row_count == 0) {
        return store_projected_boundaries(std::move(projected));
    }

    int out_precision = cast_set<int>(target_expr->data_type()->get_precision());
    int out_scale = cast_set<int>(target_expr->data_type()->get_scale());
    bool out_nullable = target_expr->data_type()->is_nullable();
    PrimitiveType output_ptype = target_expr->data_type()->get_primitive_type();

    projected.reserve(N);

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
            if ((!lo_open[i] && lo_result_col->is_null_at(lo_result_row[i])) ||                 \
                (!hi_open[i] && hi_result_col->is_null_at(hi_result_row[i]))) {                 \
                continue;                                                                       \
            }                                                                                   \
            ColumnValueRange<TYPE_##OUTPUT_PT> cvr("", out_nullable, out_precision, out_scale); \
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
            projected_boundary.only_null = orig_boundary.only_null;                             \
            projected_boundary.contains_null = orig_boundary.contains_null;                     \
            projected_boundary.boundary_cvr = std::move(cvr);                                   \
            projected.emplace_back(std::move(projected_boundary));                              \
        }                                                                                       \
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

    return store_projected_boundaries(std::move(projected));
}
// NOLINTEND(readability-function-cognitive-complexity,readability-function-size)

static SQLFilterOp convert_opcode_to_filter_op(TExprOpcode::type op) {
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
        return FILTER_IN; // sentinel: caller should skip
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
    bool rf_contains_null = false;
    if (auto hybrid_set = impl->get_set_func()) {
        rf_contains_null = hybrid_set->contain_null();
    } else if (impl->node_type() == TExprNodeType::BLOOM_PRED) {
        auto bloom = impl->get_bloom_filter_func();
        rf_contains_null = bloom && bloom->contain_null();
    } else if (impl->node_type() == TExprNodeType::NULL_AWARE_BINARY_PRED) {
        // Min/Max RF built on a null-safe equal join. The literal child holds
        // the min or max bound; the NULL semantic is conveyed by the node
        // type itself (see create_vbin_predicate in runtime_filter/utils.cpp).
        rf_contains_null = true;
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
                    using CppType = typename CvrType::CppType;

                    auto hybrid_set = impl->get_set_func();
                    if (hybrid_set) {
                        // IN filter: build a fixed-value CVR from the HybridSet
                        auto rf_cvr = CvrType::create_empty_column_value_range(
                                pb.is_nullable, boundary_cvr.precision(), boundary_cvr.scale());
                        auto* iter = hybrid_set->begin();
                        while (iter->has_next()) {
                            const void* value = iter->get_value();
                            if (value) {
                                if constexpr (std::is_same_v<CppType, String>) {
                                    // String HybridSets store StringRef*, but
                                    // ColumnValueRange<String>::CppType is
                                    // std::string. Construct from the bytes.
                                    const auto* str_val = reinterpret_cast<const StringRef*>(value);
                                    static_cast<void>(rf_cvr.add_fixed_value(
                                            CppType(str_val->data, str_val->size)));
                                } else if constexpr (std::is_same_v<CppType, StringRef>) {
                                    const auto* str_val = reinterpret_cast<const StringRef*>(value);
                                    static_cast<void>(rf_cvr.add_fixed_value(
                                            CppType(str_val->data, str_val->size)));
                                } else {
                                    static_cast<void>(rf_cvr.add_fixed_value(
                                            *reinterpret_cast<const CppType*>(value)));
                                }
                            }
                            iter->next();
                        }
                        auto boundary_copy = boundary_cvr;
                        boundary_copy.intersection(rf_cvr);
                        if (boundary_copy.is_empty_value_range()) {
                            newly_pruned.insert(pb.partition_id);
                        }
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
                        } else if constexpr (std::is_same_v<CppType, StringRef>) {
                            val = CppType(data.data, data.size);
                        } else {
                            val = *reinterpret_cast<const CppType*>(data.data);
                        }

                        SQLFilterOp op = convert_opcode_to_filter_op(impl->op());
                        if (op == FILTER_IN) {
                            return; // unrecognized opcode, skip
                        }

                        CvrType rf_cvr(boundary_cvr.column_name(), pb.is_nullable,
                                       boundary_cvr.precision(), boundary_cvr.scale());
                        static_cast<void>(rf_cvr.add_range(op, val));

                        auto boundary_copy = boundary_cvr;
                        boundary_copy.intersection(rf_cvr);
                        if (boundary_copy.is_empty_value_range()) {
                            newly_pruned.insert(pb.partition_id);
                        }
                    }
                },
                pb.boundary_cvr);
    }
}

int64_t RuntimeFilterPartitionPruner::prune_by_runtime_filters(
        const ParsedPartitionBoundaries& parsed, const VExprContextSPtrs& conjuncts,
        const std::vector<TRuntimeFilterDesc>& rf_descs, int scan_node_id) {
    if (parsed.empty()) {
        return 0;
    }
    const auto& slot_to_boundaries = parsed.slot_to_boundaries();

    // Build filter_id -> monotonicity maps.
    std::unordered_map<int, TTargetExprMonotonicity::type> filter_id_to_monotonicity;
    std::unordered_map<int, std::unordered_map<int64_t, TTargetExprMonotonicity::type>>
            filter_id_to_partition_monotonicity;
    for (const auto& desc : rf_descs) {
        if (desc.__isset.planId_to_target_monotonicity) {
            auto it = desc.planId_to_target_monotonicity.find(scan_node_id);
            if (it != desc.planId_to_target_monotonicity.end()) {
                filter_id_to_monotonicity[desc.filter_id] = it->second;
            }
        }
        if (desc.__isset.planId_to_partition_target_monotonicity) {
            auto it = desc.planId_to_partition_target_monotonicity.find(scan_node_id);
            if (it != desc.planId_to_partition_target_monotonicity.end()) {
                auto& partition_monotonicity = filter_id_to_partition_monotonicity[desc.filter_id];
                for (const auto& partition_entry : it->second) {
                    if (!partition_entry.__isset.partition_id ||
                        !partition_entry.__isset.monotonicity ||
                        partition_entry.monotonicity == TTargetExprMonotonicity::NON_MONOTONIC) {
                        continue;
                    }
                    partition_monotonicity[partition_entry.partition_id] =
                            partition_entry.monotonicity;
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

        auto* wrapper_root = assert_cast<VRuntimeFilterWrapper*>(root.get());
        int filter_id = wrapper_root->filter_id();

        VExprSPtr target_subtree = impl->children()[0];

        // Identity case: target is a simple SlotRef on a partition column
        if (target_subtree->is_slot_ref()) {
            auto* slot_ref = assert_cast<VSlotRef*>(target_subtree.get());
            SlotId slot_id = slot_ref->slot_id();
            auto boundaries_it = slot_to_boundaries.find(slot_id);
            if (boundaries_it == slot_to_boundaries.end()) {
                continue;
            }

            _try_prune_by_single_rf(boundaries_it->second, impl, newly_pruned);
        } else {
            // Non-identity case: check if it's a monotonic target
            auto mono_it = filter_id_to_monotonicity.find(filter_id);
            auto partition_mono_it = filter_id_to_partition_monotonicity.find(filter_id);
            bool has_global_mono = mono_it != filter_id_to_monotonicity.end() &&
                                   mono_it->second != TTargetExprMonotonicity::NON_MONOTONIC;
            bool has_partition_mono =
                    partition_mono_it != filter_id_to_partition_monotonicity.end() &&
                    !partition_mono_it->second.empty();
            if (!has_global_mono && !has_partition_mono) {
                continue;
            }

            const VSlotRef* leaf_slot = find_unique_slot_ref(target_subtree.get());
            if (!leaf_slot) {
                continue;
            }

            SlotId leaf_slot_id = leaf_slot->slot_id();
            if (!slot_to_boundaries.contains(leaf_slot_id)) {
                continue;
            }

            int leaf_column_id = leaf_slot->column_id();
            TTargetExprMonotonicity::type direction =
                    has_global_mono ? mono_it->second : TTargetExprMonotonicity::NON_MONOTONIC;
            const auto* partition_directions =
                    has_partition_mono ? &partition_mono_it->second : nullptr;

            auto projected = parsed.get_or_compute_projected_boundaries(
                    filter_id, target_subtree, leaf_slot_id, leaf_column_id, direction,
                    partition_directions, conjunct_ctx.get());

            if (projected == nullptr || projected->empty()) {
                continue;
            }

            _try_prune_by_single_rf(*projected, impl, newly_pruned);
        }
    }

    auto count = static_cast<int64_t>(newly_pruned.size());
    if (count > 0) {
        std::unique_lock lock(_prune_mutex);
        for (int64_t pid : newly_pruned) {
            _pruned_partition_ids.insert(pid);
        }
    }
    return count;
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
