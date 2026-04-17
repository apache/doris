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

#include "exprs/hybrid_set.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"

namespace doris {

// Helper to extract a typed value from a ColumnPtr's first row.
// Must be a template function so that `if constexpr` properly discards
// the inapplicable branch during instantiation.
template <PrimitiveType PT>
static typename PrimitiveTypeTraits<PT>::CppType extract_value_from_column(const ColumnPtr& col) {
    using CppType = typename PrimitiveTypeTraits<PT>::CppType;
    auto data = col->get_data_at(0);
    if constexpr (is_string_type(PT)) {
        return CppType(data.data, data.size);
    } else {
        return *reinterpret_cast<const CppType*>(data.data);
    }
}

void RuntimeFilterPartitionPruner::parse_boundaries(
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
        PrimitiveType ptype = slot->type()->get_primitive_type();
        int precision = cast_set<int>(slot->type()->get_precision());
        int scale = cast_set<int>(slot->type()->get_scale());
        bool is_nullable = slot->is_nullable();

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
        auto parse_texpr_node = [&](const TExprNode& node) -> std::pair<CppType, ColumnPtr> { \
            VLiteral literal(node);                                                            \
            auto col_ptr = literal.get_column_ptr();                                           \
            auto val = extract_value_from_column<TYPE_##NAME>(col_ptr);                        \
            return {val, col_ptr};                                                             \
        };                                                                                     \
        if (is_list) {                                                                         \
            auto empty_cvr = ColumnValueRange<TYPE_##NAME>::create_empty_column_value_range(    \
                    is_nullable, precision, scale);                                             \
            for (const auto& node : tb.list_values) {                                          \
                auto [val, col_ptr] = parse_texpr_node(node);                                  \
                boundary.literal_columns.push_back(std::move(col_ptr));                        \
                static_cast<void>(empty_cvr.add_fixed_value(val));                             \
            }                                                                                  \
            cvr.intersection(empty_cvr);                                                       \
        } else {                                                                               \
            if (tb.__isset.range_start) {                                                      \
                auto [val, col_ptr] = parse_texpr_node(tb.range_start);                        \
                boundary.literal_columns.push_back(std::move(col_ptr));                        \
                static_cast<void>(cvr.add_range(FILTER_LARGER_OR_EQUAL, val));                 \
            }                                                                                  \
            if (tb.__isset.range_end) {                                                        \
                auto [val, col_ptr] = parse_texpr_node(tb.range_end);                          \
                boundary.literal_columns.push_back(std::move(col_ptr));                        \
                static_cast<void>(cvr.add_range(FILTER_LESS, val));                            \
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
            _partition_column_slot_ids.insert(slot_id);
            _slot_to_boundaries[slot_id].push_back(std::move(boundary));
        }
    }

    // Count distinct partition IDs across all boundaries.
    if (!_partition_column_slot_ids.empty()) {
        phmap::flat_hash_set<int64_t> all_partition_ids;
        for (const auto& [_, slot_boundaries] : _slot_to_boundaries) {
            for (const auto& pb : slot_boundaries) {
                all_partition_ids.insert(pb.partition_id);
            }
        }
        _total_partition_count = static_cast<int64_t>(all_partition_ids.size());
    }
}

int64_t RuntimeFilterPartitionPruner::prune_by_runtime_filters(
        const VExprContextSPtrs& conjuncts) {
    if (_partition_column_slot_ids.empty()) {
        return 0;
    }

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

        // Determine the target slot_id from the RF's probe expression.
        // For IN filter: impl is VDirectInPredicate, children()[0] is VSlotRef
        // For MinMax filter: impl is VectorizedFnCall (binary pred), children()[0] is VSlotRef
        if (impl->children().empty() || !impl->children()[0]->is_slot_ref()) {
            continue;
        }
        auto* slot_ref = assert_cast<VSlotRef*>(impl->children()[0].get());
        SlotId slot_id = slot_ref->slot_id();
        if (!_partition_column_slot_ids.contains(slot_id)) {
            continue;
        }

        auto boundaries_it = _slot_to_boundaries.find(slot_id);
        if (boundaries_it == _slot_to_boundaries.end()) {
            continue;
        }

        for (const auto& pb : boundaries_it->second) {
            if (_pruned_partition_ids.contains(pb.partition_id) ||
                newly_pruned.contains(pb.partition_id)) {
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
                                    pb.is_nullable, boundary_cvr.precision(),
                                    boundary_cvr.scale());
                            auto* iter = hybrid_set->begin();
                            while (iter->has_next()) {
                                const void* value = iter->get_value();
                                if (value) {
                                    if constexpr (std::is_same_v<CppType, StringRef>) {
                                        auto* str_val =
                                                reinterpret_cast<const StringRef*>(value);
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
                        } else if (impl->node_type() == TExprNodeType::BINARY_PRED &&
                                   impl->children().size() == 2 &&
                                   impl->children()[1]->is_literal()) {
                            // MinMax filter: binary pred with literal bound
                            auto* literal =
                                    assert_cast<VLiteral*>(impl->children()[1].get());
                            auto col_ptr = literal->get_column_ptr();
                            auto data = col_ptr->get_data_at(0);
                            CppType val {};
                            if constexpr (std::is_same_v<CppType, StringRef>) {
                                val = CppType(data.data, data.size);
                            } else {
                                val = *reinterpret_cast<const CppType*>(data.data);
                            }

                            CvrType rf_cvr(boundary_cvr.column_name(), pb.is_nullable,
                                           boundary_cvr.precision(), boundary_cvr.scale());
                            SQLFilterOp op = FILTER_LARGER_OR_EQUAL;
                            if (impl->op() == TExprOpcode::LE) {
                                op = FILTER_LESS_OR_EQUAL;
                            } else if (impl->op() == TExprOpcode::LT) {
                                op = FILTER_LESS;
                            } else if (impl->op() == TExprOpcode::GE) {
                                op = FILTER_LARGER_OR_EQUAL;
                            } else if (impl->op() == TExprOpcode::GT) {
                                op = FILTER_LARGER;
                            } else {
                                return;
                            }
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

    int64_t count = static_cast<int64_t>(newly_pruned.size());
    if (count > 0) {
        std::unique_lock lock(_prune_mutex);
        for (int64_t pid : newly_pruned) {
            _pruned_partition_ids.insert(pid);
        }
    }
    return count;
}

bool RuntimeFilterPartitionPruner::is_partition_pruned(int64_t partition_id) const {
    std::shared_lock lock(_prune_mutex);
    return _pruned_partition_ids.contains(partition_id);
}

int64_t RuntimeFilterPartitionPruner::pruned_partition_count() const {
    std::shared_lock lock(_prune_mutex);
    return static_cast<int64_t>(_pruned_partition_ids.size());
}

} // namespace doris
