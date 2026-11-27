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

#include "exec/operator/groupjoin_operator_utils.h"

#include <gen_cpp/Opcodes_types.h>

#include "core/column/column_nullable.h"
#include "exec/common/util.hpp"
#include "exec/pipeline/dependency.h"
#include "exprs/vexpr.h"

namespace doris::groupjoin {
namespace {
bool needs_float_key_normalization(const DataTypePtr& type) {
    const auto primitive_type = type->get_primitive_type();
    // Complex columns may contain floating-point values in nested elements. Keep the existing
    // recursive normalization for them, while avoiding a copy for scalar key types that cannot.
    return is_float_or_double(primitive_type) || is_complex_type(primitive_type);
}

} // namespace

Status validate_group_join_node(const TPlanNode& tnode) {
    if (!tnode.__isset.group_join_node) {
        return Status::InternalError("GroupJoin node is not set");
    }

    const auto& group_join_node = tnode.group_join_node;
    if (group_join_node.join_op != TJoinOp::INNER_JOIN) {
        return Status::InternalError("GroupJoin only supports inner join now");
    }
    if (group_join_node.__isset.dist_type &&
        group_join_node.dist_type == TJoinDistributionType::BROADCAST) {
        return Status::InternalError("GroupJoin does not support broadcast join now");
    }
    if (group_join_node.agg_output_mode != TGroupJoinAggOutputMode::FINAL_RESULT) {
        return Status::InternalError(
                "GroupJoin only supports final-result aggregate output mode now: {}",
                group_join_node.agg_output_mode);
    }
    if (group_join_node.aggregate_functions.empty()) {
        return Status::InternalError("GroupJoin requires at least one aggregate function");
    }
    for (const auto& aggregate : group_join_node.aggregate_functions) {
        switch (aggregate.input_side) {
        case TGroupJoinAggSide::BUILD:
        case TGroupJoinAggSide::PROBE:
        case TGroupJoinAggSide::JOINED:
            break;
        default:
            return Status::InternalError("Unsupported GroupJoin aggregate input side: {}",
                                         aggregate.input_side);
        }
    }
    if (group_join_node.eq_join_conjuncts.empty() ||
        group_join_node.eq_join_conjuncts.size() != group_join_node.grouping_exprs.size()) {
        return Status::InternalError("GroupJoin requires one group key per join key");
    }
    for (const auto& eq_join_conjunct : group_join_node.eq_join_conjuncts) {
        if (eq_join_conjunct.__isset.opcode &&
            eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) {
            return Status::InternalError("GroupJoin does not support null-safe equal join now");
        }
    }
    return Status::OK();
}

Status do_evaluate(Block& block, VExprContextSPtrs& exprs,
                   std::vector<ColumnPtr>& key_columns_holder) {
    key_columns_holder.resize(exprs.size());
    for (size_t i = 0; i < exprs.size(); ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(exprs[i]->execute(&block, &result_column_id));
        auto& result_column = block.get_by_position(result_column_id).column;
        result_column = result_column->convert_to_full_column_if_const();
        if (needs_float_key_normalization(exprs[i]->root()->data_type())) {
            auto mutable_col = IColumn::mutate(std::move(result_column));
            mutable_col->replace_float_special_values();
            result_column = std::move(mutable_col);
        }
        key_columns_holder[i] = result_column;
    }
    return Status::OK();
}

Status extract_key_columns(size_t rows, const std::vector<ColumnPtr>& key_columns_holder,
                           ColumnRawPtrs& key_not_nullable_columns,
                           ColumnUInt8::MutablePtr& null_map_column) {
    key_not_nullable_columns.resize(key_columns_holder.size());
    null_map_column.reset();

    for (size_t i = 0; i < key_columns_holder.size(); ++i) {
        const auto* column = key_columns_holder[i].get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*column); nullable) {
            const auto& col_nested = nullable->get_nested_column();
            const auto& col_nullmap = nullable->get_null_map_data();
            if (!null_map_column) {
                null_map_column = ColumnUInt8::create();
                null_map_column->get_data().assign(rows, uint8_t {0});
            }
            VectorizedUtils::update_null_map(null_map_column->get_data(), col_nullmap);
            key_not_nullable_columns[i] = &col_nested;
        } else {
            key_not_nullable_columns[i] = column;
        }
    }
    return Status::OK();
}

Status evaluate_column(Block& block, const VExprContextSPtr& expr, ColumnPtr& result) {
    int column_id = -1;
    RETURN_IF_ERROR(expr->execute(&block, &column_id));
    result = block.get_by_position(column_id).column->convert_to_full_column_if_const();
    block.get_by_position(column_id).column = result;
    return Status::OK();
}

DataDistribution distribution(TJoinDistributionType::type type,
                              const std::vector<TExpr>& expressions) {
    return type == TJoinDistributionType::BUCKET_SHUFFLE || type == TJoinDistributionType::COLOCATE
                   ? DataDistribution(TLocalPartitionType::BUCKET_HASH_SHUFFLE, expressions)
                   : DataDistribution(TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE,
                                      expressions);
}

} // namespace doris::groupjoin
