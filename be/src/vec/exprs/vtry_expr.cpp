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

#include "vtry_expr.h"

#include <glog/logging.h>

#include "common/config.h"
#include "common/logging.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"
namespace doris::vectorized {

TryExpr::TryExpr(const TExprNode& node) : VExpr(node) {}

void insert_one_cell_to_column(MutableColumnPtr& column, const IColumn& value) {
    DCHECK(column->is_nullable());
    auto& dst = assert_cast<ColumnNullable&>(*column);
    if (const auto* const_column = check_and_get_column<ColumnConst>(column.get())) {
        dst.insert_from_maybe_not_nullable(const_column->get_data_column(), 0);
    } else {
        dst.insert_from_maybe_not_nullable(value, 0);
    }
}

Status TryExpr::execute(VExprContext* context, Block* block, int* result_column_id) {
    DCHECK(_data_type->is_nullable());
    DCHECK_EQ(_children.size(), 1);
    auto nested_expr = _children[0];
    auto batch_exec_status = nested_expr->execute(context, block, result_column_id);
    if (batch_exec_status.ok()) {
        auto nested_expr_result_data = block->get_by_position(*result_column_id);
        nested_expr_result_data.column = make_nullable(nested_expr_result_data.column);
        nested_expr_result_data.type = make_nullable(nested_expr_result_data.type);
        block->insert(nested_expr_result_data);
        *result_column_id = block->columns() - 1;
        return batch_exec_status;
    }

    auto const size = block->rows();

    auto result_column = _data_type->create_column();

    for (size_t i = 0; i < size; ++i) {
        MutableBlock tmp_mut_block(block->clone_empty());
        {
            auto& mut_cols = tmp_mut_block.mutable_columns();
            const auto& block_data = block->get_columns_with_type_and_name();
            for (size_t j = 0; j < block->columns(); ++j) {
                if (block_data[j].column) {
                    mut_cols[j]->insert_from(*block_data[j].column, i);
                }
            }
        }
        int tmp_result_column_id = -1;
        auto tmp_block = tmp_mut_block.to_block();

        auto status = nested_expr->execute(context, &tmp_block, &tmp_result_column_id);

        if (status.ok()) {
            auto& value = tmp_block.get_by_position(tmp_result_column_id).column;
            insert_one_cell_to_column(result_column, *value);
        } else {
            result_column->insert_default();
        }
    }

    block->insert({std::move(result_column), _data_type, expr_name()});
    *result_column_id = block->columns() - 1;
    return Status::OK();
}

} // namespace doris::vectorized
