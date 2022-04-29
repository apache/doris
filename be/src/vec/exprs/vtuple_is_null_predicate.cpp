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

#include "vec/exprs/vtuple_is_null_predicate.h"

#include <string_view>

#include "exprs/create_predicate_function.h"

#include "vec/core/field.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

VTupleIsNullPredicate::VTupleIsNullPredicate(const TExprNode& node)
        : VExpr(node),
          _expr_name(function_name),
          _tuple_ids(node.tuple_is_null_pred.tuple_ids.begin(),
                     node.tuple_is_null_pred.tuple_ids.end()) {}

Status VTupleIsNullPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                      VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, desc, context));
    DCHECK_EQ(0, _children.size());
    DCHECK_GT(_tuple_ids.size(), 0);

    _column_to_check.reserve(_tuple_ids.size());
    // Resolve tuple ids to column id, one tuple only need check one column to speed up
    for (auto tuple_id : _tuple_ids) {
        uint32_t loc = 0;
        for (auto& tuple_desc : desc.tuple_descriptors()) {
            if (tuple_desc->id() == tuple_id) {
                _column_to_check.emplace_back(loc);
                break;
            }
            loc += tuple_desc->slots().size();
        }
    }

    return Status::OK();
}

Status VTupleIsNullPredicate::execute(VExprContext* context, Block* block, int* result_column_id) {
    size_t num_columns_without_result = block->columns();
    auto target_rows = block->rows();
    auto ans = ColumnVector<UInt8>::create(target_rows, 1);
    auto* __restrict ans_map = ans->get_data().data();

    for (auto col_id : _column_to_check) {
        auto* __restrict null_map =
                reinterpret_cast<const ColumnNullable&>(*block->get_by_position(col_id).column)
                        .get_null_map_column()
                        .get_data()
                        .data();

        for (int i = 0; i < target_rows; ++i) {
            ans_map[i] &= null_map[i] == JOIN_NULL_HINT;
        }
    }

    // prepare a column to save result
    block->insert({std::move(ans), _data_type, _expr_name});
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

const std::string& VTupleIsNullPredicate::expr_name() const {
    return _expr_name;
}

} // namespace doris::vectorized