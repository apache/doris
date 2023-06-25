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
#include <gen_cpp/Opcodes_types.h>

#include "common/status.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

inline std::string compound_operator_to_string(TExprOpcode::type op) {
    if (op == TExprOpcode::COMPOUND_AND) {
        return "and";
    } else if (op == TExprOpcode::COMPOUND_OR) {
        return "or";
    } else {
        return "not";
    }
}

class VCompoundPred : public VectorizedFnCall {
    ENABLE_FACTORY_CREATOR(VCompoundPred);

public:
    VCompoundPred(const TExprNode& node) : VectorizedFnCall(node) {
        _op = node.opcode;
        _fn.name.function_name = compound_operator_to_string(_op);
        _expr_name = fmt::format("VCompoundPredicate[{}](arguments={},return={})",
                                 _fn.name.function_name, get_child_names(), _data_type->get_name());
    }

    VExprSPtr clone() const override { return VCompoundPred::create_shared(*this); }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute(VExprContext* context, vectorized::Block* block,
                   int* result_column_id) override {
        if (children().size() == 1 || !_all_child_is_compound_and_not_const() ||
            _children[0]->is_nullable() || _children[1]->is_nullable()) {
            // TODO:
            // When the child is nullable, make the optimization also take effect, and the processing of this piece may be more complicated
            // https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html
            return VectorizedFnCall::execute(context, block, result_column_id);
        }

        int lhs_id = -1;
        int rhs_id = -1;
        RETURN_IF_ERROR(_children[0]->execute(context, block, &lhs_id));
        ColumnPtr lhs_column =
                block->get_by_position(lhs_id).column->convert_to_full_column_if_const();

        size_t size = lhs_column->size();
        uint8* __restrict data = _get_raw_data(lhs_column);
        int filted = simd::count_zero_num((int8_t*)data, size);
        bool full = filted == 0;
        bool empty = filted == size;

        ColumnPtr rhs_column = nullptr;
        uint8* __restrict data_rhs = nullptr;
        bool full_rhs = false;
        bool empty_rhs = false;

        auto get_rhs_colum = [&]() {
            if (rhs_id == -1) {
                RETURN_IF_ERROR(_children[1]->execute(context, block, &rhs_id));
                rhs_column =
                        block->get_by_position(rhs_id).column->convert_to_full_column_if_const();
                data_rhs = _get_raw_data(rhs_column);
                int filted = simd::count_zero_num((int8_t*)data_rhs, size);
                full_rhs = filted == 0;
                empty_rhs = filted == size;
            }
            return Status::OK();
        };

        if (_op == TExprOpcode::COMPOUND_AND) {
            if (empty) {
                // empty and any = empty, return lhs
                *result_column_id = lhs_id;
            } else {
                RETURN_IF_ERROR(get_rhs_colum());

                if (full) {
                    // full and any = any, return rhs
                    *result_column_id = rhs_id;
                } else if (empty_rhs) {
                    // any and empty = empty, return rhs
                    *result_column_id = rhs_id;
                } else if (full_rhs) {
                    // any and full = any, return lhs
                    *result_column_id = lhs_id;
                } else {
                    *result_column_id = lhs_id;
                    for (size_t i = 0; i < size; i++) {
                        data[i] &= data_rhs[i];
                    }
                }
            }
        } else if (_op == TExprOpcode::COMPOUND_OR) {
            if (full) {
                // full or any = full, return lhs
                *result_column_id = lhs_id;
            } else {
                RETURN_IF_ERROR(get_rhs_colum());
                if (empty) {
                    // empty or any = any, return rhs
                    *result_column_id = rhs_id;
                } else if (full_rhs) {
                    // any or full = full, return rhs
                    *result_column_id = rhs_id;
                } else if (empty_rhs) {
                    // any or empty = any, return lhs
                    *result_column_id = lhs_id;
                } else {
                    *result_column_id = lhs_id;
                    for (size_t i = 0; i < size; i++) {
                        data[i] |= data_rhs[i];
                    }
                }
            }
        } else {
            return Status::InternalError("Compound operator must be AND or OR.");
        }

        return Status::OK();
    }

    bool is_compound_predicate() const override { return true; }

private:
    bool _all_child_is_compound_and_not_const() const {
        for (auto child : _children) {
            // we can make sure non const compound predicate's return column is allow modifyied locally.
            if (child->is_constant() || !child->is_compound_predicate()) {
                return false;
            }
        }
        return true;
    }

    uint8* _get_raw_data(ColumnPtr column) const {
        if (column->is_nullable()) {
            return assert_cast<ColumnUInt8*>(
                           assert_cast<ColumnNullable*>(column->assume_mutable().get())
                                   ->get_nested_column_ptr()
                                   .get())
                    ->get_data()
                    .data();
        } else {
            return assert_cast<ColumnUInt8*>(column->assume_mutable().get())->get_data().data();
        }
    }

    uint8* _get_null_map(ColumnPtr column) const {
        if (column->is_nullable()) {
            return assert_cast<ColumnUInt8*>(
                           assert_cast<ColumnNullable*>(column->assume_mutable().get())
                                   ->get_null_map_column_ptr()
                                   .get())
                    ->get_data()
                    .data();
        } else {
            return nullptr;
        }
    }

    TExprOpcode::type _op;
};
} // namespace doris::vectorized
