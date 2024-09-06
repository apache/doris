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
#include "gutil/integral_types.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

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

    const std::string& expr_name() const override { return _expr_name; }

    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override {
        segment_v2::InvertedIndexResultBitmap res;
        bool all_pass = true;

        switch (_op) {
        case TExprOpcode::COMPOUND_OR: {
            for (const auto& child : _children) {
                if (Status st = child->evaluate_inverted_index(context, segment_num_rows);
                    !st.ok()) {
                    LOG(ERROR) << "expr:" << child->expr_name()
                               << " evaluate_inverted_index error:" << st.to_string();
                    all_pass = false;
                    continue;
                }
                if (context->get_inverted_index_context()->has_inverted_index_result_for_expr(
                            child.get())) {
                    const auto* index_result =
                            context->get_inverted_index_context()
                                    ->get_inverted_index_result_for_expr(child.get());
                    if (res.is_empty()) {
                        res = *index_result;
                    } else {
                        res |= *index_result;
                    }
                    if (res.get_data_bitmap()->cardinality() == segment_num_rows) {
                        break; // Early exit if result is full
                    }
                } else {
                    all_pass = false;
                }
            }
            break;
        }
        case TExprOpcode::COMPOUND_AND: {
            for (const auto& child : _children) {
                if (Status st = child->evaluate_inverted_index(context, segment_num_rows);
                    !st.ok()) {
                    LOG(ERROR) << "expr:" << child->expr_name()
                               << " evaluate_inverted_index error:" << st.to_string();
                    all_pass = false;
                    continue;
                }
                if (context->get_inverted_index_context()->has_inverted_index_result_for_expr(
                            child.get())) {
                    const auto* index_result =
                            context->get_inverted_index_context()
                                    ->get_inverted_index_result_for_expr(child.get());
                    if (res.is_empty()) {
                        res = *index_result;
                    } else {
                        res &= *index_result;
                    }

                    if (res.get_data_bitmap()->isEmpty()) {
                        break; // Early exit if result is empty
                    }
                } else {
                    all_pass = false;
                }
            }
            break;
        }
        case TExprOpcode::COMPOUND_NOT: {
            const auto& child = _children[0];
            Status st = child->evaluate_inverted_index(context, segment_num_rows);
            if (!st.ok()) {
                LOG(ERROR) << "expr:" << child->expr_name()
                           << " evaluate_inverted_index error:" << st.to_string();
                return st;
            }

            if (context->get_inverted_index_context()->has_inverted_index_result_for_expr(
                        child.get())) {
                const auto* index_result =
                        context->get_inverted_index_context()->get_inverted_index_result_for_expr(
                                child.get());
                roaring::Roaring full_result;
                full_result.addRange(0, segment_num_rows);
                res = index_result->op_not(&full_result);
            } else {
                all_pass = false;
            }
            break;
        }
        default:
            return Status::NotSupported(
                    "Compound operator must be AND, OR, or NOT to execute with inverted index.");
        }

        if (all_pass && !res.is_empty()) {
            // set fast_execute when expr evaluated by inverted index correctly
            _can_fast_execute = true;
            context->get_inverted_index_context()->set_inverted_index_result_for_expr(this, res);
        }
        return Status::OK();
    }

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        if (_can_fast_execute && fast_execute(context, block, result_column_id)) {
            return Status::OK();
        }
        if (children().size() == 1 || !_all_child_is_compound_and_not_const()) {
            return VectorizedFnCall::execute(context, block, result_column_id);
        }

        int lhs_id = -1;
        int rhs_id = -1;
        RETURN_IF_ERROR(_children[0]->execute(context, block, &lhs_id));
        ColumnPtr lhs_column =
                block->get_by_position(lhs_id).column->convert_to_full_column_if_const();
        size_t size = lhs_column->size();
        bool lhs_is_nullable = lhs_column->is_nullable();
        auto [lhs_data_column, lhs_null_map] =
                _get_raw_data_and_null_map(lhs_column, lhs_is_nullable);
        int filted = simd::count_zero_num((int8_t*)lhs_data_column, size);
        bool lhs_all_true = (filted == 0);
        bool lhs_all_false = (filted == size);

        bool lhs_all_is_not_null = false;
        if (lhs_is_nullable) {
            filted = simd::count_zero_num((int8_t*)lhs_null_map, size);
            lhs_all_is_not_null = (filted == size);
        }

        ColumnPtr rhs_column = nullptr;
        uint8* __restrict rhs_data_column = nullptr;
        uint8* __restrict rhs_null_map = nullptr;
        bool rhs_is_nullable = false;
        bool rhs_all_true = false;
        bool rhs_all_false = false;
        bool rhs_all_is_not_null = false;
        bool result_is_nullable = _data_type->is_nullable();

        auto get_rhs_colum = [&]() {
            if (rhs_id == -1) {
                RETURN_IF_ERROR(_children[1]->execute(context, block, &rhs_id));
                rhs_column =
                        block->get_by_position(rhs_id).column->convert_to_full_column_if_const();
                rhs_is_nullable = rhs_column->is_nullable();
                auto rhs_nullable_column = _get_raw_data_and_null_map(rhs_column, rhs_is_nullable);
                rhs_data_column = rhs_nullable_column.first;
                rhs_null_map = rhs_nullable_column.second;
                int filted = simd::count_zero_num((int8_t*)rhs_data_column, size);
                rhs_all_true = (filted == 0);
                rhs_all_false = (filted == size);
                if (rhs_is_nullable) {
                    filted = simd::count_zero_num((int8_t*)rhs_null_map, size);
                    rhs_all_is_not_null = (filted == size);
                }
            }
            return Status::OK();
        };

        auto return_result_column_id = [&](ColumnPtr res_column, int res_id) -> int {
            if (result_is_nullable && !res_column->is_nullable()) {
                auto result_column =
                        ColumnNullable::create(res_column, ColumnUInt8::create(size, 0));
                res_id = block->columns();
                block->insert({std::move(result_column), _data_type, _expr_name});
            }
            return res_id;
        };

        auto create_null_map_column = [&](ColumnPtr& null_map_column,
                                          uint8* __restrict null_map_data) {
            if (null_map_data == nullptr) {
                null_map_column = ColumnUInt8::create(size, 0);
                null_map_data = assert_cast<ColumnUInt8*>(null_map_column->assume_mutable().get())
                                        ->get_data()
                                        .data();
            }
            return null_map_data;
        };

        auto vector_vector_null = [&]<bool is_and_op>() {
            auto col_res = ColumnUInt8::create(size);
            auto col_nulls = ColumnUInt8::create(size);
            auto* __restrict res_datas = assert_cast<ColumnUInt8*>(col_res)->get_data().data();
            auto* __restrict res_nulls = assert_cast<ColumnUInt8*>(col_nulls)->get_data().data();
            ColumnPtr temp_null_map = nullptr;
            // maybe both children are nullable / or one of children is nullable
            lhs_null_map = create_null_map_column(temp_null_map, lhs_null_map);
            rhs_null_map = create_null_map_column(temp_null_map, rhs_null_map);

            if constexpr (is_and_op) {
                for (size_t i = 0; i < size; ++i) {
                    res_nulls[i] = apply_and_null(lhs_data_column[i], lhs_null_map[i],
                                                  rhs_data_column[i], rhs_null_map[i]);
                    res_datas[i] = lhs_data_column[i] & rhs_data_column[i];
                }
            } else {
                for (size_t i = 0; i < size; ++i) {
                    res_nulls[i] = apply_or_null(lhs_data_column[i], lhs_null_map[i],
                                                 rhs_data_column[i], rhs_null_map[i]);
                    res_datas[i] = lhs_data_column[i] | rhs_data_column[i];
                }
            }
            auto result_column = ColumnNullable::create(std::move(col_res), std::move(col_nulls));
            *result_column_id = block->columns();
            block->insert({std::move(result_column), _data_type, _expr_name});
        };

        // false and NULL ----> 0
        // true  and NULL ----> NULL
        if (_op == TExprOpcode::COMPOUND_AND) {
            //1. not null column: all data is false
            //2. nullable column: null map all is not null
            if ((lhs_all_false && !lhs_is_nullable) || (lhs_all_false && lhs_all_is_not_null)) {
                // false and any = false, return lhs
                *result_column_id = return_result_column_id(lhs_column, lhs_id);
            } else {
                RETURN_IF_ERROR(get_rhs_colum());

                if ((lhs_all_true && !lhs_is_nullable) ||    //not null column
                    (lhs_all_true && lhs_all_is_not_null)) { //nullable column
                    // true and any = any, return rhs
                    *result_column_id = return_result_column_id(rhs_column, rhs_id);
                } else if ((rhs_all_false && !rhs_is_nullable) ||
                           (rhs_all_false && rhs_all_is_not_null)) {
                    // any and false = false, return rhs
                    *result_column_id = return_result_column_id(rhs_column, rhs_id);
                } else if ((rhs_all_true && !rhs_is_nullable) ||
                           (rhs_all_true && rhs_all_is_not_null)) {
                    // any and true = any, return lhs
                    *result_column_id = return_result_column_id(lhs_column, lhs_id);
                } else {
                    if (!result_is_nullable) {
                        *result_column_id = lhs_id;
                        for (size_t i = 0; i < size; i++) {
                            lhs_data_column[i] &= rhs_data_column[i];
                        }
                    } else {
                        vector_vector_null.template operator()<true>();
                    }
                }
            }
        } else if (_op == TExprOpcode::COMPOUND_OR) {
            // true  or NULL ----> 1
            // false or NULL ----> NULL
            if ((lhs_all_true && !lhs_is_nullable) || (lhs_all_true && lhs_all_is_not_null)) {
                // true or any = true, return lhs
                *result_column_id = return_result_column_id(lhs_column, lhs_id);
            } else {
                RETURN_IF_ERROR(get_rhs_colum());
                if ((lhs_all_false && !lhs_is_nullable) || (lhs_all_false && lhs_all_is_not_null)) {
                    // false or any = any, return rhs
                    *result_column_id = return_result_column_id(rhs_column, rhs_id);
                } else if ((rhs_all_true && !rhs_is_nullable) ||
                           (rhs_all_true && rhs_all_is_not_null)) {
                    // any or true = true, return rhs
                    *result_column_id = return_result_column_id(rhs_column, rhs_id);
                } else if ((rhs_all_false && !rhs_is_nullable) ||
                           (rhs_all_false && rhs_all_is_not_null)) {
                    // any or false = any, return lhs
                    *result_column_id = return_result_column_id(lhs_column, lhs_id);
                } else {
                    if (!result_is_nullable) {
                        *result_column_id = lhs_id;
                        for (size_t i = 0; i < size; i++) {
                            lhs_data_column[i] |= rhs_data_column[i];
                        }
                    } else {
                        vector_vector_null.template operator()<false>();
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
    static inline constexpr uint8 apply_and_null(UInt8 a, UInt8 l_null, UInt8 b, UInt8 r_null) {
        // (<> && false) is false, (true && NULL) is NULL
        return (l_null & r_null) | (r_null & (l_null ^ a)) | (l_null & (r_null ^ b));
    }
    static inline constexpr uint8 apply_or_null(UInt8 a, UInt8 l_null, UInt8 b, UInt8 r_null) {
        // (<> || true) is true, (false || NULL) is NULL
        return (l_null & r_null) | (r_null & (r_null ^ a)) | (l_null & (l_null ^ b));
    }

    bool _all_child_is_compound_and_not_const() const {
        for (auto child : _children) {
            // we can make sure non const compound predicate's return column is allow modifyied locally.
            if (child->is_constant() || !child->is_compound_predicate()) {
                return false;
            }
        }
        return true;
    }

    std::pair<uint8*, uint8*> _get_raw_data_and_null_map(ColumnPtr column,
                                                         bool has_nullable_column) const {
        if (has_nullable_column) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column->assume_mutable().get());
            auto* data_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_nested_column_ptr().get())
                            ->get_data()
                            .data();
            auto* null_map =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get())
                            ->get_data()
                            .data();
            return std::make_pair(data_column, null_map);
        } else {
            auto* data_column =
                    assert_cast<ColumnUInt8*>(column->assume_mutable().get())->get_data().data();
            return std::make_pair(data_column, nullptr);
        }
    }

    TExprOpcode::type _op;
};
} // namespace doris::vectorized
