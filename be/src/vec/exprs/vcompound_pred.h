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

#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
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

class VcompoundPred : public VExpr {
public:
    VcompoundPred(const TExprNode& node) : VExpr(node) {
        _op = node.opcode;
        _expr_name = "CompoundPredicate (" + compound_operator_to_string(_op) + ")";
    }

    VExpr* clone(ObjectPool* pool) const override { return pool->add(new VcompoundPred(*this)); }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute(VExprContext* context, doris::vectorized::Block* block,
                   int* result_column_id) override {
        doris::vectorized::ColumnNumbers arguments(_children.size());
        for (int i = 0; i < _children.size(); ++i) {
            int column_id = -1;
            RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
            arguments[i] = column_id;
        }

        size_t size = block->get_by_position(arguments[0]).column->size();
        uint8* data = _get_raw_data(block->get_by_position(arguments[0]).column);
        const uint8* data_rhs = nullptr;

        if (_children.size() == 2) {
            data_rhs = _get_raw_data(block->get_by_position(arguments[1]).column);

            if (const uint8* null_map = _get_null_map(block->get_by_position(arguments[1]).column);
                null_map != nullptr) {
                for (size_t i = 0; i < size; i++) {
                    data[i] &= !null_map[i];
                }
            }
        }

        if (_op == TExprOpcode::COMPOUND_AND) {
            for (size_t i = 0; i < size; i++) {
                data[i] &= data_rhs[i];
            }
        } else if (_op == TExprOpcode::COMPOUND_OR) {
            for (size_t i = 0; i < size; i++) {
                data[i] |= data_rhs[i];
            }
        } else {
            for (size_t i = 0; i < size; i++) {
                data[i] = !data[i];
            }
        }

        *result_column_id = arguments[0];
        return Status::OK();
    }

    std::string debug_string() const override {
        std::stringstream out;
        out << _expr_name << "{\n";
        out << _children[0]->debug_string();
        if (children().size() > 1) {
            out << ",\n" << _children[1]->debug_string();
        }
        out << "}";
        return out.str();
    }

private:
    uint8* _get_raw_data(ColumnPtr column) const {
        if (column->is_nullable()) {
            return assert_cast<ColumnUInt8*>(
                           assert_cast<ColumnNullable*>(column->assume_mutable().get())
                                   ->get_nested_column_ptr().get())
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
                                   ->get_null_map_column_ptr().get())
                    ->get_data()
                    .data();
        } else {
            return nullptr;
        }
    }

    TExprOpcode::type _op;

    std::string _expr_name;
};
} // namespace doris::vectorized
