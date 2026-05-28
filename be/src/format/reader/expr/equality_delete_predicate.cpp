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

#include "format/reader/expr/equality_delete_predicate.h"

#include <gen_cpp/Exprs_types.h>

#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"

namespace doris {
namespace {

bool column_value_equal(const ColumnPtr& lhs, size_t lhs_row, const ColumnPtr& rhs,
                        size_t rhs_row) {
    if (lhs->is_nullable() && rhs->is_nullable()) {
        return lhs->compare_at(lhs_row, rhs_row, *rhs, -1) == 0;
    }
    if (lhs->is_nullable()) {
        const auto& nullable_lhs = assert_cast<const ColumnNullable&>(*lhs);
        return !nullable_lhs.is_null_at(lhs_row) &&
               nullable_lhs.get_nested_column().compare_at(lhs_row, rhs_row, *rhs, -1) == 0;
    }
    if (rhs->is_nullable()) {
        const auto& nullable_rhs = assert_cast<const ColumnNullable&>(*rhs);
        return !nullable_rhs.is_null_at(rhs_row) &&
               lhs->compare_at(lhs_row, rhs_row, nullable_rhs.get_nested_column(), -1) == 0;
    }
    return lhs->compare_at(lhs_row, rhs_row, *rhs, -1) == 0;
}

} // namespace

EqualityDeletePredicate::EqualityDeletePredicate(Block delete_block, std::vector<int> field_ids)
        : VExpr(), _delete_block(std::move(delete_block)), _field_ids(std::move(field_ids)) {
    _node_type = TExprNodeType::PREDICATE;
    _opcode = TExprOpcode::DELETE;
    _data_type = std::make_shared<DataTypeBool>();
    _expr_name = "EqualityDeletePredicate";
    DCHECK_EQ(_delete_block.columns(), _field_ids.size());
    _delete_hashes = _build_hashes(_delete_block);
    for (size_t row = 0; row < _delete_hashes.size(); ++row) {
        _delete_hash_map.emplace(_delete_hashes[row], row);
    }
}

Status EqualityDeletePredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                        VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    _expr_name = "EqualityDeletePredicate";
    _prepare_finished = true;
    return Status::OK();
}

Status EqualityDeletePredicate::open(RuntimeState* state, VExprContext* context,
                                     FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& child : _children) {
        RETURN_IF_ERROR(child->open(state, context, scope));
    }
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void EqualityDeletePredicate::close(VExprContext* context,
                                    FunctionContext::FunctionStateScope scope) {
    VExpr::close(context, scope);
}

Status EqualityDeletePredicate::execute(VExprContext* context, Block* block,
                                        int* result_column_id) const {
    if (_children.size() != _field_ids.size()) {
        return Status::InternalError(
                "EqualityDeletePredicate should have {} child exprs, but got {}",
                _field_ids.size(), _children.size());
    }

    Block data_key_block;
    for (const auto& child : _children) {
        int slot = -1;
        RETURN_IF_ERROR(child->execute(context, block, &slot));
        const auto& key_column = block->get_by_position(slot);
        data_key_block.insert(
                {key_column.column, key_column.type, key_column.name});
    }

    const auto rows = data_key_block.rows();
    auto res_col = ColumnBool::create(rows, 0);
    if (_delete_hash_map.empty() || rows == 0) {
        block->insert({std::move(res_col), std::make_shared<DataTypeBool>(), expr_name()});
        *result_column_id = static_cast<int>(block->columns() - 1);
        return Status::OK();
    }

    auto data_hashes = _build_hashes(data_key_block);
    auto& result_data = res_col->get_data();
    for (size_t row = 0; row < rows; ++row) {
        const auto range = _delete_hash_map.equal_range(data_hashes[row]);
        for (auto it = range.first; it != range.second; ++it) {
            if (_equal(data_key_block, row, it->second)) {
                result_data[row] = true;
                break;
            }
        }
    }

    block->insert({std::move(res_col), std::make_shared<DataTypeBool>(), expr_name()});
    *result_column_id = static_cast<int>(block->columns() - 1);
    return Status::OK();
}

std::vector<uint64_t> EqualityDeletePredicate::_build_hashes(const Block& block) {
    std::vector<uint64_t> hashes(block.rows(), 0);
    for (const auto& column : block.get_columns()) {
        column->update_hashes_with_value(hashes.data(), nullptr);
    }
    return hashes;
}

bool EqualityDeletePredicate::_equal(const Block& data_block, size_t data_row,
                                     size_t delete_row) const {
    for (size_t column_idx = 0; column_idx < _delete_block.columns(); ++column_idx) {
        const auto& data_column = data_block.get_by_position(column_idx).column;
        const auto& delete_column = _delete_block.get_by_position(column_idx).column;
        if (!column_value_equal(data_column, data_row, delete_column, delete_row)) {
            return false;
        }
    }
    return true;
}

std::string EqualityDeletePredicate::debug_string() const {
    return _expr_name;
}

} // namespace doris
