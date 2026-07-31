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

#include "format_v2/expr/equality_delete_predicate.h"

#include <gen_cpp/Exprs_types.h>

#include <algorithm>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "util/hash_util.hpp"

namespace doris::format {
namespace {

bool column_value_equal(const ColumnPtr& lhs, size_t lhs_row, const ColumnPtr& rhs,
                        size_t rhs_row) {
    const IColumn* lhs_data = lhs.get();
    const IColumn* rhs_data = rhs.get();
    if (lhs->is_nullable()) {
        const auto& nullable_lhs = assert_cast<const ColumnNullable&>(*lhs);
        if (nullable_lhs.is_null_at(lhs_row)) {
            return rhs->is_nullable() &&
                   assert_cast<const ColumnNullable&>(*rhs).is_null_at(rhs_row);
        }
        lhs_data = &nullable_lhs.get_nested_column();
    }
    if (rhs->is_nullable()) {
        const auto& nullable_rhs = assert_cast<const ColumnNullable&>(*rhs);
        if (nullable_rhs.is_null_at(rhs_row)) {
            return false;
        }
        rhs_data = &nullable_rhs.get_nested_column();
    }
    const bool lhs_binary = check_and_get_column<ColumnString>(*lhs_data) != nullptr ||
                            check_and_get_column<ColumnVarbinary>(*lhs_data) != nullptr;
    const bool rhs_binary = check_and_get_column<ColumnString>(*rhs_data) != nullptr ||
                            check_and_get_column<ColumnVarbinary>(*rhs_data) != nullptr;
    if (lhs_binary && rhs_binary) {
        // Iceberg schema evolution may represent the same byte key as STRING in one file and
        // VARBINARY in another. Equality-delete semantics compare bytes, not column storage classes.
        return lhs_data->get_data_at(lhs_row) == rhs_data->get_data_at(rhs_row);
    }
    return lhs_data->compare_at(lhs_row, rhs_row, *rhs_data, -1) == 0;
}

void update_varbinary_hashes(const ColumnWithTypeAndName& entry, uint64_t* hashes) {
    const IColumn* data = entry.column.get();
    const uint8_t* null_map = nullptr;
    if (entry.column->is_nullable()) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*entry.column);
        data = &nullable.get_nested_column();
        null_map = nullable.get_null_map_data().data();
    }
    for (size_t row = 0; row < entry.column->size(); ++row) {
        if (null_map != nullptr && null_map[row] != 0) {
            hashes[row] = HashUtil::xxHash64NullWithSeed(hashes[row]);
            continue;
        }
        const auto bytes = data->get_data_at(row);
        hashes[row] = HashUtil::xxHash64WithSeed(bytes.data, bytes.size, hashes[row]);
    }
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
    size_t rows = 0;
    for (const auto& column : block->get_columns()) {
        rows = std::max(rows, column->size());
    }
    // Lazy readers may leave an unread projected column at block position zero while predicate
    // columns (or the all-missing-key row-count carrier) are populated later. Block::rows() only
    // inspects the first column, so derive the explicit expression count from all materialized
    // columns and let execute_column() enforce that every result has exactly this batch size.
    ColumnPtr result_column;
    RETURN_IF_ERROR(execute_column(context, block, nullptr, rows, result_column));
    block->insert({std::move(result_column), std::make_shared<DataTypeBool>(), expr_name()});
    *result_column_id = static_cast<int>(block->columns() - 1);
    return Status::OK();
}

Status EqualityDeletePredicate::execute_column_impl(VExprContext* context, const Block* block,
                                                    const Selector* selector, size_t count,
                                                    ColumnPtr& result_column) const {
    if (_children.size() != _field_ids.size()) {
        return Status::InternalError(
                "EqualityDeletePredicate should have {} child exprs, but got {}", _field_ids.size(),
                _children.size());
    }

    // This path is required when every equality key is a literal, notably when all key columns are
    // absent from an older Iceberg data file and are represented by typed NULL literals. Preserve
    // `count` so the constant result can be expanded to the caller's batch size when necessary.
    Block data_key_block;
    for (const auto& child : _children) {
        ColumnPtr key_column;
        RETURN_IF_ERROR(child->execute_column(context, block, selector, count, key_column));
        // Equality comparison operates on row-addressable columns. Materialize literal constants
        // so nullable NULL keys and regular slot columns share the same compare_at contract.
        data_key_block.insert({key_column->convert_to_full_column_if_const(),
                               child->execute_type(block), child->expr_name()});
    }
    result_column = _evaluate_key_block(data_key_block);
    return Status::OK();
}

ColumnPtr EqualityDeletePredicate::_evaluate_key_block(const Block& data_key_block) const {
    const auto rows = data_key_block.rows();
    auto res_col = ColumnBool::create(rows, 0);
    if (_delete_hash_map.empty() || rows == 0) {
        return res_col;
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
    return res_col;
}

std::vector<uint64_t> EqualityDeletePredicate::_build_hashes(const Block& block) {
    std::vector<uint64_t> hashes(block.rows(), 0);
    for (const auto& entry : block) {
        if (remove_nullable(entry.type)->get_primitive_type() == TYPE_VARBINARY) {
            // ColumnVarbinary intentionally lacks the generic column hash hook. Keep the V2 delete
            // hash byte-identical to ColumnString so schema-mapped binary keys share hash buckets.
            update_varbinary_hashes(entry, hashes.data());
        } else {
            entry.column->update_hashes_with_value(hashes.data(), nullptr);
        }
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

} // namespace doris::format
