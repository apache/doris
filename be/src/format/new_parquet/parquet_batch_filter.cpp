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

#include "format/new_parquet/parquet_batch_filter.h"

#include <algorithm>
#include <memory>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/predicate_column.h"
#include "core/data_type/primitive_type.h"
#include "exprs/vexpr_context.h"
#include "storage/predicate/column_predicate.h"
#include "storage/schema.h"

namespace doris::parquet {

namespace {

template <PrimitiveType Type>
void append_to_predicate_column(const IColumn& column, size_t rows,
                                IColumn::MutablePtr* predicate_column) {
    auto* target = assert_cast<PredicateColumnType<Type>*>(predicate_column->get());
    const auto materialized_column = column.convert_to_full_column_if_const();
    const auto* source = assert_cast<const typename PrimitiveTypeTraits<Type>::ColumnType*>(
            materialized_column.get());
    target->reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const auto storage_value =
                PrimitiveTypeConvertor<Type>::to_storage_field_type(source->get_element(row));
        target->insert_data(reinterpret_cast<const char*>(&storage_value), sizeof(storage_value));
    }
}

Status append_to_predicate_column(PrimitiveType primitive_type, const IColumn& column, size_t rows,
                                  IColumn::MutablePtr* predicate_column) {
    switch (primitive_type) {
    case TYPE_BOOLEAN:
        append_to_predicate_column<TYPE_BOOLEAN>(column, rows, predicate_column);
        break;
    case TYPE_TINYINT:
        append_to_predicate_column<TYPE_TINYINT>(column, rows, predicate_column);
        break;
    case TYPE_SMALLINT:
        append_to_predicate_column<TYPE_SMALLINT>(column, rows, predicate_column);
        break;
    case TYPE_INT:
        append_to_predicate_column<TYPE_INT>(column, rows, predicate_column);
        break;
    case TYPE_BIGINT:
        append_to_predicate_column<TYPE_BIGINT>(column, rows, predicate_column);
        break;
    case TYPE_LARGEINT:
        append_to_predicate_column<TYPE_LARGEINT>(column, rows, predicate_column);
        break;
    case TYPE_FLOAT:
        append_to_predicate_column<TYPE_FLOAT>(column, rows, predicate_column);
        break;
    case TYPE_DOUBLE:
        append_to_predicate_column<TYPE_DOUBLE>(column, rows, predicate_column);
        break;
    case TYPE_DATE:
        append_to_predicate_column<TYPE_DATE>(column, rows, predicate_column);
        break;
    case TYPE_DATETIME:
        append_to_predicate_column<TYPE_DATETIME>(column, rows, predicate_column);
        break;
    case TYPE_DATEV2:
        append_to_predicate_column<TYPE_DATEV2>(column, rows, predicate_column);
        break;
    case TYPE_DATETIMEV2:
        append_to_predicate_column<TYPE_DATETIMEV2>(column, rows, predicate_column);
        break;
    case TYPE_TIMESTAMPTZ:
        append_to_predicate_column<TYPE_TIMESTAMPTZ>(column, rows, predicate_column);
        break;
    case TYPE_DECIMALV2:
        append_to_predicate_column<TYPE_DECIMALV2>(column, rows, predicate_column);
        break;
    case TYPE_DECIMAL32:
        append_to_predicate_column<TYPE_DECIMAL32>(column, rows, predicate_column);
        break;
    case TYPE_DECIMAL64:
        append_to_predicate_column<TYPE_DECIMAL64>(column, rows, predicate_column);
        break;
    case TYPE_DECIMAL128I:
        append_to_predicate_column<TYPE_DECIMAL128I>(column, rows, predicate_column);
        break;
    case TYPE_DECIMAL256:
        append_to_predicate_column<TYPE_DECIMAL256>(column, rows, predicate_column);
        break;
    case TYPE_IPV4:
        append_to_predicate_column<TYPE_IPV4>(column, rows, predicate_column);
        break;
    case TYPE_IPV6:
        append_to_predicate_column<TYPE_IPV6>(column, rows, predicate_column);
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        auto* target = assert_cast<PredicateColumnType<TYPE_STRING>*>(predicate_column->get());
        const auto materialized_column = column.convert_to_full_column_if_const();
        const auto* source = assert_cast<const ColumnString*>(materialized_column.get());
        target->reserve(rows);
        for (size_t row = 0; row < rows; ++row) {
            const auto value = source->get_data_at(row);
            target->insert_data(value.data, value.size);
        }
        break;
    }
    default:
        return Status::NotSupported("Parquet column predicate does not support type {}",
                                    type_to_string(primitive_type));
    }
    return Status::OK();
}

Status build_predicate_column(const ColumnWithTypeAndName& column, size_t rows,
                              IColumn::MutablePtr* predicate_column) {
    DORIS_CHECK(column.type != nullptr);
    DORIS_CHECK(column.column.get() != nullptr);
    const auto field_type = column.type->get_storage_field_type();
    *predicate_column = Schema::get_predicate_column_ptr(field_type, column.type->is_nullable(),
                                                         ReaderType::READER_ALTER_TABLE);
    if (column.type->is_nullable()) {
        const auto materialized_column = column.column->convert_to_full_column_if_const();
        const auto* nullable_column = assert_cast<const ColumnNullable*>(materialized_column.get());
        auto* predicate_nullable_column = assert_cast<ColumnNullable*>(predicate_column->get());
        auto nested_predicate_column = predicate_nullable_column->get_nested_column_ptr();
        RETURN_IF_ERROR(append_to_predicate_column(column.type->get_primitive_type(),
                                                   nullable_column->get_nested_column(), rows,
                                                   &nested_predicate_column));
        auto& null_map = predicate_nullable_column->get_null_map_data();
        DCHECK(null_map.empty());
        const auto& source_null_map = nullable_column->get_null_map_data();
        null_map.insert(source_null_map.begin(), source_null_map.begin() + rows);
        return Status::OK();
    }
    return append_to_predicate_column(column.type->get_primitive_type(), *column.column, rows,
                                      predicate_column);
}

uint16_t apply_filter_to_selection(const IColumn::Filter& filter, SelectionVector* selection,
                                   uint16_t selected_rows) {
    uint16_t new_selected_rows = 0;
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        const auto row_idx = selection->get_index(selection_idx);
        if (filter[row_idx] != 0) {
            selection->set_index(new_selected_rows++, static_cast<SelectionVector::Index>(row_idx));
        }
    }
    return new_selected_rows;
}

Status execute_column_predicate_filters(const reader::FileScanRequest& request, int64_t batch_rows,
                                        Block* file_block, SelectionVector* selection,
                                        uint16_t* selected_rows) {
    for (const auto& column_filter : request.column_predicate_filters) {
        if (column_filter.predicates.empty()) {
            continue;
        }
        auto position_it = request.column_positions.find(column_filter.file_column_id);
        DORIS_CHECK(position_it != request.column_positions.end());
        const auto block_position = position_it->second;
        IColumn::MutablePtr predicate_column;
        RETURN_IF_ERROR(build_predicate_column(file_block->get_by_position(block_position),
                                               static_cast<size_t>(batch_rows), &predicate_column));
        for (const auto& predicate : column_filter.predicates) {
            DORIS_CHECK(predicate != nullptr);
            *selected_rows =
                    predicate->evaluate(*predicate_column, selection->data(), *selected_rows);
            if (*selected_rows == 0) {
                break;
            }
        }
        if (*selected_rows == 0) {
            break;
        }
    }
    return Status::OK();
}

Status execute_filter_conjuncts(const reader::FileScanRequest& request, int64_t batch_rows,
                                Block* file_block, SelectionVector* selection,
                                uint16_t* selected_rows) {
    for (const auto& conjunct : request.conjuncts) {
        if (*selected_rows == 0) {
            break;
        }
        DORIS_CHECK(conjunct != nullptr);
        IColumn::Filter filter(static_cast<size_t>(batch_rows), 1);
        bool can_filter_all = false;
        RETURN_IF_ERROR(conjunct->execute_filter(file_block, filter.data(),
                                                 static_cast<size_t>(batch_rows), false,
                                                 &can_filter_all));
        *selected_rows =
                can_filter_all ? 0 : apply_filter_to_selection(filter, selection, *selected_rows);
    }
    return Status::OK();
}

Status execute_delete_conjuncts(const reader::FileScanRequest& request, int64_t batch_rows,
                                Block* file_block, SelectionVector* selection,
                                uint16_t* selected_rows) {
    for (const auto& delete_conjunct : request.delete_conjuncts) {
        if (*selected_rows == 0) {
            break;
        }
        DORIS_CHECK(delete_conjunct != nullptr);
        int result_column_id = -1;
        RETURN_IF_ERROR(delete_conjunct->root()->execute(delete_conjunct.get(), file_block,
                                                         &result_column_id));
        DORIS_CHECK(result_column_id >= 0 &&
                    result_column_id < static_cast<int>(file_block->columns()));
        const auto& delete_filter = assert_cast<const ColumnUInt8&>(
                                            *file_block->get_by_position(result_column_id).column)
                                            .get_data();
        DORIS_CHECK(delete_filter.size() == static_cast<size_t>(batch_rows));
        IColumn::Filter keep_filter(static_cast<size_t>(batch_rows), 1);
        bool has_kept_row = false;
        for (size_t row = 0; row < static_cast<size_t>(batch_rows); ++row) {
            keep_filter[row] = !delete_filter[row];
            has_kept_row |= keep_filter[row] != 0;
        }
        file_block->erase(result_column_id);
        *selected_rows =
                !has_kept_row ? 0
                              : apply_filter_to_selection(keep_filter, selection, *selected_rows);
    }
    return Status::OK();
}

} // namespace

IColumn::Filter selection_to_filter(const SelectionVector& selection, uint16_t selected_rows,
                                    int64_t batch_rows) {
    IColumn::Filter filter(static_cast<size_t>(batch_rows), 0);
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        filter[selection.get_index(selection_idx)] = 1;
    }
    return filter;
}

Status execute_reader_expression_map(const reader::FileScanRequest& request, Block* file_block,
                                     const std::vector<reader::ColumnId>& target_columns) {
    if (target_columns.empty()) {
        return Status::OK();
    }
    for (const auto& [file_column_id, expression] : request.reader_expression_map) {
        if (std::find(target_columns.begin(), target_columns.end(), file_column_id) ==
            target_columns.end()) {
            continue;
        }
        DORIS_CHECK(expression != nullptr);
        auto position_it = request.column_positions.find(file_column_id);
        DORIS_CHECK(position_it != request.column_positions.end());
        const auto block_position = position_it->second;
        int result_column_id = -1;
        RETURN_IF_ERROR(expression->execute(file_block, &result_column_id));
        DORIS_CHECK(result_column_id >= 0 &&
                    result_column_id < static_cast<int>(file_block->columns()));
        auto result_column = file_block->get_by_position(result_column_id);
        file_block->replace_by_position(block_position, std::move(result_column.column));
        file_block->erase(result_column_id);
        file_block->get_by_position(block_position).type = std::move(result_column.type);
    }
    return Status::OK();
}

Status execute_batch_filters(const reader::FileScanRequest& request, int64_t batch_rows,
                             Block* file_block, SelectionVector* selection,
                             uint16_t* selected_rows) {
    RETURN_IF_ERROR(execute_column_predicate_filters(request, batch_rows, file_block, selection,
                                                     selected_rows));
    if (*selected_rows == 0) {
        return Status::OK();
    }
    RETURN_IF_ERROR(
            execute_filter_conjuncts(request, batch_rows, file_block, selection, selected_rows));
    if (*selected_rows == 0) {
        return Status::OK();
    }
    return execute_delete_conjuncts(request, batch_rows, file_block, selection, selected_rows);
}

} // namespace doris::parquet
