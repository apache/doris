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

#include "format_v2/lance/lance_record_batch_converter.h"

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <limits>

#include "common/consts.h"
#include "common/exception.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/lance/lance_reader_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/utils.h"

namespace doris::format::lance {
namespace {

constexpr size_t IGNORED_OUTPUT_INDEX = std::numeric_limits<size_t>::max();

} // namespace

arrow::MemoryPool* LanceRecordBatchConverter::_query_arrow_memory_pool(
        RuntimeState* runtime_state) {
    DORIS_CHECK(runtime_state != nullptr);
    if (runtime_state->exec_env() != nullptr) {
        if (auto* memory_pool = runtime_state->exec_env()->arrow_memory_pool();
            memory_pool != nullptr) {
            return memory_pool;
        }
    }
    return arrow::default_memory_pool();
}

Status LanceRecordBatchConverter::init(RuntimeState* runtime_state,
                                       const std::vector<ColumnDefinition>& projected_columns,
                                       SearchKind search_kind) {
    DORIS_CHECK(runtime_state != nullptr);
    _search_kind = search_kind;
    _timezone = runtime_state->timezone_obj();
    _memory_pool = _query_arrow_memory_pool(runtime_state);
    _output_columns.clear();
    _output_columns.reserve(projected_columns.size());
    _output_index_by_input_name.clear();
    _output_index_by_input_name.reserve(projected_columns.size());
    _global_rowid_output_index.reset();
    reset_schema();

    for (const auto& column : projected_columns) {
        if (column.type == nullptr) {
            return Status::InvalidArgument("Lance projected column '{}' has no type", column.name);
        }
        if (column.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            if (_search_kind == SearchKind::NORMAL) {
                return Status::NotSupported(
                        "Lance global row id is currently supported only for external search");
            }
            if (_global_rowid_output_index.has_value()) {
                return Status::InvalidArgument("duplicate Lance global row id projected column: {}",
                                               column.name);
            }
            if (remove_nullable(column.type)->get_primitive_type() != TYPE_STRING) {
                return Status::InvalidArgument(
                        "Lance global row id column '{}' must have Doris STRING type, but was {}",
                        column.name, column.type->get_name());
            }
            _global_rowid_output_index = _output_columns.size();
            // Lance returns the native `_rowid`, while Doris exposes it through a generated
            // global-rowid column. One input column cannot also satisfy a regular `_rowid`
            // projection, so reject an existing binding instead of silently replacing it.
            if (!_output_index_by_input_name
                         .emplace(LANCE_ROW_ID_COLUMN, *_global_rowid_output_index)
                         .second) {
                return Status::InvalidArgument(
                        "Lance global row id cannot be combined with projected column '{}'",
                        LANCE_ROW_ID_COLUMN);
            }
            _output_columns.push_back({column.name, nullptr});
            continue;
        }
        const auto output_index = _output_columns.size();
        if (!_output_index_by_input_name.emplace(column.name, output_index).second) {
            return Status::InvalidArgument("duplicate Lance projected column: {}", column.name);
        }
        if (_search_kind == SearchKind::VECTOR && column.name == LANCE_DISTANCE_COLUMN &&
            remove_nullable(column.type)->get_primitive_type() != TYPE_FLOAT) {
            return Status::InvalidArgument(
                    "Lance vector search column '{}' must have Doris FLOAT type, but was {}",
                    LANCE_DISTANCE_COLUMN, column.type->get_name());
        }
        if (_search_kind == SearchKind::FULL_TEXT && column.name == LANCE_SCORE_COLUMN &&
            remove_nullable(column.type)->get_primitive_type() != TYPE_FLOAT) {
            return Status::InvalidArgument(
                    "Lance full-text search column '{}' must have Doris FLOAT type, but was {}",
                    LANCE_SCORE_COLUMN, column.type->get_name());
        }
        _output_columns.push_back({column.name, column.type->get_serde()});
    }
    return Status::OK();
}

Status LanceRecordBatchConverter::_bind_schema(const std::shared_ptr<arrow::Schema>& schema) {
    DORIS_CHECK(schema != nullptr);
    if (_bound_schema == schema ||
        (_bound_schema != nullptr && _bound_schema->Equals(*schema, true))) {
        return Status::OK();
    }

    std::vector<size_t> arrow_column_to_output(static_cast<size_t>(schema->num_fields()),
                                               IGNORED_OUTPUT_INDEX);
    std::vector<LanceArrowArrayNormalizer> array_normalizer_for_arrow_column(
            static_cast<size_t>(schema->num_fields()));
    std::vector<bool> output_has_source_column(_output_columns.size(), false);
    for (int arrow_index = 0; arrow_index < schema->num_fields(); ++arrow_index) {
        const auto& field = schema->field(arrow_index);
        const auto output = _output_index_by_input_name.find(field->name());
        if (output == _output_index_by_input_name.end()) {
            const bool is_omitted_search_result =
                    (_search_kind == SearchKind::VECTOR &&
                     field->name() == LANCE_DISTANCE_COLUMN) ||
                    (_search_kind == SearchKind::FULL_TEXT && field->name() == LANCE_SCORE_COLUMN);
            // Lance may return this generated result even when Doris did not project it.
            if (is_omitted_search_result) {
                continue;
            }
            return Status::InternalError("Lance returned unknown column '{}'", field->name());
        }
        const auto output_index = output->second;
        if (output_has_source_column[output_index]) {
            return Status::InternalError("Lance returned duplicate column '{}'", field->name());
        }
        output_has_source_column[output_index] = true;
        arrow_column_to_output[arrow_index] = output_index;
        RETURN_IF_ERROR(LanceArrowArrayNormalizer::create(
                field, &array_normalizer_for_arrow_column[static_cast<size_t>(arrow_index)]));
    }
    for (size_t output_index = 0; output_index < _output_columns.size(); ++output_index) {
        if (!output_has_source_column[output_index]) {
            return Status::InternalError("Lance did not return requested column '{}'",
                                         _output_columns[output_index].name);
        }
    }
    _bound_schema = schema;
    _arrow_column_to_output_index = std::move(arrow_column_to_output);
    _array_normalizer_for_arrow_column = std::move(array_normalizer_for_arrow_column);
    return Status::OK();
}

Status LanceRecordBatchConverter::convert_record_batch_to_block(
        const std::shared_ptr<arrow::RecordBatch>& record_batch, Block* block,
        const std::optional<GlobalRowIdContext>& global_rowid_context, size_t* rows) {
    DORIS_CHECK(record_batch != nullptr);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(rows != nullptr);
    RETURN_IF_ERROR(_bind_schema(record_batch->schema()));

    const auto row_count = static_cast<size_t>(record_batch->num_rows());
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (int arrow_index = 0; arrow_index < record_batch->num_columns(); ++arrow_index) {
        const auto output_index = _arrow_column_to_output_index[static_cast<size_t>(arrow_index)];
        if (output_index == IGNORED_OUTPUT_INDEX) {
            continue;
        }
        const auto& output = _output_columns[output_index];
        const auto& arrow_column = record_batch->column(arrow_index);
        if (is_global_rowid_output(output_index)) {
            if (!global_rowid_context.has_value()) {
                return Status::InvalidArgument(
                        "Lance global row id requested without global row id context");
            }
            RETURN_IF_ERROR(_append_global_row_ids(arrow_column, columns[output_index],
                                                   *global_rowid_context));
            continue;
        }
        const auto& field = record_batch->schema()->field(arrow_index);
        try {
            if (arrow_column->type_id() == arrow::Type::NA) {
                columns[output_index]->insert_many_defaults(row_count);
                continue;
            }
            std::shared_ptr<arrow::Array> normalized_column;
            RETURN_IF_ERROR(
                    _array_normalizer_for_arrow_column[static_cast<size_t>(arrow_index)]
                            .normalize_for_doris(arrow_column, _memory_pool, &normalized_column));
            RETURN_IF_ERROR(output.serde->read_column_from_arrow(
                    *columns[output_index], normalized_column.get(), 0, row_count, _timezone));
        } catch (const Exception& e) {
            return Status::InternalError("convert Lance Arrow column '{}' failed: {}",
                                         field->name(), e.what());
        }
    }
    *rows = row_count;
    return Status::OK();
}

void LanceRecordBatchConverter::reset_schema() {
    _bound_schema.reset();
    _arrow_column_to_output_index.clear();
    _array_normalizer_for_arrow_column.clear();
}

Status LanceRecordBatchConverter::_append_global_row_ids(
        const std::shared_ptr<arrow::Array>& row_ids, MutableColumnPtr& output_column,
        const GlobalRowIdContext& context) const {
    if (row_ids->type_id() != arrow::Type::UINT64) {
        return Status::InternalError("Lance row id column must be Arrow UINT64, but was {}",
                                     row_ids->type()->ToString());
    }

    ColumnString* data_column = nullptr;
    ColumnUInt8::Container* null_map = nullptr;
    if (auto* nullable = check_and_get_column<ColumnNullable>(*output_column)) {
        data_column = check_and_get_column<ColumnString>(nullable->get_nested_column());
        null_map = &nullable->get_null_map_data();
    } else {
        data_column = check_and_get_column<ColumnString>(*output_column);
    }
    if (data_column == nullptr) {
        return Status::InternalError("Lance global row id output column must be STRING");
    }

    const auto typed_row_ids = std::static_pointer_cast<arrow::UInt64Array>(row_ids);
    if (typed_row_ids->null_count() != 0) {
        return Status::InternalError("Lance returned null row id");
    }
    const auto row_count = static_cast<size_t>(typed_row_ids->length());
    if (null_map != nullptr) {
        null_map->resize_fill(null_map->size() + row_count, 0);
    }
    for (size_t row = 0; row < row_count; ++row) {
        const GlobalRowLoacationV2 location(ROW_VERSION::LANCE_DATASET_ROW_ID, context.backend_id,
                                            context.file_id, typed_row_ids->Value(row));
        data_column->insert_data(reinterpret_cast<const char*>(&location), sizeof(location));
    }
    return Status::OK();
}

} // namespace doris::format::lance
