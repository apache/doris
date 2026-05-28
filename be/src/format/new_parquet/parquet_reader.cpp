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

#include "format/new_parquet/parquet_reader.h"

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <parquet/api/reader.h>

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr_context.h"
#include "format/new_parquet/column_reader.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/parquet_statistics.h"
#include "format/new_parquet/selection_vector.h"
#include "io/fs/file_reader.h"
#include "storage/predicate/column_predicate.h"
#include "util/slice.h"

namespace doris::parquet {

constexpr int64_t DEFAULT_PARQUET_READ_BATCH_SIZE = 4096;

Status arrow_status_to_doris_status(const arrow::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    if (status.IsIOError()) {
        return Status::IOError(status.ToString());
    }
    if (status.IsInvalid()) {
        return Status::InvalidArgument(status.ToString());
    }
    return Status::InternalError(status.ToString());
}

class DorisRandomAccessFile final : public arrow::io::RandomAccessFile {
public:
    DorisRandomAccessFile(io::FileReaderSPtr file_reader, io::IOContext* io_ctx)
            : _file_reader(std::move(file_reader)), _io_ctx(io_ctx) {
        set_mode(arrow::io::FileMode::READ);
    }

    arrow::Status Close() override {
        _closed = true;
        return arrow::Status::OK();
    }

    bool closed() const override { return _closed; }

    arrow::Result<int64_t> Tell() const override { return _pos; }

    arrow::Status Seek(int64_t position) override {
        if (position < 0) {
            return arrow::Status::Invalid("negative seek position");
        }
        _pos = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> GetSize() override {
        if (!_file_reader) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        return static_cast<int64_t>(_file_reader->size());
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, ReadAt(_pos, nbytes, out));
        _pos += bytes_read;
        return bytes_read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, Read(nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
        buffer->ZeroPadding();
        return buffer;
    }

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
        if (!_file_reader) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        if (position < 0 || nbytes < 0) {
            return arrow::Status::Invalid("negative read position or length");
        }
        size_t bytes_read = 0;
        Status st = _file_reader->read_at(
                static_cast<size_t>(position),
                Slice(static_cast<uint8_t*>(out), static_cast<size_t>(nbytes)), &bytes_read,
                _io_ctx);
        if (!st.ok()) {
            return arrow::Status::IOError(st.to_string_no_stack());
        }
        return static_cast<int64_t>(bytes_read);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                         int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, ReadAt(position, nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
        buffer->ZeroPadding();
        return buffer;
    }

private:
    io::FileReaderSPtr _file_reader;
    io::IOContext* _io_ctx = nullptr;
    int64_t _pos = 0;
    bool _closed = false;
};

struct ParquetReaderScanState {
    // Doris 文件句柄适配成 Arrow RandomAccessFile。该对象只处理随机读，不携带
    // table/global schema 语义。
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;

    // Arrow Parquet core reader 和 footer metadata。ParquetReader 只依赖 core API，
    // 不使用 parquet::arrow reader，也不输出 Arrow Array/RecordBatch。
    std::unique_ptr<::parquet::ParquetFileReader> file_reader;
    std::shared_ptr<::parquet::FileMetaData> metadata;
    const ::parquet::SchemaDescriptor* schema = nullptr;
    std::vector<std::unique_ptr<ParquetColumnSchema>> file_schema;

    // 当前 scan 的 top-level file-local projection 和 row group 列表。projected_fields
    // 决定输出 block；具体 leaf column reader 由 ParquetColumnReaderFactory 按需创建。
    std::vector<int> predicate_fields;
    std::vector<int> non_predicate_fields;
    std::vector<int> selected_row_groups;
    size_t next_row_group_idx = 0;
    std::shared_ptr<::parquet::RowGroupReader> current_row_group;
    std::vector<std::unique_ptr<ParquetColumnReader>> current_predicate_columns;
    std::vector<std::unique_ptr<ParquetColumnReader>> current_non_predicate_columns;
    int64_t current_row_group_rows = 0;
    int64_t current_row_group_rows_read = 0;
};

Status ParquetReader::_reset_reader_position() {
    _state->next_row_group_idx = 0;
    _state->current_row_group.reset();
    _state->current_predicate_columns.clear();
    _state->current_non_predicate_columns.clear();
    _state->current_row_group_rows = 0;
    _state->current_row_group_rows_read = 0;
    return Status::OK();
}

void ParquetReader::_reset_current_row_group() {
    _state->current_row_group.reset();
    _state->current_predicate_columns.clear();
    _state->current_non_predicate_columns.clear();
    _state->current_row_group_rows = 0;
    _state->current_row_group_rows_read = 0;
}

void ParquetReader::_fill_schema_field(const ParquetColumnSchema& column_schema,
                                       reader::SchemaField* field) const {
    field->id = column_schema.top_level_field_id;
    field->name = column_schema.name;
    field->type = column_schema.type;
    field->file_path = column_schema.file_path;
    field->field_id_path = column_schema.field_id_path;
    field->name_path = column_schema.name_path;
    field->children.clear();
    field->children.reserve(column_schema.children.size());
    for (const auto& child : column_schema.children) {
        reader::SchemaField child_field;
        _fill_schema_field(*child, &child_field);
        field->children.push_back(std::move(child_field));
    }
}

Status ParquetReader::_fill_projected_schema_field(const ParquetColumnSchema& column_schema,
                                                   const reader::FieldProjection* projection,
                                                   reader::SchemaField* field) const {
    if (field == nullptr) {
        return Status::InvalidArgument("projected schema field is null");
    }
    _fill_schema_field(column_schema, field);
    if (projection == nullptr || projection->project_all_children ||
        column_schema.children.empty()) {
        return Status::OK();
    }

    field->children.clear();
    std::map<int32_t, const reader::FieldProjection*> child_projection_by_idx;
    for (const auto& child_projection : projection->children) {
        if (child_projection.file_path.empty()) {
            return Status::InvalidArgument("Empty parquet projection path for column {}",
                                           column_schema.name);
        }
        child_projection_by_idx.emplace(child_projection.file_path.back(), &child_projection);
    }

    DataTypes child_types;
    Strings child_names;
    for (size_t child_idx = 0; child_idx < column_schema.children.size(); ++child_idx) {
        auto it = child_projection_by_idx.find(static_cast<int32_t>(child_idx));
        if (it == child_projection_by_idx.end()) {
            continue;
        }
        if (it->second->file_path != column_schema.children[child_idx]->file_path) {
            return Status::InvalidArgument("Invalid parquet projection path for column {}",
                                           column_schema.children[child_idx]->name);
        }
        reader::SchemaField child_field;
        RETURN_IF_ERROR(_fill_projected_schema_field(*column_schema.children[child_idx], it->second,
                                                     &child_field));
        child_types.push_back(child_field.type);
        child_names.push_back(child_field.name);
        field->children.push_back(std::move(child_field));
    }

    if (field->children.empty()) {
        return Status::NotSupported("Parquet projection for column {} contains no children",
                                    column_schema.name);
    }

    const auto primitive_type = remove_nullable(column_schema.type)->get_primitive_type();
    DataTypePtr projected_type;
    switch (primitive_type) {
    case TYPE_STRUCT:
        projected_type = std::make_shared<DataTypeStruct>(child_types, child_names);
        break;
    case TYPE_ARRAY:
        DORIS_CHECK(child_types.size() == 1);
        projected_type = std::make_shared<DataTypeArray>(child_types[0]);
        break;
    case TYPE_MAP:
        DORIS_CHECK(child_types.size() == 1);
        DORIS_CHECK(remove_nullable(child_types[0])->get_primitive_type() == TYPE_STRUCT);
        {
            const auto* entry_type =
                    assert_cast<const DataTypeStruct*>(remove_nullable(child_types[0]).get());
            DORIS_CHECK(entry_type->get_elements().size() == 2);
            projected_type = std::make_shared<DataTypeMap>(entry_type->get_element(0),
                                                           entry_type->get_element(1));
        }
        break;
    default:
        return Status::InvalidArgument("Cannot project children from non-complex parquet column {}",
                                       column_schema.name);
    }
    field->type =
            column_schema.type->is_nullable() ? make_nullable(projected_type) : projected_type;
    return Status::OK();
}

Status ParquetReader::_get_projected_schema_field(reader::ColumnId file_column_id,
                                                  const reader::FieldProjection* projection,
                                                  reader::SchemaField* field) const {
    if (file_column_id < 0 ||
        file_column_id >= static_cast<reader::ColumnId>(_state->file_schema.size())) {
        return Status::InvalidArgument("Invalid parquet field id {}", file_column_id);
    }
    RETURN_IF_ERROR(
            _fill_projected_schema_field(*_state->file_schema[file_column_id], projection, field));
    field->id = file_column_id;
    return Status::OK();
}

Status ParquetReader::_read_filter_columns(int64_t batch_rows, Block* file_block,
                                           SelectionVector* selection, uint16_t* selected_rows) {
    selection->resize(static_cast<size_t>(batch_rows));
    for (size_t filter_idx = 0; filter_idx < _request->predicate_columns.size(); ++filter_idx) {
        const int file_field_id = _request->predicate_columns[filter_idx];
        auto& column_reader = _state->current_predicate_columns[filter_idx];
        auto position_it = _request->column_positions.find(file_field_id);
        DORIS_CHECK(position_it != _request->column_positions.end());
        const auto block_position = position_it->second;
        auto column = file_block->get_by_position(block_position).column->assume_mutable();
        DCHECK_EQ(file_block->get_by_position(block_position).type->get_primitive_type(),
                  column_reader->type()->get_primitive_type());
        int64_t column_rows = 0;
        RETURN_IF_ERROR(column_reader->read(batch_rows, column, &column_rows));
        if (column_rows != batch_rows) {
            return Status::Corruption("Parquet filter column {} returned {} rows, expected {} rows",
                                      column_reader->name(), column_rows, batch_rows);
        }
        file_block->replace_by_position(block_position, std::move(column));
    }
    return _execute_filter_conjuncts(batch_rows, file_block, selection, selected_rows);
}

Status ParquetReader::_execute_filter_conjuncts(int64_t batch_rows, Block* file_block,
                                                SelectionVector* selection,
                                                uint16_t* selected_rows) {
    // Expression filters may reference several predicate columns. Execute them only after all
    // predicate columns in the file-local block have been materialized.
    for (const auto& expression_filter : _request->expression_filters) {
        if (expression_filter.conjunct == nullptr) {
            continue;
        }
        if (*selected_rows == 0) {
            break;
        }
        IColumn::Filter filter(static_cast<size_t>(batch_rows), 1);
        bool can_filter_all = false;
        RETURN_IF_ERROR(expression_filter.conjunct->execute_filter(file_block, filter.data(),
                                                                   static_cast<size_t>(batch_rows),
                                                                   false, &can_filter_all));
        *selected_rows =
                can_filter_all ? 0 : _apply_filter_to_selection(filter, selection, *selected_rows);
    }
    return Status::OK();
}

IColumn::Filter ParquetReader::_selection_to_filter(const SelectionVector& selection,
                                                    uint16_t selected_rows, int64_t batch_rows) {
    IColumn::Filter filter(static_cast<size_t>(batch_rows), 0);
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        filter[selection.get_index(selection_idx)] = 1;
    }
    return filter;
}

uint16_t ParquetReader::_apply_filter_to_selection(const IColumn::Filter& filter,
                                                   SelectionVector* selection,
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

Status ParquetReader::_open_next_row_group(bool* has_row_group) {
    *has_row_group = false;
    while (_state->next_row_group_idx < _state->selected_row_groups.size()) {
        const int row_group_idx = _state->selected_row_groups[_state->next_row_group_idx++];
        try {
            _state->current_row_group = _state->file_reader->RowGroup(row_group_idx);
        } catch (const ::parquet::ParquetException& e) {
            return Status::Corruption("Failed to open parquet row group {}: {}", row_group_idx,
                                      e.what());
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to open parquet row group {}: {}", row_group_idx,
                                         e.what());
        }

        auto row_group_metadata = _state->metadata->RowGroup(row_group_idx);
        _state->current_row_group_rows =
                row_group_metadata == nullptr ? 0 : row_group_metadata->num_rows();
        if (_state->current_row_group_rows < 0) {
            return Status::Corruption("Invalid negative row count in parquet row group {}",
                                      row_group_idx);
        } else if (_state->current_row_group_rows == 0) {
            _reset_current_row_group();
            continue;
        }
        _state->current_row_group_rows_read = 0;
        _state->current_predicate_columns.clear();
        _state->current_non_predicate_columns.clear();

        ParquetColumnReaderFactory column_reader_factory(_state->current_row_group,
                                                         _state->schema->num_columns());
        for (const auto file_field_id : _request->predicate_columns) {
            const auto& column_schema = _state->file_schema[file_field_id];
            const auto projection_it = _request->complex_projections.find(file_field_id);
            const auto* projection = projection_it == _request->complex_projections.end()
                                             ? nullptr
                                             : &projection_it->second;
            std::unique_ptr<ParquetColumnReader> column_reader;
            RETURN_IF_ERROR(
                    column_reader_factory.create(*column_schema, projection, &column_reader));
            _state->current_predicate_columns.push_back(std::move(column_reader));
        }
        for (const auto file_field_id : _request->non_predicate_columns) {
            const auto& column_schema = _state->file_schema[file_field_id];
            const auto projection_it = _request->complex_projections.find(file_field_id);
            const auto* projection = projection_it == _request->complex_projections.end()
                                             ? nullptr
                                             : &projection_it->second;
            std::unique_ptr<ParquetColumnReader> column_reader;
            RETURN_IF_ERROR(
                    column_reader_factory.create(*column_schema, projection, &column_reader));
            _state->current_non_predicate_columns.push_back(std::move(column_reader));
        }
        *has_row_group = true;
        break;
    }
    return Status::OK();
}

// `file_block` has the same layout as FileScanRequest::column_positions.
Status ParquetReader::_read_current_row_group_batch(int64_t batch_rows, Block* file_block,
                                                    size_t* rows) {
    if (_state->current_predicate_columns.empty() &&
        _state->current_non_predicate_columns.empty()) {
        *rows = static_cast<size_t>(batch_rows);
        return Status::OK();
    }
    SelectionVector selection;
    DORIS_CHECK(batch_rows <= std::numeric_limits<uint16_t>::max());
    uint16_t selected_rows = static_cast<uint16_t>(batch_rows);
    // 1. Read all predicate columns and evaluate selection vector.
    RETURN_IF_ERROR(_read_filter_columns(batch_rows, file_block, &selection, &selected_rows));

    // 2. Materialize all predicate columns after filtering.
    const bool need_filter_output = selected_rows != batch_rows;
    if (need_filter_output) {
        IColumn::Filter output_filter = _selection_to_filter(selection, selected_rows, batch_rows);
        for (const auto file_field_id : _request->predicate_columns) {
            auto position_it = _request->column_positions.find(file_field_id);
            DORIS_CHECK(position_it != _request->column_positions.end());
            const auto block_position = position_it->second;
            RETURN_IF_CATCH_EXCEPTION(file_block->replace_by_position(
                    block_position, file_block->get_by_position(block_position)
                                            .column->filter(output_filter, selected_rows)));
        }
    }

    // 3. Materialize all non-predicate columns with selection.
    for (size_t output_idx = 0; output_idx < _state->current_non_predicate_columns.size();
         ++output_idx) {
        auto& column_reader = _state->current_non_predicate_columns[output_idx];
        auto position_it =
                _request->column_positions.find(_request->non_predicate_columns[output_idx]);
        DORIS_CHECK(position_it != _request->column_positions.end());
        const auto block_position = position_it->second;
        auto col = file_block->get_columns()[block_position]->assume_mutable();
        DCHECK_EQ(file_block->get_by_position(block_position).type->get_primitive_type(),
                  column_reader->type()->get_primitive_type());
        if (need_filter_output) {
            [[maybe_unused]] auto old_size = col->size();
            RETURN_IF_ERROR(column_reader->select(selection, selected_rows, batch_rows, col));
            if (col->size() != old_size + selected_rows) {
                return Status::Corruption(
                        "Parquet selected output column {} returned {} rows, expected {} rows",
                        column_reader->name(), col->size(), old_size + selected_rows);
            }
        } else {
            int64_t column_rows = 0;
            RETURN_IF_ERROR(column_reader->read(batch_rows, col, &column_rows));
            if (column_rows != batch_rows) {
                return Status::Corruption(
                        "Parquet output column {} returned {} rows, expected {} rows",
                        column_reader->name(), column_rows, batch_rows);
            }
        }
    }

    *rows = static_cast<size_t>(selected_rows);
    return Status::OK();
}

ParquetReader::ParquetReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                             std::unique_ptr<io::FileDescription>& file_description,
                             std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile)
        : FileReader(system_properties, file_description, io_ctx, profile) {}

ParquetReader::~ParquetReader() = default;

Status ParquetReader::init(RuntimeState* state) {
    RETURN_IF_ERROR(reader::FileReader::init(state));
    _state = std::make_unique<ParquetReaderScanState>();
    _state->arrow_file =
            std::make_shared<DorisRandomAccessFile>(_tracing_file_reader, _io_ctx.get());

    try {
        _state->file_reader = ::parquet::ParquetFileReader::Open(
                _state->arrow_file, ::parquet::default_reader_properties());
        _state->metadata = _state->file_reader->metadata();
        _state->schema = _state->metadata != nullptr ? _state->metadata->schema() : nullptr;
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to open parquet file: {}", e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open parquet file: {}", e.what());
    }

    if (_state->metadata == nullptr || _state->schema == nullptr) {
        return Status::Corruption("Failed to read parquet metadata");
    }
    RETURN_IF_ERROR(build_parquet_column_schema(*_state->schema, &_state->file_schema));
    return Status::OK();
}

Status ParquetReader::get_schema(std::vector<reader::SchemaField>* file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("file_schema is null");
    }
    file_schema->clear();
    if (_state == nullptr || _state->schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }

    file_schema->reserve(_state->file_schema.size());
    for (size_t column_idx = 0; column_idx < _state->file_schema.size(); ++column_idx) {
        reader::SchemaField field;
        _fill_schema_field(*_state->file_schema[column_idx], &field);
        field.id = static_cast<reader::ColumnId>(column_idx);
        file_schema->push_back(std::move(field));
    }
    return Status::OK();
}

Status ParquetReader::open(std::unique_ptr<reader::FileScanRequest>& request) {
    if (_state == nullptr || _state->metadata == nullptr || _state->schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    RETURN_IF_ERROR(reader::FileReader::open(request));

    // `_request->column_positions.empty()` means all columns are needed by table reader
    if (_request->column_positions.empty()) {
        for (const auto file_column_id : _request->predicate_columns) {
            _request->column_positions.emplace(file_column_id, file_column_id);
        }
        for (const auto file_column_id : _request->non_predicate_columns) {
            _request->column_positions.emplace(file_column_id, file_column_id);
        }
    }

    const int num_fields = static_cast<int>(_state->file_schema.size());
    for (const auto file_column_id : _request->predicate_columns) {
        DORIS_CHECK(_request->column_positions.count(file_column_id) > 0);
        DORIS_CHECK(file_column_id >= 0 && file_column_id < num_fields);
    }
    for (const auto file_column_id : _request->non_predicate_columns) {
        DORIS_CHECK(_request->column_positions.count(file_column_id) > 0);
        DORIS_CHECK(file_column_id >= 0 && file_column_id < num_fields);
    }
    for (const auto& column_filter : _request->column_predicate_filters) {
        if (column_filter.file_column_id < 0 || column_filter.file_column_id >= num_fields) {
            return Status::InvalidArgument("Invalid parquet filter top-level field id {}",
                                           column_filter.file_column_id);
        }
    }
    for (const auto& [file_column_id, projection] : _request->complex_projections) {
        if (file_column_id < 0 || file_column_id >= num_fields) {
            return Status::InvalidArgument("Invalid parquet projection top-level field id {}",
                                           file_column_id);
        }
        if (projection.file_column_id != file_column_id) {
            return Status::InvalidArgument(
                    "Parquet projection column id mismatch: key={}, value={}", file_column_id,
                    projection.file_column_id);
        }
        if (!projection.file_path.empty() && projection.file_path.front() != file_column_id) {
            return Status::InvalidArgument("Invalid parquet projection root path for column {}",
                                           file_column_id);
        }
        reader::SchemaField projected_field;
        RETURN_IF_ERROR(_get_projected_schema_field(file_column_id, &projection, &projected_field));
    }
    RETURN_IF_ERROR(select_row_groups_by_statistics(*_state->metadata, _state->file_schema,
                                                    *_request, &_state->selected_row_groups));
    RETURN_IF_ERROR(_reset_reader_position());
    _eof = _state->selected_row_groups.empty();
    return Status::OK();
}

Status ParquetReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    if (_state == nullptr || _state->file_reader == nullptr || _state->schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    *rows = 0;
    if (_eof) {
        *eof = true;
        return Status::OK();
    }

    while (true) {
        if (_state->current_row_group == nullptr) {
            bool has_row_group = false;
            RETURN_IF_ERROR(_open_next_row_group(&has_row_group));
            if (!has_row_group) {
                _eof = true;
                *eof = true;
                return Status::OK();
            }
        }

        const int64_t remaining_rows =
                _state->current_row_group_rows - _state->current_row_group_rows_read;
        if (remaining_rows <= 0) {
            _reset_current_row_group();
            continue;
        }

        const int64_t batch_rows =
                std::min<int64_t>(DEFAULT_PARQUET_READ_BATCH_SIZE, remaining_rows);
        const int64_t physical_rows_read = batch_rows;
        RETURN_IF_ERROR(_read_current_row_group_batch(batch_rows, file_block, rows));
        _state->current_row_group_rows_read += physical_rows_read;
        if (_state->current_row_group_rows_read >= _state->current_row_group_rows) {
            _reset_current_row_group();
        }
        if (*rows == 0) {
            continue;
        }
        *eof = false;
        // TODO: Compute _request->reader_expression_map to filter file_block
        return Status::OK();
    }
}

Status ParquetReader::close() {
    if (_state != nullptr) {
        if (_state->file_reader != nullptr) {
            try {
                _state->file_reader->Close();
            } catch (const std::exception&) {
                // close 需要保持幂等；这里不覆盖此前 scan 路径上的真实错误。
            }
        }
        if (_state->arrow_file != nullptr) {
            static_cast<void>(arrow_status_to_doris_status(_state->arrow_file->Close()));
        }
        _state = std::make_unique<ParquetReaderScanState>();
    }
    return FileReader::close();
}

void ParquetReader::_init_profile() {
    if (_profile != nullptr) {
        static const char* parquet_profile = "ParquetReader";
        ADD_TIMER_WITH_LEVEL(_profile, parquet_profile, 1);

        _parquet_profile.filtered_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsFiltered", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_row_groups_by_min_max = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsFilteredByMinMax", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_row_groups_by_bloom_filter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsFilteredByBloomFilter", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.to_read_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsReadNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.total_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsTotalNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_group_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByGroup", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_page_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByPage", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.lazy_read_filtered_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByLazyRead", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredBytes", TUnit::BYTES, parquet_profile, 1);
        _parquet_profile.raw_rows_read = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RawRowsRead", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.column_read_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ColumnReadTime", parquet_profile, 1);
        _parquet_profile.parse_meta_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ParseMetaTime", parquet_profile, 1);
        _parquet_profile.parse_footer_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ParseFooterTime", parquet_profile, 1);
        _parquet_profile.file_reader_create_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "FileReaderCreateTime", parquet_profile, 1);
        _parquet_profile.open_file_num =
                ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "FileNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_index_read_calls =
                ADD_COUNTER_WITH_LEVEL(_profile, "PageIndexReadCalls", TUnit::UNIT, 1);
        _parquet_profile.page_index_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexFilterTime", parquet_profile, 1);
        _parquet_profile.read_page_index_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexReadTime", parquet_profile, 1);
        _parquet_profile.parse_page_index_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexParseTime", parquet_profile, 1);
        _parquet_profile.row_group_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "RowGroupFilterTime", parquet_profile, 1);
        _parquet_profile.file_footer_read_calls =
                ADD_COUNTER_WITH_LEVEL(_profile, "FileFooterReadCalls", TUnit::UNIT, 1);
        _parquet_profile.file_footer_hit_cache =
                ADD_COUNTER_WITH_LEVEL(_profile, "FileFooterHitCache", TUnit::UNIT, 1);
        _parquet_profile.decompress_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecompressTime", parquet_profile, 1);
        _parquet_profile.decompress_cnt = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "DecompressCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_read_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageReadCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheWriteCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_compressed_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheCompressedWriteCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_decompressed_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheDecompressedWriteCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheHitCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_missing_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheMissingCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_compressed_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheCompressedHitCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_decompressed_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheDecompressedHitCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.decode_header_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageHeaderDecodeTime", parquet_profile, 1);
        _parquet_profile.read_page_header_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageHeaderReadTime", parquet_profile, 1);
        _parquet_profile.decode_value_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeValueTime", parquet_profile, 1);
        _parquet_profile.decode_dict_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeDictTime", parquet_profile, 1);
        _parquet_profile.decode_level_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeLevelTime", parquet_profile, 1);
        _parquet_profile.decode_null_map_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeNullMapTime", parquet_profile, 1);
        _parquet_profile.skip_page_header_num = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "SkipPageHeaderNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.parse_page_header_num = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "ParsePageHeaderNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.predicate_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PredicateFilterTime", parquet_profile, 1);
        _parquet_profile.dict_filter_rewrite_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DictFilterRewriteTime", parquet_profile, 1);
        _parquet_profile.convert_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ConvertTime", parquet_profile, 1);
        _parquet_profile.bloom_filter_read_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "BloomFilterReadTime", parquet_profile, 1);
    }
}

} // namespace doris::parquet
