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
#include <parquet/api/schema.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/block/block.h"
#include "core/data_type/data_type_nullable.h"
#include "format/new_parquet/column_reader.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/parquet_statistics.h"
#include "io/fs/file_reader.h"
#include "storage/predicate/column_predicate.h"
#include "util/slice.h"

namespace doris::parquet {

namespace {

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
    DorisRandomAccessFile(io::FileReaderSPtr file, io::IOContext* io_ctx)
            : _file(std::move(file)), _io_ctx(io_ctx) {
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
        if (!_file) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        return static_cast<int64_t>(_file->size());
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
        if (!_file) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        if (position < 0 || nbytes < 0) {
            return arrow::Status::Invalid("negative read position or length");
        }
        size_t bytes_read = 0;
        Status st = _file->read_at(static_cast<size_t>(position),
                                   Slice(static_cast<uint8_t*>(out), static_cast<size_t>(nbytes)),
                                   &bytes_read, _io_ctx);
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
    io::FileReaderSPtr _file;
    io::IOContext* _io_ctx = nullptr;
    int64_t _pos = 0;
    bool _closed = false;
};

} // namespace

struct ParquetReaderScanState {
    // Doris 文件句柄适配成 Arrow RandomAccessFile。该对象只处理随机读，不携带
    // table/global schema 语义。
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;

    // Arrow Parquet core reader 和 footer metadata。ParquetReader 只依赖 core API，
    // 不使用 parquet::arrow reader，也不输出 Arrow Array/RecordBatch。
    std::unique_ptr<::parquet::ParquetFileReader> parquet_reader;
    std::shared_ptr<::parquet::FileMetaData> metadata;
    const ::parquet::SchemaDescriptor* schema = nullptr;
    std::vector<std::unique_ptr<ParquetColumnSchema>> file_schema;

    // 当前 scan 的 top-level file-local projection、物理需要打开的 leaf columns 和 row
    // group 列表。projected_fields 决定输出 block；required_leaf_columns 决定实际向 Arrow
    // Parquet core 请求哪些 leaf column reader。
    std::vector<int> projected_fields;
    std::vector<int> filter_fields;
    std::vector<int> required_leaf_columns;
    std::vector<reader::FileLocalFilter> local_filters;
    std::vector<int> selected_row_groups;
    size_t next_row_group_idx = 0;
    std::shared_ptr<::parquet::RowGroupReader> current_row_group;
    std::vector<std::unique_ptr<ParquetColumnReader>> current_filter_columns;
    std::vector<std::unique_ptr<ParquetColumnReader>> current_output_columns;
    std::vector<int> current_output_fields;
    std::unordered_map<int, size_t> current_filter_field_to_index;
    std::unordered_map<int, size_t> current_output_field_to_index;
    int64_t current_row_group_rows = 0;
    int64_t current_row_group_rows_read = 0;
};

namespace {

Status reset_reader_position(ParquetReaderScanState* state) {
    if (state == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    state->next_row_group_idx = 0;
    state->current_row_group.reset();
    state->current_filter_columns.clear();
    state->current_output_columns.clear();
    state->current_output_fields.clear();
    state->current_filter_field_to_index.clear();
    state->current_output_field_to_index.clear();
    state->current_row_group_rows = 0;
    state->current_row_group_rows_read = 0;
    return Status::OK();
}

void reset_current_row_group(ParquetReaderScanState* state) {
    state->current_row_group.reset();
    state->current_filter_columns.clear();
    state->current_output_columns.clear();
    state->current_output_fields.clear();
    state->current_filter_field_to_index.clear();
    state->current_output_field_to_index.clear();
    state->current_row_group_rows = 0;
    state->current_row_group_rows_read = 0;
}

void fill_schema_field(const ParquetColumnSchema& column_schema, reader::SchemaField* field) {
    field->id = column_schema.leaf_column_id >= 0 ? column_schema.leaf_column_id
                                                  : column_schema.field_id;
    field->name = column_schema.name;
    field->type = column_schema.type;
    field->children.clear();
    field->children.reserve(column_schema.children.size());
    for (const auto& child : column_schema.children) {
        reader::SchemaField child_field;
        fill_schema_field(*child, &child_field);
        field->children.push_back(std::move(child_field));
    }
}

bool has_structured_filter(const reader::FileLocalFilter& local_filter) {
    for (const auto& predicate : local_filter.predicates) {
        if (predicate != nullptr) {
            return true;
        }
    }
    return false;
}

PrimitiveType decoded_filter_type(const ParquetColumnSchema& column_schema) {
    if (column_schema.type == nullptr) {
        return INVALID_TYPE;
    }
    return remove_nullable(column_schema.type)->get_primitive_type();
}

bool has_supported_decoded_filter(const reader::FileLocalFilter& local_filter,
                                  const ParquetColumnSchema& column_schema) {
    const PrimitiveType filter_type = decoded_filter_type(column_schema);
    if (filter_type == INVALID_TYPE) {
        return false;
    }
    for (const auto& predicate : local_filter.predicates) {
        if (predicate != nullptr && predicate->primitive_type() == filter_type) {
            return true;
        }
    }
    return false;
}

void collect_filter_fields(const std::vector<std::unique_ptr<ParquetColumnSchema>>& fields,
                           const std::vector<reader::FileLocalFilter>& local_filters,
                           std::vector<int>* filter_fields) {
    filter_fields->clear();
    std::vector<bool> seen(fields.size(), false);
    for (const auto& local_filter : local_filters) {
        const int field_id = local_filter.file_column_id;
        if (field_id < 0 || field_id >= static_cast<int>(fields.size()) || seen[field_id] ||
            !has_structured_filter(local_filter)) {
            continue;
        }
        // 第一版 decoded filtering 只对 primitive file-local 列执行 ColumnPredicate。
        // VExprContext fallback 和 nested predicate 后续应走 reader expression 或
        // nested reader 的专用 selection 路径。
        if (fields[field_id]->kind != ParquetColumnSchemaKind::PRIMITIVE ||
            !has_supported_decoded_filter(local_filter, *fields[field_id])) {
            continue;
        }
        seen[field_id] = true;
        filter_fields->push_back(field_id);
    }
}

bool projected_by_filter_reader(const ParquetReaderScanState& state, int file_field_id) {
    return state.current_filter_field_to_index.find(file_field_id) !=
           state.current_filter_field_to_index.end();
}

Status read_filter_columns(ParquetReaderScanState* state, int64_t batch_rows,
                           std::unordered_map<int, ColumnPtr>* decoded_columns,
                           std::unordered_map<int, size_t>* decoded_rows) {
    for (size_t filter_idx = 0; filter_idx < state->filter_fields.size(); ++filter_idx) {
        const int file_field_id = state->filter_fields[filter_idx];
        auto& column_reader = state->current_filter_columns[filter_idx];
        MutableColumnPtr column;
        int64_t column_rows = 0;
        RETURN_IF_ERROR(column_reader->read_batch(batch_rows, &column, &column_rows));
        if (column_rows != batch_rows) {
            return Status::Corruption("Parquet filter column {} returned {} rows, expected {} rows",
                                      column_reader->name(), column_rows, batch_rows);
        }
        (*decoded_rows)[file_field_id] = static_cast<size_t>(column_rows);
        (*decoded_columns)[file_field_id] = std::move(column);
    }
    return Status::OK();
}

Status build_selection_from_filters(const ParquetReaderScanState& state, int64_t batch_rows,
                                    const std::unordered_map<int, ColumnPtr>& decoded_columns,
                                    std::vector<uint16_t>* selection, uint16_t* selected_rows) {
    if (batch_rows > std::numeric_limits<uint16_t>::max()) {
        return Status::InvalidArgument(
                "Parquet predicate batch size {} exceeds ColumnPredicate selection limit",
                batch_rows);
    }

    selection->resize(static_cast<size_t>(batch_rows));
    for (int64_t row_idx = 0; row_idx < batch_rows; ++row_idx) {
        (*selection)[row_idx] = static_cast<uint16_t>(row_idx);
    }
    *selected_rows = static_cast<uint16_t>(batch_rows);

    for (int file_field_id : state.filter_fields) {
        if (*selected_rows == 0) {
            break;
        }
        const auto filter_it = decoded_columns.find(file_field_id);
        if (filter_it == decoded_columns.end()) {
            continue;
        }
        for (const auto& local_filter : state.local_filters) {
            if (local_filter.file_column_id != file_field_id ||
                !has_structured_filter(local_filter)) {
                continue;
            }
            if (*selected_rows == 0) {
                break;
            }
            for (const auto& predicate : local_filter.predicates) {
                if (predicate == nullptr ||
                    predicate->primitive_type() !=
                            decoded_filter_type(*state.file_schema[file_field_id])) {
                    continue;
                }
                *selected_rows =
                        predicate->evaluate(*filter_it->second, selection->data(), *selected_rows);
                if (*selected_rows == 0) {
                    break;
                }
            }
        }
    }
    return Status::OK();
}

IColumn::Filter selection_to_filter(const std::vector<uint16_t>& selection, uint16_t selected_rows,
                                    int64_t batch_rows) {
    IColumn::Filter filter(static_cast<size_t>(batch_rows), 0);
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        filter[selection[selection_idx]] = 1;
    }
    return filter;
}

Status append_decoded_column(Block* file_block, int file_field_id, ColumnPtr column,
                             const ParquetColumnReader& column_reader) {
    if (!column) {
        return Status::InternalError("Parquet decoded column is null for field {}", file_field_id);
    }
    file_block->insert(
            ColumnWithTypeAndName {std::move(column), column_reader.type(), column_reader.name()});
    return Status::OK();
}

Status filter_decoded_column(ColumnPtr* column, const IColumn::Filter& filter,
                             uint16_t selected_rows) {
    ColumnPtr filtered_column;
    RETURN_IF_CATCH_EXCEPTION(filtered_column = (*column)->filter(filter, selected_rows));
    *column = std::move(filtered_column);
    return Status::OK();
}

void mark_required_leaf_columns(const ParquetColumnSchema& column_schema,
                                std::vector<bool>* required_leaf_columns) {
    if (column_schema.kind == ParquetColumnSchemaKind::PRIMITIVE) {
        if (column_schema.leaf_column_id >= 0 &&
            column_schema.leaf_column_id < static_cast<int>(required_leaf_columns->size())) {
            (*required_leaf_columns)[column_schema.leaf_column_id] = true;
        }
        return;
    }
    for (const auto& child : column_schema.children) {
        mark_required_leaf_columns(*child, required_leaf_columns);
    }
}

bool leaf_can_use_record_reader(const ParquetColumnSchema& column_schema) {
    return supported_flat_column_type(column_schema.descriptor) != nullptr;
}

const ParquetColumnSchema* find_leaf_schema(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& fields, int leaf_column_id) {
    for (const auto& field : fields) {
        if (field == nullptr) {
            continue;
        }
        std::vector<const ParquetColumnSchema*> stack;
        stack.push_back(field.get());
        while (!stack.empty()) {
            const auto* current = stack.back();
            stack.pop_back();
            if (current->leaf_column_id == leaf_column_id) {
                return current;
            }
            for (const auto& child : current->children) {
                if (child != nullptr) {
                    stack.push_back(child.get());
                }
            }
        }
    }
    return nullptr;
}

void collect_required_leaf_columns(const std::vector<std::unique_ptr<ParquetColumnSchema>>& fields,
                                   const std::vector<int>& projected_fields,
                                   const std::vector<reader::FileLocalFilter>& local_filters,
                                   int num_leaf_columns, std::vector<int>* required_leaf_columns) {
    required_leaf_columns->clear();
    std::vector<bool> required(static_cast<size_t>(num_leaf_columns), false);
    for (int field_id : projected_fields) {
        mark_required_leaf_columns(*fields[field_id], &required);
    }
    for (const auto& local_filter : local_filters) {
        const int field_id = local_filter.file_column_id;
        if (field_id >= 0 && field_id < static_cast<int>(fields.size())) {
            mark_required_leaf_columns(*fields[field_id], &required);
        }
    }
    required_leaf_columns->reserve(num_leaf_columns);
    for (int leaf_column_id = 0; leaf_column_id < num_leaf_columns; ++leaf_column_id) {
        if (required[leaf_column_id]) {
            required_leaf_columns->push_back(leaf_column_id);
        }
    }
}

Status open_next_row_group(ParquetReaderScanState* state, bool* has_row_group) {
    *has_row_group = false;
    while (state->next_row_group_idx < state->selected_row_groups.size()) {
        const int row_group_idx = state->selected_row_groups[state->next_row_group_idx++];
        try {
            state->current_row_group = state->parquet_reader->RowGroup(row_group_idx);
        } catch (const ::parquet::ParquetException& e) {
            return Status::Corruption("Failed to open parquet row group {}: {}", row_group_idx,
                                      e.what());
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to open parquet row group {}: {}", row_group_idx,
                                         e.what());
        }

        auto row_group_metadata = state->metadata->RowGroup(row_group_idx);
        state->current_row_group_rows =
                row_group_metadata == nullptr ? 0 : row_group_metadata->num_rows();
        if (state->current_row_group_rows < 0) {
            return Status::Corruption("Invalid negative row count in parquet row group {}",
                                      row_group_idx);
        }
        state->current_row_group_rows_read = 0;
        state->current_filter_columns.clear();
        state->current_output_columns.clear();
        state->current_output_fields.clear();
        state->current_filter_field_to_index.clear();
        state->current_output_field_to_index.clear();
        state->current_filter_columns.reserve(state->filter_fields.size());
        state->current_output_columns.reserve(state->projected_fields.size());

        std::vector<std::shared_ptr<::parquet::internal::RecordReader>> record_readers(
                state->schema->num_columns());
        for (int leaf_column_id : state->required_leaf_columns) {
            const auto* leaf_schema = find_leaf_schema(state->file_schema, leaf_column_id);
            if (leaf_schema != nullptr && leaf_can_use_record_reader(*leaf_schema)) {
                record_readers[leaf_column_id] = state->current_row_group->RecordReader(
                        leaf_column_id, /*read_dictionary=*/false);
                if (record_readers[leaf_column_id] == nullptr) {
                    return Status::Corruption(
                            "Failed to create parquet record reader for leaf column {}",
                            leaf_column_id);
                }
            }
        }

        ParquetColumnReaderFactory column_reader_factory(record_readers);
        for (int file_field_id : state->filter_fields) {
            const auto& column_schema = state->file_schema[file_field_id];
            std::unique_ptr<ParquetColumnReader> column_reader;
            RETURN_IF_ERROR(column_reader_factory.create(*column_schema, &column_reader));
            state->current_filter_field_to_index[file_field_id] =
                    state->current_filter_columns.size();
            state->current_filter_columns.push_back(std::move(column_reader));
        }
        for (int file_field_id : state->projected_fields) {
            if (projected_by_filter_reader(*state, file_field_id)) {
                continue;
            }
            const auto& column_schema = state->file_schema[file_field_id];
            std::unique_ptr<ParquetColumnReader> column_reader;
            RETURN_IF_ERROR(column_reader_factory.create(*column_schema, &column_reader));
            state->current_output_field_to_index[file_field_id] =
                    state->current_output_columns.size();
            state->current_output_fields.push_back(file_field_id);
            state->current_output_columns.push_back(std::move(column_reader));
        }

        if (state->current_row_group_rows == 0) {
            reset_current_row_group(state);
            continue;
        }
        *has_row_group = true;
        return Status::OK();
    }
    return Status::OK();
}

Status read_current_row_group_batch(ParquetReaderScanState* state, int64_t batch_rows,
                                    Block* file_block, size_t* rows) {
    file_block->clear();

    if (state->current_filter_columns.empty() && state->current_output_columns.empty()) {
        *rows = static_cast<size_t>(batch_rows);
        return Status::OK();
    }

    std::unordered_map<int, ColumnPtr> decoded_filter_columns;
    std::unordered_map<int, size_t> decoded_filter_rows;
    RETURN_IF_ERROR(
            read_filter_columns(state, batch_rows, &decoded_filter_columns, &decoded_filter_rows));

    std::vector<uint16_t> selection;
    uint16_t selected_rows = 0;
    RETURN_IF_ERROR(build_selection_from_filters(*state, batch_rows, decoded_filter_columns,
                                                 &selection, &selected_rows));

    if (selected_rows == 0) {
        for (auto& column_reader : state->current_output_columns) {
            RETURN_IF_ERROR(column_reader->skip(batch_rows));
        }
        *rows = 0;
        return Status::OK();
    }

    const bool need_filter_output = selected_rows != batch_rows;
    IColumn::Filter output_filter;
    if (need_filter_output) {
        output_filter = selection_to_filter(selection, selected_rows, batch_rows);
    }

    std::unordered_map<int, ColumnPtr> decoded_output_columns;
    decoded_output_columns.reserve(state->current_output_columns.size());
    for (size_t output_idx = 0; output_idx < state->current_output_columns.size(); ++output_idx) {
        auto& column_reader = state->current_output_columns[output_idx];
        MutableColumnPtr column;
        if (need_filter_output) {
            RETURN_IF_ERROR(
                    column_reader->read_selected(selection, selected_rows, batch_rows, &column));
            if (column->size() != selected_rows) {
                return Status::Corruption(
                        "Parquet selected output column {} returned {} rows, expected {} rows",
                        column_reader->name(), column->size(), selected_rows);
            }
        } else {
            int64_t column_rows = 0;
            RETURN_IF_ERROR(column_reader->read_batch(batch_rows, &column, &column_rows));
            if (column_rows != batch_rows) {
                return Status::Corruption(
                        "Parquet output column {} returned {} rows, expected {} rows",
                        column_reader->name(), column_rows, batch_rows);
            }
        }
        decoded_output_columns[state->current_output_fields[output_idx]] = std::move(column);
    }

    for (int file_field_id : state->projected_fields) {
        auto filter_it = decoded_filter_columns.find(file_field_id);
        if (filter_it != decoded_filter_columns.end()) {
            const auto reader_it = state->current_filter_field_to_index.find(file_field_id);
            if (reader_it == state->current_filter_field_to_index.end()) {
                return Status::InternalError(
                        "Missing parquet filter reader index for projected field {}",
                        file_field_id);
            }
            auto column = filter_it->second;
            if (need_filter_output) {
                RETURN_IF_ERROR(filter_decoded_column(&column, output_filter, selected_rows));
            }
            RETURN_IF_ERROR(
                    append_decoded_column(file_block, file_field_id, std::move(column),
                                          *state->current_filter_columns[reader_it->second]));
            continue;
        }

        const auto output_it = decoded_output_columns.find(file_field_id);
        const auto reader_it = state->current_output_field_to_index.find(file_field_id);
        if (output_it == decoded_output_columns.end() ||
            reader_it == state->current_output_field_to_index.end()) {
            return Status::InternalError("Missing parquet output column for projected field {}",
                                         file_field_id);
        }
        auto column = output_it->second;
        if (need_filter_output) {
            RETURN_IF_ERROR(filter_decoded_column(&column, output_filter, selected_rows));
        }
        RETURN_IF_ERROR(append_decoded_column(file_block, file_field_id, std::move(column),
                                              *state->current_output_columns[reader_it->second]));
    }

    *rows = static_cast<size_t>(selected_rows);
    return Status::OK();
}

} // namespace

ParquetReader::ParquetReader() : _state(std::make_unique<ParquetReaderScanState>()) {}

ParquetReader::~ParquetReader() = default;

Status ParquetReader::open(io::FileReaderSPtr file, io::IOContext* io_ctx) {
    RETURN_IF_ERROR(reader::FileReader::open(file, io_ctx));
    _state = std::make_unique<ParquetReaderScanState>();
    _state->arrow_file = std::make_shared<DorisRandomAccessFile>(_file, _io_ctx);

    try {
        _state->parquet_reader = ::parquet::ParquetFileReader::Open(
                _state->arrow_file, ::parquet::default_reader_properties());
        _state->metadata = _state->parquet_reader->metadata();
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
        fill_schema_field(*_state->file_schema[column_idx], &field);
        field.id = static_cast<reader::ColumnId>(column_idx);
        file_schema->push_back(std::move(field));
    }
    return Status::OK();
}

Status ParquetReader::init(const ParquetScanRequest& request) {
    return init(static_cast<const reader::FileScanRequest&>(request));
}

Status ParquetReader::init(const reader::FileScanRequest& request) {
    if (_state == nullptr || _state->metadata == nullptr || _state->schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    RETURN_IF_ERROR(reader::FileReader::init(request));

    _state->projected_fields.clear();
    _state->filter_fields.clear();
    _state->local_filters = request.local_filters;
    const int num_fields = static_cast<int>(_state->file_schema.size());
    for (auto column_id : request.projected_columns) {
        if (column_id < 0 || column_id >= num_fields) {
            return Status::InvalidArgument("Invalid parquet top-level field id {}", column_id);
        }
        _state->projected_fields.push_back(column_id);
    }
    for (const auto& local_filter : request.local_filters) {
        if (local_filter.file_column_id < 0 || local_filter.file_column_id >= num_fields) {
            return Status::InvalidArgument("Invalid parquet filter top-level field id {}",
                                           local_filter.file_column_id);
        }
    }
    collect_filter_fields(_state->file_schema, request.local_filters, &_state->filter_fields);
    collect_required_leaf_columns(_state->file_schema, _state->projected_fields,
                                  request.local_filters, _state->schema->num_columns(),
                                  &_state->required_leaf_columns);

    RETURN_IF_ERROR(select_row_groups_by_statistics(*_state->metadata, _state->file_schema, request,
                                                    &_state->selected_row_groups));
    RETURN_IF_ERROR(reset_reader_position(_state.get()));
    _eof = _state->selected_row_groups.empty();
    return Status::OK();
}

Status ParquetReader::get_block(Block* file_block, bool* eof) {
    if (eof != nullptr) {
        *eof = false;
    }
    if (file_block == nullptr || eof == nullptr) {
        return Status::InvalidArgument(
                "ParquetReader::get_block requires non-null output arguments");
    }
    if (_eof) {
        *eof = true;
        return Status::OK();
    }

    while (true) {
        if (_state == nullptr || _state->parquet_reader == nullptr || _state->schema == nullptr) {
            return Status::Uninitialized("ParquetReader is not open");
        }

        if (_state->current_row_group == nullptr) {
            bool has_row_group = false;
            RETURN_IF_ERROR(open_next_row_group(_state.get(), &has_row_group));
            if (!has_row_group) {
                _eof = true;
                *eof = true;
                return Status::OK();
            }
        }

        const int64_t remaining_rows =
                _state->current_row_group_rows - _state->current_row_group_rows_read;
        if (remaining_rows <= 0) {
            reset_current_row_group(_state.get());
            continue;
        }

        const int64_t batch_rows =
                std::min<int64_t>(DEFAULT_PARQUET_READ_BATCH_SIZE, remaining_rows);
        const int64_t physical_rows_read = batch_rows;
        size_t rows = 0;
        RETURN_IF_ERROR(read_current_row_group_batch(_state.get(), batch_rows, file_block, &rows));
        _state->current_row_group_rows_read += physical_rows_read;
        if (_state->current_row_group_rows_read >= _state->current_row_group_rows) {
            reset_current_row_group(_state.get());
        }
        if (rows == 0) {
            continue;
        }
        *eof = false;
        return Status::OK();
    }
}

Status ParquetReader::close() {
    if (_state != nullptr) {
        if (_state->parquet_reader != nullptr) {
            try {
                _state->parquet_reader->Close();
            } catch (const std::exception&) {
                // close 需要保持幂等；这里不覆盖此前 scan 路径上的真实错误。
            }
        }
        if (_state->arrow_file != nullptr) {
            static_cast<void>(arrow_status_to_doris_status(_state->arrow_file->Close()));
        }
        _state = std::make_unique<ParquetReaderScanState>();
    }
    return reader::FileReader::close();
}

} // namespace doris::parquet
