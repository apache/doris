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

#include "format/table/iceberg_reader_v2.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <iterator>
#include <string>
#include <type_traits>

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "format/table/equality_delete.h"

namespace doris::iceberg {

namespace {

constexpr const char* EQ_DELETE_HELPER_COLUMN_PREFIX = "__iceberg_eq_delete_field_";

std::string normalized_format(std::string format) {
    std::transform(format.begin(), format.end(), format.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return format;
}

bool is_parquet_format(const std::string& format) {
    return format.empty() || normalized_format(format) == "parquet";
}

const std::string& original_or_physical_path(const IcebergDataFile& data_file) {
    return data_file.original_path.empty() ? data_file.path : data_file.original_path;
}

void add_unique_column(std::vector<reader::ColumnId>* columns, reader::ColumnId column_id) {
    if (std::find(columns->begin(), columns->end(), column_id) == columns->end()) {
        columns->push_back(column_id);
    }
}

Status normalize_delete_rows(std::vector<int64_t>* rows) {
    for (int64_t row : *rows) {
        if (row < 0) {
            return Status::DataQualityError("Iceberg delete row position must be non-negative: {}",
                                            row);
        }
    }
    std::sort(rows->begin(), rows->end());
    rows->erase(std::unique(rows->begin(), rows->end()), rows->end());
    return Status::OK();
}

void append_bounded_rows(const std::vector<int64_t>& input, const IcebergDeleteFile& delete_file,
                         std::vector<int64_t>* output) {
    for (int64_t row : input) {
        if (delete_file.position_lower_bound.has_value() &&
            row < delete_file.position_lower_bound.value()) {
            continue;
        }
        if (delete_file.position_upper_bound.has_value() &&
            row > delete_file.position_upper_bound.value()) {
            continue;
        }
        output->push_back(row);
    }
}

} // namespace

IcebergTableReader::IcebergTableReader() = default;
IcebergTableReader::~IcebergTableReader() = default;

Status IcebergTableReader::init(IcebergTableReadParams params) {
    if (params.data_reader_factory == nullptr) {
        return Status::InvalidArgument("IcebergTableReader requires a data reader factory");
    }
    if (params.table_options.format != reader::FileFormat::PARQUET) {
        return Status::NotSupported("Only parquet is supported by iceberg reader v2");
    }
    _data_reader_factory = std::move(params.data_reader_factory);
    _delete_file_loader = std::move(params.delete_file_loader);
    _name_mapping = std::move(params.name_mapping);
    _runtime_profile = params.profile;
    RETURN_IF_ERROR(reader::TableReader::init(std::move(params.table_options)));

    reader::TableColumnMapperOptions mapper_options;
    mapper_options.mode = reader::TableColumnMappingMode::BY_FIELD_ID;
    mapper_options.name_mapping = _name_mapping;
    _data_reader.column_mapper = reader::TableColumnMapper(mapper_options);
    return Status::OK();
}

Status IcebergTableReader::close() {
    RETURN_IF_ERROR(reader::TableReader::close());
    return clear_task_state();
}

Status IcebergTableReader::create_reader_for_task(const reader::ScanTask& task,
                                                  std::unique_ptr<reader::FileReader>* reader) {
    if (reader == nullptr) {
        return Status::InvalidArgument("file reader output is null");
    }
    const auto* iceberg_task = dynamic_cast<const IcebergScanTask*>(&task);
    if (iceberg_task == nullptr) {
        return Status::InvalidArgument("IcebergTableReader received a non-Iceberg scan task");
    }
    const auto* data_file = dynamic_cast<const IcebergDataFile*>(iceberg_task->data_file.get());
    if (data_file == nullptr) {
        return Status::InvalidArgument("Iceberg scan task is missing Iceberg data file");
    }
    if (!is_parquet_format(data_file->format)) {
        return Status::NotSupported("Only parquet is supported by iceberg reader v2");
    }
    _current_iceberg_task = iceberg_task;
    _current_data_file = data_file;
    RETURN_IF_ERROR(_data_reader_factory->create(*iceberg_task, reader));
    if (*reader == nullptr) {
        return Status::InternalError("Iceberg data reader factory returned null reader");
    }
    return Status::OK();
}

Status IcebergTableReader::open_reader() {
    if (_data_reader.reader == nullptr || _current_iceberg_task == nullptr ||
        _current_data_file == nullptr) {
        return Status::InternalError("Iceberg reader task state is not initialized");
    }

    std::vector<reader::SchemaField> file_schema;
    RETURN_IF_ERROR(_data_reader.reader->get_schema(&file_schema));
    RETURN_IF_ERROR(_data_reader.column_mapper.create_mapping(
            _options.projected_columns, _partition_values, file_schema));
    update_row_lineage_file_column_state();

    reader::FileScanRequest file_request;
    RETURN_IF_ERROR(_data_reader.column_mapper.create_scan_request(
            file_pushdown_filters(), _options.projected_columns, &file_request));
    file_request.partition_values = _partition_values;
    RETURN_IF_ERROR(build_file_block_schema(file_schema, file_request));

    RETURN_IF_ERROR(build_position_visibility(&file_request));
    RETURN_IF_ERROR(build_equality_delete_state(&file_request, file_schema));

    _need_row_positions = _need_row_positions || needs_row_positions();
    file_request.need_row_positions = _need_row_positions;
    RETURN_IF_ERROR(_data_reader.reader->init(file_request));
    return Status::OK();
}

Status IcebergTableReader::close_current_reader() {
    RETURN_IF_ERROR(reader::TableReader::close_current_reader());
    return clear_task_state();
}

Status IcebergTableReader::finalize_chunk(Block* block) {
    if (block == nullptr) {
        return Status::InvalidArgument("block is null");
    }
    IColumn::Filter filter(block->rows(), 1);
    RETURN_IF_ERROR(apply_residual_filters(block, &filter));
    RETURN_IF_ERROR(apply_position_deletes(block, &filter));
    RETURN_IF_ERROR(apply_equality_deletes(block, &filter));
    Block::filter_block_internal(block, filter, block->columns());
    return Status::OK();
}

Status IcebergTableReader::materialize_virtual_columns(Block* table_block) {
    if (table_block == nullptr) {
        return Status::InvalidArgument("block is null");
    }
    _current_batch_row_positions.clear();
    if (_need_row_positions && table_block->rows() > 0) {
        const auto& row_positions = _data_reader.reader->current_batch_row_positions();
        if (row_positions.size() != table_block->rows()) {
            return Status::InternalError(
                    "Iceberg reader v2 requires row positions for current batch, rows={}, "
                    "positions={}",
                    table_block->rows(), row_positions.size());
        }
        _current_batch_row_positions = row_positions;
    }
    RETURN_IF_ERROR(materialize_legacy_row_id(table_block));
    RETURN_IF_ERROR(materialize_row_lineage_row_id(table_block));
    RETURN_IF_ERROR(materialize_last_updated_sequence_number(table_block));
    return Status::OK();
}

Status IcebergTableReader::build_position_visibility(reader::FileScanRequest* request) {
    if (request == nullptr) {
        return Status::InvalidArgument("file scan request is null");
    }
    std::vector<int64_t> rows;
    RETURN_IF_ERROR(load_position_delete_rows(&rows));
    std::vector<int64_t> dv_rows;
    RETURN_IF_ERROR(load_deletion_vector_rows(&dv_rows));
    rows.insert(rows.end(), dv_rows.begin(), dv_rows.end());
    RETURN_IF_ERROR(normalize_delete_rows(&rows));
    if (!rows.empty()) {
        _position_delete_rows = std::make_shared<std::vector<int64_t>>(std::move(rows));
        _need_row_positions = true;
        request->need_row_positions = true;
    }
    return Status::OK();
}

Status IcebergTableReader::load_position_delete_rows(std::vector<int64_t>* rows) {
    if (rows == nullptr) {
        return Status::InvalidArgument("position delete rows output is null");
    }
    if (_current_iceberg_task == nullptr || _current_data_file == nullptr) {
        return Status::InternalError("Iceberg reader task state is not initialized");
    }
    if (_current_iceberg_task->positional_deletes.empty()) {
        return Status::OK();
    }
    if (_delete_file_loader == nullptr) {
        return Status::InvalidArgument("Iceberg position deletes require a delete file loader");
    }
    for (const auto& delete_file : _current_iceberg_task->positional_deletes) {
        if (!is_parquet_format(delete_file.format)) {
            return Status::NotSupported("Only parquet position delete files are supported");
        }
        std::vector<int64_t> file_rows;
        RETURN_IF_ERROR(_delete_file_loader->load_position_deletes(delete_file, *_current_data_file,
                                                                   &file_rows));
        append_bounded_rows(file_rows, delete_file, rows);
    }
    return normalize_delete_rows(rows);
}

Status IcebergTableReader::load_deletion_vector_rows(std::vector<int64_t>* rows) {
    if (rows == nullptr) {
        return Status::InvalidArgument("deletion vector rows output is null");
    }
    if (_current_iceberg_task == nullptr || _current_data_file == nullptr) {
        return Status::InternalError("Iceberg reader task state is not initialized");
    }
    if (_current_iceberg_task->deletion_vectors.empty()) {
        return Status::OK();
    }
    if (_current_iceberg_task->deletion_vectors.size() != 1) {
        return Status::DataQualityError("This iceberg data file has multiple DVs.");
    }
    if (_delete_file_loader == nullptr) {
        return Status::InvalidArgument("Iceberg deletion vectors require a delete file loader");
    }
    const auto& delete_file = _current_iceberg_task->deletion_vectors.front();
    if (!delete_file.content_offset.has_value() || !delete_file.content_size_in_bytes.has_value()) {
        return Status::InternalError("Deletion vector is missing content offset or length");
    }
    RETURN_IF_ERROR(
            _delete_file_loader->load_deletion_vector(delete_file, *_current_data_file, rows));
    return normalize_delete_rows(rows);
}

Status IcebergTableReader::build_equality_delete_state(
        reader::FileScanRequest* request, const std::vector<reader::SchemaField>& file_schema) {
    if (request == nullptr) {
        return Status::InvalidArgument("file scan request is null");
    }
    if (_current_iceberg_task == nullptr) {
        return Status::InternalError("Iceberg reader task state is not initialized");
    }
    if (_current_iceberg_task->equality_deletes.empty()) {
        return Status::OK();
    }
    if (_delete_file_loader == nullptr) {
        return Status::InvalidArgument("Iceberg equality deletes require a delete file loader");
    }
    if (_runtime_profile == nullptr) {
        return Status::InvalidArgument("Iceberg equality deletes require a runtime profile");
    }

    for (const auto& delete_file : _current_iceberg_task->equality_deletes) {
        if (!is_parquet_format(delete_file.format)) {
            return Status::NotSupported("Only parquet equality delete files are supported");
        }
        if (delete_file.equality_field_ids.empty()) {
            return Status::InternalError(
                    "missing delete field ids when reading equality delete file");
        }
        IcebergEqualityDeleteData delete_data;
        RETURN_IF_ERROR(
                _delete_file_loader->load_equality_deletes(delete_file, file_schema, &delete_data));
        if (delete_data.field_ids.size() != delete_data.delete_block.columns()) {
            return Status::InternalError(
                    "equality delete field id count {} does not match delete block columns {}",
                    delete_data.field_ids.size(), delete_data.delete_block.columns());
        }
        std::vector<int> delete_col_ids;
        delete_col_ids.reserve(delete_data.field_ids.size());
        for (reader::ColumnId field_id : delete_data.field_ids) {
            RETURN_IF_ERROR(ensure_equality_key_column(field_id, file_schema, request));
            delete_col_ids.push_back(field_id);
        }
        _equality_delete_blocks.emplace_back(std::move(delete_data.delete_block));
        _equality_delete_field_ids.emplace_back(std::move(delete_col_ids));
    }

    for (size_t i = 0; i < _equality_delete_blocks.size(); ++i) {
        auto equality_delete_impl = EqualityDeleteBase::get_delete_impl(
                &_equality_delete_blocks[i], _equality_delete_field_ids[i]);
        RETURN_IF_ERROR(equality_delete_impl->init(_runtime_profile));
        _equality_delete_impls.emplace_back(std::move(equality_delete_impl));
    }
    return Status::OK();
}

Status IcebergTableReader::apply_residual_filters(Block* block, IColumn::Filter* filter) {
    if (block == nullptr) {
        return Status::InvalidArgument("block is null");
    }
    if (filter == nullptr) {
        return Status::InvalidArgument("residual filter is null");
    }
    if (_options.conjuncts.empty() || block->rows() == 0) {
        return Status::OK();
    }

    bool can_filter_all = false;
    RETURN_IF_ERROR(VExprContext::execute_conjuncts(_options.conjuncts, nullptr, block, filter,
                                                    &can_filter_all));
    return Status::OK();
}

Status IcebergTableReader::apply_position_deletes(Block* block, IColumn::Filter* filter) {
    if (block == nullptr) {
        return Status::InvalidArgument("block is null");
    }
    if (filter == nullptr) {
        return Status::InvalidArgument("position delete filter is null");
    }
    if (_position_delete_rows == nullptr || _position_delete_rows->empty() || block->rows() == 0) {
        return Status::OK();
    }
    if (_current_batch_row_positions.size() != block->rows()) {
        return Status::InternalError(
                "Iceberg reader v2 requires row positions for position delete, rows={}, "
                "positions={}",
                block->rows(), _current_batch_row_positions.size());
    }
    DCHECK(std::is_sorted(_current_batch_row_positions.begin(),
                          _current_batch_row_positions.end()));
    size_t delete_idx = 0;
    for (size_t i = 0; i < _current_batch_row_positions.size(); ++i) {
        const int64_t row_position = _current_batch_row_positions[i];
        while (delete_idx < _position_delete_rows->size() &&
               (*_position_delete_rows)[delete_idx] < row_position) {
            ++delete_idx;
        }
        if (delete_idx == _position_delete_rows->size()) {
            break;
        }
        if ((*_position_delete_rows)[delete_idx] == row_position) {
            (*filter)[i] = 0;
            ++delete_idx;
        }
    }
    return Status::OK();
}

Status IcebergTableReader::apply_equality_deletes(Block* block, IColumn::Filter* filter) {
    if (block == nullptr) {
        return Status::InvalidArgument("block is null");
    }
    if (filter == nullptr) {
        return Status::InvalidArgument("equality delete filter is null");
    }
    if (_equality_delete_impls.empty()) {
        return Status::OK();
    }
    if (block->rows() > 0) {
        auto name_to_pos = block->get_name_to_pos_map();
        for (auto& equality_delete_impl : _equality_delete_impls) {
            RETURN_IF_ERROR(equality_delete_impl->filter_data_block(
                    block, &name_to_pos, _id_to_block_column_name, *filter));
        }
    }

    std::set<size_t> positions_to_erase;
    for (const auto& helper_name : _equality_helper_column_names) {
        int pos = block->get_position_by_name(helper_name);
        if (pos < 0) {
            return Status::InternalError("Missing equality helper column '{}'", helper_name);
        }
        positions_to_erase.insert(static_cast<size_t>(pos));
    }
    if (!positions_to_erase.empty()) {
        block->erase(positions_to_erase);
    }
    return Status::OK();
}

Status IcebergTableReader::materialize_legacy_row_id(Block* block) {
    if (block == nullptr || _current_data_file == nullptr) {
        return Status::OK();
    }
    int pos = block->get_position_by_name(BeConsts::ICEBERG_ROWID_COL);
    if (pos < 0) {
        return Status::OK();
    }
    if (_current_batch_row_positions.size() != block->rows()) {
        return Status::InternalError("Iceberg row id column requires current batch row positions");
    }
    const size_t rows = block->rows();
    auto column = block->get_by_position(pos).column->assume_mutable();
    ColumnNullable* nullable_column = check_and_get_column<ColumnNullable>(column.get());
    ColumnStruct* struct_column = nullptr;
    if (nullable_column != nullptr) {
        struct_column = check_and_get_column<ColumnStruct>(&nullable_column->get_nested_column());
        auto& null_map = nullable_column->get_null_map_data();
        DCHECK_EQ(null_map.size(), rows);
        std::fill(null_map.begin(), null_map.end(), 0);
    } else {
        struct_column = check_and_get_column<ColumnStruct>(column.get());
    }
    if (struct_column == nullptr || struct_column->tuple_size() < 4) {
        return Status::InternalError("Invalid iceberg rowid column structure");
    }

    IColumn* file_path_column = &struct_column->get_column(0);
    if (auto* nullable_child = check_and_get_column<ColumnNullable>(file_path_column);
        nullable_child != nullptr) {
        file_path_column = &nullable_child->get_nested_column();
        auto& null_map = nullable_child->get_null_map_data();
        DCHECK_EQ(null_map.size(), rows);
        std::fill(null_map.begin(), null_map.end(), 0);
    }
    const std::string& file_path = original_or_physical_path(*_current_data_file);
    if (auto* string_column = check_and_get_column<ColumnString>(file_path_column);
        string_column != nullptr) {
        auto& chars = string_column->get_chars();
        auto& offsets = string_column->get_offsets();
        using Offset = std::decay_t<decltype(offsets)>::value_type;
        chars.clear();
        offsets.clear();
        chars.reserve(rows * file_path.size());
        offsets.reserve(rows);
        size_t offset = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (!file_path.empty()) {
                const size_t old_size = chars.size();
                chars.resize(old_size + file_path.size());
                std::memcpy(chars.data() + old_size, file_path.data(), file_path.size());
            }
            offset += file_path.size();
            offsets.push_back(static_cast<Offset>(offset));
        }
    } else if (auto* string64_column = check_and_get_column<ColumnString64>(file_path_column);
               string64_column != nullptr) {
        auto& chars = string64_column->get_chars();
        auto& offsets = string64_column->get_offsets();
        using Offset = std::decay_t<decltype(offsets)>::value_type;
        chars.clear();
        offsets.clear();
        chars.reserve(rows * file_path.size());
        offsets.reserve(rows);
        size_t offset = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (!file_path.empty()) {
                const size_t old_size = chars.size();
                chars.resize(old_size + file_path.size());
                std::memcpy(chars.data() + old_size, file_path.data(), file_path.size());
            }
            offset += file_path.size();
            offsets.push_back(static_cast<Offset>(offset));
        }
    } else {
        return Status::InternalError("Invalid iceberg rowid file path column {}",
                                     file_path_column->get_name());
    }

    IColumn* row_position_column = &struct_column->get_column(1);
    if (auto* nullable_child = check_and_get_column<ColumnNullable>(row_position_column);
        nullable_child != nullptr) {
        row_position_column = &nullable_child->get_nested_column();
        auto& null_map = nullable_child->get_null_map_data();
        DCHECK_EQ(null_map.size(), rows);
        std::fill(null_map.begin(), null_map.end(), 0);
    }
    auto& row_position_data = assert_cast<ColumnInt64&>(*row_position_column).get_data();
    DCHECK_EQ(row_position_data.size(), rows);
    for (size_t i = 0; i < rows; ++i) {
        row_position_data[i] = _current_batch_row_positions[i];
    }

    IColumn* partition_spec_column = &struct_column->get_column(2);
    if (auto* nullable_child = check_and_get_column<ColumnNullable>(partition_spec_column);
        nullable_child != nullptr) {
        partition_spec_column = &nullable_child->get_nested_column();
        auto& null_map = nullable_child->get_null_map_data();
        DCHECK_EQ(null_map.size(), rows);
        std::fill(null_map.begin(), null_map.end(), 0);
    }
    auto& partition_spec_data = assert_cast<ColumnInt32&>(*partition_spec_column).get_data();
    DCHECK_EQ(partition_spec_data.size(), rows);
    std::fill(partition_spec_data.begin(), partition_spec_data.end(),
              _current_data_file->partition_spec_id);

    IColumn* partition_data_column = &struct_column->get_column(3);
    if (auto* nullable_child = check_and_get_column<ColumnNullable>(partition_data_column);
        nullable_child != nullptr) {
        partition_data_column = &nullable_child->get_nested_column();
        auto& null_map = nullable_child->get_null_map_data();
        DCHECK_EQ(null_map.size(), rows);
        std::fill(null_map.begin(), null_map.end(), 0);
    }
    const std::string& partition_data = _current_data_file->partition_data_json;
    if (auto* string_column = check_and_get_column<ColumnString>(partition_data_column);
        string_column != nullptr) {
        auto& chars = string_column->get_chars();
        auto& offsets = string_column->get_offsets();
        using Offset = std::decay_t<decltype(offsets)>::value_type;
        chars.clear();
        offsets.clear();
        chars.reserve(rows * partition_data.size());
        offsets.reserve(rows);
        size_t offset = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (!partition_data.empty()) {
                const size_t old_size = chars.size();
                chars.resize(old_size + partition_data.size());
                std::memcpy(chars.data() + old_size, partition_data.data(), partition_data.size());
            }
            offset += partition_data.size();
            offsets.push_back(static_cast<Offset>(offset));
        }
    } else if (auto* string64_column = check_and_get_column<ColumnString64>(partition_data_column);
               string64_column != nullptr) {
        auto& chars = string64_column->get_chars();
        auto& offsets = string64_column->get_offsets();
        using Offset = std::decay_t<decltype(offsets)>::value_type;
        chars.clear();
        offsets.clear();
        chars.reserve(rows * partition_data.size());
        offsets.reserve(rows);
        size_t offset = 0;
        for (size_t i = 0; i < rows; ++i) {
            if (!partition_data.empty()) {
                const size_t old_size = chars.size();
                chars.resize(old_size + partition_data.size());
                std::memcpy(chars.data() + old_size, partition_data.data(), partition_data.size());
            }
            offset += partition_data.size();
            offsets.push_back(static_cast<Offset>(offset));
        }
    } else {
        return Status::InternalError("Invalid iceberg rowid partition data column {}",
                                     partition_data_column->get_name());
    }
    return Status::OK();
}

Status IcebergTableReader::materialize_row_lineage_row_id(Block* block) {
    if (block == nullptr || _current_data_file == nullptr) {
        return Status::OK();
    }
    if (_row_lineage_row_id_from_file) {
        return Status::OK();
    }
    int pos = block->get_position_by_name(ROW_LINEAGE_ROW_ID);
    if (pos < 0) {
        return Status::OK();
    }
    if (_current_data_file->first_row_id < 0) {
        return Status::OK();
    }
    if (_current_batch_row_positions.size() != block->rows()) {
        return Status::InternalError("_row_id requires current batch row positions");
    }
    auto column = block->get_by_position(pos).column->assume_mutable();
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(column.get());
        nullable_column != nullptr) {
        auto& null_map = nullable_column->get_null_map_data();
        auto& data = assert_cast<ColumnInt64&>(nullable_column->get_nested_column()).get_data();
        DCHECK_EQ(null_map.size(), block->rows());
        DCHECK_EQ(data.size(), block->rows());
        for (size_t i = 0; i < block->rows(); ++i) {
            null_map[i] = 0;
            data[i] = _current_data_file->first_row_id + _current_batch_row_positions[i];
        }
    } else {
        auto& data = assert_cast<ColumnInt64&>(*column).get_data();
        DCHECK_EQ(data.size(), block->rows());
        for (size_t i = 0; i < block->rows(); ++i) {
            data[i] = _current_data_file->first_row_id + _current_batch_row_positions[i];
        }
    }
    return Status::OK();
}

Status IcebergTableReader::materialize_last_updated_sequence_number(Block* block) {
    if (block == nullptr || _current_data_file == nullptr) {
        return Status::OK();
    }
    if (_row_lineage_last_updated_sequence_number_from_file) {
        return Status::OK();
    }
    int pos = block->get_position_by_name(ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER);
    if (pos < 0) {
        return Status::OK();
    }
    if (_current_data_file->last_updated_sequence_number >= 0) {
        auto column = block->get_by_position(pos).column->assume_mutable();
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(column.get());
            nullable_column != nullptr) {
            auto& null_map = nullable_column->get_null_map_data();
            auto& data = assert_cast<ColumnInt64&>(nullable_column->get_nested_column()).get_data();
            DCHECK_EQ(null_map.size(), block->rows());
            DCHECK_EQ(data.size(), block->rows());
            for (size_t i = 0; i < block->rows(); ++i) {
                null_map[i] = 0;
                data[i] = _current_data_file->last_updated_sequence_number;
            }
        } else {
            auto& data = assert_cast<ColumnInt64&>(*column).get_data();
            DCHECK_EQ(data.size(), block->rows());
            std::fill(data.begin(), data.end(), _current_data_file->last_updated_sequence_number);
        }
    }
    return Status::OK();
}

Status IcebergTableReader::clear_task_state() {
    _current_iceberg_task = nullptr;
    _current_data_file = nullptr;
    _position_delete_rows.reset();
    _need_row_positions = false;
    _current_batch_row_positions.clear();
    _equality_delete_blocks.clear();
    _equality_delete_field_ids.clear();
    _equality_delete_impls.clear();
    _id_to_block_column_name.clear();
    _equality_helper_column_names.clear();
    _row_lineage_row_id_from_file = false;
    _row_lineage_last_updated_sequence_number_from_file = false;
    return Status::OK();
}

void IcebergTableReader::update_row_lineage_file_column_state() {
    _row_lineage_row_id_from_file = false;
    _row_lineage_last_updated_sequence_number_from_file = false;
    const auto& mappings = _data_reader.column_mapper.mappings();
    DCHECK_EQ(mappings.size(), _options.projected_columns.size());
    for (size_t i = 0; i < mappings.size(); ++i) {
        if (!mappings[i].file_column_id.has_value()) {
            continue;
        }
        const auto& column = _options.projected_columns[i];
        if (column.name == ROW_LINEAGE_ROW_ID) {
            _row_lineage_row_id_from_file = true;
        } else if (column.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
            _row_lineage_last_updated_sequence_number_from_file = true;
        }
    }
}

std::map<int32_t, reader::TableFilter> IcebergTableReader::file_pushdown_filters() const {
    std::map<int32_t, reader::TableFilter> filters;
    for (const auto& [column_id, filter] : _table_filters) {
        const auto* column = find_projected_column(column_id);
        if (column != nullptr && is_row_lineage_column(*column)) {
            continue;
        }
        filters.emplace(column_id, filter);
    }
    return filters;
}

bool IcebergTableReader::needs_row_positions() const {
    return has_projected_column(BeConsts::ICEBERG_ROWID_COL) ||
           (has_projected_column(ROW_LINEAGE_ROW_ID) && !_row_lineage_row_id_from_file);
}

bool IcebergTableReader::has_projected_column(const std::string& name) const {
    return std::any_of(_options.projected_columns.begin(), _options.projected_columns.end(),
                       [&name](const reader::TableColumn& column) { return column.name == name; });
}

bool IcebergTableReader::is_row_lineage_column(const reader::TableColumn& column) const {
    return column.name == ROW_LINEAGE_ROW_ID || column.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER;
}

const reader::TableColumn* IcebergTableReader::find_projected_column(
        reader::ColumnId field_id) const {
    for (const auto& column : _options.projected_columns) {
        if (column.id == field_id) {
            return &column;
        }
    }
    return nullptr;
}

Status IcebergTableReader::ensure_equality_key_column(
        reader::ColumnId field_id, const std::vector<reader::SchemaField>& file_schema,
        reader::FileScanRequest* request) {
    if (request == nullptr) {
        return Status::InvalidArgument("file scan request is null");
    }
    const auto* file_field =
            _data_reader.column_mapper.find_file_field_by_table_column_id(field_id, file_schema);
    if (file_field == nullptr) {
        return Status::NotSupported(
                "equality delete field id {} is not present in current data file", field_id);
    }
    if (!file_field->children.empty()) {
        return Status::NotSupported("complex equality delete field id {} is not supported",
                                    field_id);
    }

    if (const auto* projected_column = find_projected_column(field_id);
        projected_column != nullptr) {
        _id_to_block_column_name[field_id] = projected_column->name;
    } else {
        std::string helper_name =
                std::string(EQ_DELETE_HELPER_COLUMN_PREFIX) + std::to_string(field_id);
        if (has_projected_column(helper_name)) {
            return Status::InternalError("Equality helper column '{}' conflicts with output column",
                                         helper_name);
        }
        if (_equality_helper_column_names.insert(helper_name).second) {
            reader::SchemaField helper_field;
            helper_field.id = file_field->id;
            helper_field.name = helper_name;
            helper_field.type = file_field->type;
            helper_field.children = file_field->children;
            helper_field.field_id = file_field->field_id;
            _data_reader.block_schema.push_back(std::move(helper_field));
        }
        _id_to_block_column_name[field_id] = helper_name;
    }

    add_unique_column(&request->non_predicate_columns, file_field->id);
    add_unique_column(&request->projected_columns, file_field->id);
    return Status::OK();
}

} // namespace doris::iceberg
