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

#include "format_v2/table/iceberg_position_delete_sys_table_reader.h"

#include <algorithm>
#include <optional>
#include <sstream>
#include <utility>

#include "common/cast_set.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/types.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "format/table/parquet_utils.h"
#include "format_v2/table/iceberg_schema_utils.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris::format::iceberg {

namespace {

constexpr const char* kFilePathColumn = "file_path";
constexpr const char* kPosColumn = "pos";
constexpr const char* kRowColumn = "row";
constexpr const char* kPartitionColumn = "partition";
constexpr const char* kSpecIdColumn = "spec_id";
constexpr const char* kDeleteFilePathColumn = "delete_file_path";
constexpr const char* kContentOffsetColumn = "content_offset";
constexpr const char* kContentSizeInBytesColumn = "content_size_in_bytes";
constexpr int kPositionDeleteContent = 1;
constexpr int32_t kDeleteFilePathFieldId = 2147483546;
constexpr int32_t kDeleteFilePosFieldId = 2147483545;
constexpr int32_t kDeleteFileRowFieldId = 2147483544;

bool block_has_row(const Block& block, size_t row) {
    return block.columns() > 0 && row < block.rows();
}

void insert_int64_nullable(MutableColumnPtr& column, const int64_t* value) {
    if (value == nullptr) {
        parquet_utils::insert_null(column);
    } else {
        parquet_utils::insert_int64(column, *value);
    }
}

// Fail loudly if the filled output block is malformed. Each output column must be produced by a
// file slot; a projected system-table column that is not backed by a file slot would be skipped
// during fill and left shorter than the rest, producing a block with inconsistent column lengths.
void check_output_columns_aligned(const MutableColumns& columns) {
    if (columns.empty()) {
        return;
    }
    const size_t expected_rows = columns.front()->size();
    for (const auto& column : columns) {
        DORIS_CHECK(column->size() == expected_rows)
                << "Iceberg position delete system table output block has inconsistent column "
                   "sizes; a projected column is not backed by a file slot";
    }
}

const IColumn* get_column(const Block& block, const std::string& name) {
    auto pos = block.get_position_by_name(name);
    if (pos < 0) {
        return nullptr;
    }
    return block.get_by_position(pos).column.get();
}

const IColumn* nested_column(const IColumn* column) {
    if (column == nullptr) {
        return nullptr;
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        return nullable->get_nested_column_ptr().get();
    }
    return column;
}

bool column_is_null_at(const IColumn* column, size_t row) {
    const auto* nullable = check_and_get_column<ColumnNullable>(column);
    return nullable != nullptr && nullable->is_null_at(row);
}

const ColumnString* get_string_column(const IColumn* column) {
    return check_and_get_column<ColumnString>(nested_column(column));
}

const ColumnInt64* get_int64_column(const IColumn* column) {
    return check_and_get_column<ColumnInt64>(nested_column(column));
}

ColumnDefinition build_delete_file_column(const std::string& name, DataTypePtr type) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_STRING>(name);
    column.name = name;
    column.type = std::move(type);
    return column;
}

void set_iceberg_delete_field_id(ColumnDefinition* column) {
    DORIS_CHECK(column != nullptr);
    if (column->name == kFilePathColumn) {
        column->identifier = Field::create_field<TYPE_INT>(kDeleteFilePathFieldId);
    } else if (column->name == kPosColumn) {
        column->identifier = Field::create_field<TYPE_INT>(kDeleteFilePosFieldId);
    } else if (column->name == kRowColumn && !column->has_identifier_field_id()) {
        column->identifier = Field::create_field<TYPE_INT>(kDeleteFileRowFieldId);
    }
}

class PositionDeleteFileTableReader final : public format::TableReader {
protected:
    format::TableColumnMappingMode mapping_mode() const override {
        const bool has_field_ids = supports_iceberg_scan_semantics_v1(_scan_params)
                                           ? schema_has_any_field_id(_data_reader.file_schema)
                                           : schema_has_all_field_ids(_data_reader.file_schema);
        if (!_data_reader.file_schema.empty() && has_field_ids) {
            return format::TableColumnMappingMode::BY_FIELD_ID;
        }
        return format::TableColumnMappingMode::BY_NAME;
    }

    void configure_mapper_options(format::TableColumnMapperOptions* options) const override {
        // Parquet may preserve a selected complex wrapper without its own ID; position-delete row
        // projection must use the same descendant-ID fallback as ordinary Iceberg data scans.
        options->allow_idless_complex_wrapper_projection =
                supports_iceberg_scan_semantics_v1(_scan_params) &&
                _format == format::FileFormat::PARQUET;
    }
};

} // namespace

IcebergPositionDeleteSysTableV2Reader::~IcebergPositionDeleteSysTableV2Reader() = default;

Status IcebergPositionDeleteSysTableV2Reader::prepare_split(
        const format::SplitReadOptions& options) {
    RETURN_IF_ERROR(close());
    RETURN_IF_ERROR(format::TableReader::prepare_split(options));
    _current_range = options.current_range;
    _has_split = true;
    return _init_split();
}

Status IcebergPositionDeleteSysTableV2Reader::get_block(Block* block, bool* eos) {
    SCOPED_TIMER(_profile.exec_timer);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(block->columns() == _projected_columns.size());
    block->clear_column_data(_projected_columns.size());

    if (*eos) {
        return Status::OK();
    }
    if (_io_ctx != nullptr && _io_ctx->should_stop) {
        *eos = true;
        return Status::OK();
    }
    if (!_has_split) {
        *eos = true;
        return Status::OK();
    }

    size_t read_rows = 0;
    if (_delete_file_kind == DeleteFileKind::DELETION_VECTOR) {
        return _append_deletion_vector_block(block, &read_rows, eos);
    }

    DORIS_CHECK(_position_reader != nullptr);
    if (_batch_size > 0) {
        _position_reader->set_batch_size(_batch_size);
    }

    while (true) {
        Block delete_block = _create_delete_block();
        bool position_reader_eof = false;
        RETURN_IF_ERROR(_position_reader->get_block(&delete_block, &position_reader_eof));
        const size_t delete_rows = delete_block.rows();
        if (delete_rows > 0) {
            RETURN_IF_ERROR(
                    _append_position_delete_block(block, delete_block, delete_rows, &read_rows));
            *eos = false;
            return Status::OK();
        }
        if (position_reader_eof) {
            RETURN_IF_ERROR(close());
            *eos = true;
            return Status::OK();
        }
    }
}

Status IcebergPositionDeleteSysTableV2Reader::close() {
    Status close_status = Status::OK();
    if (_position_reader != nullptr) {
        close_status = _position_reader->close();
        _position_reader.reset();
    }
    auto base_status = format::TableReader::close();
    if (!base_status.ok() && close_status.ok()) {
        close_status = std::move(base_status);
    }
    _iceberg_file_desc = nullptr;
    _delete_file_desc = nullptr;
    _read_columns.clear();
    _partition_value.reset();
    _next_dv_position.reset();
    _dv_positions = roaring::Roaring64Map();
    _has_split = false;
    return close_status;
}

std::string IcebergPositionDeleteSysTableV2Reader::debug_string() const {
    std::ostringstream out;
    out << "IcebergPositionDeleteSysTableV2Reader{base=" << format::TableReader::debug_string()
        << ", has_split=" << _has_split << ", delete_file_kind="
        << (_delete_file_kind == DeleteFileKind::DELETION_VECTOR ? "DELETION_VECTOR"
                                                                 : "POSITION_DELETE")
        << ", read_column_count=" << _read_columns.size()
        << ", dv_position_count=" << _dv_positions.cardinality() << "}";
    return out.str();
}

Status IcebergPositionDeleteSysTableV2Reader::_init_split() {
    if (_runtime_state == nullptr || _scanner_profile == nullptr || _scan_params == nullptr) {
        return Status::InvalidArgument(
                "invalid Iceberg position delete system table v2 reader context");
    }
    if (_file_slot_descs == nullptr) {
        return Status::InvalidArgument(
                "Iceberg position delete system table v2 reader requires file slot descriptors");
    }
    if (!_current_range.__isset.table_format_params ||
        !_current_range.table_format_params.__isset.iceberg_params) {
        return Status::InternalError("Iceberg position delete system table range misses params");
    }

    _iceberg_file_desc = &_current_range.table_format_params.iceberg_params;
    if (!_iceberg_file_desc->__isset.delete_files || _iceberg_file_desc->delete_files.size() != 1) {
        return Status::InternalError(
                "Iceberg position delete system table range should contain exactly one delete "
                "file");
    }
    _delete_file_desc = _iceberg_file_desc->delete_files.data();
    if (is_iceberg_deletion_vector(*_delete_file_desc)) {
        _delete_file_kind = DeleteFileKind::DELETION_VECTOR;
    } else if (_delete_file_desc->__isset.content &&
               _delete_file_desc->content == kPositionDeleteContent) {
        _delete_file_kind = DeleteFileKind::POSITION_DELETE;
    } else if (!_delete_file_desc->__isset.content) {
        return Status::InternalError(
                "Iceberg position delete system table delete file misses content");
    } else {
        return Status::InternalError(
                "Iceberg position delete system table does not support delete file content {}",
                _delete_file_desc->content);
    }

    if (_delete_file_kind == DeleteFileKind::DELETION_VECTOR) {
        return _init_deletion_vector_reader();
    }
    return _init_position_delete_reader();
}

Status IcebergPositionDeleteSysTableV2Reader::_init_position_delete_reader() {
    if (!_delete_file_desc->__isset.file_format) {
        return Status::InternalError("Iceberg position delete file misses file format");
    }
    format::FileFormat file_format;
    if (_delete_file_desc->file_format == TFileFormatType::FORMAT_PARQUET) {
        file_format = format::FileFormat::PARQUET;
    } else if (_delete_file_desc->file_format == TFileFormatType::FORMAT_ORC) {
        file_format = format::FileFormat::ORC;
    } else {
        return Status::NotSupported(
                "Iceberg position delete system table v2 reader only supports Parquet and ORC "
                "delete files, file_format={}",
                _delete_file_desc->file_format);
    }

    const bool read_row = _output_column_requested(kRowColumn);
    _init_read_columns(read_row);
    std::vector<ColumnDefinition> projected_columns;
    RETURN_IF_ERROR(_build_delete_file_projected_columns(&projected_columns));

    _position_reader = std::make_unique<PositionDeleteFileTableReader>();
    RETURN_IF_ERROR(_position_reader->init({
            .projected_columns = std::move(projected_columns),
            .conjuncts = {},
            .format = file_format,
            .scan_params = _scan_params,
            .io_ctx = _io_ctx,
            .runtime_state = _runtime_state,
            .scanner_profile = _scanner_profile,
            .file_slot_descs = nullptr,
            .push_down_agg_type = TPushAggOp::type::NONE,
            .condition_cache_digest = 0,
    }));
    // Keep standalone-reader defaults for scanner-only fields that may be added to
    // SplitReadOptions.
    auto split_options = format::SplitReadOptions {};
    split_options.partition_values = {};
    split_options.partition_prune_conjuncts = {};
    split_options.cache = nullptr;
    split_options.current_range = _current_range;
    split_options.current_split_format = file_format;
    split_options.global_rowid_context = std::nullopt;
    RETURN_IF_ERROR(_position_reader->prepare_split(split_options));
    return Status::OK();
}

Status IcebergPositionDeleteSysTableV2Reader::_init_deletion_vector_reader() {
    if (!_delete_file_desc->__isset.referenced_data_file_path ||
        _delete_file_desc->referenced_data_file_path.empty()) {
        return Status::InternalError("Iceberg deletion vector misses referenced data file path");
    }
    if (_io_ctx == nullptr) {
        return Status::InvalidArgument(
                "Iceberg position delete system table v2 reader requires IO context");
    }

    IcebergDeleteFileReaderOptions options;
    options.state = _runtime_state;
    options.profile = _scanner_profile;
    options.scan_params = _scan_params;
    options.io_ctx = _io_ctx.get();
    if (_current_range.__isset.fs_name) {
        options.fs_name = &_current_range.fs_name;
    }

    _dv_positions = roaring::Roaring64Map();
    RETURN_IF_ERROR(read_iceberg_deletion_vector(*_delete_file_desc, options, &_dv_positions));
    _next_dv_position.emplace(_dv_positions.begin());
    return Status::OK();
}

Status IcebergPositionDeleteSysTableV2Reader::_append_position_delete_block(
        Block* output_block, const Block& delete_block, size_t delete_rows, size_t* appended_rows) {
    auto columns_guard = output_block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    auto name_to_pos = output_block->get_name_to_pos_map();

    for (size_t row = 0; row < delete_rows; ++row) {
        for (const auto* slot : *_file_slot_descs) {
            auto it = name_to_pos.find(slot->col_name());
            if (it == name_to_pos.end()) {
                continue;
            }
            RETURN_IF_ERROR(_append_sys_column(columns[it->second], *slot, &delete_block, row, 0));
        }
    }
    check_output_columns_aligned(columns);
    *appended_rows = delete_rows;
    return Status::OK();
}

Status IcebergPositionDeleteSysTableV2Reader::_append_deletion_vector_block(Block* block,
                                                                            size_t* read_rows,
                                                                            bool* eof) {
    const size_t batch_size = std::max<size_t>(
            _batch_size > 0 ? _batch_size
                            : (_runtime_state == nullptr
                                       ? static_cast<size_t>(102400)
                                       : static_cast<size_t>(_runtime_state->batch_size())),
            1);
    if (!_next_dv_position.has_value() || *_next_dv_position == _dv_positions.end()) {
        *read_rows = 0;
        *eof = true;
        RETURN_IF_ERROR(close());
        return Status::OK();
    }

    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    auto name_to_pos = block->get_name_to_pos_map();

    size_t rows = 0;
    while (rows < batch_size && *_next_dv_position != _dv_positions.end()) {
        const uint64_t dv_pos = **_next_dv_position;
        for (const auto* slot : *_file_slot_descs) {
            auto it = name_to_pos.find(slot->col_name());
            if (it == name_to_pos.end()) {
                continue;
            }
            RETURN_IF_ERROR(_append_sys_column(columns[it->second], *slot, nullptr, 0, dv_pos));
        }
        ++(*_next_dv_position);
        ++rows;
    }
    check_output_columns_aligned(columns);
    *read_rows = rows;
    _record_scan_rows(rows);
    // FileScannerV2 treats eof=true as "advance to the next split" without returning the
    // current block. Keep eof false after appending rows and report EOF on the next empty call.
    *eof = false;
    return Status::OK();
}

Status IcebergPositionDeleteSysTableV2Reader::_append_sys_column(MutableColumnPtr& column,
                                                                 const SlotDescriptor& slot,
                                                                 const Block* delete_block,
                                                                 size_t source_row,
                                                                 uint64_t dv_pos) {
    const std::string& name = slot.col_name();
    if (name == kFilePathColumn) {
        if (_delete_file_kind == DeleteFileKind::DELETION_VECTOR) {
            parquet_utils::insert_string(column, _delete_file_desc->referenced_data_file_path);
            return Status::OK();
        }
        const auto* source_column = get_column(*delete_block, kFilePathColumn);
        const auto* path_column = get_string_column(source_column);
        if (path_column == nullptr || !block_has_row(*delete_block, source_row) ||
            column_is_null_at(source_column, source_row)) {
            return Status::InternalError("Iceberg position delete file_path column is missing");
        }
        parquet_utils::insert_string(column, path_column->get_data_at(source_row).to_string());
        return Status::OK();
    }

    if (name == kPosColumn) {
        if (_delete_file_kind == DeleteFileKind::DELETION_VECTOR) {
            parquet_utils::insert_int64(column, cast_set<Int64>(dv_pos));
            return Status::OK();
        }
        const auto* source_column = get_column(*delete_block, kPosColumn);
        const auto* pos_column = get_int64_column(source_column);
        if (pos_column == nullptr || !block_has_row(*delete_block, source_row) ||
            column_is_null_at(source_column, source_row)) {
            return Status::InternalError("Iceberg position delete pos column is missing");
        }
        parquet_utils::insert_int64(column, pos_column->get_element(source_row));
        return Status::OK();
    }

    if (name == kRowColumn) {
        if (delete_block != nullptr) {
            auto row_pos = delete_block->get_position_by_name(kRowColumn);
            if (row_pos >= 0) {
                auto row_column = delete_block->get_by_position(row_pos)
                                          .column->convert_to_full_column_if_const();
                if (source_row < row_column->size()) {
                    column->insert_from(*row_column, source_row);
                    return Status::OK();
                }
            }
        }
        parquet_utils::insert_null(column);
        return Status::OK();
    }

    if (name == kPartitionColumn) {
        return _append_partition_column(column, slot);
    }

    if (name == kSpecIdColumn) {
        if (_iceberg_file_desc->__isset.partition_spec_id) {
            parquet_utils::insert_int32(column,
                                        cast_set<Int32>(_iceberg_file_desc->partition_spec_id));
        } else {
            parquet_utils::insert_null(column);
        }
        return Status::OK();
    }

    if (name == kDeleteFilePathColumn) {
        parquet_utils::insert_string(column, _delete_file_output_path());
        return Status::OK();
    }

    if (name == kContentOffsetColumn) {
        const int64_t* value = _delete_file_desc->__isset.content_offset
                                       ? &_delete_file_desc->content_offset
                                       : nullptr;
        insert_int64_nullable(column, value);
        return Status::OK();
    }

    if (name == kContentSizeInBytesColumn) {
        const int64_t* value = _delete_file_desc->__isset.content_size_in_bytes
                                       ? &_delete_file_desc->content_size_in_bytes
                                       : nullptr;
        insert_int64_nullable(column, value);
        return Status::OK();
    }

    return Status::InternalError("Unknown Iceberg position delete system table column: {}", name);
}

Status IcebergPositionDeleteSysTableV2Reader::_append_partition_column(MutableColumnPtr& column,
                                                                       const SlotDescriptor& slot) {
    if (!_iceberg_file_desc->__isset.partition_data_json ||
        _iceberg_file_desc->partition_data_json.empty()) {
        parquet_utils::insert_null(column);
        return Status::OK();
    }

    if (!_partition_value) {
        auto partition_value = slot.get_data_type_ptr()->create_column();
        auto serde = slot.get_data_type_ptr()->get_serde();
        StringRef partition_data(_iceberg_file_desc->partition_data_json.data(),
                                 _iceberg_file_desc->partition_data_json.size());
        DataTypeSerDe::FormatOptions options = DataTypeSerDe::get_default_format_options();
        RETURN_IF_ERROR(serde->from_string(partition_data, *partition_value, options));
        DORIS_CHECK(partition_value->size() == 1);
        _partition_value = std::move(partition_value);
    }
    column->insert_from(*_partition_value, 0);
    return Status::OK();
}

Block IcebergPositionDeleteSysTableV2Reader::_create_delete_block() const {
    Block block;
    for (const auto& column : _read_columns) {
        block.insert(ColumnWithTypeAndName(column.type->create_column(), column.type, column.name));
    }
    return block;
}

bool IcebergPositionDeleteSysTableV2Reader::_output_column_requested(
        const std::string& name) const {
    return std::any_of(_file_slot_descs->begin(), _file_slot_descs->end(),
                       [&name](const SlotDescriptor* slot) { return slot->col_name() == name; });
}

void IcebergPositionDeleteSysTableV2Reader::_init_read_columns(bool read_row) {
    _read_columns.clear();
    _read_columns.push_back({kFilePathColumn, make_nullable(std::make_shared<DataTypeString>())});
    _read_columns.push_back({kPosColumn, make_nullable(std::make_shared<DataTypeInt64>())});
    if (read_row) {
        for (const auto* slot : *_file_slot_descs) {
            if (slot->col_name() == kRowColumn) {
                _read_columns.push_back({kRowColumn, slot->get_data_type_ptr()});
                break;
            }
        }
    }
}

Status IcebergPositionDeleteSysTableV2Reader::_build_delete_file_projected_columns(
        std::vector<ColumnDefinition>* columns) const {
    DORIS_CHECK(columns != nullptr);
    columns->clear();
    columns->reserve(_read_columns.size());
    for (const auto& column : _read_columns) {
        if (column.name == kRowColumn) {
            auto it = std::ranges::find_if(_projected_columns, [](const ColumnDefinition& field) {
                return field.name == kRowColumn;
            });
            if (it == _projected_columns.end()) {
                return Status::InternalError(
                        "Iceberg position delete system table row schema is missing");
            }
            columns->push_back(*it);
            columns->back().type = column.type;
            set_iceberg_delete_field_id(&columns->back());
            continue;
        }
        auto field = build_delete_file_column(column.name, column.type);
        set_iceberg_delete_field_id(&field);
        columns->push_back(std::move(field));
    }
    return Status::OK();
}

const std::string& IcebergPositionDeleteSysTableV2Reader::_delete_file_output_path() const {
    if (_delete_file_desc->__isset.original_path && !_delete_file_desc->original_path.empty()) {
        return _delete_file_desc->original_path;
    }
    return _delete_file_desc->path;
}

} // namespace doris::format::iceberg
