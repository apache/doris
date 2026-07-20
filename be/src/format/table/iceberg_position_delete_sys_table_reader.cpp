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

#include "format/table/iceberg_position_delete_sys_table_reader.h"

#include <gen_cpp/ExternalTableSchema_types.h>
#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "common/cast_set.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/types.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/parquet_utils.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

constexpr const char* kFilePathColumn = "file_path";
constexpr const char* kPosColumn = "pos";
constexpr const char* kRowColumn = "row";
constexpr const char* kPartitionColumn = "partition";
constexpr const char* kSpecIdColumn = "spec_id";
constexpr const char* kDeleteFilePathColumn = "delete_file_path";
constexpr const char* kContentOffsetColumn = "content_offset";
constexpr const char* kContentSizeInBytesColumn = "content_size_in_bytes";
constexpr const char* kIcebergOrcAttribute = "iceberg.id";
constexpr int kPositionDeleteContent = 1;

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

const ColumnString* get_string_column(const Block& block, const std::string& name) {
    auto pos = block.get_position_by_name(name);
    if (pos < 0) {
        return nullptr;
    }
    return check_and_get_column<ColumnString>(block.get_by_position(pos).column.get());
}

const ColumnInt64* get_int64_column(const Block& block, const std::string& name) {
    auto pos = block.get_position_by_name(name);
    if (pos < 0) {
        return nullptr;
    }
    return check_and_get_column<ColumnInt64>(block.get_by_position(pos).column.get());
}

const schema::external::TSchema* find_current_schema(const TFileScanRangeParams* params) {
    if (params == nullptr || !params->__isset.history_schema_info ||
        params->history_schema_info.empty()) {
        return nullptr;
    }
    const schema::external::TSchema* schema = &params->history_schema_info.front();
    if (params->__isset.current_schema_id) {
        for (const auto& candidate : params->history_schema_info) {
            if (candidate.__isset.schema_id && candidate.schema_id == params->current_schema_id) {
                schema = &candidate;
                break;
            }
        }
    }
    return schema;
}

template <typename ReadColumns>
std::shared_ptr<TableSchemaChangeHelper::StructNode> create_position_delete_root_node(
        const ReadColumns& read_columns) {
    auto root_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
    for (const auto& column : read_columns) {
        if (column.name == kRowColumn) {
            continue;
        }
        root_node->add_children(column.name, column.name,
                                TableSchemaChangeHelper::ConstNode::get_instance());
    }
    return root_node;
}

} // namespace

IcebergPositionDeleteSysTableReader::IcebergPositionDeleteSysTableReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TFileRangeDesc& range,
        const TFileScanRangeParams* range_params, std::shared_ptr<io::IOContext> io_ctx,
        FileMetaCache* meta_cache)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile),
          _range(range),
          _range_params(range_params),
          _io_ctx(std::move(io_ctx)) {
    _meta_cache = meta_cache;
}

IcebergPositionDeleteSysTableReader::~IcebergPositionDeleteSysTableReader() = default;

Status IcebergPositionDeleteSysTableReader::_do_init_reader(ReaderInitContext* /*ctx*/) {
    if (_state == nullptr || _profile == nullptr || _range_params == nullptr ||
        _io_ctx == nullptr) {
        return Status::InvalidArgument(
                "invalid Iceberg position delete system table reader context");
    }
    if (!_range.__isset.table_format_params || !_range.table_format_params.__isset.iceberg_params) {
        return Status::InternalError("Iceberg position delete system table range misses params");
    }

    _iceberg_file_desc = &_range.table_format_params.iceberg_params;
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
    _batch_size = _state->batch_size();

    if (_delete_file_kind == DeleteFileKind::DELETION_VECTOR) {
        return _init_deletion_vector_reader();
    }
    return _init_position_delete_reader();
}

Status IcebergPositionDeleteSysTableReader::_get_columns_impl(
        std::unordered_map<std::string, DataTypePtr>* name_to_type) {
    for (const auto* slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->get_data_type_ptr());
    }
    return Status::OK();
}

bool IcebergPositionDeleteSysTableReader::count_read_rows() {
    return _delete_file_kind == DeleteFileKind::POSITION_DELETE;
}

void IcebergPositionDeleteSysTableReader::_collect_profile_before_close() {
    if (_position_reader != nullptr) {
        _position_reader->collect_profile_before_close();
    }
}

Status IcebergPositionDeleteSysTableReader::close() {
    if (_position_reader != nullptr) {
        RETURN_IF_ERROR(_position_reader->close());
    }
    _partition_value.reset();
    _next_dv_position.reset();
    _dv_positions = roaring::Roaring64Map();
    return Status::OK();
}

Status IcebergPositionDeleteSysTableReader::_init_position_delete_reader() {
    if (!_delete_file_desc->__isset.file_format) {
        return Status::InternalError("Iceberg position delete file misses file format");
    }

    // `row` is optional in position delete files and expensive to read, so only read it when the
    // query actually projects it. Whether the delete file physically stores `row` is decided from
    // the reader's own footer/type below, reusing the same reader instance that is initialized
    // afterwards so the delete file is opened and its footer parsed only once.
    const bool row_requested = _output_column_requested(kRowColumn);

    if (_delete_file_desc->file_format == TFileFormatType::FORMAT_PARQUET) {
        auto parquet_reader =
                ParquetReader::create_unique(_profile, *_range_params, _range, _batch_size,
                                             &_state->timezone_obj(), _io_ctx, _state, _meta_cache);

        const FieldDescriptor* schema = nullptr;
        std::shared_ptr<TableSchemaChangeHelper::Node> mapped_file_schema;
        if (row_requested) {
            RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&schema));
            DORIS_CHECK(schema != nullptr);
            const auto* table_schema = find_current_schema(_range_params);
            if (table_schema == nullptr || !table_schema->__isset.root_field) {
                return Status::InternalError(
                        "Iceberg position delete system table row schema is missing");
            }
            // Position-delete mapping mode is file-wide: file_path/pos IDs must prevent an
            // ID-less physical row from being rebound by name in either reader generation.
            RETURN_IF_ERROR(TableSchemaChangeHelper::BuildTableInfoUtil::
                                    by_parquet_field_id_with_name_mapping(
                                            table_schema->root_field, *schema, mapped_file_schema));
        }
        const bool read_row =
                row_requested && mapped_file_schema->children_column_exists(kRowColumn);
        _init_read_columns(read_row);
        std::vector<std::string> read_column_names;
        read_column_names.reserve(_read_columns.size());
        for (const auto& column : _read_columns) {
            read_column_names.push_back(column.name);
        }

        ParquetInitContext pctx;
        pctx.column_names = read_column_names;
        pctx.col_name_to_block_idx = &_read_col_name_to_block_idx;
        pctx.state = _state;
        pctx.params = _range_params;
        pctx.range = &_range;
        pctx.filter_groups = false;
        if (read_row) {
            auto root_node = create_position_delete_root_node(_read_columns);
            root_node->add_children(kRowColumn,
                                    mapped_file_schema->children_file_column_name(kRowColumn),
                                    mapped_file_schema->get_children_node(kRowColumn));
            pctx.table_info_node = std::move(root_node);
        }
        RETURN_IF_ERROR(static_cast<GenericReader*>(parquet_reader.get())->init_reader(&pctx));
        _position_reader = std::move(parquet_reader);
        return Status::OK();
    }

    if (_delete_file_desc->file_format == TFileFormatType::FORMAT_ORC) {
        auto orc_reader =
                OrcReader::create_unique(_profile, _state, *_range_params, _range, _batch_size,
                                         _state->timezone(), _io_ctx, _meta_cache);

        std::shared_ptr<TableSchemaChangeHelper::Node> mapped_file_schema;
        if (row_requested) {
            const orc::Type* root_type = nullptr;
            RETURN_IF_ERROR(orc_reader->get_file_type(&root_type));
            DORIS_CHECK(root_type != nullptr);
            const auto* table_schema = find_current_schema(_range_params);
            if (table_schema == nullptr || !table_schema->__isset.root_field) {
                return Status::InternalError(
                        "Iceberg position delete system table row schema is missing");
            }
            // Resolve row against the complete delete-file type so top-level IDs keep ORC in ID
            // projection throughout the nested row subtree.
            RETURN_IF_ERROR(
                    TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id_with_name_mapping(
                            table_schema->root_field, root_type, kIcebergOrcAttribute,
                            mapped_file_schema));
        }
        const bool read_row =
                row_requested && mapped_file_schema->children_column_exists(kRowColumn);
        _init_read_columns(read_row);
        std::vector<std::string> read_column_names;
        read_column_names.reserve(_read_columns.size());
        for (const auto& column : _read_columns) {
            read_column_names.push_back(column.name);
        }

        OrcInitContext octx;
        octx.column_names = read_column_names;
        octx.col_name_to_block_idx = &_read_col_name_to_block_idx;
        octx.state = _state;
        octx.params = _range_params;
        octx.range = &_range;
        if (read_row) {
            auto root_node = create_position_delete_root_node(_read_columns);
            root_node->add_children(kRowColumn,
                                    mapped_file_schema->children_file_column_name(kRowColumn),
                                    mapped_file_schema->get_children_node(kRowColumn));
            octx.table_info_node = std::move(root_node);
        }
        RETURN_IF_ERROR(static_cast<GenericReader*>(orc_reader.get())->init_reader(&octx));
        _position_reader = std::move(orc_reader);
        return Status::OK();
    }

    return Status::NotSupported("Unsupported Iceberg position delete file format {}",
                                _delete_file_desc->file_format);
}

Status IcebergPositionDeleteSysTableReader::_init_deletion_vector_reader() {
    if (!_delete_file_desc->__isset.referenced_data_file_path ||
        _delete_file_desc->referenced_data_file_path.empty()) {
        return Status::InternalError("Iceberg deletion vector misses referenced data file path");
    }

    IcebergDeleteFileReaderOptions options;
    options.state = _state;
    options.profile = _profile;
    options.scan_params = _range_params;
    options.io_ctx = _io_ctx.get();
    options.meta_cache = _meta_cache;
    if (_range.__isset.fs_name) {
        options.fs_name = &_range.fs_name;
    }

    _dv_positions = roaring::Roaring64Map();
    RETURN_IF_ERROR(read_iceberg_deletion_vector(*_delete_file_desc, options, &_dv_positions));
    _next_dv_position.emplace(_dv_positions.begin());
    return Status::OK();
}

Status IcebergPositionDeleteSysTableReader::_do_get_next_block(Block* block, size_t* read_rows,
                                                               bool* eof) {
    DORIS_CHECK(_io_ctx != nullptr);
    if (_io_ctx->should_stop) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }

    if (_delete_file_kind == DeleteFileKind::DELETION_VECTOR) {
        return _append_deletion_vector_block(block, read_rows, eof);
    }

    if (_position_reader == nullptr) {
        return Status::InternalError("Iceberg position delete reader is not initialized");
    }

    while (true) {
        Block delete_block = _create_delete_block();
        size_t delete_rows = 0;
        bool position_reader_eof = false;
        RETURN_IF_ERROR(_position_reader->get_next_block(&delete_block, &delete_rows,
                                                         &position_reader_eof));
        if (delete_rows > 0) {
            RETURN_IF_ERROR(
                    _append_position_delete_block(block, delete_block, delete_rows, read_rows));
            *eof = position_reader_eof;
            return Status::OK();
        }
        if (position_reader_eof) {
            *read_rows = 0;
            *eof = true;
            return Status::OK();
        }
    }
}

Status IcebergPositionDeleteSysTableReader::_append_position_delete_block(Block* output_block,
                                                                          const Block& delete_block,
                                                                          size_t delete_rows,
                                                                          size_t* appended_rows) {
    auto columns_guard = output_block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    auto name_to_pos = output_block->get_name_to_pos_map();

    for (size_t row = 0; row < delete_rows; ++row) {
        for (const auto* slot : _file_slot_descs) {
            auto it = name_to_pos.find(slot->col_name());
            if (it == name_to_pos.end()) {
                continue;
            }
            RETURN_IF_ERROR(_append_sys_column(columns[it->second], *slot, &delete_block, row, 0));
        }
    }
    // Every output column must be produced by a file slot above; a projected system-table column
    // that is not backed by a file slot would be left short and yield a malformed block with
    // inconsistent column lengths.
    check_output_columns_aligned(columns);
    *appended_rows = delete_rows;
    return Status::OK();
}

Status IcebergPositionDeleteSysTableReader::_append_deletion_vector_block(Block* block,
                                                                          size_t* read_rows,
                                                                          bool* eof) {
    if (!_next_dv_position.has_value() || *_next_dv_position == _dv_positions.end()) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    const size_t rows_limit = std::max<size_t>(_batch_size, 1);

    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    auto name_to_pos = block->get_name_to_pos_map();

    size_t rows = 0;
    while (rows < rows_limit && *_next_dv_position != _dv_positions.end()) {
        const uint64_t dv_pos = **_next_dv_position;
        for (const auto* slot : _file_slot_descs) {
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
    *eof = *_next_dv_position == _dv_positions.end();
    return Status::OK();
}

Status IcebergPositionDeleteSysTableReader::_append_sys_column(MutableColumnPtr& column,
                                                               const SlotDescriptor& slot,
                                                               const Block* delete_block,
                                                               size_t source_row, uint64_t dv_pos) {
    const std::string& name = slot.col_name();
    if (name == kFilePathColumn) {
        if (_delete_file_kind == DeleteFileKind::DELETION_VECTOR) {
            parquet_utils::insert_string(column, _delete_file_desc->referenced_data_file_path);
            return Status::OK();
        }
        const auto* path_column = get_string_column(*delete_block, kFilePathColumn);
        if (path_column == nullptr || !block_has_row(*delete_block, source_row)) {
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
        const auto* pos_column = get_int64_column(*delete_block, kPosColumn);
        if (pos_column == nullptr || !block_has_row(*delete_block, source_row)) {
            return Status::InternalError("Iceberg position delete pos column is missing");
        }
        parquet_utils::insert_int64(column, pos_column->get_element(source_row));
        return Status::OK();
    }

    if (name == kRowColumn) {
        if (delete_block != nullptr) {
            auto row_pos = delete_block->get_position_by_name(kRowColumn);
            if (row_pos >= 0) {
                const auto& row_column = *delete_block->get_by_position(row_pos).column;
                if (source_row < row_column.size()) {
                    column->insert_from(row_column, source_row);
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

Status IcebergPositionDeleteSysTableReader::_append_partition_column(MutableColumnPtr& column,
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

Block IcebergPositionDeleteSysTableReader::_create_delete_block() const {
    Block block;
    for (const auto& column : _read_columns) {
        block.insert(ColumnWithTypeAndName(column.type->create_column(), column.type, column.name));
    }
    return block;
}

bool IcebergPositionDeleteSysTableReader::_output_column_requested(const std::string& name) const {
    return std::any_of(_file_slot_descs.begin(), _file_slot_descs.end(),
                       [&name](const SlotDescriptor* slot) { return slot->col_name() == name; });
}

void IcebergPositionDeleteSysTableReader::_init_read_columns(bool read_row) {
    _read_columns.clear();
    _read_columns.push_back({kFilePathColumn, std::make_shared<DataTypeString>()});
    _read_columns.push_back({kPosColumn, std::make_shared<DataTypeInt64>()});
    if (read_row) {
        for (const auto* slot : _file_slot_descs) {
            if (slot->col_name() == kRowColumn) {
                _read_columns.push_back({kRowColumn, slot->get_data_type_ptr()});
                break;
            }
        }
    }

    _read_col_name_to_block_idx.clear();
    for (size_t i = 0; i < _read_columns.size(); ++i) {
        _read_col_name_to_block_idx.emplace(_read_columns[i].name, cast_set<uint32_t>(i));
    }
}

const std::string& IcebergPositionDeleteSysTableReader::_delete_file_output_path() const {
    if (_delete_file_desc->__isset.original_path && !_delete_file_desc->original_path.empty()) {
        return _delete_file_desc->original_path;
    }
    return _delete_file_desc->path;
}

} // namespace doris
