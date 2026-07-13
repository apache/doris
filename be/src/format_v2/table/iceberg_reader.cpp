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

#include "format_v2/table/iceberg_reader.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <utility>

#include "common/cast_set.h"
#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "exprs/vslot_ref.h"
#include "format/table/deletion_vector_reader.h"
#include "format_v2/expr/cast.h"
#include "format_v2/expr/equality_delete_predicate.h"
#include "format_v2/orc/orc_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/table_reader.h"
#include "io/file_factory.h"

namespace doris::format::iceberg {

static constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
static constexpr int32_t ROW_LINEAGE_ROW_ID_FIELD_ID = 2147483540;

template <typename T>
static std::string join_values_for_debug(const std::vector<T>& values) {
    std::ostringstream out;
    out << "[";
    for (size_t idx = 0; idx < values.size(); ++idx) {
        if (idx > 0) {
            out << ", ";
        }
        out << values[idx];
    }
    out << "]";
    return out.str();
}

static bool is_projected_row_lineage_row_id(const format::ColumnDefinition& column) {
    // Iceberg row lineage columns can be bound by field id when a mapper has already been built,
    // but customize_file_scan_request() is also exercised directly by scan-request tests before the
    // mapper exists. In that path, inspect the projected table schema so row-position dependencies
    // are still added for `_row_id`.
    return column.name == ROW_LINEAGE_ROW_ID ||
           (column.has_identifier_field_id() &&
            column.get_identifier_field_id() == ROW_LINEAGE_ROW_ID_FIELD_ID);
}

static bool is_projected_iceberg_rowid(const format::ColumnDefinition& column) {
    return column.name == BeConsts::ICEBERG_ROWID_COL;
}

static std::string iceberg_delete_file_debug_string(const TIcebergDeleteFileDesc& delete_file) {
    std::ostringstream out;
    out << "TIcebergDeleteFileDesc{path=" << (delete_file.__isset.path ? delete_file.path : "null")
        << ", content=" << (delete_file.__isset.content ? delete_file.content : -1)
        << ", file_format="
        << (delete_file.__isset.file_format ? static_cast<int>(delete_file.file_format) : -1)
        << ", position_lower_bound="
        << (delete_file.__isset.position_lower_bound ? delete_file.position_lower_bound : -1)
        << ", position_upper_bound="
        << (delete_file.__isset.position_upper_bound ? delete_file.position_upper_bound : -1)
        << ", field_ids="
        << (delete_file.__isset.field_ids ? join_values_for_debug(delete_file.field_ids) : "[]")
        << ", content_offset="
        << (delete_file.__isset.content_offset ? delete_file.content_offset : -1)
        << ", content_size_in_bytes="
        << (delete_file.__isset.content_size_in_bytes ? delete_file.content_size_in_bytes : -1)
        << "}";
    return out.str();
}

static std::string iceberg_delete_files_debug_string(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::ostringstream out;
    out << "[";
    for (size_t idx = 0; idx < delete_files.size(); ++idx) {
        if (idx > 0) {
            out << ", ";
        }
        out << iceberg_delete_file_debug_string(delete_files[idx]);
    }
    out << "]";
    return out.str();
}

static std::string iceberg_params_debug_string(const std::optional<TIcebergFileDesc>& params) {
    if (!params.has_value()) {
        return "null";
    }
    const auto& iceberg_params = *params;
    std::ostringstream out;
    out << "TIcebergFileDesc{format_version="
        << (iceberg_params.__isset.format_version ? iceberg_params.format_version : -1)
        << ", content=" << (iceberg_params.__isset.content ? iceberg_params.content : -1)
        << ", original_file_path="
        << (iceberg_params.__isset.original_file_path ? iceberg_params.original_file_path : "null")
        << ", row_count=" << (iceberg_params.__isset.row_count ? iceberg_params.row_count : -1)
        << ", partition_spec_id="
        << (iceberg_params.__isset.partition_spec_id ? iceberg_params.partition_spec_id : 0)
        << ", has_partition_data_json=" << iceberg_params.__isset.partition_data_json
        << ", first_row_id="
        << (iceberg_params.__isset.first_row_id ? iceberg_params.first_row_id : -1)
        << ", last_updated_sequence_number="
        << (iceberg_params.__isset.last_updated_sequence_number
                    ? iceberg_params.last_updated_sequence_number
                    : -1)
        << ", delete_file_count="
        << (iceberg_params.__isset.delete_files ? iceberg_params.delete_files.size() : 0)
        << ", delete_files="
        << (iceberg_params.__isset.delete_files
                    ? iceberg_delete_files_debug_string(iceberg_params.delete_files)
                    : "[]")
        << "}";
    return out.str();
}

IcebergTableReader::PositionDeleteRowsCollector::PositionDeleteRowsCollector(
        std::string data_file_path, format::DeleteRows* rows)
        : _data_file_path(std::move(data_file_path)), _rows(rows) {}

Status IcebergTableReader::PositionDeleteRowsCollector::collect(const Block& block,
                                                                size_t read_rows) {
    if (read_rows == 0) {
        return Status::OK();
    }
    const auto& file_path_column = assert_cast<const ColumnString&>(
            *remove_nullable((block.get_by_position(ICEBERG_FILE_PATH_BLOCK_POSITION).column)));
    const auto& pos_column = assert_cast<const ColumnInt64&>(
            *remove_nullable(block.get_by_position(ICEBERG_ROW_POS_BLOCK_POSITION).column));
    for (size_t row = 0; row < read_rows; ++row) {
        const auto file_path = file_path_column.get_data_at(row).to_string();
        if (file_path == _data_file_path) {
            _rows->push_back(pos_column.get_element(row));
        }
    }
    return Status::OK();
}

Status IcebergTableReader::prepare_split(const format::SplitReadOptions& options) {
    _row_lineage_columns = {};
    _iceberg_params.reset();
    _delete_predicates_initialized = false;
    _position_delete_rows_storage.clear();
    _equality_delete_filters.clear();
    if (options.current_range.__isset.table_format_params &&
        options.current_range.table_format_params.__isset.iceberg_params) {
        const auto& iceberg_params = options.current_range.table_format_params.iceberg_params;
        _iceberg_params = iceberg_params;
        if (iceberg_params.__isset.first_row_id) {
            _row_lineage_columns.first_row_id = iceberg_params.first_row_id;
        }
        if (iceberg_params.__isset.last_updated_sequence_number) {
            _row_lineage_columns.last_updated_sequence_number =
                    iceberg_params.last_updated_sequence_number;
        }
    }
    RETURN_IF_ERROR(TableReader::prepare_split(options));
    if (current_split_pruned()) {
        return Status::OK();
    }
    if (_is_table_level_count_active()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_init_delete_predicates(options.current_range.table_format_params));
    return Status::OK();
}

std::string IcebergTableReader::debug_string() const {
    size_t position_delete_file_count = 0;
    size_t equality_delete_file_count = 0;
    size_t deletion_vector_file_count = 0;
    if (_iceberg_params.has_value() && _iceberg_params->__isset.delete_files) {
        for (const auto& delete_file : _iceberg_params->delete_files) {
            if (!delete_file.__isset.content) {
                continue;
            }
            if (delete_file.content == POSITION_DELETE) {
                ++position_delete_file_count;
            } else if (delete_file.content == EQUALITY_DELETE) {
                ++equality_delete_file_count;
            } else if (delete_file.content == DELETION_VECTOR) {
                ++deletion_vector_file_count;
            }
        }
    }

    std::ostringstream equality_filters;
    equality_filters << "[";
    for (size_t idx = 0; idx < _equality_delete_filters.size(); ++idx) {
        if (idx > 0) {
            equality_filters << ", ";
        }
        const auto& filter = _equality_delete_filters[idx];
        equality_filters << "EqualityDeleteFilter{field_ids="
                         << join_values_for_debug(filter.field_ids) << ", key_types=[";
        for (size_t type_idx = 0; type_idx < filter.key_types.size(); ++type_idx) {
            if (type_idx > 0) {
                equality_filters << ", ";
            }
            equality_filters << (filter.key_types[type_idx] == nullptr
                                         ? "null"
                                         : filter.key_types[type_idx]->get_name());
        }
        equality_filters << "], delete_block_rows=" << filter.delete_block.rows()
                         << ", delete_block_columns=" << filter.delete_block.columns() << "}";
    }
    equality_filters << "]";

    std::ostringstream out;
    out << "IcebergTableReader{base=" << format::TableReader::debug_string()
        << ", iceberg_params=" << iceberg_params_debug_string(_iceberg_params)
        << ", row_lineage_first_row_id=" << _row_lineage_columns.first_row_id
        << ", row_lineage_last_updated_sequence_number="
        << _row_lineage_columns.last_updated_sequence_number
        << ", need_row_lineage_row_id=" << _need_row_lineage_row_id()
        << ", need_iceberg_rowid=" << _need_iceberg_rowid()
        << ", row_position_block_position=" << _row_position_block_position
        << ", delete_predicates_initialized=" << _delete_predicates_initialized
        << ", position_delete_file_count=" << position_delete_file_count
        << ", equality_delete_file_count=" << equality_delete_file_count
        << ", deletion_vector_file_count=" << deletion_vector_file_count
        << ", position_delete_rows_storage_count=" << _position_delete_rows_storage.size()
        << ", equality_delete_filter_count=" << _equality_delete_filters.size()
        << ", equality_delete_filters=" << equality_filters.str() << "}";
    return out.str();
}

Status IcebergTableReader::materialize_virtual_columns(Block* table_block) {
    for (size_t column_idx = 0; column_idx < _data_reader.column_mapper->mappings().size();
         ++column_idx) {
        const auto& mapping = _data_reader.column_mapper->mappings()[column_idx];
        switch (mapping.virtual_column_type) {
        case format::TableVirtualColumnType::ROW_ID:
            RETURN_IF_ERROR(_materialize_row_lineage_row_id(table_block, column_idx));
            break;
        case format::TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER:
            RETURN_IF_ERROR(
                    _materialize_row_lineage_last_updated_sequence_number(table_block, column_idx));
            break;
        case format::TableVirtualColumnType::ICEBERG_ROWID:
            RETURN_IF_ERROR(_materialize_iceberg_rowid(table_block, column_idx));
            break;
        case format::TableVirtualColumnType::INVALID:
            break;
        }
    }
    return Status::OK();
}

Status IcebergTableReader::customize_file_scan_request(format::FileScanRequest* file_request) {
    RETURN_IF_ERROR(TableReader::customize_file_scan_request(file_request));
    if ((_row_lineage_columns.first_row_id >= 0 && _need_row_lineage_row_id()) ||
        _need_iceberg_rowid()) {
        RETURN_IF_ERROR(_append_row_position_output_column(file_request));
    }
    RETURN_IF_ERROR(_append_equality_delete_predicates(file_request));
    return Status::OK();
}

bool IcebergTableReader::_supports_aggregate_pushdown(TPushAggOp::type agg_type) const {
    if (!TableReader::_supports_aggregate_pushdown(agg_type)) {
        return false;
    }
    return _equality_delete_filters.empty();
}

Status IcebergTableReader::_parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                                       DeleteFileDesc* desc,
                                                       bool* has_delete_file) {
    DORIS_CHECK(desc != nullptr);
    DORIS_CHECK(has_delete_file != nullptr);
    *has_delete_file = false;
    if (!t_desc.__isset.iceberg_params) {
        return Status::OK();
    }
    const auto& iceberg_params = t_desc.iceberg_params;
    if (!iceberg_params.__isset.format_version ||
        iceberg_params.format_version < MIN_SUPPORT_DELETE_FILES_VERSION ||
        !iceberg_params.__isset.delete_files || iceberg_params.delete_files.empty()) {
        return Status::OK();
    }

    const TIcebergDeleteFileDesc* deletion_vector = nullptr;
    for (const auto& delete_file : iceberg_params.delete_files) {
        if (!delete_file.__isset.content || delete_file.content != DELETION_VECTOR) {
            continue;
        }
        if (deletion_vector != nullptr) {
            return Status::DataQualityError("This iceberg data file has multiple DVs.");
        }
        deletion_vector = &delete_file;
    }
    if (deletion_vector == nullptr) {
        return Status::OK();
    }
    if (!deletion_vector->__isset.content_offset ||
        !deletion_vector->__isset.content_size_in_bytes) {
        return Status::InternalError("Deletion vector is missing content offset or length");
    }

    const std::string data_file_path = iceberg_params.__isset.original_file_path
                                               ? iceberg_params.original_file_path
                                               : _data_file_path();
    desc->key = build_iceberg_deletion_vector_cache_key(data_file_path, *deletion_vector);
    desc->path = deletion_vector->path;
    desc->start_offset = deletion_vector->content_offset;
    desc->size = deletion_vector->content_size_in_bytes;
    desc->file_size = -1;
    desc->format = DeleteFileDesc::Format::ICEBERG;
    *has_delete_file = true;
    return Status::OK();
}

Status IcebergTableReader::_init_delete_predicates(const TTableFormatFileDesc& t_desc) {
    if (!t_desc.__isset.iceberg_params || _delete_predicates_initialized) {
        _delete_predicates_initialized = true;
        return Status::OK();
    }
    const auto& iceberg_params = t_desc.iceberg_params;
    if (!iceberg_params.__isset.format_version ||
        iceberg_params.format_version < MIN_SUPPORT_DELETE_FILES_VERSION ||
        !iceberg_params.__isset.delete_files || iceberg_params.delete_files.empty()) {
        _delete_predicates_initialized = true;
        return Status::OK();
    }

    std::vector<TIcebergDeleteFileDesc> position_delete_files;
    std::vector<TIcebergDeleteFileDesc> equality_delete_files;
    for (const auto& delete_file : iceberg_params.delete_files) {
        if (!delete_file.__isset.content) {
            continue;
        }
        if (delete_file.content == POSITION_DELETE) {
            position_delete_files.push_back(delete_file);
        } else if (delete_file.content == EQUALITY_DELETE) {
            equality_delete_files.push_back(delete_file);
        }
    }
    // Per Iceberg scan planning, position delete files apply only when there is no deletion vector
    // for the data file. DVs and position deletes now intentionally use different in-memory
    // representations, so use the Roaring pointer as the DV sentinel.
    if (_deletion_vector != nullptr) {
        position_delete_files.clear();
    }
    // Initialize position and equality delete predicates. Position delete files contain row
    // positions of deleted rows, which can be directly added to `_delete_rows`. Equality delete
    // files contain values of deleted rows, which require reading the files and building
    // predicates for later filtering.
    if (!position_delete_files.empty()) {
        RETURN_IF_ERROR(_init_position_delete_rows(position_delete_files));
    }
    if (!equality_delete_files.empty()) {
        RETURN_IF_ERROR(_init_equality_delete_predicates(equality_delete_files));
    }

    _delete_predicates_initialized = true;
    return Status::OK();
}

std::shared_ptr<io::FileSystemProperties> IcebergTableReader::_delete_file_system_properties(
        const TFileScanRangeParams& scan_params) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type =
            scan_params.__isset.file_type ? scan_params.file_type : TFileType::FILE_LOCAL;
    system_properties->properties = scan_params.properties;
    system_properties->hdfs_params = scan_params.hdfs_params;
    if (scan_params.__isset.broker_addresses) {
        system_properties->broker_addresses.assign(scan_params.broker_addresses.begin(),
                                                   scan_params.broker_addresses.end());
    }
    return system_properties;
}

std::unique_ptr<io::FileDescription> IcebergTableReader::_delete_file_description(
        const TFileRangeDesc& range) {
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = range.path;
    file_description->file_size = range.__isset.file_size ? range.file_size : -1;
    file_description->range_start_offset = range.__isset.start_offset ? range.start_offset : 0;
    file_description->range_size = range.__isset.size ? range.size : -1;
    if (range.__isset.fs_name) {
        file_description->fs_name = range.fs_name;
    }
    return file_description;
}

std::string IcebergTableReader::_data_file_path() const {
    if (_iceberg_params.has_value() && _iceberg_params->__isset.original_file_path) {
        return _iceberg_params->original_file_path;
    }
    DORIS_CHECK(_current_task != nullptr);
    DORIS_CHECK(_current_task->data_file != nullptr);
    return _current_task->data_file->path;
}

Status IcebergTableReader::_append_row_position_output_column(format::FileScanRequest* request) {
    const auto row_position_column_id = format::LocalColumnId(format::ROW_POSITION_COLUMN_ID);
    _append_file_scan_column(request, row_position_column_id, &request->non_predicate_columns);
    _row_position_block_position = request->local_positions.at(row_position_column_id).value();
    return Status::OK();
}

Status IcebergTableReader::_append_equality_delete_predicates(format::FileScanRequest* request) {
    DORIS_CHECK(request != nullptr);
    for (const auto& filter : _equality_delete_filters) {
        auto delete_predicate =
                std::make_shared<EqualityDeletePredicate>(filter.delete_block, filter.field_ids);
        DCHECK_EQ(filter.field_ids.size(), filter.key_types.size());
        for (size_t idx = 0; idx < filter.field_ids.size(); ++idx) {
            const int field_id = filter.field_ids[idx];
            auto field_it = std::ranges::find_if(
                    _data_reader.file_schema, [field_id](const format::ColumnDefinition& field) {
                        return field.has_identifier_field_id() &&
                               field.get_identifier_field_id() == field_id;
                    });
            if (field_it == _data_reader.file_schema.end()) {
                return Status::InternalError(
                        "Can not find equality delete column field id {} in data file schema",
                        field_id);
            }
            const auto field_column_id = format::LocalColumnId(field_it->file_local_id());
            _append_file_scan_column(request, field_column_id, &request->predicate_columns);
            const auto block_position = request->local_positions.at(field_column_id).value();
            auto slot = VSlotRef::create_shared(cast_set<int>(block_position),
                                                cast_set<int>(block_position), -1, field_it->type,
                                                field_it->name);
            if (field_it->type->equals(*filter.key_types[idx])) {
                delete_predicate->add_child(std::move(slot));
            } else {
                auto cast_expr = Cast::create_shared(filter.key_types[idx]);
                cast_expr->add_child(std::move(slot));
                delete_predicate->add_child(std::move(cast_expr));
            }
        }
        request->delete_conjuncts.push_back(
                VExprContext::create_shared(std::move(delete_predicate)));
    }
    return Status::OK();
}

Status IcebergTableReader::_create_delete_file_reader(const TIcebergDeleteFileDesc& delete_file,
                                                      const TFileScanRangeParams& scan_params,
                                                      IcebergDeleteFileIOContext* delete_io_ctx,
                                                      std::unique_ptr<format::FileReader>* reader) {
    DORIS_CHECK(delete_io_ctx != nullptr);
    DORIS_CHECK(reader != nullptr);
    if (!delete_file.__isset.file_format) {
        return Status::InternalError("Iceberg delete file is missing file format");
    }
    if (delete_file.file_format != TFileFormatType::FORMAT_PARQUET &&
        delete_file.file_format != TFileFormatType::FORMAT_ORC) {
        return Status::NotSupported("Unsupported Iceberg delete file format {}",
                                    delete_file.file_format);
    }
    auto delete_range = build_iceberg_delete_file_range(delete_file.path);
    if (_current_task != nullptr && _current_task->data_file != nullptr &&
        !_current_task->data_file->fs_name.empty()) {
        delete_range.__set_fs_name(_current_task->data_file->fs_name);
    }
    auto system_properties = _delete_file_system_properties(scan_params);
    auto file_description = _delete_file_description(delete_range);
    std::shared_ptr<io::IOContext> io_ctx(&delete_io_ctx->io_ctx, [](io::IOContext*) {});
    if (delete_file.file_format == TFileFormatType::FORMAT_PARQUET) {
        *reader = std::make_unique<format::parquet::ParquetReader>(
                system_properties, file_description, io_ctx, _scanner_profile);
    } else {
        *reader = std::make_unique<format::orc::OrcReader>(system_properties, file_description,
                                                           io_ctx, _scanner_profile);
    }
    RETURN_IF_ERROR((*reader)->init(_runtime_state));
    return Status::OK();
}

Status IcebergTableReader::_read_position_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                                      const TFileScanRangeParams& scan_params,
                                                      IcebergDeleteFileIOContext* delete_io_ctx,
                                                      PositionDeleteRowsCollector* collector) {
    DORIS_CHECK(collector != nullptr);
    std::unique_ptr<format::FileReader> reader;
    RETURN_IF_ERROR(_create_delete_file_reader(delete_file, scan_params, delete_io_ctx, &reader));
    DORIS_CHECK(reader != nullptr);

    std::vector<format::ColumnDefinition> schema;
    RETURN_IF_ERROR(reader->get_schema(&schema));
    format::ColumnDefinition* file_path_field = nullptr;
    format::ColumnDefinition* pos_field = nullptr;
    for (auto& field : schema) {
        if (field.name == ICEBERG_FILE_PATH) {
            file_path_field = &field;
        } else if (field.name == ICEBERG_ROW_POS) {
            pos_field = &field;
        }
    }
    if (file_path_field == nullptr || pos_field == nullptr) {
        return Status::InternalError("Position delete file is missing required columns");
    }

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {
            format::LocalColumnIndex::top_level(
                    format::LocalColumnId(file_path_field->file_local_id())),
            format::LocalColumnIndex::top_level(format::LocalColumnId(pos_field->file_local_id()))};
    request->local_positions = {
            {format::LocalColumnId(file_path_field->file_local_id()),
             format::LocalIndex(ICEBERG_FILE_PATH_BLOCK_POSITION)},
            {format::LocalColumnId(pos_field->file_local_id()),
             format::LocalIndex(ICEBERG_ROW_POS_BLOCK_POSITION)},
    };
    RETURN_IF_ERROR(reader->open(request));

    bool eof = false;
    auto build_position_delete_block = [](const format::ColumnDefinition& file_path_field,
                                          const format::ColumnDefinition& pos_field) -> Block {
        Block block;
        block.insert(
                {file_path_field.type->create_column(), file_path_field.type, ICEBERG_FILE_PATH});
        block.insert({pos_field.type->create_column(), pos_field.type, ICEBERG_ROW_POS});
        return block;
    };
    while (!eof) {
        Block block = build_position_delete_block(*file_path_field, *pos_field);
        size_t read_rows = 0;
        RETURN_IF_ERROR(reader->get_block(&block, &read_rows, &eof));
        RETURN_IF_ERROR(collector->collect(block, read_rows));
    }
    return reader->close();
}

Status IcebergTableReader::_init_position_delete_rows(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    TFileScanRangeParams delete_scan_params =
            _scan_params == nullptr ? TFileScanRangeParams() : *_scan_params;
    format::DeleteRows position_delete_rows;
    IcebergDeleteFileIOContext delete_io_ctx(_runtime_state);
    PositionDeleteRowsCollector collector(_data_file_path(), &position_delete_rows);
    for (const auto& delete_file : delete_files) {
        RETURN_IF_ERROR(_read_position_delete_file(delete_file, delete_scan_params, &delete_io_ctx,
                                                   &collector));
    }
    if (position_delete_rows.empty()) {
        return Status::OK();
    }
    // Position delete files and deletion vectors both become row-position deletes for the
    // common TableReader DeletePredicate path. Keep the merged rows in a member vector because
    // DeletePredicate stores a reference to the vector used by _delete_rows.
    _position_delete_rows_storage.insert(_position_delete_rows_storage.end(),
                                         position_delete_rows.begin(), position_delete_rows.end());
    std::sort(_position_delete_rows_storage.begin(), _position_delete_rows_storage.end());
    _position_delete_rows_storage.erase(
            std::unique(_position_delete_rows_storage.begin(), _position_delete_rows_storage.end()),
            _position_delete_rows_storage.end());
    _delete_rows = &_position_delete_rows_storage;
    return Status::OK();
}

Status IcebergTableReader::_init_equality_delete_predicates(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    TFileScanRangeParams delete_scan_params =
            _scan_params == nullptr ? TFileScanRangeParams() : *_scan_params;
    IcebergDeleteFileIOContext delete_io_ctx(_runtime_state);
    for (const auto& delete_file : delete_files) {
        RETURN_IF_ERROR(
                _read_equality_delete_file(delete_file, delete_scan_params, &delete_io_ctx));
    }
    return Status::OK();
}

Status IcebergTableReader::_read_equality_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                                      const TFileScanRangeParams& scan_params,
                                                      IcebergDeleteFileIOContext* delete_io_ctx) {
    if (!delete_file.__isset.field_ids || delete_file.field_ids.empty()) {
        return Status::InternalError("Iceberg equality delete file is missing field ids");
    }
    std::unique_ptr<format::FileReader> reader;
    RETURN_IF_ERROR(_create_delete_file_reader(delete_file, scan_params, delete_io_ctx, &reader));
    DORIS_CHECK(reader != nullptr);

    std::vector<format::ColumnDefinition> schema;
    RETURN_IF_ERROR(reader->get_schema(&schema));
    std::vector<format::ColumnDefinition> delete_fields;
    std::vector<int> delete_field_ids;
    std::vector<DataTypePtr> delete_key_types;
    for (const auto field_id : delete_file.field_ids) {
        auto field_it = std::find_if(schema.begin(), schema.end(),
                                     [field_id](const format::ColumnDefinition& field) {
                                         return field.has_identifier_field_id() &&
                                                field_id == field.get_identifier_field_id();
                                     });
        if (field_it == schema.end()) {
            return Status::InternalError("Can not find field id {} in equality delete file {}",
                                         field_id, delete_file.path);
        }
        if (!field_it->children.empty()) {
            return Status::NotSupported(
                    "Iceberg equality delete does not support complex column {}", field_it->name);
        }
        delete_fields.push_back(*field_it);
        delete_field_ids.push_back(field_id);
        delete_key_types.push_back(field_it->type);
    }

    auto request = std::make_shared<format::FileScanRequest>();
    for (size_t idx = 0; idx < delete_fields.size(); ++idx) {
        const auto local_column_id = format::LocalColumnId(delete_fields[idx].file_local_id());
        request->non_predicate_columns.push_back(
                format::LocalColumnIndex::top_level(local_column_id));
        request->local_positions.emplace(local_column_id, format::LocalIndex(idx));
    }
    RETURN_IF_ERROR(reader->open(request));

    auto build_equality_delete_block =
            [](const std::vector<format::ColumnDefinition> fields) -> Block {
        Block block;
        for (const auto& field : fields) {
            block.insert({field.type->create_column(), field.type, field.name});
        }
        return block;
    };
    Block delete_block = build_equality_delete_block(delete_fields);
    MutableBlock mutable_delete_block(std::move(delete_block));
    bool eof = false;
    while (!eof) {
        Block block = build_equality_delete_block(delete_fields);
        size_t read_rows = 0;
        RETURN_IF_ERROR(reader->get_block(&block, &read_rows, &eof));
        if (read_rows > 0) {
            RETURN_IF_ERROR(mutable_delete_block.merge(block));
        }
    }
    RETURN_IF_ERROR(reader->close());
    delete_block = mutable_delete_block.to_block();
    _equality_delete_filters.push_back(
            EqualityDeleteFilter {.field_ids = std::move(delete_field_ids),
                                  .key_types = std::move(delete_key_types),
                                  .delete_block = std::move(delete_block)});
    return Status::OK();
}

Status IcebergTableReader::_materialize_row_lineage_row_id(Block* table_block, size_t column_idx) {
    if (_row_lineage_columns.first_row_id < 0) {
        return Status::OK();
    }
    DORIS_CHECK(_row_position_block_position < _data_reader.block_template.columns());
    const auto& row_position_column = assert_cast<const ColumnInt64&>(
            *_data_reader.block_template.get_by_position(_row_position_block_position).column);
    DORIS_CHECK(row_position_column.size() == table_block->rows());
    auto column = IColumn::mutate(
            table_block->get_by_position(column_idx).column->convert_to_full_column_if_const());
    auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
    auto& null_map = nullable_column->get_null_map_data();
    auto& data = assert_cast<ColumnInt64&>(*nullable_column->get_nested_column_ptr()).get_data();
    DORIS_CHECK(null_map.size() == row_position_column.size());
    DORIS_CHECK(data.size() == row_position_column.size());
    for (size_t row = 0; row < row_position_column.size(); ++row) {
        if (null_map[row]) {
            null_map[row] = 0;
            data[row] = _row_lineage_columns.first_row_id + row_position_column.get_element(row);
        }
    }
    table_block->replace_by_position(column_idx, std::move(column));
    return Status::OK();
}

Status IcebergTableReader::_materialize_iceberg_rowid(Block* table_block, size_t column_idx) {
    DORIS_CHECK(_row_position_block_position < _data_reader.block_template.columns());
    const auto& row_position_column = assert_cast<const ColumnInt64&>(
            *_data_reader.block_template.get_by_position(_row_position_block_position).column);
    DORIS_CHECK(row_position_column.size() == table_block->rows());

    const auto& type = table_block->get_by_position(column_idx).type;
    auto column = type->create_column();
    auto* nullable_column = check_and_get_column<ColumnNullable>(column.get());
    auto* struct_column = nullable_column != nullptr
                                  ? check_and_get_column<ColumnStruct>(
                                            nullable_column->get_nested_column_ptr().get())
                                  : check_and_get_column<ColumnStruct>(column.get());
    DORIS_CHECK(struct_column != nullptr);
    DORIS_CHECK(struct_column->tuple_size() >= 4);

    const auto rows = row_position_column.size();
    const auto file_path = _data_file_path();
    const int32_t partition_spec_id =
            _iceberg_params.has_value() && _iceberg_params->__isset.partition_spec_id
                    ? _iceberg_params->partition_spec_id
                    : 0;
    const std::string partition_data_json =
            _iceberg_params.has_value() && _iceberg_params->__isset.partition_data_json
                    ? _iceberg_params->partition_data_json
                    : "";

    auto& file_path_column = struct_column->get_column(0);
    auto& row_pos_column = struct_column->get_column(1);
    auto& spec_id_column = struct_column->get_column(2);
    auto& partition_data_column = struct_column->get_column(3);
    file_path_column.reserve(rows);
    row_pos_column.reserve(rows);
    spec_id_column.reserve(rows);
    partition_data_column.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        file_path_column.insert_data(file_path.data(), file_path.size());
        const int64_t row_pos = row_position_column.get_element(row);
        row_pos_column.insert_data(reinterpret_cast<const char*>(&row_pos), sizeof(row_pos));
        spec_id_column.insert_data(reinterpret_cast<const char*>(&partition_spec_id),
                                   sizeof(partition_spec_id));
        partition_data_column.insert_data(partition_data_json.data(), partition_data_json.size());
    }
    if (nullable_column != nullptr) {
        nullable_column->get_null_map_data().resize_fill(rows, 0);
    }
    table_block->replace_by_position(column_idx, std::move(column));
    return Status::OK();
}

Status IcebergTableReader::_materialize_row_lineage_last_updated_sequence_number(
        Block* table_block, size_t column_idx) {
    if (_row_lineage_columns.last_updated_sequence_number < 0) {
        return Status::OK();
    }
    auto column = IColumn::mutate(
            table_block->get_by_position(column_idx).column->convert_to_full_column_if_const());
    auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
    auto& null_map = nullable_column->get_null_map_data();
    auto& data = assert_cast<ColumnInt64&>(*nullable_column->get_nested_column_ptr()).get_data();
    DORIS_CHECK(null_map.size() == table_block->rows());
    DORIS_CHECK(data.size() == table_block->rows());
    for (size_t row = 0; row < table_block->rows(); ++row) {
        if (null_map[row]) {
            null_map[row] = 0;
            data[row] = _row_lineage_columns.last_updated_sequence_number;
        }
    }
    table_block->replace_by_position(column_idx, std::move(column));
    return Status::OK();
}

bool IcebergTableReader::_need_row_lineage_row_id() const {
    if (_data_reader.column_mapper != nullptr) {
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            if (mapping.virtual_column_type == format::TableVirtualColumnType::ROW_ID) {
                return true;
            }
        }
    }
    return std::ranges::any_of(_projected_columns, is_projected_row_lineage_row_id);
}

bool IcebergTableReader::_need_iceberg_rowid() const {
    if (_data_reader.column_mapper != nullptr) {
        for (const auto& mapping : _data_reader.column_mapper->mappings()) {
            if (mapping.virtual_column_type == format::TableVirtualColumnType::ICEBERG_ROWID) {
                return true;
            }
        }
    }
    return std::ranges::any_of(_projected_columns, is_projected_iceberg_rowid);
}

} // namespace doris::format::iceberg
