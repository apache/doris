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

#include "vec/exec/format/table/parquet_metadata_reader.h"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <memory>
#include <unordered_map>
#include <utility>

#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/exec/format/parquet/parquet_thrift_util.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"
#include "vec/exec/format/table/parquet_utils.h"

namespace doris::vectorized {

using namespace parquet_utils;

class ParquetMetadataReader::ModeHandler {
public:
    explicit ModeHandler(RuntimeState* state) : _state(state) {}
    virtual ~ModeHandler() = default;

    virtual void init_slot_pos_map(const std::vector<SlotDescriptor*>& slots) = 0;
    virtual Status append_rows(const std::string& path, FileMetaData* metadata,
                               std::vector<MutableColumnPtr>& columns) = 0;

protected:
    RuntimeState* _state = nullptr;

    static std::unordered_map<std::string, int> _build_name_to_pos_map(
            const std::vector<SlotDescriptor*>& slots) {
        std::unordered_map<std::string, int> name_to_pos;
        name_to_pos.reserve(slots.size());
        for (size_t i = 0; i < slots.size(); ++i) {
            name_to_pos.emplace(to_lower(slots[i]->col_name()), static_cast<int>(i));
        }
        return name_to_pos;
    }

    template <size_t N>
    static void _init_slot_pos_map(const std::unordered_map<std::string, int>& name_to_pos,
                                   const std::array<const char*, N>& column_names,
                                   std::array<int, N>* slot_pos) {
        slot_pos->fill(-1);
        for (size_t i = 0; i < column_names.size(); ++i) {
            auto it = name_to_pos.find(column_names[i]);
            if (it != name_to_pos.end()) {
                (*slot_pos)[i] = it->second;
            }
        }
    }
};

class ParquetSchemaModeHandler final : public ParquetMetadataReader::ModeHandler {
public:
    explicit ParquetSchemaModeHandler(RuntimeState* state) : ModeHandler(state) {}

    void init_slot_pos_map(const std::vector<SlotDescriptor*>& slots) override {
        const auto& name_to_pos = _build_name_to_pos_map(slots);
        _init_slot_pos_map(name_to_pos, kSchemaColumnNames, &_slot_pos);
    }

    Status append_rows(const std::string& path, FileMetaData* metadata,
                       std::vector<MutableColumnPtr>& columns) override {
        const auto& fields = metadata->schema().get_fields_schema();
        for (const auto& field : fields) {
            RETURN_IF_ERROR(_append_schema_field(path, field, field.name, columns));
        }
        return Status::OK();
    }

private:
    std::array<int, SCHEMA_COLUMN_COUNT> _slot_pos {};

    Status _append_schema_field(const std::string& path, const FieldSchema& field,
                                const std::string& current_path,
                                std::vector<MutableColumnPtr>& columns) {
        if (!field.children.empty()) {
            for (const auto& child : field.children) {
                std::string child_path = current_path.empty()
                                                 ? child.name
                                                 : fmt::format("{}.{}", current_path, child.name);
                RETURN_IF_ERROR(_append_schema_field(path, child, child_path, columns));
            }
            return Status::OK();
        }

        auto insert_if_requested = [&](SchemaColumnIndex idx, auto&& inserter, auto&&... args) {
            int pos = _slot_pos[idx];
            if (pos >= 0) {
                inserter(columns[pos], std::forward<decltype(args)>(args)...);
            }
        };

        insert_if_requested(SCHEMA_FILE_NAME, insert_string, path);
        insert_if_requested(SCHEMA_COLUMN_NAME, insert_string, field.name);
        insert_if_requested(SCHEMA_COLUMN_PATH, insert_string, current_path);
        insert_if_requested(SCHEMA_PHYSICAL_TYPE, insert_string,
                            physical_type_to_string(field.physical_type));
        insert_if_requested(SCHEMA_LOGICAL_TYPE, insert_string,
                            logical_type_to_string(field.parquet_schema));
        insert_if_requested(SCHEMA_REPETITION_LEVEL, insert_int32, field.repetition_level);
        insert_if_requested(SCHEMA_DEFINITION_LEVEL, insert_int32, field.definition_level);

        int32_t type_length =
                field.parquet_schema.__isset.type_length ? field.parquet_schema.type_length : 0;
        insert_if_requested(SCHEMA_TYPE_LENGTH, insert_int32, type_length);
        int32_t precision =
                field.parquet_schema.__isset.precision ? field.parquet_schema.precision : 0;
        insert_if_requested(SCHEMA_PRECISION, insert_int32, precision);
        int32_t scale = field.parquet_schema.__isset.scale ? field.parquet_schema.scale : 0;
        insert_if_requested(SCHEMA_SCALE, insert_int32, scale);
        bool is_nullable_field =
                !field.parquet_schema.__isset.repetition_type ||
                field.parquet_schema.repetition_type != tparquet::FieldRepetitionType::REQUIRED;
        insert_if_requested(SCHEMA_IS_NULLABLE, insert_bool, is_nullable_field);
        return Status::OK();
    }
};

class ParquetMetadataModeHandler final : public ParquetMetadataReader::ModeHandler {
public:
    explicit ParquetMetadataModeHandler(RuntimeState* state) : ModeHandler(state) {}

    void init_slot_pos_map(const std::vector<SlotDescriptor*>& slots) override {
        std::unordered_map<std::string, int> name_to_pos = _build_name_to_pos_map(slots);
        _init_slot_pos_map(name_to_pos, kMetadataColumnNames, &_slot_pos);
    }

    Status append_rows(const std::string& path, FileMetaData* metadata,
                       std::vector<MutableColumnPtr>& columns) override {
        const tparquet::FileMetaData& thrift_meta = metadata->to_thrift();
        if (thrift_meta.row_groups.empty()) {
            return Status::OK();
        }

        std::unordered_map<std::string, const FieldSchema*> path_map;
        const auto& fields = metadata->schema().get_fields_schema();
        for (const auto& field : fields) {
            build_path_map(field, "", &path_map);
        }

        for (size_t rg_index = 0; rg_index < thrift_meta.row_groups.size(); ++rg_index) {
            const auto& row_group = thrift_meta.row_groups[rg_index];
            for (size_t col_idx = 0; col_idx < row_group.columns.size(); ++col_idx) {
                const auto& column_chunk = row_group.columns[col_idx];
                if (!column_chunk.__isset.meta_data) {
                    continue;
                }
                const auto& column_meta = column_chunk.meta_data;
                std::string column_path = join_path(column_meta.path_in_schema);
                const FieldSchema* schema_field = nullptr;
                auto it = path_map.find(column_path);
                if (it != path_map.end()) {
                    schema_field = it->second;
                }

                auto insert_if_requested = [&](MetadataColumnIndex idx, auto&& inserter,
                                               auto&&... args) {
                    int pos = _slot_pos[idx];
                    if (pos >= 0) {
                        inserter(columns[pos], std::forward<decltype(args)>(args)...);
                    }
                };

                insert_if_requested(META_FILE_NAME, insert_string,
                                    column_chunk.__isset.file_path ? column_chunk.file_path : path);
                insert_if_requested(META_ROW_GROUP_ID, insert_int32, static_cast<Int32>(rg_index));
                insert_if_requested(META_COLUMN_ID, insert_int32, static_cast<Int32>(col_idx));
                std::string column_name =
                        column_meta.path_in_schema.empty() ? "" : column_meta.path_in_schema.back();
                insert_if_requested(META_COLUMN_NAME, insert_string, column_name);
                insert_if_requested(META_COLUMN_PATH, insert_string, column_path);
                insert_if_requested(META_PHYSICAL_TYPE, insert_string,
                                    physical_type_to_string(column_meta.type));

                if (schema_field != nullptr) {
                    insert_if_requested(META_LOGICAL_TYPE, insert_string,
                                        logical_type_to_string(schema_field->parquet_schema));
                    int32_t type_length = schema_field->parquet_schema.__isset.type_length
                                                  ? schema_field->parquet_schema.type_length
                                                  : 0;
                    insert_if_requested(META_TYPE_LENGTH, insert_int32, type_length);
                    if (schema_field->parquet_schema.__isset.converted_type) {
                        insert_if_requested(META_CONVERTED_TYPE, insert_string,
                                            converted_type_to_string(
                                                    schema_field->parquet_schema.converted_type));
                    } else {
                        insert_if_requested(META_CONVERTED_TYPE, insert_string, "");
                    }
                } else {
                    insert_if_requested(META_LOGICAL_TYPE, insert_string, "");
                    insert_if_requested(META_TYPE_LENGTH, insert_int32, 0);
                    insert_if_requested(META_CONVERTED_TYPE, insert_string, "");
                }

                insert_if_requested(META_NUM_VALUES, insert_int64, column_meta.num_values);
                if (column_meta.__isset.statistics && column_meta.statistics.__isset.null_count) {
                    insert_if_requested(META_NULL_COUNT, insert_int64,
                                        column_meta.statistics.null_count);
                } else {
                    insert_if_requested(META_NULL_COUNT, insert_null);
                }
                if (column_meta.__isset.statistics &&
                    column_meta.statistics.__isset.distinct_count) {
                    insert_if_requested(META_DISTINCT_COUNT, insert_int64,
                                        column_meta.statistics.distinct_count);
                } else {
                    insert_if_requested(META_DISTINCT_COUNT, insert_null);
                }

                insert_if_requested(META_ENCODINGS, insert_string,
                                    encodings_to_string(column_meta.encodings));
                insert_if_requested(META_COMPRESSION, insert_string,
                                    compression_to_string(column_meta.codec));
                insert_if_requested(META_DATA_PAGE_OFFSET, insert_int64,
                                    column_meta.data_page_offset);
                if (column_meta.__isset.index_page_offset) {
                    insert_if_requested(META_INDEX_PAGE_OFFSET, insert_int64,
                                        column_meta.index_page_offset);
                } else {
                    insert_if_requested(META_INDEX_PAGE_OFFSET, insert_null);
                }
                if (column_meta.__isset.dictionary_page_offset) {
                    insert_if_requested(META_DICTIONARY_PAGE_OFFSET, insert_int64,
                                        column_meta.dictionary_page_offset);
                } else {
                    insert_if_requested(META_DICTIONARY_PAGE_OFFSET, insert_null);
                }
                insert_if_requested(META_TOTAL_COMPRESSED_SIZE, insert_int64,
                                    column_meta.total_compressed_size);
                insert_if_requested(META_TOTAL_UNCOMPRESSED_SIZE, insert_int64,
                                    column_meta.total_uncompressed_size);

                if (column_meta.__isset.statistics) {
                    static const cctz::time_zone kUtc0 = cctz::utc_time_zone();
                    const cctz::time_zone& ctz = _state != nullptr ? _state->timezone_obj() : kUtc0;

                    std::string min_value;
                    std::string max_value;
                    bool has_min = try_get_statistics_encoded_value(column_meta.statistics, true,
                                                                    &min_value);
                    bool has_max = try_get_statistics_encoded_value(column_meta.statistics, false,
                                                                    &max_value);

                    if (has_min) {
                        std::string decoded = decode_statistics_value(
                                schema_field, column_meta.type, min_value, ctz);
                        insert_if_requested(META_STATISTICS_MIN, insert_string, decoded);
                    } else {
                        insert_if_requested(META_STATISTICS_MIN, insert_null);
                    }
                    if (has_max) {
                        std::string decoded = decode_statistics_value(
                                schema_field, column_meta.type, max_value, ctz);
                        insert_if_requested(META_STATISTICS_MAX, insert_string, decoded);
                    } else {
                        insert_if_requested(META_STATISTICS_MAX, insert_null);
                    }
                } else {
                    insert_if_requested(META_STATISTICS_MIN, insert_null);
                    insert_if_requested(META_STATISTICS_MAX, insert_null);
                }
            }
        }
        return Status::OK();
    }

private:
    std::array<int, META_COLUMN_COUNT> _slot_pos {};
};

ParquetMetadataReader::ParquetMetadataReader(std::vector<SlotDescriptor*> slots,
                                             RuntimeState* state, RuntimeProfile* profile,
                                             TMetaScanRange scan_range)
        : _state(state), _slots(std::move(slots)), _scan_range(std::move(scan_range)) {
    (void)profile;
}

ParquetMetadataReader::~ParquetMetadataReader() = default;

Status ParquetMetadataReader::init_reader() {
    RETURN_IF_ERROR(_init_from_scan_range(_scan_range));
    if (_mode_type == Mode::SCHEMA) {
        _mode_handler = std::make_unique<ParquetSchemaModeHandler>(_state);
    } else {
        _mode_handler = std::make_unique<ParquetMetadataModeHandler>(_state);
    }
    _mode_handler->init_slot_pos_map(_slots);
    return Status::OK();
}

Status ParquetMetadataReader::_init_from_scan_range(const TMetaScanRange& scan_range) {
    if (!scan_range.__isset.parquet_params) {
        return Status::InvalidArgument(
                "Missing parquet parameters for parquet_metadata table function");
    }
    const TParquetMetadataParams& params = scan_range.parquet_params;
    std::vector<std::string> resolved_paths;
    if (scan_range.__isset.serialized_splits && !scan_range.serialized_splits.empty()) {
        resolved_paths.assign(scan_range.serialized_splits.begin(),
                              scan_range.serialized_splits.end());
    } else if (params.__isset.paths && !params.paths.empty()) {
        resolved_paths.assign(params.paths.begin(), params.paths.end());
    } else {
        return Status::InvalidArgument("Property 'path' must be set for parquet_metadata");
    }
    _paths.swap(resolved_paths);

    if (params.__isset.mode) {
        _mode = params.mode;
    } else {
        _mode = MODE_METADATA; // default
    }

    if (params.__isset.file_type) {
        _file_type = params.file_type;
    } else {
        return Status::InvalidArgument("Property 'file_type' must be set for parquet_metadata");
    }
    if (params.__isset.properties) {
        _properties = params.properties;
    }
    std::string lower_mode = _mode;
    std::ranges::transform(lower_mode, lower_mode.begin(),
                           [](unsigned char c) { return std::tolower(c); });
    if (lower_mode == MODE_SCHEMA) {
        _mode_type = Mode::SCHEMA;
        _mode = MODE_SCHEMA;
    } else {
        _mode_type = Mode::METADATA;
        _mode = MODE_METADATA;
    }
    return Status::OK();
}

Status ParquetMetadataReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_eof) {
        *eof = true;
        *read_rows = 0;
        return Status::OK();
    }

    // Scanner may call multiple times; we surface data once and mark eof on the next call.
    // When reusing a Block, wipe row data but keep column structure intact.
    bool mem_reuse = block->mem_reuse();
    std::vector<MutableColumnPtr> columns(_slots.size());
    if (mem_reuse) {
        block->clear_column_data();
        for (size_t i = 0; i < _slots.size(); ++i) {
            columns[i] = block->get_by_position(i).column->assume_mutable();
        }
    } else {
        for (size_t i = 0; i < _slots.size(); ++i) {
            columns[i] = _slots[i]->get_empty_mutable_column();
        }
    }

    size_t rows_before = block->rows();
    RETURN_IF_ERROR(_build_rows(columns));

    if (!mem_reuse) {
        for (size_t i = 0; i < _slots.size(); ++i) {
            block->insert(ColumnWithTypeAndName(
                    std::move(columns[i]), _slots[i]->get_data_type_ptr(), _slots[i]->col_name()));
        }
    } else {
        columns.clear();
    }

    size_t produced = block->rows() - rows_before;
    *read_rows = produced;
    _eof = true;
    *eof = (produced == 0);
    return Status::OK();
}

// Iterate all configured paths and append metadata rows into the provided columns.
Status ParquetMetadataReader::_build_rows(std::vector<MutableColumnPtr>& columns) {
    for (const auto& path : _paths) {
        RETURN_IF_ERROR(_append_file_rows(path, columns));
    }
    return Status::OK();
}

// Open a single Parquet file, read its footer, and dispatch to schema/metadata handlers.
Status ParquetMetadataReader::_append_file_rows(const std::string& path,
                                                std::vector<MutableColumnPtr>& columns) {
    io::FileSystemProperties system_properties;
    system_properties.system_type = _file_type;
    system_properties.properties = _properties;
    io::FileDescription file_desc;
    file_desc.path = path;
    io::FileReaderSPtr file_reader = DORIS_TRY(FileFactory::create_file_reader(
            system_properties, file_desc, io::FileReaderOptions::DEFAULT, nullptr));

    std::unique_ptr<FileMetaData> file_metadata;
    size_t meta_size = 0;
    io::IOContext io_ctx;
    RETURN_IF_ERROR(parse_thrift_footer(file_reader, &file_metadata, &meta_size, &io_ctx, false));

    if (_mode_handler == nullptr) {
        return Status::InternalError(
                "Parquet metadata reader is not initialized with mode handler");
    }
    return _mode_handler->append_rows(path, file_metadata.get(), columns);
}

Status ParquetMetadataReader::close() {
    return Status::OK();
}

} // namespace doris::vectorized
