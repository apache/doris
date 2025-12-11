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
#include <cctype>
#include <memory>
#include <sstream>
#include <unordered_map>

#include "common/logging.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/exec/format/parquet/parquet_thrift_util.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"

namespace doris::vectorized {
namespace {

constexpr const char* MODE_SCHEMA = "parquet_schema";
constexpr const char* MODE_METADATA = "parquet_metadata";

enum SchemaColumnIndex {
    SCHEMA_FILE_NAME = 0,
    SCHEMA_COLUMN_NAME,
    SCHEMA_COLUMN_PATH,
    SCHEMA_PHYSICAL_TYPE,
    SCHEMA_LOGICAL_TYPE,
    SCHEMA_REPETITION_LEVEL,
    SCHEMA_DEFINITION_LEVEL,
    SCHEMA_TYPE_LENGTH,
    SCHEMA_PRECISION,
    SCHEMA_SCALE,
    SCHEMA_IS_NULLABLE
};

enum MetadataColumnIndex {
    META_FILE_NAME = 0,
    META_ROW_GROUP_ID,
    META_COLUMN_ID,
    META_COLUMN_NAME,
    META_COLUMN_PATH,
    META_PHYSICAL_TYPE,
    META_LOGICAL_TYPE,
    META_TYPE_LENGTH,
    META_CONVERTED_TYPE,
    META_NUM_VALUES,
    META_NULL_COUNT,
    META_DISTINCT_COUNT,
    META_ENCODINGS,
    META_COMPRESSION,
    META_DATA_PAGE_OFFSET,
    META_INDEX_PAGE_OFFSET,
    META_DICTIONARY_PAGE_OFFSET,
    META_TOTAL_COMPRESSED_SIZE,
    META_TOTAL_UNCOMPRESSED_SIZE,
    META_STATISTICS_MIN,
    META_STATISTICS_MAX
};

std::string join_path(const std::vector<std::string>& items) {
    if (items.empty()) {
        return "";
    }
    std::ostringstream oss;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i != 0) {
            oss << ".";
        }
        oss << items[i];
    }
    return oss.str();
}

template <typename ColumnType, typename T>
void insert_numeric_impl(MutableColumnPtr& column, T value) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        nullable->get_null_map_data().push_back(0);
        auto& nested = nullable->get_nested_column();
        assert_cast<ColumnType&>(nested).insert_value(value);
    } else {
        assert_cast<ColumnType&>(*column).insert_value(value);
    }
}

void insert_int32(MutableColumnPtr& column, Int32 value) {
    insert_numeric_impl<ColumnInt32>(column, value);
}

void insert_int64(MutableColumnPtr& column, Int64 value) {
    insert_numeric_impl<ColumnInt64>(column, value);
}

void insert_bool(MutableColumnPtr& column, bool value) {
    insert_numeric_impl<ColumnUInt8>(column, static_cast<UInt8>(value));
}

void insert_string(MutableColumnPtr& column, const std::string& value) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        nullable->get_null_map_data().push_back(0);
        auto& nested = nullable->get_nested_column();
        assert_cast<ColumnString&>(nested).insert_data(value.c_str(), value.size());
    } else {
        assert_cast<ColumnString&>(*column).insert_data(value.c_str(), value.size());
    }
}

void insert_null(MutableColumnPtr& column) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        nullable->get_null_map_data().push_back(1);
        nullable->get_nested_column().insert_default();
    } else {
        column->insert_default();
    }
}

std::string physical_type_to_string(tparquet::Type::type type) {
    switch (type) {
    case tparquet::Type::BOOLEAN:
        return "BOOLEAN";
    case tparquet::Type::INT32:
        return "INT32";
    case tparquet::Type::INT64:
        return "INT64";
    case tparquet::Type::INT96:
        return "INT96";
    case tparquet::Type::FLOAT:
        return "FLOAT";
    case tparquet::Type::DOUBLE:
        return "DOUBLE";
    case tparquet::Type::BYTE_ARRAY:
        return "BYTE_ARRAY";
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return "FIXED_LEN_BYTE_ARRAY";
    default:
        return "UNKNOWN";
    }
}

std::string compression_to_string(tparquet::CompressionCodec::type codec) {
    switch (codec) {
    case tparquet::CompressionCodec::UNCOMPRESSED:
        return "UNCOMPRESSED";
    case tparquet::CompressionCodec::SNAPPY:
        return "SNAPPY";
    case tparquet::CompressionCodec::GZIP:
        return "GZIP";
    case tparquet::CompressionCodec::LZO:
        return "LZO";
    case tparquet::CompressionCodec::BROTLI:
        return "BROTLI";
    case tparquet::CompressionCodec::LZ4:
        return "LZ4";
    case tparquet::CompressionCodec::ZSTD:
        return "ZSTD";
    case tparquet::CompressionCodec::LZ4_RAW:
        return "LZ4_RAW";
    default:
        return "UNKNOWN";
    }
}

std::string converted_type_to_string(tparquet::ConvertedType::type type) {
    switch (type) {
    case tparquet::ConvertedType::UTF8:
        return "UTF8";
    case tparquet::ConvertedType::MAP:
        return "MAP";
    case tparquet::ConvertedType::MAP_KEY_VALUE:
        return "MAP_KEY_VALUE";
    case tparquet::ConvertedType::LIST:
        return "LIST";
    case tparquet::ConvertedType::ENUM:
        return "ENUM";
    case tparquet::ConvertedType::DECIMAL:
        return "DECIMAL";
    case tparquet::ConvertedType::DATE:
        return "DATE";
    case tparquet::ConvertedType::TIME_MILLIS:
        return "TIME_MILLIS";
    case tparquet::ConvertedType::TIME_MICROS:
        return "TIME_MICROS";
    case tparquet::ConvertedType::TIMESTAMP_MILLIS:
        return "TIMESTAMP_MILLIS";
    case tparquet::ConvertedType::TIMESTAMP_MICROS:
        return "TIMESTAMP_MICROS";
    case tparquet::ConvertedType::UINT_8:
        return "UINT_8";
    case tparquet::ConvertedType::UINT_16:
        return "UINT_16";
    case tparquet::ConvertedType::UINT_32:
        return "UINT_32";
    case tparquet::ConvertedType::UINT_64:
        return "UINT_64";
    case tparquet::ConvertedType::INT_8:
        return "INT_8";
    case tparquet::ConvertedType::INT_16:
        return "INT_16";
    case tparquet::ConvertedType::INT_32:
        return "INT_32";
    case tparquet::ConvertedType::INT_64:
        return "INT_64";
    case tparquet::ConvertedType::JSON:
        return "JSON";
    case tparquet::ConvertedType::BSON:
        return "BSON";
    case tparquet::ConvertedType::INTERVAL:
        return "INTERVAL";
    default:
        return "UNKNOWN";
    }
}

std::string logical_type_to_string(const tparquet::SchemaElement& element) {
    if (element.__isset.logicalType) {
        const auto& logical = element.logicalType;
        if (logical.__isset.STRING) {
            return "STRING";
        } else if (logical.__isset.MAP) {
            return "MAP";
        } else if (logical.__isset.LIST) {
            return "LIST";
        } else if (logical.__isset.ENUM) {
            return "ENUM";
        } else if (logical.__isset.DECIMAL) {
            return "DECIMAL";
        } else if (logical.__isset.DATE) {
            return "DATE";
        } else if (logical.__isset.TIME) {
            return "TIME";
        } else if (logical.__isset.TIMESTAMP) {
            return "TIMESTAMP";
        } else if (logical.__isset.INTEGER) {
            return "INTEGER";
        } else if (logical.__isset.UNKNOWN) {
            return "UNKNOWN";
        } else if (logical.__isset.JSON) {
            return "JSON";
        } else if (logical.__isset.BSON) {
            return "BSON";
        } else if (logical.__isset.UUID) {
            return "UUID";
        } else if (logical.__isset.FLOAT16) {
            return "FLOAT16";
        } else if (logical.__isset.VARIANT) {
            return "VARIANT";
        } else if (logical.__isset.GEOMETRY) {
            return "GEOMETRY";
        } else if (logical.__isset.GEOGRAPHY) {
            return "GEOGRAPHY";
        }
    }
    if (element.__isset.converted_type) {
        return converted_type_to_string(element.converted_type);
    }
    return "";
}

std::string encodings_to_string(const std::vector<tparquet::Encoding::type>& encodings) {
    std::ostringstream oss;
    for (size_t i = 0; i < encodings.size(); ++i) {
        if (i != 0) {
            oss << ",";
        }
        switch (encodings[i]) {
        case tparquet::Encoding::PLAIN:
            oss << "PLAIN";
            break;
        case tparquet::Encoding::PLAIN_DICTIONARY:
            oss << "PLAIN_DICTIONARY";
            break;
        case tparquet::Encoding::RLE:
            oss << "RLE";
            break;
        case tparquet::Encoding::BIT_PACKED:
            oss << "BIT_PACKED";
            break;
        case tparquet::Encoding::DELTA_BINARY_PACKED:
            oss << "DELTA_BINARY_PACKED";
            break;
        case tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
            oss << "DELTA_LENGTH_BYTE_ARRAY";
            break;
        case tparquet::Encoding::DELTA_BYTE_ARRAY:
            oss << "DELTA_BYTE_ARRAY";
            break;
        case tparquet::Encoding::RLE_DICTIONARY:
            oss << "RLE_DICTIONARY";
            break;
        default:
            oss << "UNKNOWN";
            break;
        }
    }
    return oss.str();
}

std::string statistics_value_to_string(const tparquet::Statistics& statistics, bool is_min) {
    if (is_min) {
        if (statistics.__isset.min_value) {
            return std::string(statistics.min_value);
        }
        if (statistics.__isset.min) {
            return std::string(statistics.min);
        }
    } else {
        if (statistics.__isset.max_value) {
            return std::string(statistics.max_value);
        }
        if (statistics.__isset.max) {
            return std::string(statistics.max);
        }
    }
    return "";
}

void build_path_map(const FieldSchema& field, const std::string& prefix,
                    std::unordered_map<std::string, const FieldSchema*>* map) {
    std::string current = prefix.empty() ? field.name : fmt::format("{}.{}", prefix, field.name);
    if (field.children.empty()) {
        (*map)[current] = &field;
    } else {
        for (const auto& child : field.children) {
            build_path_map(child, current, map);
        }
    }
}

} // namespace

ParquetMetadataReader::ParquetMetadataReader(const std::vector<SlotDescriptor*>& slots,
                                             RuntimeState* state, RuntimeProfile* profile,
                                             const TMetaScanRange& scan_range)
        : _slots(slots), _state(state), _profile(profile), _scan_range(scan_range) {}

Status ParquetMetadataReader::init_reader() {
    RETURN_IF_ERROR(_init_from_scan_range(_scan_range));
    return Status::OK();
}

Status ParquetMetadataReader::_init_from_scan_range(const TMetaScanRange& scan_range) {
    if (!scan_range.__isset.parquet_params) {
        return Status::InvalidArgument(
                "Missing parquet parameters for parquet_metadata table function");
    }
    const TParquetMetadataParams& params = scan_range.parquet_params;
    if (!params.__isset.paths || params.paths.empty()) {
        return Status::InvalidArgument("Property 'path' must be set for parquet_metadata");
    }
    _paths.assign(params.paths.begin(), params.paths.end());

    if (params.__isset.mode) {
        _mode = params.mode;
    } else {
        _mode = MODE_METADATA;
    }
    std::string lower_mode = _mode;
    std::transform(lower_mode.begin(), lower_mode.end(), lower_mode.begin(),
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

    bool mem_reuse = block->mem_reuse();
    std::vector<MutableColumnPtr> columns(_slots.size());
    if (mem_reuse) {
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
            block->insert(ColumnWithTypeAndName(std::move(columns[i]),
                                                _slots[i]->get_data_type_ptr(),
                                                _slots[i]->col_name()));
        }
    } else {
        columns.clear();
    }

    _eof = true;
    *eof = true;
    *read_rows = block->rows() - rows_before;
    return Status::OK();
}

Status ParquetMetadataReader::_build_rows(std::vector<MutableColumnPtr>& columns) {
    for (const auto& path : _paths) {
        RETURN_IF_ERROR(_append_file_rows(path, columns));
    }
    return Status::OK();
}

Status ParquetMetadataReader::_append_file_rows(const std::string& path,
                                                std::vector<MutableColumnPtr>& columns) {
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(path, &file_reader));

    std::unique_ptr<FileMetaData> file_metadata;
    size_t meta_size = 0;
    io::IOContext io_ctx;
    RETURN_IF_ERROR(parse_thrift_footer(file_reader, &file_metadata, &meta_size, &io_ctx, false));

    if (_mode_type == Mode::SCHEMA) {
        RETURN_IF_ERROR(_append_schema_rows(path, file_metadata.get(), columns));
    } else {
        RETURN_IF_ERROR(_append_metadata_rows(path, file_metadata.get(), columns));
    }
    return Status::OK();
}

Status ParquetMetadataReader::_append_schema_rows(const std::string& path, FileMetaData* metadata,
                                                  std::vector<MutableColumnPtr>& columns) {
    const auto& fields = metadata->schema().get_fields_schema();
    for (const auto& field : fields) {
        RETURN_IF_ERROR(_append_schema_field(path, field, field.name, columns));
    }
    return Status::OK();
}

Status ParquetMetadataReader::_append_schema_field(const std::string& path,
                                                   const FieldSchema& field,
                                                   const std::string& current_path,
                                                   std::vector<MutableColumnPtr>& columns) {
    if (!field.children.empty()) {
        for (const auto& child : field.children) {
            std::string child_path =
                    current_path.empty() ? child.name : fmt::format("{}.{}", current_path, child.name);
            RETURN_IF_ERROR(_append_schema_field(path, child, child_path, columns));
        }
        return Status::OK();
    }

    insert_string(columns[SCHEMA_FILE_NAME], path);
    insert_string(columns[SCHEMA_COLUMN_NAME], field.name);
    insert_string(columns[SCHEMA_COLUMN_PATH], current_path);
    insert_string(columns[SCHEMA_PHYSICAL_TYPE], physical_type_to_string(field.physical_type));
    insert_string(columns[SCHEMA_LOGICAL_TYPE], logical_type_to_string(field.parquet_schema));
    insert_int32(columns[SCHEMA_REPETITION_LEVEL], field.repetition_level);
    insert_int32(columns[SCHEMA_DEFINITION_LEVEL], field.definition_level);

    int32_t type_length = field.parquet_schema.__isset.type_length ? field.parquet_schema.type_length
                                                                   : 0;
    insert_int32(columns[SCHEMA_TYPE_LENGTH], type_length);
    int32_t precision =
            field.parquet_schema.__isset.precision ? field.parquet_schema.precision : 0;
    insert_int32(columns[SCHEMA_PRECISION], precision);
    int32_t scale = field.parquet_schema.__isset.scale ? field.parquet_schema.scale : 0;
    insert_int32(columns[SCHEMA_SCALE], scale);
    bool is_nullable_field =
            !field.parquet_schema.__isset.repetition_type ||
            field.parquet_schema.repetition_type != tparquet::FieldRepetitionType::REQUIRED;
    insert_bool(columns[SCHEMA_IS_NULLABLE], is_nullable_field);
    return Status::OK();
}

Status ParquetMetadataReader::_append_metadata_rows(const std::string& path,
                                                    FileMetaData* metadata,
                                                    std::vector<MutableColumnPtr>& columns) {
    const tparquet::FileMetaData& thrift_meta = metadata->to_thrift();
    if (!thrift_meta.__isset.row_groups) {
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

            insert_string(columns[META_FILE_NAME],
                          column_chunk.__isset.file_path ? column_chunk.file_path : path);
            insert_int32(columns[META_ROW_GROUP_ID], static_cast<Int32>(rg_index));
            insert_int32(columns[META_COLUMN_ID], static_cast<Int32>(col_idx));
            std::string column_name =
                    column_meta.path_in_schema.empty() ? "" : column_meta.path_in_schema.back();
            insert_string(columns[META_COLUMN_NAME], column_name);
            insert_string(columns[META_COLUMN_PATH], column_path);
            insert_string(columns[META_PHYSICAL_TYPE],
                          physical_type_to_string(column_meta.type));

            if (schema_field != nullptr) {
                insert_string(columns[META_LOGICAL_TYPE],
                              logical_type_to_string(schema_field->parquet_schema));
                int32_t type_length = schema_field->parquet_schema.__isset.type_length
                                              ? schema_field->parquet_schema.type_length
                                              : 0;
                insert_int32(columns[META_TYPE_LENGTH], type_length);
                if (schema_field->parquet_schema.__isset.converted_type) {
                    insert_string(columns[META_CONVERTED_TYPE],
                                  converted_type_to_string(
                                          schema_field->parquet_schema.converted_type));
                } else {
                    insert_string(columns[META_CONVERTED_TYPE], "");
                }
            } else {
                insert_string(columns[META_LOGICAL_TYPE], "");
                insert_int32(columns[META_TYPE_LENGTH], 0);
                insert_string(columns[META_CONVERTED_TYPE], "");
            }

            insert_int64(columns[META_NUM_VALUES], column_meta.num_values);
            if (column_meta.__isset.statistics && column_meta.statistics.__isset.null_count) {
                insert_int64(columns[META_NULL_COUNT], column_meta.statistics.null_count);
            } else {
                insert_null(columns[META_NULL_COUNT]);
            }
            if (column_meta.__isset.statistics &&
                column_meta.statistics.__isset.distinct_count) {
                insert_int64(columns[META_DISTINCT_COUNT], column_meta.statistics.distinct_count);
            } else {
                insert_null(columns[META_DISTINCT_COUNT]);
            }

            insert_string(columns[META_ENCODINGS], encodings_to_string(column_meta.encodings));
            insert_string(columns[META_COMPRESSION],
                          compression_to_string(column_meta.codec));
            insert_int64(columns[META_DATA_PAGE_OFFSET], column_meta.data_page_offset);
            if (column_meta.__isset.index_page_offset) {
                insert_int64(columns[META_INDEX_PAGE_OFFSET],
                             column_meta.index_page_offset);
            } else {
                insert_null(columns[META_INDEX_PAGE_OFFSET]);
            }
            if (column_meta.__isset.dictionary_page_offset) {
                insert_int64(columns[META_DICTIONARY_PAGE_OFFSET],
                             column_meta.dictionary_page_offset);
            } else {
                insert_null(columns[META_DICTIONARY_PAGE_OFFSET]);
            }
            insert_int64(columns[META_TOTAL_COMPRESSED_SIZE],
                         column_meta.total_compressed_size);
            insert_int64(columns[META_TOTAL_UNCOMPRESSED_SIZE],
                         column_meta.total_uncompressed_size);

            if (column_meta.__isset.statistics) {
                std::string min_value =
                        statistics_value_to_string(column_meta.statistics, true);
                std::string max_value =
                        statistics_value_to_string(column_meta.statistics, false);
                if (!min_value.empty()) {
                    insert_string(columns[META_STATISTICS_MIN], min_value);
                } else {
                    insert_null(columns[META_STATISTICS_MIN]);
                }
                if (!max_value.empty()) {
                    insert_string(columns[META_STATISTICS_MAX], max_value);
                } else {
                    insert_null(columns[META_STATISTICS_MAX]);
                }
            } else {
                insert_null(columns[META_STATISTICS_MIN]);
                insert_null(columns[META_STATISTICS_MAX]);
            }
        }
    }
    return Status::OK();
}

Status ParquetMetadataReader::close() {
    return Status::OK();
}

} // namespace doris::vectorized
