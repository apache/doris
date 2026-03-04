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
#include <cstring>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/hdfs_builder.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/string_view.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
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
            RETURN_IF_ERROR(_append_schema_node(path, field, columns));
        }
        return Status::OK();
    }

private:
    std::array<int, SCHEMA_COLUMN_COUNT> _slot_pos {};

    static std::string _repetition_type_to_string(tparquet::FieldRepetitionType::type type) {
        switch (type) {
        case tparquet::FieldRepetitionType::REQUIRED:
            return "REQUIRED";
        case tparquet::FieldRepetitionType::OPTIONAL:
            return "OPTIONAL";
        case tparquet::FieldRepetitionType::REPEATED:
            return "REPEATED";
        default:
            return "UNKNOWN";
        }
    }

    Status _append_schema_node(const std::string& path, const FieldSchema& field,
                               std::vector<MutableColumnPtr>& columns) {
        auto insert_if_requested = [&](SchemaColumnIndex idx, auto&& inserter, auto&&... args) {
            int pos = _slot_pos[idx];
            if (pos >= 0) {
                inserter(columns[pos], std::forward<decltype(args)>(args)...);
            }
        };

        insert_if_requested(SCHEMA_FILE_NAME, insert_string, path);
        insert_if_requested(SCHEMA_NAME, insert_string, field.parquet_schema.name);

        if (field.parquet_schema.__isset.type) {
            insert_if_requested(SCHEMA_TYPE, insert_string,
                                physical_type_to_string(field.parquet_schema.type));
        } else {
            insert_if_requested(SCHEMA_TYPE, insert_null);
        }

        if (field.parquet_schema.__isset.type_length) {
            insert_if_requested(SCHEMA_TYPE_LENGTH, insert_int64,
                                static_cast<Int64>(field.parquet_schema.type_length));
        } else {
            insert_if_requested(SCHEMA_TYPE_LENGTH, insert_null);
        }

        if (field.parquet_schema.__isset.repetition_type) {
            insert_if_requested(SCHEMA_REPETITION_TYPE, insert_string,
                                _repetition_type_to_string(field.parquet_schema.repetition_type));
        } else {
            insert_if_requested(SCHEMA_REPETITION_TYPE, insert_null);
        }

        int64_t num_children = field.parquet_schema.__isset.num_children
                                       ? static_cast<int64_t>(field.parquet_schema.num_children)
                                       : 0;
        insert_if_requested(SCHEMA_NUM_CHILDREN, insert_int64, static_cast<Int64>(num_children));

        if (field.parquet_schema.__isset.converted_type) {
            insert_if_requested(SCHEMA_CONVERTED_TYPE, insert_string,
                                converted_type_to_string(field.parquet_schema.converted_type));
        } else {
            insert_if_requested(SCHEMA_CONVERTED_TYPE, insert_null);
        }

        if (field.parquet_schema.__isset.scale) {
            insert_if_requested(SCHEMA_SCALE, insert_int64,
                                static_cast<Int64>(field.parquet_schema.scale));
        } else {
            insert_if_requested(SCHEMA_SCALE, insert_null);
        }

        if (field.parquet_schema.__isset.precision) {
            insert_if_requested(SCHEMA_PRECISION, insert_int64,
                                static_cast<Int64>(field.parquet_schema.precision));
        } else {
            insert_if_requested(SCHEMA_PRECISION, insert_null);
        }

        if (field.parquet_schema.__isset.field_id) {
            insert_if_requested(SCHEMA_FIELD_ID, insert_int64,
                                static_cast<Int64>(field.parquet_schema.field_id));
        } else {
            insert_if_requested(SCHEMA_FIELD_ID, insert_null);
        }

        std::string logical = logical_type_to_string(field.parquet_schema);
        if (logical.empty()) {
            insert_if_requested(SCHEMA_LOGICAL_TYPE, insert_null);
        } else {
            insert_if_requested(SCHEMA_LOGICAL_TYPE, insert_string, logical);
        }

        for (const auto& child : field.children) {
            RETURN_IF_ERROR(_append_schema_node(path, child, columns));
        }
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

        const int kv_pos = _slot_pos[META_KEY_VALUE_METADATA];
        bool has_kv_map = false;
        Field kv_map_field;
        if (kv_pos >= 0 && thrift_meta.__isset.key_value_metadata &&
            !thrift_meta.key_value_metadata.empty()) {
            Array keys;
            Array values;
            keys.reserve(thrift_meta.key_value_metadata.size());
            values.reserve(thrift_meta.key_value_metadata.size());
            for (const auto& kv : thrift_meta.key_value_metadata) {
                keys.emplace_back(Field::create_field<TYPE_VARBINARY>(doris::StringView(kv.key)));
                if (kv.__isset.value) {
                    values.emplace_back(
                            Field::create_field<TYPE_VARBINARY>(doris::StringView(kv.value)));
                } else {
                    values.emplace_back(Field {});
                }
            }
            Map map_value;
            map_value.reserve(2);
            map_value.emplace_back(Field::create_field<TYPE_ARRAY>(std::move(keys)));
            map_value.emplace_back(Field::create_field<TYPE_ARRAY>(std::move(values)));
            kv_map_field = Field::create_field<TYPE_MAP>(std::move(map_value));
            has_kv_map = true;
        }

        for (size_t rg_index = 0; rg_index < thrift_meta.row_groups.size(); ++rg_index) {
            const auto& row_group = thrift_meta.row_groups[rg_index];
            Int64 row_group_num_rows = static_cast<Int64>(row_group.num_rows);
            Int64 row_group_num_columns = static_cast<Int64>(row_group.columns.size());
            Int64 row_group_bytes = static_cast<Int64>(row_group.total_byte_size);
            Int64 row_group_compressed_bytes = 0;
            if (row_group.__isset.total_compressed_size) {
                row_group_compressed_bytes = static_cast<Int64>(row_group.total_compressed_size);
            } else {
                for (const auto& col_chunk : row_group.columns) {
                    if (!col_chunk.__isset.meta_data) {
                        continue;
                    }
                    row_group_compressed_bytes += col_chunk.meta_data.total_compressed_size;
                }
            }

            for (size_t col_idx = 0; col_idx < row_group.columns.size(); ++col_idx) {
                const auto& column_chunk = row_group.columns[col_idx];
                if (!column_chunk.__isset.meta_data) {
                    continue;
                }
                const auto& column_meta = column_chunk.meta_data;
                std::string path_in_schema = join_path(column_meta.path_in_schema);
                const FieldSchema* schema_field = nullptr;
                auto it = path_map.find(path_in_schema);
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
                insert_if_requested(META_ROW_GROUP_ID, insert_int64, static_cast<Int64>(rg_index));
                insert_if_requested(META_ROW_GROUP_NUM_ROWS, insert_int64, row_group_num_rows);
                insert_if_requested(META_ROW_GROUP_NUM_COLUMNS, insert_int64,
                                    row_group_num_columns);
                insert_if_requested(META_ROW_GROUP_BYTES, insert_int64, row_group_bytes);
                insert_if_requested(META_COLUMN_ID, insert_int64, static_cast<Int64>(col_idx));

                // `ColumnChunk.file_offset` is deprecated and can be 0 even when page offsets are present.
                // Fall back to the first page (dictionary/data) offset to provide a useful value.
                Int64 file_offset = static_cast<Int64>(column_chunk.file_offset);
                if (file_offset == 0) {
                    if (column_meta.__isset.dictionary_page_offset) {
                        file_offset = static_cast<Int64>(column_meta.dictionary_page_offset);
                    } else {
                        file_offset = static_cast<Int64>(column_meta.data_page_offset);
                    }
                }
                insert_if_requested(META_FILE_OFFSET, insert_int64, file_offset);
                insert_if_requested(META_NUM_VALUES, insert_int64, column_meta.num_values);
                insert_if_requested(META_PATH_IN_SCHEMA, insert_string, path_in_schema);
                insert_if_requested(META_TYPE, insert_string,
                                    physical_type_to_string(column_meta.type));

                if (column_meta.__isset.statistics) {
                    static const cctz::time_zone kUtc0 = cctz::utc_time_zone();
                    const cctz::time_zone& ctz = _state != nullptr ? _state->timezone_obj() : kUtc0;

                    const auto& stats = column_meta.statistics;

                    if (stats.__isset.min) {
                        insert_if_requested(META_STATS_MIN, insert_string,
                                            decode_statistics_value(schema_field, column_meta.type,
                                                                    stats.min, ctz));
                    } else {
                        insert_if_requested(META_STATS_MIN, insert_null);
                    }
                    if (stats.__isset.max) {
                        insert_if_requested(META_STATS_MAX, insert_string,
                                            decode_statistics_value(schema_field, column_meta.type,
                                                                    stats.max, ctz));
                    } else {
                        insert_if_requested(META_STATS_MAX, insert_null);
                    }

                    if (stats.__isset.null_count) {
                        insert_if_requested(META_STATS_NULL_COUNT, insert_int64, stats.null_count);
                    } else {
                        insert_if_requested(META_STATS_NULL_COUNT, insert_null);
                    }
                    if (stats.__isset.distinct_count) {
                        insert_if_requested(META_STATS_DISTINCT_COUNT, insert_int64,
                                            stats.distinct_count);
                    } else {
                        insert_if_requested(META_STATS_DISTINCT_COUNT, insert_null);
                    }

                    // Prefer min_value/max_value, but fall back to deprecated min/max so the column
                    // is still populated for older files.
                    std::string encoded_min_value;
                    std::string encoded_max_value;
                    bool has_min_value = false;
                    bool has_max_value = false;
                    if (stats.__isset.min_value) {
                        encoded_min_value = stats.min_value;
                        has_min_value = true;
                    } else if (stats.__isset.min) {
                        encoded_min_value = stats.min;
                        has_min_value = true;
                    }
                    if (stats.__isset.max_value) {
                        encoded_max_value = stats.max_value;
                        has_max_value = true;
                    } else if (stats.__isset.max) {
                        encoded_max_value = stats.max;
                        has_max_value = true;
                    }
                    if (has_min_value) {
                        insert_if_requested(META_STATS_MIN_VALUE, insert_string,
                                            decode_statistics_value(schema_field, column_meta.type,
                                                                    encoded_min_value, ctz));
                    } else {
                        insert_if_requested(META_STATS_MIN_VALUE, insert_null);
                    }
                    if (has_max_value) {
                        insert_if_requested(META_STATS_MAX_VALUE, insert_string,
                                            decode_statistics_value(schema_field, column_meta.type,
                                                                    encoded_max_value, ctz));
                    } else {
                        insert_if_requested(META_STATS_MAX_VALUE, insert_null);
                    }

                    if (stats.__isset.is_min_value_exact) {
                        insert_if_requested(META_MIN_IS_EXACT, insert_bool,
                                            stats.is_min_value_exact);
                    } else {
                        insert_if_requested(META_MIN_IS_EXACT, insert_null);
                    }
                    if (stats.__isset.is_max_value_exact) {
                        insert_if_requested(META_MAX_IS_EXACT, insert_bool,
                                            stats.is_max_value_exact);
                    } else {
                        insert_if_requested(META_MAX_IS_EXACT, insert_null);
                    }
                } else {
                    insert_if_requested(META_STATS_MIN, insert_null);
                    insert_if_requested(META_STATS_MAX, insert_null);
                    insert_if_requested(META_STATS_NULL_COUNT, insert_null);
                    insert_if_requested(META_STATS_DISTINCT_COUNT, insert_null);
                    insert_if_requested(META_STATS_MIN_VALUE, insert_null);
                    insert_if_requested(META_STATS_MAX_VALUE, insert_null);
                    insert_if_requested(META_MIN_IS_EXACT, insert_null);
                    insert_if_requested(META_MAX_IS_EXACT, insert_null);
                }

                insert_if_requested(META_COMPRESSION, insert_string,
                                    compression_to_string(column_meta.codec));
                insert_if_requested(META_ENCODINGS, insert_string,
                                    encodings_to_string(column_meta.encodings));

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
                insert_if_requested(META_DATA_PAGE_OFFSET, insert_int64,
                                    column_meta.data_page_offset);

                insert_if_requested(META_TOTAL_COMPRESSED_SIZE, insert_int64,
                                    column_meta.total_compressed_size);
                insert_if_requested(META_TOTAL_UNCOMPRESSED_SIZE, insert_int64,
                                    column_meta.total_uncompressed_size);

                if (kv_pos >= 0) {
                    if (has_kv_map) {
                        columns[kv_pos]->insert(kv_map_field);
                    } else {
                        insert_null(columns[kv_pos]);
                    }
                }

                if (column_meta.__isset.bloom_filter_offset) {
                    insert_if_requested(META_BLOOM_FILTER_OFFSET, insert_int64,
                                        column_meta.bloom_filter_offset);
                } else {
                    insert_if_requested(META_BLOOM_FILTER_OFFSET, insert_null);
                }
                if (column_meta.__isset.bloom_filter_length) {
                    insert_if_requested(META_BLOOM_FILTER_LENGTH, insert_int64,
                                        static_cast<Int64>(column_meta.bloom_filter_length));
                } else {
                    insert_if_requested(META_BLOOM_FILTER_LENGTH, insert_null);
                }

                insert_if_requested(META_ROW_GROUP_COMPRESSED_BYTES, insert_int64,
                                    row_group_compressed_bytes);
            }
        }
        return Status::OK();
    }

private:
    std::array<int, META_COLUMN_COUNT> _slot_pos {};
};

class ParquetFileMetadataModeHandler final : public ParquetMetadataReader::ModeHandler {
public:
    explicit ParquetFileMetadataModeHandler(RuntimeState* state) : ModeHandler(state) {}

    void init_slot_pos_map(const std::vector<SlotDescriptor*>& slots) override {
        const auto& name_to_pos = _build_name_to_pos_map(slots);
        _init_slot_pos_map(name_to_pos, kFileMetadataColumnNames, &_slot_pos);
    }

    Status append_rows(const std::string& path, FileMetaData* metadata,
                       std::vector<MutableColumnPtr>& columns) override {
        const tparquet::FileMetaData& thrift_meta = metadata->to_thrift();

        auto insert_if_requested = [&](FileMetadataColumnIndex idx, auto&& inserter,
                                       auto&&... args) {
            int pos = _slot_pos[idx];
            if (pos >= 0) {
                inserter(columns[pos], std::forward<decltype(args)>(args)...);
            }
        };

        insert_if_requested(FILE_META_FILE_NAME, insert_string, path);
        if (thrift_meta.__isset.created_by) {
            insert_if_requested(FILE_META_CREATED_BY, insert_string, thrift_meta.created_by);
        } else {
            insert_if_requested(FILE_META_CREATED_BY, insert_null);
        }
        insert_if_requested(FILE_META_NUM_ROWS, insert_int64,
                            static_cast<Int64>(thrift_meta.num_rows));
        insert_if_requested(FILE_META_NUM_ROW_GROUPS, insert_int64,
                            static_cast<Int64>(thrift_meta.row_groups.size()));
        insert_if_requested(FILE_META_FORMAT_VERSION, insert_int64,
                            static_cast<Int64>(thrift_meta.version));
        if (thrift_meta.__isset.encryption_algorithm) {
            const auto& algo = thrift_meta.encryption_algorithm;
            std::string algo_name;
            if (algo.__isset.AES_GCM_V1) {
                algo_name = "AES_GCM_V1";
            } else if (algo.__isset.AES_GCM_CTR_V1) {
                algo_name = "AES_GCM_CTR_V1";
            }
            if (!algo_name.empty()) {
                insert_if_requested(FILE_META_ENCRYPTION_ALGORITHM, insert_string, algo_name);
            } else {
                insert_if_requested(FILE_META_ENCRYPTION_ALGORITHM, insert_null);
            }
        } else {
            insert_if_requested(FILE_META_ENCRYPTION_ALGORITHM, insert_null);
        }
        if (thrift_meta.__isset.footer_signing_key_metadata) {
            insert_if_requested(FILE_META_FOOTER_SIGNING_KEY_METADATA, insert_string,
                                thrift_meta.footer_signing_key_metadata);
        } else {
            insert_if_requested(FILE_META_FOOTER_SIGNING_KEY_METADATA, insert_null);
        }
        return Status::OK();
    }

private:
    std::array<int, FILE_META_COLUMN_COUNT> _slot_pos {};
};

class ParquetKeyValueModeHandler final : public ParquetMetadataReader::ModeHandler {
public:
    explicit ParquetKeyValueModeHandler(RuntimeState* state) : ModeHandler(state) {}

    void init_slot_pos_map(const std::vector<SlotDescriptor*>& slots) override {
        const auto& name_to_pos = _build_name_to_pos_map(slots);
        _init_slot_pos_map(name_to_pos, kKeyValueColumnNames, &_slot_pos);
    }

    Status append_rows(const std::string& path, FileMetaData* metadata,
                       std::vector<MutableColumnPtr>& columns) override {
        const tparquet::FileMetaData& thrift_meta = metadata->to_thrift();
        if (!thrift_meta.__isset.key_value_metadata || thrift_meta.key_value_metadata.empty()) {
            return Status::OK();
        }

        auto insert_if_requested = [&](KeyValueColumnIndex idx, auto&& inserter, auto&&... args) {
            int pos = _slot_pos[idx];
            if (pos >= 0) {
                inserter(columns[pos], std::forward<decltype(args)>(args)...);
            }
        };

        for (const auto& kv : thrift_meta.key_value_metadata) {
            insert_if_requested(KV_FILE_NAME, insert_string, path);
            insert_if_requested(KV_KEY, insert_string, kv.key);
            if (kv.__isset.value) {
                insert_if_requested(KV_VALUE, insert_string, kv.value);
            } else {
                insert_if_requested(KV_VALUE, insert_null);
            }
        }
        return Status::OK();
    }

private:
    std::array<int, KV_COLUMN_COUNT> _slot_pos {};
};

class ParquetBloomProbeModeHandler final : public ParquetMetadataReader::ModeHandler {
public:
    ParquetBloomProbeModeHandler(RuntimeState* state, TFileType::type file_type,
                                 std::map<std::string, std::string> properties, std::string column,
                                 std::string literal)
            : ModeHandler(state),
              _file_type(file_type),
              _properties(std::move(properties)),
              _column(std::move(column)),
              _literal(std::move(literal)) {}

    void init_slot_pos_map(const std::vector<SlotDescriptor*>& slots) override {
        const auto& name_to_pos = _build_name_to_pos_map(slots);
        _init_slot_pos_map(name_to_pos, kBloomProbeColumnNames, &_slot_pos);
    }

    Status append_rows(const std::string& path, FileMetaData* metadata,
                       std::vector<MutableColumnPtr>& columns) override {
        const FieldSchema* schema = metadata->schema().get_column(_column);
        if (schema == nullptr) {
            return Status::InvalidArgument(
                    fmt::format("Column '{}' not found for parquet_bloom_probe", _column));
        }
        int parquet_col_id = schema->physical_column_index;
        PrimitiveType primitive_type = _get_primitive(schema->data_type);
        if (!ParquetPredicate::bloom_filter_supported(primitive_type)) {
            return Status::InvalidArgument(
                    fmt::format("Column '{}' type {} does not support parquet bloom filter probe",
                                _column, primitive_type));
        }

        std::string encoded_literal;
        RETURN_IF_ERROR(
                _encode_literal(schema->physical_type, primitive_type, _literal, &encoded_literal));

        io::FileSystemProperties system_properties;
        system_properties.system_type = _file_type;
        system_properties.properties = _properties;
        io::FileDescription file_desc;
        file_desc.path = path;
        io::FileReaderSPtr file_reader = DORIS_TRY(FileFactory::create_file_reader(
                system_properties, file_desc, io::FileReaderOptions::DEFAULT, nullptr));
        io::IOContext io_ctx;

        const tparquet::FileMetaData& thrift_meta = metadata->to_thrift();
        if (thrift_meta.row_groups.empty()) {
            return Status::OK();
        }

        for (size_t rg_idx = 0; rg_idx < thrift_meta.row_groups.size(); ++rg_idx) {
            if (parquet_col_id < 0 ||
                parquet_col_id >= thrift_meta.row_groups[rg_idx].columns.size()) {
                return Status::InvalidArgument(fmt::format(
                        "Invalid column index {} for parquet_bloom_probe", parquet_col_id));
            }
            const auto& column_chunk = thrift_meta.row_groups[rg_idx].columns[parquet_col_id];
            std::optional<bool> excludes;
            if (column_chunk.__isset.meta_data &&
                column_chunk.meta_data.__isset.bloom_filter_offset) {
                ParquetPredicate::ColumnStat stat;
                auto st = ParquetPredicate::read_bloom_filter(column_chunk.meta_data, file_reader,
                                                              &io_ctx, &stat);
                if (st.ok() && stat.bloom_filter) {
                    bool might_contain = stat.bloom_filter->test_bytes(encoded_literal.data(),
                                                                       encoded_literal.size());
                    excludes = !might_contain;
                }
            }
            _emit_row(path, static_cast<Int64>(rg_idx), excludes, columns);
        }
        return Status::OK();
    }

private:
    std::array<int, BLOOM_COLUMN_COUNT> _slot_pos {};
    TFileType::type _file_type;
    std::map<std::string, std::string> _properties;
    std::string _column;
    std::string _literal;

    PrimitiveType _get_primitive(const DataTypePtr& type) const {
        if (auto nullable = typeid_cast<const DataTypeNullable*>(type.get())) {
            return nullable->get_nested_type()->get_primitive_type();
        }
        return type->get_primitive_type();
    }

    Status _encode_literal(tparquet::Type::type physical_type, PrimitiveType primitive_type,
                           const std::string& literal, std::string* out) const {
        try {
            switch (physical_type) {
            case tparquet::Type::INT32: {
                int64_t v = std::stoll(literal);
                int32_t v32 = static_cast<int32_t>(v);
                out->assign(reinterpret_cast<const char*>(&v32), sizeof(int32_t));
                return Status::OK();
            }
            case tparquet::Type::INT64: {
                int64_t v = std::stoll(literal);
                out->assign(reinterpret_cast<const char*>(&v), sizeof(int64_t));
                return Status::OK();
            }
            case tparquet::Type::FLOAT: {
                float v = std::stof(literal);
                out->assign(reinterpret_cast<const char*>(&v), sizeof(float));
                return Status::OK();
            }
            case tparquet::Type::DOUBLE: {
                double v = std::stod(literal);
                out->assign(reinterpret_cast<const char*>(&v), sizeof(double));
                return Status::OK();
            }
            case tparquet::Type::BYTE_ARRAY: {
                // For string/blob, use raw bytes from the literal.
                *out = literal;
                return Status::OK();
            }
            default:
                break;
            }
        } catch (const std::exception& e) {
            return Status::InvalidArgument(fmt::format(
                    "Failed to parse literal '{}' for parquet bloom probe: {}", literal, e.what()));
        }
        return Status::NotSupported(
                fmt::format("Physical type {} for column '{}' not supported in parquet_bloom_probe",
                            physical_type, _column));
    }

    void _emit_row(const std::string& path, Int64 row_group_id, std::optional<bool> excludes,
                   std::vector<MutableColumnPtr>& columns) {
        if (_slot_pos[BLOOM_FILE_NAME] >= 0) {
            insert_string(columns[_slot_pos[BLOOM_FILE_NAME]], path);
        }
        if (_slot_pos[BLOOM_ROW_GROUP_ID] >= 0) {
            insert_int32(columns[_slot_pos[BLOOM_ROW_GROUP_ID]], static_cast<Int32>(row_group_id));
        }
        if (_slot_pos[BLOOM_EXCLUDES] >= 0) {
            int32_t excludes_val = -1; // -1: no bloom filter present
            if (excludes.has_value()) {
                excludes_val = excludes.value() ? 1 : 0;
            }
            insert_int32(columns[_slot_pos[BLOOM_EXCLUDES]], excludes_val);
        }
    }
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
    } else if (_mode_type == Mode::FILE_METADATA) {
        _mode_handler = std::make_unique<ParquetFileMetadataModeHandler>(_state);
    } else if (_mode_type == Mode::KEY_VALUE_METADATA) {
        _mode_handler = std::make_unique<ParquetKeyValueModeHandler>(_state);
    } else if (_mode_type == Mode::BLOOM_PROBE) {
        _mode_handler = std::make_unique<ParquetBloomProbeModeHandler>(
                _state, _file_type, _properties, _bloom_column, _bloom_literal);
    } else {
        _mode_handler = std::make_unique<ParquetMetadataModeHandler>(_state);
    }
    _mode_handler->init_slot_pos_map(_slots);
    return Status::OK();
}

Status ParquetMetadataReader::_init_from_scan_range(const TMetaScanRange& scan_range) {
    if (!scan_range.__isset.parquet_params) {
        return Status::InvalidArgument(
                "Missing parquet parameters for parquet_meta table function");
    }
    const TParquetMetadataParams& params = scan_range.parquet_params;
    std::vector<std::string> resolved_paths;
    if (scan_range.__isset.serialized_splits && !scan_range.serialized_splits.empty()) {
        resolved_paths.assign(scan_range.serialized_splits.begin(),
                              scan_range.serialized_splits.end());
    } else if (params.__isset.paths && !params.paths.empty()) {
        resolved_paths.assign(params.paths.begin(), params.paths.end());
    } else {
        return Status::InvalidArgument("Property 'path' must be set for parquet_meta");
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
    if (params.__isset.bloom_column) {
        _bloom_column = params.bloom_column;
    }
    if (params.__isset.bloom_literal) {
        _bloom_literal = params.bloom_literal;
    }
    std::string lower_mode = _mode;
    std::ranges::transform(lower_mode, lower_mode.begin(),
                           [](unsigned char c) { return std::tolower(c); });
    if (lower_mode == MODE_SCHEMA) {
        _mode_type = Mode::SCHEMA;
        _mode = MODE_SCHEMA;
    } else if (lower_mode == MODE_FILE_METADATA) {
        _mode_type = Mode::FILE_METADATA;
        _mode = MODE_FILE_METADATA;
    } else if (lower_mode == MODE_KEY_VALUE_METADATA) {
        _mode_type = Mode::KEY_VALUE_METADATA;
        _mode = MODE_KEY_VALUE_METADATA;
    } else if (lower_mode == MODE_BLOOM_PROBE) {
        _mode_type = Mode::BLOOM_PROBE;
        _mode = MODE_BLOOM_PROBE;
    } else {
        _mode_type = Mode::METADATA;
        _mode = MODE_METADATA;
    }
    if (_mode_type == Mode::BLOOM_PROBE && (_bloom_column.empty() || _bloom_literal.empty())) {
        return Status::InvalidArgument(
                "Properties 'bloom_column' and 'bloom_literal' must be set for "
                "parquet_bloom_probe");
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
    if (_file_type == TFileType::FILE_HDFS) {
        system_properties.hdfs_params = ::doris::parse_properties(system_properties.properties);
    }
    io::FileDescription file_desc;
    file_desc.path = path;
    io::FileReaderSPtr file_reader = DORIS_TRY(FileFactory::create_file_reader(
            system_properties, file_desc, io::FileReaderOptions::DEFAULT, nullptr));

    std::unique_ptr<FileMetaData> file_metadata;
    size_t meta_size = 0;
    io::IOContext io_ctx;
    RETURN_IF_ERROR(
            parse_thrift_footer(file_reader, &file_metadata, &meta_size, &io_ctx, true, true));

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
