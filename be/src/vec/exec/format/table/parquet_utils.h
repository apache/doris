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

#pragma once

#include <parquet/statistics.h>

#include <array>
#include <string>
#include <unordered_map>
#include <vector>

#include "cctz/time_zone.h"
#include "vec/core/types.h"
#include "vec/exec/format/parquet/schema_desc.h"

namespace doris::vectorized::parquet_utils {

inline constexpr const char* MODE_SCHEMA = "parquet_schema";
inline constexpr const char* MODE_METADATA = "parquet_metadata";
inline constexpr const char* MODE_FILE_METADATA = "parquet_file_metadata";
inline constexpr const char* MODE_KEY_VALUE_METADATA = "parquet_kv_metadata";
inline constexpr const char* MODE_BLOOM_PROBE = "parquet_bloom_probe";

enum SchemaColumnIndex : size_t {
    SCHEMA_FILE_NAME = 0,
    SCHEMA_NAME,
    SCHEMA_TYPE,
    SCHEMA_TYPE_LENGTH,
    SCHEMA_REPETITION_TYPE,
    SCHEMA_NUM_CHILDREN,
    SCHEMA_CONVERTED_TYPE,
    SCHEMA_SCALE,
    SCHEMA_PRECISION,
    SCHEMA_FIELD_ID,
    SCHEMA_LOGICAL_TYPE,
    SCHEMA_COLUMN_COUNT
};

enum MetadataColumnIndex : size_t {
    META_FILE_NAME = 0,
    META_ROW_GROUP_ID,
    META_ROW_GROUP_NUM_ROWS,
    META_ROW_GROUP_NUM_COLUMNS,
    META_ROW_GROUP_BYTES,
    META_COLUMN_ID,
    META_FILE_OFFSET,
    META_NUM_VALUES,
    META_PATH_IN_SCHEMA,
    META_TYPE,
    META_STATS_MIN,
    META_STATS_MAX,
    META_STATS_NULL_COUNT,
    META_STATS_DISTINCT_COUNT,
    META_STATS_MIN_VALUE,
    META_STATS_MAX_VALUE,
    META_COMPRESSION,
    META_ENCODINGS,
    META_INDEX_PAGE_OFFSET,
    META_DICTIONARY_PAGE_OFFSET,
    META_DATA_PAGE_OFFSET,
    META_TOTAL_COMPRESSED_SIZE,
    META_TOTAL_UNCOMPRESSED_SIZE,
    META_KEY_VALUE_METADATA,
    META_BLOOM_FILTER_OFFSET,
    META_BLOOM_FILTER_LENGTH,
    META_MIN_IS_EXACT,
    META_MAX_IS_EXACT,
    META_ROW_GROUP_COMPRESSED_BYTES,
    META_COLUMN_COUNT
};

enum FileMetadataColumnIndex : size_t {
    FILE_META_FILE_NAME = 0,
    FILE_META_CREATED_BY,
    FILE_META_NUM_ROWS,
    FILE_META_NUM_ROW_GROUPS,
    FILE_META_FORMAT_VERSION,
    FILE_META_ENCRYPTION_ALGORITHM,
    FILE_META_FOOTER_SIGNING_KEY_METADATA,
    FILE_META_COLUMN_COUNT
};

enum KeyValueColumnIndex : size_t { KV_FILE_NAME = 0, KV_KEY, KV_VALUE, KV_COLUMN_COUNT };

enum BloomProbeColumnIndex : size_t {
    BLOOM_FILE_NAME = 0,
    BLOOM_ROW_GROUP_ID,
    BLOOM_EXCLUDES,
    BLOOM_COLUMN_COUNT
};

inline constexpr std::array<const char*, SCHEMA_COLUMN_COUNT> kSchemaColumnNames = {
        "file_name",      "name",  "type",      "type_length", "repetition_type", "num_children",
        "converted_type", "scale", "precision", "field_id",    "logical_type"};

inline constexpr std::array<const char*, META_COLUMN_COUNT> kMetadataColumnNames = {
        "file_name",
        "row_group_id",
        "row_group_num_rows",
        "row_group_num_columns",
        "row_group_bytes",
        "column_id",
        "file_offset",
        "num_values",
        "path_in_schema",
        "type",
        "stats_min",
        "stats_max",
        "stats_null_count",
        "stats_distinct_count",
        "stats_min_value",
        "stats_max_value",
        "compression",
        "encodings",
        "index_page_offset",
        "dictionary_page_offset",
        "data_page_offset",
        "total_compressed_size",
        "total_uncompressed_size",
        "key_value_metadata",
        "bloom_filter_offset",
        "bloom_filter_length",
        "min_is_exact",
        "max_is_exact",
        "row_group_compressed_bytes"};

inline constexpr std::array<const char*, FILE_META_COLUMN_COUNT> kFileMetadataColumnNames = {
        "file_name",
        "created_by",
        "num_rows",
        "num_row_groups",
        "format_version",
        "encryption_algorithm",
        "footer_signing_key_metadata"};

inline constexpr std::array<const char*, KV_COLUMN_COUNT> kKeyValueColumnNames = {"file_name",
                                                                                  "key", "value"};

inline constexpr std::array<const char*, BLOOM_COLUMN_COUNT> kBloomProbeColumnNames = {
        "file_name", "row_group_id", "bloom_filter_excludes"};

std::string join_path(const std::vector<std::string>& items);

void insert_int32(MutableColumnPtr& column, Int32 value);
void insert_int64(MutableColumnPtr& column, Int64 value);
void insert_bool(MutableColumnPtr& column, bool value);
void insert_string(MutableColumnPtr& column, const std::string& value);
void insert_null(MutableColumnPtr& column);

std::string physical_type_to_string(tparquet::Type::type type);
std::string compression_to_string(tparquet::CompressionCodec::type codec);
std::string converted_type_to_string(tparquet::ConvertedType::type type);
std::string logical_type_to_string(const tparquet::SchemaElement& element);
std::string encodings_to_string(const std::vector<tparquet::Encoding::type>& encodings);

bool try_get_statistics_encoded_value(const tparquet::Statistics& statistics, bool is_min,
                                      std::string* encoded_value);
std::string bytes_to_hex_string(const std::string& bytes);
std::string decode_statistics_value(const FieldSchema* schema_field,
                                    tparquet::Type::type physical_type,
                                    const std::string& encoded_value, const cctz::time_zone& ctz);

void build_path_map(const FieldSchema& field, const std::string& prefix,
                    std::unordered_map<std::string, const FieldSchema*>* map);

void merge_stats(const std::shared_ptr<::parquet::Statistics>& left,
                 const std::shared_ptr<::parquet::Statistics>& right);

} // namespace doris::vectorized::parquet_utils
