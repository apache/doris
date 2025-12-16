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

enum SchemaColumnIndex : size_t {
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
    SCHEMA_IS_NULLABLE,
    SCHEMA_COLUMN_COUNT
};

enum MetadataColumnIndex : size_t {
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
    META_STATISTICS_MAX,
    META_COLUMN_COUNT
};

inline constexpr std::array<const char*, SCHEMA_COLUMN_COUNT> kSchemaColumnNames = {
        "file_name",        "column_name",      "column_path", "physical_type", "logical_type",
        "repetition_level", "definition_level", "type_length", "precision",     "scale",
        "is_nullable"};

inline constexpr std::array<const char*, META_COLUMN_COUNT> kMetadataColumnNames = {
        "file_name",
        "row_group_id",
        "column_id",
        "column_name",
        "column_path",
        "physical_type",
        "logical_type",
        "type_length",
        "converted_type",
        "num_values",
        "null_count",
        "distinct_count",
        "encodings",
        "compression",
        "data_page_offset",
        "index_page_offset",
        "dictionary_page_offset",
        "total_compressed_size",
        "total_uncompressed_size",
        "statistics_min",
        "statistics_max"};

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

} // namespace doris::vectorized::parquet_utils
