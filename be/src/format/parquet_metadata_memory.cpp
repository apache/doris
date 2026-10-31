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

#include "format/parquet_metadata_memory.h"

#include <gen_cpp/parquet_types.h>

#include <algorithm>

#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"

namespace doris {
namespace {
template <typename T>
size_t vector_bytes(const std::vector<T>& values) {
    return values.capacity() * sizeof(T);
}

size_t string_vector_bytes(const std::vector<std::string>& values) {
    size_t bytes = vector_bytes(values);
    for (const auto& value : values) {
        bytes += parquet_string_memory_size(value);
    }
    return bytes;
}

size_t key_value_bytes(const std::vector<tparquet::KeyValue>& values) {
    size_t bytes = vector_bytes(values);
    for (const auto& value : values) {
        bytes += parquet_string_memory_size(value.key) + parquet_string_memory_size(value.value);
    }
    return bytes;
}

size_t column_dynamic_size(const tparquet::ColumnChunk& column) {
    const auto& meta = column.meta_data;
    const auto& stats = meta.statistics;
    const auto& encryption = column.crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY;
    return parquet_string_memory_size(column.file_path) +
           parquet_string_memory_size(column.encrypted_column_metadata) +
           string_vector_bytes(encryption.path_in_schema) +
           parquet_string_memory_size(encryption.key_metadata) + vector_bytes(meta.encodings) +
           string_vector_bytes(meta.path_in_schema) + key_value_bytes(meta.key_value_metadata) +
           parquet_string_memory_size(stats.min) + parquet_string_memory_size(stats.max) +
           parquet_string_memory_size(stats.min_value) +
           parquet_string_memory_size(stats.max_value) + vector_bytes(meta.encoding_stats) +
           vector_bytes(meta.size_statistics.repetition_level_histogram) +
           vector_bytes(meta.size_statistics.definition_level_histogram) +
           vector_bytes(meta.geospatial_statistics.geospatial_types);
}
} // namespace

size_t parquet_schema_element_dynamic_size(const tparquet::SchemaElement& schema) {
    return parquet_string_memory_size(schema.name) +
           parquet_string_memory_size(schema.logicalType.GEOMETRY.crs) +
           parquet_string_memory_size(schema.logicalType.GEOGRAPHY.crs);
}

size_t parquet_metadata_dynamic_size(const tparquet::FileMetaData& metadata) {
    size_t bytes = vector_bytes(metadata.schema) + vector_bytes(metadata.row_groups) +
                   key_value_bytes(metadata.key_value_metadata) +
                   vector_bytes(metadata.column_orders) +
                   parquet_string_memory_size(metadata.created_by) +
                   parquet_string_memory_size(metadata.footer_signing_key_metadata);
    const auto& algorithm = metadata.encryption_algorithm;
    bytes += parquet_string_memory_size(algorithm.AES_GCM_V1.aad_prefix) +
             parquet_string_memory_size(algorithm.AES_GCM_V1.aad_file_unique) +
             parquet_string_memory_size(algorithm.AES_GCM_CTR_V1.aad_prefix) +
             parquet_string_memory_size(algorithm.AES_GCM_CTR_V1.aad_file_unique);
    for (const auto& schema : metadata.schema) {
        bytes += parquet_schema_element_dynamic_size(schema);
    }
    for (const auto& group : metadata.row_groups) {
        bytes += vector_bytes(group.columns) + vector_bytes(group.sorting_columns);
        for (const auto& column : group.columns) {
            bytes += column_dynamic_size(column);
        }
    }
    return bytes;
}

size_t parquet_type_memory_size(const IDataType* type, std::unordered_set<const IDataType*>* seen) {
    if (type == nullptr || !seen->insert(type).second) {
        return 0;
    }
    // Scalar types have only fixed-size state. Use the larger scalar carrier as an estimate,
    // and allow two pointers for the shared_ptr control block. Shared nested types count once.
    size_t bytes =
            std::max(sizeof(DataTypeDecimal256), sizeof(DataTypeVariantV2)) + 2 * sizeof(void*);
    if (type->is_nullable()) {
        return sizeof(DataTypeNullable) + 2 * sizeof(void*) +
               parquet_type_memory_size(
                       assert_cast<const DataTypeNullable*>(type)->get_nested_type().get(), seen);
    }
    switch (type->get_primitive_type()) {
    case TYPE_ARRAY:
        return sizeof(DataTypeArray) + 2 * sizeof(void*) +
               parquet_type_memory_size(
                       assert_cast<const DataTypeArray*>(type)->get_nested_type().get(), seen);
    case TYPE_MAP: {
        const auto* map = assert_cast<const DataTypeMap*>(type);
        return sizeof(DataTypeMap) + 2 * sizeof(void*) +
               parquet_type_memory_size(map->get_key_type().get(), seen) +
               parquet_type_memory_size(map->get_value_type().get(), seen);
    }
    case TYPE_STRUCT: {
        const auto* structure = assert_cast<const DataTypeStruct*>(type);
        bytes = sizeof(DataTypeStruct) + 2 * sizeof(void*) +
                vector_bytes(structure->get_elements()) +
                string_vector_bytes(structure->get_element_names());
        for (const auto& child : structure->get_elements()) {
            bytes += parquet_type_memory_size(child.get(), seen);
        }
        break;
    }
    default:
        break;
    }
    return bytes;
}
} // namespace doris
