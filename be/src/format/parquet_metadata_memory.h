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

#include <cstddef>
#include <string>
#include <unordered_set>
#include <vector>

namespace tparquet {
class FileMetaData;
class SchemaElement;
} // namespace tparquet

namespace doris {
class IDataType;

// Estimated retained allocations, excluding the enclosing object's sizeof(). Capacities count
// reserved storage, so a vector with 1 used slot and 100 reserved slots is charged for all 100.
size_t parquet_metadata_dynamic_size(const tparquet::FileMetaData& metadata);
size_t parquet_schema_element_dynamic_size(const tparquet::SchemaElement& schema);
size_t parquet_type_memory_size(const IDataType* type, std::unordered_set<const IDataType*>* seen);

inline size_t parquet_string_memory_size(const std::string& value) {
    // Counting capacity also for short strings is deliberately conservative and portable across
    // libstdc++/libc++; inline storage is already in sizeof(std::string).
    return value.capacity() + 1;
}

template <typename Field>
size_t parquet_fields_dynamic_size(const std::vector<Field>& fields,
                                   std::unordered_set<const IDataType*>* seen) {
    size_t bytes = fields.capacity() * sizeof(Field);
    for (const auto& field : fields) {
        bytes += parquet_string_memory_size(field.name) +
                 parquet_string_memory_size(field.lower_case_name) +
                 parquet_schema_element_dynamic_size(field.parquet_schema) +
                 parquet_type_memory_size(field.data_type.get(), seen) +
                 parquet_fields_dynamic_size(field.children, seen);
        if constexpr (requires { field.variant_physical_type; }) {
            bytes += parquet_type_memory_size(field.variant_physical_type.get(), seen) +
                     parquet_string_memory_size(field.unsupported_reason);
        }
    }
    return bytes;
}
} // namespace doris
