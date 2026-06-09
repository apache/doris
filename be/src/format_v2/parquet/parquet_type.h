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

#include <parquet/types.h>

#include <string>

#include "core/data_type/data_type.h"

namespace parquet {
class ColumnDescriptor;
} // namespace parquet

namespace doris::parquet {

// Parquet logical/converted annotation 解析后留下的额外编码信息。
// 这对应 DuckDB ParquetColumnSchema::type_info：Doris type 只能表达最终展示类型，
// 读值时还需要知道 decimal/timestamp/time 在 Parquet 中的物理编码方式。
enum class ParquetExtraTypeInfo {
    NONE,
    DECIMAL_INT32,
    DECIMAL_INT64,
    DECIMAL_BYTE_ARRAY,
    UNIT_MS,
    UNIT_MICROS,
    UNIT_NS,
    IMPALA_TIMESTAMP,
};

enum class ParquetTimeUnit {
    UNKNOWN,
    MILLIS,
    MICROS,
    NANOS,
};

// Parquet file-local column descriptor 的类型解析结果。
// 该结构只解释 Parquet physical/logical/converted type，不包含 table/global schema
// evolution，也不依赖 Arrow internal RecordReader API。
struct ParquetTypeDescriptor {
    DataTypePtr doris_type;
    ParquetExtraTypeInfo extra_type_info = ParquetExtraTypeInfo::NONE;
    ParquetTimeUnit time_unit = ParquetTimeUnit::UNKNOWN;
    ::parquet::Type::type physical_type = ::parquet::Type::UNDEFINED;
    ::parquet::ConvertedType::type converted_type = ::parquet::ConvertedType::UNDEFINED;
    int integer_bit_width = -1;
    int decimal_precision = -1;
    int decimal_scale = -1;
    int fixed_length = -1;
    bool is_unsigned_integer = false;
    bool is_decimal = false;
    bool is_timestamp = false;
    bool is_string_like = false;
    bool supports_record_reader = true;
    std::string reason; // reason if supports_record_reader is false
};

// 返回 Parquet leaf column 的 file-local 展示名。
std::string parquet_column_name(const ::parquet::ColumnDescriptor* column);

// 将 Parquet file-local column descriptor 解析成 Doris file-local 类型和读值所需的
// 编码信息。这里不做 table schema evolution；类型提升和 default/generated/partition
// 列由 table 层处理。
ParquetTypeDescriptor resolve_parquet_type(const ::parquet::ColumnDescriptor* column);

// 判断当前阶段是否可以通过 Arrow Parquet RecordReader 读取该列。
// 当前支持 flat primitive/string/decimal/timestamp。复杂 nested column 仍通过 children
// 递归组合，list/map assembler 后续补齐。
bool supports_record_reader(const ParquetTypeDescriptor& type_descriptor);

} // namespace doris::parquet
