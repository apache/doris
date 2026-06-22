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
#include "core/data_type_serde/decoded_column_view.h"

namespace parquet {
class ColumnDescriptor;
} // namespace parquet

namespace doris::format::parquet {

// ============================================================================
// Parquet 额外类型编码信息
// ============================================================================
//
// Doris 的 DataType 只能表达最终展示类型（如 Decimal(10,2)、DATETIMEV2(6)），
// 但 reader 读值时还需要知道 Parquet 物理存储方式。
// ParquetExtraTypeInfo 补全了这个信息。

enum class ParquetExtraTypeInfo {
    NONE,               // 无特殊编码，按物理类型直接读取
    DECIMAL_INT32,      // decimal 存为 4-byte big-endian int
    DECIMAL_INT64,      // decimal 存为 8-byte big-endian int
    DECIMAL_BYTE_ARRAY, // decimal 存为变长或定长 big-endian byte array
    UNIT_MS,            // 时间精度为毫秒
    UNIT_MICROS,        // 时间精度为微秒
    UNIT_NS,            // 时间精度为纳秒
    IMPALA_TIMESTAMP,   // INT96 格式的 Impala 兼容 timestamp
    FLOAT16,            // 半精度浮点（FIXED_LEN_BYTE_ARRAY(2) → Float32）
};

enum class ParquetTimeUnit {
    UNKNOWN,
    MILLIS,
    MICROS,
    NANOS,
};

// ============================================================================
// Parquet 类型解析结果 — resolve_parquet_type() 的输出
// ============================================================================
//
// 将 Arrow ColumnDescriptor（physical_type + logical_type + converted_type）
// 解析为 Doris DataType + 读值时需要的全部编码信息。
//
// 三级解析优先级：logical_type（优先）→ converted_type（次）→ physical_type（兜底）
//
// 关键字段说明：
//   doris_type        — Doris 侧的最终类型（如 DECIMAL128(10,2)、DATETIMEV2(6)）
//   extra_type_info   — 物理编码方式（如 DECIMAL_INT32、IMPALA_TIMESTAMP）
//   physical_type     — Parquet 物理类型（INT32/INT64/BYTE_ARRAY/...）
//   is_string_like    — 物理类型是 binary 且不是 decimal/FLOAT16 → 归为 string-like
//   supports_record_reader — 是否可以通过 Arrow RecordReader 读取（当前全部支持）
struct ParquetTypeDescriptor {
    DataTypePtr doris_type;
    ParquetExtraTypeInfo extra_type_info = ParquetExtraTypeInfo::NONE;
    ParquetTimeUnit time_unit = ParquetTimeUnit::UNKNOWN;
    ::parquet::Type::type physical_type = ::parquet::Type::UNDEFINED;
    ::parquet::ConvertedType::type converted_type = ::parquet::ConvertedType::UNDEFINED;
    int integer_bit_width = -1;                // INT_8/16/32/64 的位宽
    int decimal_precision = -1;                // DECIMAL(p,s) 的精度
    int decimal_scale = -1;                    // DECIMAL(p,s) 的小数位
    int fixed_length = -1;                     // FIXED_LEN_BYTE_ARRAY 的固定长度
    bool is_unsigned_integer = false;          // 是否 unsigned 整数（UINT_8/16/32/64）
    bool is_decimal = false;                   // 是否 decimal 类型
    bool is_timestamp = false;                 // 是否 timestamp 类型
    bool timestamp_is_adjusted_to_utc = false; // timestamp 是否已 UTC 归一化
    bool is_string_like = false;               // binary 但不是 decimal/FLOAT16
    bool supports_record_reader = true;        // 能否通过 Arrow RecordReader 读取
    std::string unsupported_reason;            // 非空表示该 Parquet 逻辑类型暂不支持
};

// 返回 Parquet leaf column 的 file-local 展示名（如 "a.b.c"）。
std::string parquet_column_name(const ::parquet::ColumnDescriptor* column);

// 将 Parquet ColumnDescriptor 解析为 ParquetTypeDescriptor。
// 不做 table schema evolution：类型提升和 default/generated/partition 列由 TableReader 处理。
ParquetTypeDescriptor resolve_parquet_type(const ::parquet::ColumnDescriptor* column);

// 判断该类型是否可以通过 Arrow Parquet RecordReader 读取。
// 当前所有已知物理类型均支持，预留扩展性。
bool supports_record_reader(const ParquetTypeDescriptor& type_descriptor);

// 返回该类型的值在 Arrow RecordReader 中的解码方式。
// 用于 ParquetLeafReader 确定按 INT32/INT64/FLOAT/BINARY 等哪种格式解读 values buffer。
DecodedValueKind decoded_value_kind(const ParquetTypeDescriptor& type_descriptor);

} // namespace doris::format::parquet
