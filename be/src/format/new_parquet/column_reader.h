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

#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"
#include "core/data_type/data_type.h"

namespace parquet {
class ColumnDescriptor;
class ColumnReader;
} // namespace parquet

namespace doris {
class IColumn;

namespace parquet {

// 单个 file-local Parquet leaf column 的读取状态。
// 该状态包装 Arrow Parquet core ColumnReader，只负责把物理列读取成 Doris column；
// 不解释 Iceberg/global schema，也不处理 table-level cast/default/generated 语义。
struct ParquetColumnReaderState {
    int file_column_id = -1;
    int parquet_column_ordinal = -1;
    const ::parquet::ColumnDescriptor* descriptor = nullptr;
    DataTypePtr type;
    std::string name;
    std::shared_ptr<::parquet::ColumnReader> reader;
};

// 返回 Parquet leaf column 的 file-local 展示名。
std::string column_name(const ::parquet::ColumnDescriptor* column);

// 将 Parquet file-local column descriptor 映射成 Doris file-local 类型。
// 这里不做 table schema evolution；类型提升和 default/generated/partition 列由 table
// 层处理。
DataTypePtr parquet_column_to_doris_type(const ::parquet::ColumnDescriptor* column);

// 判断当前阶段是否可以直接通过 Arrow Parquet core ColumnReader 读取该列。
// 阶段一只支持 flat primitive/string/decimal/timestamp。
DataTypePtr supported_flat_column_type(const ::parquet::ColumnDescriptor* column);

// 读取一个 file-local column batch，并写入 Doris-owned mutable column。
Status read_column_batch(ParquetColumnReaderState& column_state, int64_t batch_rows,
                         MutableColumnPtr* result_column, int64_t* rows_read);

} // namespace parquet
} // namespace doris
