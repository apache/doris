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
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"

namespace parquet {
class ColumnDescriptor;
class ColumnReader;
} // namespace parquet

namespace doris {
class IColumn;

namespace parquet {
struct ParquetColumnSchema;

// 返回 Parquet leaf column 的 file-local 展示名。
std::string column_name(const ::parquet::ColumnDescriptor* column);

// 将 Parquet file-local column descriptor 映射成 Doris file-local 类型。
// 这里不做 table schema evolution；类型提升和 default/generated/partition 列由 table
// 层处理。
DataTypePtr parquet_column_to_doris_type(const ::parquet::ColumnDescriptor* column);

// 判断当前阶段是否可以直接通过 Arrow Parquet core ColumnReader 读取该列。
// 阶段一只支持 flat primitive/string/decimal/timestamp。
DataTypePtr supported_flat_column_type(const ::parquet::ColumnDescriptor* column);

// Doris 的 Parquet column reader 抽象。
// 该类包装 Arrow Parquet core ColumnReader，负责将 file-local Parquet leaf column 读取成
// Doris-owned column。它不理解 Iceberg/global schema，也不处理 table-level
// cast/default/generated/partition 语义。
class ParquetColumnReader {
public:
    virtual ~ParquetColumnReader() = default;

    virtual int file_column_id() const = 0;
    virtual int parquet_column_ordinal() const = 0;
    virtual const DataTypePtr& type() const = 0;
    virtual const std::string& name() const = 0;

    // 读取一个 file-local column batch。当前实现只支持顺序读取；后续延时物化会在该
    // 抽象下扩展 skip/selective read/cache 等能力。
    virtual Status read_batch(int64_t batch_rows, MutableColumnPtr* result_column,
                              int64_t* rows_read) = 0;

    // 跳过指定行数。阶段一暂未实现，接口先保留给延时物化和复杂类型读取。
    virtual Status skip(int64_t rows);
};

// Parquet column reader 工厂。
// 工厂绑定当前 row group 的 Arrow Parquet core ColumnReader 列表，并根据 file-local
// schema tree 创建 Doris 自己的 column reader。后续 reader options、Dremel assembler、
// 延时物化 cache/skip 策略都应挂在该工厂上下文里，而不是继续扩展自由函数参数。
class ParquetColumnReaderFactory {
public:
    explicit ParquetColumnReaderFactory(
            const std::vector<std::shared_ptr<::parquet::ColumnReader>>& arrow_readers);

    // 根据 file-local schema tree 创建 column reader。复杂类型会在这里递归创建
    // children。该入口只理解 Parquet file schema，不处理 table/global schema。
    Status create(const ParquetColumnSchema& column_schema,
                  std::unique_ptr<ParquetColumnReader>* reader) const;

private:
    Status create_primitive(const ParquetColumnSchema& column_schema,
                            std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_struct(const ParquetColumnSchema& column_schema,
                         std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_primitive_reader(int file_column_id,
                                   const ::parquet::ColumnDescriptor* descriptor,
                                   DataTypePtr type, std::string name,
                                   std::shared_ptr<::parquet::ColumnReader> arrow_reader,
                                   std::unique_ptr<ParquetColumnReader>* reader) const;

    const std::vector<std::shared_ptr<::parquet::ColumnReader>>& _arrow_readers;
};

} // namespace parquet
} // namespace doris
