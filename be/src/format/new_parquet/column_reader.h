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
#include "format/new_parquet/parquet_type.h"
#include "format/new_parquet/selection_vector.h"

namespace parquet {
class ColumnDescriptor;
class RowGroupReader;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace doris {
class IColumn;

namespace parquet {
struct ParquetColumnSchema;

// Doris 的 Parquet column reader 抽象。
// 该类包装 Arrow Parquet RecordReader，负责将 file-local Parquet leaf column 读取成
// Doris-owned column。它不理解 Iceberg/global schema，也不处理 table-level
// cast/default/generated/partition 语义。
class ParquetColumnReader {
public:
    virtual ~ParquetColumnReader() = default;

    // FileReader 暴露给上层 scan request 的 file-local column id。
    // 对 top-level primitive 列，它通常等于 Parquet leaf column id；对 struct/list/map
    // 这类复杂列，它表示 file schema tree 中的逻辑字段 id。
    virtual int file_column_id() const = 0;

    // Parquet 文件内部的 leaf column id，用于访问 RowGroupReader::RecordReader、
    // ColumnChunk metadata、statistics/page index 等 Parquet 物理列结构。
    // 只有 primitive leaf reader 有有效值；复杂列 reader 没有单一 leaf column，返回 -1。
    virtual int parquet_leaf_column_id() const = 0;

    virtual const DataTypePtr& type() const = 0;
    virtual const std::string& name() const = 0;

    // 读取一个 file-local column batch。
    virtual Status read(int64_t rows, MutableColumnPtr* column, int64_t* rows_read) = 0;

    // 跳过指定行数。这里必须使用 row-level skip，不能退回到 value-level Skip。
    virtual Status skip(int64_t rows);

    // 按 selection 读取当前 batch 中需要输出的行，并在末尾跳过 batch 内剩余行。
    // 该方法只允许通过 skip + read 推进 reader 游标，不允许退化为整批 read + filter。
    virtual Status select(const SelectionVector& sel, uint16_t selected_rows, int64_t batch_rows,
                          MutableColumnPtr* column);
};

// Parquet column reader 工厂。
// 工厂绑定当前 row group，并根据 file-local schema tree 创建 Doris 自己的 column
// reader。Arrow internal RecordReader 的创建和缓存必须封装在这里，避免泄露到
// ParquetReader 主流程。后续 reader options、Dremel assembler、延时物化 cache/skip
// 策略都应挂在该工厂上下文里，而不是继续扩展自由函数参数。
class ParquetColumnReaderFactory {
public:
    ParquetColumnReaderFactory(std::shared_ptr<::parquet::RowGroupReader> row_group,
                               int num_leaf_columns);

    // 根据 file-local schema tree 创建 column reader。复杂类型会在这里递归创建
    // children。该入口只理解 Parquet file schema，不处理 table/global schema。
    Status create(const ParquetColumnSchema& column_schema,
                  std::unique_ptr<ParquetColumnReader>* reader) const;

private:
    Status create_scalar_column_reader(const ParquetColumnSchema& column_schema,
                         std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_struct_column_reader(const ParquetColumnSchema& column_schema,
                         std::unique_ptr<ParquetColumnReader>* reader) const;

    Status get_record_reader(int leaf_column_id, const ::parquet::ColumnDescriptor* descriptor,
                             const std::string& name,
                             std::shared_ptr<::parquet::internal::RecordReader>* reader) const;

    Status create_scalar_reader(int parquet_leaf_column_id,
                                const ParquetTypeDescriptor& type_descriptor,
                                const ::parquet::ColumnDescriptor* descriptor, DataTypePtr type,
                                std::string name,
                                std::shared_ptr<::parquet::internal::RecordReader> record_reader,
                                std::unique_ptr<ParquetColumnReader>* reader) const;

    std::shared_ptr<::parquet::RowGroupReader> _row_group;
    mutable std::vector<std::shared_ptr<::parquet::internal::RecordReader>> _record_readers;
};

} // namespace parquet
} // namespace doris
