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
#include <string>

#include "format_v2/column_data.h"
#include "format_v2/parquet/reader/column_reader.h"

namespace doris::format::parquet {

// 虚拟列 reader：生成行的全局唯一 RowId。
//
// 不对应任何 Parquet 物理列，不持有 RecordReader。
// RowId 编码格式：<version:1byte><backend_id:8bytes><file_id:4bytes><row_id:4bytes>，
// 共 17 bytes，以 String 类型输出。
//
// row_id = _row_group_first_row + _next_row_position，随 read() 递增。
// 用于 TopN filter 等需要跨文件唯一定位行的场景。
class GlobalRowIdColumnReader final : public ParquetColumnReader {
public:
    GlobalRowIdColumnReader(format::GlobalRowIdContext context, int64_t row_group_first_row,
                            ParquetColumnReaderProfile profile = {});

    int file_column_id() const override;
    int parquet_leaf_column_id() const override;
    const DataTypePtr& type() const override;
    const std::string& name() const override;

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    // 将单个 row_id 编码为 17-byte RowId 字符串并追加到 column。
    void append_row_id(uint32_t row_id, MutableColumnPtr& column) const;

    format::GlobalRowIdContext _context; // RowId 前缀（version + backend_id + file_id）
    int64_t _row_group_first_row = 0;    // 当前 RG 在文件中的起始行号
    int64_t _next_row_position = 0;      // 下一个待输出的行位置
};

} // namespace doris::format::parquet
