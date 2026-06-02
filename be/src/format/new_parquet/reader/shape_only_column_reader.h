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
#include "format/new_parquet/reader/column_reader.h"

namespace doris::parquet {

class ShapeOnlyColumnReader final : public ParquetColumnReader {
public:
    explicit ShapeOnlyColumnReader(std::unique_ptr<ParquetColumnReader> source);

    int file_column_id() const override;
    int parquet_leaf_column_id() const override;
    const DataTypePtr& type() const override;
    const std::string& name() const override;

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;
    Status select(const SelectionVector& sel, uint16_t selected_rows, int64_t batch_rows,
                  MutableColumnPtr& column) override;

private:
    std::unique_ptr<ParquetColumnReader> _source;
};

class RowPositionColumnReader final : public ParquetColumnReader {
public:
    explicit RowPositionColumnReader(int64_t row_group_first_row);

    int file_column_id() const override;
    int parquet_leaf_column_id() const override;
    const DataTypePtr& type() const override;
    const std::string& name() const override;

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    int64_t _row_group_first_row = 0;
    int64_t _next_row_position = 0;
};

} // namespace doris::parquet
