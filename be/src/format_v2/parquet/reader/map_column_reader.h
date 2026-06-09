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
#include <utility>

#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/reader/nested_column_reader.h"

namespace doris::parquet {

class MapColumnReader final : public ParquetColumnReader {
public:
    MapColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                    std::unique_ptr<ParquetColumnReader> key_reader,
                    std::unique_ptr<ParquetColumnReader> value_reader)
            : ParquetColumnReader(schema, type),
              _key_reader(std::move(key_reader)),
              _value_reader(std::move(value_reader)) {}

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

    std::unique_ptr<ParquetColumnReader> _key_reader;
    std::unique_ptr<ParquetColumnReader> _value_reader;
    NestedScalarOverflow _key_overflow;
    NestedScalarOverflow _value_overflow;
    NestedStructOverflow _struct_value_overflow;
};

} // namespace doris::parquet
