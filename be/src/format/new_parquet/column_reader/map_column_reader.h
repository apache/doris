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

#include "format/new_parquet/column_reader/column_reader.h"
#include "format/new_parquet/column_reader/nested_level_assembler.h"
#include "format/new_parquet/parquet_column_schema.h"

namespace doris::parquet {

class MapColumnReader final : public ParquetColumnReader {
public:
    MapColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                    std::unique_ptr<ParquetColumnReader> key_reader,
                    std::unique_ptr<ParquetColumnReader> value_reader)
            : _field_id(schema.top_level_field_id),
              _nullable_definition_level(schema.nullable_definition_level),
              _repeated_repetition_level(schema.repeated_repetition_level),
              _type(std::move(type)),
              _name(schema.name),
              _key_reader(std::move(key_reader)),
              _value_reader(std::move(value_reader)) {}

    int file_column_id() const override { return _field_id; }
    int parquet_leaf_column_id() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

    int _field_id = -1;
    int16_t _nullable_definition_level = 0;
    int16_t _repeated_repetition_level = 0;
    DataTypePtr _type;
    std::string _name;
    std::unique_ptr<ParquetColumnReader> _key_reader;
    std::unique_ptr<ParquetColumnReader> _value_reader;
    NestedScalarOverflow _key_overflow;
    NestedScalarOverflow _value_overflow;
    NestedStructOverflow _struct_value_overflow;
};

} // namespace doris::parquet
