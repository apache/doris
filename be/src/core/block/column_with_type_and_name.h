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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/ColumnWithTypeAndName.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <iosfwd>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/types.h"

namespace doris {
class ColumnNullable;
class PColumnMeta;
} // namespace doris

namespace doris {

struct NullableColumnInfo {
    bool has_null = false;
    bool only_null = false;
    bool is_const = false;
    bool is_nullable = false;
};

using NullableColumnInfos = std::vector<NullableColumnInfo>;

// class WriteBuffer;

/** Column data along with its data type and name.
  * Column data could be nullptr - to represent just 'header' of column.
  * Name could be either name from a table or some temporary generated name during expression evaluation.
  */

struct ColumnWithTypeAndName {
    ColumnPtr column;
    DataTypePtr type;
    String name;

    ColumnWithTypeAndName() = default;
    ColumnWithTypeAndName(ColumnPtr column_, DataTypePtr type_, String name_)
            : column(std::move(column_)), type(std::move(type_)), name(std::move(name_)) {}

    /// Uses type->create_column() to create column
    ColumnWithTypeAndName(const DataTypePtr& type_, String name_)
            : column(type_->create_column()), type(type_), name(std::move(name_)) {}

    ColumnWithTypeAndName clone_empty() const;
    bool operator==(const ColumnWithTypeAndName& other) const;

    void dump_structure(std::ostream& out) const;
    String dump_structure() const;
    std::string to_string(size_t row_num, const DataTypeSerDe::FormatOptions& format_options) const;
#ifdef BE_TEST
    std::string to_string(size_t row_num) const;
#endif

    void to_pb_column_meta(PColumnMeta* col_meta) const;

    const ColumnUInt8::Ptr& get_nullable_null_map_column() const;
    NullableColumnInfo get_nullable_column_info() const;
    ColumnWithTypeAndName unnest_nullable(const NullableColumnInfo& info,
                                          bool replace_null_data_to_default) const;

    Status check_type_and_column_match() const;

private:
    const ColumnNullable& get_nullable_column() const;
};

} // namespace doris
