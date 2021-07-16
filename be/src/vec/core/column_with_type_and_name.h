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

#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

// class WriteBuffer;

/** Column data along with its data type and name.
  * Column data could be nullptr - to represent just 'header' of column.
  * Name could be either name from a table or some temporary generated name during expression evaluation.
  */

struct ColumnWithTypeAndName {
    ColumnPtr column;
    DataTypePtr type;
    String name;

    ColumnWithTypeAndName() {}
    ColumnWithTypeAndName(const ColumnPtr& column_, const DataTypePtr& type_, const String& name_)
            : column(column_), type(type_), name(name_) {}

    /// Uses type->create_column() to create column
    ColumnWithTypeAndName(const DataTypePtr& type_, const String& name_)
            : column(type_->create_column()), type(type_), name(name_) {}

    ColumnWithTypeAndName clone_empty() const;
    bool operator==(const ColumnWithTypeAndName& other) const;

    void dump_structure(std::ostream& out) const;
    String dump_structure() const;
    std::string to_string(size_t row_num) const;
};

} // namespace doris::vectorized
