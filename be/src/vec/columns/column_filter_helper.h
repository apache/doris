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

#include "column_nullable.h"

namespace doris::vectorized {
class ColumnFilterHelper {
public:
    ColumnFilterHelper(IColumn&);

    void resize_fill(size_t size, UInt8 value);
    void insert_value(UInt8 value);
    void reserve(size_t size);

    [[nodiscard]] size_t size() const { return _column.size(); }

private:
    ColumnNullable& _column;
    ColumnUInt8& _value_column;
    ColumnUInt8& _null_map_column;
};
} // namespace doris::vectorized