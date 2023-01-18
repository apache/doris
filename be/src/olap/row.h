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

#include <sstream>
#include <string>

#include "olap/row_cursor_cell.h"
#include "olap/schema.h"

namespace doris {

class MemPool;
class Arena;

// The row has all columns layed out in memory based on the schema.column_offset()
struct ContiguousRow {
    ContiguousRow(const Schema* schema, const void* row) : _schema(schema), _row((void*)row) {}
    ContiguousRow(const Schema* schema, void* row) : _schema(schema), _row(row) {}
    RowCursorCell cell(uint32_t cid) const {
        return RowCursorCell((char*)_row + _schema->column_offset(cid));
    }
    void set_is_null(uint32_t cid, bool is_null) const { _schema->set_is_null(_row, cid, is_null); }
    const Schema* schema() const { return _schema; }
    void* row_ptr() const { return _row; }

private:
    const Schema* _schema;
    void* _row;
};

} // namespace doris
