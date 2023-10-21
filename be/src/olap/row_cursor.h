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

#include <butil/macros.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/olap_tuple.h"
#include "olap/row_cursor_cell.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"

namespace doris {
class Field;

// Delegate the operation of a row of data
class RowCursor {
public:
    static const int DEFAULT_TEXT_LENGTH = 128;

    RowCursor();

    // Traverse and destroy the field cursor
    ~RowCursor();

    // Create a RowCursor based on the schema
    Status init(TabletSchemaSPtr schema);
    Status init(const std::vector<TabletColumn>& schema);

    // Create a RowCursor based on the first n columns of the schema
    Status init(const std::vector<TabletColumn>& schema, size_t column_count);
    Status init(TabletSchemaSPtr schema, size_t column_count);

    // Create a RowCursor based on the schema and column id list
    // which is used for the calculation process only uses some discontinuous prefix columns
    Status init(TabletSchemaSPtr schema, const std::vector<uint32_t>& columns);

    // Initialize with the size of the key, currently only used when splitting the range of key
    Status init_scan_key(TabletSchemaSPtr schema, const std::vector<std::string>& keys);

    Status init_scan_key(TabletSchemaSPtr schema, const std::vector<std::string>& keys,
                         const std::shared_ptr<Schema>& shared_schema);

    RowCursorCell cell(uint32_t cid) const { return RowCursorCell(nullable_cell_ptr(cid)); }

    // RowCursor received a continuous buf
    void attach(char* buf) { _fixed_buf = buf; }

    // Deserialize the value of each field from the string array,
    // Each array item must be a \0 terminated string
    // and the input string and line cursor need the same number of columns
    Status from_tuple(const OlapTuple& tuple);

    // Returns the number of columns in the current row cursor
    size_t field_count() const { return _schema->column_ids().size(); }

    // Output row cursor content in string format, only for using of log and debug
    std::string to_string() const;
    OlapTuple to_tuple() const;

    bool is_delete() const {
        auto sign_idx = _schema->delete_sign_idx();
        if (sign_idx < 0) {
            return false;
        }
        return *reinterpret_cast<const char*>(cell(sign_idx).cell_ptr()) > 0;
    }

    // set max/min for key field in _field_array
    Status build_max_key();
    Status build_min_key();

    char* get_buf() const { return _fixed_buf; }

    // this two functions is used in unit test
    size_t get_fixed_len() const { return _fixed_len; }
    size_t get_variable_len() const { return _variable_len; }

    // Get column nullable pointer with column id
    // TODO(zc): make this return const char*
    char* nullable_cell_ptr(uint32_t cid) const { return _fixed_buf + _schema->column_offset(cid); }
    char* cell_ptr(uint32_t cid) const { return _fixed_buf + _schema->column_offset(cid) + 1; }

    bool is_null(size_t index) const { return *reinterpret_cast<bool*>(nullable_cell_ptr(index)); }

    void set_null(size_t index) const { *reinterpret_cast<bool*>(nullable_cell_ptr(index)) = true; }

    void set_not_null(size_t index) const {
        *reinterpret_cast<bool*>(nullable_cell_ptr(index)) = false;
    }

    size_t column_size(uint32_t cid) const { return _schema->column_size(cid); }

    const Field* column_schema(uint32_t cid) const { return _schema->column(cid); }

    const Schema* schema() const { return _schema.get(); }

    char* row_ptr() const { return _fixed_buf; }

private:
    Status _init(const std::vector<uint32_t>& columns);
    Status _init(const std::shared_ptr<Schema>& shared_schema,
                 const std::vector<uint32_t>& columns);
    // common init function
    Status _init(const std::vector<TabletColumn>& schema, const std::vector<uint32_t>& columns);
    Status _alloc_buf();

    Status _init_scan_key(TabletSchemaSPtr schema, const std::vector<std::string>& scan_keys);

    std::unique_ptr<Schema> _schema;

    char* _fixed_buf = nullptr; // point to fixed buf
    size_t _fixed_len;
    char* _owned_fixed_buf = nullptr; // point to buf allocated in init function

    char* _variable_buf = nullptr;
    size_t _variable_len;
    size_t _string_field_count;
    char** _long_text_buf;

    DISALLOW_COPY_AND_ASSIGN(RowCursor);
};
} // namespace doris
