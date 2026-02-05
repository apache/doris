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
#include "common/compile_check_begin.h"
class Field;

// Delegate the operation of a row of data
class RowCursor {
public:
    static const int DEFAULT_TEXT_LENGTH = 128;

    RowCursor();

    // Traverse and destroy the field cursor
    ~RowCursor();

    // Initialize with the size of the key, currently only used when splitting the range of key
    Status init_scan_key(TabletSchemaSPtr schema, const std::vector<std::string>& keys);

    Status init_scan_key(TabletSchemaSPtr schema, const std::vector<std::string>& keys,
                         const std::shared_ptr<Schema>& shared_schema);

    RowCursorCell cell(uint32_t cid) const { return RowCursorCell(_nullable_cell_ptr(cid)); }

    // Deserialize the value of each field from the string array,
    // Each array item must be a \0 terminated string
    // and the input string and line cursor need the same number of columns
    Status from_tuple(const OlapTuple& tuple);

    // Output row cursor content in string format
    std::string to_string() const;

    // this two functions is used in unit test
    size_t get_fixed_len() const { return _fixed_len; }
    size_t get_variable_len() const { return _variable_len; }

    const Field* column_schema(uint32_t cid) const { return _schema->column(cid); }

    const Schema* schema() const { return _schema.get(); }

private:
    Status _init(TabletSchemaSPtr schema, uint32_t column_count);
    Status _init(const std::vector<uint32_t>& columns);
    Status _init(const std::shared_ptr<Schema>& shared_schema,
                 const std::vector<uint32_t>& columns);
    // common init function
    Status _init(const std::vector<TabletColumnPtr>& schema, const std::vector<uint32_t>& columns);
    Status _alloc_buf();

    Status _init_scan_key(TabletSchemaSPtr schema, const std::vector<std::string>& scan_keys);

    // Get column nullable pointer with column id
    // TODO(zc): make this return const char*
    char* _nullable_cell_ptr(uint32_t cid) const {
        return _fixed_buf + _schema->column_offset(cid);
    }
    char* _cell_ptr(uint32_t cid) const { return _fixed_buf + _schema->column_offset(cid) + 1; }

    void _set_null(uint32_t index) const {
        *reinterpret_cast<bool*>(_nullable_cell_ptr(index)) = true;
    }

    void _set_not_null(uint32_t index) const {
        *reinterpret_cast<bool*>(_nullable_cell_ptr(index)) = false;
    }

    bool _is_null(uint32_t index) const {
        return *reinterpret_cast<bool*>(_nullable_cell_ptr(index));
    }

    std::unique_ptr<Schema> _schema;

    char* _fixed_buf = nullptr; // point to fixed buf
    size_t _fixed_len;
    char* _owned_fixed_buf = nullptr; // point to buf allocated in init function

    char* _variable_buf = nullptr;
    size_t _variable_len;
    size_t _string_field_count;
    char** _long_text_buf = nullptr;

    std::vector<std::string> _row_string;

    DISALLOW_COPY_AND_ASSIGN(RowCursor);
};
#include "common/compile_check_end.h"
} // namespace doris
