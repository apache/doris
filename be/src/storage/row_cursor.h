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

#include "common/consts.h"
#include "common/status.h"
#include "storage/olap_tuple.h"
#include "storage/row_cursor_cell.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
#include "common/compile_check_begin.h"
class StorageField;

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

    OlapTuple to_tuple() const;

    bool is_delete() const {
        auto sign_idx = _schema->delete_sign_idx();
        if (sign_idx < 0) {
            return false;
        }
        return *reinterpret_cast<const char*>(cell(sign_idx).cell_ptr()) > 0;
    }

    char* get_buf() const { return _fixed_buf; }

    // this two functions is used in unit test
    size_t get_fixed_len() const { return _fixed_len; }
    size_t get_variable_len() const { return _variable_len; }

    const StorageField* column_schema(uint32_t cid) const { return _schema->column(cid); }

    const Schema* schema() const { return _schema.get(); }

    // Encode one row into binary according given num_keys.
    // A cell will be encoded in the format of a marker and encoded content.
    // When encoding row, if any cell isn't found in row, this function will
    // fill a marker and return. If padding_minimal is true, KEY_MINIMAL_MARKER will
    // be added, if padding_minimal is false, KEY_MAXIMAL_MARKER will be added.
    // If all num_keys are found in row, no marker will be added.
    template <bool is_mow = false>
    void encode_key_with_padding(std::string* buf, size_t num_keys, bool padding_minimal) const {
        for (uint32_t cid = 0; cid < num_keys; cid++) {
            auto field = _schema->column(cid);
            if (field == nullptr) {
                if (padding_minimal) {
                    buf->push_back(KeyConsts::KEY_MINIMAL_MARKER);
                } else {
                    if (is_mow) {
                        buf->push_back(KeyConsts::KEY_NORMAL_NEXT_MARKER);
                    } else {
                        buf->push_back(KeyConsts::KEY_MAXIMAL_MARKER);
                    }
                }
                break;
            }

            auto c = cell(cid);
            if (c.is_null()) {
                buf->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
                continue;
            }
            buf->push_back(KeyConsts::KEY_NORMAL_MARKER);
            if (is_mow) {
                field->full_encode_ascending(c.cell_ptr(), buf);
            } else {
                field->encode_ascending(c.cell_ptr(), buf);
            }
        }
    }

    // Encode one row into binary according given num_keys.
    // Client call this function must assure that row contains the first
    // num_keys columns.
    template <bool full_encode = false>
    void encode_key(std::string* buf, size_t num_keys) const {
        for (uint32_t cid = 0; cid < num_keys; cid++) {
            auto c = cell(cid);
            if (c.is_null()) {
                buf->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
                continue;
            }
            buf->push_back(KeyConsts::KEY_NORMAL_MARKER);
            if (full_encode) {
                _schema->column(cid)->full_encode_ascending(c.cell_ptr(), buf);
            } else {
                _schema->column(cid)->encode_ascending(c.cell_ptr(), buf);
            }
        }
    }

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
