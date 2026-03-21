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
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "storage/olap_tuple.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
#include "common/compile_check_begin.h"
class StorageField;

// Delegate the operation of a row of data.
// Stores values as core::Field objects instead of raw byte buffers.
class RowCursor {
public:
    RowCursor();
    ~RowCursor();
    RowCursor(const RowCursor&) = delete;
    RowCursor& operator=(const RowCursor&) = delete;
    RowCursor(RowCursor&&) noexcept;
    RowCursor& operator=(RowCursor&&) noexcept;

    // Initialize from OlapTuple (which now stores Fields).
    // Sets up the schema and copies Fields from the tuple.
    Status init(TabletSchemaSPtr schema, const OlapTuple& tuple);
    Status init(TabletSchemaSPtr schema, const OlapTuple& tuple,
                const std::shared_ptr<Schema>& shared_schema);

    // Initialize with schema and num_columns, creating null Fields.
    // Caller sets individual fields via mutable_field().
    Status init(TabletSchemaSPtr schema, size_t num_columns);

    // Initialize from typed Fields directly.
    Status init_scan_key(TabletSchemaSPtr schema, std::vector<Field> fields);

    const Field& field(uint32_t cid) const { return _fields[cid]; }
    Field& mutable_field(uint32_t cid) { return _fields[cid]; }

    size_t field_count() const { return _fields.size(); }

    const StorageField* column_schema(uint32_t cid) const { return _schema->column(cid); }
    const Schema* schema() const { return _schema.get(); }

    // Returns a deep copy of this RowCursor with the same schema and field values.
    RowCursor clone() const;

    // Pad all CHAR-type fields in-place to their declared column length using '\0'.
    // RowCursor holds CHAR values in compute format (unpadded). Call this before
    // comparing against storage-format data (e.g. _seek_block) where CHAR is padded.
    void pad_char_fields();

    // Output row cursor content in string format
    std::string to_string() const;

    // Encode one row into binary according given num_keys.
    // Internally converts each core::Field to its storage representation via
    // PrimitiveTypeConvertor before passing to KeyCoder.
    // CHAR fields are zero-padded to column.length() for encoding.
    template <bool is_mow = false>
    void encode_key_with_padding(std::string* buf, size_t num_keys, bool padding_minimal) const;

    // Encode one row into binary according given num_keys.
    // Client must ensure that row contains the first num_keys columns.
    template <bool full_encode = false>
    void encode_key(std::string* buf, size_t num_keys) const;

    // Encode a single field at column index 'cid' into 'buf'.
    void encode_single_field(uint32_t cid, std::string* buf, bool full_encode) const {
        const auto& f = _fields[cid];
        DCHECK(!f.is_null());
        _encode_field(_schema->column(cid), f, full_encode, buf);
    }

private:
    // Copy Fields from an OlapTuple into this cursor.
    Status from_tuple(const OlapTuple& tuple);

    void _init_schema(TabletSchemaSPtr schema, uint32_t column_count);
    void _init_schema(const std::shared_ptr<Schema>& shared_schema, uint32_t column_count);

    // Helper: encode a single non-null field for the given column.
    // Converts the core::Field to storage format and calls KeyCoder.
    void _encode_field(const StorageField* storage_field, const Field& f, bool full_encode,
                       std::string* buf) const;

    std::unique_ptr<Schema> _schema;
    std::vector<Field> _fields;
};
#include "common/compile_check_end.h"
} // namespace doris
