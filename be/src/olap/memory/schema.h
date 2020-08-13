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

#include "olap/memory/common.h"
#include "olap/tablet_schema.h"

namespace doris {
namespace memory {

// This file contains type and schema adaptors
// from olap's type and schema to memory engine's type and schema

// Memory engine's column type, just use FieldType for now
typedef FieldType ColumnType;

// Return true if this ColumnType is supported
bool supported(ColumnType type);

// Memory engine's column schema, simple wrapper of TabletColumn.
// TODO: Add more properties and methods later
class ColumnSchema {
public:
    explicit ColumnSchema(const TabletColumn& tcolumn);
    ColumnSchema(uint32_t cid, const string& name, ColumnType type, bool nullable, bool is_key);

    // Get column id
    inline uint32_t cid() const { return static_cast<uint32_t>(_tcolumn.unique_id()); }

    // Get column name
    inline std::string name() const { return _tcolumn.name(); }

    // Get column type
    inline ColumnType type() const { return _tcolumn.type(); }

    // Get is nullable
    inline bool is_nullable() const { return _tcolumn.is_nullable(); }

    // Get is key
    inline bool is_key() const { return _tcolumn.is_key(); }

    std::string type_name() const;
    std::string debug_string() const;

private:
    // Note: do not add more field into this class, it needs to be identical to TabletColumn
    TabletColumn _tcolumn;
};

// Memory engine's tablet schema, simple wrapper of TabletSchema.
// Schema have some differences comparing to original TabletSchema:
// 1. there is a hidden delete_flag column (with special cid=0) to mark
//    deleted rows
// 2. in the future, there may be a special compound primary key column
//    if primary-key has multiple columns
// TODO: Add more properties and methods later
class Schema : public RefCountedThreadSafe<Schema> {
public:
    // Create schema by description string, utility method for test
    static Status create(const string& desc, scoped_refptr<Schema>* sc);

    explicit Schema(const TabletSchema& tschema);

    std::string debug_string() const;

    inline size_t num_columns() const { return _tschema.num_columns(); }

    inline size_t num_key_columns() const { return _tschema.num_key_columns(); }

    const TabletSchema& get_tablet_schema() const { return _tschema; }

    // Get ColumnSchema by index
    const ColumnSchema* get(size_t idx) const;

    // Get ColumnSchema by name
    const ColumnSchema* get_by_name(const string& name) const;

    // Get column id space size
    //
    // For example:
    // If a schema have 5 columns with id [1, 2, 3, 5, 6]
    // It's cid_size equals max(cid)+1 = 7
    uint32_t cid_size() const;

    // Get ColumnSchema by column id
    const ColumnSchema* get_by_cid(uint32_t cid) const;

    // Get column type byte size by column id
    size_t get_column_byte_size(uint32_t cid) const {
        DCHECK_LT(cid, _column_byte_sizes.size());
        return _column_byte_sizes[cid];
    }

private:
    TabletSchema _tschema;
    uint32_t _cid_size;
    std::unordered_map<string, const ColumnSchema*> _name_to_col;
    vector<const ColumnSchema*> _cid_to_col;
    vector<size_t> _column_byte_sizes;
};

} // namespace memory
} // namespace doris
