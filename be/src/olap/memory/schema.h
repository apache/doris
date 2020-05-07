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

// Memory engine's column schema, simple wrapper of TabletColumn.
// TODO: Add more properties and methods later
class ColumnSchema {
public:
    explicit ColumnSchema(const TabletColumn& tcolumn);
    ColumnSchema(uint32_t cid, const string& name, ColumnType type, bool nullable, bool is_key);
    inline uint32_t cid() const { return static_cast<uint32_t>(_tcolumn.unique_id()); }
    inline std::string name() const { return _tcolumn.name(); }
    inline ColumnType type() const { return _tcolumn.type(); }
    inline bool is_nullable() const { return _tcolumn.is_nullable(); }
    inline bool is_key() const { return _tcolumn.is_key(); }

    std::string type_name() const;
    std::string debug_string() const;

private:
    TabletColumn _tcolumn;
};

// Memory engine's tablet schema, simple wrapper of TabletSchema.
// Schema have some differences comparing to original TablteSchema:
// 1. there is a hidden delete_flag column (with special cid=0) to mark
//    deleted rows
// 2. in the future, there may be a special compound primary key column
//    if primary-key has multiple columns
// TODO: Add more properties and methods later
class Schema {
public:
    explicit Schema(const TabletSchema& tschema);
    std::string debug_string() const;
    inline size_t num_columns() const { return _tschema.num_columns(); }
    inline size_t num_key_columns() const { return _tschema.num_key_columns(); }

    const ColumnSchema* get(size_t idx) const;

    const ColumnSchema* get_by_name(const string& name) const;

    uint32_t cid_size() const;
    const ColumnSchema* get_by_cid(uint32_t cid) const;

private:
    TabletSchema _tschema;
    uint32_t _cid_size;
    std::unordered_map<string, const ColumnSchema*> _name_to_col;
    vector<const ColumnSchema*> _cid_to_col;
};

} // namespace memory
} // namespace doris
