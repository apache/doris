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

#include "olap/memory/schema.h"

namespace doris {
namespace memory {

ColumnSchema::ColumnSchema(const TabletColumn& tcolumn) : _tcolumn(tcolumn) {}

ColumnSchema::ColumnSchema(uint32_t cid, const string& name, ColumnType type, bool nullable,
                           bool is_key) {
    ColumnPB cpb;
    cpb.set_unique_id(cid);
    cpb.set_name(name);
    cpb.set_type(TabletColumn::get_string_by_field_type(type));
    cpb.set_is_nullable(nullable);
    cpb.set_is_key(is_key);
    _tcolumn.init_from_pb(cpb);
}

std::string ColumnSchema::type_name() const {
    return TabletColumn::get_string_by_field_type(_tcolumn.type());
}

std::string ColumnSchema::debug_string() const {
    return StringPrintf("cid=%d %s %s%s%s", cid(), name().c_str(), type_name().c_str(),
                        is_nullable() ? " nullable" : "", is_key() ? " key" : "");
}

//////////////////////////////////////////////////////////////////////////////

Schema::Schema(const TabletSchema& tschema) : _tschema(tschema) {
    _cid_size = 1;
    _cid_to_col.resize(_cid_size, nullptr);
    for (size_t i = 0; i < num_columns(); i++) {
        const ColumnSchema* cs = get(i);
        _cid_size = std::max(_cid_size, cs->cid() + 1);
        _cid_to_col.resize(_cid_size, nullptr);
        _cid_to_col[cs->cid()] = cs;
        _name_to_col[cs->name()] = cs;
    }
}

std::string Schema::debug_string() const {
    std::string ret("(");
    for (size_t i = 0; i < num_columns(); i++) {
        const ColumnSchema* cs = get(i);
        if (i > 0) {
            ret.append(", ");
        }
        ret.append(cs->debug_string());
    }
    ret.append(")");
    return ret;
}

uint32_t Schema::cid_size() const {
    return _cid_size;
}

const ColumnSchema* Schema::get(size_t idx) const {
    if (idx < num_columns()) {
        // TODO: this is a hack, improve this in the future
        return reinterpret_cast<const ColumnSchema*>(&_tschema.columns()[idx]);
    }
    return nullptr;
}

const ColumnSchema* Schema::get_by_name(const string& name) const {
    auto itr = _name_to_col.find(name);
    if (itr == _name_to_col.end()) {
        return nullptr;
    } else {
        return itr->second;
    }
}

const ColumnSchema* Schema::get_by_cid(uint32_t cid) const {
    if (cid < _cid_to_col.size()) {
        return _cid_to_col[cid];
    } else {
        return nullptr;
    }
}

} // namespace memory
} // namespace doris
