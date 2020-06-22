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

#include "gutil/strings/split.h"

namespace doris {
namespace memory {

bool supported(ColumnType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT:
    case OLAP_FIELD_TYPE_SMALLINT:
    case OLAP_FIELD_TYPE_INT:
    case OLAP_FIELD_TYPE_BIGINT:
    case OLAP_FIELD_TYPE_LARGEINT:
    case OLAP_FIELD_TYPE_FLOAT:
    case OLAP_FIELD_TYPE_DOUBLE:
    case OLAP_FIELD_TYPE_BOOL:
        return true;
    default:
        return false;
    }
}

size_t get_type_byte_size(ColumnType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT:
        return 1;
    case OLAP_FIELD_TYPE_SMALLINT:
        return 2;
    case OLAP_FIELD_TYPE_INT:
        return 4;
    case OLAP_FIELD_TYPE_BIGINT:
        return 8;
    case OLAP_FIELD_TYPE_LARGEINT:
        return 16;
    case OLAP_FIELD_TYPE_FLOAT:
        return 4;
    case OLAP_FIELD_TYPE_DOUBLE:
        return 8;
    case OLAP_FIELD_TYPE_BOOL:
        return 1;
    default:
        return 0;
    }
}

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

Status Schema::create(const string& desc, scoped_refptr<Schema>* sc) {
    TabletSchemaPB tspb;
    std::vector<std::string> cs = strings::Split(desc, ",", strings::SkipWhitespace());
    uint32_t cid = 1;
    for (std::string& c : cs) {
        ColumnPB* cpb = tspb.add_column();
        std::vector<std::string> fs = strings::Split(c, " ", strings::SkipWhitespace());
        if (fs.size() < 2) {
            return Status::InvalidArgument("bad schema desc");
        }
        cpb->set_is_key(cid == 1);
        cpb->set_unique_id(cid++);
        cpb->set_name(fs[0]);
        cpb->set_type(fs[1]);
        if (fs.size() == 3 && fs[2] == "null") {
            cpb->set_is_nullable(true);
        }
    }
    tspb.set_keys_type(KeysType::UNIQUE_KEYS);
    tspb.set_next_column_unique_id(cid);
    tspb.set_num_short_key_columns(1);
    tspb.set_is_in_memory(false);
    TabletSchema ts;
    ts.init_from_pb(tspb);
    sc->reset(new Schema(ts));
    return Status::OK();
}

Schema::Schema(const TabletSchema& tschema) : _tschema(tschema) {
    _cid_size = 1;
    _cid_to_col.resize(_cid_size, nullptr);
    _column_byte_sizes.resize(_cid_size, 0);
    for (size_t i = 0; i < num_columns(); i++) {
        const ColumnSchema* cs = get(i);
        _cid_size = std::max(_cid_size, cs->cid() + 1);
        _cid_to_col.resize(_cid_size, nullptr);
        _cid_to_col[cs->cid()] = cs;
        _name_to_col[cs->name()] = cs;
        _column_byte_sizes.resize(_cid_size, 0);
        _column_byte_sizes[cs->cid()] = get_type_byte_size(cs->type());
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
