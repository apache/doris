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

#include "olap/rowset/rowset_meta.h"

#include "common/logging.h"
#include "google/protobuf/util/message_differencer.h"
#include "io/fs/local_file_system.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "olap/olap_common.h"
#include "olap/storage_policy.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"

namespace doris {

RowsetMeta::~RowsetMeta() = default;

bool RowsetMeta::init(const std::string& pb_rowset_meta) {
    bool ret = _deserialize_from_pb(pb_rowset_meta);
    if (!ret) {
        return false;
    }
    _init();
    return true;
}

bool RowsetMeta::init_from_pb(const RowsetMetaPB& rowset_meta_pb) {
    if (rowset_meta_pb.has_tablet_schema()) {
        _schema = TabletSchemaCache::instance()->insert(
                rowset_meta_pb.tablet_schema().SerializeAsString());
    }
    // Release ownership of TabletSchemaPB from `rowset_meta_pb` and then set it back to `rowset_meta_pb`,
    // this won't break const semantics of `rowset_meta_pb`, because `rowset_meta_pb` is not changed
    // before and after call this method.
    auto& mut_rowset_meta_pb = const_cast<RowsetMetaPB&>(rowset_meta_pb);
    auto schema = mut_rowset_meta_pb.release_tablet_schema();
    _rowset_meta_pb = mut_rowset_meta_pb;
    mut_rowset_meta_pb.set_allocated_tablet_schema(schema);
    _init();
    return true;
}

bool RowsetMeta::init_from_json(const std::string& json_rowset_meta) {
    bool ret = json2pb::JsonToProtoMessage(json_rowset_meta, &_rowset_meta_pb);
    if (!ret) {
        return false;
    }
    _init();
    return true;
}

bool RowsetMeta::json_rowset_meta(std::string* json_rowset_meta) {
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(_rowset_meta_pb, json_rowset_meta, json_options);
    return ret;
}

const io::FileSystemSPtr& RowsetMeta::fs() {
    if (!_fs) {
        if (is_local()) {
            _fs = io::global_local_filesystem();
        } else {
            _fs = get_filesystem(resource_id());
            LOG_IF(WARNING, !_fs) << "Cannot get file system: " << resource_id();
        }
    }
    return _fs;
}

void RowsetMeta::set_fs(io::FileSystemSPtr fs) {
    if (fs && fs->type() != io::FileSystemType::LOCAL) {
        _rowset_meta_pb.set_resource_id(fs->id());
    }
    _fs = std::move(fs);
}

void RowsetMeta::to_rowset_pb(RowsetMetaPB* rs_meta_pb) const {
    *rs_meta_pb = _rowset_meta_pb;
    if (_schema) {
        _schema->to_schema_pb(rs_meta_pb->mutable_tablet_schema());
    }
}

RowsetMetaPB RowsetMeta::get_rowset_pb() {
    RowsetMetaPB rowset_meta_pb = _rowset_meta_pb;
    if (_schema) {
        _schema->to_schema_pb(rowset_meta_pb.mutable_tablet_schema());
    }
    return rowset_meta_pb;
}

void RowsetMeta::set_tablet_schema(const TabletSchemaSPtr& tablet_schema) {
    _schema = TabletSchemaCache::instance()->insert(tablet_schema->to_key());
}

bool RowsetMeta::_deserialize_from_pb(const std::string& value) {
    RowsetMetaPB rowset_meta_pb;
    if (!rowset_meta_pb.ParseFromString(value)) {
        return false;
    }
    if (rowset_meta_pb.has_tablet_schema()) {
        _schema = TabletSchemaCache::instance()->insert(
                rowset_meta_pb.tablet_schema().SerializeAsString());
        rowset_meta_pb.clear_tablet_schema();
    }
    _rowset_meta_pb = rowset_meta_pb;
    return true;
}

bool RowsetMeta::_serialize_to_pb(std::string* value) {
    if (value == nullptr) {
        return false;
    }
    RowsetMetaPB rowset_meta_pb = _rowset_meta_pb;
    if (_schema) {
        _schema->to_schema_pb(rowset_meta_pb.mutable_tablet_schema());
    }
    return rowset_meta_pb.SerializeToString(value);
}

void RowsetMeta::_init() {
    if (_rowset_meta_pb.rowset_id() > 0) {
        _rowset_id.init(_rowset_meta_pb.rowset_id());
    } else {
        _rowset_id.init(_rowset_meta_pb.rowset_id_v2());
    }
}

bool operator==(const RowsetMeta& a, const RowsetMeta& b) {
    if (a._rowset_id != b._rowset_id) return false;
    if (a._is_removed_from_rowset_meta != b._is_removed_from_rowset_meta) return false;
    if (!google::protobuf::util::MessageDifferencer::Equals(a._rowset_meta_pb, b._rowset_meta_pb))
        return false;
    return true;
}

} // namespace doris
