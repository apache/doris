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

#include <gen_cpp/olap_file.pb.h>

#include <memory>

#include "common/logging.h"
#include "google/protobuf/util/message_differencer.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/storage_policy.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"
#include "vec/common/schema_util.h"

namespace doris {

RowsetMeta::~RowsetMeta() {
    if (_handle) {
        TabletSchemaCache::instance()->release(_handle);
    }
}

bool RowsetMeta::init(std::string_view pb_rowset_meta) {
    bool ret = _deserialize_from_pb(pb_rowset_meta);
    if (!ret) {
        return false;
    }
    _init();
    return true;
}

bool RowsetMeta::init(const RowsetMeta* rowset_meta) {
    RowsetMetaPB rowset_meta_pb;
    rowset_meta->to_rowset_pb(&rowset_meta_pb);
    return init_from_pb(rowset_meta_pb);
}

bool RowsetMeta::init_from_pb(const RowsetMetaPB& rowset_meta_pb) {
    if (rowset_meta_pb.has_tablet_schema()) {
        set_tablet_schema(rowset_meta_pb.tablet_schema());
    }
    // Release ownership of TabletSchemaPB from `rowset_meta_pb` and then set it back to `rowset_meta_pb`,
    // this won't break const semantics of `rowset_meta_pb`, because `rowset_meta_pb` is not changed
    // before and after call this method.
    auto& mut_rowset_meta_pb = const_cast<RowsetMetaPB&>(rowset_meta_pb);
    auto* schema = mut_rowset_meta_pb.release_tablet_schema();
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

io::FileSystemSPtr RowsetMeta::fs() {
    if (is_local()) {
        return io::global_local_filesystem();
    }

    auto storage_resource = remote_storage_resource();
    if (storage_resource) {
        return storage_resource.value()->fs;
    } else {
        LOG(WARNING) << storage_resource.error();
        return nullptr;
    }
}

Result<const StorageResource*> RowsetMeta::remote_storage_resource() {
    if (is_local()) {
        return ResultError(Status::InternalError(
                "local rowset has no storage resource. tablet_id={} rowset_id={}", tablet_id(),
                _rowset_id.to_string()));
    }

    if (!_storage_resource.fs) {
        // not initialized yet
        auto storage_resource = get_storage_resource(resource_id());
        if (storage_resource) {
            _storage_resource = std::move(storage_resource->first);
        } else {
            return ResultError(Status::InternalError("cannot find storage resource. resource_id={}",
                                                     resource_id()));
        }
    }

    return &_storage_resource;
}

void RowsetMeta::set_remote_storage_resource(StorageResource resource) {
    _storage_resource = std::move(resource);
    _rowset_meta_pb.set_resource_id(_storage_resource.fs->id());
}

bool RowsetMeta::has_variant_type_in_schema() const {
    return _schema && _schema->num_variant_columns() > 0;
}

void RowsetMeta::to_rowset_pb(RowsetMetaPB* rs_meta_pb, bool skip_schema) const {
    *rs_meta_pb = _rowset_meta_pb;
    if (_schema) [[likely]] {
        rs_meta_pb->set_schema_version(_schema->schema_version());
        if (!skip_schema) {
            // For cloud, separate tablet schema from rowset meta to reduce persistent size.
            _schema->to_schema_pb(rs_meta_pb->mutable_tablet_schema());
        }
    }
    rs_meta_pb->set_has_variant_type_in_schema(has_variant_type_in_schema());
}

RowsetMetaPB RowsetMeta::get_rowset_pb(bool skip_schema) const {
    RowsetMetaPB rowset_meta_pb;
    to_rowset_pb(&rowset_meta_pb, skip_schema);
    return rowset_meta_pb;
}

void RowsetMeta::set_tablet_schema(const TabletSchemaSPtr& tablet_schema) {
    if (_handle) {
        TabletSchemaCache::instance()->release(_handle);
    }
    auto pair = TabletSchemaCache::instance()->insert(tablet_schema->to_key());
    _handle = pair.first;
    _schema = pair.second;
}

void RowsetMeta::set_tablet_schema(const TabletSchemaPB& tablet_schema) {
    if (_handle) {
        TabletSchemaCache::instance()->release(_handle);
    }
    auto pair = TabletSchemaCache::instance()->insert(
            TabletSchema::deterministic_string_serialize(tablet_schema));
    _handle = pair.first;
    _schema = pair.second;
}

bool RowsetMeta::_deserialize_from_pb(std::string_view value) {
    if (!_rowset_meta_pb.ParseFromArray(value.data(), value.size())) {
        _rowset_meta_pb.Clear();
        return false;
    }
    if (_rowset_meta_pb.has_tablet_schema()) {
        set_tablet_schema(_rowset_meta_pb.tablet_schema());
        _rowset_meta_pb.set_allocated_tablet_schema(nullptr);
    }
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

void RowsetMeta::add_segments_file_size(const std::vector<size_t>& seg_file_size) {
    _rowset_meta_pb.set_enable_segments_file_size(true);
    for (auto fsize : seg_file_size) {
        _rowset_meta_pb.add_segments_file_size(fsize);
    }
}

int64_t RowsetMeta::segment_file_size(int seg_id) {
    DCHECK(_rowset_meta_pb.segments_file_size().empty() ||
           _rowset_meta_pb.segments_file_size_size() > seg_id)
            << _rowset_meta_pb.segments_file_size_size() << ' ' << seg_id;
    return _rowset_meta_pb.enable_segments_file_size()
                   ? (_rowset_meta_pb.segments_file_size_size() > seg_id
                              ? _rowset_meta_pb.segments_file_size(seg_id)
                              : -1)
                   : -1;
}

void RowsetMeta::merge_rowset_meta(const RowsetMeta& other) {
    set_num_segments(num_segments() + other.num_segments());
    set_num_rows(num_rows() + other.num_rows());
    set_data_disk_size(data_disk_size() + other.data_disk_size());
    set_index_disk_size(index_disk_size() + other.index_disk_size());
    for (auto&& key_bound : other.get_segments_key_bounds()) {
        add_segment_key_bounds(key_bound);
    }
    if (_rowset_meta_pb.enable_segments_file_size() &&
        other._rowset_meta_pb.enable_segments_file_size()) {
        for (auto fsize : other.segments_file_size()) {
            _rowset_meta_pb.add_segments_file_size(fsize);
        }
    }
    if (_rowset_meta_pb.enable_inverted_index_file_info() &&
        other._rowset_meta_pb.enable_inverted_index_file_info()) {
        for (auto finfo : other.inverted_index_file_info()) {
            InvertedIndexFileInfo* new_file_info = _rowset_meta_pb.add_inverted_index_file_info();
            *new_file_info = finfo;
        }
    }
    // In partial update the rowset schema maybe updated when table contains variant type, so we need the newest schema to be updated
    // Otherwise the schema is stale and lead to wrong data read
    if (tablet_schema()->num_variant_columns() > 0) {
        // merge extracted columns
        TabletSchemaSPtr merged_schema;
        static_cast<void>(vectorized::schema_util::get_least_common_schema(
                {tablet_schema(), other.tablet_schema()}, nullptr, merged_schema));
        if (*_schema != *merged_schema) {
            set_tablet_schema(merged_schema);
        }
    }
    if (rowset_state() == RowsetStatePB::BEGIN_PARTIAL_UPDATE) {
        set_rowset_state(RowsetStatePB::COMMITTED);
    }
}

InvertedIndexFileInfo RowsetMeta::inverted_index_file_info(int seg_id) {
    return _rowset_meta_pb.enable_inverted_index_file_info()
                   ? (_rowset_meta_pb.inverted_index_file_info_size() > seg_id
                              ? _rowset_meta_pb.inverted_index_file_info(seg_id)
                              : InvertedIndexFileInfo())
                   : InvertedIndexFileInfo();
}

void RowsetMeta::add_inverted_index_files_info(
        const std::vector<InvertedIndexFileInfo>& idx_file_info) {
    _rowset_meta_pb.set_enable_inverted_index_file_info(true);
    for (auto finfo : idx_file_info) {
        auto* new_file_info = _rowset_meta_pb.add_inverted_index_file_info();
        *new_file_info = finfo;
    }
}

void RowsetMeta::update_inverted_index_files_info(
        const std::vector<InvertedIndexFileInfo>& idx_file_info) {
    _rowset_meta_pb.clear_inverted_index_file_info();
    add_inverted_index_files_info(idx_file_info);
}

bool operator==(const RowsetMeta& a, const RowsetMeta& b) {
    if (a._rowset_id != b._rowset_id) return false;
    if (a._is_removed_from_rowset_meta != b._is_removed_from_rowset_meta) return false;
    if (!google::protobuf::util::MessageDifferencer::Equals(a._rowset_meta_pb, b._rowset_meta_pb))
        return false;
    return true;
}

} // namespace doris
