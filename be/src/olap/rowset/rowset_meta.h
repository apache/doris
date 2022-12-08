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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_H

#include <memory>
#include <string>
#include <vector>

#include "common/logging.h"
#include "gen_cpp/olap_file.pb.h"
#include "google/protobuf/util/message_differencer.h"
#include "io/fs/file_system.h"
#include "io/fs/file_system_map.h"
#include "io/fs/local_file_system.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"

namespace doris {

class RowsetMeta;
using RowsetMetaSharedPtr = std::shared_ptr<RowsetMeta>;

class RowsetMeta {
public:
    virtual ~RowsetMeta() = default;

    virtual bool init(const std::string& pb_rowset_meta) {
        bool ret = _deserialize_from_pb(pb_rowset_meta);
        if (!ret) {
            return false;
        }
        _init();
        return true;
    }

    virtual bool init_from_pb(const RowsetMetaPB& rowset_meta_pb) {
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

    virtual bool init_from_json(const std::string& json_rowset_meta) {
        bool ret = json2pb::JsonToProtoMessage(json_rowset_meta, &_rowset_meta_pb);
        if (!ret) {
            return false;
        }
        _init();
        return true;
    }

    virtual bool serialize(std::string* value) { return _serialize_to_pb(value); }

    virtual bool json_rowset_meta(std::string* json_rowset_meta) {
        json2pb::Pb2JsonOptions json_options;
        json_options.pretty_json = true;
        bool ret = json2pb::ProtoMessageToJson(_rowset_meta_pb, json_rowset_meta, json_options);
        return ret;
    }

    // This method may return nullptr.
    io::FileSystemSPtr fs() {
        if (!_fs) {
            if (is_local()) {
                return io::global_local_filesystem();
            } else {
                _fs = io::FileSystemMap::instance()->get(resource_id());
                LOG_IF(WARNING, !_fs) << "Cannot get file system: " << resource_id();
            }
        }
        return _fs;
    }

    void set_fs(io::FileSystemSPtr fs) { _fs = std::move(fs); }

    const io::ResourceId& resource_id() const { return _rowset_meta_pb.resource_id(); }

    void set_resource_id(io::ResourceId resource_id) {
        _rowset_meta_pb.set_resource_id(std::move(resource_id));
    }

    bool is_local() const { return !_rowset_meta_pb.has_resource_id(); }

    RowsetId rowset_id() const { return _rowset_id; }

    void set_rowset_id(const RowsetId& rowset_id) {
        // rowset id is a required field, just set it to 0
        _rowset_meta_pb.set_rowset_id(0);
        _rowset_id = rowset_id;
        _rowset_meta_pb.set_rowset_id_v2(rowset_id.to_string());
    }

    int64_t tablet_id() const { return _rowset_meta_pb.tablet_id(); }

    void set_tablet_id(int64_t tablet_id) { _rowset_meta_pb.set_tablet_id(tablet_id); }

    TabletUid tablet_uid() const { return _rowset_meta_pb.tablet_uid(); }

    void set_tablet_uid(TabletUid tablet_uid) {
        *(_rowset_meta_pb.mutable_tablet_uid()) = tablet_uid.to_proto();
    }

    int64_t txn_id() const { return _rowset_meta_pb.txn_id(); }

    void set_txn_id(int64_t txn_id) { _rowset_meta_pb.set_txn_id(txn_id); }

    int32_t tablet_schema_hash() const { return _rowset_meta_pb.tablet_schema_hash(); }

    void set_tablet_schema_hash(int64_t tablet_schema_hash) {
        _rowset_meta_pb.set_tablet_schema_hash(tablet_schema_hash);
    }

    RowsetTypePB rowset_type() const { return _rowset_meta_pb.rowset_type(); }

    void set_rowset_type(RowsetTypePB rowset_type) { _rowset_meta_pb.set_rowset_type(rowset_type); }

    RowsetStatePB rowset_state() const { return _rowset_meta_pb.rowset_state(); }

    void set_rowset_state(RowsetStatePB rowset_state) {
        _rowset_meta_pb.set_rowset_state(rowset_state);
    }

    Version version() const {
        return {_rowset_meta_pb.start_version(), _rowset_meta_pb.end_version()};
    }

    void set_version(Version version) {
        _rowset_meta_pb.set_start_version(version.first);
        _rowset_meta_pb.set_end_version(version.second);
    }

    bool has_version() const {
        return _rowset_meta_pb.has_start_version() && _rowset_meta_pb.has_end_version();
    }

    int64_t start_version() const { return _rowset_meta_pb.start_version(); }

    void set_start_version(int64_t start_version) {
        _rowset_meta_pb.set_start_version(start_version);
    }

    int64_t end_version() const { return _rowset_meta_pb.end_version(); }

    void set_end_version(int64_t end_version) { _rowset_meta_pb.set_end_version(end_version); }

    int64_t num_rows() const { return _rowset_meta_pb.num_rows(); }

    void set_num_rows(int64_t num_rows) { _rowset_meta_pb.set_num_rows(num_rows); }

    size_t total_disk_size() const { return _rowset_meta_pb.total_disk_size(); }

    void set_total_disk_size(size_t total_disk_size) {
        _rowset_meta_pb.set_total_disk_size(total_disk_size);
    }

    size_t data_disk_size() const { return _rowset_meta_pb.data_disk_size(); }

    void set_data_disk_size(size_t data_disk_size) {
        _rowset_meta_pb.set_data_disk_size(data_disk_size);
    }

    size_t index_disk_size() const { return _rowset_meta_pb.index_disk_size(); }

    void set_index_disk_size(size_t index_disk_size) {
        _rowset_meta_pb.set_index_disk_size(index_disk_size);
    }

    void zone_maps(std::vector<ZoneMap>* zone_maps) {
        for (const ZoneMap& zone_map : _rowset_meta_pb.zone_maps()) {
            zone_maps->push_back(zone_map);
        }
    }

    void set_zone_maps(const std::vector<ZoneMap>& zone_maps) {
        for (const ZoneMap& zone_map : zone_maps) {
            ZoneMap* new_zone_map = _rowset_meta_pb.add_zone_maps();
            *new_zone_map = zone_map;
        }
    }

    void add_zone_map(const ZoneMap& zone_map) {
        ZoneMap* new_zone_map = _rowset_meta_pb.add_zone_maps();
        *new_zone_map = zone_map;
    }

    bool has_delete_predicate() const { return _rowset_meta_pb.has_delete_predicate(); }

    const DeletePredicatePB& delete_predicate() const { return _rowset_meta_pb.delete_predicate(); }

    DeletePredicatePB* mutable_delete_predicate() {
        return _rowset_meta_pb.mutable_delete_predicate();
    }

    void set_delete_predicate(const DeletePredicatePB& delete_predicate) {
        DeletePredicatePB* new_delete_condition = _rowset_meta_pb.mutable_delete_predicate();
        *new_delete_condition = delete_predicate;
    }

    bool empty() const { return _rowset_meta_pb.empty(); }

    void set_empty(bool empty) { _rowset_meta_pb.set_empty(empty); }

    PUniqueId load_id() const { return _rowset_meta_pb.load_id(); }

    void set_load_id(PUniqueId load_id) {
        PUniqueId* new_load_id = _rowset_meta_pb.mutable_load_id();
        new_load_id->set_hi(load_id.hi());
        new_load_id->set_lo(load_id.lo());
    }

    bool delete_flag() const { return _rowset_meta_pb.delete_flag(); }

    int64_t creation_time() const { return _rowset_meta_pb.creation_time(); }

    void set_creation_time(int64_t creation_time) {
        return _rowset_meta_pb.set_creation_time(creation_time);
    }

    int64_t partition_id() const { return _rowset_meta_pb.partition_id(); }

    void set_partition_id(int64_t partition_id) {
        return _rowset_meta_pb.set_partition_id(partition_id);
    }

    int64_t num_segments() const { return _rowset_meta_pb.num_segments(); }

    void set_num_segments(int64_t num_segments) { _rowset_meta_pb.set_num_segments(num_segments); }

    void to_rowset_pb(RowsetMetaPB* rs_meta_pb) const {
        *rs_meta_pb = _rowset_meta_pb;
        if (_schema) {
            _schema->to_schema_pb(rs_meta_pb->mutable_tablet_schema());
        }
    }

    RowsetMetaPB get_rowset_pb() {
        RowsetMetaPB rowset_meta_pb = _rowset_meta_pb;
        if (_schema) {
            _schema->to_schema_pb(rowset_meta_pb.mutable_tablet_schema());
        }
        return rowset_meta_pb;
    }

    bool is_singleton_delta() const {
        return has_version() && _rowset_meta_pb.start_version() == _rowset_meta_pb.end_version();
    }

    // Some time, we may check if this rowset is in rowset meta manager's meta by using RowsetMetaManager::check_rowset_meta.
    // But, this check behavior may cost a lot of time when it is frequent.
    // If we explicitly remove this rowset from rowset meta manager's meta, we can set _is_removed_from_rowset_meta to true,
    // And next time when we want to check if this rowset is in rowset mata manager's meta, we can
    // check is_remove_from_rowset_meta() first.
    void set_remove_from_rowset_meta() { _is_removed_from_rowset_meta = true; }

    bool is_remove_from_rowset_meta() const { return _is_removed_from_rowset_meta; }

    SegmentsOverlapPB segments_overlap() const { return _rowset_meta_pb.segments_overlap_pb(); }

    void set_segments_overlap(SegmentsOverlapPB segments_overlap) {
        _rowset_meta_pb.set_segments_overlap_pb(segments_overlap);
    }

    static bool comparator(const RowsetMetaSharedPtr& left, const RowsetMetaSharedPtr& right) {
        return left->end_version() < right->end_version();
    }

    // return true if segments in this rowset has overlapping data.
    // this is not same as `segments_overlap()` method.
    // `segments_overlap()` only return the value of "segments_overlap" field in rowset meta,
    // but "segments_overlap" may be UNKNOWN.
    //
    // Returns true iff all of the following conditions are met
    // 1. the rowset contains more than one segment
    // 2. the rowset's start version == end version (non-singleton rowset was generated by compaction process
    //    which always produces non-overlapped segments)
    // 3. segments_overlap() flag is not NONOVERLAPPING (OVERLAP_UNKNOWN and OVERLAPPING are OK)
    bool is_segments_overlapping() const {
        return num_segments() > 1 && is_singleton_delta() && segments_overlap() != NONOVERLAPPING;
    }

    // get the compaction score of this rowset.
    // if segments are overlapping, the score equals to the number of segments,
    // otherwise, score is 1.
    uint32_t get_compaction_score() const {
        uint32_t score = 0;
        if (!is_segments_overlapping()) {
            score = 1;
        } else {
            score = num_segments();
            CHECK(score > 0);
        }
        return score;
    }

    void get_segments_key_bounds(std::vector<KeyBoundsPB>* segments_key_bounds) const {
        for (const KeyBoundsPB& key_range : _rowset_meta_pb.segments_key_bounds()) {
            segments_key_bounds->push_back(key_range);
        }
    }
    virtual bool get_first_segment_key_bound(KeyBoundsPB* key_bounds) {
        // for compatibility, old version has not segment key bounds
        if (_rowset_meta_pb.segments_key_bounds_size() == 0) {
            return false;
        }
        *key_bounds = _rowset_meta_pb.segments_key_bounds(0);
        return true;
    }
    virtual bool get_last_segment_key_bound(KeyBoundsPB* key_bounds) {
        if (_rowset_meta_pb.segments_key_bounds_size() == 0) {
            return false;
        }
        *key_bounds =
                _rowset_meta_pb.segments_key_bounds(_rowset_meta_pb.segments_key_bounds_size() - 1);
        return true;
    }

    void set_segments_key_bounds(const std::vector<KeyBoundsPB>& segments_key_bounds) {
        for (const KeyBoundsPB& key_bounds : segments_key_bounds) {
            KeyBoundsPB* new_key_bounds = _rowset_meta_pb.add_segments_key_bounds();
            *new_key_bounds = key_bounds;
        }
    }

    void set_oldest_write_timestamp(int64_t timestamp) {
        _rowset_meta_pb.set_oldest_write_timestamp(timestamp);
    }

    void set_newest_write_timestamp(int64_t timestamp) {
        _rowset_meta_pb.set_newest_write_timestamp(timestamp);
    }

    int64_t oldest_write_timestamp() const { return _rowset_meta_pb.oldest_write_timestamp(); }

    int64_t newest_write_timestamp() const { return _rowset_meta_pb.newest_write_timestamp(); }

    void set_tablet_schema(const TabletSchemaSPtr& tablet_schema) {
        DCHECK(_schema == nullptr);
        _schema = TabletSchemaCache::instance()->insert(tablet_schema->to_key());
    }

    TabletSchemaSPtr tablet_schema() { return _schema; }

private:
    bool _deserialize_from_pb(const std::string& value) {
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

    bool _serialize_to_pb(std::string* value) {
        if (value == nullptr) {
            return false;
        }
        RowsetMetaPB rowset_meta_pb = _rowset_meta_pb;
        if (_schema) {
            _schema->to_schema_pb(rowset_meta_pb.mutable_tablet_schema());
        }
        return rowset_meta_pb.SerializeToString(value);
    }

    void _init() {
        if (_rowset_meta_pb.rowset_id() > 0) {
            _rowset_id.init(_rowset_meta_pb.rowset_id());
        } else {
            _rowset_id.init(_rowset_meta_pb.rowset_id_v2());
        }
    }

    friend bool operator==(const RowsetMeta& a, const RowsetMeta& b) {
        if (a._rowset_id != b._rowset_id) return false;
        if (a._is_removed_from_rowset_meta != b._is_removed_from_rowset_meta) return false;
        if (!google::protobuf::util::MessageDifferencer::Equals(a._rowset_meta_pb,
                                                                b._rowset_meta_pb))
            return false;
        return true;
    }

    friend bool operator!=(const RowsetMeta& a, const RowsetMeta& b) { return !(a == b); }

private:
    RowsetMetaPB _rowset_meta_pb;
    TabletSchemaSPtr _schema = nullptr;
    RowsetId _rowset_id;
    io::FileSystemSPtr _fs;
    bool _is_removed_from_rowset_meta = false;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_H
