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

#include <gen_cpp/olap_file.pb.h>

#include <memory>
#include <string>
#include <vector>

#include "io/fs/file_system.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/storage_policy.h"
#include "olap/tablet_fwd.h"
#include "runtime/memory/lru_cache_policy.h"

namespace doris {

class RowsetMeta {
public:
    RowsetMeta() = default;
    ~RowsetMeta();

    bool init(std::string_view pb_rowset_meta);

    bool init(const RowsetMeta* rowset_meta);

    bool init_from_pb(const RowsetMetaPB& rowset_meta_pb);

    bool init_from_json(const std::string& json_rowset_meta);

    bool serialize(std::string* value) { return _serialize_to_pb(value); }

    bool json_rowset_meta(std::string* json_rowset_meta);

    // If the rowset is a local rowset, return the global local file system.
    // Otherwise, return the remote file system corresponding to rowset's resource id.
    // Note that if the resource id cannot be found for the corresponding remote file system, nullptr will be returned.
    io::FileSystemSPtr fs();

    Result<const StorageResource*> remote_storage_resource();

    void set_remote_storage_resource(StorageResource resource);

    const std::string& resource_id() const { return _rowset_meta_pb.resource_id(); }

    bool is_local() const { return !_rowset_meta_pb.has_resource_id(); }

    bool has_variant_type_in_schema() const;

    RowsetId rowset_id() const { return _rowset_id; }

    void set_rowset_id(const RowsetId& rowset_id) {
        // rowset id is a required field, just set it to 0
        _rowset_meta_pb.set_rowset_id(0);
        _rowset_id = rowset_id;
        _rowset_meta_pb.set_rowset_id_v2(rowset_id.to_string());
    }

    int64_t tablet_id() const { return _rowset_meta_pb.tablet_id(); }

    void set_tablet_id(int64_t tablet_id) { _rowset_meta_pb.set_tablet_id(tablet_id); }

    int64_t index_id() const { return _rowset_meta_pb.index_id(); }

    void set_index_id(int64_t index_id) { _rowset_meta_pb.set_index_id(index_id); }

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

    int64_t end_version() const { return _rowset_meta_pb.end_version(); }

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

    void set_delete_predicate(DeletePredicatePB delete_predicate) {
        DeletePredicatePB* new_delete_condition = _rowset_meta_pb.mutable_delete_predicate();
        *new_delete_condition = std::move(delete_predicate);
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

    // Convert to RowsetMetaPB, skip_schema is only used by cloud to separate schema from rowset meta.
    void to_rowset_pb(RowsetMetaPB* rs_meta_pb, bool skip_schema = false) const;

    // Convert to RowsetMetaPB, skip_schema is only used by cloud to separate schema from rowset meta.
    RowsetMetaPB get_rowset_pb(bool skip_schema = false) const;

    inline DeletePredicatePB* mutable_delete_pred_pb() {
        return _rowset_meta_pb.mutable_delete_predicate();
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
    // Returns true if all of the following conditions are met
    // 1. the rowset contains more than one segment
    // 2. the rowset's start version == end version (non-singleton rowset was generated by compaction process
    //    which always produces non-overlapped segments)
    // 3. segments_overlap() flag is not NONOVERLAPPING (OVERLAP_UNKNOWN and OVERLAPPING are OK)
    bool is_segments_overlapping() const {
        return num_segments() > 1 && is_singleton_delta() && segments_overlap() != NONOVERLAPPING;
    }

    bool produced_by_compaction() const {
        return has_version() &&
               (start_version() < end_version() ||
                (start_version() == end_version() && segments_overlap() == NONOVERLAPPING));
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

    uint32_t get_merge_way_num() const {
        uint32_t way_num = 0;
        if (!is_segments_overlapping()) {
            if (num_segments() == 0) {
                way_num = 0;
            } else {
                way_num = 1;
            }
        } else {
            way_num = num_segments();
            CHECK(way_num > 0);
        }
        return way_num;
    }

    void get_segments_key_bounds(std::vector<KeyBoundsPB>* segments_key_bounds) const {
        for (const KeyBoundsPB& key_range : _rowset_meta_pb.segments_key_bounds()) {
            segments_key_bounds->push_back(key_range);
        }
    }

    auto& get_segments_key_bounds() const { return _rowset_meta_pb.segments_key_bounds(); }

    bool get_first_segment_key_bound(KeyBoundsPB* key_bounds) {
        // for compatibility, old version has not segment key bounds
        if (_rowset_meta_pb.segments_key_bounds_size() == 0) {
            return false;
        }
        *key_bounds = *_rowset_meta_pb.segments_key_bounds().begin();
        return true;
    }

    bool get_last_segment_key_bound(KeyBoundsPB* key_bounds) {
        if (_rowset_meta_pb.segments_key_bounds_size() == 0) {
            return false;
        }
        *key_bounds = *_rowset_meta_pb.segments_key_bounds().rbegin();
        return true;
    }

    void set_segments_key_bounds(const std::vector<KeyBoundsPB>& segments_key_bounds) {
        for (const KeyBoundsPB& key_bounds : segments_key_bounds) {
            KeyBoundsPB* new_key_bounds = _rowset_meta_pb.add_segments_key_bounds();
            *new_key_bounds = key_bounds;
        }
    }

    void add_segment_key_bounds(KeyBoundsPB segments_key_bounds) {
        *_rowset_meta_pb.add_segments_key_bounds() = std::move(segments_key_bounds);
        set_segments_overlap(OVERLAPPING);
    }

    void set_newest_write_timestamp(int64_t timestamp) {
        _rowset_meta_pb.set_newest_write_timestamp(timestamp);
    }

    int64_t newest_write_timestamp() const { return _rowset_meta_pb.newest_write_timestamp(); }

    void set_tablet_schema(const TabletSchemaSPtr& tablet_schema);
    void set_tablet_schema(const TabletSchemaPB& tablet_schema);

    const TabletSchemaSPtr& tablet_schema() const { return _schema; }

    void set_txn_expiration(int64_t expiration) { _rowset_meta_pb.set_txn_expiration(expiration); }

    void set_compaction_level(int64_t compaction_level) {
        _rowset_meta_pb.set_compaction_level(compaction_level);
    }

    int64_t compaction_level() { return _rowset_meta_pb.compaction_level(); }

    // `seg_file_size` MUST ordered by segment id
    void add_segments_file_size(const std::vector<size_t>& seg_file_size);

    // Return -1 if segment file size is unknown
    int64_t segment_file_size(int seg_id);

    const auto& segments_file_size() const { return _rowset_meta_pb.segments_file_size(); }

    // Used for partial update, when publish, partial update may add a new rowset and we should update rowset meta
    void merge_rowset_meta(const RowsetMeta& other);

    InvertedIndexFileInfo inverted_index_file_info(int seg_id);

    const auto& inverted_index_file_info() const {
        return _rowset_meta_pb.inverted_index_file_info();
    }

    void add_inverted_index_files_info(const std::vector<InvertedIndexFileInfo>& idx_file_info);

    void update_inverted_index_files_info(const std::vector<InvertedIndexFileInfo>& idx_file_info);

    // Because the member field '_handle' is a raw pointer, use member func 'init' to replace copy ctor
    RowsetMeta(const RowsetMeta&) = delete;
    RowsetMeta operator=(const RowsetMeta&) = delete;

private:
    bool _deserialize_from_pb(std::string_view value);

    bool _serialize_to_pb(std::string* value);

    void _init();

    friend bool operator==(const RowsetMeta& a, const RowsetMeta& b);

    friend bool operator!=(const RowsetMeta& a, const RowsetMeta& b) { return !(a == b); }

private:
    RowsetMetaPB _rowset_meta_pb;
    TabletSchemaSPtr _schema;
    Cache::Handle* _handle = nullptr;
    RowsetId _rowset_id;
    StorageResource _storage_resource;
    bool _is_removed_from_rowset_meta = false;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_H
