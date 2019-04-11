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

#ifndef DORIS_BE_SRC_OLAP_TABLET_META_H
#define DORIS_BE_SRC_OLAP_TABLET_META_H

#include <mutex>
#include <string>
#include <vector>

#include "common/logging.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet_schema.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/delete_handler.h"

using std::string;
using std::vector;

namespace doris {

// Lifecycle states that a Tablet can be in. Legal state transitions for a
// Tablet object:
//
//   NOTREADY -> RUNNING -> TOMBSTONED -> STOPPED -> SHUTDOWN
//      |           |            |          ^^^
//      |           |            +----------+||
//      |           +------------------------+|
//      +-------------------------------------+

enum TabletState {
    // Tablet is under alter table, rollup, clone
    TABLET_NOTREADY,

    TABLET_RUNNING,

    // Tablet integrity has been violated, such as missing versions.
    // In this state, tablet will not accept any incoming request.
    // Report this state to FE, scheduling BE to drop tablet.
    TABLET_TOMBSTONED,

    // Tablet is shutting down, files in disk still remained.
    TABLET_STOPPED,

    // Files have been removed, tablet has been shutdown completely.
    TABLET_SHUTDOWN
};

class RowsetMeta;
class Rowset;
class DataDir;

class AlterTabletTask {
public:
    AlterTabletTask() {}
    OLAPStatus init_from_pb(const AlterTabletPB& alter_task);
    OLAPStatus to_alter_pb(AlterTabletPB* alter_task);

    inline const AlterTabletState& alter_state() const { return _alter_state; }
    inline void set_alter_state(AlterTabletState alter_state) { _alter_state = alter_state; }

    inline int64_t related_tablet_id() const { return _related_tablet_id; }
    inline int64_t related_schema_hash() const { return _related_schema_hash; }
    inline void set_related_tablet_id(int64_t related_tablet_id) { _related_tablet_id = related_tablet_id; }
    inline void set_related_schema_hash(int64_t schema_hash) { _related_schema_hash = schema_hash; }

    inline const AlterTabletType& alter_type() const { return _alter_type; }
    inline void set_alter_type(AlterTabletType alter_type) { _alter_type = alter_type; }

    const vector<RowsetMetaSharedPtr>& rowsets_to_alter() const { return _rowsets_to_alter; }
    void add_rowset_to_alter(const RowsetMetaSharedPtr& rs_meta) {
        return _rowsets_to_alter.push_back(rs_meta);
    }

private:
    AlterTabletState _alter_state;
    int64_t _related_tablet_id;
    int64_t _related_schema_hash;
    AlterTabletType _alter_type;
    vector<RowsetMetaSharedPtr> _rowsets_to_alter;
};


typedef std::shared_ptr<AlterTabletTask> AlterTabletTaskSharedPtr;

// Class encapsulates meta of tablet.
// The concurrency control is handled in Tablet Class, not in this class.
class TabletMeta {
public:
    static OLAPStatus create(int64_t table_id, int64_t partition_id,
                             int64_t tablet_id, int64_t schema_hash,
                             uint64_t shard_id, const TTabletSchema& tablet_schema,
                             uint32_t next_unique_id,
                             const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                             TabletMeta** tablet_meta);
    TabletMeta();
    TabletMeta(DataDir* data_dir);
    TabletMeta(int64_t table_id, int64_t partition_id,
               int64_t tablet_id, int64_t schema_hash,
               uint64_t shard_id, const TTabletSchema& tablet_schema,
               uint32_t next_unique_id,
               const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id);

    // Function create_from_file is used to be compatible with previous tablet_meta.
    // Previous tablet_meta is a physical file in tablet dir, which is not stored in rocksdb.
    OLAPStatus create_from_file(const std::string& file_path);
    OLAPStatus save(const std::string& file_path);
    static OLAPStatus save(const string& file_path, TabletMetaPB& tablet_meta_pb);
    OLAPStatus save_meta();

    OLAPStatus serialize(string* meta_binary) const;
    OLAPStatus deserialize(const string& meta_binary);
    OLAPStatus init_from_pb(const TabletMetaPB& tablet_meta_pb);

    OLAPStatus to_meta_pb(TabletMetaPB* tablet_meta_pb);
    OLAPStatus to_json(std::string* json_string, json2pb::Pb2JsonOptions& options);

    inline void set_data_dir(DataDir* data_dir);

    inline const int64_t table_id() const;
    inline const int64_t partition_id() const;
    inline const int64_t tablet_id() const;
    inline const int64_t schema_hash() const;
    inline const int16_t shard_id() const;
    void set_shard_id(int32_t shard_id);
    inline int64_t creation_time() const;
    void set_creation_time(int64_t creation_time);
    inline int32_t cumulative_layer_point() const;
    void set_cumulative_layer_point(int32_t new_point);

    inline const size_t num_rows() const;
    // disk space occupied by tablet
    inline const size_t tablet_footprint() const;
    inline const size_t version_count() const;
    Version max_version() const;

    inline const TabletState& tablet_state() const;
    inline OLAPStatus set_tablet_state(TabletState state);

    inline const TabletSchema& tablet_schema() const;

    inline const vector<RowsetMetaSharedPtr>& all_rs_metas() const;
    OLAPStatus add_rs_meta(const RowsetMetaSharedPtr& rs_meta);
    RowsetMetaSharedPtr acquire_rs_meta_by_version(const Version& version) const;
    OLAPStatus delete_rs_meta_by_version(const Version& version, vector<RowsetMetaSharedPtr>* deleted_rs_metas);
    OLAPStatus modify_rs_metas(const vector<RowsetMetaSharedPtr>& to_add,
                               const vector<RowsetMetaSharedPtr>& to_delete);
    OLAPStatus revise_rs_metas(const std::vector<RowsetMetaSharedPtr>& rs_metas);
    OLAPStatus revise_inc_rs_metas(const std::vector<RowsetMetaSharedPtr>& rs_metas);

    inline const vector<RowsetMetaSharedPtr>& all_inc_rs_metas() const;
    OLAPStatus add_inc_rs_meta(const RowsetMetaSharedPtr& rs_meta);
    OLAPStatus delete_inc_rs_meta_by_version(const Version& version);
    RowsetMetaSharedPtr acquire_inc_rs_meta_by_version(const Version& version) const;

    OLAPStatus add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version);
    OLAPStatus remove_delete_predicate_by_version(const Version& version);
    DelPredicateArray delete_predicates() const;
    bool version_for_delete_predicate(const Version& version);
    AlterTabletTaskSharedPtr alter_task() const;
    OLAPStatus add_alter_task(const AlterTabletTask& alter_task);
    OLAPStatus delete_alter_task();
    void set_alter_state(AlterTabletState alter_state);

private:
    TabletState _tablet_state;
    TabletSchema _schema;

    vector<RowsetMetaSharedPtr> _rs_metas;
    vector<RowsetMetaSharedPtr> _inc_rs_metas;
    DelPredicateArray _del_pred_array;

    AlterTabletTaskSharedPtr _alter_task;

    DataDir* _data_dir;
    TabletMetaPB _tablet_meta_pb;

    RWMutex _meta_lock;
};

inline void TabletMeta::set_data_dir(DataDir* data_dir) {
    _data_dir = data_dir;
}

inline const int64_t TabletMeta::table_id() const {
    return _tablet_meta_pb.table_id();
}

inline const int64_t TabletMeta::partition_id() const {
    return _tablet_meta_pb.partition_id();
}

inline const int64_t TabletMeta::tablet_id() const {
    return _tablet_meta_pb.tablet_id();
}

inline const int64_t TabletMeta::schema_hash() const {
    return _tablet_meta_pb.schema_hash();
}

inline const int16_t TabletMeta::shard_id() const {
    return _tablet_meta_pb.shard_id();
}

inline int64_t TabletMeta::creation_time() const {
    return _tablet_meta_pb.creation_time();
}

inline int32_t TabletMeta::cumulative_layer_point() const {
    return _tablet_meta_pb.cumulative_layer_point();
}

inline const size_t TabletMeta::num_rows() const {
    size_t num_rows = 0;
    for (auto& rs : _rs_metas) {
        num_rows += rs->num_rows();
    }
    return num_rows;
}

inline const size_t TabletMeta::tablet_footprint() const {
    size_t total_size = 0;
    for (auto& rs : _rs_metas) {
        total_size += rs->data_disk_size();
    }
    return total_size;
}

inline const size_t TabletMeta::version_count() const {
    return _rs_metas.size();
}

inline const TabletState& TabletMeta::tablet_state() const {
    return _tablet_state;
}

inline OLAPStatus TabletMeta::set_tablet_state(TabletState state) {
    switch (state) {
        case TABLET_NOTREADY:
            _tablet_meta_pb.set_tablet_state(PB_NOTREADY);
            break;
        case TABLET_RUNNING:
            _tablet_meta_pb.set_tablet_state(PB_RUNNING);
            break;
        case TABLET_TOMBSTONED:
            _tablet_meta_pb.set_tablet_state(PB_TOMBSTONED);
            break;
        case TABLET_STOPPED:
            _tablet_meta_pb.set_tablet_state(PB_STOPPED);
            break;
        case TABLET_SHUTDOWN:
            _tablet_meta_pb.set_tablet_state(PB_SHUTDOWN);
            break;
        default:
            LOG(WARNING) << "tablet has no state. tablet=" << tablet_id()
                          << ", schema_hash=" << schema_hash();
    }
    return OLAP_SUCCESS;
}

inline const TabletSchema& TabletMeta::tablet_schema() const {
    return _schema;
}

inline const vector<RowsetMetaSharedPtr>& TabletMeta::all_rs_metas() const {
    return _rs_metas;
}

inline const vector<RowsetMetaSharedPtr>& TabletMeta::all_inc_rs_metas() const {
    return _inc_rs_metas;
}

// return value not reference
// MVCC modification for alter task, upper application get a alter task mirror
inline AlterTabletTaskSharedPtr TabletMeta::alter_task() const {
    return _alter_task;
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_TABLET_META_H
