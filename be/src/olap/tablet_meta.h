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

enum TabletState {
    TABLET_NOTREADY,
    TABLET_RUNNING,
    TABLET_TOMBSTONED,
    TABLET_STOPPED,
    TABLET_SHUTDOWN
};

enum AlterTabletState {
    ALTER_NONE,
    ALTER_ALTERING,
    ALTER_FINISHED,
    ALTER_FAILED
};

class RowsetMeta;
class Rowset;
class DataDir;

class AlterTabletTask {
public:
    OLAPStatus init_from_pb(const AlterTabletPB& alter_tablet_task);
    OLAPStatus to_alter_pb(AlterTabletPB* alter_task);
    OLAPStatus clear();

    inline int64_t related_tablet_id() const { return _related_tablet_id; }
    inline int64_t related_schema_hash() const { return _related_schema_hash; }
    inline void set_related_tablet_id(int64_t related_tablet_id) { _related_tablet_id = related_tablet_id; }
    inline void set_related_schema_hash(int64_t schema_hash) { _related_schema_hash = schema_hash; }

    const vector<RowsetMetaSharedPtr>& rowsets_to_alter() const { return _rowsets_to_alter; }

    const AlterTabletState& alter_state() const { return _alter_state; }
    const AlterTabletType& alter_type() const { return _alter_type; }
    void set_alter_type(AlterTabletType alter_type) { _alter_type = alter_type; }
private:
    int64_t _related_tablet_id;
    int64_t _related_schema_hash;
    vector<RowsetMetaSharedPtr> _rowsets_to_alter;
    AlterTabletState _alter_state;
    AlterTabletType _alter_type;
};

class TabletMeta {
public:
    OLAPStatus init();
    OLAPStatus load_and_init();
    int file_delta_size();
    OLAPStatus set_shard(int32_t shard_id);
    OLAPStatus save(const std::string& file_path);
    OLAPStatus clear_schema_change_status();
    OLAPStatus delete_all_versions();
    OLAPStatus delete_version(Version version) const;
    OLAPStatus add_version(Version version, VersionHash version_hash,
                           int32_t segment_group_id, int32_t num_segments,
                           int64_t index_size, int64_t data_size, int64_t num_rows,
                           bool empty, const vector<KeyRange>* column_statistics);
    const PDelta* get_delta(int index) const;
    const uint32_t get_cumulative_compaction_score() const;
    const uint32_t get_base_compaction_score() const;
    void set_cumulative_layer_point(int32_t point);
    int32_t cumulative_layer_point();
    int file_version_size();
    FileVersionMessage& file_version(int32_t index);

    static OLAPStatus create(int64_t table_id, int64_t partition_id,
                             int64_t tablet_id, int64_t schema_hash,
                             uint64_t shard_id, const TTabletSchema& tablet_schema,
                             uint32_t next_unique_id,
                             const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                             TabletMeta** tablet_meta);
    TabletMeta();
    TabletMeta(int64_t table_id, int64_t partition_id,
               int64_t tablet_id, int64_t schema_hash,
               uint64_t shard_id, const TTabletSchema& tablet_schema,
               uint32_t next_unique_id,
               const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id);
    TabletMeta(const std::string& file_name);
    TabletMeta(DataDir* data_dir);

    OLAPStatus init_from_pb(const TabletMetaPB& tablet_meta_pb);
    OLAPStatus serialize(string* meta_binary) const;
    OLAPStatus serialize_unlock(string* meta_binary) const;

    OLAPStatus deserialize(const string& meta_binary);
    OLAPStatus deserialize_unlock(const string& meta_binary);

    OLAPStatus save_meta();
    OLAPStatus save_meta_unlock();

    OLAPStatus to_tablet_pb(TabletMetaPB* tablet_meta_pb);
    OLAPStatus to_tablet_pb_unlock(TabletMetaPB* tablet_meta_pb);

    OLAPStatus add_inc_rs_meta(const RowsetMetaSharedPtr& rs_meta);
    OLAPStatus delete_inc_rs_meta_by_version(const Version& version);
    RowsetMetaSharedPtr acquire_inc_rs_meta(const Version& version) const;

    Version max_version() const;

    OLAPStatus add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version);
    DelPredicateArray delete_predicates();

    inline const vector<RowsetMetaSharedPtr>& all_inc_rs_metas() const;
    inline const vector<RowsetMetaSharedPtr>& all_rs_metas() const;

    OLAPStatus modify_rowsets(const vector<RowsetMetaSharedPtr>& to_add,
                              const vector<RowsetMetaSharedPtr>& to_delete);

    inline const int64_t table_id() const;
    inline const int64_t partition_id() const;
    inline const int64_t tablet_id() const;
    inline const int64_t schema_hash() const;
    inline const int16_t shard_id() const;
    inline const size_t num_rows() const;
    inline const size_t data_size() const;

    inline const TabletState& tablet_state() const;
    inline const AlterTabletTask& alter_task() const;
    inline const AlterTabletState& alter_state() const;
    OLAPStatus add_alter_task(const AlterTabletTask& alter_task);
    OLAPStatus delete_alter_task();

private:
    int64_t _table_id;
    int64_t _partition_id;
    int64_t _tablet_id;
    int64_t _schema_hash;
    int16_t _shard_id;

    TabletSchema _schema;
    vector<RowsetMetaSharedPtr> _rs_metas;
    vector<RowsetMetaSharedPtr> _inc_rs_metas;
    DelPredicateArray _del_pred_array;

    TabletState _tablet_state;
    AlterTabletTask _alter_task;

    TabletMetaPB _tablet_meta_pb;
    DataDir* _data_dir;

    mutable std::mutex _mutex;
};

inline const vector<RowsetMetaSharedPtr>& TabletMeta::all_inc_rs_metas() const {
    return _inc_rs_metas;
}

inline const vector<RowsetMetaSharedPtr>& TabletMeta::all_rs_metas() const {
    return _rs_metas;
}

inline const int64_t TabletMeta::table_id() const {
    return _tablet_id;
}

inline const int64_t TabletMeta::partition_id() const {
    return _partition_id;
}

inline const int64_t TabletMeta::tablet_id() const {
    return _tablet_id;
}

inline const int64_t TabletMeta::schema_hash() const {
    return _schema_hash;
}

inline const int16_t TabletMeta::shard_id() const {
    return _shard_id;
}

inline const size_t TabletMeta::num_rows() const {
    size_t num_rows = 0;
    for (auto& rs : _rs_metas) {
        num_rows += rs->num_rows();
    }
    return num_rows;

}

inline const size_t TabletMeta::data_size() const {
    size_t total_size = 0;
    for (auto& rs : _rs_metas) {
        total_size += rs->data_disk_size();
    }
    return total_size;
}

inline const TabletState& TabletMeta::tablet_state() const {
    return _tablet_state;
}

inline const AlterTabletTask& TabletMeta::alter_task() const {
    return _alter_task;
}

inline const AlterTabletState& TabletMeta::alter_state() const {
    return _alter_task.alter_state();
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_TABLET_META_H
