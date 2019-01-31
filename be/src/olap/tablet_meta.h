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

class RowsetMeta;
class Rowset;
class DataDir;

class AlterTabletTask {
public:
    AlterTabletTask();
    OLAPStatus init_from_pb(const AlterTabletPB& alter_task);
    OLAPStatus to_alter_pb(AlterTabletPB* alter_task);
    OLAPStatus clear();

    const AlterTabletState& alter_state() const { return _alter_state; }
    void set_alter_state(AlterTabletState alter_state) { _alter_state = alter_state; }

    inline int64_t related_tablet_id() const { return _related_tablet_id; }
    inline int64_t related_schema_hash() const { return _related_schema_hash; }
    inline void set_related_tablet_id(int64_t related_tablet_id) { _related_tablet_id = related_tablet_id; }
    inline void set_related_schema_hash(int64_t schema_hash) { _related_schema_hash = schema_hash; }

    const AlterTabletType& alter_type() const { return _alter_type; }
    void set_alter_type(AlterTabletType alter_type) { _alter_type = alter_type; }

    const vector<RowsetMetaSharedPtr>& rowsets_to_alter() const { return _rowsets_to_alter; }

private:
    AlterTabletState _alter_state;
    int64_t _related_tablet_id;
    int64_t _related_schema_hash;
    AlterTabletType _alter_type;
    vector<RowsetMetaSharedPtr> _rowsets_to_alter;
};

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
    OLAPStatus save(const std::string& file_path);
    OLAPStatus create_from_file(const std::string& file_path);

    OLAPStatus serialize(string* meta_binary) const;
    OLAPStatus serialize_unlock(string* meta_binary) const;

    OLAPStatus init_from_pb(const TabletMetaPB& tablet_meta_pb);
    OLAPStatus deserialize(const string& meta_binary);

    OLAPStatus save_meta();
    OLAPStatus save_meta_unlock();

    OLAPStatus to_tablet_pb(TabletMetaPB* tablet_meta_pb);
    OLAPStatus to_tablet_pb_unlock(TabletMetaPB* tablet_meta_pb);

    inline const vector<RowsetMetaSharedPtr>& all_inc_rs_metas() const;
    inline const vector<RowsetMetaSharedPtr>& all_rs_metas() const;
    OLAPStatus add_rs_meta(const RowsetMetaSharedPtr& rs_meta);
    OLAPStatus modify_rs_metas(const vector<RowsetMetaSharedPtr>& to_add,
                               const vector<RowsetMetaSharedPtr>& to_delete);
    OLAPStatus revise_rs_metas(const std::vector<RowsetMetaSharedPtr>& rs_metas);

    OLAPStatus delete_rs_meta_by_version(const Version& version);
    OLAPStatus add_inc_rs_meta(const RowsetMetaSharedPtr& rs_meta);
    OLAPStatus delete_inc_rs_meta_by_version(const Version& version);
    RowsetMetaSharedPtr acquire_inc_rs_meta(const Version& version) const;

    OLAPStatus add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version);
    OLAPStatus remove_delete_predicate_by_version(const Version& version);
    DelPredicateArray delete_predicates() const;
    bool version_for_delete_predicate(const Version& version);

    inline const int64_t table_id() const;
    inline const int64_t partition_id() const;
    inline const int64_t tablet_id() const;
    inline const int64_t schema_hash() const;
    inline const int16_t shard_id() const;
    void set_shard_id(int32_t shard_id);
    inline int64_t creation_time() const;
    void set_creation_time(int64_t creation_time);

    inline const size_t num_rows() const;
    inline const size_t data_size() const;
    inline const size_t version_count() const;
    Version max_version() const;

    inline const TabletState& tablet_state() const;
    inline const AlterTabletTask& alter_task() const;
    inline AlterTabletTask* mutable_alter_task();
    OLAPStatus add_alter_task(const AlterTabletTask& alter_task);
    OLAPStatus delete_alter_task();
    inline int32_t cumulative_layer_point() const;
    void set_cumulative_layer_point(int32_t new_point);
    inline const TabletSchema& tablet_schema() const;

private:
    int64_t _table_id;
    int64_t _partition_id;
    int64_t _tablet_id;
    int64_t _schema_hash;
    int16_t _shard_id;

    int64_t _creation_time;
    int32_t _cumulative_layer_point;

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

inline const size_t TabletMeta::version_count() const {
    return _rs_metas.size();
}

inline const TabletState& TabletMeta::tablet_state() const {
    return _tablet_state;
}

inline const AlterTabletTask& TabletMeta::alter_task() const {
    return _alter_task;
}

inline AlterTabletTask* TabletMeta::mutable_alter_task() {
    return &_alter_task;
}

inline int64_t TabletMeta::creation_time() const {
    return _creation_time;
}

inline int32_t TabletMeta::cumulative_layer_point() const {
    return _cumulative_layer_point;
}

inline const TabletSchema& TabletMeta::tablet_schema() const {
    return _schema;
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_TABLET_META_H
