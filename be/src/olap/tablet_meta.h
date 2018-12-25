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

#include <string>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/Types_types.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"

using std::string;
using std::vector;

namespace doris {

enum TabletState {
    NOTREADY,
    RUNNING,
    TOMBSTONED,
    STOPPED,
    SHUTDOWN
};

enum AlterTabletState {
    NONE,
    RUNNING,
    FINISHED,
    FAILED
};

enum AlterTabletType {
    SCHEMA_CHANGE = 0,
    ROLLUP = 1
}

class AlterTabletTask {
public:
    OLAPStatus deserialize_from_pb(const AlterTabletPB& alter_tablet_task);
    OLAPStatus to_meta_pb(AlterTabletTaskPB* alter_task);
    OLAPStatus clear();

    inline int64_t related_tablet_id() { return _related_tablet_id; }
    inline int64_t related_schema_hash() { return _related_schema_hash; }

    vector<RowsetMeta>& rowsets_to_alter() { return _rowsets_to_alter; }

    AlterTabletState alter_state() { return _alter_state; }
    AlterTabletType { return _alter_type; }
private:
    int64_t _related_tablet_id;
    int64_t _related_schema_hash;
    vector<RowsetMeta> _rowsets_to_alter;
    AlterTabletState _alter_state;
    AlterTabletType _alter_type;
}

class TabletMeta {
public:
    TabletMeta(DataDir* data_dir);

    OLAPStatus serialize(string* meta_binary);
    OLAPStatus serialize_unlock(string* meta_binary);

    OLAPStatus deserialize(const string& meta_binary);
    OLAPStatus deserialize_unlock(const string& meta_binary);

    OLAPStatus save_meta();
    OLAPStatus save_meta_unlock();

    OLAPStatus to_meta_pb(TabletMetaPB* tablet_meta_pb);
    OLAPStatus to_meta_pb_unlock(TabletMetaPB* tablet_meta_pb);

    Newstatus add_inc_rs_meta(const RowsetMeta& rs_meta);
    OLAPStatus delete_inc_rs_meta_by_version(const Version& version);
    const RowsetMetaSharedPtr get_inc_rs_meta(const Version& version) const;

    const std::vector<RowsetMeta>& all_inc_rs_metas() const;
    const std::vector<RowsetMeta>& all_rs_metas() const;

    OLAPStatus modify_rowsets(const vector<RowsetMetaSharedPtr>& to_add,
                              const vector<RowsetMetaSharedPtr>& to_delete);

    inline const int64_t table_id() const;
    inline const string table_name() const;
    inline const int64_t partition_id() const;
    inline const int64_t tablet_id() const;
    inline const int64_t schema_hash() const;
    inline const int16_t shard_id();

    inline const AlterTabletTask& alter_task() const;
    inline const AlterTabletState& alter_state() const;
    OLAPStatus add_alter_task(const AlterTabletTask& alter_task);
    OLAPStatus delete_alter_task();

    inline const TabletState& tablet_state() const;
private:

    int64_t _table_id;
    int64_t _partition_id;
    int64_t _tablet_id;
    int64_t _schema_hash;
    int16_t _shard_id;

    TabletSchema _schema;
    vector<RowsetMetaSharedPtr> _rs_metas;
    vector<RowsetMetaSharedPtr> _inc_rs_metas;

    TabletState _tablet_state;
    AlterTabletTask _alter_task;

    TabletMetaPB _tablet_meta_pb;
    DataDir* _data_dir;

    std::mutex _mutex;
};

inline const int64_t table_id() const {
    return _tablet_id;
}

inline const int64_t partition_id() const {
    return _partition_id;
}

inline const int64_t tablet_id() const {
    return _tablet_id;
}

inline const int64_t schema_hash() const {
    return _schema_hash;
}

inline const int16_t shard_id() {
    return _shard_id;
}

inline const AlterTabletTask& alter_task() const {
    return _alter_task;
}

inline const AlterTabletState& alter_state() const {
    return _alter_task.alter_state();
}

inline const TabletState& tablet_state() const {
    return _tablet_state;
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_TABLET_META_H
