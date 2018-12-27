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

    inline int64_t related_tablet_id() { return _related_tablet_id; }
    inline int64_t related_schema_hash() { return _related_schema_hash; }
    inline int64_t set_related_tablet_id(int64_t related_tablet_id) { _related_tablet_id = related_tablet_id; }
    inline int64_t set_related_schema_hash(int64_t schema_hash) { _related_schema_hash = schema_hash; }

    vector<RowsetMetaSharedPtr>& rowsets_to_alter() { return _rowsets_to_alter; }

    const AlterTabletState& alter_state() const { return _alter_state; }
    const AlterTabletType& alter_type() const { return _alter_type; }
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
    FileVersionMessage& file_version(int32_t index);
    int file_version_size();
    int file_delta_size();
    Version get_latest_version();
    OLAPStatus set_shard(int32_t shard_id);
    OLAPStatus save(const std::string& file_path);
    OLAPStatus clear_schema_change_status();
    OLAPStatus delete_all_versions();
    OLAPStatus delete_version(Version version);
    OLAPStatus add_version(Version version, VersionHash version_hash,
                           int32_t segment_group_id, int32_t num_segments,
                           int64_t index_size, int64_t data_size, int64_t num_rows,
                           bool empty, const std::vector<KeyRange>* column_statistics);
    const PDelta* get_incremental_version(Version version) const;
    std::string& file_name() const;
    const PDelta* get_delta(int index) const;
    const PDelta* get_base_version() const;
    const uint32_t get_cumulative_compaction_score() const;
    const uint32_t get_base_compaction_score() const;
    const OLAPStatus version_creation_time(const Version& version, int64_t* creation_time) const;
    void set_tablet_id(int64_t tablet_id);
    void set_schema_hash(TSchemaHash schema_hash);
    void set_cumulative_layer_point(int32_t point);
    void set_next_column_unique_id(int32_t unique_id);
    void set_compress_kind(CompressKind kind);
    void set_keys_type(KeysType keys_type);
    void set_data_file_type(DataFileType type);
    int32_t cumulative_layer_point();
    void set_num_rows_per_data_block(size_t default_num_rows_per_column_file_block);

    TabletMeta();
    TabletMeta(const std::string& file_name);
    TabletMeta(DataDir* data_dir);

    OLAPStatus serialize(string* meta_binary);
    OLAPStatus serialize_unlock(string* meta_binary);

    OLAPStatus deserialize(const string& meta_binary);
    OLAPStatus deserialize_unlock(const string& meta_binary);

    OLAPStatus save_meta();
    OLAPStatus save_meta_unlock();

    OLAPStatus to_tablet_pb(TabletMetaPB* tablet_meta_pb);
    OLAPStatus to_tablet_pb_unlock(TabletMetaPB* tablet_meta_pb);

    OLAPStatus add_inc_rs_meta(const RowsetMetaSharedPtr& rs_meta);
    OLAPStatus delete_inc_rs_meta_by_version(const Version& version);
    const RowsetMetaSharedPtr get_inc_rs_meta(const Version& version) const;
    DeletePredicatePB* add_delete_predicates();

    const std::vector<RowsetMetaSharedPtr>& all_inc_rs_metas() const;
    const std::vector<RowsetMetaSharedPtr>& all_rs_metas() const;

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

inline const int16_t TabletMeta::shard_id() {
    return _shard_id;
}

inline const AlterTabletTask& TabletMeta::alter_task() const {
    return _alter_task;
}

inline const AlterTabletState& TabletMeta::alter_state() const {
    return _alter_task.alter_state();
}

inline const TabletState& TabletMeta::tablet_state() const {
    return _tablet_state;
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_TABLET_META_H
