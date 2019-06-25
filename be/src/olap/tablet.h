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

#ifndef DORIS_BE_SRC_OLAP_TABLET_H
#define DORIS_BE_SRC_OLAP_TABLET_H

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_define.h"
#include "olap/tuple.h"
#include "olap/row_cursor.h"
#include "olap/rowset_graph.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "util/once.h"

namespace doris {

class DataDir;
class Tablet;
class TabletMeta;

using TabletSharedPtr = std::shared_ptr<Tablet>;

class Tablet : public std::enable_shared_from_this<Tablet> {
public:
    static TabletSharedPtr create_tablet_from_meta_file(
            const std::string& header_file,
            DataDir* data_dir = nullptr);
    static TabletSharedPtr create_tablet_from_meta(
            TabletMetaSharedPtr tablet_meta,
            DataDir* data_dir  = nullptr);

    Tablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir);
    ~Tablet();

    OLAPStatus init_once();
    OLAPStatus init();
    inline bool init_succeeded();

    bool is_used();
    inline DataDir* data_dir() const;
    OLAPStatus register_tablet_into_dir();
    OLAPStatus deregister_tablet_from_dir();
    std::string tablet_path() const;

    // operation for TabletState
    TabletState tablet_state() const { return _state; }
    inline OLAPStatus set_tablet_state(TabletState state);

    // Property encapsulated in TabletMeta
    inline const TabletMetaSharedPtr tablet_meta();
    OLAPStatus save_meta();
    OLAPStatus merge_tablet_meta(const TabletMeta& hdr, int to_version);
    OLAPStatus revise_tablet_meta(const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                                  const std::vector<Version>& versions_to_delete);


    TabletUid tablet_uid();
    inline int64_t table_id() const;
    inline const std::string full_name() const;
    inline int64_t partition_id() const;
    inline int64_t tablet_id() const;
    inline int32_t schema_hash() const;
    inline int16_t shard_id();
    inline const int64_t creation_time() const;
    inline void set_creation_time(int64_t creation_time);
    inline const int64_t cumulative_layer_point() const;
    inline void set_cumulative_layer_point(const int64_t new_point);

    inline bool equal(int64_t tablet_id, int32_t schema_hash);
    inline size_t tablet_footprint(); // disk space occupied by tablet
    inline size_t num_rows();
    inline int version_count() const;
    inline Version max_version() const;

    // propreties encapsulated in TabletSchema
    inline const TabletSchema& tablet_schema() const;
    inline KeysType keys_type() const;
    inline size_t num_columns() const;
    inline size_t num_null_columns() const;
    inline size_t num_key_columns() const ;
    inline size_t num_short_key_columns() const;
    inline size_t num_rows_per_row_block() const;
    inline CompressKind compress_kind() const;
    inline double bloom_filter_fpp() const;
    inline size_t next_unique_id() const;
    inline size_t row_size() const;
    inline size_t field_index(const string& field_name) const;

    // operation in rowsets
    OLAPStatus add_rowset(RowsetSharedPtr rowset);
    OLAPStatus modify_rowsets(const vector<RowsetSharedPtr>& to_add,
                              const vector<RowsetSharedPtr>& to_delete);
    const RowsetSharedPtr get_rowset_by_version(const Version& version) const;
    size_t get_rowset_size_by_version(const Version& version);
    const RowsetSharedPtr rowset_with_max_version() const;
    RowsetSharedPtr rowset_with_largest_size();

    OLAPStatus add_inc_rowset(const RowsetSharedPtr& rowset);
    bool has_expired_inc_rowset();
    void delete_inc_rowset_by_version(const Version& version,
                                      const VersionHash& version_hash);
    void delete_expired_inc_rowsets();

    OLAPStatus capture_consistent_versions(const Version& spec_version, vector<Version>* version_path) const;
    OLAPStatus check_version_integrity(const Version& version);
    bool check_version_exist(const Version& version) const;
    void list_versions(std::vector<Version>* versions) const;

    OLAPStatus capture_consistent_rowsets(const Version& spec_version,
                                          vector<RowsetSharedPtr>* rowsets) const;
    OLAPStatus capture_consistent_rowsets(const vector<Version>& version_path,
                                          vector<RowsetSharedPtr>* rowsets) const;
    OLAPStatus capture_rs_readers(const Version& spec_version,
                                  vector<RowsetReaderSharedPtr>* rs_readers) const;
    OLAPStatus capture_rs_readers(const vector<Version>& version_path,
                                  vector<RowsetReaderSharedPtr>* rs_readers) const;

    DelPredicateArray delete_predicates() { return _tablet_meta->delete_predicates(); }
    OLAPStatus add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version);
    bool version_for_delete_predicate(const Version& version);
    bool version_for_load_deletion(const Version& version);

    // message for alter task
    AlterTabletTaskSharedPtr alter_task();
    OLAPStatus add_alter_task(int64_t related_tablet_id, int32_t related_schema_hash,
                        const vector<Version>& versions_to_alter,
                        const AlterTabletType alter_type);
    OLAPStatus delete_alter_task();
    OLAPStatus set_alter_state(AlterTabletState state);
    OLAPStatus protected_delete_alter_task();

    // meta lock
    inline void obtain_header_rdlock() { _meta_lock.rdlock(); }
    inline void obtain_header_wrlock() { _meta_lock.wrlock(); }
    inline void release_header_lock() { _meta_lock.unlock(); }
    inline RWMutex* get_header_lock_ptr() { return &_meta_lock; }

    // ingest lock
    inline void obtain_push_lock() { _ingest_lock.lock(); }
    inline void release_push_lock() { _ingest_lock.unlock(); }
    inline Mutex* get_push_lock() { return &_ingest_lock; }

    // base lock
    inline bool try_base_compaction_lock() { return _base_lock.trylock() == OLAP_SUCCESS; }
    inline void obtain_base_compaction_lock() { _base_lock.lock(); }
    inline void release_base_compaction_lock() { _base_lock.unlock(); }

    // cumulative lock
    inline bool try_cumulative_lock() { return (OLAP_SUCCESS == _cumulative_lock.trylock()); }
    inline void obtain_cumulative_lock() { _cumulative_lock.lock(); }
    inline void release_cumulative_lock() { _cumulative_lock.unlock(); }

    inline RWMutex* get_migration_lock_ptr() { return &_migration_lock; }

    // operation for compaction
    bool can_do_compaction();
    const uint32_t calc_cumulative_compaction_score() const;
    const uint32_t calc_base_compaction_score() const;
    OLAPStatus compute_all_versions_hash(const std::vector<Version>& versions,
                                         VersionHash* version_hash) const;

    // operation for clone
    void calc_missed_versions(int64_t spec_version, vector<Version>* missed_versions);

    void calc_missed_versions_unlock(int64_t spec_version, vector<Version>* missed_versions) const;

    // This function to find max continous version from the beginning.
    // There are 1, 2, 3, 5, 6, 7 versions belongs tablet.
    // Version 3 is target.
    OLAPStatus max_continuous_version_from_begining(Version* version, VersionHash* v_hash);

    // operation for query
    OLAPStatus split_range(
            const OlapTuple& start_key_strings,
            const OlapTuple& end_key_strings,
            uint64_t request_block_row_count,
            vector<OlapTuple>* ranges);

    // operation for recover tablet
    OLAPStatus recover_tablet_until_specfic_version(const int64_t& spec_version,
                                                    const int64_t& version_hash);

    // I/O Error handler
    void set_io_error();
    void set_bad(bool is_bad) { _is_bad = is_bad; }

    int64_t last_compaction_failure_time() { return _last_compaction_failure_time; }

    void set_last_compaction_failure_time(int64_t time) {
        _last_compaction_failure_time = time;
    }

    void delete_all_files();

    bool check_path(const std::string& check_path);

    // check rowset_id is valid
    bool check_rowset_id(RowsetId rowset_id);

    OLAPStatus next_rowset_id(RowsetId* id);
    OLAPStatus set_next_rowset_id(RowsetId new_rowset_id);

    OLAPStatus set_partition_id(int64_t partition_id);

    RowsetId initial_end_rowset_id() {
        return _tablet_meta->initial_end_rowset_id();
    }

private:
    void _print_missed_versions(const std::vector<Version>& missed_versions) const;
    OLAPStatus _check_added_rowset(const RowsetSharedPtr& rowset);

private:
    TabletState _state;
    TabletMetaSharedPtr _tablet_meta;
    TabletSchema _schema;

    DataDir* _data_dir;
    std::string _tablet_path;
    RowsetGraph _rs_graph;

    DorisInitOnce _init_once;
    RWMutex _meta_lock;
    Mutex _ingest_lock;
    Mutex _base_lock;
    Mutex _cumulative_lock;
    RWMutex _migration_lock;
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _rs_version_map;
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _inc_rs_version_map;

    std::atomic<bool> _is_bad;   // if this tablet is broken, set to true. default is false
    std::atomic<int64_t> _last_compaction_failure_time; // timestamp of last compaction failure

    DISALLOW_COPY_AND_ASSIGN(Tablet);
};

inline bool Tablet::init_succeeded() {
    return _init_once.init_succeeded();
}

inline DataDir* Tablet::data_dir() const {
    return _data_dir;
}

inline OLAPStatus Tablet::set_tablet_state(TabletState state) {
    RETURN_NOT_OK(_tablet_meta->set_tablet_state(state));
    _state = state;
    return OLAP_SUCCESS;
}

inline const TabletMetaSharedPtr Tablet::tablet_meta() {
    return _tablet_meta;
}

inline int64_t Tablet::table_id() const {
    return _tablet_meta->table_id();
}

inline const std::string Tablet::full_name() const {
    std::stringstream ss;
    ss << _tablet_meta->tablet_id() 
       << "." << _tablet_meta->schema_hash() 
       << "." << _tablet_meta->tablet_uid().to_string();
    return ss.str();
}

inline int64_t Tablet::partition_id() const {
    return _tablet_meta->partition_id();
}

inline int64_t Tablet::tablet_id() const {
    return _tablet_meta->tablet_id();
}

inline int32_t Tablet::schema_hash() const {
    return _tablet_meta->schema_hash();
}

inline int16_t Tablet::shard_id() {
    return _tablet_meta->shard_id();
}

inline const int64_t Tablet::creation_time() const {
    return _tablet_meta->creation_time();
}  // namespace doris

inline void Tablet::set_creation_time(int64_t creation_time) {
    _tablet_meta->set_creation_time(creation_time);
}

inline const int64_t Tablet::cumulative_layer_point() const {
    return _tablet_meta->cumulative_layer_point();
}

void inline Tablet::set_cumulative_layer_point(const int64_t new_point) {
    return _tablet_meta->set_cumulative_layer_point(new_point);
}

inline bool Tablet::equal(int64_t tablet_id, int32_t schema_hash) {
    return (_tablet_meta->tablet_id() == tablet_id) && (_tablet_meta->schema_hash() == schema_hash);
}

inline size_t Tablet::tablet_footprint() {
    ReadLock rdlock(&_meta_lock);
    return _tablet_meta->tablet_footprint();
}

inline size_t Tablet::num_rows() {
    ReadLock rdlock(&_meta_lock);
    return _tablet_meta->num_rows();
}

inline int Tablet::version_count() const {
    return _tablet_meta->version_count();
}

inline Version Tablet::max_version() const {
    return _tablet_meta->max_version();
}

inline const TabletSchema& Tablet::tablet_schema() const {
    return _schema;
}

inline KeysType Tablet::keys_type() const {
    return _schema.keys_type();
}

inline size_t Tablet::num_columns() const {
    return _schema.num_columns();
}

inline size_t Tablet::num_null_columns() const {
    return _schema.num_null_columns();
}

inline size_t Tablet::num_key_columns() const {
    return _schema.num_key_columns();
}

inline size_t Tablet::num_short_key_columns() const {
    return _schema.num_short_key_columns();
}

inline size_t Tablet::num_rows_per_row_block() const {
    return _schema.num_rows_per_row_block();
}

inline CompressKind Tablet::compress_kind() const {
    return _schema.compress_kind();
}

inline double Tablet::bloom_filter_fpp() const {
    return _schema.bloom_filter_fpp();
}

inline size_t Tablet::next_unique_id() const {
    return _schema.next_column_unique_id();
}

inline size_t Tablet::field_index(const string& field_name) const {
    return _schema.field_index(field_name);
}

inline size_t Tablet::row_size() const {
    return _schema.row_size();
}

}

#endif // DORIS_BE_SRC_OLAP_TABLET_H
