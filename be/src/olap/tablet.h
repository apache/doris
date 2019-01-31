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
#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/tablet_meta.h"
#include "olap/tuple.h"
#include "olap/row_cursor.h"
#include "olap/rowset_graph.h"
#include "olap/utils.h"
#include "olap/rowset/rowset_reader.h"

namespace doris {
class TabletMeta;
class Rowset;
class Tablet;
class RowBlockPosition;
class DataDir;
class RowsetReader;
class ColumnData;
class SegmentGroup;

using TabletSharedPtr = std::shared_ptr<Tablet>;

class Tablet : public std::enable_shared_from_this<Tablet> {
public:
    static TabletSharedPtr create_from_tablet_meta_file(
            const std::string& header_file,
            DataDir* data_dir = nullptr);
    static TabletSharedPtr create_from_tablet_meta(
            TabletMeta* meta,
            DataDir* data_dir  = nullptr);

    Tablet(TabletMeta* tablet_meta, DataDir* data_dir);
    ~Tablet();

    OLAPStatus load();
    OLAPStatus load_rowsets();
    inline bool is_loaded();
    OLAPStatus save_tablet_meta();

    bool has_expired_incremental_rowset();
    void delete_expired_incremental_rowset();
    OLAPStatus revise_tablet_meta(const TabletMeta& tablet_meta,
                                  const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                                  const std::vector<Version>& versions_to_delete);
    OLAPStatus compute_all_versions_hash(const std::vector<Version>& versions,
                                         VersionHash* version_hash) const;
    OLAPStatus merge_tablet_meta(const TabletMeta& hdr, int to_version);
    bool has_version(const Version& version) const;
    void list_versions(std::vector<Version>* versions) const;
    void mark_dropped() { _is_dropped = true; }
    bool is_dropped() { return _is_dropped; }
    void delete_all_files();
    void obtain_header_rdlock() { _meta_lock.rdlock(); }
    void obtain_header_wrlock() { _meta_lock.wrlock(); }
    void release_header_lock() { _meta_lock.unlock(); }
    RWMutex* get_header_lock_ptr() { return &_meta_lock; }
    void obtain_push_lock() { _ingest_lock.lock(); }
    void release_push_lock() { _ingest_lock.unlock(); }
    Mutex* get_push_lock() { return &_ingest_lock; }
    bool try_base_compaction_lock() { return _base_lock.trylock() == OLAP_SUCCESS; }
    void obtain_base_compaction_lock() { _base_lock.lock(); }
    void release_base_compaction_lock() { _base_lock.unlock(); }
    bool try_cumulative_lock() { return (OLAP_SUCCESS == _cumulative_lock.trylock()); }
    void obtain_cumulative_lock() { _cumulative_lock.lock(); }
    void release_cumulative_lock() { _cumulative_lock.unlock(); }
    std::string construct_pending_data_dir_path() const;
    std::string construct_dir_path() const;
    int version_count() const;
    const uint32_t calc_cumulative_compaction_score() const;
    const uint32_t calc_base_compaction_score() const;
    inline KeysType keys_type() const;
    bool version_for_delete_predicate(const Version& version);
    bool version_for_load_deletion(const Version& version);
    inline const int64_t creation_time() const;
    void set_creation_time(int64_t creation_time);
    inline const int32_t cumulative_layer_point() const;
    inline void set_cumulative_layer_point(const int32_t new_point);
    AlterTabletState alter_state();
    OLAPStatus set_alter_state(AlterTabletState state);
    bool is_schema_changing();
    OLAPStatus delete_alter_task();
    void add_alter_task(int64_t tablet_id, int64_t schema_hash,
                        const vector<Version>& versions_to_alter,
                        const AlterTabletType alter_type);
    const AlterTabletTask& alter_task();
    bool is_used();
    std::string storage_root_path_name() const;
    std::string tablet_path() const;
    OLAPStatus test_version(const Version& version);
    size_t get_version_data_size(const Version& version);
    OLAPStatus recover_tablet_until_specfic_version(const int64_t& spec_version,
                                                    const int64_t& version_hash);
    const std::string& rowset_path_prefix();
    const size_t id() { return _id; }
    void set_id(size_t id) { _id = id; }
    OLAPStatus register_tablet_into_dir();




    OLAPStatus init_once();
    OLAPStatus capture_consistent_rowsets(const Version& spec_version,
                                          vector<RowsetSharedPtr>* rowsets) const;
    OLAPStatus capture_consistent_rowsets(const vector<Version>& version_vec,
                                          vector<RowsetSharedPtr>* rowsets) const;
    OLAPStatus release_rowsets(vector<RowsetSharedPtr>* rowsets) const;
    OLAPStatus capture_rs_readers(const Version& spec_version,
                                  vector<RowsetReaderSharedPtr>* rs_readers) const;
    OLAPStatus capture_rs_readers(const vector<Version>& version_path,
                                  vector<RowsetReaderSharedPtr>* rs_readers) const;
    OLAPStatus release_rs_readers(vector<RowsetReaderSharedPtr>* rs_readers) const;
    OLAPStatus capture_consistent_versions(const Version& version, vector<Version>* span_versions) const;
    OLAPStatus modify_rowsets(std::vector<Version>* old_version,
                              const vector<RowsetSharedPtr>& to_add,
                              const vector<RowsetSharedPtr>& to_delete);
    OLAPStatus add_rowset(RowsetSharedPtr rowset);

    inline const TabletMeta& tablet_meta();
    inline int64_t table_id() const;
    inline std::string table_name() const;
    inline int64_t partition_id() const;
    inline int64_t tablet_id() const;
    inline int64_t schema_hash() const;
    inline int16_t shard_id();
    inline DataDir* data_dir() const;
    inline bool equal(int64_t tablet_id, int64_t schema_hash);

    inline const TabletSchema& tablet_schema() const;
    inline const std::string full_name() const;
    inline size_t num_columns() const;
    inline size_t num_null_columns() const;
    inline size_t num_key_columns() const ;
    inline size_t num_short_key_columns() const;
    inline size_t next_unique_id() const;
    inline size_t num_rows_per_row_block() const;
    inline CompressKind compress_kind() const;
    inline double bloom_filter_fpp() const;

    size_t field_index(const string& field_name) const;
    size_t row_size() const;
    size_t get_index_size() const;
    size_t all_rowsets_size() const;
    size_t get_data_size();
    size_t num_rows();
    size_t get_rowset_size(const Version& version);
    OLAPStatus get_tablet_info(TTabletInfo* tablet_info);

    TabletState tablet_state() const;

    const RowsetSharedPtr get_rowset_by_version(const Version& version) const;
    const RowsetSharedPtr rowset_with_max_version() const;
    RowsetSharedPtr rowset_with_largest_size();
    OLAPStatus all_rowsets(vector<RowsetSharedPtr> rowsets);

    OLAPStatus add_inc_rowset(const RowsetSharedPtr& rowset);
    RowsetSharedPtr get_inc_rowset(const Version& version) const;
    OLAPStatus delete_inc_rowset_by_version(const Version& version);
    OLAPStatus delete_expired_inc_rowset();
    bool is_deletion_rowset(const Version& version);

    RWMutex* meta_lock();
    Mutex* ingest_lock();
    Mutex* base_lock();
    Mutex* cumulative_lock();

    void calc_missed_versions(int64_t spec_version, vector<Version>* missed_versions);

    // This function to find max continous version from the beginning.
    // There are 1, 2, 3, 5, 6, 7 versions belongs tablet.
    // Version 3 is target.
    OLAPStatus max_continuous_version_from_begining(Version* version, VersionHash* v_hash);

    size_t deletion_rowset_size();
    bool can_do_compaction();

    OLAPStatus add_delete_predicates(const DeletePredicatePB& delete_predicate, int64_t version) {
        return _tablet_meta->add_delete_predicate(delete_predicate, version);
    }

    google::protobuf::RepeatedPtrField<DeletePredicatePB>
    delete_predicates() {
        return _tablet_meta->delete_predicates();
    }

    google::protobuf::RepeatedPtrField<DeletePredicatePB>*
    mutable_delete_predicate();

    DeletePredicatePB* mutable_delete_predicate(int index);

    OLAPStatus split_range(
            const OlapTuple& start_key_strings,
            const OlapTuple& end_key_strings,
            uint64_t request_block_row_count,
            vector<OlapTuple>* ranges);

    uint32_t segment_size() const;
    void set_io_error();

private:
    void _delete_incremental_rowset(const Version& version,
                                    const VersionHash& version_hash,
                                    vector<string>* files_to_remove);

    TabletMeta* _tablet_meta;
    TabletSchema _schema;
    DataDir* _data_dir;
    std::string _tablet_path;

    TabletState _state;
    RowsetGraph _rs_graph;

    std::atomic<bool> _is_loaded;
    bool _is_dropped;
    size_t _id;
    Mutex _load_lock;
    RWMutex _meta_lock;
    Mutex _ingest_lock;
    Mutex _base_lock;
    Mutex _cumulative_lock;

    // used for hash-struct of hash_map<Version, Rowset*>.
    struct HashOfVersion {
        size_t operator()(const Version& version) const {
            size_t seed = 0;
            seed = HashUtil::hash64(&version.first, sizeof(version.first), seed);
            seed = HashUtil::hash64(&version.second, sizeof(version.second), seed);
            return seed;
        }
    };
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _rs_version_map;

    DISALLOW_COPY_AND_ASSIGN(Tablet);
};

inline bool Tablet::is_loaded() {
    return _is_loaded;
}

inline const TabletMeta& Tablet::tablet_meta() {
    return *_tablet_meta;
}

inline int64_t Tablet::table_id() const {
    return _tablet_meta->table_id();
}

inline int64_t Tablet::partition_id() const {
    return _tablet_meta->partition_id();
}

inline int64_t Tablet::tablet_id() const {
    return _tablet_meta->tablet_id();
}

inline int64_t Tablet::schema_hash() const {
    return _tablet_meta->schema_hash();
}

inline int16_t Tablet::shard_id() {
    return _tablet_meta->shard_id();
}

inline DataDir* Tablet::data_dir() const {
    return _data_dir;
}

inline bool Tablet::equal(int64_t tablet_id, int64_t schema_hash) {
    return (_tablet_meta->tablet_id() == tablet_id) && (_tablet_meta->schema_hash() == schema_hash);
}

inline const TabletSchema& Tablet::tablet_schema() const {
    return _schema;
}

inline const std::string Tablet::full_name() const {
    std::string tablet_name = std::to_string(_tablet_meta->tablet_id())
                              + "." +
                              std::to_string(_tablet_meta->schema_hash());
    return tablet_name;
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

inline size_t Tablet::next_unique_id() const {
    return _schema.next_column_unique_id();
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

inline KeysType Tablet::keys_type() const {
    return _schema.keys_type();
}

inline const int64_t Tablet::creation_time() const {
    return _tablet_meta->creation_time();
}  // namespace doris

inline void Tablet::set_creation_time(int64_t creation_time) {
    _tablet_meta->set_creation_time(creation_time);
}

inline int Tablet::version_count() const {
    return _tablet_meta->version_count();
}

inline const int32_t Tablet::cumulative_layer_point() const {
    return _tablet_meta->cumulative_layer_point();
}

void inline Tablet::set_cumulative_layer_point(const int32_t new_point) {
    return _tablet_meta->set_cumulative_layer_point(new_point);
}

}

#endif // DORIS_BE_SRC_OLAP_TABLET_H
