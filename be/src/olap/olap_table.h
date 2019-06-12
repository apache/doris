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

#ifndef DORIS_BE_SRC_OLAP_OLAP_TABLE_H
#define DORIS_BE_SRC_OLAP_OLAP_TABLE_H

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
#include "olap/olap_header.h"
#include "olap/tuple.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"

namespace doris {
class FieldInfo;
class ColumnData;
class OLAPHeader;
class SegmentGroup;
class OLAPTable;
class RowBlockPosition;
class OlapStore;

// Define OLAPTable's shared_ptr. It is used for
typedef std::shared_ptr<OLAPTable> OLAPTablePtr;

enum BaseCompactionStage {
    BASE_COMPACTION_WAITING = 0,
    BASE_COMPACTION_RUNNING = 1,
};

struct BaseCompactionStatus {
    BaseCompactionStatus() : status(BASE_COMPACTION_WAITING), version(-1) {}

    BaseCompactionStage status;
    int32_t version;
};

enum PushStage {
    PUSH_WAITING = 0,
    PUSH_RUNNING = 1,
};

struct PushStatus {
    PushStatus() : status(PUSH_WAITING), version(-1) {}

    PushStage status;
    int32_t version;
};

enum SyncStage {
    SYNC_WAITING = 0,
    SYNC_RUNNING = 1,
    SYNC_DONE = 2,
    SYNC_FAILED = 3,
};

struct SyncStatus {
    SyncStatus() : status(SYNC_WAITING), version(-1) {}

    SyncStage status;
    int32_t version;
};

struct SchemaChangeStatus {
    SchemaChangeStatus() : status(ALTER_TABLE_WAITING), schema_hash(0), version(-1) {}

    AlterTableStatus status;
    SchemaHash schema_hash;
    int32_t version;
};

class OLAPTable : public std::enable_shared_from_this<OLAPTable> {
public:
    static OLAPTablePtr create_from_header_file(
            TTabletId tablet_id,
            TSchemaHash schema_hash,
            const std::string& header_file,
            OlapStore* store = nullptr);
    static OLAPTablePtr create_from_header_file_for_check(
            TTabletId tablet_id,
            TSchemaHash schema_hash,
            const std::string& header_file);

    static OLAPTablePtr create_from_header(
            OLAPHeader* header,
            OlapStore* store = nullptr);

    explicit OLAPTable(OLAPHeader* header, OlapStore* store);
    explicit OLAPTable(OLAPHeader* header);

    virtual ~OLAPTable();

    // Initializes table and loads indices for all versions.
    // Returns OLAP_SUCCESS on success.
    OLAPStatus load();

    bool is_loaded() {
        return _is_loaded;
    }

    OLAPStatus load_indices();

    OLAPStatus save_header();

    OLAPHeader* get_header() {
        return _header;
    }

    OLAPStatus select_versions_to_span(const Version& version,
                                   std::vector<Version>* span_versions) const;

    // Acquire data sources needed for querying the given version.
    // The data source must later be released with release_data_source()
    // to avoid a memory leak. Returns a vector of acquired sources. If
    // the vector is empty, we were unable to obtain the sources.
    //
    // Elements in the output vector are order-sensitive.
    // For example, to fetch version 109, OLAPData vector is returned.
    //      OLAPData:0-100      +
    //      OLAPData:101-110    +
    //      OLAPData:110-110    -
    void acquire_data_sources(const Version& version, std::vector<ColumnData*>* sources) const;

    // Acquire data sources whose versions are specified by version_list.
    // If you want specified OLAPDatas instead of calling
    // OLAPHeader->select_versions_to_span(), call this function. In the
    // scenarios like Cumulative Delta and Base generating, different
    // strategies can be applied.
    // @param [in] version_list
    // @param [out] sources
    void acquire_data_sources_by_versions(const std::vector<Version>& version_list,
                                          std::vector<ColumnData*>* sources) const;

    // Releases the acquired data sources. Returns true on success.
    OLAPStatus release_data_sources(std::vector<ColumnData*>* data_sources) const;

    // Registers a newly created data source, making it available for
    // querying.  Adds a reference to the data source in the header file.
    OLAPStatus register_data_source(const std::vector<SegmentGroup*>& segment_group_vec);

    // Unregisters the data source for given version, frees up resources.
    // resources include memory, files.
    // After unregister, segment_group will point to the associated SegmentGroup.
    OLAPStatus unregister_data_source(const Version& version, std::vector<SegmentGroup*>* segment_group_vec);

    // if pending data is push_for_delete, delete conditions is not null
    OLAPStatus add_pending_version(int64_t partition_id, int64_t transaction_id,
                                 const std::vector<std::string>* delete_conditions);
    OLAPStatus add_pending_segment_group(SegmentGroup* segment_group);
    int32_t current_pending_segment_group_id(int64_t transaction_id);

    OLAPStatus add_pending_data(SegmentGroup* segment_group, const std::vector<TCondition>* delete_conditions);

    bool has_pending_data(int64_t transaction_id);

    bool has_pending_data();

    void delete_pending_data(int64_t transaction_id);

    // check the pending data that still not publish version
    void get_expire_pending_data(std::vector<int64_t>* transaction_ids);

    bool has_expired_incremental_data();
    void delete_expired_incremental_data();

    // don't need header lock, because it occurs before loading tablet
    void load_pending_data();

    OLAPStatus publish_version(int64_t transaction_id, Version version, VersionHash version_hash);

    const PDelta* get_incremental_delta(Version version) const {
        return _header->get_incremental_version(version);
    }

    // calculate holes of version
    // need header rdlock outside
    void get_missing_versions_with_header_locked(
            int64_t until_version, std::vector<Version>* missing_versions) const;

    // check if pending data is push_for_delete
    // need to obtain header rdlock outside
    OLAPStatus is_push_for_delete(int64_t transaction_id, bool* is_push_for_delete) const;

    // need to obtain header wrlock outside
    OLAPStatus clone_data(const OLAPHeader& clone_header,
                          const std::vector<const PDelta*>& clone_deltas,
                          const std::vector<Version>& versions_to_delete);

    // Atomically replaces one set of data sources with another. Returns
    // true on success.
    OLAPStatus replace_data_sources(const std::vector<Version>* old_versions,
                                const std::vector<SegmentGroup*>* new_data_sources,
                                std::vector<SegmentGroup*>* old_data_sources);

    // Computes the cumulative hash for given versions.
    // Only use Base file and Delta files to compute for simplicity and
    // accuracy. XOR operation of version_hash satisfies associative laws and
    // commutative laws. For example,
    //     version(0,99) = version(0,90) + version(91,100) - version(100,100)
    //     version_hash(0,99) = version_hash(0,90)
    //                          ^ version_hash(91,100)
    //                          ^ version_hash(100,100)
    OLAPStatus compute_all_versions_hash(const std::vector<Version>& versions,
                                         VersionHash* version_hash) const;

    // used for restore, merge the (0, to_version) in 'hdr'
    OLAPStatus merge_header(const OLAPHeader& hdr, int to_version);

    // Used by monitoring OLAPTable
    void list_data_files(std::set<std::string>* filenames) const;

    void list_index_files(std::set<std::string>* filenames) const;

    bool has_segment_group(const Version& version, const SegmentGroup* new_segment_group) const;

    void list_versions(std::vector<Version>* versions) const;

    // Return version list and their corresponding version hashes
    void list_version_entities(std::vector<VersionEntity>* version_entities) const;

    // mark this table to be dropped, all files will be deleted when
    // ~OLAPTable()
    void mark_dropped() {
        _is_dropped = true;
    }

    // Delete all files for this table (.hdr, *.dat, *.idx). This should only
    // be called if no one is accessing the table.
    void delete_all_files();

    // Methods to obtain and release locks.
    void obtain_header_rdlock() {
        _header_lock.rdlock();
    }
    void obtain_header_wrlock() {
        _header_lock.wrlock();
    }
    void release_header_lock() {
        _header_lock.unlock();
    }

    RWMutex* get_header_lock_ptr() {
        return &_header_lock;
    }
    
    OLAPStatus try_migration_rdlock() {
        return _migration_lock.tryrdlock();
    }
    
    OLAPStatus try_migration_wrlock() {
        return _migration_lock.trywrlock();
    }
    
    void release_migration_lock() {
        _migration_lock.unlock();
    }

    // Prevent push operations execute concurrently.
    void obtain_push_lock() {
        _push_lock.lock();
    }
    void release_push_lock() {
        _push_lock.unlock();
    }

    Mutex* get_push_lock() {
        return &_push_lock;
    }

    // Prevent base compaction operations execute concurrently.
    bool try_base_compaction_lock() {
        return _base_compaction_lock.trylock() == OLAP_SUCCESS;
    }
    void obtain_base_compaction_lock() {
        _base_compaction_lock.lock();
    }
    void release_base_compaction_lock() {
        _base_compaction_lock.unlock();
    }

    // Prevent cumulative compaction operations execute concurrently.
    bool try_cumulative_lock() {
        return (OLAP_SUCCESS == _cumulative_lock.trylock());
    }

    void obtain_cumulative_lock() {
        _cumulative_lock.lock();
    }

    void release_cumulative_lock() {
        _cumulative_lock.unlock();
    }

    // Construct index file path according version, version_hash and segment
    // We construct file path through header file name. header file name likes:
    //      tables_root_path/db/table/index/table_index_schemaversion.hdr
    // Index file path is:
    //          tables_root_path/db/table/index
    //             /table_index_schemaversion_start_end_versionhash_segment.idx
    // The typical index file path is:
    // /home/work/olap/storage/data/db2/DailyWinfoIdeaStats/PRIMARY/
    // DailyWinfoIdeaStats_PRIMARY_20120428_0_200_735382373247_1.idx
    std::string construct_index_file_path(const Version& version,
                                          VersionHash version_hash,
                                          int32_t segment_group_id, int32_t segment) const;

    // Same as construct_index_file_path except that file suffix is .dat
    // The typical index file path is:
    // /home/work/olap/storage/data/db2/DailyWinfoIdeaStats/PRIMARY/
    // DailyWinfoIdeaStats_PRIMARY_20120428_0_200_735382373247_1.dat
    std::string construct_data_file_path(const Version& version,
                                         VersionHash version_hash,
                                         int32_t segment_group_id, int32_t segment) const;

    // For index file, suffix is "idx", for data file, suffix is "dat".
    static std::string construct_file_path(const std::string& tablet_path,
                                           const Version& version,
                                           VersionHash version_hash,
                                           int32_t segment_group_id, int32_t segment,
                                           const std::string& suffix);

    std::string construct_pending_data_dir_path() const;
    std::string construct_pending_index_file_path(
        TTransactionId transaction_id, int32_t segment_group_id, int32_t segment) const;
    std::string construct_pending_data_file_path(
        TTransactionId transaction_id, int32_t segment_group_id, int32_t segment) const;
    std::string construct_incremental_delta_dir_path() const;
    std::string construct_incremental_index_file_path(
        Version version, VersionHash version_hash, int32_t segment_group_id, int32_t segment) const;
    std::string construct_incremental_data_file_path(
        Version version, VersionHash version_hash, int32_t segment_group_id, int32_t segment) const;

    std::string construct_file_name(const Version& version,
                                    VersionHash version_hash,
                                    int32_t segment_group_id, int32_t segment,
                                    const std::string& suffix) const;

    // Return -1 if field name is invalid, else return field index in schema.
    int32_t get_field_index(const std::string& field_name) const;

    // Return 0 if file_name is invalid, else return field size in schema.
    size_t get_field_size(const std::string& field_name) const;

    size_t get_return_column_size(const std::string& field_name) const;

    // One row in a specified OLAPTable comprises of fixed number of columns
    // with fixed length.
    size_t get_row_size() const;

    // Get olap table statistics for SHOW STATUS
    size_t get_index_size() const;

    int64_t get_data_size() const;

    int64_t get_num_rows() const;

    // Returns fully qualified name for this OLAP table.
    // eg. db4.DailyUnitStats.PRIMARY
    const std::string& full_name() const {
        return _full_name;
    }

    void set_full_name(std::string full_name) {
        _full_name = full_name;
    }

    std::vector<FieldInfo>& tablet_schema() {
        return _tablet_schema;
    }

    size_t num_fields() const {
        return _num_fields;
    }

    size_t num_null_fields() const {
        return _num_null_fields;
    }

    size_t num_key_fields() const {
        return _num_key_fields;
    }

    size_t id() const {
        return _id;
    }

    void set_id(size_t id) {
        _id = id;
    }

    // Expose some header attributes
    const std::string header_file_name() const {
        return _header->file_name();
    }

    TTabletId tablet_id() const {
        return _tablet_id;
    }

    void set_tablet_id(TTabletId tablet_id)      {
        _tablet_id = tablet_id;
    }

    size_t num_short_key_fields() const {
        return _header->num_short_key_fields();
    }

    uint32_t next_unique_id() const {
        return _header->next_column_unique_id();
    }

    TSchemaHash schema_hash() const {
        return _schema_hash;
    }

    void set_schema_hash(TSchemaHash schema_hash) {
        _schema_hash = schema_hash;
    }

    OlapStore* store() const {
        return _store;
    }

    int file_delta_size() const {
        return _header->file_delta_size();
    }

    const PDelta& delta(int index) const {
        return _header->delta(index);
    }

    const PDelta* get_delta(int index) const {
        return _header->get_delta(index);
    }

    const PDelta* lastest_delta() const {
        return _header->get_lastest_delta_version();
    }

    const PDelta* lastest_version() const {
        return _header->get_lastest_version();
    }

    // need to obtain header rdlock outside
    const PDelta* least_complete_version(
        const std::vector<Version>& missing_versions) const;

    const PDelta* base_version() const {
        return _header->get_base_version();
    }

    // 在使用之前对header加锁
    const uint32_t get_cumulative_compaction_score() const {
        return _header->get_cumulative_compaction_score();
    }

    const uint32_t get_base_compaction_score() const {
        return _header->get_base_compaction_score();
    }

    const OLAPStatus delete_version(const Version& version) {
        return _header->delete_version(version);
    }

    const OLAPStatus version_creation_time(const Version& version, int64_t* creation_time) {
        return _header->version_creation_time(version, creation_time);
    }

    DataFileType data_file_type() const {
        return _header->data_file_type();
    }

    // num rows per rowBlock, typically it is 256 or 512.
    size_t num_rows_per_row_block() const {
        return _num_rows_per_row_block;
    }

    CompressKind compress_kind() const {
        return _compress_kind;
    }

    int delete_data_conditions_size() const {
        return _header->delete_data_conditions_size();
    }

    DeleteConditionMessage* add_delete_data_conditions() {
        return _header->add_delete_data_conditions();
    }

    const google::protobuf::RepeatedPtrField<DeleteConditionMessage>&
    delete_data_conditions() {
        return _header->delete_data_conditions();
    }

    google::protobuf::RepeatedPtrField<DeleteConditionMessage>*
    mutable_delete_data_conditions() {
        return _header->mutable_delete_data_conditions();
    }

    DeleteConditionMessage* mutable_delete_data_conditions(int index) {
        return _header->mutable_delete_data_conditions(index);
    }

    double bloom_filter_fpp() const {
        if (_header->has_bf_fpp()) {
            return _header->bf_fpp();
        }

        return BLOOM_FILTER_DEFAULT_FPP;
    }

    KeysType keys_type() const {
        if (_header->has_keys_type()) {
            return _header->keys_type();
        }

        return KeysType::AGG_KEYS;
    }

    bool is_delete_data_version(Version version) {
        return _header->is_delete_data_version(version);
    }

    bool is_load_delete_version(Version version);

    const int64_t creation_time() const {
        return _header->creation_time();
    }

    void set_creation_time(int64_t time_seconds) {
        _header->set_creation_time(time_seconds);
    }

    // versions in [0, m_cumulative_layer_point) is base and cumulative versions;
    // versions in [m_cumulative_layer_point, newest_delta_version] is delta versons;
    // 在使用之前对header加锁
    const int32_t cumulative_layer_point() const {
        return _header->cumulative_layer_point();
    }

    // 在使用之前对header加锁
    void set_cumulative_layer_point(const int32_t new_point) {
        LOG(INFO) << "cumulative_layer_point: " << new_point;
        _header->set_cumulative_layer_point(new_point);
    }

    // Judge whether olap table in schema change state
    bool is_schema_changing();

    bool get_schema_change_request(TTabletId* tablet_id,
                                   SchemaHash* schema_hash,
                                   std::vector<Version>* versions_to_changed,
                                   AlterTabletType* alter_table_type) const;

    void set_schema_change_request(TTabletId tablet_id,
                                   TSchemaHash schema_hash,
                                   const std::vector<Version>& versions_to_changed,
                                   const AlterTabletType alter_table_type);

    bool remove_last_schema_change_version(OLAPTablePtr new_olap_table);
    void clear_schema_change_request();

    SchemaChangeStatus schema_change_status() {
        return _schema_change_status;
    }

    void set_schema_change_status(AlterTableStatus status,
                                  SchemaHash schema_hash,
                                  int32_t version) {
        _schema_change_status.status = status;
        _schema_change_status.schema_hash = schema_hash;
        _schema_change_status.version = version;
        VLOG(3) << "set schema change status. tablet_id=" << _tablet_id
                << ", schema_hash=" << _schema_change_status.schema_hash
                << ", status=" << _schema_change_status.status;
    }

    void clear_schema_change_status() {
        set_schema_change_status(ALTER_TABLE_WAITING, 0, -1);
    }

    bool equal(TTabletId tablet_id, TSchemaHash schema_hash) {
        if (this->tablet_id() != tablet_id || this->schema_hash() != schema_hash) {
            return false;
        }

        return true;
    }

    OLAPStatus split_range(
            const OlapTuple& start_key_strings,
            const OlapTuple& end_key_strings,
            uint64_t request_block_row_count,
            std::vector<OlapTuple>* ranges);

    uint32_t segment_size() const {
        return _header->segment_size();
    }

    void set_io_error();

    bool is_used();

    void set_bad(bool is_bad) { _is_bad = is_bad; }

    int64_t last_compaction_failure_time() { return _last_compaction_failure_time; }

    void set_last_compaction_failure_time(int64_t time) {
        _last_compaction_failure_time = time;
    }

    // 得到当前table的root path路径，路径末尾不带斜杠(/)
    std::string storage_root_path_name() {
        return _storage_root_path;
    }

    std::string tablet_path() const {
        return _tablet_path;
    }

    std::string get_field_name_by_index(uint32_t index) {
        if (index < _tablet_schema.size()) {
            return _tablet_schema[index].name;
        }

        return "";
    }

    FieldType get_field_type_by_index(uint32_t index) {
        if (index < _tablet_schema.size()) {
            return _tablet_schema[index].type;
        }
    
        return OLAP_FIELD_TYPE_NONE;
    }

    FieldAggregationMethod get_aggregation_by_index(uint32_t index) {
        if (index < _tablet_schema.size()) {
            return _tablet_schema[index].aggregation;
        }

        return OLAP_FIELD_AGGREGATION_UNKNOWN;
    }

    OLAPStatus test_version(const Version& version);

    VersionEntity get_version_entity_by_version(const Version& version);
    size_t get_version_index_size(const Version& version);
    size_t get_version_data_size(const Version& version);

    bool is_dropped() {
        return _is_dropped;
    }

    OLAPStatus recover_tablet_until_specfic_version(const int64_t& until_version,
                                                    const int64_t& version_hash);
private:
    // used for hash-struct of hash_map<Version, SegmentGroup*>.
    struct HashOfVersion {
        uint64_t operator()(const Version& version) const {
            uint64_t hash_value = version.first;
            hash_value = (hash_value << 32) + version.second;
            return hash_value;
        }
    };

    struct HashOfString {
        size_t operator()(const std::string& str) const {
            return std::hash<std::string>()(str);
        }
    };

    // List files with suffix "idx" or "dat".
    void _list_files_with_suffix(const std::string& file_suffix,
                                 std::set<std::string>* file_names) const;

    OLAPStatus _publish_version(int64_t transaction_id, Version version, VersionHash version_hash);

    // 获取最大的index（只看大小）
    SegmentGroup* _get_largest_index();

    SegmentGroup* _construct_segment_group_from_version(const PDelta* delta, int32_t segment_group_id);

    // check if version is same, may delete local data
    OLAPStatus _handle_existed_version(int64_t transaction_id, const Version& version,
                                       const VersionHash& version_hash);

    // like "9-9" "10-10", for incremental cloning
    OLAPStatus _add_incremental_data(std::vector<SegmentGroup*>& index_vec, int64_t transaction_id,
                                     const Version& version, const VersionHash& version_hash);

    void _delete_incremental_data(const Version& version, const VersionHash& version_hash,
                                  std::vector<std::string>* files_to_remove);

    OLAPStatus _create_hard_link(const std::string& from, const std::string& to,
                                 std::vector<std::string>* linked_success_files);

    TTabletId _tablet_id;
    TSchemaHash _schema_hash;
    OLAPHeader* _header;
    size_t _num_rows_per_row_block;
    CompressKind _compress_kind;
    // Set it true when table is dropped, table files and data structures
    // can be used and not deleted until table is destructed.
    bool _is_dropped;
    std::string _full_name;
    std::vector<FieldInfo> _tablet_schema;  // field info vector is table schema.

    // Version mapping to SegmentGroup.
    // data source can be base delta, cumulative delta, singleton delta.
    using version_olap_index_map_t = std::unordered_map<Version, std::vector<SegmentGroup*>, HashOfVersion>;
    version_olap_index_map_t _data_sources;
    using transaction_olap_index_map_t = std::unordered_map<int64_t, std::vector<SegmentGroup*>>;
    transaction_olap_index_map_t _pending_data_sources;

    size_t _num_fields;
    size_t _num_null_fields;
    size_t _num_key_fields;
    // filed name -> field position in row
    using field_index_map_t = std::unordered_map<std::string, int32_t, HashOfString>;
    field_index_map_t _field_index_map;
    std::vector<int32_t> _field_sizes;
    // A series of status
    SchemaChangeStatus _schema_change_status;
    // related locks to ensure that commands are executed correctly.
    RWMutex _header_lock;    
    RWMutex _migration_lock;
    Mutex _push_lock;
    Mutex _cumulative_lock;
    Mutex _base_compaction_lock;
    size_t _id;                        // uniq id, used in cache
    std::string _storage_root_path;
    OlapStore* _store;
    std::atomic<bool> _is_loaded;
    Mutex _load_lock;
    std::string _tablet_path;

    bool _table_for_check;
    std::atomic<bool> _is_bad;   // if this tablet is broken, set to true. default is false
    std::atomic<int64_t> _last_compaction_failure_time; // timestamp of last compaction failure

    DISALLOW_COPY_AND_ASSIGN(OLAPTable);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_TABLE_H
