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

namespace doris {
class TabletMeta;
class Rowset;
class Tablet;
class RowBlockPosition;
class DataDir;
class RowsetReader;

using TabletSharedPtr = std::shared_ptr<Tablet>;

class Tablet : public std::enable_shared_from_this<Tablet> {
public:
    Tablet(TabletMeta* tablet_meta, DataDir* data_dir);
    ~Tablet();

    const int64_t table_id() const;
    const std::string table_name() const;
    const int64_t partition_id() const;
    const int64_t tablet_id() const;
    const int64_t schema_hash() const;
    const int16_t shard_id();
    DataDir* data_dir() const;
    KeysType keys_type() const;
    double bloom_filter_fpp() const;
    bool equal(TTabletId tablet_id, TSchemaHash schema_hash);

    TabletSchema* schema() const;
    const std::string& full_name() const;
    size_t num_fields() const;
    size_t num_null_fields();
    size_t num_key_fields();
    size_t num_short_key_fields() const;
    size_t next_unique_id() const;
    size_t num_rows_per_row_block() const;
    CompressKind compress_kind();

    size_t get_field_index(const std::string& field_name) const;
    size_t get_row_size() const;
    size_t all_rowsets_size() const;
    size_t num_rows() const;
    FieldType get_field_type_by_index(size_t index);
    FieldAggregationMethod get_aggregation_by_index(size_t index);
    OLAPStatus test_version(const Version& version);
    VersionEntity get_version_entity_by_version(const Version& version);
    size_t get_rowset_size(const Version& version);

    AlterTabletState alter_tablet_state();
    TabletState tablet_state() const;

    const RowsetSharedPtr get_rowset(int index) const;
    const RowsetSharedPtr lastest_rowset() const;
    OLAPStatus all_rowsets(vector<RowsetSharedPtr> rowsets);
    OLAPStatus modify_rowsets(vector<RowsetSharedPtr>& to_add,
                             vector<RowsetSharedPtr>& to_delete);

    OLAPStatus add_inc_rowset(const Rowset& rowset);
    RowsetSharedPtr get_inc_rowset(const Version& version) const;
    OLAPStatus delete_inc_rowset_by_version(const Version& version);
    OLAPStatus delete_expired_inc_rowset();
    OLAPStatus is_deletion_rowset(const Version& version) const;

    OLAPStatus create_snapshot();
    OLAPStatus capture_consistent_rowsets(const Version& spec_version, vector<std::shared_ptr<RowsetReader>>* rs_readers);
    void acquire_rs_reader_by_version(const vector<Version>& version_vec,
                                      vector<std::shared_ptr<RowsetReader>>* rs_readers) const;
    OLAPStatus release_rs_readers(vector<std::shared_ptr<RowsetReader>>* rs_readers) const;

    RWMutex* meta_lock();
    Mutex* ingest_lock();
    Mutex* base_lock();
    Mutex* cumulative_lock();

    bool has_version(const Version& version) const;
    void list_versions(vector<Version>* versions) const;
    void calc_missed_versions(int64_t spec_version, vector<Version>* missed_versions) const;

    // versions in [0, m_cumulative_layer_point) is base and cumulative versions;
    // versions in [m_cumulative_layer_point, newest_delta_version] is delta versons;
    // 在使用之前对header加锁
    const int32_t cumulative_layer_point() const;
    void set_cumulative_layer_point(const int32_t new_point);
    const size_t get_cumulative_compaction_score() const;
    const size_t get_base_compaction_score() const;
    size_t deletion_rowset_size();
    bool can_do_compaction();

    DeletePredicatePB* add_delete_predicates() {
        return _tablet_meta.add_delete_predicates();
    }

    const google::protobuf::RepeatedPtrField<DeletePredicatePB>&
    delete_predicates();

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
    bool is_used();
    bool is_schema_changing();
    OLAPStatus recover_tablet_until_specfic_version(const int64_t& spec_version);

    size_t get_version_data_size(const Version& version);
    RowsetSharedPtr rowset_with_largest_size();
public:
    DataDir* _data_dir;
    TabletState _state;
    RowsetGraph* _rs_graph;

    TabletMeta _tablet_meta;
    TabletSchema* _schema;
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
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _version_rowset_map;

    DISALLOW_COPY_AND_ASSIGN(Tablet);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_TABLET_H
