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

#include "olap/tablet.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>

#include <algorithm>
#include <map>
#include <set>

#include <boost/filesystem.hpp>

#include "olap/field.h"
#include "olap/rowset/column_data.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/olap_index.h"
#include "olap/reader.h"
#include "olap/data_dir.h"
#include "olap/row_cursor.h"
#include "util/defer_op.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/column_data.h"
#include "olap/rowset/segment_group.h"

using std::pair;
using std::map;
using std::nothrow;
using std::set;
using std::sort;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

TabletSharedPtr Tablet::create_from_tablet_meta_file(
            const string& file_path, DataDir* data_dir) {
    TabletMeta* tablet_meta = NULL;
    tablet_meta = new(nothrow) TabletMeta();
    if (tablet_meta == nullptr) {
        LOG(WARNING) << "fail to malloc TabletMeta.";
        return NULL;
    }

    if (tablet_meta->create_from_file(file_path) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to load tablet_meta. file_path=" << file_path;
        delete tablet_meta;
        return nullptr;
    }

    // add new fields
    boost::filesystem::path file_path_path(file_path);
    std::string shard_path = file_path_path.parent_path().parent_path().parent_path().string();
    std::string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    uint64_t shard = stol(shard_str);
    tablet_meta->set_shard_id(shard);

    // save tablet_meta info to kv db
    // tablet_meta key format: tablet_id + "_" + schema_hash
    OLAPStatus res = TabletMetaManager::save(data_dir, tablet_meta->tablet_id(),
                                             tablet_meta->schema_hash(), tablet_meta);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to save tablet_meta to db. file_path=" << file_path;
        delete tablet_meta;
        return nullptr;
    }
    return create_from_tablet_meta(tablet_meta, data_dir);
}

TabletSharedPtr Tablet::create_from_tablet_meta(
        TabletMeta* tablet_meta,
        DataDir* data_dir) {
    TabletSharedPtr tablet = std::make_shared<Tablet>(tablet_meta, data_dir);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to malloc a table.";
        return nullptr;
    }

    return tablet;
}

Tablet::Tablet(TabletMeta* tablet_meta, DataDir* data_dir)
  : _tablet_meta(tablet_meta),
    _schema(tablet_meta->tablet_schema()),
    _data_dir(data_dir) {
    _is_loaded = false;
    _is_dropped = false;
    std::stringstream tablet_path_stream;
    tablet_path_stream << _data_dir->path() << DATA_PREFIX << "/" << _tablet_meta->shard_id();
    tablet_path_stream << "/" << _tablet_meta->tablet_id() << "/" << _tablet_meta->schema_hash();
    _tablet_path = tablet_path_stream.str();
    _rs_graph->construct_rowset_graph(_tablet_meta->all_rs_metas());
}

Tablet::~Tablet() {
    // ensure that there is nobody using Tablet, like acquiring OLAPData(SegmentGroup)
    WriteLock wrlock(&_meta_lock);
    _rs_version_map.clear();
}

OLAPStatus Tablet::load() {
    OLAPStatus res = OLAP_SUCCESS;
    MutexLock l(&_load_lock);

    if (_is_loaded) {
        return OLAP_SUCCESS;
    }

    res = load_rowsets();

    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to load indices. res=" << res
                    << ", tablet='" << full_name();
        TabletManager::instance()->drop_tablet(tablet_id(), schema_hash());
        return res;
    }

    _is_loaded = true;
    return res;
}

OLAPStatus Tablet::load_rowsets() {
    OLAPStatus res = OLAP_SUCCESS;
    ReadLock rdlock(&_meta_lock);
    TabletMeta* tablet_meta = _tablet_meta;
    VLOG(3) << "begin to load indices. table=" << full_name()
            << ", version_size=" << tablet_meta->version_count();
    for (auto& rs_meta :  tablet_meta->all_rs_metas()) {
        Version version = { rs_meta->start_version(), rs_meta->end_version() };
        RowsetSharedPtr rowset(new AlphaRowset(&_schema, _tablet_path, _data_dir, rs_meta));
        _rs_version_map[version] = rowset;
    }

    for (auto& it : _rs_version_map) {
        res = it.second->init();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load rowset. "
                         << "version=" << it.first.first << "-" << it.first.second;
            return res;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus Tablet::init_once() {
    ReadLock rdlock(&_meta_lock);
    for (auto& it : _tablet_meta->all_rs_metas()) {
        Version version = it->version();
        RowsetSharedPtr rowset(nullptr);
        rowset->init();
        _rs_version_map[version] = rowset;
        rowset->init();
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::save_tablet_meta() {
    OLAPStatus res = _tablet_meta->save_meta();
    if (res != OLAP_SUCCESS) {
       LOG(WARNING) << "fail to save tablet_meta. res=" << res
                    << ", root=" << _data_dir->path();
    }

    return res;
}

bool Tablet::can_do_compaction() {
    // 如果table正在做schema change，则通过选路判断数据是否转换完成
    // 如果选路成功，则转换完成，可以进行BE
    // 如果选路失败，则转换未完成，不能进行BE
    ReadLock rdlock(&_meta_lock);
    const RowsetSharedPtr lastest_delta = rowset_with_max_version();
    if (lastest_delta == NULL) {
        return false;
    }

    Version test_version = Version(0, lastest_delta->end_version());
    vector<Version> path_versions;
    if (OLAP_SUCCESS != _rs_graph->capture_consistent_versions(test_version, &path_versions)) {
        LOG(WARNING) << "tablet has missed version. tablet=" << full_name();
        return false;
    }

    if (this->is_schema_changing()) {
        Version test_version = Version(0, lastest_delta->end_version());
        vector<Version> path_versions;
        if (OLAP_SUCCESS != _rs_graph->capture_consistent_versions(test_version, &path_versions)) {
            return false;
        }
    }

    return true;
}

OLAPStatus Tablet::compute_all_versions_hash(const vector<Version>& versions,
                                             VersionHash* version_hash) const {
    DCHECK(version_hash != nullptr) << "invalid parameter, version_hash is nullptr";

    int64_t v_hash  = 0L;
    for (auto version : versions) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            LOG(WARNING) << "fail to find Rowset. "
                << "version=" << version.first << "-" << version.second;
            return OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR;
        }
        v_hash ^= it->second->version_hash();
    }
    *version_hash = v_hash;
    return OLAP_SUCCESS;
}

void Tablet::delete_all_files() {
    // Release resources like memory and disk space.
    // we have to call list_versions first, or else error occurs when
    // removing hash_map item and iterating hash_map concurrently.
    for (auto it = _rs_version_map.begin(); it != _rs_version_map.end(); ++it) {
        it->second->remove();
        _rs_version_map.erase(it);
    }
}

string Tablet::construct_pending_data_dir_path() const {
    return _tablet_path + PENDING_DELTA_PREFIX;
}

string Tablet::construct_dir_path() const {
    return _tablet_path;
}

OLAPStatus Tablet::capture_consistent_rowsets(const Version& spec_version,
                                              vector<RowsetSharedPtr>* rowsets) const {
    vector<Version> version_path;
    _rs_graph->capture_consistent_versions(spec_version, &version_path);

    capture_consistent_rowsets(version_path, rowsets);
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_consistent_rowsets(const vector<Version>& version_path,
                                        vector<RowsetSharedPtr>* rowsets) const {
    DCHECK(rowsets != nullptr && rowsets->empty());
    for (auto& version : version_path) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            LOG(WARNING) << "fail to find Rowset for version. tablet=" << full_name()
                         << ", version='" << version.first << "-" << version.second;
            release_rowsets(rowsets);
            return OLAP_SUCCESS;
        }

        rowsets->push_back(it->second);
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::release_rowsets(vector<RowsetSharedPtr>* rowsets) const {
    DCHECK(rowsets != nullptr) << "rowsets is null. tablet=" << full_name();
    rowsets->clear();
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_rs_readers(const Version& spec_version,
                                      vector<RowsetReaderSharedPtr>* rs_readers) const {
    vector<Version> version_path;
    _rs_graph->capture_consistent_versions(spec_version, &version_path);

    capture_rs_readers(version_path, rs_readers);
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_rs_readers(const vector<Version>& version_path,
                                vector<RowsetReaderSharedPtr>* rs_readers) const {
    DCHECK(rs_readers != NULL && rs_readers->empty());
    for (auto version : version_path) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            LOG(WARNING) << "fail to find Rowset for version. tablet=" << full_name()
                         << ", version='" << version.first << "-" << version.second;
            release_rs_readers(rs_readers);
            return OLAP_SUCCESS;
        }

        std::shared_ptr<RowsetReader> rs_reader(it->second->create_reader());
        rs_readers->push_back(rs_reader);
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::release_rs_readers(vector<RowsetReaderSharedPtr>* rs_readers) const {
    DCHECK(rs_readers != nullptr) << "rs_readers is null. tablet=" << full_name();
    rs_readers->clear();
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_consistent_versions(
                        const Version& version, vector<Version>* span_versions) const {
    OLAPStatus status = _rs_graph->capture_consistent_versions(version, span_versions);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to generate shortest version path. tablet=" << full_name()
                     << ", version='" << version.first << "-" << version.second;
    }
    return status;
}

OLAPStatus Tablet::add_inc_rowset(const Rowset& rowset) {
    return _tablet_meta->add_inc_rs_meta(rowset.rowset_meta());
}

OLAPStatus Tablet::delete_expired_inc_rowset() {
    time_t now = time(NULL);
    vector<Version> expired_versions;
    WriteLock wrlock(&_meta_lock);
    for (auto& it : _tablet_meta->all_inc_rs_metas()) {
        double diff = difftime(now, it->creation_time());
        if (diff >= config::inc_rowset_expired_sec) {
            expired_versions.push_back(it->version());
        }
    }
    for (auto& it : expired_versions) {
        delete_inc_rowset_by_version(it);
        VLOG(3) << "delete expired inc_rowset. tablet=" << full_name()
                << ", version=" << it.first << "-" << it.second;
    }

    if (_tablet_meta->save_meta() != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet_meta when delete expired inc_rowset. "
                   << "tablet=" << full_name();
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::delete_inc_rowset_by_version(const Version& version) {
    _tablet_meta->delete_inc_rs_meta_by_version(version);
    VLOG(3) << "delete inc rowset. tablet=" << full_name()
            << ", version=" << version.first << "-" << version.second;
    return OLAP_SUCCESS;
}

void Tablet::calc_missed_versions(int64_t spec_version,
                                  vector<Version>* missed_versions) {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;
    ReadLock rdlock(&_meta_lock);
    std::list<Version> existing_versions;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    existing_versions.sort([](const Version& a, const Version& b) {
        // simple because 2 versions are certainly not overlapping
        return a.first < b.first;
    });

    // find the missing version until spec_version
    int64_t last_version = -1;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            for (int64_t i = last_version + 1; i < version.first; ++i) {
                missed_versions->emplace_back(i, i);
            }
        }
        last_version = version.second;
        if (spec_version <= last_version) {
            break;
        }
    }
    for (int64_t i = last_version + 1; i <= spec_version; ++i) {
        missed_versions->emplace_back(i, i);
    }
}

OLAPStatus Tablet::max_continuous_version_from_begining(Version* version, VersionHash* v_hash) {
    ReadLock rdlock(&_meta_lock);
    std::vector<std::pair<Version, VersionHash>> existing_versions;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version() , rs->version_hash());
    }

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const std::pair<Version, VersionHash>& left,
                 const std::pair<Version, VersionHash>& right) {
                 // simple because 2 versions are certainly not overlapping
                 return left.first.first < right.first.first;
              });
    Version max_continuous_version = { -1, 0 };
    VersionHash max_continuous_version_hash = 0;
    for (int i = 0; i < existing_versions.size(); ++i) {
        if (existing_versions[i].first.first > max_continuous_version.first + 1) {
            break;
        }
        max_continuous_version = existing_versions[i].first;
        max_continuous_version_hash = existing_versions[i].second;
    }
    *version = max_continuous_version;
    *v_hash = max_continuous_version_hash;
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::get_tablet_info(TTabletInfo* tablet_info) {
    ReadLock rdlock(&_meta_lock);
    tablet_info->tablet_id = _tablet_meta->tablet_id();
    tablet_info->schema_hash = _tablet_meta->schema_hash();
    tablet_info->row_count = _tablet_meta->num_rows();
    tablet_info->data_size = _tablet_meta->data_size();
    Version version = { -1, 0 };
    VersionHash v_hash = 0;
    max_continuous_version_from_begining(&version, &v_hash);
    tablet_info->version = version.second;
    tablet_info->version_hash = v_hash;
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::modify_rowsets(std::vector<Version>* old_version,
                                  const vector<RowsetSharedPtr>& to_add,
                                  const vector<RowsetSharedPtr>& to_delete) {
    WriteLock wrlock(&_meta_lock);
    vector<RowsetMetaSharedPtr> rs_metas_to_add;
    for (auto& rs : to_add) {
        rs_metas_to_add.push_back(rs->rowset_meta());
    }

    vector<RowsetMetaSharedPtr> rs_metas_to_delete;
    for (auto& rs : to_delete) {
        rs_metas_to_delete.push_back(rs->rowset_meta());
    }

    _tablet_meta->modify_rs_metas(rs_metas_to_add, rs_metas_to_delete);
    for (auto& rs : to_delete) {
        auto it = _rs_version_map.find(rs->version());
        _rs_version_map.erase(it);
    }
    _rs_graph->reconstruct_rowset_graph(_tablet_meta->all_rs_metas());

    return OLAP_SUCCESS;
}

OLAPStatus Tablet::add_rowset(RowsetSharedPtr rowset) {
    WriteLock wrlock(&_meta_lock);
    _tablet_meta->add_rs_meta(rowset->rowset_meta());
    _rs_version_map[rowset->version()] = rowset; 
    _rs_graph->add_version_to_graph(rowset->version());
    return OLAP_SUCCESS;
}

RowsetSharedPtr Tablet::rowset_with_largest_size() {
    ReadLock rdlock(&_meta_lock);
    RowsetSharedPtr largest_rowset = nullptr;
    for (auto& it : _rs_version_map) {
        // use segment_group of base file as target segment_group when base is not empty,
        // or try to find the biggest segment_group.
        if (largest_rowset->empty() || largest_rowset->zero_num_rows()) {
            continue;
        }
        if (it.second->rowset_meta()->index_disk_size()
                > largest_rowset->rowset_meta()->index_disk_size()) {
            largest_rowset = it.second;
        }
    }

    return largest_rowset;
}

OLAPStatus Tablet::split_range(
        const OlapTuple& start_key_strings,
        const OlapTuple& end_key_strings,
        uint64_t request_block_row_count,
        vector<OlapTuple>* ranges) {
    if (ranges == NULL) {
        LOG(WARNING) << "parameter end_row is null.";
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    RowCursor start_key;
    RowCursor end_key;

    // 如果有startkey，用startkey初始化；反之则用minkey初始化
    if (start_key_strings.size() > 0) {
        if (start_key.init_scan_key(_schema, start_key_strings.values()) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to initial key strings with RowCursor type.";
            return OLAP_ERR_INIT_FAILED;
        }

        if (start_key.from_tuple(start_key_strings) != OLAP_SUCCESS) {
            LOG(WARNING) << "init end key failed";
            return OLAP_ERR_INVALID_SCHEMA;
        }
    } else {
        if (start_key.init(_schema, num_short_key_columns()) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to initial key strings with RowCursor type.";
            return OLAP_ERR_INIT_FAILED;
        }

        start_key.allocate_memory_for_string_type(_schema);
        start_key.build_min_key();
    }

    // 和startkey一样处理，没有则用maxkey初始化
    if (end_key_strings.size() > 0) {
        if (OLAP_SUCCESS != end_key.init_scan_key(_schema, end_key_strings.values())) {
            LOG(WARNING) << "fail to parse strings to key with RowCursor type.";
            return OLAP_ERR_INVALID_SCHEMA;
        }

        if (end_key.from_tuple(end_key_strings) != OLAP_SUCCESS) {
            LOG(WARNING) << "init end key failed";
            return OLAP_ERR_INVALID_SCHEMA;
        }
    } else {
        if (end_key.init(_schema, num_short_key_columns()) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to initial key strings with RowCursor type.";
            return OLAP_ERR_INIT_FAILED;
        }

        end_key.allocate_memory_for_string_type(_schema);
        end_key.build_max_key();
    }

    ReadLock rdlock(get_header_lock_ptr());
    RowsetSharedPtr rowset = rowset_with_largest_size();

    // 如果找不到合适的rowset，就直接返回startkey，endkey
    if (rowset == nullptr) {
        VLOG(3) << "there is no base file now, may be tablet is empty.";
        // it may be right if the tablet is empty, so we return success.
        ranges->emplace_back(start_key.to_tuple());
        ranges->emplace_back(end_key.to_tuple());
        return OLAP_SUCCESS;
    }
    AlphaRowset* alpha_rowset = reinterpret_cast<AlphaRowset*>(rowset.get());
    OLAPStatus status = alpha_rowset->split_range(start_key, end_key, request_block_row_count, ranges);
    return status;
}

bool Tablet::has_version(const Version& version) const {
    return (_rs_version_map.find(version) != _rs_version_map.end());
}

void Tablet::list_versions(vector<Version>* versions) const {
    DCHECK(versions != nullptr && versions->empty());

    // versions vector is not sorted.
    for (auto& it : _rs_version_map) {
        versions->push_back(it.first);
    }
}

size_t Tablet::field_index(const string& field_name) const {
    return _schema.field_index(field_name);
}

size_t Tablet::row_size() const {
    return _schema.row_size();
}

size_t Tablet::get_data_size() {
    ReadLock rdlock(&_meta_lock);
    return _tablet_meta->data_size();
}

size_t Tablet::num_rows() {
    ReadLock rdlock(&_meta_lock);
    return _tablet_meta->num_rows();
}

bool Tablet::is_deletion_rowset(const Version& version) {
    if (version.first != version.second) {
        return false;
    }

    for (auto& it : _tablet_meta->delete_predicates()) {
        if (it.version() == version.first
              && it.version() == version.second) {
            return true;
        }
    }

    return false;
}

const AlterTabletTask& Tablet::alter_task() {
    return _tablet_meta->alter_task();
}

void Tablet::add_alter_task(int64_t tablet_id,
                            int64_t schema_hash,
                            const vector<Version>& versions_to_alter,
                            const AlterTabletType alter_type) {
    delete_alter_task();
    AlterTabletTask alter_task;
    alter_task.set_alter_state(ALTER_ALTERING);
    alter_task.set_related_tablet_id(tablet_id);
    alter_task.set_related_schema_hash(schema_hash);
    for (auto& version : versions_to_alter) {
        RowsetMetaSharedPtr rs_meta = _tablet_meta->acquire_inc_rs_meta(version);
    }

    alter_task.set_alter_type(alter_type);
    _tablet_meta->add_alter_task(alter_task);
}

OLAPStatus Tablet::delete_alter_task() {
    LOG(INFO) << "delete alter task from table. tablet=" << full_name();
    return _tablet_meta->delete_alter_task();
}

void Tablet::set_io_error() {
    OLAP_LOG_WARNING("io error occur.[tablet_full_name='%s', root_path_name='%s']",
                     full_name().c_str());
}

bool Tablet::is_used() {
    return _data_dir->is_used();
}

size_t Tablet::get_version_data_size(const Version& version) {
    RowsetSharedPtr rowset = _rs_version_map[version];
    return rowset->data_disk_size();
}

OLAPStatus Tablet::recover_tablet_until_specfic_version(const int64_t& spec_version,
                                                        const int64_t& version_hash) {
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::test_version(const Version& version) {
    vector<Version> span_versions;
    ReadLock rdlock(&_meta_lock);
    OLAPStatus res = _rs_graph->capture_consistent_versions(version, &span_versions);

    return res;
}

OLAPStatus Tablet::register_tablet_into_dir() {
    return _data_dir->register_tablet(this);
}

bool Tablet::version_for_delete_predicate(const Version& version) {
    return _tablet_meta->version_for_delete_predicate(version);
}

bool Tablet::version_for_load_deletion(const Version& version) {
    RowsetSharedPtr rowset = _rs_version_map.at(version);
    return rowset->delete_flag();
}

const RowsetSharedPtr Tablet::get_rowset_by_version(const Version& version) const {
    RowsetSharedPtr rowset = _rs_version_map.at(version);
    return rowset;
}

const RowsetSharedPtr Tablet::rowset_with_max_version() const {
    Version max_version = _tablet_meta->max_version();
    RowsetSharedPtr rowset = _rs_version_map.at(max_version);
    return rowset;
}

std::string Tablet::storage_root_path_name() const {
    return _data_dir->path();
}

std::string Tablet::tablet_path() const {
    return _tablet_path;
}

const uint32_t Tablet::calc_cumulative_compaction_score() const {
    uint32_t score = 0;
    bool base_rowset_exist = false;
    const int32_t point = cumulative_layer_point();
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() >= point) {
            score++;
        }
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
    }

    // base不存在可能是tablet正在做alter table，先不选它，设score=0
    return base_rowset_exist ? score : 0;
}

const uint32_t Tablet::calc_base_compaction_score() const {
    uint32_t score = 0;
    const int32_t point = cumulative_layer_point();
    bool base_rowset_exist = false;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() < point) {
            score++;
        }
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
    }
    score = score < config::base_compaction_num_cumulative_deltas ? 0 : score;

    // base不存在可能是tablet正在做alter table，先不选它，设score=0
    return base_rowset_exist ? score : 0;
}

bool Tablet::has_expired_incremental_rowset() {
    bool exist = false;
    time_t now = time(NULL);
    ReadLock rdlock(&_meta_lock);
    for (auto& rs_meta : _tablet_meta->all_inc_rs_metas()) {
        double diff = difftime(now, rs_meta->creation_time());
        if (diff >= config::inc_rowset_expired_sec) {
            exist = true;
            break;
        }
    }
    return exist;
}

void Tablet::delete_expired_incremental_rowset() {
    time_t now = time(NULL);
    std::vector<std::pair<Version, VersionHash>> expired_versions;
    std::vector<string> files_to_remove;
    WriteLock wrlock(&_meta_lock);
    for (auto& rs_meta : _tablet_meta->all_inc_rs_metas()) {
        double diff = difftime(now, rs_meta->creation_time());
        if (diff >= config::inc_rowset_expired_sec) {
            Version version(rs_meta->start_version(), rs_meta->end_version());
            expired_versions.push_back(std::make_pair(version, rs_meta->version_hash()));
            VLOG(3) << "find expire incremental rowset. tablet=" << full_name() << ", "
                    << "version=" << rs_meta->start_version() << "-" << rs_meta->end_version() << ", "
                    << "exist_sec=" << diff;
        }
    }

    if (expired_versions.empty()) { return; }

    for (auto& pair: expired_versions) {
        _delete_incremental_rowset(pair.first, pair.second, &files_to_remove);
        VLOG(3) << "delete expire incremental data. table=" << full_name() << ", "
                << "version=" << pair.first.first << "-" << pair.first.second;
    }

    if (save_tablet_meta() != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet_meta when delete expire incremental data."
                   << "tablet=" << full_name();
    }
    remove_files(files_to_remove);
}

OLAPStatus Tablet::revise_tablet_meta(const TabletMeta& tablet_meta,
                              const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                              const std::vector<Version>& versions_to_delete) {
    LOG(INFO) << "begin to clone data to tablet. tablet=" << full_name()
              << ", rowsets_to_clone=" << rowsets_to_clone.size()
              << ", versions_to_delete_size=" << versions_to_delete.size();
    OLAPStatus res = OLAP_SUCCESS;
    do {
        // load new local tablet_meta to operate on
        TabletMeta new_tablet_meta;
        TabletMetaManager::get_header(_data_dir, tablet_id(), schema_hash(), &new_tablet_meta);

        // delete versions from new local tablet_meta
        for (const Version& version : versions_to_delete) {
            res = new_tablet_meta.delete_rs_meta_by_version(version);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to delete version from new local tablet meta. tablet=" << full_name()
                             << ", version=" << version.first << "-" << version.second;
                break;
            }
            if (new_tablet_meta.version_for_delete_predicate(version)) {
                new_tablet_meta.remove_delete_predicate_by_version(version);
            }
            LOG(INFO) << "delete version from new local tablet_meta when clone. [table='" << full_name()
                      << "', version=" << version.first << "-" << version.second << "]";
        }

        if (res != OLAP_SUCCESS) {
            break;
        }

        for (auto& rs_meta : rowsets_to_clone) {
            Version version(rs_meta->start_version(), rs_meta->end_version());
            new_tablet_meta.add_rs_meta(rs_meta);

            // add delete conditions to new local tablet_meta, if it exists in tablet_meta
            if (version.first == version.second) {
                for (auto it = tablet_meta.delete_predicates().begin();
                     it != tablet_meta.delete_predicates().end(); ++it) {
                    if (it->version() == version.first) {
                        // add it
                        new_tablet_meta.add_delete_predicate(*it, version.first);
                        LOG(INFO) << "add delete condition when clone. [table=" << full_name()
                                  << " version=" << it->version() << "]";
                        break;
                    }
                }
            }
        }

        if (res != OLAP_SUCCESS) {
            break;
        }

       VLOG(3) << "load rowsets successfully when clone. tablet=" << full_name()
                << ", added rowset size=" << rowsets_to_clone.size();
        // save and reload tablet_meta
        res = TabletMetaManager::save(_data_dir, tablet_id(), schema_hash(), &new_tablet_meta);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to save new local tablet_meta when clone. res:" << res;
            break;
        }
        res = TabletMetaManager::get_header(_data_dir, tablet_id(), schema_hash(), _tablet_meta);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to reload original tablet_meta when clone. tablet=" << full_name()
                         << ", res=" << res;
            break;
        }

    } while (0);

    LOG(INFO) << "finish to clone data to tablet. res=" << res << ", "
              << "table=" << full_name() << ", "
              << "rowsets_to_clone=" << rowsets_to_clone.size();
    return res;
}

void Tablet::_delete_incremental_rowset(const Version& version,
                                         const VersionHash& version_hash,
                                         vector<string>* files_to_remove) {
    RowsetMetaSharedPtr rowset = _tablet_meta->acquire_inc_rs_meta(version);
    if (rowset == nullptr) { return; }

    _tablet_meta->delete_inc_rs_meta_by_version(version);
    VLOG(3) << "delete incremental rowset. tablet=" << full_name() << ", "
            << "version=" << version.first << "-" << version.second;
}

AlterTabletState Tablet::alter_state() {
    return _tablet_meta->alter_task().alter_state();
}

OLAPStatus Tablet::set_alter_state(AlterTabletState state) {
    _tablet_meta->mutable_alter_task()->set_alter_state(state);
    return OLAP_SUCCESS;
}

bool Tablet::is_schema_changing() {
    return alter_state() != ALTER_NONE;
}

}  // namespace doris
