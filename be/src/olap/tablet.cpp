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

#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"

namespace doris {

TabletSharedPtr Tablet::create_tablet_from_meta_file(
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
    string shard_path = file_path_path.parent_path().parent_path().parent_path().string();
    string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
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
    return create_tablet_from_meta(tablet_meta, data_dir);
}

TabletSharedPtr Tablet::create_tablet_from_meta(
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
  : _state(tablet_meta->tablet_state()),
    _tablet_meta(tablet_meta),
    _schema(tablet_meta->tablet_schema()),
    _data_dir(data_dir) {
    _is_dropped = false;
    _tablet_meta->set_data_dir(_data_dir);

    _tablet_path.append(_data_dir->path());
    _tablet_path.append(DATA_PREFIX);
    _tablet_path.append("/");
    _tablet_path.append(std::to_string(_tablet_meta->shard_id()));
    _tablet_path.append("/");
    _tablet_path.append(std::to_string(_tablet_meta->tablet_id()));
    _tablet_path.append("/");
    _tablet_path.append(std::to_string(_tablet_meta->schema_hash()));

    _rs_graph.construct_rowset_graph(_tablet_meta->all_rs_metas());
}

Tablet::~Tablet() {
    WriteLock wrlock(&_meta_lock);
    _rs_version_map.clear();
}

OLAPStatus Tablet::init_once() {
    OLAPStatus res = OLAP_SUCCESS;
    WriteLock wrlock(&_meta_lock);
    TabletMeta* tablet_meta = _tablet_meta;
    VLOG(3) << "begin to load indices. tablet=" << full_name()
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
            break;
        }
    }

    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to load indices. res=" << res
                    << ", tablet='" << full_name();
        TabletManager::instance()->drop_tablet(tablet_id(), schema_hash());
        return res;
    }

    return res;
}

OLAPStatus Tablet::init() {
    return _init_once.init([this] { return init_once(); });
}

bool Tablet::is_used() {
    return _data_dir->is_used();
}

string Tablet::tablet_path() const {
    return _tablet_path;
}

OLAPStatus Tablet::save_meta() {
    OLAPStatus res = _tablet_meta->save_meta();
    if (res != OLAP_SUCCESS) {
       LOG(WARNING) << "fail to save tablet_meta. res=" << res
                    << ", root=" << _data_dir->path();
    }

    return res;
}

OLAPStatus Tablet::revise_tablet_meta(const TabletMeta& tablet_meta,
                              const vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                              const vector<Version>& versions_to_delete) {
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

OLAPStatus Tablet::register_tablet_into_dir() {
    return _data_dir->register_tablet(this);
}

OLAPStatus Tablet::add_rowset(RowsetSharedPtr rowset) {
    WriteLock wrlock(&_meta_lock);
    RETURN_NOT_OK(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;
    RETURN_NOT_OK(_rs_graph.add_version_to_graph(rowset->version()));
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::modify_rowsets(const vector<RowsetSharedPtr>& to_add,
                                  const vector<RowsetSharedPtr>& to_delete) {
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

    for (auto& rs : to_add) {
        _rs_version_map[rs->version()] = rs;;
    }

    _rs_graph.reconstruct_rowset_graph(_tablet_meta->all_rs_metas());

    return OLAP_SUCCESS;
}

const RowsetSharedPtr Tablet::get_rowset_by_version(const Version& version) const {
    RowsetSharedPtr rowset = _rs_version_map.at(version);
    return rowset;
}

size_t Tablet::get_rowset_size_by_version(const Version& version) {
    RowsetSharedPtr rowset = _rs_version_map[version];
    return rowset->data_disk_size();
}

const RowsetSharedPtr Tablet::rowset_with_max_version() const {
    Version max_version = _tablet_meta->max_version();
    RowsetSharedPtr rowset = _rs_version_map.at(max_version);
    return rowset;
}

RowsetSharedPtr Tablet::rowset_with_largest_size() {
    RowsetSharedPtr largest_rowset = nullptr;
    for (auto& it : _rs_version_map) {
        // use segment_group of base file as target segment_group when base is not empty,
        // or try to find the biggest segment_group.
        if (it.second->empty() || it.second->zero_num_rows()) {
            continue;
        }
        if (largest_rowset == nullptr || it.second->rowset_meta()->index_disk_size()
                > largest_rowset->rowset_meta()->index_disk_size()) {
            largest_rowset = it.second;
        }
    }

    return largest_rowset;
}

OLAPStatus Tablet::add_inc_rowset(const RowsetSharedPtr& rowset) {
    WriteLock wrlock(&_meta_lock);
    RETURN_NOT_OK(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;
    RETURN_NOT_OK(_rs_graph.add_version_to_graph(rowset->version()));
    RETURN_NOT_OK(_tablet_meta->add_inc_rs_meta(rowset->rowset_meta()));
    RETURN_NOT_OK(_tablet_meta->save_meta());
    return OLAP_SUCCESS;
}

bool Tablet::has_expired_inc_rowset() {
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

void Tablet::delete_inc_rowset_by_version(const Version& version,
                                          const VersionHash& version_hash,
                                          vector<string>* files_to_remove) {
    RowsetMetaSharedPtr rowset = _tablet_meta->acquire_inc_rs_meta_by_version(version);
    if (rowset == nullptr) { return; }

    _tablet_meta->delete_inc_rs_meta_by_version(version);
    VLOG(3) << "delete incremental rowset. tablet=" << full_name() << ", "
            << "version=" << version.first << "-" << version.second;
}

void Tablet::delete_expired_inc_rowsets() {
    time_t now = time(NULL);
    vector<pair<Version, VersionHash>> expired_versions;
    vector<string> files_to_remove;
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
        delete_inc_rowset_by_version(pair.first, pair.second, &files_to_remove);
        VLOG(3) << "delete expire incremental data. table=" << full_name() << ", "
                << "version=" << pair.first.first << "-" << pair.first.second;
    }

    if (save_meta() != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet_meta when delete expire incremental data."
                   << "tablet=" << full_name();
    }
    remove_files(files_to_remove);
}

OLAPStatus Tablet::capture_consistent_versions(
                        const Version& version, vector<Version>* span_versions) const {
    OLAPStatus status = _rs_graph.capture_consistent_versions(version, span_versions);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to generate shortest version path. tablet=" << full_name()
                     << ", version='" << version.first << "-" << version.second;
    }
    return status;
}

OLAPStatus Tablet::check_version_integrity(const Version& version) {
    vector<Version> span_versions;
    ReadLock rdlock(&_meta_lock);
    return _rs_graph.capture_consistent_versions(version, &span_versions);
}

bool Tablet::check_version_exist(const Version& version) const {
    return (_rs_version_map.find(version) != _rs_version_map.end());
}

void Tablet::list_versions(vector<Version>* versions) const {
    DCHECK(versions != nullptr && versions->empty());

    // versions vector is not sorted.
    for (auto& it : _rs_version_map) {
        versions->push_back(it.first);
    }
}

OLAPStatus Tablet::capture_consistent_rowsets(const Version& spec_version,
                                              vector<RowsetSharedPtr>* rowsets) const {
    vector<Version> version_path;
    RETURN_NOT_OK(_rs_graph.capture_consistent_versions(spec_version, &version_path));
    RETURN_NOT_OK(capture_consistent_rowsets(version_path, rowsets));
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
            return OLAP_ERR_CAPTURE_ROWSET_ERROR;
        }

        rowsets->push_back(it->second);
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_rs_readers(const Version& spec_version,
                                      vector<RowsetReaderSharedPtr>* rs_readers) const {
    vector<Version> version_path;
    RETURN_NOT_OK(_rs_graph.capture_consistent_versions(spec_version, &version_path));
    RETURN_NOT_OK(capture_rs_readers(version_path, rs_readers));
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
            return OLAP_ERR_CAPTURE_ROWSET_READER_ERROR;
        }
        // if the rowset is empty, there is no need to read
        if (it->second->empty()) {
            continue;
        }
        std::shared_ptr<RowsetReader> rs_reader(it->second->create_reader());
        rs_readers->push_back(rs_reader);
    }
    return OLAP_SUCCESS;
}

bool Tablet::version_for_delete_predicate(const Version& version) {
    return _tablet_meta->version_for_delete_predicate(version);
}

bool Tablet::version_for_load_deletion(const Version& version) {
    RowsetSharedPtr rowset = _rs_version_map.at(version);
    return rowset->delete_flag();
}

AlterTabletState Tablet::alter_state() {
    return _tablet_meta->alter_task().alter_state();
}

bool Tablet::is_schema_changing() {
    return alter_state() != ALTER_NONE;
}

OLAPStatus Tablet::set_alter_state(AlterTabletState state) {
    _tablet_meta->mutable_alter_task()->set_alter_state(state);
    return OLAP_SUCCESS;
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
        RowsetMetaSharedPtr rs_meta = _tablet_meta->acquire_rs_meta_by_version(version);
        alter_task.add_rowset_to_alter(rs_meta);
    }

    alter_task.set_alter_type(alter_type);
    _tablet_meta->add_alter_task(alter_task);
}

OLAPStatus Tablet::delete_alter_task() {
    LOG(INFO) << "delete alter task from table. tablet=" << full_name();
    return _tablet_meta->delete_alter_task();
}

OLAPStatus Tablet::protected_delete_alter_task() {
    WriteLock wrlock(&_meta_lock);
    OLAPStatus res = delete_alter_task();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to delete alter task from table. res=" << res
                        << ", full_name=" << full_name();
        return res;
    }

    res = save_meta();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to save tablet header. res=" << res
                     << ", full_name=" << full_name();
        return res;
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
    if (OLAP_SUCCESS != _rs_graph.capture_consistent_versions(test_version, &path_versions)) {
        LOG(WARNING) << "tablet has missed version. tablet=" << full_name();
        return false;
    }

    if (this->is_schema_changing()) {
        Version test_version = Version(0, lastest_delta->end_version());
        vector<Version> path_versions;
        if (OLAP_SUCCESS != _rs_graph.capture_consistent_versions(test_version, &path_versions)) {
            return false;
        }
    }

    return true;
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
    vector<pair<Version, VersionHash>> existing_versions;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version() , rs->version_hash());
    }

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const pair<Version, VersionHash>& left,
                 const pair<Version, VersionHash>& right) {
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

OLAPStatus Tablet::recover_tablet_until_specfic_version(const int64_t& spec_version,
                                                        const int64_t& version_hash) {
    return OLAP_SUCCESS;
}

void Tablet::set_io_error() {
    OLAP_LOG_WARNING("io error occur.[tablet_full_name='%s', root_path_name='%s']",
                     full_name().c_str());
}

void Tablet::delete_all_files() {
    // Release resources like memory and disk space.
    // we have to call list_versions first, or else error occurs when
    // removing hash_map item and iterating hash_map concurrently.
    ReadLock rdlock(&_meta_lock);
    for (auto it = _rs_version_map.begin(); it != _rs_version_map.end(); ++it) {
        it->second->remove();
        _rs_version_map.erase(it);
    }
}

}  // namespace doris
