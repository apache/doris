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
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "util/time.h"

namespace doris {

using std::pair;
using std::nothrow;
using std::sort;
using std::string;
using std::vector;

TabletSharedPtr Tablet::create_tablet_from_meta(
        TabletMetaSharedPtr tablet_meta,
        DataDir* data_dir) {
    TabletSharedPtr tablet = std::make_shared<Tablet>(tablet_meta, data_dir);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to malloc a table.";
        return nullptr;
    }

    return tablet;
}

Tablet::Tablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir)
  : _state(tablet_meta->tablet_state()),
    _tablet_meta(tablet_meta),
    _schema(tablet_meta->tablet_schema()),
    _data_dir(data_dir),
    _is_bad(false),
    _last_cumu_compaction_failure_time(0),
    _last_base_compaction_failure_time(0),
    _last_cumu_compaction_success_time(0),
    _last_base_compaction_success_time(0) {

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
    _inc_rs_version_map.clear();
}

OLAPStatus Tablet::_init_once_action() {
    OLAPStatus res = OLAP_SUCCESS;
    VLOG(3) << "begin to load tablet. tablet=" << full_name()
            << ", version_size=" << _tablet_meta->version_count();
    for (auto& rs_meta :  _tablet_meta->all_rs_metas()) {
        Version version = { rs_meta->start_version(), rs_meta->end_version() };
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(&_schema, _tablet_path, rs_meta, &rowset);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to init rowset. tablet_id:" << tablet_id()
                         << ", schema_hash:" << schema_hash()
                         << ", version=" << version.first << "-" << version.second
                         << ", res:" << res;
            return res;
        }
        _rs_version_map[version] = std::move(rowset);
    }

    // init incremental rowset
    for (auto& inc_rs_meta : _tablet_meta->all_inc_rs_metas()) {
        Version version = { inc_rs_meta->start_version(), inc_rs_meta->end_version() };
        RowsetSharedPtr rowset = get_rowset_by_version(version);
        if (rowset == nullptr) {
            res = RowsetFactory::create_rowset(&_schema, _tablet_path, inc_rs_meta, &rowset);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to init incremental rowset. tablet_id:" << tablet_id()
                             << ", schema_hash:" << schema_hash()
                             << ", version=" << version.first << "-" << version.second
                             << ", res:" << res;
                return res;
            }
        }
        _inc_rs_version_map[version] = std::move(rowset);
    }

    _cumulative_point = -1;

    return res;
}

OLAPStatus Tablet::init() {
    return _init_once.call([this] { return _init_once_action(); });
}

bool Tablet::is_used() {
    return !_is_bad && _data_dir->is_used();
}

TabletUid Tablet::tablet_uid() {
    return _tablet_meta->tablet_uid();
}

string Tablet::tablet_path() const {
    return _tablet_path;
}

OLAPStatus Tablet::set_tablet_state(TabletState state) {
    if (_tablet_meta->tablet_state() == TABLET_SHUTDOWN && state != TABLET_SHUTDOWN) {
        LOG(WARNING) << "could not change tablet state from shutdown to " << state;
        return OLAP_ERR_META_INVALID_ARGUMENT;
    }
    RETURN_NOT_OK(_tablet_meta->set_tablet_state(state));
    _state = state;
    return OLAP_SUCCESS;
}

// should save tablet meta to remote meta store
// if it's a primary replica
OLAPStatus Tablet::save_meta() {
    OLAPStatus res = _tablet_meta->save_meta(_data_dir);
    if (res != OLAP_SUCCESS) {
       LOG(FATAL) << "fail to save tablet_meta. res=" << res
                    << ", root=" << _data_dir->path();
    }
    _schema = _tablet_meta->tablet_schema();

    return res;
}

OLAPStatus Tablet::revise_tablet_meta(
        const vector<RowsetMetaSharedPtr>& rowsets_to_clone,
        const vector<Version>& versions_to_delete) {
    LOG(INFO) << "begin to clone data to tablet. tablet=" << full_name()
              << ", rowsets_to_clone=" << rowsets_to_clone.size()
              << ", versions_to_delete_size=" << versions_to_delete.size();
    OLAPStatus res = OLAP_SUCCESS;
    do {
        // load new local tablet_meta to operate on
        TabletMetaSharedPtr new_tablet_meta(new (nothrow) TabletMeta());
        RETURN_NOT_OK(generate_tablet_meta_copy(new_tablet_meta));

        // delete versions from new local tablet_meta
        for (const Version& version : versions_to_delete) {
            res = new_tablet_meta->delete_rs_meta_by_version(version, nullptr);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to delete version from new local tablet meta. tablet=" << full_name()
                             << ", version=" << version.first << "-" << version.second;
                break;
            }
            if (new_tablet_meta->version_for_delete_predicate(version)) {
                new_tablet_meta->remove_delete_predicate_by_version(version);
            }
            LOG(INFO) << "delete version from new local tablet_meta when clone. [table='" << full_name()
                      << "', version=" << version.first << "-" << version.second << "]";
        }

        if (res != OLAP_SUCCESS) {
            break;
        }

        for (auto& rs_meta : rowsets_to_clone) {
            new_tablet_meta->add_rs_meta(rs_meta);
        }

        if (res != OLAP_SUCCESS) {
            break;
        }

       VLOG(3) << "load rowsets successfully when clone. tablet=" << full_name()
                << ", added rowset size=" << rowsets_to_clone.size();
        // save and reload tablet_meta
        res = new_tablet_meta->save_meta(_data_dir);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to save new local tablet_meta when clone. res:" << res;
            break;
        }
        _tablet_meta = new_tablet_meta;
    } while (0);

    for (auto& version : versions_to_delete) {
        auto it = _rs_version_map.find(version);
        StorageEngine::instance()->add_unused_rowset(it->second);
        _rs_version_map.erase(it);
    }
    for (auto& it : _inc_rs_version_map) {
        StorageEngine::instance()->add_unused_rowset(it.second);
    }
    _inc_rs_version_map.clear();

    for (auto& rs_meta : rowsets_to_clone) {
        Version version = { rs_meta->start_version(), rs_meta->end_version() };
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(&_schema, _tablet_path, rs_meta, &rowset);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to init rowset. version=" << version.first << "-" << version.second;
            return res;
        }
        _rs_version_map[version] = std::move(rowset);
    }

    _rs_graph.reconstruct_rowset_graph(_tablet_meta->all_rs_metas());

    LOG(INFO) << "finish to clone data to tablet. res=" << res << ", "
              << "table=" << full_name() << ", "
              << "rowsets_to_clone=" << rowsets_to_clone.size();
    return res;
}

OLAPStatus Tablet::register_tablet_into_dir() {
    return _data_dir->register_tablet(this);
}

OLAPStatus Tablet::deregister_tablet_from_dir() {
    return _data_dir->deregister_tablet(this);
}

OLAPStatus Tablet::add_rowset(RowsetSharedPtr rowset, bool need_persist) {
    WriteLock wrlock(&_meta_lock);
    // if the rowset already exist, should not return version already exist
    // should return OLAP_SUCCESS
    if (contains_rowset(rowset->rowset_id())) {
        return OLAP_SUCCESS;
    }
    RETURN_NOT_OK(_check_added_rowset(rowset));
    RETURN_NOT_OK(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;
    RETURN_NOT_OK(_rs_graph.add_version_to_graph(rowset->version()));

    vector<RowsetSharedPtr> rowsets_to_delete;
    // yiguolei: temp code, should remove the rowset contains by this rowset
    // but it should be removed in multi path version
    for (auto& it : _rs_version_map) {
        if ((it.first.first >= rowset->start_version() && it.first.second < rowset->end_version())
            || (it.first.first > rowset->start_version() && it.first.second <= rowset->end_version())) {
            if (it.second == nullptr) {
                LOG(FATAL) << "there exist a version "
                           << " start_version=" << it.first.first
                           << " end_version=" << it.first.second
                           << " contains the input rs with version "
                           << " start_version=" << rowset->start_version()
                           << " end_version=" << rowset->end_version()
                           << " but the related rs is null";
                return OLAP_ERR_PUSH_ROWSET_NOT_FOUND;
            } else {
                rowsets_to_delete.push_back(it.second);
            }
        }
    }
    modify_rowsets(std::vector<RowsetSharedPtr>(), rowsets_to_delete);

    if (need_persist) {
        RowsetMetaPB rowset_meta_pb;
        rowset->rowset_meta()->to_rowset_pb(&rowset_meta_pb);
        OLAPStatus res = RowsetMetaManager::save(data_dir()->get_meta(), tablet_uid(), 
            rowset->rowset_id(), rowset_meta_pb);
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "failed to save rowset to local meta store" << rowset->rowset_id();
        }
    }
    ++_newly_created_rowset_num;
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
        _rs_version_map[rs->version()] = rs;
        ++_newly_created_rowset_num;
    }

    _rs_graph.reconstruct_rowset_graph(_tablet_meta->all_rs_metas());

    return OLAP_SUCCESS;
}

// snapshot manager may call this api to check if version exists, so that 
// the version maybe not exist
const RowsetSharedPtr Tablet::get_rowset_by_version(const Version& version) const {
    auto iter = _rs_version_map.find(version);
    if (iter == _rs_version_map.end()) {
        LOG(INFO) << "no rowset for version:" << version.first << "-" << version.second
                << ", tablet: " << full_name();
        return nullptr;
    }
    RowsetSharedPtr rowset = iter->second;
    return rowset;
}

// This function only be called by SnapshotManager to perform incremental clone.
// Interal data structure will be protected in SnapSshotManager.
// There is no necessity to add lock in this place.
const RowsetSharedPtr Tablet::get_inc_rowset_by_version(const Version& version) const {
    auto iter = _inc_rs_version_map.find(version);
    if (iter == _inc_rs_version_map.end()) {
        LOG(INFO) << "no rowset for version:" << version.first << "-" << version.second
                  << ", tablet: " << full_name();
        return nullptr;
    }
    RowsetSharedPtr rowset = iter->second;
    return rowset;
}

size_t Tablet::get_rowset_size_by_version(const Version& version) {
    DCHECK(_rs_version_map.find(version) != _rs_version_map.end())
            << "invalid version:" << version.first << "-" << version.second;
    auto iter = _rs_version_map.find(version);
    if (iter == _rs_version_map.end()) {
        LOG(WARNING) << "no rowset for version:" << version.first << "-" << version.second
                << ", tablet: " << full_name();
        return -1;
    }
    RowsetSharedPtr rowset = iter->second;
    return rowset->data_disk_size();
}

const RowsetSharedPtr Tablet::rowset_with_max_version() const {
    Version max_version = _tablet_meta->max_version();
    if (max_version.first == -1) {
        return nullptr;
    }
    DCHECK(_rs_version_map.find(max_version) != _rs_version_map.end())
            << "invalid version:" << max_version.first << "-" << max_version.second;
    auto iter = _rs_version_map.find(max_version);
    if (iter == _rs_version_map.end()) {
        LOG(WARNING) << "no rowset for version:" << max_version.first << "-" << max_version.second;
        return nullptr;
    }
    RowsetSharedPtr rowset = iter->second;
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

// add inc rowset should not persist tablet meta, because the 
OLAPStatus Tablet::add_inc_rowset(const RowsetSharedPtr& rowset) {
    WriteLock wrlock(&_meta_lock);
    if (contains_rowset(rowset->rowset_id())) {
        return OLAP_SUCCESS;
    }
    // check if the rowset id is valid
    RETURN_NOT_OK(_check_added_rowset(rowset));
    RETURN_NOT_OK(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;
    _inc_rs_version_map[rowset->version()] = rowset;
    RETURN_NOT_OK(_rs_graph.add_version_to_graph(rowset->version()));
    RETURN_NOT_OK(_tablet_meta->add_inc_rs_meta(rowset->rowset_meta()));
    ++_newly_created_rowset_num;
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
                                          const VersionHash& version_hash) {
    // delete incremental rowset from map
    auto it = _inc_rs_version_map.find(version);
    if (it != _inc_rs_version_map.end()) {
        _inc_rs_version_map.erase(it);
    }
    RowsetMetaSharedPtr rowset_meta = _tablet_meta->acquire_inc_rs_meta_by_version(version);
    if (rowset_meta == nullptr) { return; }

    _tablet_meta->delete_inc_rs_meta_by_version(version);
    VLOG(3) << "delete incremental rowset. tablet=" << full_name() << ", "
            << "version=" << version.first << "-" << version.second;
}

void Tablet::delete_expired_inc_rowsets() {
    time_t now = time(nullptr);
    vector<pair<Version, VersionHash>> expired_versions;
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
        delete_inc_rowset_by_version(pair.first, pair.second);
        VLOG(3) << "delete expire incremental data. tablet=" << full_name() << ", "
                << "version=" << pair.first.first << "-" << pair.first.second;
    }

    if (save_meta() != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet_meta when delete expire incremental data."
                   << "tablet=" << full_name();
    }
}

OLAPStatus Tablet::capture_consistent_versions(
                        const Version& spec_version, vector<Version>* version_path) const {
    OLAPStatus status = _rs_graph.capture_consistent_versions(spec_version, version_path);
    if (status != OLAP_SUCCESS) {
        std::vector<Version> missed_versions;
        calc_missed_versions_unlock(spec_version.second, &missed_versions);
        if (missed_versions.empty()) {
            LOG(WARNING) << "tablet:" << full_name()
                         << ", version already has been merged. "
                         << "spec_version: " << spec_version.first
                         << "-" << spec_version.second;
            status = OLAP_ERR_VERSION_ALREADY_MERGED;
        } else {
            LOG(WARNING) << "status:" << status << ", tablet:" << full_name()
                         << ", missed version for version:"
                         << spec_version.first << "-" << spec_version.second;
            _print_missed_versions(missed_versions);
        }
        return status;
    }
    return status;
}

OLAPStatus Tablet::check_version_integrity(const Version& version) {
    vector<Version> span_versions;
    ReadLock rdlock(&_meta_lock);
    return capture_consistent_versions(version, &span_versions);
}

// if any rowset contains the version's data, it means the version
// already exist
bool Tablet::check_version_exist(const Version& version) const {
    for (auto& it : _rs_version_map) {
        if (it.first.first <= version.first && it.first.second >= version.second) {
            return true;
        }
    }
    return false;
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
    RETURN_NOT_OK(capture_consistent_versions(spec_version, &version_path));
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
    RETURN_NOT_OK(capture_consistent_versions(spec_version, &version_path));
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
        RowsetReaderSharedPtr rs_reader;
        auto res = it->second->create_reader(&rs_reader);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to create reader for rowset:" << it->second->rowset_id();
            return OLAP_ERR_CAPTURE_ROWSET_READER_ERROR;
        }
        rs_readers->push_back(std::move(rs_reader));
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version) {
    return _tablet_meta->add_delete_predicate(delete_predicate, version);
}

bool Tablet::version_for_delete_predicate(const Version& version) {
    return _tablet_meta->version_for_delete_predicate(version);
}

bool Tablet::version_for_load_deletion(const Version& version) {
    RowsetSharedPtr rowset = _rs_version_map.at(version);
    return rowset->delete_flag();
}


AlterTabletTaskSharedPtr Tablet::alter_task() {
    return _tablet_meta->alter_task();
}

OLAPStatus Tablet::add_alter_task(int64_t related_tablet_id,
                            int32_t related_schema_hash,
                            const vector<Version>& versions_to_alter,
                            const AlterTabletType alter_type) {
    AlterTabletTask alter_task;
    alter_task.set_alter_state(ALTER_RUNNING);
    alter_task.set_related_tablet_id(related_tablet_id);
    alter_task.set_related_schema_hash(related_schema_hash);
    alter_task.set_alter_type(alter_type);
    RETURN_NOT_OK(_tablet_meta->add_alter_task(alter_task));
    LOG(INFO) << "successfully add alter task for tablet_id:" << this->tablet_id()
              << ", schema_hash:" << this->schema_hash()
              << ", related_tablet_id " << related_tablet_id
              << ", related_schema_hash " << related_schema_hash
              << ", alter_type " << alter_type;
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::delete_alter_task() {
    LOG(INFO) << "delete alter task from table. tablet=" << full_name();
    return _tablet_meta->delete_alter_task();
}

OLAPStatus Tablet::set_alter_state(AlterTabletState state) {
    return _tablet_meta->set_alter_state(state);
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

void Tablet::set_io_error() {
    OLAP_LOG_WARNING("io error occur.[tablet_full_name='%s', root_path_name='%s']",
                     full_name().c_str());
}

OLAPStatus Tablet::recover_tablet_until_specfic_version(const int64_t& spec_version,
                                                        const int64_t& version_hash) {
    return OLAP_SUCCESS;
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
    if (OLAP_SUCCESS != capture_consistent_versions(test_version, &path_versions)) {
        return false;
    }

    return true;
}

const uint32_t Tablet::calc_cumulative_compaction_score() const {
    uint32_t score = 0;
    bool base_rowset_exist = false;
    const int64_t point = cumulative_layer_point();
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() >= point) {
            score++;
        }
        if (rs_meta->start_version() == rs_meta->end_version()) {
            score += rs_meta->num_segments();
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
    const int64_t point = cumulative_layer_point();
    bool base_rowset_exist = false;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() < point) {
            score++;
        }
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
    }

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

void Tablet::compute_version_hash_from_rowsets(
        const std::vector<RowsetSharedPtr>& rowsets, VersionHash* version_hash) const {
    DCHECK(version_hash != nullptr) << "invalid parameter, version_hash is nullptr";
    int64_t v_hash  = 0;
    for (auto& rowset : rowsets) {
        v_hash ^= rowset->version_hash();
    }
    *version_hash = v_hash;
}


void Tablet::calc_missed_versions(int64_t spec_version,
                                  vector<Version>* missed_versions) {
    ReadLock rdlock(&_meta_lock);
    calc_missed_versions_unlock(spec_version, missed_versions);
}

void Tablet::calc_missed_versions_unlock(int64_t spec_version,
                                  vector<Version>* missed_versions) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;
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
    return _max_continuous_version_from_begining(version, v_hash);
}

OLAPStatus Tablet::_max_continuous_version_from_begining(Version* version, VersionHash* v_hash) {
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
        if (existing_versions[i].first.first > max_continuous_version.second + 1) {
            break;
        }
        max_continuous_version = existing_versions[i].first;
        max_continuous_version_hash = existing_versions[i].second;
    }
    *version = max_continuous_version;
    *v_hash = max_continuous_version_hash;
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::calculate_cumulative_point() {
    WriteLock wrlock(&_meta_lock);
    if (_cumulative_point != -1)  {
        return OLAP_SUCCESS;
    }
    
    std::list<Version> existing_versions;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    existing_versions.sort([](const Version& a, const Version& b) {
        // simple because 2 versions are certainly not overlapping
        return a.first < b.first;
    });

    int64_t prev_version = -1;
    for (const Version& version : existing_versions) {
        if (version.first > prev_version + 1) {
            break;
        }
        if (version.first == version.second) {
            _cumulative_point = version.first;
            break;
        }
        prev_version = version.second;
        _cumulative_point = version.second + 1;
    }
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
    return rowset->split_range(start_key, end_key, request_block_row_count, ranges);
}

void Tablet::delete_all_files() {
    // Release resources like memory and disk space.
    // we have to call list_versions first, or else error occurs when
    // removing hash_map item and iterating hash_map concurrently.
    ReadLock rdlock(&_meta_lock);
    for (auto it = _rs_version_map.begin(); it != _rs_version_map.end(); ++it) {
        it->second->remove();
    }
    _rs_version_map.clear();
    for (auto it = _inc_rs_version_map.begin(); it != _inc_rs_version_map.end(); ++it) {
        it->second->remove();
    }
    _inc_rs_version_map.clear();
}

bool Tablet::check_path(const std::string& path_to_check) {
    ReadLock rdlock(&_meta_lock);
    if (path_to_check == _tablet_path) {
        return true;
    }
    boost::filesystem::path tablet_schema_hash_path(_tablet_path);
    boost::filesystem::path tablet_id_path = tablet_schema_hash_path.parent_path();
    std::string tablet_id_dir = tablet_id_path.string();
    if (path_to_check == tablet_id_dir) {
        return true;
    }
    for (auto& version_rowset : _rs_version_map) {
        bool ret = version_rowset.second->check_path(path_to_check);
        if (ret) {
            return true;
        }
    }
    for (auto& inc_version_rowset : _inc_rs_version_map) {
        bool ret = inc_version_rowset.second->check_path(path_to_check);
        if (ret) {
            return true;
        }
    }
    return false;
}

// check rowset id in tablet meta and in rowset meta atomicly
// for example, during publish version stage, it will first add rowset meta to tablet meta and then
// remove it from rowset meta manager. If we check tablet meta first and then check rowset meta using 2 step unlocked
// the sequence maybe: 1. check in tablet meta [return false]  2. add to tablet meta  3. remove from rowset meta manager
// 4. check in rowset meta manager return false. so that the rowset maybe checked return false it means it is useless and 
// will be treated as a garbage.
bool Tablet::check_rowset_id(const RowsetId& rowset_id) {
    ReadLock rdlock(&_meta_lock);
    if (StorageEngine::instance()->rowset_id_in_use(rowset_id)) {
        return true;
    }
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    for (auto& inc_version_rowset : _inc_rs_version_map) {
        if (inc_version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(), rowset_id)) {
        return true;
    }
    return false;
}

void Tablet::_print_missed_versions(const std::vector<Version>& missed_versions) const {
    std::stringstream ss;
    ss << full_name() << " has "<< missed_versions.size() << " missed version:";
    // print at most 10 version
    for (int i = 0; i < 10 && i < missed_versions.size(); ++i) {
        ss << missed_versions[i].first << "-" << missed_versions[i].second << ",";
    }
    LOG(WARNING) << ss.str();
}

OLAPStatus Tablet::_check_added_rowset(const RowsetSharedPtr& rowset) {
    if (rowset == nullptr) {
        return OLAP_ERR_ROWSET_INVALID;
    }

    // check if there exist a rowset contains the added rowset
    for (auto& it : _rs_version_map) {
        if (it.first.first <= rowset->start_version() 
            && it.first.second >= rowset->end_version()) {
            if (it.second == nullptr) {
                LOG(FATAL) << "there exist a version "
                           << " start_version=" << it.first.first
                           << " end_version=" << it.first.second
                           << " contains the input rs with version "
                           << " start_version=" << rowset->start_version()
                           << " end_version=" << rowset->end_version()
                           << " but the related rs is null";
                return OLAP_ERR_PUSH_ROWSET_NOT_FOUND;
            } else {
                return OLAP_ERR_PUSH_VERSION_ALREADY_EXIST;
            }
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus Tablet::set_partition_id(int64_t partition_id) {
    return _tablet_meta->set_partition_id(partition_id);
}

TabletInfo Tablet::get_tablet_info() {
    return TabletInfo(tablet_id(), schema_hash(), tablet_uid());
}

void Tablet::pick_candicate_rowsets_to_cumulative_compaction(std::vector<RowsetSharedPtr>* candidate_rowsets) {
    ReadLock rdlock(&_meta_lock);
    for (auto& it : _rs_version_map) {
        if (it.first.first >= _cumulative_point) {
            candidate_rowsets->push_back(it.second);
        }
    }
}

void Tablet::pick_candicate_rowsets_to_base_compaction(std::vector<RowsetSharedPtr>* candidate_rowsets) {
    ReadLock rdlock(&_meta_lock);
    for (auto& it : _rs_version_map) {
        if (it.first.first < _cumulative_point) {
            candidate_rowsets->push_back(it.second);
        }
    }
}

OLAPStatus Tablet::get_compaction_status(std::string* json_result) {
    rapidjson::Document root;
    root.SetObject();

    auto version_comparator = [] (const Version& lhs, const Version& rhs)
        {
            if (lhs.first < rhs.first) return true;
            if (lhs.first == rhs.first) return lhs.second < rhs.second;
            return false;
        };

    auto rowset_version_map = std::map<Version, bool, std::function<bool(const Version&, const Version&)>>{
        version_comparator
    };
    auto rowset_segment_map = std::map<Version, int64_t, std::function<bool(const Version&, const Version&)>>{
        version_comparator
    };

    {
        ReadLock rdlock(&_meta_lock);
        for (auto& it : _rs_version_map) {
            rowset_version_map[it.first] = version_for_delete_predicate(it.first);
            rowset_segment_map[it.first] = it.second->num_segments();
        }
    }

    root.AddMember("cumulative point", _cumulative_point.load(), root.GetAllocator());
    rapidjson::Value cumu_value;
    std::string format_str = ToStringFromUnixMillis(_last_cumu_compaction_failure_time.load());
    cumu_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative failure time", cumu_value, root.GetAllocator());
    rapidjson::Value base_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_failure_time.load());
    base_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base failure time", base_value, root.GetAllocator());
    rapidjson::Value cumu_success_value;
    format_str = ToStringFromUnixMillis(_last_cumu_compaction_success_time.load());
    cumu_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative success time", cumu_success_value, root.GetAllocator());
    rapidjson::Value base_success_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_success_time.load());
    base_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base success time", base_success_value, root.GetAllocator());

    // print all rowsets' version as an array
    rapidjson::Document versions_arr;
    versions_arr.SetArray();
    for (auto& it : rowset_version_map) {
        rapidjson::Value value;
        std::string version_str = strings::Substitute("[$0-$1] $2 $3",
            it.first.first, it.first.second, rowset_segment_map[it.first], (it.second ? "DELETE" : ""));
        value.SetString(version_str.c_str(), version_str.length(), versions_arr.GetAllocator()); 
        versions_arr.PushBack(value, versions_arr.GetAllocator());
    }
    root.AddMember("versions", versions_arr, root.GetAllocator());

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());

    return OLAP_SUCCESS;
}

OLAPStatus Tablet::do_tablet_meta_checkpoint() {
    WriteLock store_lock(&_meta_store_lock);
    if (_newly_created_rowset_num == 0) {
        return OLAP_SUCCESS;
    }
    if (UnixMillis() - _last_checkpoint_time < config::tablet_meta_checkpoint_min_interval_secs * 1000
        && _newly_created_rowset_num < config::tablet_meta_checkpoint_min_new_rowsets_num) {
        return OLAP_SUCCESS;
    }
    // hold read lock not write lock, because it will not modify meta structure
    ReadLock rdlock(&_meta_lock);
    if (tablet_state() != TABLET_RUNNING) {
        LOG(INFO) << "tablet is under state=" << tablet_state()
                  << ", not running, skip do checkpoint"
                  << ", tablet=" << full_name();
        return OLAP_SUCCESS;
    }
    LOG(INFO) << "start to do tablet meta checkpoint, tablet=" << full_name();
    RETURN_NOT_OK(save_meta());
    // if save meta successfully, then should remove the rowset meta existing in tablet
    // meta from rowset meta store
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if(rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id())) {
            RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id());
            LOG(INFO) << "remove rowset id from meta store because it is already persistent with tablet meta"
                       << ", rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }
    _newly_created_rowset_num = 0;
    _last_checkpoint_time = UnixMillis();
    return OLAP_SUCCESS;
}

bool Tablet::rowset_meta_is_useful(RowsetMetaSharedPtr rowset_meta) {
    ReadLock rdlock(&_meta_lock);
    bool find_rowset_id = false;
    bool find_version = false;
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_meta->rowset_id()) {
            find_rowset_id = true;
        }
        if (version_rowset.second->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    for (auto& inc_version_rowset : _inc_rs_version_map) {
        if (inc_version_rowset.second->rowset_id() == rowset_meta->rowset_id()) {
            find_rowset_id = true;
        }
        if (inc_version_rowset.second->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    if (find_rowset_id || !find_version) {
        return true;
    } else {
        return false;
    }
}

bool Tablet::contains_rowset(const RowsetId rowset_id) {
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    for (auto& inc_version_rowset : _inc_rs_version_map) {
        if (inc_version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    return false;
}

void Tablet::build_tablet_report_info(TTabletInfo* tablet_info) {
    ReadLock rdlock(&_meta_lock);
    tablet_info->tablet_id = _tablet_meta->tablet_id();
    tablet_info->schema_hash = _tablet_meta->schema_hash();
    tablet_info->row_count = _tablet_meta->num_rows();
    tablet_info->data_size = _tablet_meta->tablet_footprint();
    Version version = { -1, 0 };
    VersionHash v_hash = 0;
    _max_continuous_version_from_begining(&version, &v_hash);
    auto max_rowset = rowset_with_max_version();
    if (max_rowset != nullptr) {
        if (max_rowset->version() != version) {
            tablet_info->__set_version_miss(true);
        }
    } else {
        // if the tablet is in running state, it is not under schema change
        // and could not get rowset, it is bad should clone it
        if (tablet_state() == TABLET_RUNNING) {
            tablet_info->__set_used(false);
        }
    }
    tablet_info->version = version.second;
    tablet_info->version_hash = v_hash;
    tablet_info->__set_partition_id(_tablet_meta->partition_id());
    tablet_info->__set_storage_medium(_data_dir->storage_medium());
    tablet_info->__set_version_count(_tablet_meta->version_count());
    tablet_info->__set_path_hash(_data_dir->path_hash());
    return;
}

// should use this method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
OLAPStatus Tablet::generate_tablet_meta_copy(TabletMetaSharedPtr new_tablet_meta) {
    TabletMetaPB tablet_meta_pb;
    RETURN_NOT_OK(_tablet_meta->to_meta_pb(&tablet_meta_pb));
    RETURN_NOT_OK(new_tablet_meta->init_from_pb(tablet_meta_pb));
    return OLAP_SUCCESS;
}

}  // namespace doris
