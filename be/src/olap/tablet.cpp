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
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <stdio.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <set>

#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta_manager.h"
#include "util/path_util.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"
#include "util/trace.h"

namespace doris {

using std::pair;
using std::nothrow;
using std::sort;
using std::string;
using std::vector;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_count, MetricUnit::OPERATIONS);

TabletSharedPtr Tablet::create_tablet_from_meta(TabletMetaSharedPtr tablet_meta,
                                                const StorageParamPB& storage_param,
                                                DataDir* data_dir) {
    return std::make_shared<Tablet>(tablet_meta, storage_param, data_dir);
}

Tablet::Tablet(TabletMetaSharedPtr tablet_meta, const StorageParamPB& storage_param,
               DataDir* data_dir, const std::string& cumulative_compaction_type)
        : BaseTablet(tablet_meta, storage_param, data_dir),
          _is_bad(false),
          _last_cumu_compaction_failure_millis(0),
          _last_base_compaction_failure_millis(0),
          _last_cumu_compaction_success_millis(0),
          _last_base_compaction_success_millis(0),
          _cumulative_point(K_INVALID_CUMULATIVE_POINT),
          _newly_created_rowset_num(0),
          _last_checkpoint_time(0),
          _cumulative_compaction_type(cumulative_compaction_type),
          _last_record_scan_count(0),
          _last_record_scan_count_timestamp(time(nullptr)),
          _is_clone_occurred(false) {
    // construct _timestamped_versioned_tracker from rs and stale rs meta
    _timestamped_version_tracker.construct_versioned_tracker(_tablet_meta->all_rs_metas(),
                                                             _tablet_meta->all_stale_rs_metas());

    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_count);
}

Status Tablet::_init_once_action() {
    Status res = Status::OK();
    VLOG_NOTICE << "begin to load tablet. tablet=" << full_name()
                << ", version_size=" << _tablet_meta->version_count();

#ifdef BE_TEST
    // init cumulative compaction policy by type
    _cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    _cumulative_compaction_type);
#endif

    for (const auto& rs_meta : _tablet_meta->all_rs_metas()) {
        Version version = rs_meta->version();
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(&_schema, _tablet_path_desc, rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init rowset. tablet_id=" << tablet_id()
                         << ", schema_hash=" << schema_hash() << ", version=" << version
                         << ", res=" << res;
            return res;
        }
        _rs_version_map[version] = std::move(rowset);
    }

    // init stale rowset
    for (auto& stale_rs_meta : _tablet_meta->all_stale_rs_metas()) {
        Version version = stale_rs_meta->version();
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(&_schema, _tablet_path_desc, stale_rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init stale rowset. tablet_id:" << tablet_id()
                         << ", schema_hash:" << schema_hash() << ", version=" << version
                         << ", res:" << res;
            return res;
        }
        _stale_rs_version_map[version] = std::move(rowset);
    }

    return res;
}

Status Tablet::init() {
    return _init_once.call([this] { return _init_once_action(); });
}

// should save tablet meta to remote meta store
// if it's a primary replica
void Tablet::save_meta() {
    auto res = _tablet_meta->save_meta(_data_dir);
    CHECK_EQ(res, Status::OK()) << "fail to save tablet_meta. res=" << res
                                << ", root=" << _data_dir->path();
}

Status Tablet::revise_tablet_meta(const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                                  const std::vector<Version>& versions_to_delete) {
    LOG(INFO) << "begin to revise tablet. tablet=" << full_name()
              << ", rowsets_to_clone=" << rowsets_to_clone.size()
              << ", versions_to_delete=" << versions_to_delete.size();
    Status res = Status::OK();
    do {
        // load new local tablet_meta to operate on
        TabletMetaSharedPtr new_tablet_meta(new (nothrow) TabletMeta(*_tablet_meta));

        // delete versions from new local tablet_meta
        for (const Version& version : versions_to_delete) {
            new_tablet_meta->delete_rs_meta_by_version(version, nullptr);
            if (new_tablet_meta->version_for_delete_predicate(version)) {
                new_tablet_meta->remove_delete_predicate_by_version(version);
            }
            LOG(INFO) << "delete version from new local tablet_meta when clone. [table="
                      << full_name() << ", version=" << version << "]";
        }

        // add new cloned rowset
        for (auto& rs_meta : rowsets_to_clone) {
            new_tablet_meta->add_rs_meta(rs_meta);
        }
        VLOG_NOTICE << "load rowsets successfully when clone. tablet=" << full_name()
                    << ", added rowset size=" << rowsets_to_clone.size();
        // save and reload tablet_meta
        res = new_tablet_meta->save_meta(_data_dir);
        if (!res.ok()) {
            LOG(WARNING) << "failed to save new local tablet_meta when clone. res:" << res;
            break;
        }
        _tablet_meta = new_tablet_meta;
    } while (0);

    for (auto& version : versions_to_delete) {
        auto it = _rs_version_map.find(version);
        DCHECK(it != _rs_version_map.end());
        StorageEngine::instance()->add_unused_rowset(it->second);
        _rs_version_map.erase(it);
    }

    for (auto& rs_meta : rowsets_to_clone) {
        Version version = {rs_meta->start_version(), rs_meta->end_version()};
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(&_schema, _tablet_path_desc, rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init rowset. version=" << version;
            return res;
        }
        _rs_version_map[version] = std::move(rowset);
    }

    // reconstruct from tablet meta
    _timestamped_version_tracker.construct_versioned_tracker(_tablet_meta->all_rs_metas());
    // clear stale rowset
    for (auto& it : _stale_rs_version_map) {
        StorageEngine::instance()->add_unused_rowset(it.second);
    }
    _stale_rs_version_map.clear();
    _tablet_meta->clear_stale_rowset();

    LOG(INFO) << "finish to revise tablet. res=" << res << ", "
              << "table=" << full_name();
    return res;
}

Status Tablet::add_rowset(RowsetSharedPtr rowset, bool need_persist) {
    DCHECK(rowset != nullptr);
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    // If the rowset already exist, just return directly.  The rowset_id is an unique-id,
    // we can use it to check this situation.
    if (_contains_rowset(rowset->rowset_id())) {
        return Status::OK();
    }
    // Otherwise, the version should be not contained in any existing rowset.
    RETURN_NOT_OK(_contains_version(rowset->version()));

    RETURN_NOT_OK(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;
    _timestamped_version_tracker.add_version(rowset->version());

    std::vector<RowsetSharedPtr> rowsets_to_delete;
    // yiguolei: temp code, should remove the rowset contains by this rowset
    // but it should be removed in multi path version
    for (auto& it : _rs_version_map) {
        if (rowset->version().contains(it.first) && rowset->version() != it.first) {
            CHECK(it.second != nullptr)
                    << "there exist a version=" << it.first
                    << " contains the input rs with version=" << rowset->version()
                    << ", but the related rs is null";
            rowsets_to_delete.push_back(it.second);
        }
    }
    std::vector<RowsetSharedPtr> empty_vec;
    modify_rowsets(empty_vec, rowsets_to_delete);

    if (need_persist) {
        Status res =
                RowsetMetaManager::save(data_dir()->get_meta(), tablet_uid(), rowset->rowset_id(),
                                        rowset->rowset_meta()->get_rowset_pb());
        if (!res.ok()) {
            LOG(FATAL) << "failed to save rowset to local meta store" << rowset->rowset_id();
        }
    }
    ++_newly_created_rowset_num;
    return Status::OK();
}

void Tablet::modify_rowsets(std::vector<RowsetSharedPtr>& to_add,
                            std::vector<RowsetSharedPtr>& to_delete) {
    // the compaction process allow to compact the single version, eg: version[4-4].
    // this kind of "single version compaction" has same "input version" and "output version".
    // which means "to_add->version()" equals to "to_delete->version()".
    // So we should delete the "to_delete" before adding the "to_add",
    // otherwise, the "to_add" will be deleted from _rs_version_map, eventually.
    //
    // And if the version of "to_add" and "to_delete" are exactly same. eg:
    // to_add:      [7-7]
    // to_delete:   [7-7]
    // In this case, we no longer need to add the rowset in "to_delete" to
    // _stale_rs_version_map, but can delete it directly.

    bool same_version = true;
    std::sort(to_add.begin(), to_add.end(), Rowset::comparator);
    std::sort(to_delete.begin(), to_delete.end(), Rowset::comparator);
    if (to_add.size() == to_delete.size()) {
        for (int i = 0; i < to_add.size(); ++i) {
            if (to_add[i]->version() != to_delete[i]->version()) {
                same_version = false;
                break;
            }
        }
    } else {
        same_version = false;
    }

    std::vector<RowsetMetaSharedPtr> rs_metas_to_delete;
    for (auto& rs : to_delete) {
        rs_metas_to_delete.push_back(rs->rowset_meta());
        _rs_version_map.erase(rs->version());

        if (!same_version) {
            // put compaction rowsets in _stale_rs_version_map.
            _stale_rs_version_map[rs->version()] = rs;
        }
    }

    std::vector<RowsetMetaSharedPtr> rs_metas_to_add;
    for (auto& rs : to_add) {
        rs_metas_to_add.push_back(rs->rowset_meta());
        _rs_version_map[rs->version()] = rs;

        if (!same_version) {
            // If version are same, then _timestamped_version_tracker
            // already has this version, no need to add again.
            _timestamped_version_tracker.add_version(rs->version());
        }
        ++_newly_created_rowset_num;
    }

    _tablet_meta->modify_rs_metas(rs_metas_to_add, rs_metas_to_delete, same_version);

    if (!same_version) {
        // add rs_metas_to_delete to tracker
        _timestamped_version_tracker.add_stale_path_version(rs_metas_to_delete);
    } else {
        // delete rowset in "to_delete" directly
        for (auto& rs : to_delete) {
            LOG(INFO) << "add unused rowset " << rs->rowset_id() << " because of same version";
            StorageEngine::instance()->add_unused_rowset(rs);
        }
    }
}

// snapshot manager may call this api to check if version exists, so that
// the version maybe not exist
const RowsetSharedPtr Tablet::get_rowset_by_version(const Version& version,
                                                    bool find_in_stale) const {
    auto iter = _rs_version_map.find(version);
    if (iter == _rs_version_map.end()) {
        if (find_in_stale) {
            return get_stale_rowset_by_version(version);
        }
        return nullptr;
    }
    return iter->second;
}

const RowsetSharedPtr Tablet::get_stale_rowset_by_version(const Version& version) const {
    auto iter = _stale_rs_version_map.find(version);
    if (iter == _stale_rs_version_map.end()) {
        VLOG_NOTICE << "no rowset for version:" << version << ", tablet: " << full_name();
        return nullptr;
    }
    return iter->second;
}

// Already under _meta_lock
const RowsetSharedPtr Tablet::rowset_with_max_version() const {
    Version max_version = _tablet_meta->max_version();
    if (max_version.first == -1) {
        return nullptr;
    }

    auto iter = _rs_version_map.find(max_version);
    if (iter == _rs_version_map.end()) {
        DCHECK(false) << "invalid version:" << max_version;
        return nullptr;
    }
    return iter->second;
}

RowsetSharedPtr Tablet::_rowset_with_largest_size() {
    RowsetSharedPtr largest_rowset = nullptr;
    for (auto& it : _rs_version_map) {
        if (it.second->empty() || it.second->zero_num_rows()) {
            continue;
        }
        if (largest_rowset == nullptr || it.second->rowset_meta()->index_disk_size() >
                                                 largest_rowset->rowset_meta()->index_disk_size()) {
            largest_rowset = it.second;
        }
    }

    return largest_rowset;
}

// add inc rowset should not persist tablet meta, because it will be persisted when publish txn.
Status Tablet::add_inc_rowset(const RowsetSharedPtr& rowset) {
    DCHECK(rowset != nullptr);
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    if (_contains_rowset(rowset->rowset_id())) {
        return Status::OK();
    }
    RETURN_NOT_OK(_contains_version(rowset->version()));

    RETURN_NOT_OK(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;

    _timestamped_version_tracker.add_version(rowset->version());

    ++_newly_created_rowset_num;
    return Status::OK();
}

void Tablet::_delete_stale_rowset_by_version(const Version& version) {
    RowsetMetaSharedPtr rowset_meta = _tablet_meta->acquire_stale_rs_meta_by_version(version);
    if (rowset_meta == nullptr) {
        return;
    }
    _tablet_meta->delete_stale_rs_meta_by_version(version);
    VLOG_NOTICE << "delete stale rowset. tablet=" << full_name() << ", version=" << version;
}

void Tablet::delete_expired_stale_rowset() {
    int64_t now = UnixSeconds();
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    // Compute the end time to delete rowsets, when a expired rowset createtime less then this time, it will be deleted.
    double expired_stale_sweep_endtime =
            ::difftime(now, config::tablet_rowset_stale_sweep_time_sec);

    std::vector<int64_t> path_id_vec;
    // capture the path version to delete
    _timestamped_version_tracker.capture_expired_paths(
            static_cast<int64_t>(expired_stale_sweep_endtime), &path_id_vec);

    if (path_id_vec.empty()) {
        return;
    }

    const RowsetSharedPtr lastest_delta = rowset_with_max_version();
    if (lastest_delta == nullptr) {
        LOG(WARNING) << "lastest_delta is null " << tablet_id();
        return;
    }

    // fetch missing version before delete
    std::vector<Version> missed_versions;
    calc_missed_versions_unlocked(lastest_delta->end_version(), &missed_versions);

    if (!missed_versions.empty()) {
        LOG(WARNING) << "tablet:" << full_name()
                     << ", missed version for version:" << lastest_delta->end_version();
        _print_missed_versions(missed_versions);
        return;
    }

    // do check consistent operation
    auto path_id_iter = path_id_vec.begin();

    std::map<int64_t, PathVersionListSharedPtr> stale_version_path_map;
    while (path_id_iter != path_id_vec.end()) {
        PathVersionListSharedPtr version_path =
                _timestamped_version_tracker.fetch_and_delete_path_by_id(*path_id_iter);

        Version test_version = Version(0, lastest_delta->end_version());
        stale_version_path_map[*path_id_iter] = version_path;

        Status status = capture_consistent_versions(test_version, nullptr);
        // 1. When there is no consistent versions, we must reconstruct the tracker.
        if (!status.ok()) {
            // 2. fetch missing version after delete
            std::vector<Version> after_missed_versions;
            calc_missed_versions_unlocked(lastest_delta->end_version(), &after_missed_versions);

            // 2.1 check whether missed_versions and after_missed_versions are the same.
            // when they are the same, it means we can delete the path securely.
            bool is_missing = missed_versions.size() != after_missed_versions.size();

            if (!is_missing) {
                for (int ver_index = 0; ver_index < missed_versions.size(); ver_index++) {
                    if (missed_versions[ver_index] != after_missed_versions[ver_index]) {
                        is_missing = true;
                        break;
                    }
                }
            }

            if (is_missing) {
                LOG(WARNING) << "The consistent version check fails, there are bugs. "
                             << "Reconstruct the tracker to recover versions in tablet="
                             << tablet_id();

                // 3. try to recover
                _timestamped_version_tracker.recover_versioned_tracker(stale_version_path_map);

                // 4. double check the consistent versions
                // fetch missing version after recover
                std::vector<Version> recover_missed_versions;
                calc_missed_versions_unlocked(lastest_delta->end_version(),
                                              &recover_missed_versions);

                // 4.1 check whether missed_versions and recover_missed_versions are the same.
                // when they are the same, it means we recover successfully.
                bool is_recover_missing = missed_versions.size() != recover_missed_versions.size();

                if (!is_recover_missing) {
                    for (int ver_index = 0; ver_index < missed_versions.size(); ver_index++) {
                        if (missed_versions[ver_index] != recover_missed_versions[ver_index]) {
                            is_recover_missing = true;
                            break;
                        }
                    }
                }

                // 5. check recover fail, version is mission
                if (is_recover_missing) {
                    if (!config::ignore_rowset_stale_unconsistent_delete) {
                        LOG(FATAL) << "rowset stale unconsistent delete. tablet= " << tablet_id();
                    } else {
                        LOG(WARNING) << "rowset stale unconsistent delete. tablet= " << tablet_id();
                    }
                }
            }
            return;
        }
        path_id_iter++;
    }

    auto old_size = _stale_rs_version_map.size();
    auto old_meta_size = _tablet_meta->all_stale_rs_metas().size();

    // do delete operation
    auto to_delete_iter = stale_version_path_map.begin();
    while (to_delete_iter != stale_version_path_map.end()) {
        std::vector<TimestampedVersionSharedPtr>& to_delete_version =
                to_delete_iter->second->timestamped_versions();
        for (auto& timestampedVersion : to_delete_version) {
            auto it = _stale_rs_version_map.find(timestampedVersion->version());
            if (it != _stale_rs_version_map.end()) {
                // delete rowset
                StorageEngine::instance()->add_unused_rowset(it->second);
                _stale_rs_version_map.erase(it);
                VLOG_NOTICE << "delete stale rowset tablet=" << full_name() << " version["
                            << timestampedVersion->version().first << ","
                            << timestampedVersion->version().second
                            << "] move to unused_rowset success " << std::fixed
                            << expired_stale_sweep_endtime;
            } else {
                LOG(WARNING) << "delete stale rowset tablet=" << full_name() << " version["
                             << timestampedVersion->version().first << ","
                             << timestampedVersion->version().second
                             << "] not find in stale rs version map";
            }
            _delete_stale_rowset_by_version(timestampedVersion->version());
        }
        to_delete_iter++;
    }

    bool reconstructed = _reconstruct_version_tracker_if_necessary();

    VLOG_NOTICE << "delete stale rowset _stale_rs_version_map tablet=" << full_name()
                << " current_size=" << _stale_rs_version_map.size() << " old_size=" << old_size
                << " current_meta_size=" << _tablet_meta->all_stale_rs_metas().size()
                << " old_meta_size=" << old_meta_size << " sweep endtime " << std::fixed
                << expired_stale_sweep_endtime << ", reconstructed=" << reconstructed;

#ifndef BE_TEST
    save_meta();
#endif
}

bool Tablet::_reconstruct_version_tracker_if_necessary() {
    double orphan_vertex_ratio = _timestamped_version_tracker.get_orphan_vertex_ratio();
    if (orphan_vertex_ratio >= config::tablet_version_graph_orphan_vertex_ratio) {
        _timestamped_version_tracker.construct_versioned_tracker(
                _tablet_meta->all_rs_metas(), _tablet_meta->all_stale_rs_metas());
        return true;
    }
    return false;
}

Status Tablet::capture_consistent_versions(const Version& spec_version,
                                           std::vector<Version>* version_path, bool quiet) const {
    Status status =
            _timestamped_version_tracker.capture_consistent_versions(spec_version, version_path);
    if (!status.ok() && !quiet) {
        std::vector<Version> missed_versions;
        calc_missed_versions_unlocked(spec_version.second, &missed_versions);
        if (missed_versions.empty()) {
            // if version_path is null, it may be a compaction check logic.
            // so to avoid print too many logs.
            if (version_path != nullptr) {
                LOG(WARNING) << "tablet:" << full_name()
                             << ", version already has been merged. spec_version: " << spec_version;
            }
            status = Status::OLAPInternalError(OLAP_ERR_VERSION_ALREADY_MERGED);
        } else {
            if (version_path != nullptr) {
                LOG(WARNING) << "status:" << status << ", tablet:" << full_name()
                             << ", missed version for version:" << spec_version;
                _print_missed_versions(missed_versions);
            }
        }
    }
    return status;
}

Status Tablet::check_version_integrity(const Version& version, bool quiet) {
    std::shared_lock rdlock(_meta_lock);
    return capture_consistent_versions(version, nullptr, quiet);
}

// If any rowset contains the specific version, it means the version already exist
bool Tablet::check_version_exist(const Version& version) const {
    for (auto& it : _rs_version_map) {
        if (it.first.contains(version)) {
            return true;
        }
    }
    return false;
}

// The meta read lock should be held before calling
void Tablet::acquire_version_and_rowsets(
        std::vector<std::pair<Version, RowsetSharedPtr>>* version_rowsets) const {
    for (const auto& it : _rs_version_map) {
        version_rowsets->emplace_back(it.first, it.second);
    }
}

Status Tablet::capture_consistent_rowsets(const Version& spec_version,
                                          std::vector<RowsetSharedPtr>* rowsets) const {
    std::vector<Version> version_path;
    RETURN_NOT_OK(capture_consistent_versions(spec_version, &version_path));
    RETURN_NOT_OK(_capture_consistent_rowsets_unlocked(version_path, rowsets));
    return Status::OK();
}

Status Tablet::_capture_consistent_rowsets_unlocked(const std::vector<Version>& version_path,
                                                    std::vector<RowsetSharedPtr>* rowsets) const {
    DCHECK(rowsets != nullptr && rowsets->empty());
    rowsets->reserve(version_path.size());
    for (auto& version : version_path) {
        bool is_find = false;
        do {
            auto it = _rs_version_map.find(version);
            if (it != _rs_version_map.end()) {
                is_find = true;
                rowsets->push_back(it->second);
                break;
            }

            auto it_expired = _stale_rs_version_map.find(version);
            if (it_expired != _stale_rs_version_map.end()) {
                is_find = true;
                rowsets->push_back(it_expired->second);
                break;
            }
        } while (0);

        if (!is_find) {
            LOG(WARNING) << "fail to find Rowset for version. tablet=" << full_name()
                         << ", version='" << version;
            return Status::OLAPInternalError(OLAP_ERR_CAPTURE_ROWSET_ERROR);
        }
    }
    return Status::OK();
}

Status Tablet::capture_rs_readers(const Version& spec_version,
                                  std::vector<RowsetReaderSharedPtr>* rs_readers) const {
    std::vector<Version> version_path;
    RETURN_NOT_OK(capture_consistent_versions(spec_version, &version_path));
    RETURN_NOT_OK(capture_rs_readers(version_path, rs_readers));
    return Status::OK();
}

Status Tablet::capture_rs_readers(const std::vector<Version>& version_path,
                                  std::vector<RowsetReaderSharedPtr>* rs_readers) const {
    DCHECK(rs_readers != nullptr && rs_readers->empty());
    for (auto version : version_path) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            VLOG_NOTICE << "fail to find Rowset in rs_version for version. tablet=" << full_name()
                        << ", version='" << version.first << "-" << version.second;

            it = _stale_rs_version_map.find(version);
            if (it == _stale_rs_version_map.end()) {
                LOG(WARNING) << "fail to find Rowset in stale_rs_version for version. tablet="
                             << full_name() << ", version='" << version.first << "-"
                             << version.second;
                return Status::OLAPInternalError(OLAP_ERR_CAPTURE_ROWSET_READER_ERROR);
            }
        }
        RowsetReaderSharedPtr rs_reader;
        auto res = it->second->create_reader(&rs_reader);
        if (!res.ok()) {
            LOG(WARNING) << "failed to create reader for rowset:" << it->second->rowset_id();
            return Status::OLAPInternalError(OLAP_ERR_CAPTURE_ROWSET_READER_ERROR);
        }
        rs_readers->push_back(std::move(rs_reader));
    }
    return Status::OK();
}

void Tablet::add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version) {
    _tablet_meta->add_delete_predicate(delete_predicate, version);
}

// TODO(lingbin): what is the difference between version_for_delete_predicate() and
// version_for_load_deletion()? should at least leave a comment
bool Tablet::version_for_delete_predicate(const Version& version) {
    return _tablet_meta->version_for_delete_predicate(version);
}

bool Tablet::version_for_load_deletion(const Version& version) {
    RowsetSharedPtr rowset = _rs_version_map.at(version);
    return rowset->delete_flag();
}

bool Tablet::can_do_compaction(size_t path_hash, CompactionType compaction_type) {
    if (compaction_type == CompactionType::BASE_COMPACTION && tablet_state() != TABLET_RUNNING) {
        // base compaction can only be done for tablet in TABLET_RUNNING state.
        // but cumulative compaction can be done for TABLET_NOTREADY, such as tablet under alter process.
        return false;
    }

    if (data_dir()->path_hash() != path_hash || !is_used() || !init_succeeded()) {
        return false;
    }

    if (tablet_state() == TABLET_RUNNING) {
        // if tablet state is running, we need to check if it has consistent versions.
        // tablet in other state such as TABLET_NOTREADY may not have complete versions.
        std::shared_lock rdlock(_meta_lock);
        const RowsetSharedPtr lastest_delta = rowset_with_max_version();
        if (lastest_delta == nullptr) {
            return false;
        }

        Version test_version = Version(0, lastest_delta->end_version());
        if (!capture_consistent_versions(test_version, nullptr)) {
            return false;
        }
    }
    return true;
}

uint32_t Tablet::calc_compaction_score(
        CompactionType compaction_type,
        std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
    // Need meta lock, because it will iterator "all_rs_metas" of tablet meta.
    std::shared_lock rdlock(_meta_lock);
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        return _calc_cumulative_compaction_score(cumulative_compaction_policy);
    } else {
        DCHECK_EQ(compaction_type, CompactionType::BASE_COMPACTION);
        return _calc_base_compaction_score();
    }
}

const uint32_t Tablet::_calc_cumulative_compaction_score(
        std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
#ifndef BE_TEST
    if (_cumulative_compaction_policy == nullptr ||
        _cumulative_compaction_policy->name() != cumulative_compaction_policy->name()) {
        _cumulative_compaction_policy = cumulative_compaction_policy;
    }
#endif
    uint32_t score = 0;
    _cumulative_compaction_policy->calc_cumulative_compaction_score(
            tablet_state(), _tablet_meta->all_rs_metas(), cumulative_layer_point(), &score);
    return score;
}

const uint32_t Tablet::_calc_base_compaction_score() const {
    uint32_t score = 0;
    const int64_t point = cumulative_layer_point();
    bool base_rowset_exist = false;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->start_version() >= point) {
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        }

        score += rs_meta->get_compaction_score();
    }

    // base不存在可能是tablet正在做alter table，先不选它，设score=0
    return base_rowset_exist ? score : 0;
}

void Tablet::calc_missed_versions(int64_t spec_version, std::vector<Version>* missed_versions) {
    std::shared_lock rdlock(_meta_lock);
    calc_missed_versions_unlocked(spec_version, missed_versions);
}

// for example:
//     [0-4][5-5][8-8][9-9]
// if spec_version = 6, we still return {7} other than {6, 7}
void Tablet::calc_missed_versions_unlocked(int64_t spec_version,
                                           std::vector<Version>* missed_versions) const {
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

    // From the first version(=0),  find the missing version until spec_version
    int64_t last_version = -1;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            for (int64_t i = last_version + 1; i < version.first && i <= spec_version; ++i) {
                missed_versions->emplace_back(Version(i, i));
            }
        }
        last_version = version.second;
        if (last_version >= spec_version) {
            break;
        }
    }
    for (int64_t i = last_version + 1; i <= spec_version; ++i) {
        missed_versions->emplace_back(Version(i, i));
    }
}

void Tablet::max_continuous_version_from_beginning(Version* version, Version* max_version) {
    std::shared_lock rdlock(_meta_lock);
    _max_continuous_version_from_beginning_unlocked(version, max_version);
}

void Tablet::_max_continuous_version_from_beginning_unlocked(Version* version,
                                                             Version* max_version) const {
    std::vector<Version> existing_versions;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const Version& left, const Version& right) {
                  // simple because 2 versions are certainly not overlapping
                  return left.first < right.first;
              });

    Version max_continuous_version = {-1, 0};
    for (int i = 0; i < existing_versions.size(); ++i) {
        if (existing_versions[i].first > max_continuous_version.second + 1) {
            break;
        }
        max_continuous_version = existing_versions[i];
    }
    *version = max_continuous_version;
    // tablet may not has rowset, eg, tablet has just been clear for restore.
    if (max_version != nullptr && !existing_versions.empty()) {
        *max_version = existing_versions.back();
    }
}

void Tablet::calculate_cumulative_point() {
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    int64_t ret_cumulative_point;
    _cumulative_compaction_policy->calculate_cumulative_point(
            this, _tablet_meta->all_rs_metas(), _cumulative_point, &ret_cumulative_point);

    if (ret_cumulative_point == K_INVALID_CUMULATIVE_POINT) {
        return;
    }

    set_cumulative_layer_point(ret_cumulative_point);
}

Status Tablet::split_range(const OlapTuple& start_key_strings, const OlapTuple& end_key_strings,
                           uint64_t request_block_row_count, std::vector<OlapTuple>* ranges) {
    DCHECK(ranges != nullptr);

    size_t key_num = 0;
    RowCursor start_key;
    // 如果有startkey，用startkey初始化；反之则用minkey初始化
    if (start_key_strings.size() > 0) {
        if (start_key.init_scan_key(_schema, start_key_strings.values()) != Status::OK()) {
            LOG(WARNING) << "fail to initial key strings with RowCursor type.";
            return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
        }

        if (start_key.from_tuple(start_key_strings) != Status::OK()) {
            LOG(WARNING) << "init end key failed";
            return Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA);
        }
        key_num = start_key_strings.size();
    } else {
        if (start_key.init(_schema, num_short_key_columns()) != Status::OK()) {
            LOG(WARNING) << "fail to initial key strings with RowCursor type.";
            return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
        }

        start_key.allocate_memory_for_string_type(_schema);
        start_key.build_min_key();
        key_num = num_short_key_columns();
    }

    RowCursor end_key;
    // 和startkey一样处理，没有则用maxkey初始化
    if (end_key_strings.size() > 0) {
        if (!end_key.init_scan_key(_schema, end_key_strings.values())) {
            LOG(WARNING) << "fail to parse strings to key with RowCursor type.";
            return Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA);
        }

        if (end_key.from_tuple(end_key_strings) != Status::OK()) {
            LOG(WARNING) << "init end key failed";
            return Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA);
        }
    } else {
        if (end_key.init(_schema, num_short_key_columns()) != Status::OK()) {
            LOG(WARNING) << "fail to initial key strings with RowCursor type.";
            return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
        }

        end_key.allocate_memory_for_string_type(_schema);
        end_key.build_max_key();
    }

    std::shared_lock rdlock(_meta_lock);
    RowsetSharedPtr rowset = _rowset_with_largest_size();

    // 如果找不到合适的rowset，就直接返回startkey，endkey
    if (rowset == nullptr) {
        VLOG_NOTICE << "there is no base file now, may be tablet is empty.";
        // it may be right if the tablet is empty, so we return success.
        ranges->emplace_back(start_key.to_tuple());
        ranges->emplace_back(end_key.to_tuple());
        return Status::OK();
    }
    return rowset->split_range(start_key, end_key, request_block_row_count, key_num, ranges);
}

// NOTE: only used when create_table, so it is sure that there is no concurrent reader and writer.
void Tablet::delete_all_files() {
    // Release resources like memory and disk space.
    std::shared_lock rdlock(_meta_lock);
    for (auto it : _rs_version_map) {
        it.second->remove();
    }
    _rs_version_map.clear();

    for (auto it : _stale_rs_version_map) {
        it.second->remove();
    }
    _stale_rs_version_map.clear();
}

bool Tablet::check_path(const std::string& path_to_check) const {
    std::shared_lock rdlock(_meta_lock);
    if (path_to_check == _tablet_path_desc.filepath) {
        return true;
    }
    std::string tablet_id_dir = path_util::dir_name(_tablet_path_desc.filepath);
    if (path_to_check == tablet_id_dir) {
        return true;
    }
    for (auto& version_rowset : _rs_version_map) {
        bool ret = version_rowset.second->check_path(path_to_check);
        if (ret) {
            return true;
        }
    }
    for (auto& stale_version_rowset : _stale_rs_version_map) {
        bool ret = stale_version_rowset.second->check_path(path_to_check);
        if (ret) {
            return true;
        }
    }
    return false;
}

// check rowset id in tablet-meta and in rowset-meta atomicly
// for example, during publish version stage, it will first add rowset meta to tablet meta and then
// remove it from rowset meta manager. If we check tablet meta first and then check rowset meta using 2 step unlocked
// the sequence maybe: 1. check in tablet meta [return false]  2. add to tablet meta  3. remove from rowset meta manager
// 4. check in rowset meta manager return false. so that the rowset maybe checked return false it means it is useless and
// will be treated as a garbage.
bool Tablet::check_rowset_id(const RowsetId& rowset_id) {
    std::shared_lock rdlock(_meta_lock);
    if (StorageEngine::instance()->rowset_id_in_use(rowset_id)) {
        return true;
    }
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    for (auto& stale_version_rowset : _stale_rs_version_map) {
        if (stale_version_rowset.second->rowset_id() == rowset_id) {
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
    ss << full_name() << " has " << missed_versions.size() << " missed version:";
    // print at most 10 version
    for (int i = 0; i < 10 && i < missed_versions.size(); ++i) {
        ss << missed_versions[i] << ",";
    }
    LOG(WARNING) << ss.str();
}

Status Tablet::_contains_version(const Version& version) {
    // check if there exist a rowset contains the added rowset
    for (auto& it : _rs_version_map) {
        if (it.first.contains(version)) {
            // TODO(lingbin): Is this check unnecessary?
            // because the value type is std::shared_ptr, when will it be nullptr?
            // In addition, in this class, there are many places that do not make this judgment
            // when access _rs_version_map's value.
            CHECK(it.second != nullptr) << "there exist a version=" << it.first
                                        << " contains the input rs with version=" << version
                                        << ", but the related rs is null";
            return Status::OLAPInternalError(OLAP_ERR_PUSH_VERSION_ALREADY_EXIST);
        }
    }

    return Status::OK();
}

Status Tablet::set_partition_id(int64_t partition_id) {
    return _tablet_meta->set_partition_id(partition_id);
}

TabletInfo Tablet::get_tablet_info() const {
    return TabletInfo(tablet_id(), schema_hash(), tablet_uid());
}

void Tablet::pick_candidate_rowsets_to_cumulative_compaction(
        int64_t skip_window_sec, std::vector<RowsetSharedPtr>* candidate_rowsets) {
    if (_cumulative_point == K_INVALID_CUMULATIVE_POINT) {
        return;
    }
    std::shared_lock rdlock(_meta_lock);
    _cumulative_compaction_policy->pick_candidate_rowsets(skip_window_sec, _rs_version_map,
                                                          _cumulative_point, candidate_rowsets);
}

void Tablet::pick_candidate_rowsets_to_base_compaction(vector<RowsetSharedPtr>* candidate_rowsets) {
    std::shared_lock rdlock(_meta_lock);
    for (auto& it : _rs_version_map) {
        if (it.first.first < _cumulative_point) {
            candidate_rowsets->push_back(it.second);
        }
    }
}

// For http compaction action
void Tablet::get_compaction_status(std::string* json_result) {
    rapidjson::Document root;
    root.SetObject();

    rapidjson::Document path_arr;
    path_arr.SetArray();

    std::vector<RowsetSharedPtr> rowsets;
    std::vector<RowsetSharedPtr> stale_rowsets;
    std::vector<bool> delete_flags;
    {
        std::shared_lock rdlock(_meta_lock);
        rowsets.reserve(_rs_version_map.size());
        for (auto& it : _rs_version_map) {
            rowsets.push_back(it.second);
        }
        std::sort(rowsets.begin(), rowsets.end(), Rowset::comparator);

        stale_rowsets.reserve(_stale_rs_version_map.size());
        for (auto& it : _stale_rs_version_map) {
            stale_rowsets.push_back(it.second);
        }
        std::sort(stale_rowsets.begin(), stale_rowsets.end(), Rowset::comparator);

        delete_flags.reserve(rowsets.size());
        for (auto& rs : rowsets) {
            delete_flags.push_back(version_for_delete_predicate(rs->version()));
        }
        // get snapshot version path json_doc
        _timestamped_version_tracker.get_stale_version_path_json_doc(path_arr);
    }
    rapidjson::Value cumulative_policy_type;
    std::string policy_type_str = "cumulative compaction policy not initializied";
    if (_cumulative_compaction_policy.get() != nullptr) {
        policy_type_str = _cumulative_compaction_policy->name();
    }
    cumulative_policy_type.SetString(policy_type_str.c_str(), policy_type_str.length(),
                                     root.GetAllocator());
    root.AddMember("cumulative policy type", cumulative_policy_type, root.GetAllocator());
    root.AddMember("cumulative point", _cumulative_point.load(), root.GetAllocator());
    rapidjson::Value cumu_value;
    std::string format_str = ToStringFromUnixMillis(_last_cumu_compaction_failure_millis.load());
    cumu_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative failure time", cumu_value, root.GetAllocator());
    rapidjson::Value base_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_failure_millis.load());
    base_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base failure time", base_value, root.GetAllocator());
    rapidjson::Value cumu_success_value;
    format_str = ToStringFromUnixMillis(_last_cumu_compaction_success_millis.load());
    cumu_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative success time", cumu_success_value, root.GetAllocator());
    rapidjson::Value base_success_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_success_millis.load());
    base_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base success time", base_success_value, root.GetAllocator());

    // print all rowsets' version as an array
    rapidjson::Document versions_arr;
    rapidjson::Document missing_versions_arr;
    versions_arr.SetArray();
    missing_versions_arr.SetArray();
    int64_t last_version = -1;
    for (int i = 0; i < rowsets.size(); ++i) {
        const Version& ver = rowsets[i]->version();
        if (ver.first != last_version + 1) {
            rapidjson::Value miss_value;
            miss_value.SetString(
                    strings::Substitute("[$0-$1]", last_version + 1, ver.first).c_str(),
                    missing_versions_arr.GetAllocator());
            missing_versions_arr.PushBack(miss_value, missing_versions_arr.GetAllocator());
        }
        rapidjson::Value value;
        std::string disk_size =
                PrettyPrinter::print(rowsets[i]->rowset_meta()->total_disk_size(), TUnit::BYTES);
        std::string version_str = strings::Substitute(
                "[$0-$1] $2 $3 $4 $5 $6", ver.first, ver.second, rowsets[i]->num_segments(),
                (delete_flags[i] ? "DELETE" : "DATA"),
                SegmentsOverlapPB_Name(rowsets[i]->rowset_meta()->segments_overlap()),
                rowsets[i]->rowset_id().to_string(), disk_size);
        value.SetString(version_str.c_str(), version_str.length(), versions_arr.GetAllocator());
        versions_arr.PushBack(value, versions_arr.GetAllocator());
        last_version = ver.second;
    }
    root.AddMember("rowsets", versions_arr, root.GetAllocator());
    root.AddMember("missing_rowsets", missing_versions_arr, root.GetAllocator());

    // print all stale rowsets' version as an array
    rapidjson::Document stale_versions_arr;
    stale_versions_arr.SetArray();
    for (int i = 0; i < stale_rowsets.size(); ++i) {
        const Version& ver = stale_rowsets[i]->version();
        rapidjson::Value value;
        std::string disk_size = PrettyPrinter::print(
                stale_rowsets[i]->rowset_meta()->total_disk_size(), TUnit::BYTES);
        std::string version_str = strings::Substitute(
                "[$0-$1] $2 $3 $4", ver.first, ver.second, stale_rowsets[i]->num_segments(),
                stale_rowsets[i]->rowset_id().to_string(), disk_size);
        value.SetString(version_str.c_str(), version_str.length(),
                        stale_versions_arr.GetAllocator());
        stale_versions_arr.PushBack(value, stale_versions_arr.GetAllocator());
    }
    root.AddMember("stale_rowsets", stale_versions_arr, root.GetAllocator());

    // add stale version rowsets
    root.AddMember("stale version path", path_arr, root.GetAllocator());

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());
}

bool Tablet::do_tablet_meta_checkpoint() {
    std::lock_guard<std::shared_mutex> store_lock(_meta_store_lock);
    if (_newly_created_rowset_num == 0) {
        return false;
    }
    if (UnixMillis() - _last_checkpoint_time <
                config::tablet_meta_checkpoint_min_interval_secs * 1000 &&
        _newly_created_rowset_num < config::tablet_meta_checkpoint_min_new_rowsets_num) {
        return false;
    }

    // hold read-lock other than write-lock, because it will not modify meta structure
    std::shared_lock rdlock(_meta_lock);
    if (tablet_state() != TABLET_RUNNING) {
        LOG(INFO) << "tablet is under state=" << tablet_state()
                  << ", not running, skip do checkpoint"
                  << ", tablet=" << full_name();
        return false;
    }
    VLOG_NOTICE << "start to do tablet meta checkpoint, tablet=" << full_name();
    save_meta();
    // if save meta successfully, then should remove the rowset meta existing in tablet
    // meta from rowset meta store
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if (rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(),
                                                 rs_meta->rowset_id())) {
            RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id());
            VLOG_NOTICE << "remove rowset id from meta store because it is already persistent with "
                        << "tablet meta, rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }

    // check _stale_rs_version_map to remove meta from rowset meta store
    for (auto& rs_meta : _tablet_meta->all_stale_rs_metas()) {
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if (rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(),
                                                 rs_meta->rowset_id())) {
            RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(), rs_meta->rowset_id());
            VLOG_NOTICE << "remove rowset id from meta store because it is already persistent with "
                        << "tablet meta, rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }

    _newly_created_rowset_num = 0;
    _last_checkpoint_time = UnixMillis();
    return true;
}

bool Tablet::rowset_meta_is_useful(RowsetMetaSharedPtr rowset_meta) {
    std::shared_lock rdlock(_meta_lock);
    bool find_version = false;
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_meta->rowset_id()) {
            return true;
        }
        if (version_rowset.second->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    for (auto& stale_version_rowset : _stale_rs_version_map) {
        if (stale_version_rowset.second->rowset_id() == rowset_meta->rowset_id()) {
            return true;
        }
        if (stale_version_rowset.second->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    return !find_version;
}

bool Tablet::_contains_rowset(const RowsetId rowset_id) {
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    for (auto& stale_version_rowset : _stale_rs_version_map) {
        if (stale_version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    return false;
}

void Tablet::build_tablet_report_info(TTabletInfo* tablet_info) {
    std::shared_lock rdlock(_meta_lock);
    tablet_info->tablet_id = _tablet_meta->tablet_id();
    tablet_info->schema_hash = _tablet_meta->schema_hash();
    tablet_info->row_count = _tablet_meta->num_rows();
    tablet_info->data_size = _tablet_meta->tablet_footprint();

    // Here we need to report to FE if there are any missing versions of tablet.
    // We start from the initial version and traverse backwards until we meet a discontinuous version.
    Version cversion;
    Version max_version;
    _max_continuous_version_from_beginning_unlocked(&cversion, &max_version);
    tablet_info->__set_version_miss(cversion.second < max_version.second);
    // find rowset with max version
    auto iter = _rs_version_map.find(max_version);
    if (iter == _rs_version_map.end()) {
        // If the tablet is in running state, it must not be doing schema-change. so if we can not
        // access its rowsets, it means that the tablet is bad and needs to be reported to the FE
        // for subsequent repairs (through the cloning task)
        if (tablet_state() == TABLET_RUNNING) {
            tablet_info->__set_used(false);
        }
        // For other states, FE knows that the tablet is in a certain change process, so here
        // still sets the state to normal when reporting. Note that every task has an timeout,
        // so if the task corresponding to this change hangs, when the task timeout, FE will know
        // and perform state modification operations.
    }

    // the report version is the largest continuous version, same logic as in FE side
    tablet_info->version = cversion.second;
    // Useless but it is a required filed in TTabletInfo
    tablet_info->version_hash = 0;
    tablet_info->__set_partition_id(_tablet_meta->partition_id());
    if (FilePathDesc::is_remote(_data_dir->storage_medium())) {
        tablet_info->__set_storage_medium(
                fs::fs_util::get_t_storage_medium(_tablet_meta->storage_medium()));
    } else {
        tablet_info->__set_storage_medium(_data_dir->storage_medium());
    }
    tablet_info->__set_version_count(_tablet_meta->version_count());
    tablet_info->__set_path_hash(_data_dir->path_hash());
    tablet_info->__set_is_in_memory(_tablet_meta->tablet_schema().is_in_memory());
}

// should use this method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
void Tablet::generate_tablet_meta_copy(TabletMetaSharedPtr new_tablet_meta) const {
    std::shared_lock rdlock(_meta_lock);
    generate_tablet_meta_copy_unlocked(new_tablet_meta);
}

// this is a unlocked version of generate_tablet_meta_copy()
// some method already hold the _meta_lock before calling this,
// such as EngineCloneTask::_finish_clone -> tablet->revise_tablet_meta
void Tablet::generate_tablet_meta_copy_unlocked(TabletMetaSharedPtr new_tablet_meta) const {
    TabletMetaPB tablet_meta_pb;
    _tablet_meta->to_meta_pb(&tablet_meta_pb);
    new_tablet_meta->init_from_pb(tablet_meta_pb);
}

double Tablet::calculate_scan_frequency() {
    time_t now = time(nullptr);
    int64_t current_count = query_scan_count->value();
    double interval = difftime(now, _last_record_scan_count_timestamp);
    double scan_frequency = (current_count - _last_record_scan_count) * 60 / interval;
    if (interval >= config::tablet_scan_frequency_time_node_interval_second) {
        _last_record_scan_count = current_count;
        _last_record_scan_count_timestamp = now;
    }
    return scan_frequency;
}

Status Tablet::prepare_compaction_and_calculate_permits(CompactionType compaction_type,
                                                        TabletSharedPtr tablet, int64_t* permits) {
    std::vector<RowsetSharedPtr> compaction_rowsets;
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (watch.elapsed_time() / 1e9 > config::cumulative_compaction_trace_threshold) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        TRACE("create cumulative compaction");
        StorageEngine::instance()->create_cumulative_compaction(tablet, _cumulative_compaction);
        DorisMetrics::instance()->cumulative_compaction_request_total->increment(1);
        Status res = _cumulative_compaction->prepare_compact();
        if (!res.ok()) {
            set_last_cumu_compaction_failure_time(UnixMillis());
            *permits = 0;
            if (res != Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION)) {
                DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
                return Status::InternalError(
                        fmt::format("prepare cumulative compaction with err: {}", res));
            }
            // return OK if OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION, so that we don't need to
            // print too much useless logs.
            // And because we set permits to 0, so even if we return OK here, nothing will be done.
            return Status::OK();
        }
        compaction_rowsets = _cumulative_compaction->get_input_rowsets();
    } else {
        DCHECK_EQ(compaction_type, CompactionType::BASE_COMPACTION);
        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (watch.elapsed_time() / 1e9 > config::base_compaction_trace_threshold) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        TRACE("create base compaction");
        StorageEngine::instance()->create_base_compaction(tablet, _base_compaction);
        DorisMetrics::instance()->base_compaction_request_total->increment(1);
        Status res = _base_compaction->prepare_compact();
        if (!res.ok()) {
            set_last_base_compaction_failure_time(UnixMillis());
            *permits = 0;
            if (res != Status::OLAPInternalError(OLAP_ERR_BE_NO_SUITABLE_VERSION)) {
                DorisMetrics::instance()->base_compaction_request_failed->increment(1);
                return Status::InternalError(
                        fmt::format("prepare base compaction with err: {}", res));
            }
            // return OK if OLAP_ERR_BE_NO_SUITABLE_VERSION, so that we don't need to
            // print too much useless logs.
            // And because we set permits to 0, so even if we return OK here, nothing will be done.
            return Status::OK();
        }
        compaction_rowsets = _base_compaction->get_input_rowsets();
    }
    *permits = 0;
    for (auto rowset : compaction_rowsets) {
        *permits += rowset->rowset_meta()->get_compaction_score();
    }
    return Status::OK();
}

void Tablet::execute_compaction(CompactionType compaction_type) {
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (!config::disable_compaction_trace_log &&
                watch.elapsed_time() / 1e9 > config::cumulative_compaction_trace_threshold) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        TRACE("execute cumulative compaction");
        Status res = _cumulative_compaction->execute_compact();
        if (!res.ok()) {
            set_last_cumu_compaction_failure_time(UnixMillis());
            DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
            LOG(WARNING) << "failed to do cumulative compaction. res=" << res
                         << ", tablet=" << full_name();
            return;
        }
        set_last_cumu_compaction_failure_time(0);
    } else {
        DCHECK_EQ(compaction_type, CompactionType::BASE_COMPACTION);
        scoped_refptr<Trace> trace(new Trace);
        MonotonicStopWatch watch;
        watch.start();
        SCOPED_CLEANUP({
            if (!config::disable_compaction_trace_log &&
                watch.elapsed_time() / 1e9 > config::base_compaction_trace_threshold) {
                LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
            }
        });
        ADOPT_TRACE(trace.get());

        TRACE("create base compaction");
        Status res = _base_compaction->execute_compact();
        if (!res.ok()) {
            set_last_base_compaction_failure_time(UnixMillis());
            DorisMetrics::instance()->base_compaction_request_failed->increment(1);
            LOG(WARNING) << "failed to do base compaction. res=" << res
                         << ", tablet=" << full_name();
            return;
        }
        set_last_base_compaction_failure_time(0);
    }
}

void Tablet::reset_compaction(CompactionType compaction_type) {
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        _cumulative_compaction.reset();
    } else {
        _base_compaction.reset();
    }
}

} // namespace doris
