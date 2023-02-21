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

#include <bvar/reducer.h>
#include <bvar/window.h>
#include <ctype.h>
#include <fmt/core.h>
#include <glog/logging.h>
#include <opentelemetry/common/threadlocal.h>
#include <pthread.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <stdio.h>
#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_set>

#include "agent/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/stringpiece.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "olap/base_compaction.h"
#include "olap/base_tablet.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "segment_loader.h"
#include "service/point_query_executor.h"
#include "util/defer_op.h"
#include "util/path_util.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"
#include "util/trace.h"
#include "util/uid_util.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/jsonb/serialize.h"

namespace doris {
using namespace ErrorCode;

using std::pair;
using std::nothrow;
using std::sort;
using std::string;
using std::vector;
using io::FileSystemSPtr;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_finish_count, MetricUnit::OPERATIONS);

bvar::Adder<uint64_t> exceed_version_limit_counter;
bvar::Window<bvar::Adder<uint64_t>> exceed_version_limit_counter_minute(
        &exceed_version_limit_counter, 60);

TabletSharedPtr Tablet::create_tablet_from_meta(TabletMetaSharedPtr tablet_meta,
                                                DataDir* data_dir) {
    return std::make_shared<Tablet>(tablet_meta, data_dir);
}

Tablet::Tablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir,
               const std::string& cumulative_compaction_type)
        : BaseTablet(tablet_meta, data_dir),
          _is_bad(false),
          _last_cumu_compaction_failure_millis(0),
          _last_base_compaction_failure_millis(0),
          _last_cumu_compaction_success_millis(0),
          _last_base_compaction_success_millis(0),
          _cumulative_point(K_INVALID_CUMULATIVE_POINT),
          _newly_created_rowset_num(0),
          _last_checkpoint_time(0),
          _cumulative_compaction_type(cumulative_compaction_type),
          _is_clone_occurred(false),
          _last_missed_version(-1),
          _last_missed_time_s(0) {
    // construct _timestamped_versioned_tracker from rs and stale rs meta
    _timestamped_version_tracker.construct_versioned_tracker(_tablet_meta->all_rs_metas(),
                                                             _tablet_meta->all_stale_rs_metas());
    // if !_tablet_meta->all_rs_metas()[0]->tablet_schema(),
    // that mean the tablet_meta is still no upgrade to doris 1.2 versions.
    // Before doris 1.2 version, rowset metas don't have tablet schema.
    // And when upgrade to doris 1.2 version,
    // all rowset metas will be set the tablet schmea from tablet meta.
    if (_tablet_meta->all_rs_metas().empty() || !_tablet_meta->all_rs_metas()[0]->tablet_schema()) {
        _max_version_schema = BaseTablet::tablet_schema();
    } else {
        _max_version_schema =
                rowset_meta_with_max_schema_version(_tablet_meta->all_rs_metas())->tablet_schema();
    }
    DCHECK(_max_version_schema);

    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_finish_count);
}

Status Tablet::_init_once_action() {
    Status res = Status::OK();
    VLOG_NOTICE << "begin to load tablet. tablet=" << full_name()
                << ", version_size=" << _tablet_meta->version_count();

#ifdef BE_TEST
    // init cumulative compaction policy by type
    _cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy();
#endif

    RowsetVector rowset_vec;
    for (const auto& rs_meta : _tablet_meta->all_rs_metas()) {
        Version version = rs_meta->version();
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(_schema, _tablet_path, rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init rowset. tablet_id=" << tablet_id()
                         << ", schema_hash=" << schema_hash() << ", version=" << version
                         << ", res=" << res;
            return res;
        }
        rowset_vec.push_back(rowset);
        _rs_version_map[version] = std::move(rowset);
    }

    // init stale rowset
    for (auto& stale_rs_meta : _tablet_meta->all_stale_rs_metas()) {
        Version version = stale_rs_meta->version();
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(_schema, _tablet_path, stale_rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init stale rowset. tablet_id:" << tablet_id()
                         << ", schema_hash:" << schema_hash() << ", version=" << version
                         << ", res:" << res;
            return res;
        }
        _stale_rs_version_map[version] = std::move(rowset);
    }

    if (_schema->keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        _rowset_tree = std::make_unique<RowsetTree>();
        res = _rowset_tree->Init(rowset_vec);
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
    RowsetVector rs_to_delete, rs_to_add;

    for (auto& version : versions_to_delete) {
        auto it = _rs_version_map.find(version);
        DCHECK(it != _rs_version_map.end());
        StorageEngine::instance()->add_unused_rowset(it->second);
        rs_to_delete.push_back(it->second);
        _rs_version_map.erase(it);
    }

    for (auto& rs_meta : rowsets_to_clone) {
        Version version = {rs_meta->start_version(), rs_meta->end_version()};
        RowsetSharedPtr rowset;
        res = RowsetFactory::create_rowset(_schema, _tablet_path, rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init rowset. version=" << version;
            return res;
        }
        rs_to_add.push_back(rowset);
        _rs_version_map[version] = std::move(rowset);
    }

    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        auto new_rowset_tree = std::make_unique<RowsetTree>();
        ModifyRowSetTree(*_rowset_tree, rs_to_delete, rs_to_add, new_rowset_tree.get());
        _rowset_tree = std::move(new_rowset_tree);
        for (auto rowset_ptr : rs_to_add) {
            RETURN_IF_ERROR(update_delete_bitmap_without_lock(rowset_ptr));
        }
    }

    do {
        // load new local tablet_meta to operate on
        TabletMetaSharedPtr new_tablet_meta(new (nothrow) TabletMeta(*_tablet_meta));

        // delete versions from new local tablet_meta
        for (const Version& version : versions_to_delete) {
            new_tablet_meta->delete_rs_meta_by_version(version, nullptr);
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

RowsetSharedPtr Tablet::get_rowset(const RowsetId& rowset_id) {
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_id) {
            return version_rowset.second;
        }
    }
    for (auto& stale_version_rowset : _stale_rs_version_map) {
        if (stale_version_rowset.second->rowset_id() == rowset_id) {
            return stale_version_rowset.second;
        }
    }
    return nullptr;
}

Status Tablet::add_rowset(RowsetSharedPtr rowset) {
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

    // Update rowset tree
    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        auto new_rowset_tree = std::make_unique<RowsetTree>();
        ModifyRowSetTree(*_rowset_tree, {}, {rowset}, new_rowset_tree.get());
        _rowset_tree = std::move(new_rowset_tree);
    }

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
    ++_newly_created_rowset_num;
    return Status::OK();
}

Status Tablet::modify_rowsets(std::vector<RowsetSharedPtr>& to_add,
                              std::vector<RowsetSharedPtr>& to_delete, bool check_delete) {
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

    if (to_add.empty() && to_delete.empty()) {
        return Status::OK();
    }

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

    if (check_delete) {
        for (auto& rs : to_delete) {
            auto find_rs = _rs_version_map.find(rs->version());
            if (find_rs == _rs_version_map.end()) {
                LOG(WARNING) << "try to delete not exist version " << rs->version() << " from "
                             << full_name();
                return Status::Error<DELETE_VERSION_ERROR>();
            } else if (find_rs->second->rowset_id() != rs->rowset_id()) {
                LOG(WARNING) << "try to delete version " << rs->version() << " from " << full_name()
                             << ", but rowset id changed, delete rowset id is " << rs->rowset_id()
                             << ", exists rowsetid is" << find_rs->second->rowset_id();
                return Status::Error<DELETE_VERSION_ERROR>();
            }
        }
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

    // Update rowset tree
    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        auto new_rowset_tree = std::make_unique<RowsetTree>();
        ModifyRowSetTree(*_rowset_tree, to_delete, to_add, new_rowset_tree.get());
        _rowset_tree = std::move(new_rowset_tree);
    }

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
    return Status::OK();
}

void Tablet::add_rowsets(const std::vector<RowsetSharedPtr>& to_add) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    rs_metas.reserve(to_add.size());
    for (auto& rs : to_add) {
        _rs_version_map.emplace(rs->version(), rs);
        _timestamped_version_tracker.add_version(rs->version());
        rs_metas.push_back(rs->rowset_meta());
    }
    _tablet_meta->modify_rs_metas(rs_metas, {});
}

void Tablet::delete_rowsets(const std::vector<RowsetSharedPtr>& to_delete, bool move_to_stale) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    rs_metas.reserve(to_delete.size());
    for (auto& rs : to_delete) {
        rs_metas.push_back(rs->rowset_meta());
        _rs_version_map.erase(rs->version());
    }
    _tablet_meta->modify_rs_metas({}, rs_metas, !move_to_stale);
    if (move_to_stale) {
        for (auto& rs : to_delete) {
            _stale_rs_version_map[rs->version()] = rs;
        }
        _timestamped_version_tracker.add_stale_path_version(rs_metas);
    } else {
        for (auto& rs : to_delete) {
            _timestamped_version_tracker.delete_version(rs->version());
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

RowsetMetaSharedPtr Tablet::rowset_meta_with_max_schema_version(
        const std::vector<RowsetMetaSharedPtr>& rowset_metas) {
    return *std::max_element(
            rowset_metas.begin(), rowset_metas.end(),
            [](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
                return !a->tablet_schema()
                               ? true
                               : (!b->tablet_schema()
                                          ? false
                                          : a->tablet_schema()->schema_version() <
                                                    b->tablet_schema()->schema_version());
            });
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

    // Update rowset tree
    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        auto new_rowset_tree = std::make_unique<RowsetTree>();
        ModifyRowSetTree(*_rowset_tree, {}, {rowset}, new_rowset_tree.get());
        _rowset_tree = std::move(new_rowset_tree);
    }

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
            status = Status::Error<VERSION_ALREADY_MERGED>();
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

bool Tablet::exceed_version_limit(int32_t limit) const {
    if (_tablet_meta->version_count() > limit) {
        exceed_version_limit_counter << 1;
        return true;
    }
    return false;
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
            return Status::Error<CAPTURE_ROWSET_ERROR>();
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
                return Status::Error<CAPTURE_ROWSET_READER_ERROR>();
            }
        }
        RowsetReaderSharedPtr rs_reader;
        auto res = it->second->create_reader(&rs_reader);
        if (!res.ok()) {
            LOG(WARNING) << "failed to create reader for rowset:" << it->second->rowset_id();
            return Status::Error<CAPTURE_ROWSET_READER_ERROR>();
        }
        rs_readers->push_back(std::move(rs_reader));
    }
    return Status::OK();
}

bool Tablet::version_for_delete_predicate(const Version& version) {
    return _tablet_meta->version_for_delete_predicate(version);
}

bool Tablet::can_do_compaction(size_t path_hash, CompactionType compaction_type) {
    if (compaction_type == CompactionType::BASE_COMPACTION && tablet_state() != TABLET_RUNNING) {
        // base compaction can only be done for tablet in TABLET_RUNNING state.
        // but cumulative compaction can be done for TABLET_NOTREADY, such as tablet under alter process.
        return false;
    }

    // unique key table with merge-on-write also cann't do cumulative compaction under alter
    // process. It may cause the delete bitmap calculation error, such as two
    // rowsets have same key.
    if (tablet_state() != TABLET_RUNNING && keys_type() == UNIQUE_KEYS &&
        enable_unique_key_merge_on_write()) {
        return false;
    }

    if (data_dir()->path_hash() != path_hash || !is_used() || !init_succeeded()) {
        return false;
    }

    if (tablet_state() == TABLET_NOTREADY) {
        // In TABLET_NOTREADY, we keep last 10 versions in new tablet so base tablet max_version
        // not merged in new tablet and then we can do compaction
        return true;
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

uint32_t Tablet::calc_cold_data_compaction_score() const {
    uint32_t score = 0;
    std::vector<RowsetMetaSharedPtr> cooldowned_rowsets;
    int64_t max_delete_version = 0;
    {
        std::shared_lock rlock(_meta_lock);
        for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
            if (!rs_meta->is_local()) {
                cooldowned_rowsets.push_back(rs_meta);
                if (rs_meta->has_delete_predicate() &&
                    rs_meta->end_version() > max_delete_version) {
                    max_delete_version = rs_meta->end_version();
                }
            }
        }
    }
    for (auto& rs_meta : cooldowned_rowsets) {
        if (rs_meta->end_version() < max_delete_version) {
            score += rs_meta->num_segments();
        } else {
            score += rs_meta->get_compaction_score();
        }
    }
    return (keys_type() != KeysType::DUP_KEYS) ? score * 2 : score;
}

uint32_t Tablet::_calc_cumulative_compaction_score(
        std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
#ifndef BE_TEST
    if (_cumulative_compaction_policy == nullptr ||
        _cumulative_compaction_policy->name() != cumulative_compaction_policy->name()) {
        _cumulative_compaction_policy = cumulative_compaction_policy;
    }
#endif
    return _cumulative_compaction_policy->calc_cumulative_compaction_score(this);
}

uint32_t Tablet::_calc_base_compaction_score() const {
    uint32_t score = 0;
    const int64_t point = cumulative_layer_point();
    bool base_rowset_exist = false;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->start_version() >= point || !rs_meta->is_local()) {
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
    bool has_version_cross;
    std::shared_lock rdlock(_meta_lock);
    _max_continuous_version_from_beginning_unlocked(version, max_version, &has_version_cross);
}

void Tablet::_max_continuous_version_from_beginning_unlocked(Version* version, Version* max_version,
                                                             bool* has_version_cross) const {
    std::vector<Version> existing_versions;
    *has_version_cross = false;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const Version& left, const Version& right) {
                  // simple because 2 versions are certainly not overlapping
                  return left.first < right.first;
              });

    Version max_continuous_version = {-1, -1};
    for (int i = 0; i < existing_versions.size(); ++i) {
        if (existing_versions[i].first > max_continuous_version.second + 1) {
            break;
        } else if (existing_versions[i].first <= max_continuous_version.second) {
            *has_version_cross = true;
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

    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        // clear rowset_tree
        _rowset_tree = std::make_unique<RowsetTree>();
    }
}

bool Tablet::check_path(const std::string& path_to_check) const {
    std::shared_lock rdlock(_meta_lock);
    if (path_to_check == _tablet_path) {
        return true;
    }
    auto tablet_id_dir = io::Path(_tablet_path).parent_path();
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
            return Status::Error<PUSH_VERSION_ALREADY_EXIST>();
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

std::vector<RowsetSharedPtr> Tablet::pick_candidate_rowsets_to_cumulative_compaction() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    if (_cumulative_point == K_INVALID_CUMULATIVE_POINT) {
        return candidate_rowsets;
    }
    {
        std::shared_lock rlock(_meta_lock);
        for (const auto& [version, rs] : _rs_version_map) {
            if (version.first >= _cumulative_point && rs->is_local()) {
                candidate_rowsets.push_back(rs);
            }
        }
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    return candidate_rowsets;
}

std::vector<RowsetSharedPtr> Tablet::pick_candidate_rowsets_to_base_compaction() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    {
        std::shared_lock rlock(_meta_lock);
        for (const auto& [version, rs] : _rs_version_map) {
            // Do compaction on local rowsets only.
            if (version.first < _cumulative_point && rs->is_local()) {
                candidate_rowsets.push_back(rs);
            }
        }
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    return candidate_rowsets;
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
            delete_flags.push_back(rs->rowset_meta()->has_delete_predicate());
        }
        // get snapshot version path json_doc
        _timestamped_version_tracker.get_stale_version_path_json_doc(path_arr);
    }
    rapidjson::Value cumulative_policy_type;
    std::string policy_type_str = "cumulative compaction policy not initializied";
    if (_cumulative_compaction_policy != nullptr) {
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
                    strings::Substitute("[$0-$1]", last_version + 1, ver.first - 1).c_str(),
                    missing_versions_arr.GetAllocator());
            missing_versions_arr.PushBack(miss_value, missing_versions_arr.GetAllocator());
        }
        rapidjson::Value value;
        std::string disk_size = PrettyPrinter::print(
                static_cast<uint64_t>(rowsets[i]->rowset_meta()->total_disk_size()), TUnit::BYTES);
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
                static_cast<uint64_t>(stale_rowsets[i]->rowset_meta()->total_disk_size()),
                TUnit::BYTES);
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

// need check if consecutive version missing in full report
// alter tablet will ignore this check
void Tablet::build_tablet_report_info(TTabletInfo* tablet_info,
                                      bool enable_consecutive_missing_check) {
    std::shared_lock rdlock(_meta_lock);
    tablet_info->tablet_id = _tablet_meta->tablet_id();
    tablet_info->schema_hash = _tablet_meta->schema_hash();
    tablet_info->row_count = _tablet_meta->num_rows();
    tablet_info->data_size = _tablet_meta->tablet_local_size();

    // Here we need to report to FE if there are any missing versions of tablet.
    // We start from the initial version and traverse backwards until we meet a discontinuous version.
    Version cversion;
    Version max_version;
    bool has_version_cross;
    _max_continuous_version_from_beginning_unlocked(&cversion, &max_version, &has_version_cross);
    // cause publish version task runs concurrently, version may be flying
    // so we add a consecutive miss check to solve this problem:
    // if publish version 5 arrives but version 4 flying, we may judge replica miss version
    // and set version miss in tablet_info, which makes fe treat this replica as unhealth
    // and lead to other problems
    if (enable_consecutive_missing_check) {
        if (cversion.second < max_version.second) {
            if (_last_missed_version == cversion.second + 1) {
                if (MonotonicSeconds() - _last_missed_time_s >= 60) {
                    // version missed for over 60 seconds
                    tablet_info->__set_version_miss(true);
                    _last_missed_version = -1;
                    _last_missed_time_s = 0;
                }
            } else {
                _last_missed_version = cversion.second + 1;
                _last_missed_time_s = MonotonicSeconds();
            }
        }
    } else {
        tablet_info->__set_version_miss(cversion.second < max_version.second);
    }
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

    if (has_version_cross && tablet_state() == TABLET_RUNNING) {
        tablet_info->__set_used(false);
    }

    if (tablet_state() == TABLET_SHUTDOWN) {
        tablet_info->__set_used(false);
    }

    // the report version is the largest continuous version, same logic as in FE side
    tablet_info->version = cversion.second;
    // Useless but it is a required filed in TTabletInfo
    tablet_info->version_hash = 0;
    tablet_info->__set_partition_id(_tablet_meta->partition_id());
    tablet_info->__set_storage_medium(_data_dir->storage_medium());
    tablet_info->__set_version_count(_tablet_meta->version_count());
    tablet_info->__set_path_hash(_data_dir->path_hash());
    tablet_info->__set_is_in_memory(_tablet_meta->tablet_schema()->is_in_memory());
    tablet_info->__set_replica_id(replica_id());
    tablet_info->__set_remote_data_size(_tablet_meta->tablet_remote_size());
    if (tablet_state() == TABLET_RUNNING && _tablet_meta->storage_policy_id() > 0) {
        tablet_info->__set_cooldown_replica_id(_cooldown_replica_id);
        tablet_info->__set_cooldown_term(_cooldown_term);
    }
    if (_tablet_meta->cooldown_meta_id().initialized()) {
        tablet_info->__set_cooldown_meta_id(_tablet_meta->cooldown_meta_id().to_thrift());
    }
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
            if (!res.is<CUMULATIVE_NO_SUITABLE_VERSION>()) {
                DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
                return Status::InternalError("prepare cumulative compaction with err: {}", res);
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
            if (!res.is<BE_NO_SUITABLE_VERSION>()) {
                DorisMetrics::instance()->base_compaction_request_failed->increment(1);
                return Status::InternalError("prepare base compaction with err: {}", res);
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

Status Tablet::create_initial_rowset(const int64_t req_version) {
    Status res = Status::OK();
    if (req_version < 1) {
        LOG(WARNING) << "init version of tablet should at least 1. req.ver=" << req_version;
        return Status::Error<CE_CMD_PARAMS_ERROR>();
    }
    Version version(0, req_version);
    RowsetSharedPtr new_rowset;
    do {
        // there is no data in init rowset, so overlapping info is unknown.
        std::unique_ptr<RowsetWriter> rs_writer;
        RowsetWriterContext context;
        context.version = version;
        context.rowset_state = VISIBLE;
        context.segments_overlap = OVERLAP_UNKNOWN;
        context.tablet_schema = tablet_schema();
        context.newest_write_timestamp = UnixSeconds();
        res = create_rowset_writer(context, &rs_writer);

        if (!res.ok()) {
            LOG(WARNING) << "failed to init rowset writer for tablet " << full_name();
            break;
        }
        res = rs_writer->flush();
        if (!res.ok()) {
            LOG(WARNING) << "failed to flush rowset writer for tablet " << full_name();
            break;
        }

        new_rowset = rs_writer->build();
        res = add_rowset(new_rowset);
        if (!res.ok()) {
            LOG(WARNING) << "failed to add rowset for tablet " << full_name();
            break;
        }
    } while (0);

    // Unregister index and delete files(index and data) if failed
    if (!res.ok()) {
        LOG(WARNING) << "fail to create initial rowset. res=" << res << " version=" << req_version;
        StorageEngine::instance()->add_unused_rowset(new_rowset);
        return res;
    }
    set_cumulative_layer_point(req_version + 1);
    return res;
}

Status Tablet::create_vertical_rowset_writer(RowsetWriterContext& context,
                                             std::unique_ptr<RowsetWriter>* rowset_writer) {
    _init_context_common_fields(context);
    return RowsetFactory::create_rowset_writer(context, true, rowset_writer);
}

Status Tablet::create_rowset_writer(RowsetWriterContext& context,
                                    std::unique_ptr<RowsetWriter>* rowset_writer) {
    _init_context_common_fields(context);
    return RowsetFactory::create_rowset_writer(context, false, rowset_writer);
}

void Tablet::_init_context_common_fields(RowsetWriterContext& context) {
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = tablet_uid();

    context.tablet_id = tablet_id();
    context.partition_id = partition_id();
    context.tablet_schema_hash = schema_hash();
    context.rowset_type = tablet_meta()->preferred_rowset_type();
    // Alpha Rowset will be removed in the future, so that if the tablet's default rowset type is
    // alpah rowset, then set the newly created rowset to storage engine's default rowset.
    if (context.rowset_type == ALPHA_ROWSET) {
        context.rowset_type = StorageEngine::instance()->default_rowset_type();
    }
    if (context.fs != nullptr && context.fs->type() != io::FileSystemType::LOCAL) {
        context.rowset_dir = BetaRowset::remote_tablet_path(tablet_id());
    } else {
        context.rowset_dir = tablet_path();
    }
    context.data_dir = data_dir();
    context.enable_unique_key_merge_on_write = enable_unique_key_merge_on_write();
}

Status Tablet::create_rowset(RowsetMetaSharedPtr rowset_meta, RowsetSharedPtr* rowset) {
    return RowsetFactory::create_rowset(tablet_schema(), tablet_path(), rowset_meta, rowset);
}

Status Tablet::cooldown() {
    std::unique_lock schema_change_lock(_schema_change_lock, std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "Failed to own schema_change_lock. tablet=" << tablet_id();
        return Status::Error<TRY_LOCK_FAILED>();
    }
    // Check executing serially with compaction task.
    std::unique_lock base_compaction_lock(_base_compaction_lock, std::try_to_lock);
    if (!base_compaction_lock.owns_lock()) {
        LOG(WARNING) << "Failed to own base_compaction_lock. tablet=" << tablet_id();
        return Status::Error<TRY_LOCK_FAILED>();
    }
    std::unique_lock cumu_compaction_lock(_cumulative_compaction_lock, std::try_to_lock);
    if (!cumu_compaction_lock.owns_lock()) {
        LOG(WARNING) << "Failed to own cumu_compaction_lock. tablet=" << tablet_id();
        return Status::Error<TRY_LOCK_FAILED>();
    }
    int64_t cooldown_replica_id = _cooldown_replica_id;
    if (cooldown_replica_id <= 0) { // wait for FE to push cooldown conf
        return Status::InternalError("invalid cooldown_replica_id");
    }

    std::shared_ptr<io::RemoteFileSystem> fs;
    RETURN_IF_ERROR(get_remote_file_system(storage_policy_id(), &fs));

    if (cooldown_replica_id == replica_id()) {
        RETURN_IF_ERROR(_cooldown_data(fs));
    } else {
        RETURN_IF_ERROR(_follow_cooldowned_data(fs, cooldown_replica_id));
    }
    return Status::OK();
}

Status Tablet::_cooldown_data(const std::shared_ptr<io::RemoteFileSystem>& dest_fs) {
    auto old_rowset = pick_cooldown_rowset();
    if (!old_rowset) {
        return Status::InternalError("cannot pick cooldown rowset in tablet {}", tablet_id());
    }
    RowsetId new_rowset_id = StorageEngine::instance()->next_rowset_id();

    auto start = std::chrono::steady_clock::now();

    Status st;
    {
        std::shared_lock slock(_remote_files_lock, std::try_to_lock);
        if (!slock.owns_lock()) {
            return Status::Status::Error<TRY_LOCK_FAILED>("try remote_files_lock failed");
        }
        st = old_rowset->upload_to(dest_fs.get(), new_rowset_id);
    }
    if (!st.ok()) {
        // reclaim the incomplete rowset data in remote storage
        record_unused_remote_rowset(new_rowset_id, dest_fs->id(), old_rowset->num_segments());
        return st;
    }

    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);
    LOG(INFO) << "Upload rowset " << old_rowset->version() << " " << new_rowset_id.to_string()
              << " to " << dest_fs->root_path().native() << ", tablet_id=" << tablet_id()
              << ", duration=" << duration.count() << ", capacity=" << old_rowset->data_disk_size()
              << ", tp=" << old_rowset->data_disk_size() / duration.count();

    // gen a new rowset
    auto new_rowset_meta = std::make_shared<RowsetMeta>(*old_rowset->rowset_meta());
    new_rowset_meta->set_rowset_id(new_rowset_id);
    new_rowset_meta->set_resource_id(dest_fs->id());
    new_rowset_meta->set_fs(dest_fs);
    new_rowset_meta->set_creation_time(time(nullptr));
    UniqueId cooldown_meta_id = UniqueId::gen_uid();

    // upload cooldowned rowset meta to remote fs
    RETURN_IF_ERROR(write_cooldown_meta(dest_fs, cooldown_meta_id, new_rowset_meta, {}));

    RowsetSharedPtr new_rowset;
    RowsetFactory::create_rowset(_schema, _tablet_path, new_rowset_meta, &new_rowset);

    {
        std::unique_lock meta_wlock(_meta_lock);
        if (tablet_state() == TABLET_RUNNING) {
            delete_rowsets({std::move(old_rowset)}, false);
            add_rowsets({std::move(new_rowset)});
            // TODO(plat1ko): process primary key
            _tablet_meta->set_cooldown_meta_id(cooldown_meta_id);
        }
    }
    {
        std::unique_lock meta_rlock(_meta_lock);
        save_meta();
    }
    return Status::OK();
}

Status Tablet::_read_cooldown_meta(const std::shared_ptr<io::RemoteFileSystem>& fs,
                                   int64_t cooldown_replica_id, TabletMetaPB* tablet_meta_pb) {
    std::string remote_meta_path =
            BetaRowset::remote_tablet_meta_path(tablet_id(), cooldown_replica_id);
    IOContext io_ctx;
    io::FileReaderSPtr tablet_meta_reader;
    RETURN_IF_ERROR(fs->open_file(remote_meta_path, &tablet_meta_reader, &io_ctx));
    auto file_size = tablet_meta_reader->size();
    size_t bytes_read;
    auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[file_size]);
    RETURN_IF_ERROR(tablet_meta_reader->read_at(0, {buf.get(), file_size}, io_ctx, &bytes_read));
    tablet_meta_reader->close();
    if (!tablet_meta_pb->ParseFromArray(buf.get(), file_size)) {
        return Status::InternalError("malformed tablet meta");
    }
    return Status::OK();
}

Status Tablet::write_cooldown_meta(const std::shared_ptr<io::RemoteFileSystem>& fs,
                                   UniqueId cooldown_meta_id,
                                   const RowsetMetaSharedPtr& new_rs_meta,
                                   const std::vector<RowsetMetaSharedPtr>& to_deletes) {
    std::unordered_set<Version, HashOfVersion> to_delete_set;
    for (auto& rs_meta : to_deletes) {
        to_delete_set.emplace(rs_meta->version());
    }

    std::vector<RowsetMetaSharedPtr> cooldowned_rs_metas;
    {
        std::shared_lock meta_rlock(_meta_lock);
        for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
            if (!rs_meta->is_local()) {
                if (to_delete_set.find(rs_meta->version()) != to_delete_set.end()) {
                    continue;
                }
                cooldowned_rs_metas.emplace_back(rs_meta);
            }
        }
    }
    cooldowned_rs_metas.emplace_back(new_rs_meta);
    std::sort(cooldowned_rs_metas.begin(), cooldowned_rs_metas.end(), RowsetMeta::comparator);

    // check_version_continuity
    if (!cooldowned_rs_metas.empty()) {
        RowsetMetaSharedPtr prev_rowset_meta = cooldowned_rs_metas.front();
        for (size_t i = 1; i < cooldowned_rs_metas.size(); ++i) {
            RowsetMetaSharedPtr rowset_meta = cooldowned_rs_metas[i];
            if (rowset_meta->start_version() != prev_rowset_meta->end_version() + 1) {
                LOG(WARNING) << "There are missed versions among rowsets. "
                             << "prev_rowset_meta version=" << prev_rowset_meta->start_version()
                             << "-" << prev_rowset_meta->end_version()
                             << ", rowset_meta version=" << rowset_meta->start_version() << "-"
                             << rowset_meta->end_version();
                return Status::Error<CUMULATIVE_MISS_VERSION>();
            }
            prev_rowset_meta = rowset_meta;
        }
    }

    TabletMetaPB tablet_meta_pb;
    auto rs_metas = tablet_meta_pb.mutable_rs_metas();
    rs_metas->Reserve(cooldowned_rs_metas.size());
    for (auto& rs_meta : cooldowned_rs_metas) {
        rs_metas->Add(rs_meta->get_rowset_pb());
    }
    tablet_meta_pb.mutable_cooldown_meta_id()->set_hi(cooldown_meta_id.hi);
    tablet_meta_pb.mutable_cooldown_meta_id()->set_lo(cooldown_meta_id.lo);

    std::string remote_meta_path =
            BetaRowset::remote_tablet_meta_path(tablet_id(), _tablet_meta->replica_id());
    io::FileWriterPtr tablet_meta_writer;
    RETURN_IF_ERROR(fs->create_file(remote_meta_path, &tablet_meta_writer));
    auto val = tablet_meta_pb.SerializeAsString();
    RETURN_IF_ERROR(tablet_meta_writer->append({val.data(), val.size()}));
    return tablet_meta_writer->close();
}

Status Tablet::_follow_cooldowned_data(const std::shared_ptr<io::RemoteFileSystem>& fs,
                                       int64_t cooldown_replica_id) {
    LOG(INFO) << "try to follow cooldowned data. tablet_id=" << tablet_id()
              << " cooldown_replica_id=" << cooldown_replica_id
              << " local replica=" << replica_id();
    // MUST executing serially with cold data compaction, because compaction input rowsets may be deleted by this function
    std::unique_lock cold_compaction_lock(_cold_compaction_lock, std::try_to_lock);
    if (!cold_compaction_lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>("try cold_compaction_lock failed");
    }

    TabletMetaPB cooldown_meta_pb;
    RETURN_IF_ERROR(_read_cooldown_meta(fs, cooldown_replica_id, &cooldown_meta_pb));
    DCHECK(cooldown_meta_pb.rs_metas_size() > 0);
    if (_tablet_meta->cooldown_meta_id() == cooldown_meta_pb.cooldown_meta_id()) {
        // cooldowned rowsets are same, no need to follow
        return Status::OK();
    }

    int64_t cooldowned_version = cooldown_meta_pb.rs_metas().rbegin()->end_version();

    std::vector<RowsetSharedPtr> overlap_rowsets;
    bool version_aligned = false;

    {
        std::lock_guard wlock(_meta_lock);
        if (tablet_state() != TABLET_RUNNING) {
            return Status::InternalError("tablet not running");
        }

        for (auto& [v, rs] : _rs_version_map) {
            if (v.second == cooldowned_version) {
                version_aligned = true;
                break;
            }
        }
        if (!version_aligned) {
            return Status::InternalError("cooldowned version is not aligned");
        }
        for (auto& [v, rs] : _rs_version_map) {
            if (v.second <= cooldowned_version) {
                overlap_rowsets.push_back(rs);
            } else if (!rs->is_local()) {
                return Status::InternalError("cooldowned version larger than that to follow");
            }
        }
        std::sort(overlap_rowsets.begin(), overlap_rowsets.end(), Rowset::comparator);
        auto rs_pb_it = cooldown_meta_pb.rs_metas().begin();
        auto rs_it = overlap_rowsets.begin();
        for (; rs_pb_it != cooldown_meta_pb.rs_metas().end() && rs_it != overlap_rowsets.end();
             ++rs_pb_it, ++rs_it) {
            // skip cooldowned rowset with same version in BE
            if ((*rs_it)->is_local() || rs_pb_it->end_version() != (*rs_it)->end_version()) {
                break;
            }
        }
        std::vector<RowsetSharedPtr> to_delete(rs_it, overlap_rowsets.end());
        std::vector<RowsetSharedPtr> to_add;
        to_add.reserve(cooldown_meta_pb.rs_metas().end() - rs_pb_it);
        for (; rs_pb_it != cooldown_meta_pb.rs_metas().end(); ++rs_pb_it) {
            auto rs_meta = std::make_shared<RowsetMeta>();
            rs_meta->init_from_pb(*rs_pb_it);
            RowsetSharedPtr rs;
            RowsetFactory::create_rowset(_schema, _tablet_path, std::move(rs_meta), &rs);
            to_add.push_back(std::move(rs));
        }
        // Note: We CANNOT call `modify_rowsets` here because `modify_rowsets` cannot process version graph correctly.
        delete_rowsets(to_delete, false);
        add_rowsets(to_add);
        // TODO(plat1ko): process primary key
        _tablet_meta->set_cooldown_meta_id(cooldown_meta_pb.cooldown_meta_id());
    }
    {
        std::lock_guard rlock(_meta_lock);
        save_meta();
    }

    return Status::OK();
}

RowsetSharedPtr Tablet::pick_cooldown_rowset() {
    RowsetSharedPtr rowset;
    // TODO(plat1ko): should we maintain `cooldowned_version` in `Tablet`?
    int64_t cooldowned_version = -1;
    // We pick the rowset with smallest start version in local.
    int64_t min_local_version = std::numeric_limits<int64_t>::max();
    {
        std::shared_lock meta_rlock(_meta_lock);
        for (auto& [v, rs] : _rs_version_map) {
            if (!rs->is_local()) {
                cooldowned_version = std::max(cooldowned_version, v.second);
            } else if (v.first < min_local_version) { // this is a local rowset
                min_local_version = v.first;
                rowset = rs;
            }
        }
    }
    if (!rowset) {
        return nullptr;
    }
    if (min_local_version != cooldowned_version + 1) { // ensure version continuity
        if (UNLIKELY(cooldowned_version != -1)) {
            LOG(WARNING) << "version not continuous. tablet_id=" << tablet_id()
                         << " cooldowned_version=" << cooldowned_version
                         << " min_local_version=" << min_local_version;
        }
        return nullptr;
    }
    return rowset;
}

bool Tablet::need_cooldown(int64_t* cooldown_timestamp, size_t* file_size) {
    int64_t id = storage_policy_id();
    if (id <= 0) {
        VLOG_DEBUG << "tablet does not need cooldown, tablet id: " << tablet_id();
        return false;
    }
    auto storage_policy = get_storage_policy(id);
    if (!storage_policy) {
        LOG(WARNING) << "Cannot get storage policy: " << id;
        return false;
    }
    auto cooldown_ttl_sec = storage_policy->cooldown_ttl;
    auto cooldown_datetime = storage_policy->cooldown_datetime;
    RowsetSharedPtr rowset = pick_cooldown_rowset();
    if (!rowset) {
        VLOG_DEBUG << "pick cooldown rowset, get null, tablet id: " << tablet_id();
        return false;
    }

    int64_t newest_cooldown_time = std::numeric_limits<int64_t>::max();
    if (cooldown_ttl_sec >= 0) {
        newest_cooldown_time = rowset->newest_write_timestamp() + cooldown_ttl_sec;
    }
    if (cooldown_datetime > 0) {
        newest_cooldown_time = std::min(newest_cooldown_time, cooldown_datetime);
    }

    // the rowset should do cooldown job only if it's cooldown ttl plus newest write time is less than
    // current time or it's datatime is less than current time
    if (newest_cooldown_time < UnixSeconds()) {
        *cooldown_timestamp = newest_cooldown_time;
        *file_size = rowset->data_disk_size();
        VLOG_DEBUG << "tablet need cooldown, tablet id: " << tablet_id()
                   << " file_size: " << *file_size;
        return true;
    }

    VLOG_DEBUG << "tablet does not need cooldown, tablet id: " << tablet_id()
               << " ttl sec: " << cooldown_ttl_sec << " cooldown datetime: " << cooldown_datetime
               << " newest write time: " << rowset->newest_write_timestamp();
    return false;
}

void Tablet::record_unused_remote_rowset(const RowsetId& rowset_id, const std::string& resource,
                                         int64_t num_segments) {
    auto gc_key = REMOTE_ROWSET_GC_PREFIX + rowset_id.to_string();
    RemoteRowsetGcPB gc_pb;
    gc_pb.set_resource_id(resource);
    gc_pb.set_tablet_id(tablet_id());
    gc_pb.set_num_segments(num_segments);
    auto st =
            _data_dir->get_meta()->put(META_COLUMN_FAMILY_INDEX, gc_key, gc_pb.SerializeAsString());
    if (!st.ok()) {
        LOG(WARNING) << "failed to record unused remote rowset. tablet_id=" << tablet_id()
                     << " rowset_id=" << rowset_id << " resource_id=" << resource;
    }
}

Status Tablet::remove_all_remote_rowsets() {
    DCHECK(_state == TABLET_SHUTDOWN);
    std::set<std::string> resource_ids;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (!rs_meta->is_local()) {
            resource_ids.insert(rs_meta->resource_id());
        }
    }
    if (resource_ids.empty()) {
        return Status::OK();
    }
    auto tablet_gc_key = REMOTE_TABLET_GC_PREFIX + std::to_string(tablet_id());
    RemoteTabletGcPB gc_pb;
    for (auto& resource_id : resource_ids) {
        gc_pb.add_resource_ids(resource_id);
    }
    return _data_dir->get_meta()->put(META_COLUMN_FAMILY_INDEX, tablet_gc_key,
                                      gc_pb.SerializeAsString());
}

void Tablet::remove_unused_remote_files() {
    auto tablets = StorageEngine::instance()->tablet_manager()->get_all_tablet([](Tablet* t) {
        return t->tablet_meta()->cooldown_meta_id().initialized() && t->is_used() &&
               t->tablet_state() == TABLET_RUNNING;
    });
    TConfirmUnusedRemoteFilesRequest req;
    req.__isset.confirm_list = true;
    // tablet_id -> [fs, unused_remote_files]
    using unused_remote_files_buffer_t = std::unordered_map<
            int64_t, std::pair<std::shared_ptr<io::RemoteFileSystem>, std::vector<io::Path>>>;
    unused_remote_files_buffer_t buffer;
    int64_t num_files_in_buffer = 0;
    // assume a filename is 0.1KB, buffer size should not larger than 100MB
    constexpr int64_t max_files_in_buffer = 1000000;

    auto calc_unused_remote_files = [&req, &buffer, &num_files_in_buffer](Tablet* t) {
        auto storage_policy = get_storage_policy(t->storage_policy_id());
        if (storage_policy == nullptr) {
            LOG(WARNING) << "could not find storage_policy, storage_policy_id="
                         << t->storage_policy_id();
            return;
        }
        auto resource = get_storage_resource(storage_policy->resource_id);
        auto dest_fs = std::static_pointer_cast<io::RemoteFileSystem>(resource.fs);
        if (dest_fs == nullptr) {
            LOG(WARNING) << "could not find resource, resouce_id=" << storage_policy->resource_id;
            return;
        }
        DCHECK(atol(dest_fs->id().c_str()) == storage_policy->resource_id);
        DCHECK(dest_fs->type() != io::FileSystemType::LOCAL);

        Status st;
        std::vector<io::Path> files;
        {
            std::unique_lock xlock(t->_remote_files_lock, std::try_to_lock);
            if (!xlock.owns_lock()) {
                LOG(WARNING) << "try remote_files_lock failed. tablet_id=" << t->tablet_id();
                return;
            }
            // FIXME(plat1ko): What if user reset resource in storage policy to another resource?
            //  Maybe we should also list files in previously uploaded resources.
            st = dest_fs->list(BetaRowset::remote_tablet_path(t->tablet_id()), &files);
        }
        if (!st.ok()) {
            LOG(WARNING) << "encounter error when remove unused remote files, tablet_id="
                         << t->tablet_id() << " : " << st;
        }
        if (files.empty()) {
            return;
        }
        // get all cooldowned rowsets
        std::unordered_set<std::string> cooldowned_rowsets;
        UniqueId cooldown_meta_id;
        {
            std::shared_lock rlock(t->_meta_lock);
            for (auto& rs_meta : t->_tablet_meta->all_rs_metas()) {
                if (!rs_meta->is_local()) {
                    cooldowned_rowsets.insert(rs_meta->rowset_id().to_string());
                }
            }
            cooldown_meta_id = t->_tablet_meta->cooldown_meta_id();
        }
        // {replica_id}.meta
        std::string remote_meta_path = std::to_string(t->replica_id()) + ".meta";
        // filter out the paths that should be reserved
        // clang-format off
        files.erase(std::remove_if(files.begin(), files.end(), [&](io::Path& path) {
            const std::string& path_str = path.native();
            if (StringPiece(path_str).ends_with(".meta")) {
                return path_str == remote_meta_path;
            }
            if (StringPiece(path_str).ends_with(".dat")) {
                // extract rowset id. filename format: {rowset_id}_{segment_num}.dat
                auto end = path_str.rfind('_');
                if (UNLIKELY(end == std::string::npos)) {
                    return false;
                }
                return !!cooldowned_rowsets.count(path_str.substr(0, end)); 
            }
            if (StringPiece(path_str).ends_with(".idx")) {
                // extract rowset id. filename format: {rowset_id}_{segment_num}_{index_id}.idx
                auto end = path_str.find('_');
                if (UNLIKELY(end == std::string::npos)) {
                    return false;
                }
                return !!cooldowned_rowsets.count(path_str.substr(0, end));
            }
            return false;
        }), files.end());
        // clang-format on
        if (files.empty()) {
            return;
        }
        files.shrink_to_fit();
        num_files_in_buffer += files.size();
        buffer.insert({t->tablet_id(), {std::move(dest_fs), std::move(files)}});
        auto& info = req.confirm_list.emplace_back();
        info.__set_tablet_id(t->tablet_id());
        info.__set_cooldown_replica_id(t->replica_id());
        info.__set_cooldown_meta_id(cooldown_meta_id.to_thrift());
    };

    auto confirm_and_remove_files = [&buffer, &req, &num_files_in_buffer]() {
        TConfirmUnusedRemoteFilesResult result;
        LOG(INFO) << "begin to confirm unused remote files. num_tablets=" << buffer.size()
                  << " num_files=" << num_files_in_buffer;
        auto st = MasterServerClient::instance()->confirm_unused_remote_files(req, &result);
        if (!st.ok()) {
            LOG(WARNING) << st;
            return;
        }
        for (auto id : result.confirmed_tablets) {
            if (auto it = buffer.find(id); LIKELY(it != buffer.end())) {
                auto& fs = it->second.first;
                auto& files = it->second.second;
                // delete unused files
                LOG(INFO) << "delete unused files. root_path=" << fs->root_path()
                          << " tablet_id=" << id;
                io::Path dir("data/" + std::to_string(id));
                for (auto& file : files) {
                    file = dir / file;
                    LOG(INFO) << "delete unused file: " << file.native();
                }
                st = fs->batch_delete(files);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to delete unused files, tablet_id=" << id << " : "
                                 << st;
                }
            }
        }
    };

    // batch confirm to reduce FE's overhead
    auto next_confirm_time = std::chrono::steady_clock::now() +
                             std::chrono::seconds(config::confirm_unused_remote_files_interval_sec);
    for (auto& t : tablets) {
        if (t.use_count() <= 1 // this means tablet has been dropped
            || t->_cooldown_replica_id != t->replica_id() || t->_state != TABLET_RUNNING) {
            continue;
        }
        calc_unused_remote_files(t.get());
        if (num_files_in_buffer > 0 && (num_files_in_buffer > max_files_in_buffer ||
                                        std::chrono::steady_clock::now() > next_confirm_time)) {
            confirm_and_remove_files();
            buffer.clear();
            req.confirm_list.clear();
            num_files_in_buffer = 0;
            next_confirm_time =
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds(config::confirm_unused_remote_files_interval_sec);
        }
    }
    if (num_files_in_buffer > 0) {
        confirm_and_remove_files();
    }
}

TabletSchemaSPtr Tablet::tablet_schema() const {
    std::shared_lock wrlock(_meta_lock);
    return _max_version_schema;
}

void Tablet::update_max_version_schema(const TabletSchemaSPtr& tablet_schema) {
    std::lock_guard wrlock(_meta_lock);
    // Double Check for concurrent update
    if (!_max_version_schema ||
        tablet_schema->schema_version() > _max_version_schema->schema_version()) {
        _max_version_schema = tablet_schema;
    }
}

TabletSchemaSPtr Tablet::get_max_version_schema(std::lock_guard<std::shared_mutex>&) {
    return _max_version_schema;
}

Status Tablet::lookup_row_data(const Slice& encoded_key, const RowLocation& row_location,
                               RowsetSharedPtr input_rowset, const TupleDescriptor* desc,
                               vectorized::Block* block, bool write_to_cache) {
    // read row data
    BetaRowsetSharedPtr rowset = std::static_pointer_cast<BetaRowset>(input_rowset);
    if (!rowset) {
        return Status::NotFound(
                fmt::format("rowset {} not found", row_location.rowset_id.to_string()));
    }

    const TabletSchemaSPtr tablet_schema = rowset->tablet_schema();
    SegmentCacheHandle segment_cache;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(rowset, &segment_cache, true));
    // find segment
    auto it = std::find_if(segment_cache.get_segments().begin(), segment_cache.get_segments().end(),
                           [&row_location](const segment_v2::SegmentSharedPtr& seg) {
                               return seg->id() == row_location.segment_id;
                           });
    if (it == segment_cache.get_segments().end()) {
        return Status::NotFound(fmt::format("rowset {} 's segemnt not found, seg_id {}",
                                            row_location.rowset_id.to_string(),
                                            row_location.segment_id));
    }
    // read from segment column by column, row by row
    segment_v2::SegmentSharedPtr segment = *it;
    size_t row_size = 0;
    MonotonicStopWatch watch;
    watch.start();
    Defer _defer([&]() {
        LOG_EVERY_N(INFO, 500) << "get a single_row, cost(us):" << watch.elapsed_time() / 1000
                               << ", row_size:" << row_size;
    });
    if (tablet_schema->store_row_column()) {
        // create _source column
        segment_v2::ColumnIterator* column_iterator = nullptr;
        RETURN_IF_ERROR(segment->new_column_iterator(tablet_schema->column(BeConsts::ROW_STORE_COL),
                                                     &column_iterator));
        std::unique_ptr<segment_v2::ColumnIterator> ptr_guard(column_iterator);
        segment_v2::ColumnIteratorOptions opt;
        OlapReaderStatistics stats;
        opt.file_reader = segment->file_reader().get();
        opt.stats = &stats;
        opt.use_page_cache = !config::disable_storage_page_cache;
        column_iterator->init(opt);
        // get and parse tuple row
        vectorized::MutableColumnPtr column_ptr = vectorized::ColumnString::create();
        std::vector<segment_v2::rowid_t> rowids {
                static_cast<segment_v2::rowid_t>(row_location.row_id)};
        RETURN_IF_ERROR(column_iterator->read_by_rowids(rowids.data(), 1, column_ptr));
        assert(column_ptr->size() == 1);
        auto string_column = static_cast<vectorized::ColumnString*>(column_ptr.get());
        if (write_to_cache) {
            StringRef value = string_column->get_data_at(0);
            RowCache::instance()->insert({tablet_id(), encoded_key},
                                         Slice {value.data, value.size});
        }
        vectorized::JsonbSerializeUtil::jsonb_to_block(*desc, *string_column, *block);
        return Status::OK();
    }
    __builtin_unreachable();
}

Status Tablet::lookup_row_key(const Slice& encoded_key, const RowsetIdUnorderedSet* rowset_ids,
                              RowLocation* row_location, uint32_t version,
                              RowsetSharedPtr* rowset) {
    std::vector<std::pair<RowsetSharedPtr, int32_t>> selected_rs;
    size_t seq_col_length = 0;
    if (_schema->has_sequence_col()) {
        seq_col_length = _schema->column(_schema->sequence_col_idx()).length() + 1;
    }
    Slice key_without_seq = Slice(encoded_key.get_data(), encoded_key.get_size() - seq_col_length);
    _rowset_tree->FindRowsetsWithKeyInRange(key_without_seq, rowset_ids, &selected_rs);
    if (selected_rs.empty()) {
        return Status::NotFound("No rowsets contains the key in key range");
    }
    // Usually newly written data has a higher probability of being modified, so prefer
    // to search the key in the rowset with larger version.
    std::sort(selected_rs.begin(), selected_rs.end(),
              [](std::pair<RowsetSharedPtr, int32_t>& a, std::pair<RowsetSharedPtr, int32_t>& b) {
                  if (a.first->end_version() == b.first->end_version()) {
                      return a.second > b.second;
                  }
                  return a.first->end_version() > b.first->end_version();
              });
    RowLocation loc;
    for (auto& rs : selected_rs) {
        if (rs.first->end_version() > version) {
            continue;
        }
        SegmentCacheHandle segment_cache_handle;
        RETURN_NOT_OK(SegmentLoader::instance()->load_segments(
                std::static_pointer_cast<BetaRowset>(rs.first), &segment_cache_handle, true));
        auto& segments = segment_cache_handle.get_segments();
        DCHECK_GT(segments.size(), rs.second);
        Status s = segments[rs.second]->lookup_row_key(encoded_key, &loc);
        if (s.is<NOT_FOUND>()) {
            continue;
        }
        if (!s.ok()) {
            return s;
        }
        loc.rowset_id = rs.first->rowset_id();
        if (_tablet_meta->delete_bitmap().contains_agg({loc.rowset_id, loc.segment_id, version},
                                                       loc.row_id)) {
            // if has sequence col, we continue to compare the sequence_id of
            // all rowsets, util we find an existing key.
            if (_schema->has_sequence_col()) {
                continue;
            }
            // The key is deleted, we don't need to search for it any more.
            break;
        }
        *row_location = loc;
        if (rowset) {
            // return it's rowset
            *rowset = rs.first;
        }
        // find it and return
        return s;
    }
    return Status::NotFound("can't find key in all rowsets");
}

// load segment may do io so it should out lock
Status Tablet::_load_rowset_segments(const RowsetSharedPtr& rowset,
                                     std::vector<segment_v2::SegmentSharedPtr>* segments) {
    auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
    RETURN_IF_ERROR(beta_rowset->load_segments(segments));
    return Status::OK();
}

// caller should hold meta_lock
Status Tablet::calc_delete_bitmap(RowsetId rowset_id,
                                  const std::vector<segment_v2::SegmentSharedPtr>& segments,
                                  const RowsetIdUnorderedSet* specified_rowset_ids,
                                  DeleteBitmapPtr delete_bitmap, int64_t end_version,
                                  bool check_pre_segments) {
    std::vector<segment_v2::SegmentSharedPtr> pre_segments;
    OlapStopWatch watch;

    Version dummy_version(end_version + 1, end_version + 1);
    for (auto& seg : segments) {
        seg->load_pk_index_and_bf(); // We need index blocks to iterate
        auto pk_idx = seg->get_primary_key_index();
        int total = pk_idx->num_rows();
        uint32_t row_id = 0;
        int32_t remaining = total;
        bool exact_match = false;
        std::string last_key;
        int batch_size = 1024;
        while (remaining > 0) {
            std::unique_ptr<segment_v2::IndexedColumnIterator> iter;
            RETURN_IF_ERROR(pk_idx->new_iterator(&iter));

            size_t num_to_read = std::min(batch_size, remaining);
            auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                    pk_idx->type_info()->type(), 1, 0);
            auto index_column = index_type->create_column();
            Slice last_key_slice(last_key);
            RETURN_IF_ERROR(iter->seek_at_or_after(&last_key_slice, &exact_match));

            size_t num_read = num_to_read;
            RETURN_IF_ERROR(iter->next_batch(&num_read, index_column));
            DCHECK(num_to_read == num_read);
            last_key = index_column->get_data_at(num_read - 1).to_string();

            // exclude last_key, last_key will be read in next batch.
            if (num_read == batch_size && num_read != remaining) {
                num_read -= 1;
            }
            for (size_t i = 0; i < num_read; i++) {
                Slice key =
                        Slice(index_column->get_data_at(i).data, index_column->get_data_at(i).size);
                RowLocation loc;
                // first check if exist in pre segment
                if (check_pre_segments) {
                    auto st = _check_pk_in_pre_segments(rowset_id, pre_segments, key, dummy_version,
                                                        delete_bitmap, &loc);
                    if (st.ok()) {
                        delete_bitmap->add({rowset_id, loc.segment_id, dummy_version.first},
                                           loc.row_id);
                        ++row_id;
                        continue;
                    } else if (st.is<ALREADY_EXIST>()) {
                        delete_bitmap->add({rowset_id, seg->id(), dummy_version.first}, row_id);
                        ++row_id;
                        continue;
                    }
                }

                if (specified_rowset_ids != nullptr && !specified_rowset_ids->empty()) {
                    auto st = lookup_row_key(key, specified_rowset_ids, &loc,
                                             dummy_version.first - 1);
                    CHECK(st.ok() || st.is<NOT_FOUND>() || st.is<ALREADY_EXIST>());
                    if (st.is<NOT_FOUND>()) {
                        ++row_id;
                        continue;
                    }

                    // sequence id smaller than the previous one, so delete current row
                    if (st.is<ALREADY_EXIST>()) {
                        loc.rowset_id = rowset_id;
                        loc.segment_id = seg->id();
                        loc.row_id = row_id;
                    }

                    // delete bitmap will be calculate when memtable flush and
                    // publish. The two stages may see different versions.
                    // When there is sequence column, the currently imported data
                    // of rowset may be marked for deletion at memtablet flush or
                    // publish because the seq column is smaller than the previous
                    // rowset.
                    // just set 0 as a unified temporary version number, and update to
                    // the real version number later.
                    delete_bitmap->add({loc.rowset_id, loc.segment_id, 0}, loc.row_id);
                }
                ++row_id;
            }
            remaining -= num_read;
        }
        if (check_pre_segments) {
            pre_segments.emplace_back(seg);
        }
    }
    LOG(INFO) << "construct delete bitmap tablet: " << tablet_id() << " rowset: " << rowset_id
              << " dummy_version: " << dummy_version
              << "bitmap num: " << delete_bitmap->delete_bitmap.size()
              << " cost: " << watch.get_elapse_time_us() << "(us)";
    return Status::OK();
}

Status Tablet::_check_pk_in_pre_segments(
        RowsetId rowset_id, const std::vector<segment_v2::SegmentSharedPtr>& pre_segments,
        const Slice& key, const Version& version, DeleteBitmapPtr delete_bitmap, RowLocation* loc) {
    for (auto it = pre_segments.rbegin(); it != pre_segments.rend(); ++it) {
        auto st = (*it)->lookup_row_key(key, loc);
        CHECK(st.ok() || st.is<NOT_FOUND>() || st.is<ALREADY_EXIST>());
        if (st.is<NOT_FOUND>()) {
            continue;
        } else if (st.ok() && _schema->has_sequence_col() &&
                   delete_bitmap->contains({rowset_id, loc->segment_id, version.first},
                                           loc->row_id)) {
            // if has sequence col, we continue to compare the sequence_id of
            // all segments, util we find an existing key.
            continue;
        }
        return st;
    }
    return Status::NotFound("Can't find key in the segment");
}

void Tablet::_rowset_ids_difference(const RowsetIdUnorderedSet& cur,
                                    const RowsetIdUnorderedSet& pre, RowsetIdUnorderedSet* to_add,
                                    RowsetIdUnorderedSet* to_del) {
    for (const auto& id : cur) {
        if (pre.find(id) == pre.end()) {
            to_add->insert(id);
        }
    }
    for (const auto& id : pre) {
        if (cur.find(id) == cur.end()) {
            to_del->insert(id);
        }
    }
}

// The caller should hold _rowset_update_lock and _meta_lock lock.
Status Tablet::update_delete_bitmap_without_lock(const RowsetSharedPtr& rowset) {
    int64_t cur_version = rowset->start_version();
    std::vector<segment_v2::SegmentSharedPtr> segments;
    _load_rowset_segments(rowset, &segments);

    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(tablet_id());
    RETURN_IF_ERROR(calc_delete_bitmap(rowset->rowset_id(), segments, nullptr, delete_bitmap,
                                       cur_version - 1, true));

    for (auto iter = delete_bitmap->delete_bitmap.begin();
         iter != delete_bitmap->delete_bitmap.end(); ++iter) {
        int ret = _tablet_meta->delete_bitmap().set(
                {std::get<0>(iter->first), std::get<1>(iter->first), cur_version}, iter->second);
        DCHECK(ret == 1);
        if (ret != 1) {
            LOG(INFO) << "failed to set delete bimap, key is: |" << std::get<0>(iter->first) << "|"
                      << std::get<1>(iter->first) << "|" << cur_version;
            return Status::InternalError("failed to set delete bimap");
        }
    }

    return Status::OK();
}

Status Tablet::update_delete_bitmap(const RowsetSharedPtr& rowset, DeleteBitmapPtr delete_bitmap,
                                    const RowsetIdUnorderedSet& pre_rowset_ids) {
    RowsetIdUnorderedSet cur_rowset_ids;
    RowsetIdUnorderedSet rowset_ids_to_add;
    RowsetIdUnorderedSet rowset_ids_to_del;
    int64_t cur_version = rowset->start_version();

    std::vector<segment_v2::SegmentSharedPtr> segments;
    _load_rowset_segments(rowset, &segments);

    std::lock_guard<std::mutex> rwlock(_rowset_update_lock);
    std::shared_lock meta_rlock(_meta_lock);
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(tablet_id())) {
        LOG(INFO) << "tablet is under alter process, update delete bitmap later, tablet_id="
                  << tablet_id();
        return Status::OK();
    }
    cur_rowset_ids = all_rs_id(cur_version - 1);
    _rowset_ids_difference(cur_rowset_ids, pre_rowset_ids, &rowset_ids_to_add, &rowset_ids_to_del);
    if (!rowset_ids_to_add.empty() || !rowset_ids_to_del.empty()) {
        LOG(INFO) << "rowset_ids_to_add: " << rowset_ids_to_add.size()
                  << ", rowset_ids_to_del: " << rowset_ids_to_del.size();
    }
    for (const auto& to_del : rowset_ids_to_del) {
        delete_bitmap->remove({to_del, 0, 0}, {to_del, UINT32_MAX, INT64_MAX});
    }

    RETURN_IF_ERROR(calc_delete_bitmap(rowset->rowset_id(), segments, &rowset_ids_to_add,
                                       delete_bitmap, cur_version - 1, true));

    // update version without write lock, compaction and publish_txn
    // will update delete bitmap, handle compaction with _rowset_update_lock
    // and publish_txn runs sequential so no need to lock here
    for (auto iter = delete_bitmap->delete_bitmap.begin();
         iter != delete_bitmap->delete_bitmap.end(); ++iter) {
        int ret = _tablet_meta->delete_bitmap().set(
                {std::get<0>(iter->first), std::get<1>(iter->first), cur_version}, iter->second);
        DCHECK(ret == 1);
        if (ret != 1) {
            LOG(INFO) << "failed to set delete bimap, key is: |" << std::get<0>(iter->first) << "|"
                      << std::get<1>(iter->first) << "|" << cur_version;
            return Status::InternalError("failed to set delete bimap");
        }
    }

    return Status::OK();
}

void Tablet::calc_compaction_output_rowset_delete_bitmap(
        const std::vector<RowsetSharedPtr>& input_rowsets, const RowIdConversion& rowid_conversion,
        uint64_t start_version, uint64_t end_version,
        std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>>* location_map,
        DeleteBitmap* output_rowset_delete_bitmap) {
    RowLocation src;
    RowLocation dst;
    for (auto& rowset : input_rowsets) {
        src.rowset_id = rowset->rowset_id();
        for (uint32_t seg_id = 0; seg_id < rowset->num_segments(); ++seg_id) {
            src.segment_id = seg_id;
            DeleteBitmap subset_map(tablet_id());
            _tablet_meta->delete_bitmap().subset({rowset->rowset_id(), seg_id, start_version},
                                                 {rowset->rowset_id(), seg_id, end_version},
                                                 &subset_map);
            // traverse all versions and convert rowid
            for (auto iter = subset_map.delete_bitmap.begin();
                 iter != subset_map.delete_bitmap.end(); ++iter) {
                auto cur_version = std::get<2>(iter->first);
                for (auto index = iter->second.begin(); index != iter->second.end(); ++index) {
                    src.row_id = *index;
                    if (rowid_conversion.get(src, &dst) != 0) {
                        VLOG_CRITICAL << "Can't find rowid, may be deleted by the delete_handler, "
                                      << " src loaction: |" << src.rowset_id << "|"
                                      << src.segment_id << "|" << src.row_id
                                      << " version: " << cur_version;
                        continue;
                    }
                    VLOG_DEBUG << "calc_compaction_output_rowset_delete_bitmap dst location: |"
                               << dst.rowset_id << "|" << dst.segment_id << "|" << dst.row_id
                               << " src location: |" << src.rowset_id << "|" << src.segment_id
                               << "|" << src.row_id << " start version: " << start_version
                               << "end version" << end_version;
                    (*location_map)[rowset].emplace_back(src, dst);
                    output_rowset_delete_bitmap->add({dst.rowset_id, dst.segment_id, cur_version},
                                                     dst.row_id);
                }
            }
        }
    }
}

void Tablet::merge_delete_bitmap(const DeleteBitmap& delete_bitmap) {
    _tablet_meta->delete_bitmap().merge(delete_bitmap);
}

Status Tablet::check_rowid_conversion(
        RowsetSharedPtr dst_rowset,
        const std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>>&
                location_map) {
    if (location_map.empty()) {
        VLOG_DEBUG << "check_rowid_conversion, location_map is empty";
        return Status::OK();
    }
    std::vector<segment_v2::SegmentSharedPtr> dst_segments;
    _load_rowset_segments(dst_rowset, &dst_segments);
    std::unordered_map<RowsetId, std::vector<segment_v2::SegmentSharedPtr>, HashOfRowsetId>
            input_rowsets_segment;

    VLOG_DEBUG << "check_rowid_conversion, dst_segments size: " << dst_segments.size();
    for (auto [src_rowset, locations] : location_map) {
        std::vector<segment_v2::SegmentSharedPtr>& segments =
                input_rowsets_segment[src_rowset->rowset_id()];
        if (segments.empty()) {
            _load_rowset_segments(src_rowset, &segments);
        }
        for (auto& [src, dst] : locations) {
            std::string src_key;
            std::string dst_key;
            Status s = segments[src.segment_id]->read_key_by_rowid(src.row_id, &src_key);
            if (UNLIKELY(s.is<NOT_IMPLEMENTED_ERROR>())) {
                LOG(INFO) << "primary key index of old version does not "
                             "support reading key by rowid";
                break;
            }
            if (UNLIKELY(!s)) {
                LOG(WARNING) << "failed to get src key: |" << src.rowset_id << "|" << src.segment_id
                             << "|" << src.row_id << " status: " << s;
                DCHECK(false);
                return s;
            }

            s = dst_segments[dst.segment_id]->read_key_by_rowid(dst.row_id, &dst_key);
            if (UNLIKELY(!s)) {
                LOG(WARNING) << "failed to get dst key: |" << dst.rowset_id << "|" << dst.segment_id
                             << "|" << dst.row_id << " status: " << s;
                DCHECK(false);
                return s;
            }

            VLOG_DEBUG << "check_rowid_conversion, src: |" << src.rowset_id << "|" << src.segment_id
                       << "|" << src.row_id << "|" << src_key << " dst: |" << dst.rowset_id << "|"
                       << dst.segment_id << "|" << dst.row_id << "|" << dst_key;
            if (UNLIKELY(src_key.compare(dst_key) != 0)) {
                LOG(WARNING) << "failed to check key, src key: |" << src.rowset_id << "|"
                             << src.segment_id << "|" << src.row_id << "|" << src_key
                             << " dst key: |" << dst.rowset_id << "|" << dst.segment_id << "|"
                             << dst.row_id << "|" << dst_key;
                DCHECK(false);
                return Status::InternalError("failed to check rowid conversion");
            }
        }
    }
    return Status::OK();
}

RowsetIdUnorderedSet Tablet::all_rs_id(int64_t max_version) const {
    RowsetIdUnorderedSet rowset_ids;
    for (const auto& rs_it : _rs_version_map) {
        if (rs_it.first.second <= max_version) {
            rowset_ids.insert(rs_it.second->rowset_id());
        }
    }
    return rowset_ids;
}

bool Tablet::check_all_rowset_segment() {
    for (auto& version_rowset : _rs_version_map) {
        RowsetSharedPtr rowset = version_rowset.second;
        if (!rowset->check_rowset_segment()) {
            LOG(WARNING) << "Tablet Segment Check. find a bad tablet, tablet_id=" << tablet_id();
            return false;
        }
    }
    return true;
}

void Tablet::set_skip_compaction(bool skip, CompactionType compaction_type, int64_t start) {
    if (!skip) {
        _skip_cumu_compaction = false;
        _skip_base_compaction = false;
        return;
    }
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        _skip_cumu_compaction = true;
        _skip_cumu_compaction_ts = start;
    } else {
        DCHECK(compaction_type == CompactionType::BASE_COMPACTION);
        _skip_base_compaction = true;
        _skip_base_compaction_ts = start;
    }
}

bool Tablet::should_skip_compaction(CompactionType compaction_type, int64_t now) {
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION && _skip_cumu_compaction &&
        now < _skip_cumu_compaction_ts + 120) {
        return true;
    } else if (compaction_type == CompactionType::BASE_COMPACTION && _skip_base_compaction &&
               now < _skip_base_compaction_ts + 120) {
        return true;
    }
    return false;
}

} // namespace doris
