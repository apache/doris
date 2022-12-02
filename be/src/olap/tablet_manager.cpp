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

#include "olap/tablet_manager.h"

#include <gen_cpp/Types_types.h>
#include <rapidjson/document.h>
#include <re2/re2.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>

#include "env/env.h"
#include "env/env_util.h"
#include "gutil/strings/strcat.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/schema_change.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/file_utils.h"
#include "util/histogram.h"
#include "util/path_util.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"
#include "util/trace.h"

using std::list;
using std::map;
using std::set;
using std::string;
using std::vector;
using strings::Substitute;

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(tablet_meta_mem_consumption, MetricUnit::BYTES, "",
                                   mem_consumption, Labels({{"type", "tablet_meta"}}));

TabletManager::TabletManager(int32_t tablet_map_lock_shard_size)
        : _mem_tracker(std::make_shared<MemTracker>("TabletManager")),
          _tablets_shards_size(tablet_map_lock_shard_size),
          _tablets_shards_mask(tablet_map_lock_shard_size - 1) {
    CHECK_GT(_tablets_shards_size, 0);
    CHECK_EQ(_tablets_shards_size & _tablets_shards_mask, 0);
    _tablets_shards.resize(_tablets_shards_size);
    REGISTER_HOOK_METRIC(tablet_meta_mem_consumption,
                         [this]() { return _mem_tracker->consumption(); });
}

TabletManager::~TabletManager() {
    DEREGISTER_HOOK_METRIC(tablet_meta_mem_consumption);
}

Status TabletManager::_add_tablet_unlocked(TTabletId tablet_id, const TabletSharedPtr& tablet,
                                           bool update_meta, bool force) {
    Status res = Status::OK();
    VLOG_NOTICE << "begin to add tablet to TabletManager. "
                << "tablet_id=" << tablet_id << ", force=" << force;

    TabletSharedPtr existed_tablet = nullptr;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    const auto& iter = tablet_map.find(tablet_id);
    if (iter != tablet_map.end()) {
        existed_tablet = iter->second;
    }

    if (existed_tablet == nullptr) {
        return _add_tablet_to_map_unlocked(tablet_id, tablet, update_meta, false /*keep_files*/,
                                           false /*drop_old*/);
    }
    // During restore process, the tablet is exist and snapshot loader will replace the tablet's rowsets
    // and then reload the tablet, the tablet's path will the same
    if (!force) {
        if (existed_tablet->tablet_path() == tablet->tablet_path()) {
            LOG(WARNING) << "add the same tablet twice! tablet_id=" << tablet_id
                         << ", tablet_path=" << tablet->tablet_path();
            return Status::OLAPInternalError(OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE);
        }
        if (existed_tablet->data_dir() == tablet->data_dir()) {
            LOG(WARNING) << "add tablet with same data dir twice! tablet_id=" << tablet_id;
            return Status::OLAPInternalError(OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE);
        }
    }

    // During storage migration, the tablet is moved to another disk, have to check
    // if the new tablet's rowset version is larger than the old one to prevent losting data during
    // migration
    int64_t old_time, new_time;
    int32_t old_version, new_version;
    {
        std::shared_lock rdlock(existed_tablet->get_header_lock());
        const RowsetSharedPtr old_rowset = existed_tablet->rowset_with_max_version();
        const RowsetSharedPtr new_rowset = tablet->rowset_with_max_version();
        // If new tablet is empty, it is a newly created schema change tablet.
        // the old tablet is dropped before add tablet. it should not exist old tablet
        if (new_rowset == nullptr) {
            // it seems useless to call unlock and return here.
            // it could prevent error when log level is changed in the future.
            LOG(FATAL) << "new tablet is empty and old tablet exists. it should not happen."
                       << " tablet_id=" << tablet_id;
            return Status::OLAPInternalError(OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE);
        }
        old_time = old_rowset == nullptr ? -1 : old_rowset->creation_time();
        new_time = new_rowset->creation_time();
        old_version = old_rowset == nullptr ? -1 : old_rowset->end_version();
        new_version = new_rowset->end_version();
    }

    // In restore process, we replace all origin files in tablet dir with
    // the downloaded snapshot files. Then we try to reload tablet header.
    // force == true means we forcibly replace the Tablet in tablet_map
    // with the new one. But if we do so, the files in the tablet dir will be
    // dropped when the origin Tablet deconstruct.
    // So we set keep_files == true to not delete files when the
    // origin Tablet deconstruct.
    // During restore process, snapshot loader
    // replaced the old tablet's rowset with new rowsets, but the tablet path is reused, if drop files
    // here, the new rowset's file will also be dropped, so use keep files here
    bool keep_files = force ? true : false;
    if (force ||
        (new_version > old_version || (new_version == old_version && new_time > old_time))) {
        // check if new tablet's meta is in store and add new tablet's meta to meta store
        res = _add_tablet_to_map_unlocked(tablet_id, tablet, update_meta, keep_files,
                                          true /*drop_old*/);
    } else {
        tablet->set_tablet_state(TABLET_SHUTDOWN);
        tablet->save_meta();
        {
            std::lock_guard<std::shared_mutex> shutdown_tablets_wrlock(_shutdown_tablets_lock);
            _shutdown_tablets.push_back(tablet);
        }
        LOG(INFO) << "set tablet to shutdown state."
                  << "tablet_id=" << tablet->tablet_id()
                  << ", tablet_path=" << tablet->tablet_path();

        res = Status::OLAPInternalError(OLAP_ERR_ENGINE_INSERT_OLD_TABLET);
    }
    LOG(WARNING) << "add duplicated tablet. force=" << force << ", res=" << res
                 << ", tablet_id=" << tablet_id << ", old_version=" << old_version
                 << ", new_version=" << new_version << ", old_time=" << old_time
                 << ", new_time=" << new_time
                 << ", old_tablet_path=" << existed_tablet->tablet_path()
                 << ", new_tablet_path=" << tablet->tablet_path();

    return res;
}

Status TabletManager::_add_tablet_to_map_unlocked(TTabletId tablet_id,
                                                  const TabletSharedPtr& tablet, bool update_meta,
                                                  bool keep_files, bool drop_old) {
    // check if new tablet's meta is in store and add new tablet's meta to meta store
    Status res = Status::OK();
    if (update_meta) {
        // call tablet save meta in order to valid the meta
        tablet->save_meta();
    }
    if (drop_old) {
        // If the new tablet is fresher than the existing one, then replace
        // the existing tablet with the new one.
        // Use default replica_id to ignore whether replica_id is match when drop tablet.
        RETURN_NOT_OK_LOG(_drop_tablet_unlocked(tablet_id, /* replica_id */ 0, keep_files, false),
                          strings::Substitute("failed to drop old tablet when add new tablet. "
                                              "tablet_id=$0",
                                              tablet_id));
    }
    // Register tablet into DataDir, so that we can manage tablet from
    // the perspective of root path.
    // Example: unregister all tables when a bad disk found.
    tablet->register_tablet_into_dir();
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map[tablet_id] = tablet;
    _add_tablet_to_partition(tablet);

    VLOG_NOTICE << "add tablet to map successfully."
                << " tablet_id=" << tablet_id;

    return res;
}

bool TabletManager::check_tablet_id_exist(TTabletId tablet_id) {
    std::shared_lock rdlock(_get_tablets_shard_lock(tablet_id));
    return _check_tablet_id_exist_unlocked(tablet_id);
}

bool TabletManager::_check_tablet_id_exist_unlocked(TTabletId tablet_id) {
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    return tablet_map.find(tablet_id) != tablet_map.end();
}

Status TabletManager::create_tablet(const TCreateTabletReq& request, std::vector<DataDir*> stores) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    DorisMetrics::instance()->create_tablet_requests_total->increment(1);

    int64_t tablet_id = request.tablet_id;
    LOG(INFO) << "begin to create tablet. tablet_id=" << tablet_id;

    std::lock_guard<std::shared_mutex> wrlock(_get_tablets_shard_lock(tablet_id));
    TRACE("got tablets shard lock");
    // Make create_tablet operation to be idempotent:
    // 1. Return true if tablet with same tablet_id and schema_hash exist;
    //           false if tablet with same tablet_id but different schema_hash exist.
    // 2. When this is an alter task, if the tablet(both tablet_id and schema_hash are
    // same) already exist, then just return true(an duplicate request). But if
    // tablet_id exist but with different schema_hash, return an error(report task will
    // eventually trigger its deletion).
    if (_get_tablet_unlocked(tablet_id) != nullptr) {
        LOG(INFO) << "success to create tablet. tablet already exist. tablet_id=" << tablet_id;
        return Status::OK();
    }

    TabletSharedPtr base_tablet = nullptr;
    bool is_schema_change = false;
    // If the CreateTabletReq has base_tablet_id then it is a alter-tablet request
    if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
        is_schema_change = true;
        base_tablet = _get_tablet_unlocked(request.base_tablet_id);
        if (base_tablet == nullptr) {
            LOG(WARNING) << "fail to create tablet(change schema), base tablet does not exist. "
                         << "new_tablet_id=" << tablet_id
                         << ", base_tablet_id=" << request.base_tablet_id;
            DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
            return Status::OLAPInternalError(OLAP_ERR_TABLE_CREATE_META_ERROR);
        }
        // If we are doing schema-change, we should use the same data dir
        // TODO(lingbin): A litter trick here, the directory should be determined before
        // entering this method
        if (request.storage_medium == base_tablet->data_dir()->storage_medium()) {
            stores.clear();
            stores.push_back(base_tablet->data_dir());
        }
    }

    // set alter type to schema-change. it is useless
    TabletSharedPtr tablet =
            _internal_create_tablet_unlocked(request, is_schema_change, base_tablet.get(), stores);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to create tablet. tablet_id=" << request.tablet_id;
        DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
        return Status::OLAPInternalError(OLAP_ERR_CE_CMD_PARAMS_ERROR);
    }
    TRACE("succeed to create tablet");

    LOG(INFO) << "success to create tablet. tablet_id=" << tablet_id;
    return Status::OK();
}

TabletSharedPtr TabletManager::_internal_create_tablet_unlocked(
        const TCreateTabletReq& request, const bool is_schema_change, const Tablet* base_tablet,
        const std::vector<DataDir*>& data_dirs) {
    // If in schema-change state, base_tablet must also be provided.
    // i.e., is_schema_change and base_tablet are either assigned or not assigned
    DCHECK((is_schema_change && base_tablet) || (!is_schema_change && !base_tablet));

    // NOTE: The existence of tablet_id and schema_hash has already been checked,
    // no need check again here.

    auto tablet =
            _create_tablet_meta_and_dir_unlocked(request, is_schema_change, base_tablet, data_dirs);
    if (tablet == nullptr) {
        return nullptr;
    }

    int64_t new_tablet_id = request.tablet_id;
    int32_t new_schema_hash = request.tablet_schema.schema_hash;

    // should remove the tablet's pending_id no matter create-tablet success or not
    DataDir* data_dir = tablet->data_dir();
    SCOPED_CLEANUP({ data_dir->remove_pending_ids(StrCat(TABLET_ID_PREFIX, new_tablet_id)); });

    // TODO(yiguolei)
    // the following code is very difficult to understand because it mixed alter tablet v2
    // and alter tablet v1 should remove alter tablet v1 code after v0.12
    Status res = Status::OK();
    bool is_tablet_added = false;
    do {
        res = tablet->init();
        if (!res.ok()) {
            LOG(WARNING) << "tablet init failed. tablet:" << tablet->full_name();
            break;
        }

        // Create init version if this is not a restore mode replica and request.version is set
        // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
        // if (!in_restore_mode && request.__isset.version) {
        // create initial rowset before add it to storage engine could omit many locks
        res = tablet->create_initial_rowset(request.version);
        if (!res.ok()) {
            LOG(WARNING) << "fail to create initial version for tablet. res=" << res;
            break;
        }

        if (is_schema_change) {
            // if this is a new alter tablet, has to set its state to not ready
            // because schema change handler depends on it to check whether history data
            // convert finished
            tablet->set_tablet_state(TabletState::TABLET_NOTREADY);
        }
        // Add tablet to StorageEngine will make it visible to user
        // Will persist tablet meta
        res = _add_tablet_unlocked(new_tablet_id, tablet, /*update_meta*/ true, false);
        if (!res.ok()) {
            LOG(WARNING) << "fail to add tablet to StorageEngine. res=" << res;
            break;
        }
        is_tablet_added = true;

        // TODO(lingbin): The following logic seems useless, can be removed?
        // Because if _add_tablet_unlocked() return OK, we must can get it from map.
        TabletSharedPtr tablet_ptr = _get_tablet_unlocked(new_tablet_id);
        if (tablet_ptr == nullptr) {
            res = Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
            LOG(WARNING) << "fail to get tablet. res=" << res;
            break;
        }
        TRACE("add tablet to StorageEngine");
    } while (0);

    if (res.ok()) {
        return tablet;
    }
    // something is wrong, we need clear environment
    if (is_tablet_added) {
        Status status = _drop_tablet_unlocked(new_tablet_id, request.replica_id, false, false);
        if (!status.ok()) {
            LOG(WARNING) << "fail to drop tablet when create tablet failed. res=" << res;
        }
    } else {
        tablet->delete_all_files();
        TabletMetaManager::remove(data_dir, new_tablet_id, new_schema_hash);
    }
    return nullptr;
}

static string _gen_tablet_dir(const string& dir, int16_t shard_id, int64_t tablet_id) {
    string path = dir;
    path = path_util::join_path_segments(path, DATA_PREFIX);
    path = path_util::join_path_segments(path, std::to_string(shard_id));
    path = path_util::join_path_segments(path, std::to_string(tablet_id));
    return path;
}

TabletSharedPtr TabletManager::_create_tablet_meta_and_dir_unlocked(
        const TCreateTabletReq& request, const bool is_schema_change, const Tablet* base_tablet,
        const std::vector<DataDir*>& data_dirs) {
    string pending_id = StrCat(TABLET_ID_PREFIX, request.tablet_id);
    // Many attempts are made here in the hope that even if a disk fails, it can still continue.
    DataDir* last_dir = nullptr;
    for (auto& data_dir : data_dirs) {
        if (last_dir != nullptr) {
            // If last_dir != null, it means the last attempt to create a tablet failed
            last_dir->remove_pending_ids(pending_id);
        }
        last_dir = data_dir;

        TabletMetaSharedPtr tablet_meta;
        // if create meta failed, do not need to clean dir, because it is only in memory
        Status res = _create_tablet_meta_unlocked(request, data_dir, is_schema_change, base_tablet,
                                                  &tablet_meta);
        if (!res.ok()) {
            LOG(WARNING) << "fail to create tablet meta. res=" << res
                         << ", root=" << data_dir->path();
            continue;
        }

        string tablet_dir =
                _gen_tablet_dir(data_dir->path(), tablet_meta->shard_id(), request.tablet_id);
        string schema_hash_dir = path_util::join_path_segments(
                tablet_dir, std::to_string(request.tablet_schema.schema_hash));

        // Because the tablet is removed asynchronously, so that the dir may still exist when BE
        // receive create-tablet request again, For example retried schema-change request
        if (FileUtils::check_exist(schema_hash_dir)) {
            LOG(WARNING) << "skip this dir because tablet path exist, path=" << schema_hash_dir;
            continue;
        } else {
            data_dir->add_pending_ids(pending_id);
            Status st = FileUtils::create_dir(schema_hash_dir);
            if (!st.ok()) {
                LOG(WARNING) << "create dir fail. path=" << schema_hash_dir
                             << " error=" << st.to_string();
                continue;
            }
        }

        TabletSharedPtr new_tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
        DCHECK(new_tablet != nullptr);
        return new_tablet;
    }
    return nullptr;
}

Status TabletManager::drop_tablet(TTabletId tablet_id, TReplicaId replica_id,
                                  bool is_drop_table_or_partition) {
    auto& shard = _get_tablets_shard(tablet_id);
    std::lock_guard wrlock(shard.lock);
    if (shard.tablets_under_clone.count(tablet_id) > 0) {
        LOG(INFO) << "tablet " << tablet_id << " is under clone, skip drop task";
        return Status::Aborted("aborted");
    }
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    return _drop_tablet_unlocked(tablet_id, replica_id, false, is_drop_table_or_partition);
}

// Drop specified tablet.
Status TabletManager::_drop_tablet_unlocked(TTabletId tablet_id, TReplicaId replica_id,
                                            bool keep_files, bool is_drop_table_or_partition) {
    LOG(INFO) << "begin drop tablet. tablet_id=" << tablet_id << ", replica_id=" << replica_id;
    DorisMetrics::instance()->drop_tablet_requests_total->increment(1);

    // Fetch tablet which need to be dropped
    TabletSharedPtr to_drop_tablet = _get_tablet_unlocked(tablet_id);
    if (to_drop_tablet == nullptr) {
        LOG(WARNING) << "fail to drop tablet because it does not exist. "
                     << "tablet_id=" << tablet_id;
        return Status::OK();
    }
    // We should compare replica id to avoid dropping new cloned tablet.
    // Iff request replica id is 0, FE may be an older release, then we drop this tablet as before.
    if (to_drop_tablet->replica_id() != replica_id && replica_id != 0) {
        LOG(WARNING) << "fail to drop tablet because replica id not match. "
                     << "tablet_id=" << tablet_id << ", replica_id=" << to_drop_tablet->replica_id()
                     << ", request replica_id=" << replica_id;
        return Status::Aborted("aborted");
    }

    _remove_tablet_from_partition(to_drop_tablet);
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map.erase(tablet_id);
    if (!keep_files) {
        // drop tablet will update tablet meta, should lock
        std::lock_guard<std::shared_mutex> wrlock(to_drop_tablet->get_header_lock());
        LOG(INFO) << "set tablet to shutdown state and remove it from memory. "
                  << "tablet_id=" << tablet_id << ", tablet_path=" << to_drop_tablet->tablet_path();
        // NOTE: has to update tablet here, but must not update tablet meta directly.
        // because other thread may hold the tablet object, they may save meta too.
        // If update meta directly here, other thread may override the meta
        // and the tablet will be loaded at restart time.
        // To avoid this exception, we first set the state of the tablet to `SHUTDOWN`.
        to_drop_tablet->set_tablet_state(TABLET_SHUTDOWN);
        // We must record unused remote rowsets path info to OlapMeta before tablet state is marked as TABLET_SHUTDOWN in OlapMeta,
        // otherwise if BE shutdown after saving tablet state, these remote rowsets path info will lost.
        if (is_drop_table_or_partition) {
            RETURN_IF_ERROR(to_drop_tablet->remove_all_remote_rowsets());
        } else {
            to_drop_tablet->remove_self_owned_remote_rowsets();
        }
        to_drop_tablet->save_meta();
        {
            std::lock_guard<std::shared_mutex> wrdlock(_shutdown_tablets_lock);
            _shutdown_tablets.push_back(to_drop_tablet);
        }
    }

    to_drop_tablet->deregister_tablet_from_dir();
    return Status::OK();
}

Status TabletManager::drop_tablets_on_error_root_path(
        const std::vector<TabletInfo>& tablet_info_vec) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    Status res = Status::OK();
    if (tablet_info_vec.empty()) { // This is a high probability event
        return res;
    }
    std::vector<std::set<size_t>> local_tmp_vector(_tablets_shards_size);
    for (size_t idx = 0; idx < tablet_info_vec.size(); ++idx) {
        local_tmp_vector[tablet_info_vec[idx].tablet_id & _tablets_shards_mask].insert(idx);
    }
    for (int32 i = 0; i < _tablets_shards_size; ++i) {
        if (local_tmp_vector[i].empty()) {
            continue;
        }
        std::lock_guard<std::shared_mutex> wrlock(_tablets_shards[i].lock);
        for (size_t idx : local_tmp_vector[i]) {
            const TabletInfo& tablet_info = tablet_info_vec[idx];
            TTabletId tablet_id = tablet_info.tablet_id;
            VLOG_NOTICE << "drop_tablet begin. tablet_id=" << tablet_id;
            TabletSharedPtr dropped_tablet = _get_tablet_unlocked(tablet_id);
            if (dropped_tablet == nullptr) {
                LOG(WARNING) << "dropping tablet not exist, "
                             << " tablet=" << tablet_id;
                continue;
            } else {
                _remove_tablet_from_partition(dropped_tablet);
                tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
                tablet_map.erase(tablet_id);
            }
        }
    }
    return res;
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, bool include_deleted, string* err) {
    std::shared_lock rdlock(_get_tablets_shard_lock(tablet_id));
    return _get_tablet_unlocked(tablet_id, include_deleted, err);
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, bool include_deleted,
                                                    string* err) {
    TabletSharedPtr tablet;
    tablet = _get_tablet_unlocked(tablet_id);
    if (tablet == nullptr && include_deleted) {
        std::shared_lock rdlock(_shutdown_tablets_lock);
        for (auto& deleted_tablet : _shutdown_tablets) {
            CHECK(deleted_tablet != nullptr) << "deleted tablet is nullptr";
            if (deleted_tablet->tablet_id() == tablet_id) {
                tablet = deleted_tablet;
                break;
            }
        }
    }

    if (tablet == nullptr) {
        if (err != nullptr) {
            *err = "tablet does not exist. " + BackendOptions::get_localhost();
        }
        return nullptr;
    }

    if (!tablet->is_used()) {
        LOG(WARNING) << "tablet cannot be used. tablet=" << tablet_id;
        if (err != nullptr) {
            *err = "tablet cannot be used. " + BackendOptions::get_localhost();
        }
        return nullptr;
    }

    return tablet;
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, TabletUid tablet_uid,
                                          bool include_deleted, string* err) {
    std::shared_lock rdlock(_get_tablets_shard_lock(tablet_id));
    TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, include_deleted, err);
    if (tablet != nullptr && tablet->tablet_uid() == tablet_uid) {
        return tablet;
    }
    return nullptr;
}

std::vector<TabletSharedPtr> TabletManager::get_all_tablet() {
    std::vector<TabletSharedPtr> res;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rdlock(tablets_shard.lock);
        for (const auto& tablet_map : tablets_shard.tablet_map) {
            // these are tablets which is not deleted
            TabletSharedPtr tablet = tablet_map.second;
            if (!tablet->is_used()) {
                LOG(WARNING) << "tablet cannot be used. tablet=" << tablet->tablet_id();
                continue;
            }
            res.emplace_back(tablet);
        }
    }
    return res;
}

bool TabletManager::get_tablet_id_and_schema_hash_from_path(const string& path,
                                                            TTabletId* tablet_id,
                                                            TSchemaHash* schema_hash) {
    // the path like: /data/14/10080/964828783/
    static re2::RE2 normal_re("/data/\\d+/(\\d+)/(\\d+)($|/)");
    // match tablet schema hash data path, for example, the path is /data/1/16791/29998
    // 1 is shard id , 16791 is tablet id, 29998 is schema hash
    if (RE2::PartialMatch(path, normal_re, tablet_id, schema_hash)) {
        return true;
    }

    // If we can't match normal path pattern, this may be a path which is a empty tablet
    // directory. Use this pattern to match empty tablet directory. In this case schema_hash
    // will be set to zero.
    static re2::RE2 empty_tablet_re("/data/\\d+/(\\d+)($|/$)");
    if (!RE2::PartialMatch(path, empty_tablet_re, tablet_id)) {
        return false;
    }
    *schema_hash = 0;
    return true;
}

bool TabletManager::get_rowset_id_from_path(const string& path, RowsetId* rowset_id) {
    // the path like: /data/14/10080/964828783/02000000000000969144d8725cb62765f9af6cd3125d5a91_0.dat
    static re2::RE2 re("/data/\\d+/\\d+/\\d+/([A-Fa-f0-9]+)_.*");
    string id_str;
    bool ret = RE2::PartialMatch(path, re, &id_str);
    if (ret) {
        rowset_id->init(id_str);
        return true;
    }
    return false;
}

void TabletManager::get_tablet_stat(TTabletStatResult* result) {
    std::shared_ptr<std::vector<TTabletStat>> local_cache;
    {
        std::lock_guard<std::mutex> guard(_tablet_stat_cache_mutex);
        local_cache = _tablet_stat_list_cache;
    }
    result->__set_tablet_stat_list(*local_cache);
}

TabletSharedPtr TabletManager::find_best_tablet_to_compaction(
        CompactionType compaction_type, DataDir* data_dir,
        const std::unordered_set<TTabletId>& tablet_submitted_compaction, uint32_t* score,
        std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
    int64_t now_ms = UnixMillis();
    const string& compaction_type_str =
            compaction_type == CompactionType::BASE_COMPACTION ? "base" : "cumulative";
    uint32_t highest_score = 0;
    uint32_t compaction_score = 0;
    TabletSharedPtr best_tablet;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rdlock(tablets_shard.lock);
        for (const auto& tablet_map : tablets_shard.tablet_map) {
            const TabletSharedPtr& tablet_ptr = tablet_map.second;
            if (tablet_ptr->should_skip_compaction(compaction_type, UnixSeconds())) {
                continue;
            }
            if (!tablet_ptr->can_do_compaction(data_dir->path_hash(), compaction_type)) {
                continue;
            }

            auto search = tablet_submitted_compaction.find(tablet_ptr->tablet_id());
            if (search != tablet_submitted_compaction.end()) {
                continue;
            }

            int64_t last_failure_ms = tablet_ptr->last_cumu_compaction_failure_time();
            if (compaction_type == CompactionType::BASE_COMPACTION) {
                last_failure_ms = tablet_ptr->last_base_compaction_failure_time();
            }
            if (now_ms - last_failure_ms <= 5000) {
                VLOG_DEBUG << "Too often to check compaction, skip it. "
                           << "compaction_type=" << compaction_type_str
                           << ", last_failure_time_ms=" << last_failure_ms
                           << ", tablet_id=" << tablet_ptr->tablet_id();
                continue;
            }

            if (compaction_type == CompactionType::BASE_COMPACTION) {
                std::unique_lock<std::mutex> lock(tablet_ptr->get_base_compaction_lock(),
                                                  std::try_to_lock);
                if (!lock.owns_lock()) {
                    LOG(INFO) << "can not get base lock: " << tablet_ptr->tablet_id();
                    continue;
                }
            } else {
                std::unique_lock<std::mutex> lock(tablet_ptr->get_cumulative_compaction_lock(),
                                                  std::try_to_lock);
                if (!lock.owns_lock()) {
                    LOG(INFO) << "can not get cumu lock: " << tablet_ptr->tablet_id();
                    continue;
                }
            }

            uint32_t current_compaction_score = tablet_ptr->calc_compaction_score(
                    compaction_type, cumulative_compaction_policy);
            if (current_compaction_score < 5) {
                LOG(INFO) << "tablet set skip compaction, tablet_id: " << tablet_ptr->tablet_id();
                tablet_ptr->set_skip_compaction(true, compaction_type, UnixSeconds());
            }
            if (current_compaction_score > highest_score) {
                highest_score = current_compaction_score;
                compaction_score = current_compaction_score;
                best_tablet = tablet_ptr;
            }
        }
    }

    if (best_tablet != nullptr) {
        VLOG_CRITICAL << "Found the best tablet for compaction. "
                      << "compaction_type=" << compaction_type_str
                      << ", tablet_id=" << best_tablet->tablet_id() << ", path=" << data_dir->path()
                      << ", compaction_score=" << compaction_score
                      << ", highest_score=" << highest_score;
        *score = compaction_score;
    }
    return best_tablet;
}

Status TabletManager::load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id,
                                            TSchemaHash schema_hash, const string& meta_binary,
                                            bool update_meta, bool force, bool restore,
                                            bool check_path) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    Status status = tablet_meta->deserialize(meta_binary);
    if (!status.ok()) {
        LOG(WARNING) << "fail to load tablet because can not parse meta_binary string. "
                     << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
                     << ", path=" << data_dir->path();
        return Status::OLAPInternalError(OLAP_ERR_HEADER_PB_PARSE_FAILED);
    }
    tablet_meta->init_rs_metas_fs(data_dir->fs());

    // check if tablet meta is valid
    if (tablet_meta->tablet_id() != tablet_id || tablet_meta->schema_hash() != schema_hash) {
        LOG(WARNING) << "fail to load tablet because meet invalid tablet meta. "
                     << "trying to load tablet(tablet_id=" << tablet_id
                     << ", schema_hash=" << schema_hash << ")"
                     << ", but meet tablet=" << tablet_meta->full_name()
                     << ", path=" << data_dir->path();
        return Status::OLAPInternalError(OLAP_ERR_HEADER_PB_PARSE_FAILED);
    }
    if (tablet_meta->tablet_uid().hi == 0 && tablet_meta->tablet_uid().lo == 0) {
        LOG(WARNING) << "fail to load tablet because its uid == 0. "
                     << "tablet=" << tablet_meta->full_name() << ", path=" << data_dir->path();
        return Status::OLAPInternalError(OLAP_ERR_HEADER_PB_PARSE_FAILED);
    }

    if (restore) {
        // we're restoring tablet from trash, tablet state should be changed from shutdown back to running
        tablet_meta->set_tablet_state(TABLET_RUNNING);
    }

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to load tablet. tablet_id=" << tablet_id
                     << ", schema_hash:" << schema_hash;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR);
    }

    // NOTE: method load_tablet_from_meta could be called by two cases as below
    // case 1: BE start;
    // case 2: Clone Task/Restore
    // For case 1 doesn't need path check because BE is just starting and not ready,
    // just check tablet meta status to judge whether tablet is delete is enough.
    // For case 2, If a tablet has just been copied to local BE,
    // it may be cleared by gc-thread(see perform_path_gc_by_tablet) because the tablet meta may not be loaded to memory.
    // So clone task should check path and then failed and retry in this case.
    if (check_path && !Env::Default()->path_exists(tablet->tablet_path()).ok()) {
        LOG(WARNING) << "tablet path not exists, create tablet failed, path="
                     << tablet->tablet_path();
        return Status::OLAPInternalError(OLAP_ERR_TABLE_ALREADY_DELETED_ERROR);
    }

    if (tablet_meta->tablet_state() == TABLET_SHUTDOWN) {
        LOG(INFO) << "fail to load tablet because it is to be deleted. tablet_id=" << tablet_id
                  << " schema_hash=" << schema_hash << ", path=" << data_dir->path();
        {
            std::lock_guard<std::shared_mutex> shutdown_tablets_wrlock(_shutdown_tablets_lock);
            _shutdown_tablets.push_back(tablet);
        }
        return Status::OLAPInternalError(OLAP_ERR_TABLE_ALREADY_DELETED_ERROR);
    }
    // NOTE: We do not check tablet's initial version here, because if BE restarts when
    // one tablet is doing schema-change, we may meet empty tablet.
    if (tablet->max_version().first == -1 && tablet->tablet_state() == TABLET_RUNNING) {
        LOG(WARNING) << "fail to load tablet. it is in running state but without delta. "
                     << "tablet=" << tablet->full_name() << ", path=" << data_dir->path();
        // tablet state is invalid, drop tablet
        return Status::OLAPInternalError(OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR);
    }

    RETURN_NOT_OK_LOG(tablet->init(),
                      strings::Substitute("tablet init failed. tablet=$0", tablet->full_name()));

    std::lock_guard<std::shared_mutex> wrlock(_get_tablets_shard_lock(tablet_id));
    RETURN_NOT_OK_LOG(_add_tablet_unlocked(tablet_id, tablet, update_meta, force),
                      strings::Substitute("fail to add tablet. tablet=$0", tablet->full_name()));

    return Status::OK();
}

Status TabletManager::load_tablet_from_dir(DataDir* store, TTabletId tablet_id,
                                           SchemaHash schema_hash, const string& schema_hash_path,
                                           bool force, bool restore) {
    LOG(INFO) << "begin to load tablet from dir. "
              << " tablet_id=" << tablet_id << " schema_hash=" << schema_hash
              << " path = " << schema_hash_path << " force = " << force << " restore = " << restore;
    // not add lock here, because load_tablet_from_meta already add lock
    std::string header_path = TabletMeta::construct_header_file_path(schema_hash_path, tablet_id);
    // should change shard id before load tablet
    std::string shard_path =
            path_util::dir_name(path_util::dir_name(path_util::dir_name(header_path)));
    std::string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    int32_t shard = stol(shard_str);
    // load dir is called by clone, restore, storage migration
    // should change tablet uid when tablet object changed
    RETURN_NOT_OK_LOG(
            TabletMeta::reset_tablet_uid(header_path),
            strings::Substitute("failed to set tablet uid when copied meta file. header_path=%0",
                                header_path));
    ;

    if (!Env::Default()->path_exists(header_path).ok()) {
        LOG(WARNING) << "fail to find header file. [header_path=" << header_path << "]";
        return Status::OLAPInternalError(OLAP_ERR_FILE_NOT_EXIST);
    }

    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    if (tablet_meta->create_from_file(header_path) != Status::OK()) {
        LOG(WARNING) << "fail to load tablet_meta. file_path=" << header_path;
        return Status::OLAPInternalError(OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR);
    }
    // has to change shard id here, because meta file maybe copied from other source
    // its shard is different from local shard
    tablet_meta->set_shard_id(shard);
    std::string meta_binary;
    tablet_meta->serialize(&meta_binary);
    RETURN_NOT_OK_LOG(load_tablet_from_meta(store, tablet_id, schema_hash, meta_binary, true, force,
                                            restore, true),
                      strings::Substitute("fail to load tablet. header_path=$0", header_path));

    return Status::OK();
}

Status TabletManager::report_tablet_info(TTabletInfo* tablet_info) {
    DorisMetrics::instance()->report_tablet_requests_total->increment(1);
    LOG(INFO) << "begin to process report tablet info."
              << "tablet_id=" << tablet_info->tablet_id;

    Status res = Status::OK();

    TabletSharedPtr tablet = get_tablet(tablet_info->tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "can't find tablet=" << tablet_info->tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    tablet->build_tablet_report_info(tablet_info);
    VLOG_TRACE << "success to process report tablet info.";
    return res;
}

Status TabletManager::build_all_report_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    DCHECK(tablets_info != nullptr);
    VLOG_NOTICE << "begin to build all report tablets info";

    // build the expired txn map first, outside the tablet map lock
    std::map<TabletInfo, std::vector<int64_t>> expire_txn_map;
    StorageEngine::instance()->txn_manager()->build_expire_txn_map(&expire_txn_map);
    LOG(INFO) << "find expired transactions for " << expire_txn_map.size() << " tablets";

    DorisMetrics::instance()->report_all_tablets_requests_total->increment(1);
    HistogramStat tablet_version_num_hist;
    auto local_cache = std::make_shared<std::vector<TTabletStat>>();
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rdlock(tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            uint64_t tablet_id = item.first;
            TabletSharedPtr tablet_ptr = item.second;
            TTablet t_tablet;
            TTabletInfo tablet_info;
            tablet_ptr->build_tablet_report_info(&tablet_info, true);
            // find expired transaction corresponding to this tablet
            TabletInfo tinfo(tablet_id, tablet_ptr->schema_hash(), tablet_ptr->tablet_uid());
            auto find = expire_txn_map.find(tinfo);
            if (find != expire_txn_map.end()) {
                tablet_info.__set_transaction_ids(find->second);
                expire_txn_map.erase(find);
            }
            t_tablet.tablet_infos.push_back(tablet_info);
            tablet_version_num_hist.add(tablet_ptr->version_count());
            tablets_info->emplace(tablet_id, t_tablet);
            TTabletStat t_tablet_stat;
            t_tablet_stat.__set_tablet_id(tablet_info.tablet_id);
            t_tablet_stat.__set_data_size(tablet_info.data_size);
            t_tablet_stat.__set_remote_data_size(tablet_info.remote_data_size);
            t_tablet_stat.__set_row_num(tablet_info.row_count);
            t_tablet_stat.__set_version_count(tablet_info.version_count);
            local_cache->emplace_back(std::move(t_tablet_stat));
        }
    }
    {
        std::lock_guard<std::mutex> guard(_tablet_stat_cache_mutex);
        _tablet_stat_list_cache = local_cache;
    }
    DorisMetrics::instance()->tablet_version_num_distribution->set_histogram(
            tablet_version_num_hist);
    LOG(INFO) << "success to build all report tablets info. tablet_count=" << tablets_info->size();
    return Status::OK();
}

Status TabletManager::start_trash_sweep() {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    {
        std::vector<TabletSharedPtr>
                all_tablets; // we use this vector to save all tablet ptr for saving lock time.
        for (auto& tablets_shard : _tablets_shards) {
            tablet_map_t& tablet_map = tablets_shard.tablet_map;
            {
                std::shared_lock rdlock(tablets_shard.lock);
                for (auto& item : tablet_map) {
                    // try to clean empty item
                    all_tablets.push_back(item.second);
                }
            }
            // Avoid hold the shard lock too long, so we get tablet to a vector and clean here
            for (const auto& tablet : all_tablets) {
                tablet->delete_expired_stale_rowset();
            }
            all_tablets.clear();
        }
    }

    int32_t clean_num = 0;
    do {
#ifndef BE_TEST
        sleep(1);
#endif
        clean_num = 0;
        // should get write lock here, because it will remove tablet from shut_down_tablets
        // and get tablet will access shut_down_tablets
        std::lock_guard<std::shared_mutex> wrlock(_shutdown_tablets_lock);
        auto it = _shutdown_tablets.begin();
        while (it != _shutdown_tablets.end()) {
            // check if the meta has the tablet info and its state is shutdown
            if (it->use_count() > 1) {
                // it means current tablet is referenced by other thread
                ++it;
                continue;
            }
            TabletMetaSharedPtr tablet_meta(new TabletMeta());
            Status check_st = TabletMetaManager::get_meta((*it)->data_dir(), (*it)->tablet_id(),
                                                          (*it)->schema_hash(), tablet_meta);
            if (check_st.ok()) {
                if (tablet_meta->tablet_state() != TABLET_SHUTDOWN ||
                    tablet_meta->tablet_uid() != (*it)->tablet_uid()) {
                    LOG(WARNING) << "tablet's state changed to normal, skip remove dirs"
                                 << " tablet id = " << tablet_meta->tablet_id()
                                 << " schema hash = " << tablet_meta->schema_hash()
                                 << " old tablet_uid=" << (*it)->tablet_uid()
                                 << " cur tablet_uid=" << tablet_meta->tablet_uid();
                    // remove it from list
                    it = _shutdown_tablets.erase(it);
                    continue;
                }
                // move data to trash
                const auto& tablet_path = (*it)->tablet_path();
                if (Env::Default()->path_exists(tablet_path).ok()) {
                    // take snapshot of tablet meta
                    auto meta_file_path = fmt::format("{}/{}.hdr", tablet_path, (*it)->tablet_id());
                    (*it)->tablet_meta()->save(meta_file_path);
                    LOG(INFO) << "start to move tablet to trash. " << tablet_path;
                    Status rm_st = (*it)->data_dir()->move_to_trash(tablet_path);
                    if (rm_st != Status::OK()) {
                        LOG(WARNING) << "fail to move dir to trash. " << tablet_path;
                        ++it;
                        continue;
                    }
                }
                // remove tablet meta
                TabletMetaManager::remove((*it)->data_dir(), (*it)->tablet_id(),
                                          (*it)->schema_hash());
                LOG(INFO) << "successfully move tablet to trash. "
                          << "tablet_id=" << (*it)->tablet_id()
                          << ", schema_hash=" << (*it)->schema_hash()
                          << ", tablet_path=" << tablet_path;
                it = _shutdown_tablets.erase(it);
                ++clean_num;
            } else {
                // if could not find tablet info in meta store, then check if dir existed
                const auto& tablet_path = (*it)->tablet_path();
                if (Env::Default()->path_exists(tablet_path).ok()) {
                    LOG(WARNING) << "errors while load meta from store, skip this tablet. "
                                 << "tablet_id=" << (*it)->tablet_id()
                                 << ", schema_hash=" << (*it)->schema_hash();
                    ++it;
                } else {
                    LOG(INFO) << "could not find tablet dir, skip it and remove it from gc-queue. "
                              << "tablet_id=" << (*it)->tablet_id()
                              << ", schema_hash=" << (*it)->schema_hash()
                              << ", tablet_path=" << tablet_path;
                    it = _shutdown_tablets.erase(it);
                }
            }

            // yield to avoid holding _tablet_map_lock for too long
            if (clean_num >= 200) {
                break;
            }
        }
        // >= 200 means there may be more tablets need to be handled
        // So continue
    } while (clean_num >= 200);
    return Status::OK();
} // start_trash_sweep

void TabletManager::register_clone_tablet(int64_t tablet_id) {
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::lock_guard<std::shared_mutex> wrlock(shard.lock);
    shard.tablets_under_clone.insert(tablet_id);
}

void TabletManager::unregister_clone_tablet(int64_t tablet_id) {
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::lock_guard<std::shared_mutex> wrlock(shard.lock);
    shard.tablets_under_clone.erase(tablet_id);
}

void TabletManager::try_delete_unused_tablet_path(DataDir* data_dir, TTabletId tablet_id,
                                                  SchemaHash schema_hash,
                                                  const string& schema_hash_path) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    // acquire the read lock, so that there is no creating tablet or load tablet from meta tasks
    // create tablet and load tablet task should check whether the dir exists
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::shared_lock rdlock(shard.lock);

    // check if meta already exists
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    Status check_st = TabletMetaManager::get_meta(data_dir, tablet_id, schema_hash, tablet_meta);
    if (check_st.ok()) {
        LOG(INFO) << "tablet meta exist is meta store, skip delete the path " << schema_hash_path;
        return;
    }

    if (shard.tablets_under_clone.count(tablet_id) > 0) {
        LOG(INFO) << "tablet is under clone, skip delete the path " << schema_hash_path;
        return;
    }

    // TODO(ygl): may do other checks in the future
    if (Env::Default()->path_exists(schema_hash_path).ok()) {
        LOG(INFO) << "start to move tablet to trash. tablet_path = " << schema_hash_path;
        Status rm_st = data_dir->move_to_trash(schema_hash_path);
        if (!rm_st.ok()) {
            LOG(WARNING) << "fail to move dir to trash. dir=" << schema_hash_path;
        } else {
            LOG(INFO) << "move path " << schema_hash_path << " to trash successfully";
        }
    }
    return;
}

void TabletManager::update_root_path_info(std::map<string, DataDirInfo>* path_map,
                                          size_t* tablet_count) {
    DCHECK(tablet_count);
    *tablet_count = 0;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rdlock(tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            TabletSharedPtr tablet = item.second;
            ++(*tablet_count);
            auto iter = path_map->find(tablet->data_dir()->path());
            if (iter == path_map->end()) {
                continue;
            }
            if (iter->second.is_used) {
                iter->second.local_used_capacity += tablet->tablet_local_size();
                iter->second.remote_used_capacity += tablet->tablet_remote_size();
            }
        }
    }
}

void TabletManager::get_partition_related_tablets(int64_t partition_id,
                                                  std::set<TabletInfo>* tablet_infos) {
    std::shared_lock rdlock(_partition_tablet_map_lock);
    if (_partition_tablet_map.find(partition_id) != _partition_tablet_map.end()) {
        *tablet_infos = _partition_tablet_map[partition_id];
    }
}

void TabletManager::do_tablet_meta_checkpoint(DataDir* data_dir) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    std::vector<TabletSharedPtr> related_tablets;
    {
        for (auto& tablets_shard : _tablets_shards) {
            std::shared_lock rdlock(tablets_shard.lock);
            for (auto& item : tablets_shard.tablet_map) {
                TabletSharedPtr& tablet_ptr = item.second;
                if (tablet_ptr->tablet_state() != TABLET_RUNNING) {
                    continue;
                }

                if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash() ||
                    !tablet_ptr->is_used() || !tablet_ptr->init_succeeded()) {
                    continue;
                }
                related_tablets.push_back(tablet_ptr);
            }
        }
    }
    int counter = 0;
    MonotonicStopWatch watch;
    watch.start();
    for (TabletSharedPtr tablet : related_tablets) {
        if (tablet->do_tablet_meta_checkpoint()) {
            ++counter;
        }
    }
    int64_t cost = watch.elapsed_time() / 1000 / 1000;
    LOG(INFO) << "finish to do meta checkpoint on dir: " << data_dir->path()
              << ", number: " << counter << ", cost(ms): " << cost;
}

Status TabletManager::_create_tablet_meta_unlocked(const TCreateTabletReq& request, DataDir* store,
                                                   const bool is_schema_change,
                                                   const Tablet* base_tablet,
                                                   TabletMetaSharedPtr* tablet_meta) {
    uint32_t next_unique_id = 0;
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    if (!is_schema_change) {
        for (uint32_t col_idx = 0; col_idx < request.tablet_schema.columns.size(); ++col_idx) {
            col_idx_to_unique_id[col_idx] = col_idx;
        }
        next_unique_id = request.tablet_schema.columns.size();
    } else {
        next_unique_id = base_tablet->next_unique_id();
        auto& new_columns = request.tablet_schema.columns;
        for (uint32_t new_col_idx = 0; new_col_idx < new_columns.size(); ++new_col_idx) {
            const TColumn& column = new_columns[new_col_idx];
            // For schema change, compare old_tablet and new_tablet:
            // 1. if column exist in both new_tablet and old_tablet, choose the column's
            //    unique_id in old_tablet to be the column's ordinal number in new_tablet
            // 2. if column exists only in new_tablet, assign next_unique_id of old_tablet
            //    to the new column
            int32_t old_col_idx = base_tablet->tablet_schema()->field_index(column.column_name);
            if (old_col_idx != -1) {
                uint32_t old_unique_id =
                        base_tablet->tablet_schema()->column(old_col_idx).unique_id();
                col_idx_to_unique_id[new_col_idx] = old_unique_id;
            } else {
                // Not exist in old tablet, it is a new added column
                col_idx_to_unique_id[new_col_idx] = next_unique_id++;
            }
        }
    }
    VLOG_NOTICE << "creating tablet meta. next_unique_id=" << next_unique_id;

    // We generate a new tablet_uid for this new tablet.
    uint64_t shard_id = 0;
    RETURN_NOT_OK_LOG(store->get_shard(&shard_id), "fail to get root path shard");
    Status res = TabletMeta::create(request, TabletUid::gen_uid(), shard_id, next_unique_id,
                                    col_idx_to_unique_id, tablet_meta);
    RETURN_NOT_OK(res);
    if (request.__isset.storage_format) {
        if (request.storage_format == TStorageFormat::DEFAULT) {
            (*tablet_meta)
                    ->set_preferred_rowset_type(StorageEngine::instance()->default_rowset_type());
        } else if (request.storage_format == TStorageFormat::V1) {
            (*tablet_meta)->set_preferred_rowset_type(ALPHA_ROWSET);
        } else if (request.storage_format == TStorageFormat::V2) {
            (*tablet_meta)->set_preferred_rowset_type(BETA_ROWSET);
        } else {
            LOG(FATAL) << "invalid TStorageFormat: " << request.storage_format;
            return Status::OLAPInternalError(OLAP_ERR_CE_CMD_PARAMS_ERROR);
        }
    }
    return res;
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id) {
    VLOG_NOTICE << "begin to get tablet. tablet_id=" << tablet_id;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    const auto& iter = tablet_map.find(tablet_id);
    if (iter != tablet_map.end()) {
        return iter->second;
    }
    return nullptr;
}

void TabletManager::_add_tablet_to_partition(const TabletSharedPtr& tablet) {
    std::lock_guard<std::shared_mutex> wrlock(_partition_tablet_map_lock);
    _partition_tablet_map[tablet->partition_id()].insert(tablet->get_tablet_info());
}

void TabletManager::_remove_tablet_from_partition(const TabletSharedPtr& tablet) {
    std::lock_guard<std::shared_mutex> wrlock(_partition_tablet_map_lock);
    _partition_tablet_map[tablet->partition_id()].erase(tablet->get_tablet_info());
    if (_partition_tablet_map[tablet->partition_id()].empty()) {
        _partition_tablet_map.erase(tablet->partition_id());
    }
}

void TabletManager::obtain_specific_quantity_tablets(vector<TabletInfo>& tablets_info,
                                                     int64_t num) {
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rdlock(tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            TabletSharedPtr tablet = item.second;
            if (tablets_info.size() >= num) {
                return;
            }
            if (tablet == nullptr) {
                continue;
            }
            tablets_info.push_back(tablet->get_tablet_info());
        }
    }
}

std::shared_mutex& TabletManager::_get_tablets_shard_lock(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).lock;
}

TabletManager::tablet_map_t& TabletManager::_get_tablet_map(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).tablet_map;
}

TabletManager::tablets_shard& TabletManager::_get_tablets_shard(TTabletId tabletId) {
    return _tablets_shards[tabletId & _tablets_shards_mask];
}

void TabletManager::get_tablets_distribution_on_different_disks(
        std::map<int64_t, std::map<DataDir*, int64_t>>& tablets_num_on_disk,
        std::map<int64_t, std::map<DataDir*, std::vector<TabletSize>>>& tablets_info_on_disk) {
    std::vector<DataDir*> data_dirs = StorageEngine::instance()->get_stores();
    std::map<int64_t, std::set<TabletInfo>> partition_tablet_map;
    {
        // When drop tablet, '_partition_tablet_map_lock' is locked in 'tablet_shard_lock'.
        // To avoid locking 'tablet_shard_lock' in '_partition_tablet_map_lock', we lock and
        // copy _partition_tablet_map here.
        std::shared_lock rdlock(_partition_tablet_map_lock);
        partition_tablet_map = _partition_tablet_map;
    }
    std::map<int64_t, std::set<TabletInfo>>::iterator partition_iter = partition_tablet_map.begin();
    for (; partition_iter != partition_tablet_map.end(); ++partition_iter) {
        std::map<DataDir*, int64_t> tablets_num;
        std::map<DataDir*, std::vector<TabletSize>> tablets_info;
        for (int i = 0; i < data_dirs.size(); i++) {
            tablets_num[data_dirs[i]] = 0;
        }
        int64_t partition_id = partition_iter->first;
        std::set<TabletInfo>::iterator tablet_info_iter = (partition_iter->second).begin();
        for (; tablet_info_iter != (partition_iter->second).end(); ++tablet_info_iter) {
            // get_tablet() will hold 'tablet_shard_lock'
            TabletSharedPtr tablet = get_tablet(tablet_info_iter->tablet_id);
            if (tablet == nullptr) {
                continue;
            }
            DataDir* data_dir = tablet->data_dir();
            size_t tablet_footprint = tablet->tablet_footprint();
            tablets_num[data_dir]++;
            TabletSize tablet_size(tablet_info_iter->tablet_id, tablet_info_iter->schema_hash,
                                   tablet_footprint);
            tablets_info[data_dir].push_back(tablet_size);
        }
        tablets_num_on_disk[partition_id] = tablets_num;
        tablets_info_on_disk[partition_id] = tablets_info;
    }
}

struct SortCtx {
    SortCtx(TabletSharedPtr tablet, int64_t cooldown_timestamp, int64_t file_size)
            : tablet(tablet), cooldown_timestamp(cooldown_timestamp), file_size(file_size) {}
    TabletSharedPtr tablet;
    int64_t cooldown_timestamp;
    int64_t file_size;
};

void TabletManager::get_cooldown_tablets(std::vector<TabletSharedPtr>* tablets) {
    std::vector<SortCtx> sort_ctx_vec;
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rdlock(tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            const TabletSharedPtr& tablet = item.second;
            int64_t cooldown_timestamp = -1;
            size_t file_size = -1;
            if (tablet->need_cooldown(&cooldown_timestamp, &file_size)) {
                sort_ctx_vec.emplace_back(tablet, cooldown_timestamp, file_size);
            }
        }
    }

    std::sort(sort_ctx_vec.begin(), sort_ctx_vec.end(), [](SortCtx a, SortCtx b) {
        if (a.cooldown_timestamp != -1 && b.cooldown_timestamp != -1) {
            return a.cooldown_timestamp < b.cooldown_timestamp;
        }

        if (a.cooldown_timestamp != -1 && b.cooldown_timestamp == -1) {
            return true;
        }

        if (a.cooldown_timestamp == -1 && b.cooldown_timestamp != -1) {
            return false;
        }

        return a.file_size > b.file_size;
    });

    for (SortCtx& ctx : sort_ctx_vec) {
        VLOG_DEBUG << "get cooldown tablet: " << ctx.tablet->tablet_id();
        tablets->push_back(std::move(ctx.tablet));
    }
}

void TabletManager::get_all_tablets_storage_format(TCheckStorageFormatResult* result) {
    DCHECK(result != nullptr);
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rdlock(tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            uint64_t tablet_id = item.first;
            if (item.second->all_beta()) {
                result->v2_tablets.push_back(tablet_id);
            } else {
                result->v1_tablets.push_back(tablet_id);
            }
        }
    }
    result->__isset.v1_tablets = true;
    result->__isset.v2_tablets = true;
}

std::set<int64_t> TabletManager::check_all_tablet_segment(bool repair) {
    std::set<int64_t> bad_tablets;
    for (const auto& tablets_shard : _tablets_shards) {
        std::lock_guard<std::shared_mutex> wrlock(tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            TabletSharedPtr tablet = item.second;
            if (!tablet->check_all_rowset_segment()) {
                bad_tablets.insert(tablet->tablet_id());
                if (repair) {
                    tablet->set_tablet_state(TABLET_SHUTDOWN);
                    tablet->save_meta();
                    {
                        std::lock_guard<std::shared_mutex> shutdown_tablets_wrlock(
                                _shutdown_tablets_lock);
                        _shutdown_tablets.push_back(tablet);
                    }
                    LOG(WARNING) << "There are some segments lost, set tablet to shutdown state."
                                 << "tablet_id=" << tablet->tablet_id()
                                 << ", tablet_path=" << tablet->tablet_path();
                }
            }
        }
    }
    return bad_tablets;
}

} // end namespace doris
