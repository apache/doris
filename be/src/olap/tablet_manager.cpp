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

#include <signal.h>

#include <algorithm>
#include <cstdio>
#include <new>
#include <queue>
#include <set>
#include <random>
#include <stdlib.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <rapidjson/document.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <re2/re2.h>

#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/lru_cache.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/schema_change.h"
#include "olap/data_dir.h"
#include "olap/utils.h"
#include "olap/olap_common.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_id_generator.h"
#include "util/time.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"

using apache::thrift::ThriftDebugString;
using boost::filesystem::canonical;
using boost::filesystem::directory_iterator;
using boost::filesystem::path;
using boost::filesystem::recursive_directory_iterator;
using std::back_inserter;
using std::copy;
using std::inserter;
using std::list;
using std::map;
using std::nothrow;
using std::pair;
using std::priority_queue;
using std::set;
using std::set_difference;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

bool _sort_tablet_by_creation_time(const TabletSharedPtr& a, const TabletSharedPtr& b) {
    return a->creation_time() < b->creation_time();
}

TabletManager::TabletManager()
    : _tablet_stat_cache_update_time_ms(0),
      _available_storage_medium_type_count(0) { }

OLAPStatus TabletManager::_add_tablet_unlock(TTabletId tablet_id, SchemaHash schema_hash,
                                 const TabletSharedPtr& tablet, bool update_meta, bool force) {
    OLAPStatus res = OLAP_SUCCESS;
    VLOG(3) << "begin to add tablet to TabletManager. "
            << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
            << ", force=" << force;

    TabletSharedPtr table_item = nullptr;
    for (TabletSharedPtr item : _tablet_map[tablet_id].table_arr) {
        if (item->equal(tablet_id, schema_hash)) {
            table_item = item;
            break;
        }
    }

    if (table_item == nullptr) {
        VLOG(3) << "not find exist tablet just add it to map"
                << " tablet_id = " << tablet_id
                << " schema_hash = " << schema_hash;
        return _add_tablet_to_map(tablet_id, schema_hash, tablet, update_meta, false, false);
    }

    if (!force) {
        if (table_item->tablet_path() == tablet->tablet_path()) {
            LOG(WARNING) << "add the same tablet twice! tablet_id="
                         << tablet_id << " schema_hash=" << schema_hash;
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
        if (table_item->data_dir() == tablet->data_dir()) {
            LOG(WARNING) << "add tablet with same data dir twice! tablet_id="
                         << tablet_id << " schema_hash=" << schema_hash;
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
    }

    table_item->obtain_header_rdlock();
    const RowsetSharedPtr old_rowset = table_item->rowset_with_max_version();
    const RowsetSharedPtr new_rowset = tablet->rowset_with_max_version();

    // if new tablet is empty, it is a newly created schema change tablet
    // the old tablet is dropped before add tablet. it should not exist old tablet
    if (new_rowset == nullptr) {
        table_item->release_header_lock();
        // it seems useless to call unlock and return here.
        // it could prevent error when log level is changed in the future.
        LOG(FATAL) << "new tablet is empty and old tablet exists. it should not happen."
                   << " tablet_id=" << tablet_id << " schema_hash=" << schema_hash;
        return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    int64_t old_time = old_rowset == nullptr ? -1 : old_rowset->creation_time();
    int64_t new_time = new_rowset->creation_time();
    int32_t old_version = old_rowset == nullptr ? -1 : old_rowset->end_version();
    int32_t new_version = new_rowset->end_version();
    table_item->release_header_lock();

    /*
     * In restore process, we replace all origin files in tablet dir with
     * the downloaded snapshot files. Than we try to reload tablet header.
     * force == true means we forcibly replace the Tablet in _tablet_map
     * with the new one. But if we do so, the files in the tablet dir will be
     * dropped when the origin Tablet deconstruct.
     * So we set keep_files == true to not delete files when the
     * origin Tablet deconstruct.
     */
    bool keep_files = force ? true : false;
    if (force || (new_version > old_version
            || (new_version == old_version && new_time > old_time))) {
        // check if new tablet's meta is in store and add new tablet's meta to meta store
        res = _add_tablet_to_map(tablet_id, schema_hash, tablet, update_meta, keep_files, true);
    } else {
        res = OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    LOG(WARNING) << "add duplicated tablet. force=" << force << ", res=" << res
            << ", tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
            << ", old_version=" << old_version << ", new_version=" << new_version
            << ", old_time=" << old_time << ", new_time=" << new_time
            << ", old_tablet_path=" << table_item->tablet_path()
            << ", new_tablet_path=" << tablet->tablet_path();

    return res;
} // add_tablet

OLAPStatus TabletManager::_add_tablet_to_map(TTabletId tablet_id, SchemaHash schema_hash,
                                 const TabletSharedPtr& tablet, bool update_meta, 
                                 bool keep_files, bool drop_old) {
     // check if new tablet's meta is in store and add new tablet's meta to meta store
    OLAPStatus res = OLAP_SUCCESS;
    if (update_meta) {
        // call tablet save meta in order to valid the meta
        res = tablet->save_meta();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to save new tablet's meta to meta store" 
                            << " tablet_id = " << tablet_id
                            << " schema_hash = " << schema_hash;
            return res;
        }
    }
    if (drop_old) {
        // if the new tablet is fresher than current one
        // then delete current one and add new one
        res = _drop_tablet_unlock(tablet_id, schema_hash, keep_files);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to drop old tablet when add new tablet"
                            << " tablet_id = " << tablet_id
                            << " schema_hash = " << schema_hash;
            return res;
        }
    }
    // Register tablet into StorageEngine, so that we can manage tablet from
    // the perspective of root path.
    // Example: unregister all tables when a bad disk found.
    res = tablet->register_tablet_into_dir();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to register tablet into StorageEngine. res=" << res
                        << ", data_dir=" << tablet->data_dir()->path();
        return res;
    }
    _tablet_map[tablet_id].table_arr.push_back(tablet);
    _tablet_map[tablet_id].table_arr.sort(_sort_tablet_by_creation_time);

    // add the tablet id to partition map
    _partition_tablet_map[tablet->partition_id()].insert(tablet->get_tablet_info());

    VLOG(3) << "add tablet to map successfully" 
            << " tablet_id = " << tablet_id
            << " schema_hash = " << schema_hash;   
    return res;                              
}

// this method is called when engine restarts so that not add any locks
void TabletManager::cancel_unfinished_schema_change() {
    // Schema Change在引擎退出时schemachange信息还保存在在Header里，
    // 引擎重启后，需清除schemachange信息，上层会重做
    uint64_t canceled_num = 0;
    LOG(INFO) << "begin to cancel unfinished schema change.";

    for (const auto& tablet_instance : _tablet_map) {
        for (TabletSharedPtr tablet : tablet_instance.second.table_arr) {
            if (tablet == nullptr) {
                LOG(WARNING) << "tablet does not exist. tablet_id=" << tablet_instance.first;
                continue;
            }
            AlterTabletTaskSharedPtr alter_task = tablet->alter_task();
            // if alter task's state == finished, could not do anything
            if (alter_task == nullptr || alter_task->alter_state() == ALTER_FINISHED) {
                continue;
            }

            OLAPStatus res = tablet->set_alter_state(ALTER_FAILED);
            if (res != OLAP_SUCCESS) {
                LOG(FATAL) << "fail to set alter state. res=" << res
                        << ", base_tablet=" << tablet->full_name();
                return;
            }
            res = tablet->save_meta();
            if (res != OLAP_SUCCESS) {
                LOG(FATAL) << "fail to save base tablet meta. res=" << res
                        << ", base_tablet=" << tablet->full_name();
                return;
            }

            LOG(INFO) << "cancel unfinished alter tablet task. base_tablet=" << tablet->full_name();
            ++canceled_num;
        }
    }

    LOG(INFO) << "finish to cancel unfinished schema change! canceled_num=" << canceled_num;
}

bool TabletManager::check_tablet_id_exist(TTabletId tablet_id) {
    ReadLock rlock(&_tablet_map_lock);
    return _check_tablet_id_exist_unlock(tablet_id);
} // check_tablet_id_exist

bool TabletManager::_check_tablet_id_exist_unlock(TTabletId tablet_id) {
    bool is_exist = false;

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end() && it->second.table_arr.size() != 0) {
        is_exist = true;
    }
    return is_exist;
} // check_tablet_id_exist

void TabletManager::clear() {
    _tablet_map.clear();
    _shutdown_tablets.clear();
} // clear

OLAPStatus TabletManager::create_tablet(const TCreateTabletReq& request,
    std::vector<DataDir*> stores) {
    WriteLock wrlock(&_tablet_map_lock);
    LOG(INFO) << "begin to process create tablet. tablet=" << request.tablet_id
              << ", schema_hash=" << request.tablet_schema.schema_hash;
    OLAPStatus res = OLAP_SUCCESS;
    DorisMetrics::create_tablet_requests_total.increment(1);
    // Make sure create_tablet operation is idempotent:
    // return success if tablet with same tablet_id and schema_hash exist,
    //        false if tablet with same tablet_id but different schema_hash exist
    // during alter, if the tablet(same tabletid and schema hash) already exist
    // then just return true, if tablet id with different schema hash exist, wait report
    // task to delete the tablet
    if (_check_tablet_id_exist_unlock(request.tablet_id)) {
        TabletSharedPtr tablet = _get_tablet_with_no_lock(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (tablet != nullptr) {
            LOG(INFO) << "create tablet success for tablet already exist.";
            return OLAP_SUCCESS;
        } else {
            LOG(WARNING) << "tablet with different schema hash already exists.";
            return OLAP_ERR_CE_TABLET_ID_EXIST;
        }
    }

    TabletSharedPtr ref_tablet = nullptr;
    bool is_schema_change_tablet = false;
    // if the CreateTabletReq has base_tablet_id then it is a alter tablet request
    if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
        is_schema_change_tablet = true;
        ref_tablet = _get_tablet_with_no_lock(request.base_tablet_id, request.base_schema_hash);
        if (ref_tablet == nullptr) {
            LOG(WARNING) << "fail to create new tablet. new_tablet_id=" << request.tablet_id
                         << ", new_schema_hash=" << request.tablet_schema.schema_hash
                         << ", because could not find base tablet, base_tablet_id=" << request.base_tablet_id
                         << ", base_schema_hash=" << request.base_schema_hash;
            return OLAP_ERR_TABLE_CREATE_META_ERROR;
        }
        // schema change should use the same data dir
        stores.clear();
        stores.push_back(ref_tablet->data_dir());
    }
    // set alter type to schema change. it is useless
    TabletSharedPtr tablet = _internal_create_tablet(AlterTabletType::SCHEMA_CHANGE, request, 
        is_schema_change_tablet, ref_tablet, stores);
    if (tablet == nullptr) {
        res = OLAP_ERR_CE_CMD_PARAMS_ERROR;
        LOG(WARNING) << "fail to create tablet. res=" << res;
    }

    LOG(INFO) << "finish to process create tablet. res=" << res;
    return res;
} // create_tablet

TabletSharedPtr TabletManager::create_tablet(const AlterTabletType alter_type, 
        const TCreateTabletReq& request, const bool is_schema_change_tablet,
        const TabletSharedPtr ref_tablet, std::vector<DataDir*> data_dirs) {
    DCHECK(is_schema_change_tablet && ref_tablet != nullptr);
    WriteLock wrlock(&_tablet_map_lock);
    return _internal_create_tablet(alter_type, request, is_schema_change_tablet,
        ref_tablet, data_dirs);
}

TabletSharedPtr TabletManager::_internal_create_tablet(const AlterTabletType alter_type,
        const TCreateTabletReq& request, const bool is_schema_change_tablet,
        const TabletSharedPtr ref_tablet, std::vector<DataDir*> data_dirs) {
    DCHECK((is_schema_change_tablet && ref_tablet != nullptr) || (!is_schema_change_tablet && ref_tablet == nullptr));
    // check if the tablet with specified tablet id and schema hash already exists
    TabletSharedPtr checked_tablet = _get_tablet_with_no_lock(request.tablet_id, request.tablet_schema.schema_hash);
    if (checked_tablet != nullptr) {
        LOG(WARNING) << "failed to create tablet because tablet already exist." 
                     << " tablet id = " << request.tablet_id
                     << " schema hash = " << request.tablet_schema.schema_hash;
        return nullptr;
    }
    bool is_tablet_added = false;
    TabletSharedPtr tablet = _create_tablet_meta_and_dir(request, is_schema_change_tablet, 
        ref_tablet, data_dirs);
    if (tablet == nullptr) {
        return nullptr;
    }

    // TODO(yiguolei)
    // the following code is very difficult to understand because it mixed alter tablet v2 and alter tablet v1
    // should remove alter tablet v1 code after v0.12
    OLAPStatus res = OLAP_SUCCESS;
    do {
        res = tablet->init();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "tablet init failed. tablet:" << tablet->full_name();
            break;
        }
        if (!is_schema_change_tablet || (request.__isset.base_tablet_id && request.base_tablet_id > 0)) {
            // Create init version if this is not a restore mode replica and request.version is set
            // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
            // if (!in_restore_mode && request.__isset.version) {
            // create inital rowset before add it to storage engine could omit many locks
            res = _create_inital_rowset(tablet, request);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to create initial version for tablet. res=" << res;
                break;
            }
        }
        if (is_schema_change_tablet) {
            if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
                LOG(INFO) << "this request is for alter tablet request v2, so that not add alter task to tablet";
                // if this is a new alter tablet, has to set its state to not ready
                // because schema change hanlder depends on it to check whether history data
                // convert finished
                tablet->set_tablet_state(TabletState::TABLET_NOTREADY);
            } else {
                // add alter task to new tablet if it is a new tablet during schema change
                tablet->add_alter_task(ref_tablet->tablet_id(), ref_tablet->schema_hash(), 
                    vector<Version>(), alter_type);
            }
            // 有可能出现以下2种特殊情况：
            // 1. 因为操作系统时间跳变，导致新生成的表的creation_time小于旧表的creation_time时间
            // 2. 因为olap engine代码中统一以秒为单位，所以如果2个操作(比如create一个表,
            //    然后立即alter该表)之间的时间间隔小于1s，则alter得到的新表和旧表的creation_time会相同
            //
            // 当出现以上2种情况时，为了能够区分alter得到的新表和旧表，这里把新表的creation_time设置为
            // 旧表的creation_time加1
            if (tablet->creation_time() <= ref_tablet->creation_time()) {
                LOG(WARNING) << "new tablet's creation time is less than or equal to old tablet"
                            << "new_tablet_creation_time=" << tablet->creation_time()
                            << ", ref_tablet_creation_time=" << ref_tablet->creation_time();
                int64_t new_creation_time = ref_tablet->creation_time() + 1;
                tablet->set_creation_time(new_creation_time);
            }
        }
        // Add tablet to StorageEngine will make it visiable to user
        res = _add_tablet_unlock(request.tablet_id, request.tablet_schema.schema_hash, tablet, true, false);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add tablet to StorageEngine. res=" << res;
            break;
        }
        is_tablet_added = true;
        TabletSharedPtr tablet_ptr = _get_tablet_with_no_lock(request.tablet_id, request.tablet_schema.schema_hash);
        if (tablet_ptr == nullptr) {
            res = OLAP_ERR_TABLE_NOT_FOUND;
            LOG(WARNING) << "fail to get tablet. res=" << res;
            break;
        }
    } while (0);

    // should remove the pending path of tablet id no matter create tablet success or not
    tablet->data_dir()->remove_pending_ids(TABLET_ID_PREFIX + std::to_string(request.tablet_id));

    // clear environment
    if (res != OLAP_SUCCESS) {
        DorisMetrics::create_tablet_requests_failed.increment(1);
        if (is_tablet_added) {
            OLAPStatus status = _drop_tablet_unlock(
                    request.tablet_id, request.tablet_schema.schema_hash, false);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to drop tablet when create tablet failed. res=" << res;
            }
        } else {
            tablet->delete_all_files();
            TabletMetaManager::remove(tablet->data_dir(), request.tablet_id, request.tablet_schema.schema_hash);
        }
        return nullptr;
    } else {
        LOG(INFO) << "finish to process create tablet. res=" << res;
        return tablet;
    }
} // create_tablet

TabletSharedPtr TabletManager::_create_tablet_meta_and_dir(
        const TCreateTabletReq& request, const bool is_schema_change_tablet,
        const TabletSharedPtr ref_tablet, std::vector<DataDir*> data_dirs) {
    TabletSharedPtr tablet;
    // Try to create tablet on each of all_available_root_path, util success
    DataDir* last_dir = nullptr; 
    for (auto& data_dir : data_dirs) {
        if (last_dir != nullptr) {
            // if last dir != null, it means preivous create tablet retry failed
            last_dir->remove_pending_ids(TABLET_ID_PREFIX + std::to_string(request.tablet_id));
        }
        last_dir = data_dir;
        TabletMetaSharedPtr tablet_meta;
        // if create meta faild, do not need to clean dir, because it is only in memory
        OLAPStatus res = _create_tablet_meta(request, data_dir, is_schema_change_tablet, ref_tablet, &tablet_meta);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create tablet meta. res=" << res << ", root=" << data_dir->path();
            continue;
        }
        

        stringstream schema_hash_dir_stream;
        schema_hash_dir_stream << data_dir->path()
                << DATA_PREFIX
                << "/" << tablet_meta->shard_id()
                << "/" << request.tablet_id
                << "/" << request.tablet_schema.schema_hash;
        string schema_hash_dir = schema_hash_dir_stream.str();
        boost::filesystem::path schema_hash_path(schema_hash_dir);
        boost::filesystem::path tablet_path = schema_hash_path.parent_path();
        std::string tablet_dir = tablet_path.string();
        // because the tablet is removed async, so that the dir may still exist
        // when be receive create tablet again. For example redo schema change
        if (check_dir_existed(schema_hash_dir)) {
            LOG(WARNING) << "skip this dir because tablet path exist, path="<< schema_hash_dir;
            continue;
        } else {
            data_dir->add_pending_ids(TABLET_ID_PREFIX + std::to_string(request.tablet_id));
            res = create_dirs(schema_hash_dir);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "create dir fail. [res=" << res << " path:" << schema_hash_dir;
                continue;
            }
        }

        tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
        if (tablet == nullptr) {
            LOG(WARNING) << "fail to load tablet from tablet_meta. root_path:" << data_dir->path();
            res = remove_all_dir(tablet_dir);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "remove tablet dir:" << tablet_dir;
            }
            continue;
        }
        break;
    }
    return tablet;
}

// Drop tablet specified, the main logical is as follows:
// 1. tablet not in schema change:
//      drop specified tablet directly;
// 2. tablet in schema change:
//      a. schema change not finished && dropped tablet is base :
//          base tablet cannot be dropped;
//      b. other cases:
//          drop specified tablet and clear schema change info.
OLAPStatus TabletManager::drop_tablet(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    WriteLock wlock(&_tablet_map_lock);
    return _drop_tablet_unlock(tablet_id, schema_hash, keep_files);
} // drop_tablet


// Drop tablet specified, the main logical is as follows:
// 1. tablet not in schema change:
//      drop specified tablet directly;
// 2. tablet in schema change:
//      a. schema change not finished && dropped tablet is base :
//          base tablet cannot be dropped;
//      b. other cases:
//          drop specified tablet and clear schema change info.
OLAPStatus TabletManager::_drop_tablet_unlock(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    LOG(INFO) << "begin to process drop tablet."
        << "tablet=" << tablet_id << ", schema_hash=" << schema_hash;
    DorisMetrics::drop_tablet_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    // Get tablet which need to be droped
    TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    if (dropped_tablet == nullptr) {
        LOG(WARNING) << "tablet to drop does not exist already."
                     << " tablet_id=" << tablet_id
                     << ", schema_hash=" << schema_hash;
        return OLAP_SUCCESS;
    }

    // Try to get schema change info
    AlterTabletTaskSharedPtr alter_task = dropped_tablet->alter_task();

    // Drop tablet directly when not in schema change
    if (alter_task == nullptr) {
        return _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    }

    AlterTabletState alter_state = alter_task->alter_state();
    TTabletId related_tablet_id = alter_task->related_tablet_id();
    TSchemaHash related_schema_hash = alter_task->related_schema_hash();;

    // Check tablet is in schema change or not, is base tablet or not
    bool is_schema_change_finished = (alter_state == ALTER_FINISHED || alter_state == ALTER_FAILED);

    bool is_drop_base_tablet = false;
    TabletSharedPtr related_tablet = _get_tablet_with_no_lock(
            related_tablet_id, related_schema_hash);
    if (related_tablet == nullptr) {
        LOG(WARNING) << "drop tablet directly when related tablet not found. "
                     << " tablet_id=" << related_tablet_id
                     << " schema_hash=" << related_schema_hash;
        return _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    }

    if (dropped_tablet->creation_time() < related_tablet->creation_time()) {
        is_drop_base_tablet = true;
    }

    if (is_drop_base_tablet && !is_schema_change_finished) {
        LOG(WARNING) << "base tablet in schema change cannot be droped. tablet="
                     << dropped_tablet->full_name();
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // Drop specified tablet and clear schema change info
    // must first break the link and then drop the tablet
    // if drop tablet, then break link. the link maybe exists but the tablet 
    // not exist when be restarts
    related_tablet->obtain_header_wrlock();
    // should check the related tablet id in alter task is current tablet to be dropped
    // A related to B, BUT B related to C
    // if drop A, should not clear B's alter task
    AlterTabletTaskSharedPtr related_alter_task = related_tablet->alter_task();
    if (related_alter_task != nullptr && related_alter_task->related_tablet_id() == tablet_id
        && related_alter_task->related_schema_hash() == schema_hash) {
        related_tablet->delete_alter_task();
        res = related_tablet->save_meta();
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "fail to save tablet header. res=" << res
                    << ", tablet=" << related_tablet->full_name();
        }
    }
    related_tablet->release_header_lock();
    res = _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to drop tablet which in schema change. tablet="
                     << dropped_tablet->full_name();
        return res;
    }

    LOG(INFO) << "finish to drop tablet. res=" << res;
    return res;
} // drop_tablet_unlock

OLAPStatus TabletManager::drop_tablets_on_error_root_path(
        const vector<TabletInfo>& tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    WriteLock wlock(&_tablet_map_lock);

    for (const TabletInfo& tablet_info : tablet_info_vec) {
        TTabletId tablet_id = tablet_info.tablet_id;
        TSchemaHash schema_hash = tablet_info.schema_hash;
        VLOG(3) << "drop_tablet begin. tablet_id=" << tablet_id
                << ", schema_hash=" << schema_hash;
        TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
        if (dropped_tablet == nullptr) {
            LOG(WARNING) << "dropping tablet not exist. " 
                         << " tablet=" << tablet_id
                         << " schema_hash=" << schema_hash;
            continue;
        } else {
            for (list<TabletSharedPtr>::iterator it = _tablet_map[tablet_id].table_arr.begin();
                    it != _tablet_map[tablet_id].table_arr.end();) {
                if ((*it)->equal(tablet_id, schema_hash)) {
                    it = _tablet_map[tablet_id].table_arr.erase(it);
                    _partition_tablet_map[(*it)->partition_id()].erase((*it)->get_tablet_info());
                    if (_partition_tablet_map[(*it)->partition_id()].empty()) {
                        _partition_tablet_map.erase((*it)->partition_id());
                    } 
                } else {
                    ++it;
                }
            }
        }
    }

    return res;
} // drop_tablets_on_error_root_path

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                          bool include_deleted, std::string* err) {
    ReadLock rlock(&_tablet_map_lock);
    return _get_tablet(tablet_id, schema_hash, include_deleted, err);
} // get_tablet

TabletSharedPtr TabletManager::_get_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                           bool include_deleted, std::string* err) {
    TabletSharedPtr tablet;
    tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    if (tablet == nullptr && include_deleted) {
        for (auto& deleted_tablet : _shutdown_tablets) {
            CHECK(deleted_tablet != nullptr) << "deleted tablet in nullptr";
            if (deleted_tablet->tablet_id() == tablet_id && deleted_tablet->schema_hash() == schema_hash) {
                tablet = deleted_tablet;
                break;
            }
        }
    }

    if (tablet != nullptr) {
        if (!tablet->is_used()) {
            LOG(WARNING) << "tablet cannot be used. tablet=" << tablet_id;
            if (err != nullptr) { *err = "tablet cannot be used"; }
            tablet.reset();
        }
    } else if (err != nullptr) {
        *err = "tablet does not exist";
    }

    return tablet;
} // get_tablet

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                          TabletUid tablet_uid, bool include_deleted,
                                          std::string* err) {
    ReadLock rlock(&_tablet_map_lock);
    TabletSharedPtr tablet = _get_tablet(tablet_id, schema_hash, include_deleted, err);
    if (tablet != nullptr && tablet->tablet_uid() == tablet_uid) {
        return tablet;
    }
    return nullptr;
} // get_tablet

bool TabletManager::get_tablet_id_and_schema_hash_from_path(
        const std::string& path, TTabletId* tablet_id, TSchemaHash* schema_hash) {
    static re2::RE2 normal_re("/data/\\d+/(\\d+)/(\\d+)($|/)");
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

bool TabletManager::get_rowset_id_from_path(const std::string& path, RowsetId* rowset_id) {
    static re2::RE2 re("/data/\\d+/\\d+/\\d+/([A-Fa-f0-9]+)_.*");
    std::string id_str;
    bool ret = RE2::PartialMatch(path, re, &id_str);
    if (ret) {
        rowset_id->init(id_str);
        return true;
    }
    return false;
}

void TabletManager::get_tablet_stat(TTabletStatResult& result) {
    VLOG(3) << "begin to get all tablet stat.";

    // get current time
    int64_t current_time = UnixMillis();

    // update cache if too old
    {
        std::lock_guard<std::mutex> l(_tablet_stat_mutex);
        if (current_time - _tablet_stat_cache_update_time_ms >
                config::tablet_stat_cache_update_interval_second * 1000) {
            VLOG(3) << "update tablet stat.";
            _build_tablet_stat();
        }
    }

    result.__set_tablets_stats(_tablet_stat_cache);
} // get_tablet_stat

TabletSharedPtr TabletManager::find_best_tablet_to_compaction(
            CompactionType compaction_type, DataDir* data_dir) {
    ReadLock tablet_map_rdlock(&_tablet_map_lock);
    uint32_t highest_score = 0;
    TabletSharedPtr best_tablet;
    int64_t now = UnixMillis();
    for (tablet_map_t::value_type& table_ins : _tablet_map){
        for (TabletSharedPtr& table_ptr : table_ins.second.table_arr) {
            AlterTabletTaskSharedPtr cur_alter_task = table_ptr->alter_task();
            if (cur_alter_task != nullptr && cur_alter_task->alter_state() != ALTER_FINISHED 
                && cur_alter_task->alter_state() != ALTER_FAILED) {
                    TabletSharedPtr related_tablet = _get_tablet_with_no_lock(cur_alter_task->related_tablet_id(), 
                        cur_alter_task->related_schema_hash());
                    if (related_tablet != nullptr && table_ptr->creation_time() > related_tablet->creation_time()) {
                        // it means cur tablet is a new tablet during schema change or rollup, skip compaction
                        continue;
                    }
            }
            // if tablet is not ready, it maybe a new tablet under schema change, not do compaction
            if (table_ptr->tablet_state() == TABLET_NOTREADY) {
                continue;
            }

            if (table_ptr->data_dir()->path_hash() != data_dir->path_hash()
                    || !table_ptr->is_used() || !table_ptr->init_succeeded() || !table_ptr->can_do_compaction()) {
                continue;
            }

            if (now - table_ptr->last_compaction_failure_time() <= config::min_compaction_failure_interval_sec * 1000) {
                continue;
            }

            if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                MutexLock lock(table_ptr->get_cumulative_lock(), TRY_LOCK);
                if (!lock.own_lock()) {
                    continue;
                }
            }

            if (compaction_type == CompactionType::BASE_COMPACTION) {
                MutexLock lock(table_ptr->get_base_lock(), TRY_LOCK);
                if (!lock.own_lock()) {
                    continue;
                }
            }

            ReadLock rdlock(table_ptr->get_header_lock_ptr());
            uint32_t table_score = 0;
            if (compaction_type == CompactionType::BASE_COMPACTION) {
                table_score = table_ptr->calc_base_compaction_score();
            } else if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                table_score = table_ptr->calc_cumulative_compaction_score();
            }
            if (table_score > highest_score) {
                highest_score = table_score;
                best_tablet = table_ptr;
            }
        }
    }

    if (best_tablet != nullptr) {
        LOG(INFO) << "find best tablet to do compaction."
            << " type: " << (compaction_type == CompactionType::CUMULATIVE_COMPACTION ? "cumulative" : "base")
            << ", tablet id: " << best_tablet->tablet_id() << ", score: " << highest_score;
    }
    return best_tablet;
}

OLAPStatus TabletManager::load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id,
        TSchemaHash schema_hash, const std::string& meta_binary, bool update_meta, bool force) {
    WriteLock wlock(&_tablet_map_lock);
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    OLAPStatus status = tablet_meta->deserialize(meta_binary);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "parse meta_binary string failed for tablet_id:" << tablet_id << ", schema_hash:" << schema_hash;
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }

    // check if tablet meta is valid
    if (tablet_meta->tablet_id() != tablet_id || tablet_meta->schema_hash() != schema_hash) {
        LOG(WARNING) << "tablet meta load from meta is invalid"
                   << " input tablet id=" << tablet_id
                   << " input tablet schema_hash=" << schema_hash
                   << " meta tablet=" << tablet_meta->full_name();
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }
    if (tablet_meta->tablet_uid().hi == 0 && tablet_meta->tablet_uid().lo == 0) {
        LOG(WARNING) << "not load this tablet because uid == 0"
                  << " tablet=" << tablet_meta->full_name();
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }

    // init must be called
    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to new tablet. tablet_id=" << tablet_id << ", schema_hash:" << schema_hash;
        return OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR;
    }

    if (tablet_meta->tablet_state() == TABLET_SHUTDOWN) {
        LOG(INFO) << "tablet is to be deleted, skip load it"
                  << " tablet id = " << tablet_meta->tablet_id()
                  << " schema hash = " << tablet_meta->schema_hash();
        _shutdown_tablets.push_back(tablet);
        return OLAP_ERR_TABLE_ALREADY_DELETED_ERROR;
    }
    // not check tablet init version because when be restarts during alter task the new tablet may be empty
    if (tablet->max_version().first == -1 && tablet->tablet_state() == TABLET_RUNNING) {	
        LOG(WARNING) << "tablet is in running state without delta is invalid."	
                     << "tablet=" << tablet->full_name();	
        // tablet state is invalid, drop tablet	
        return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;	
    }

    OLAPStatus res = tablet->init();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "tablet init failed. tablet:" << tablet->full_name();
        return res;
    }
    res = _add_tablet_unlock(tablet_id, schema_hash, tablet, update_meta, force);
    if (res != OLAP_SUCCESS) {
        // insert existed tablet return OLAP_SUCCESS
        if (res == OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE) {
            LOG(WARNING) << "add duplicate tablet. tablet=" << tablet->full_name();
        }

        LOG(WARNING) << "failed to add tablet. tablet=" << tablet->full_name();
        return res;
    }

    return OLAP_SUCCESS;
} // load_tablet_from_meta

OLAPStatus TabletManager::load_tablet_from_dir(
        DataDir* store, TTabletId tablet_id, SchemaHash schema_hash,
        const string& schema_hash_path, bool force) {
    LOG(INFO) << "begin to load tablet from dir. " 
                << " tablet_id=" << tablet_id
                << " schema_hash=" << schema_hash
                << " path = " << schema_hash_path;
    // not add lock here, because load_tablet_from_meta already add lock
    string header_path = TabletMeta::construct_header_file_path(schema_hash_path, tablet_id);
    // should change shard id before load tablet
    path boost_header_path(header_path);
    std::string shard_path = boost_header_path.parent_path().parent_path().parent_path().string();
    std::string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    int32_t shard = stol(shard_str);
    // load dir is called by clone, restore, storage migration
    // should change tablet uid when tablet object changed
    OLAPStatus res = TabletMeta::reset_tablet_uid(header_path);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to set tablet uid when copied tablet meta file"
                     << " header_path=" << header_path;
        return res;
    }
    TabletMetaSharedPtr tablet_meta(new(nothrow) TabletMeta());
    do {
        if (access(header_path.c_str(), F_OK) != 0) {
            LOG(WARNING) << "fail to find header file. [header_path=" << header_path << "]";
            res = OLAP_ERR_FILE_NOT_EXIST;
            break;
        }
        if (tablet_meta == nullptr) {
            LOG(WARNING) << "fail to malloc TabletMeta.";
            res = OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
            break;
        }

        if (tablet_meta->create_from_file(header_path) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load tablet_meta. file_path=" << header_path;
            res = OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
            break;
        }
        // has to change shard id here, because meta file maybe copyed from other source
        // its shard is different from local shard
        tablet_meta->set_shard_id(shard);
        std::string meta_binary;
        tablet_meta->serialize(&meta_binary);
        res = load_tablet_from_meta(store, tablet_id, schema_hash, meta_binary, true, force);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load tablet. [header_path=" << header_path << "]";
            res = OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
            break;
        }
    } while (0);
    return res;
} // load_tablet_from_dir

void TabletManager::release_schema_change_lock(TTabletId tablet_id) {
    VLOG(3) << "release_schema_change_lock begin. tablet_id=" << tablet_id;
    ReadLock rlock(&_tablet_map_lock);

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it == _tablet_map.end()) {
        LOG(WARNING) << "tablet does not exists. tablet=" << tablet_id;
    } else {
        it->second.schema_change_lock.unlock();
    }
    VLOG(3) << "release_schema_change_lock end. tablet_id=" << tablet_id;
} // release_schema_change_lock

OLAPStatus TabletManager::report_tablet_info(TTabletInfo* tablet_info) {
    DorisMetrics::report_tablet_requests_total.increment(1);
    LOG(INFO) << "begin to process report tablet info."
              << "tablet_id=" << tablet_info->tablet_id
              << ", schema_hash=" << tablet_info->schema_hash;

    OLAPStatus res = OLAP_SUCCESS;

    TabletSharedPtr tablet = get_tablet(
            tablet_info->tablet_id, tablet_info->schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "can't find tablet. " 
                     << " tablet=" << tablet_info->tablet_id
                     << " schema_hash=" << tablet_info->schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    tablet->build_tablet_report_info(tablet_info);
    VLOG(10) << "success to process report tablet info.";
    return res;
} // report_tablet_info

OLAPStatus TabletManager::report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    LOG(INFO) << "begin to process report all tablets info.";

    // build the expired txn map first, outside the tablet map lock
    std::map<TabletInfo, std::set<int64_t>> expire_txn_map;
    StorageEngine::instance()->txn_manager()->build_expire_txn_map(&expire_txn_map);

    ReadLock rlock(&_tablet_map_lock);
    DorisMetrics::report_all_tablets_requests_total.increment(1);

    if (tablets_info == nullptr) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    for (const auto& item : _tablet_map) {
        if (item.second.table_arr.size() == 0) {
            continue;
        }

        TTablet tablet;
        for (TabletSharedPtr tablet_ptr : item.second.table_arr) {
            if (tablet_ptr == nullptr) {
                continue;
            }

            TTabletInfo tablet_info;
            tablet_ptr->build_tablet_report_info(&tablet_info);

            // find expire transaction corresponding to this tablet
            TabletInfo tinfo = TabletInfo(tablet_ptr->tablet_id(), tablet_ptr->schema_hash(), tablet_ptr->tablet_uid());
            vector<int64_t> transaction_ids;
            auto find = expire_txn_map.find(tinfo);
            if (find != expire_txn_map.end()) {
                for(auto& it : find->second) {
                    transaction_ids.push_back(it);
                }
            }
            tablet_info.__set_transaction_ids(transaction_ids);

            tablet.tablet_infos.push_back(tablet_info);
        }

        if (tablet.tablet_infos.size() != 0) {
            tablets_info->insert(pair<TTabletId, TTablet>(tablet.tablet_infos[0].tablet_id, tablet));
        }
    }

    LOG(INFO) << "success to process report all tablets info. tablet_num=" << tablets_info->size();
    return OLAP_SUCCESS;
} // report_all_tablets_info

OLAPStatus TabletManager::start_trash_sweep() {
    {
        ReadLock rlock(&_tablet_map_lock);
        std::vector<int64_t> tablets_to_clean;
        for (auto& item : _tablet_map) {
            // try to clean empty item
            if (item.second.table_arr.empty()) {
                // try to get schema change lock if could get schema change lock, then nobody 
                // own the lock could remove the item
                // it will core if schema change thread may hold the lock and this thread will deconstruct lock
                if (item.second.schema_change_lock.trylock() == OLAP_SUCCESS) {
                    item.second.schema_change_lock.unlock();
                    tablets_to_clean.push_back(item.first);
                }
            }
            for (TabletSharedPtr tablet : item.second.table_arr) {
                if (tablet == nullptr) {
                    continue;
                }
                tablet->delete_expired_inc_rowsets();
            }
        }
        // clean empty tablet id item
        for (const auto& tablet_id_to_clean : tablets_to_clean) {
            if (_tablet_map[tablet_id_to_clean].table_arr.empty()) {
                _tablet_map.erase(tablet_id_to_clean);
            }
        }
    }

    int32_t clean_num = 0;
    do {
        sleep(1);
        clean_num = 0;
        // should get write lock here, because it will remove tablet from shut_down_tablets
        // and get tablet will access shut_down_tablets
        WriteLock wlock(&_tablet_map_lock);
        auto it = _shutdown_tablets.begin();
        for (; it != _shutdown_tablets.end();) { 
            // check if the meta has the tablet info and its state is shutdown
            if (it->use_count() > 1) {
                // it means current tablet is referenced in other thread
                ++it;
                continue;
            }
            TabletMetaSharedPtr new_tablet_meta(new(nothrow) TabletMeta());
            if (new_tablet_meta == nullptr) {
                LOG(WARNING) << "fail to malloc TabletMeta.";
                ++it;
                continue;
            }
            OLAPStatus check_st = TabletMetaManager::get_meta((*it)->data_dir(), 
                (*it)->tablet_id(), (*it)->schema_hash(), new_tablet_meta);
            if (check_st == OLAP_SUCCESS) {
                if (new_tablet_meta->tablet_state() != TABLET_SHUTDOWN
                    || new_tablet_meta->tablet_uid() != (*it)->tablet_uid()) {
                    LOG(WARNING) << "tablet's state changed to normal, skip remove dirs"
                                << " tablet id = " << new_tablet_meta->tablet_id()
                                << " schema hash = " << new_tablet_meta->schema_hash()
                                << " old tablet_uid=" << (*it)->tablet_uid()
                                << " cur tablet_uid=" << new_tablet_meta->tablet_uid();
                    // remove it from list
                    it = _shutdown_tablets.erase(it);
                    continue;
                }
                if (check_dir_existed((*it)->tablet_path())) {
                    // take snapshot of tablet meta
                    std::string meta_file = (*it)->tablet_path() + "/" + std::to_string((*it)->tablet_id()) + ".hdr";
                    (*it)->tablet_meta()->save(meta_file);
                    LOG(INFO) << "start to move path to trash" 
                            << " tablet path = " << (*it)->tablet_path();
                    OLAPStatus rm_st = move_to_trash((*it)->tablet_path(), (*it)->tablet_path());
                    if (rm_st != OLAP_SUCCESS) {
                        LOG(WARNING) << "failed to move dir to trash"
                                    << " dir = " << (*it)->tablet_path();
                        ++it;
                        continue;
                    }
                }
                TabletMetaManager::remove((*it)->data_dir(), (*it)->tablet_id(), (*it)->schema_hash());
                LOG(INFO) << "successfully move tablet to trash." 
                            << " tablet id " << (*it)->tablet_id()
                            << " schema hash " << (*it)->schema_hash()
                            << " tablet path " << (*it)->tablet_path();
                it = _shutdown_tablets.erase(it);
                ++ clean_num;
            } else {
                // if could not find tablet info in meta store, then check if dir existed
                if (check_dir_existed((*it)->tablet_path())) {
                    LOG(WARNING) << "errors while load meta from store, skip this tablet" 
                                << " tablet id " << (*it)->tablet_id()
                                << " schema hash " << (*it)->schema_hash();
                    ++it;
                } else {
                    LOG(INFO) << "could not find tablet dir, skip move to trash, remove it from gc queue." 
                            << " tablet id " << (*it)->tablet_id()
                            << " schema hash " << (*it)->schema_hash()
                            << " tablet path " << (*it)->tablet_path();
                    it = _shutdown_tablets.erase(it);
                }
            }

            // if clean 100 tablets, should yield
            if (clean_num >= 200) {
                break;
            }
        }
    } while (clean_num >= 200);
    return OLAP_SUCCESS;
} // start_trash_sweep

bool TabletManager::try_schema_change_lock(TTabletId tablet_id) {
    bool res = false;
    VLOG(3) << "try_schema_change_lock begin. tablet_id=" << tablet_id;
    ReadLock rlock(&_tablet_map_lock);

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it == _tablet_map.end()) {
        LOG(WARNING) << "tablet does not exists. tablet_id=" << tablet_id;
    } else {
        res = (it->second.schema_change_lock.trylock() == OLAP_SUCCESS);
    }
    VLOG(3) << "try_schema_change_lock end. tablet_id=" <<  tablet_id;
    return res;
} // try_schema_change_lock

void TabletManager::update_root_path_info(std::map<std::string, DataDirInfo>* path_map,
    int* tablet_counter) {
    ReadLock rlock(&_tablet_map_lock);
    for (auto& entry : _tablet_map) {
        TableInstances& instance = entry.second;
        for (auto& tablet : instance.table_arr) {
            (*tablet_counter) ++ ;
            int64_t data_size = tablet->tablet_footprint();
            auto find = path_map->find(tablet->data_dir()->path());
            if (find == path_map->end()) {
                continue;
            }
            if (find->second.is_used) {
                find->second.data_used_capacity += data_size;
            }
        }
    }
} // update_root_path_info

void TabletManager::get_partition_related_tablets(int64_t partition_id, std::set<TabletInfo>* tablet_infos) {
    ReadLock rlock(&_tablet_map_lock);
    if (_partition_tablet_map.find(partition_id) != _partition_tablet_map.end()) {
        for (auto& tablet_info : _partition_tablet_map[partition_id]) {
            tablet_infos->insert(tablet_info);
        }
    }
}

void TabletManager::do_tablet_meta_checkpoint(DataDir* data_dir) {
    vector<TabletSharedPtr> related_tablets;
    {
        ReadLock tablet_map_rdlock(&_tablet_map_lock);
        for (tablet_map_t::value_type& table_ins : _tablet_map){
            for (TabletSharedPtr& table_ptr : table_ins.second.table_arr) {
                // if tablet is not ready, it maybe a new tablet under schema change, not do compaction
                if (table_ptr->tablet_state() != TABLET_RUNNING) {
                    continue;
                }

                if (table_ptr->data_dir()->path_hash() != data_dir->path_hash()
                        || !table_ptr->is_used() || !table_ptr->init_succeeded()) {
                    continue;
                }
                related_tablets.push_back(table_ptr);
            }
        }
    }
    for (TabletSharedPtr tablet : related_tablets) {
        tablet->do_tablet_meta_checkpoint();
    }
    return;
}

void TabletManager::_build_tablet_stat() {
    _tablet_stat_cache.clear();

    ReadLock rdlock(&_tablet_map_lock);
    for (const auto& item : _tablet_map) {
        if (item.second.table_arr.size() == 0) {
            continue;
        }

        TTabletStat stat;
        stat.tablet_id = item.first;
        for (TabletSharedPtr tablet : item.second.table_arr) {
            if (tablet == nullptr) {
                continue;
            }
            // we only get base tablet's stat
            stat.__set_data_size(tablet->tablet_footprint());
            stat.__set_row_num(tablet->num_rows());
            VLOG(3) << "tablet_id=" << item.first
                    << ", data_size=" << tablet->tablet_footprint()
                    << ", row_num:" << tablet->num_rows();
            break;
        }

        _tablet_stat_cache.emplace(item.first, stat);
    }

    _tablet_stat_cache_update_time_ms = UnixMillis();
}

OLAPStatus TabletManager::_create_inital_rowset(
        TabletSharedPtr tablet, const TCreateTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;

    if (request.version < 1) {
        LOG(WARNING) << "init version of tablet should at least 1.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    } else {
        Version version(0, request.version);
        VLOG(3) << "begin to create init version. "
                << "begin=" << version.first << ", end=" << version.second;
        RowsetSharedPtr new_rowset;
        do {
            if (version.first > version.second) {
                LOG(WARNING) << "begin should not larger than end." 
                            << " begin=" << version.first
                            << " end=" << version.second;
                res = OLAP_ERR_INPUT_PARAMETER_ERROR;
                break;
            }
            RowsetWriterContext context;
            context.rowset_id = StorageEngine::instance()->next_rowset_id();
            context.tablet_uid = tablet->tablet_uid();
            context.tablet_id = tablet->tablet_id();
            context.partition_id = tablet->partition_id();
            context.tablet_schema_hash = tablet->schema_hash();
            context.rowset_type = DEFAULT_ROWSET_TYPE;
            context.rowset_path_prefix = tablet->tablet_path();
            context.tablet_schema = &(tablet->tablet_schema());
            context.rowset_state = VISIBLE;
            context.data_dir = tablet->data_dir();
            context.version = version;
            context.version_hash = request.version_hash;

            std::unique_ptr<RowsetWriter> builder;
            res = RowsetFactory::create_rowset_writer(context, &builder);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to init rowset writer for tablet " << tablet->full_name();
                break;
            }
            res = builder->flush();
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to flush rowset writer for tablet " << tablet->full_name();
                break;
            }

            new_rowset = builder->build();
            res = tablet->add_rowset(new_rowset, false);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to add rowset for tablet " << tablet->full_name();
                break;
            }
        } while (0);

        // Unregister index and delete files(index and data) if failed
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create init base version. "
                         << " res=" << res 
                         << " version=" << request.version;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            return res;
        }
    }
    tablet->set_cumulative_layer_point(request.version + 1);
    // should not save tablet meta here, because it will be saved if add to map successfully
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to save header. [tablet=" << tablet->full_name() << "]";
    }

    return res;
}

OLAPStatus TabletManager::_create_tablet_meta(
        const TCreateTabletReq& request,
        DataDir* store,
        const bool is_schema_change_tablet,
        const TabletSharedPtr ref_tablet,
        TabletMetaSharedPtr* tablet_meta) {
    uint64_t shard_id = 0;
    OLAPStatus res = store->get_shard(&shard_id);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to get root path shard. res=" << res;
        return res;
    }

    uint32_t next_unique_id = 0;
    uint32_t col_ordinal = 0;
    std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
    if (!is_schema_change_tablet) {
        for (TColumn column : request.tablet_schema.columns) {
            col_ordinal_to_unique_id[col_ordinal] = col_ordinal;
            col_ordinal++;
        }
        next_unique_id = col_ordinal;
    } else {
        next_unique_id = ref_tablet->next_unique_id();
        size_t num_columns = ref_tablet->num_columns();
        size_t field = 0;
        for (TColumn column : request.tablet_schema.columns) {
            /*
             * for schema change, compare old_tablet and new_tablet
             * 1. if column in both new_tablet and old_tablet,
             * assign unique_id of old_tablet to the column of new_tablet
             * 2. if column exists only in new_tablet, assign next_unique_id of old_tablet
             * to the new column
             *
            */
            for (field = 0 ; field < num_columns; ++field) {
                if (ref_tablet->tablet_schema().column(field).name() == column.column_name) {
                    uint32_t unique_id = ref_tablet->tablet_schema().column(field).unique_id();
                    col_ordinal_to_unique_id[col_ordinal] = unique_id;
                    break;
                }
            }
            if (field == num_columns) {
                col_ordinal_to_unique_id[col_ordinal] = next_unique_id;
                next_unique_id++;
            }
            col_ordinal++;
        }
    }

    LOG(INFO) << "next_unique_id:" << next_unique_id;
    // it is a new tablet meta obviously, should generate a new tablet id
    TabletUid  tablet_uid = TabletUid::gen_uid();
    res = TabletMeta::create(request.table_id, request.partition_id,
                       request.tablet_id, request.tablet_schema.schema_hash,
                       shard_id, request.tablet_schema,
                       next_unique_id, col_ordinal_to_unique_id,
                       tablet_meta, tablet_uid);
    return res;
}

OLAPStatus TabletManager::_drop_tablet_directly_unlocked(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    OLAPStatus res = OLAP_SUCCESS;

    TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    if (dropped_tablet == nullptr) {
        LOG(WARNING) << "fail to drop not existed tablet. " 
                     << " tablet_id=" << tablet_id
                     << " schema_hash=" << schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    for (list<TabletSharedPtr>::iterator it = _tablet_map[tablet_id].table_arr.begin();
            it != _tablet_map[tablet_id].table_arr.end();) {
        if ((*it)->equal(tablet_id, schema_hash)) {
            TabletSharedPtr tablet = *it;
            _partition_tablet_map[(*it)->partition_id()].erase((*it)->get_tablet_info());
            if (_partition_tablet_map[(*it)->partition_id()].empty()) {
                _partition_tablet_map.erase((*it)->partition_id());
            } 
            it = _tablet_map[tablet_id].table_arr.erase(it);
            if (!keep_files) {
                // drop tablet will update tablet meta, should lock
                WriteLock wrlock(tablet->get_header_lock_ptr()); 
                LOG(INFO) << "set tablet to shutdown state and remove it from memory"
                          << " tablet_id=" << tablet_id
                          << " schema_hash=" << schema_hash
                          << " tablet path=" << dropped_tablet->tablet_path();
                // has to update tablet here, must not update tablet meta directly
                // because other thread may hold the tablet object, they may save meta too
                // if update meta directly here, other thread may override the meta
                // and the tablet will be loaded at restart time.
                tablet->set_tablet_state(TABLET_SHUTDOWN);
                res = tablet->save_meta();
                if (res != OLAP_SUCCESS) {
                    LOG(WARNING) << "fail to drop tablet. " 
                                 << " tablet_id=" << tablet_id
                                 << " schema_hash=" << schema_hash;
                    return res;
                }
                _shutdown_tablets.push_back(tablet);
            }
        } else {
            ++it;
        }
    }

    res = dropped_tablet->deregister_tablet_from_dir();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to unregister from root path. " 
                     << " res= " << res
                     << " tablet=" << tablet_id;
    }

    return res;
} // _drop_tablet_directly_unlocked

TabletSharedPtr TabletManager::_get_tablet_with_no_lock(TTabletId tablet_id, SchemaHash schema_hash) {
    VLOG(3) << "begin to get tablet. tablet_id=" << tablet_id
            << ", schema_hash=" << schema_hash;
    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end()) {
        for (TabletSharedPtr tablet : it->second.table_arr) {
            CHECK(tablet != nullptr) << "tablet is nullptr:" << tablet;
            if (tablet->equal(tablet_id, schema_hash)) {
                VLOG(3) << "get tablet success. tablet_id=" << tablet_id
                        << ", schema_hash=" << schema_hash;
                return tablet;
            }
        }
    }

    VLOG(3) << "fail to get tablet. tablet_id=" << tablet_id
            << ", schema_hash=" << schema_hash;
    // Return empty tablet if fail
    TabletSharedPtr tablet;
    return tablet;
} // _get_tablet_with_no_lock

} // doris
