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

#include "olap/storage_engine.h"

#include <signal.h>

#include <algorithm>
#include <cstdio>
#include <new>
#include <queue>
#include <set>
#include <random>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <rapidjson/document.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "agent/file_downloader.h"
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
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_meta_manager.h"
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

TxnManager* TxnManager::_s_instance = nullptr;
std::mutex TxnManager::_mlock;

TxnManager* TxnManager::instance() {
    if (_s_instance == nullptr) {
        std::lock_guard<std::mutex> lock(_mlock);
        if (_s_instance == nullptr) {
            _s_instance = new TxnManager();
        }
    }
    return _s_instance;
}

TxnManager::TxnManager() {
    for (int i = 0; i < _txn_lock_num; ++i) {
        _txn_locks[i] = std::make_shared<RWMutex>();
    }
}

// prepare txn should always be allowed because ingest task will be retried 
// could not distinguish rollup, schema change or base table, prepare txn successfully will allow
// ingest retried
OLAPStatus TxnManager::prepare_txn(
    TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, 
    const PUniqueId& load_id) {

    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    WriteLock wrlock(_get_txn_lock(transaction_id));
    WriteLock txn_wrlock(&_txn_map_lock);
    auto it = _txn_tablet_map.find(key);
    if (it != _txn_tablet_map.end()) {
        auto load_itr = it->second.find(tablet_info);
        if (load_itr != it->second.end()) {
            // found load for txn,tablet
            // case 1: user commit rowset, then the load id must be equal
            std::pair<PUniqueId, RowsetSharedPtr>& load_info = load_itr->second;
            // check if load id is equal
            if (load_info.first.hi() == load_id.hi() 
                && load_info.first.lo() == load_id.lo()
                && load_info.second != NULL) {
                LOG(WARNING) << "find transaction exists when add to engine."
                    << "partition_id: " << key.first
                    << ", transaction_id: " << key.second
                    << ", tablet: " << tablet_info.to_string();
                return OLAP_SUCCESS;
            }
        }
    }
    // not found load id
    // case 1: user start a new txn, rowset_ptr = null
    // case 2: loading txn from meta env
    std::pair<PUniqueId, RowsetSharedPtr> load_info(load_id, NULL); 
    _txn_tablet_map[key][tablet_info] = load_info;
    VLOG(3) << "add transaction to engine successfully."
            << "partition_id: " << key.first
            << ", transaction_id: " << key.second
            << ", tablet: " << tablet_info.to_string();
    return OLAP_SUCCESS;
}

OLAPStatus TxnManager::commit_txn(
    OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, 
    const PUniqueId& load_id, RowsetSharedPtr rowset_ptr, bool is_recovery) {

    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    if (rowset_ptr == NULL) {
        LOG(WARNING) << "could not commit txn because rowset ptr is null. "
                     << "partition_id: " << key.first
                     << ", transaction_id: " << key.second
                     << ", tablet: " << tablet_info.to_string();
    }
    WriteLock wrlock(_get_txn_lock(transaction_id));
    {
        // get tx
        ReadLock rdlock(&_txn_map_lock);
        auto it = _txn_tablet_map.find(key);
        if (it != _txn_tablet_map.end()) {
            auto load_itr = it->second.find(tablet_info);
            if (load_itr != it->second.end()) {
                // found load for txn,tablet
                // case 1: user commit rowset, then the load id must be equal
                std::pair<PUniqueId, RowsetSharedPtr>& load_info = load_itr->second;
                // check if load id is equal
                if (load_info.first.hi() == load_id.hi() 
                    && load_info.first.lo() == load_id.lo()
                    && load_info.second != NULL
                    && load_info.second->rowset_id() == rowset_ptr->rowset_id()) {
                    // find a rowset with same rowset id, then it means a duplicate call
                    LOG(INFO) << "find transaction exists when add to engine."
                              << "partition_id: " << key.first
                              << ", transaction_id: " << key.second
                              << ", tablet: " << tablet_info.to_string();
                    return OLAP_SUCCESS;
                } else if (load_info.first.hi() == load_id.hi() 
                    && load_info.first.lo() == load_id.lo()
                    && load_info.second != NULL
                    && load_info.second->rowset_id() != rowset_ptr->rowset_id()) {
                    // find a rowset with different rowset id, then it should not happen, just return errors
                    LOG(WARNING) << "find transaction exists when add to engine."
                                 << "partition_id: " << key.first
                                 << ", transaction_id: " << key.second
                                 << ", tablet: " << tablet_info.to_string();
                    return OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST;
                }
            }
        }
    }
    
    // if not in recovery mode, then should persist the meta to meta env
    // save meta need access disk, it maybe very slow, so that it is not in global txn lock
    // it is under a single txn lock
    if (!is_recovery) {
        OLAPStatus save_status = RowsetMetaManager::save(meta, rowset_ptr->rowset_id(), 
            rowset_ptr->rowset_meta());
        if (save_status != OLAP_SUCCESS) {
            LOG(WARNING) << "save committed rowset failed. when commit txn rowset_id:" 
                        << rowset_ptr->rowset_id() 
                        << "tablet id: " << tablet_id
                        << "txn id:" << transaction_id;
            return OLAP_ERR_ROWSET_SAVE_FAILED;
        }
    }

    {
        WriteLock wrlock(&_txn_map_lock);
        std::pair<PUniqueId, RowsetSharedPtr> load_info(load_id, rowset_ptr); 
        _txn_tablet_map[key][tablet_info] = load_info;
        VLOG(3) << "add transaction to engine successfully."
                << "partition_id: " << key.first
                << ", transaction_id: " << key.second
                << ", tablet: " << tablet_info.to_string();
    }
    return OLAP_SUCCESS;
}

// remove a txn from txn manager
OLAPStatus TxnManager::publish_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                                   TTabletId tablet_id, SchemaHash schema_hash,
                                   Version& version, VersionHash& version_hash) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    RowsetSharedPtr rowset_ptr = NULL;
    WriteLock wrlock(_get_txn_lock(transaction_id));
    {
        ReadLock rlock(&_txn_map_lock);
        auto it = _txn_tablet_map.find(key);
        if (it != _txn_tablet_map.end()) {
            auto load_itr = it->second.find(tablet_info);
            if (load_itr != it->second.end()) {
                // found load for txn,tablet
                // case 1: user commit rowset, then the load id must be equal
                std::pair<PUniqueId, RowsetSharedPtr>& load_info = load_itr->second;
                rowset_ptr = load_info.second;
            }
        }
    }
    // save meta need access disk, it maybe very slow, so that it is not in global txn lock
    // it is under a single txn lock
    if (rowset_ptr != NULL) {
        // TODO(ygl): rowset is already set version here, memory is changed, if save failed
        // it maybe a fatal error
        rowset_ptr->set_version_and_version_hash(version, version_hash);
        OLAPStatus save_status = RowsetMetaManager::save(meta, 
            rowset_ptr->rowset_id(),
            rowset_ptr->rowset_meta());
        if (save_status != OLAP_SUCCESS) {
            LOG(WARNING) << "save committed rowset failed. when publish txn rowset_id:" 
                         << rowset_ptr->rowset_id()
                         << ", tablet id: " << tablet_id
                         << ", txn id:" << transaction_id;
            return OLAP_ERR_ROWSET_SAVE_FAILED;
        }
    } else {
        return OLAP_ERR_TRANSACTION_NOT_EXIST;
    }
    {
        WriteLock wrlock(&_txn_map_lock);
        auto it = _txn_tablet_map.find(key);
        if (it != _txn_tablet_map.end()) {
            it->second.erase(tablet_info);
            LOG(INFO) << "publish txn successfully."
                      << ", partition_id: " << key.first
                      << ", txn_id: " << key.second
                      << ", tablet: " << tablet_info.to_string();
            if (it->second.empty()) {
                _txn_tablet_map.erase(it);
            }
        }
        return OLAP_SUCCESS;
    }
}

// txn could be rollbacked if it does not have related rowset
// if the txn has related rowset then could not rollback it, because it 
// may be committed in another thread and our current thread meets erros when writing to data file
// be has to wait for fe call clear txn api
OLAPStatus TxnManager::rollback_txn(TPartitionId partition_id, TTransactionId transaction_id,
                                    TTabletId tablet_id, SchemaHash schema_hash) {
    
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    WriteLock wrlock(_get_txn_lock(transaction_id));
    WriteLock txn_wrlock(&_txn_map_lock);
    auto it = _txn_tablet_map.find(key);
    if (it != _txn_tablet_map.end()) {
        auto load_itr = it->second.find(tablet_info);
        if (load_itr != it->second.end()) {
            // found load for txn,tablet
            // case 1: user commit rowset, then the load id must be equal
            std::pair<PUniqueId, RowsetSharedPtr>& load_info = load_itr->second;
            if (load_info.second != NULL) {
                // if rowset is not null, it means other thread may commit the rowset
                // should not delete txn any more
                return OLAP_ERR_TRANSACTION_ALREADY_COMMITTED;
            }
        }
        it->second.erase(tablet_info);
        LOG(INFO) << "rollback transaction from engine successfully."
                  << " partition_id: " << key.first
                  << ", transaction_id: " << key.second
                  << ", tablet: " << tablet_info.to_string();
        if (it->second.empty()) {
            _txn_tablet_map.erase(it);
        }
        return OLAP_SUCCESS;
    }
    return OLAP_SUCCESS;
}

// fe call this api to clear unused rowsets in be
// could not delete the rowset if it already has a valid version
OLAPStatus TxnManager::delete_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                                  TTabletId tablet_id, SchemaHash schema_hash) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    WriteLock wrlock(_get_txn_lock(transaction_id));
    WriteLock txn_wrlock(&_txn_map_lock);
    auto it = _txn_tablet_map.find(key);
    if (it != _txn_tablet_map.end()) {
        auto load_itr = it->second.find(tablet_info);
        if (load_itr != it->second.end()) {
            // found load for txn,tablet
            // case 1: user commit rowset, then the load id must be equal
            std::pair<PUniqueId, RowsetSharedPtr>& load_info = load_itr->second;
            if (load_info.second != NULL && meta != NULL) {
                if (load_info.second->version().first > 0) { 
                    LOG(WARNING) << "could not delete transaction from engine, "
                                 << "just remove it from memory not delete from disk" 
                                 << " because related rowset already published."
                                 << ",partition_id: " << key.first
                                 << ", transaction_id: " << key.second
                                 << ", tablet: " << tablet_info.to_string()
                                 << ", rowset id: " << load_info.second->rowset_id()
                                 << ", version: " << load_info.second->version().first;
                    return OLAP_ERR_TRANSACTION_ALREADY_VISIBLE;
                } else {
                    RowsetMetaManager::remove(meta, load_info.second->rowset_id());
                    StorageEngine::instance()->add_unused_rowset(load_info.second);
                }
            }
        }
        it->second.erase(tablet_info);
        LOG(INFO) << "delete transaction from engine successfully."
                << ",partition_id: " << key.first
                << ", transaction_id: " << key.second
                << ", tablet: " << tablet_info.to_string();
        if (it->second.empty()) {
            _txn_tablet_map.erase(it);
        }
        return OLAP_SUCCESS;
    } else {
        return OLAP_ERR_TRANSACTION_NOT_EXIST;
    }
}

void TxnManager::get_tablet_related_txns(TabletSharedPtr tablet, int64_t* partition_id,
                                         std::set<int64_t>* transaction_ids) {
    if (tablet.get() == nullptr || partition_id == nullptr || transaction_ids == nullptr) {
        OLAP_LOG_WARNING("parameter is null when get transactions by tablet");
        return;
    }

    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash());
    ReadLock txn_rdlock(&_txn_map_lock);
    for (auto& it : _txn_tablet_map) {
        if (it.second.find(tablet_info) != it.second.end()) {
            *partition_id = it.first.first;
            transaction_ids->insert(it.first.second);
            VLOG(3) << "find transaction on tablet."
                    << "partition_id: " << it.first.first
                    << ", transaction_id: " << it.first.second
                    << ", tablet: " << tablet_info.to_string();
        }
    }
}

void TxnManager::get_txn_related_tablets(const TTransactionId transaction_id,
                                         TPartitionId partition_id,
                                         std::map<TabletInfo, RowsetSharedPtr>* tablet_infos) {
    // get tablets in this transaction
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    ReadLock rdlock(_get_txn_lock(transaction_id));
    ReadLock txn_rdlock(&_txn_map_lock);
    auto it = _txn_tablet_map.find(key);
    if (it == _txn_tablet_map.end()) {
        OLAP_LOG_WARNING("could not find tablet for [partition_id=%ld transaction_id=%ld]",
                            partition_id, transaction_id);
        return;
    }
    std::map<TabletInfo, std::pair<PUniqueId, RowsetSharedPtr>> load_info_map = it->second;

    // each tablet
    for (auto& load_info : load_info_map) {
        const TabletInfo& tablet_info = load_info.first;
	    tablet_infos->emplace(tablet_info,load_info.second.second);
    }
}
                                

bool TxnManager::has_txn(TPartitionId partition_id, TTransactionId transaction_id,
                         TTabletId tablet_id, SchemaHash schema_hash) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    ReadLock rdlock(_get_txn_lock(transaction_id));
    ReadLock txn_rdlock(&_txn_map_lock);
    auto it = _txn_tablet_map.find(key);
    bool found = it != _txn_tablet_map.end()
                 && it->second.find(tablet_info) != it->second.end();

    return found;
}


bool TxnManager::has_committed_txn(TPartitionId partition_id, TTransactionId transaction_id,
                       TTabletId tablet_id, SchemaHash schema_hash) {

    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    ReadLock rdlock(_get_txn_lock(transaction_id));
    ReadLock txn_rdlock(&_txn_map_lock);
    auto it = _txn_tablet_map.find(key);
    if (it != _txn_tablet_map.end()) {
        auto load_itr = it->second.find(tablet_info);
        if (load_itr != it->second.end()) {
            // found load for txn,tablet
            // case 1: user commit rowset, then the load id must be equal
            std::pair<PUniqueId, RowsetSharedPtr>& load_info = load_itr->second;
            if (load_info.second != NULL) {
                return true;
            }
        }
    }
    return false;
}

bool TxnManager::get_expire_txns(TTabletId tablet_id, std::vector<int64_t>* transaction_ids) {
    /*
    time_t now = time(NULL);
    ReadLock rdlock(&_txn_tablet_map);
    for (auto& it : _header->pending_delta()) {
        double diff = difftime(now, it.creation_time());
        if (diff >= config::pending_data_expire_time_sec) {
            transaction_ids->push_back(it.transaction_id());
            VLOG(3) << "find expire pending data. table=" << full_name() << ", "
                    << "transaction_id=" << it.transaction_id() << " exist_sec=" << diff;
        }
    }
    */
   return true;
}

} // namespace doris
