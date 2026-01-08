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

#include "cloud/cloud_pending_rs_mgr.h"

#include <chrono>

#include "cloud/config.h"
#include "common/logging.h"
#include "util/thread.h"

namespace doris {

CloudPendingRSMgr::CloudPendingRSMgr() : _stop_latch(1) {}

CloudPendingRSMgr::~CloudPendingRSMgr() {
    _stop_latch.count_down();
    if (_clean_thread) {
        _clean_thread->join();
    }
}

Status CloudPendingRSMgr::init() {
    auto st = Thread::create(
            "CloudPendingRSMgr", "clean_pending_rs_thread",
            [this]() { this->_clean_thread_callback(); }, &_clean_thread);
    if (!st.ok()) {
        LOG(WARNING) << "failed to create thread for CloudPendingRSMgr, error: " << st;
    }
    return st;
}

void CloudPendingRSMgr::add_pending_rowset(int64_t txn_id, int64_t tablet_id,
                                           const RowsetMetaSharedPtr& rowset_meta,
                                           int64_t expiration_time) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    TxnTabletKey key(txn_id, tablet_id);
    _pending_rs_map[key] = PendingRowsetValue(rowset_meta, expiration_time);
    _expiration_map.emplace(expiration_time, key);
    LOG(INFO) << "add pending rowset, txn_id=" << txn_id << ", tablet_id=" << tablet_id
              << ", rowset_id=" << rowset_meta->rowset_id().to_string()
              << ", expiration_time=" << expiration_time;
}

Status CloudPendingRSMgr::get_pending_rowset(int64_t txn_id, int64_t tablet_id,
                                             RowsetMetaSharedPtr* rowset_meta) {
    std::shared_lock<std::shared_mutex> rlock(_rwlock);
    TxnTabletKey key(txn_id, tablet_id);
    auto iter = _pending_rs_map.find(key);
    if (iter == _pending_rs_map.end()) {
        return Status::Error<ErrorCode::NOT_FOUND>(
                "pending rowset not found, txn_id={}, tablet_id={}", txn_id, tablet_id);
    }
    *rowset_meta = iter->second.rowset_meta;
    return Status::OK();
}

Status CloudPendingRSMgr::update_pending_rowset(int64_t txn_id, int64_t tablet_id,
                                                const RowsetMetaSharedPtr& rowset_meta) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    TxnTabletKey key(txn_id, tablet_id);
    auto iter = _pending_rs_map.find(key);
    if (iter == _pending_rs_map.end()) {
        return Status::Error<ErrorCode::NOT_FOUND>(
                "pending rowset not found, txn_id={}, tablet_id={}", txn_id, tablet_id);
    }
    iter->second.rowset_meta = rowset_meta;
    LOG(INFO) << "update pending rowset, txn_id=" << txn_id << ", tablet_id=" << tablet_id
              << ", rowset_id=" << rowset_meta->rowset_id().to_string();
    return Status::OK();
}

void CloudPendingRSMgr::remove_pending_rowset(int64_t txn_id, int64_t tablet_id) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    TxnTabletKey key(txn_id, tablet_id);
    auto iter = _pending_rs_map.find(key);
    if (iter != _pending_rs_map.end()) {
        LOG(INFO) << "remove pending rowset, txn_id=" << txn_id << ", tablet_id=" << tablet_id;
        _pending_rs_map.erase(iter);
    }
}

void CloudPendingRSMgr::remove_expired_pending_rowsets() {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    int64_t current_time = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count();

    while (!_expiration_map.empty()) {
        auto iter = _expiration_map.begin();
        // Check if entry exists in main map
        if (_pending_rs_map.find(iter->second) == _pending_rs_map.end()) {
            _expiration_map.erase(iter);
            continue;
        }

        // Not expired yet, break
        if (iter->first > current_time) {
            break;
        }

        auto rs_iter = _pending_rs_map.find(iter->second);
        if (rs_iter != _pending_rs_map.end() &&
            iter->first == rs_iter->second.expiration_time) {
            LOG(INFO) << "clean expired pending rowset, txn_id=" << rs_iter->first.txn_id
                      << ", tablet_id=" << rs_iter->first.tablet_id
                      << ", expiration_time=" << rs_iter->second.expiration_time;
            _pending_rs_map.erase(iter->second);
        }
        _expiration_map.erase(iter);
    }
}

void CloudPendingRSMgr::_clean_thread_callback() {
    do {
        remove_expired_pending_rowsets();
    } while (!_stop_latch.wait_for(
            std::chrono::seconds(config::remove_expired_tablet_txn_info_interval_seconds)));
}

} // namespace doris
