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

#include "cloud/cloud_committed_rs_mgr.h"

#include <chrono>

#include "cloud/config.h"
#include "common/logging.h"
#include "olap/rowset/rowset_meta.h"
#include "util/thread.h"

namespace doris {

CloudCommittedRSMgr::CloudCommittedRSMgr() : _stop_latch(1) {}

CloudCommittedRSMgr::~CloudCommittedRSMgr() {
    _stop_latch.count_down();
    if (_clean_thread) {
        _clean_thread->join();
    }
}

Status CloudCommittedRSMgr::init() {
    auto st = Thread::create(
            "CloudCommittedRSMgr", "clean_committed_rs_thread",
            [this]() { this->_clean_thread_callback(); }, &_clean_thread);
    if (!st.ok()) {
        LOG(WARNING) << "failed to create thread for CloudCommittedRSMgr, error: " << st;
    }
    return st;
}

void CloudCommittedRSMgr::add_committed_rowset(int64_t txn_id, int64_t tablet_id,
                                               RowsetMetaSharedPtr rowset_meta,
                                               int64_t expiration_time) {
    int64_t txn_expiration_min =
            duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count() +
            config::tablet_txn_info_min_expired_seconds;
    expiration_time = std::max(txn_expiration_min, expiration_time);
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    TxnTabletKey key(txn_id, tablet_id);
    _committed_rs_map.insert_or_assign(key, CommittedRowsetValue(rowset_meta, expiration_time));
    _expiration_map.emplace(expiration_time, key);
    LOG(INFO) << "add pending rowset, txn_id=" << txn_id << ", tablet_id=" << tablet_id
              << ", rowset_id=" << rowset_meta->rowset_id().to_string()
              << ", expiration_time=" << expiration_time;
}

Result<std::pair<RowsetMetaSharedPtr, int64_t>> CloudCommittedRSMgr::get_committed_rowset(
        int64_t txn_id, int64_t tablet_id) {
    std::shared_lock<std::shared_mutex> rlock(_rwlock);
    TxnTabletKey key(txn_id, tablet_id);
    if (auto it = _empty_rowset_markers.find(key); it != _empty_rowset_markers.end()) {
        return std::make_pair(nullptr, it->second);
    }
    auto iter = _committed_rs_map.find(key);
    if (iter == _committed_rs_map.end()) {
        return ResultError(Status::Error<ErrorCode::NOT_FOUND>(
                "committed rowset not found, txn_id={}, tablet_id={}", txn_id, tablet_id));
    }
    return std::make_pair(iter->second.rowset_meta, iter->second.expiration_time);
}

void CloudCommittedRSMgr::remove_committed_rowset(int64_t txn_id, int64_t tablet_id) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    _committed_rs_map.erase({txn_id, tablet_id});
}

void CloudCommittedRSMgr::remove_expired_committed_rowsets() {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    int64_t current_time = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count();

    while (!_expiration_map.empty()) {
        auto iter = _expiration_map.begin();
        if (!_committed_rs_map.contains(iter->second) &&
            !_empty_rowset_markers.contains(iter->second)) {
            _expiration_map.erase(iter);
            continue;
        }
        int64_t expiration_time = iter->first;
        if (expiration_time > current_time) {
            break;
        }

        auto key = iter->second;
        _committed_rs_map.erase(key);
        _empty_rowset_markers.erase(key);
        LOG(INFO) << "clean expired pending cloud rowset, txn_id=" << key.txn_id
                  << ", tablet_id=" << key.tablet_id << ", expiration_time=" << expiration_time;

        _expiration_map.erase(iter);
    }
}

void CloudCommittedRSMgr::mark_empty_rowset(int64_t txn_id, int64_t tablet_id,
                                            int64_t txn_expiration) {
    int64_t txn_expiration_min =
            duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count() +
            config::tablet_txn_info_min_expired_seconds;
    txn_expiration = std::max(txn_expiration_min, txn_expiration);

    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    TxnTabletKey txn_key(txn_id, tablet_id);
    _empty_rowset_markers.insert_or_assign(txn_key, txn_expiration);
    _expiration_map.emplace(txn_expiration, txn_key);
}

void CloudCommittedRSMgr::_clean_thread_callback() {
    do {
        remove_expired_committed_rowsets();
    } while (!_stop_latch.wait_for(
            std::chrono::seconds(config::remove_expired_tablet_txn_info_interval_seconds)));
}

} // namespace doris
