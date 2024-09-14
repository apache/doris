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

#include "cloud/cloud_txn_delete_bitmap_cache.h"

#include <fmt/core.h>

#include <chrono>
#include <memory>
#include <shared_mutex>

#include "cloud/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "olap/olap_common.h"
#include "olap/tablet_meta.h"
#include "olap/txn_manager.h"

namespace doris {

CloudTxnDeleteBitmapCache::CloudTxnDeleteBitmapCache(size_t size_in_bytes)
        : LRUCachePolicyTrackingManual(CachePolicy::CacheType::CLOUD_TXN_DELETE_BITMAP_CACHE,
                                       size_in_bytes, LRUCacheType::SIZE, 86400, 4),
          _stop_latch(1) {}

CloudTxnDeleteBitmapCache::~CloudTxnDeleteBitmapCache() {
    _stop_latch.count_down();
    _clean_thread->join();
}

Status CloudTxnDeleteBitmapCache::init() {
    auto st = Thread::create(
            "CloudTxnDeleteBitmapCache", "clean_txn_dbm_thread",
            [this]() { this->_clean_thread_callback(); }, &_clean_thread);
    if (!st.ok()) {
        LOG(WARNING) << "failed to create thread for CloudTxnDeleteBitmapCache, error: " << st;
    }
    return st;
}

Status CloudTxnDeleteBitmapCache::get_tablet_txn_info(
        TTransactionId transaction_id, int64_t tablet_id, RowsetSharedPtr* rowset,
        DeleteBitmapPtr* delete_bitmap, RowsetIdUnorderedSet* rowset_ids, int64_t* txn_expiration,
        std::shared_ptr<PartialUpdateInfo>* partial_update_info,
        std::shared_ptr<PublishStatus>* publish_status, TxnPublishInfo* previous_publish_info) {
    {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        TxnKey key(transaction_id, tablet_id);
        auto iter = _txn_map.find(key);
        if (iter == _txn_map.end()) {
            return Status::Error<ErrorCode::NOT_FOUND, false>(
                    "not found txn info, tablet_id={}, transaction_id={}", tablet_id,
                    transaction_id);
        }
        *rowset = iter->second.rowset;
        *txn_expiration = iter->second.txn_expiration;
        *partial_update_info = iter->second.partial_update_info;
        *publish_status = iter->second.publish_status;
        *previous_publish_info = iter->second.publish_info;
    }
    RETURN_IF_ERROR(
            get_delete_bitmap(transaction_id, tablet_id, delete_bitmap, rowset_ids, nullptr));
    return Status::OK();
}

Status CloudTxnDeleteBitmapCache::get_delete_bitmap(
        TTransactionId transaction_id, int64_t tablet_id, DeleteBitmapPtr* delete_bitmap,
        RowsetIdUnorderedSet* rowset_ids, std::shared_ptr<PublishStatus>* publish_status) {
    if (publish_status) {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        TxnKey txn_key(transaction_id, tablet_id);
        auto iter = _txn_map.find(txn_key);
        if (iter == _txn_map.end()) {
            return Status::Error<ErrorCode::NOT_FOUND, false>(
                    "not found txn info, tablet_id={}, transaction_id={}", tablet_id,
                    transaction_id);
        }
        *publish_status = iter->second.publish_status;
    }
    std::string key_str = fmt::format("{}/{}", transaction_id, tablet_id);
    CacheKey key(key_str);
    Cache::Handle* handle = lookup(key);

    DeleteBitmapCacheValue* val =
            handle == nullptr ? nullptr : reinterpret_cast<DeleteBitmapCacheValue*>(value(handle));
    if (val) {
        *delete_bitmap = val->delete_bitmap;
        if (rowset_ids) {
            *rowset_ids = val->rowset_ids;
        }
        // must call release handle to reduce the reference count,
        // otherwise there will be memory leak
        release(handle);
    } else {
        LOG_INFO("cache missed when get delete bitmap")
                .tag("txn_id", transaction_id)
                .tag("tablet_id", tablet_id);
        // Because of the rowset_ids become empty, all delete bitmap
        // will be recalculate in CalcDeleteBitmapTask
        *delete_bitmap = std::make_shared<DeleteBitmap>(tablet_id);
    }
    return Status::OK();
}

void CloudTxnDeleteBitmapCache::set_tablet_txn_info(
        TTransactionId transaction_id, int64_t tablet_id, DeleteBitmapPtr delete_bitmap,
        const RowsetIdUnorderedSet& rowset_ids, RowsetSharedPtr rowset, int64_t txn_expiration,
        std::shared_ptr<PartialUpdateInfo> partial_update_info) {
    int64_t txn_expiration_min =
            duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count() +
            config::tablet_txn_info_min_expired_seconds;
    txn_expiration = std::max(txn_expiration_min, txn_expiration);
    {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        TxnKey txn_key(transaction_id, tablet_id);
        std::shared_ptr<PublishStatus> publish_status =
                std::make_shared<PublishStatus>(PublishStatus::INIT);
        _txn_map[txn_key] = TxnVal(rowset, txn_expiration, std::move(partial_update_info),
                                   std::move(publish_status));
        _expiration_txn.emplace(txn_expiration, txn_key);
    }
    std::string key_str = fmt::format("{}/{}", transaction_id, tablet_id);
    CacheKey key(key_str);

    auto val = new DeleteBitmapCacheValue(delete_bitmap, rowset_ids);
    size_t charge = sizeof(DeleteBitmapCacheValue);
    for (auto& [k, v] : val->delete_bitmap->delete_bitmap) {
        charge += v.getSizeInBytes();
    }
    auto* handle = insert(key, val, charge, charge, CachePriority::NORMAL);
    // must call release handle to reduce the reference count,
    // otherwise there will be memory leak
    release(handle);
    LOG_INFO("set txn related delete bitmap")
            .tag("txn_id", transaction_id)
            .tag("expiration", txn_expiration)
            .tag("tablet_id", tablet_id)
            .tag("delete_bitmap_size", charge);
}

Status CloudTxnDeleteBitmapCache::update_tablet_txn_info(TTransactionId transaction_id,
                                                         int64_t tablet_id,
                                                         DeleteBitmapPtr delete_bitmap,
                                                         const RowsetIdUnorderedSet& rowset_ids,
                                                         PublishStatus publish_status,
                                                         TxnPublishInfo publish_info) {
    {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        TxnKey txn_key(transaction_id, tablet_id);
        if (!_txn_map.contains(txn_key)) {
            return Status::Error<ErrorCode::NOT_FOUND, false>(
                    "not found txn info, tablet_id={}, transaction_id={}, may be expired and be "
                    "removed",
                    tablet_id, transaction_id);
        }
        TxnVal& txn_val = _txn_map[txn_key];
        *(txn_val.publish_status) = publish_status;
        if (publish_status == PublishStatus::SUCCEED) {
            txn_val.publish_info = publish_info;
        }
    }
    std::string key_str = fmt::format("{}/{}", transaction_id, tablet_id);
    CacheKey key(key_str);

    auto val = new DeleteBitmapCacheValue(delete_bitmap, rowset_ids);
    size_t charge = sizeof(DeleteBitmapCacheValue);
    for (auto& [k, v] : val->delete_bitmap->delete_bitmap) {
        charge += v.getSizeInBytes();
    }
    auto* handle = insert(key, val, charge, charge, CachePriority::NORMAL);
    // must call release handle to reduce the reference count,
    // otherwise there will be memory leak
    release(handle);
    LOG_INFO("update txn related delete bitmap")
            .tag("txn_id", transaction_id)
            .tag("tablt_id", tablet_id)
            .tag("delete_bitmap_size", charge)
            .tag("publish_status", static_cast<int>(publish_status));
    return Status::OK();
}

void CloudTxnDeleteBitmapCache::remove_expired_tablet_txn_info() {
    TEST_SYNC_POINT_RETURN_WITH_VOID("CloudTxnDeleteBitmapCache::remove_expired_tablet_txn_info");
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    while (!_expiration_txn.empty()) {
        auto iter = _expiration_txn.begin();
        if (_txn_map.find(iter->second) == _txn_map.end()) {
            _expiration_txn.erase(iter);
            continue;
        }
        int64_t current_time = duration_cast<std::chrono::seconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count();
        if (iter->first > current_time) {
            break;
        }
        auto txn_iter = _txn_map.find(iter->second);
        if ((txn_iter != _txn_map.end()) && (iter->first == txn_iter->second.txn_expiration)) {
            LOG_INFO("clean expired delete bitmap")
                    .tag("txn_id", txn_iter->first.txn_id)
                    .tag("expiration", txn_iter->second.txn_expiration)
                    .tag("tablt_id", txn_iter->first.tablet_id);
            std::string key_str = std::to_string(txn_iter->first.txn_id) + "/" +
                                  std::to_string(txn_iter->first.tablet_id); // Cache key container
            CacheKey cache_key(key_str);
            erase(cache_key);
            _txn_map.erase(iter->second);
        }
        _expiration_txn.erase(iter);
    }
}

void CloudTxnDeleteBitmapCache::remove_unused_tablet_txn_info(TTransactionId transaction_id,
                                                              int64_t tablet_id) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    TxnKey txn_key(transaction_id, tablet_id);
    auto txn_iter = _txn_map.find(txn_key);
    if (txn_iter != _txn_map.end()) {
        LOG_INFO("remove unused tablet txn info")
                .tag("txn_id", txn_iter->first.txn_id)
                .tag("tablt_id", txn_iter->first.tablet_id);
        std::string key_str = std::to_string(txn_iter->first.txn_id) + "/" +
                              std::to_string(txn_iter->first.tablet_id); // Cache key container
        CacheKey cache_key(key_str);
        erase(cache_key);
        _txn_map.erase(txn_key);
    }
}

void CloudTxnDeleteBitmapCache::_clean_thread_callback() {
    do {
        remove_expired_tablet_txn_info();
    } while (!_stop_latch.wait_for(
            std::chrono::seconds(config::remove_expired_tablet_txn_info_interval_seconds)));
}

} // namespace doris