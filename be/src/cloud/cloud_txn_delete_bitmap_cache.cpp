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

#include "common/status.h"
#include "common/sync_point.h"
#include "olap/olap_common.h"
#include "olap/tablet_meta.h"

namespace doris {

CloudTxnDeleteBitmapCache::CloudTxnDeleteBitmapCache(size_t size_in_bytes)
        : LRUCachePolicy(CachePolicy::CacheType::CLOUD_TXN_DELETE_BITMAP_CACHE, size_in_bytes,
                         LRUCacheType::SIZE, 86400, 4),
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
        std::shared_ptr<PartialUpdateInfo>* partial_update_info) {
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
    }
    std::string key_str = fmt::format("{}/{}", transaction_id, tablet_id);
    CacheKey key(key_str);
    Cache::Handle* handle = lookup(key);

    DeleteBitmapCacheValue* val =
            handle == nullptr ? nullptr : reinterpret_cast<DeleteBitmapCacheValue*>(value(handle));
    if (val) {
        *delete_bitmap = val->delete_bitmap;
        *rowset_ids = val->rowset_ids;
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
    if (txn_expiration <= 0) {
        txn_expiration = duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count() +
                         120;
    }
    {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        TxnKey txn_key(transaction_id, tablet_id);
        _txn_map[txn_key] = TxnVal(rowset, txn_expiration, std::move(partial_update_info));
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

void CloudTxnDeleteBitmapCache::update_tablet_txn_info(TTransactionId transaction_id,
                                                       int64_t tablet_id,
                                                       DeleteBitmapPtr delete_bitmap,
                                                       const RowsetIdUnorderedSet& rowset_ids) {
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
            .tag("delete_bitmap_size", charge);
}

void CloudTxnDeleteBitmapCache::remove_expired_tablet_txn_info() {
    TEST_SYNC_POINT_RETURN_WITH_VOID("CloudTxnDeleteBitmapCache::remove_expired_tablet_txn_info");
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    while (!_expiration_txn.empty()) {
        auto iter = _expiration_txn.begin();
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

void CloudTxnDeleteBitmapCache::_clean_thread_callback() {
    do {
        remove_expired_tablet_txn_info();
    } while (!_stop_latch.wait_for(std::chrono::seconds(300)));
}

} // namespace doris