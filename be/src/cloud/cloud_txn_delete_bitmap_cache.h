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

#pragma once

#include <mutex>

#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/partial_update_info.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_meta.h"
#include "util/countdown_latch.h"

namespace doris {

// Record transaction related delete bitmaps using a lru cache.
class CloudTxnDeleteBitmapCache : public LRUCachePolicy {
public:
    CloudTxnDeleteBitmapCache(size_t size_in_bytes);

    ~CloudTxnDeleteBitmapCache() override;

    Status init();

    Status get_tablet_txn_info(TTransactionId transaction_id, int64_t tablet_id,
                               RowsetSharedPtr* rowset, DeleteBitmapPtr* delete_bitmap,
                               RowsetIdUnorderedSet* rowset_ids, int64_t* txn_expiration,
                               std::shared_ptr<PartialUpdateInfo>* partial_update_info);

    void set_tablet_txn_info(TTransactionId transaction_id, int64_t tablet_id,
                             DeleteBitmapPtr delete_bitmap, const RowsetIdUnorderedSet& rowset_ids,
                             RowsetSharedPtr rowset, int64_t txn_expirationm,
                             std::shared_ptr<PartialUpdateInfo> partial_update_info);

    void update_tablet_txn_info(TTransactionId transaction_id, int64_t tablet_id,
                                DeleteBitmapPtr delete_bitmap,
                                const RowsetIdUnorderedSet& rowset_ids);

    void remove_expired_tablet_txn_info();

private:
    void _clean_thread_callback();

    struct DeleteBitmapCacheValue {
        DeleteBitmapPtr delete_bitmap;
        // records rowsets calc in commit txn
        RowsetIdUnorderedSet rowset_ids;

        DeleteBitmapCacheValue(DeleteBitmapPtr delete_bitmap_, const RowsetIdUnorderedSet& ids_)
                : delete_bitmap(std::move(delete_bitmap_)), rowset_ids(ids_) {}
    };

    struct TxnKey {
        TTransactionId txn_id;
        int64_t tablet_id;
        TxnKey(TTransactionId txn_id_, int64_t tablet_id_)
                : txn_id(txn_id_), tablet_id(tablet_id_) {}
        auto operator<=>(const TxnKey&) const = default;
    };

    struct TxnVal {
        RowsetSharedPtr rowset;
        int64_t txn_expiration;
        std::shared_ptr<PartialUpdateInfo> partial_update_info;
        TxnVal() : txn_expiration(0) {};
        TxnVal(RowsetSharedPtr rowset_, int64_t txn_expiration_,
               std::shared_ptr<PartialUpdateInfo> partial_update_info_)
                : rowset(std::move(rowset_)),
                  txn_expiration(txn_expiration_),
                  partial_update_info(std::move(partial_update_info_)) {}
    };

    std::map<TxnKey, TxnVal> _txn_map;
    std::multimap<int64_t, TxnKey> _expiration_txn;
    std::shared_mutex _rwlock;
    scoped_refptr<Thread> _clean_thread;
    CountDownLatch _stop_latch;
};

} // namespace doris
