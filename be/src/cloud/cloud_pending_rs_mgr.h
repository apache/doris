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

#include <map>
#include <memory>
#include <shared_mutex>

#include "common/status.h"
#include "olap/rowset/rowset_meta.h"
#include "util/countdown_latch.h"

namespace doris {

class Thread;

// Manages temporary rowset metadata for cloud storage transactions in memory.
// This cache stores rowset metadata produced during import operations before they
// are committed to MS. When FE/BE notifies the final version/visible_ts, BE can
// update and promote these temporary rowsets to the tablet meta without fetching
// from MS.
class CloudPendingRSMgr {
public:
    CloudPendingRSMgr();
    ~CloudPendingRSMgr();

    Status init();

    // Add a temporary rowset metadata to the pending manager
    // @param txn_id: transaction id
    // @param tablet_id: tablet id
    // @param rowset_meta: rowset metadata to store
    // @param expiration_time: expiration time in seconds since epoch
    void add_pending_rowset(int64_t txn_id, int64_t tablet_id,
                            const RowsetMetaSharedPtr& rowset_meta, int64_t expiration_time);

    // Get a temporary rowset metadata from the pending manager
    // @param txn_id: transaction id
    // @param tablet_id: tablet id
    // @param rowset_meta: output parameter for the rowset metadata
    // @return Status::OK() if found, Status::NotFound() if not found
    Status get_pending_rowset(int64_t txn_id, int64_t tablet_id,
                              RowsetMetaSharedPtr* rowset_meta);

    // Update a temporary rowset metadata in the pending manager
    // @param txn_id: transaction id
    // @param tablet_id: tablet id
    // @param rowset_meta: new rowset metadata
    // @return Status::OK() if updated, Status::NotFound() if not found
    Status update_pending_rowset(int64_t txn_id, int64_t tablet_id,
                                 const RowsetMetaSharedPtr& rowset_meta);

    // Remove a temporary rowset metadata from the pending manager
    // @param txn_id: transaction id
    // @param tablet_id: tablet id
    void remove_pending_rowset(int64_t txn_id, int64_t tablet_id);

    // Remove expired entries from the pending manager
    void remove_expired_pending_rowsets();

private:
    void _clean_thread_callback();

    struct TxnTabletKey {
        int64_t txn_id;
        int64_t tablet_id;

        TxnTabletKey(int64_t txn_id_, int64_t tablet_id_)
                : txn_id(txn_id_), tablet_id(tablet_id_) {}

        auto operator<=>(const TxnTabletKey&) const = default;
    };

    struct PendingRowsetValue {
        RowsetMetaSharedPtr rowset_meta;
        int64_t expiration_time; // seconds since epoch

        PendingRowsetValue() : expiration_time(0) {}

        PendingRowsetValue(RowsetMetaSharedPtr rowset_meta_, int64_t expiration_time_)
                : rowset_meta(std::move(rowset_meta_)), expiration_time(expiration_time_) {}
    };

    // Map: <txn_id, tablet_id> -> <rowset_meta, expiration_time>
    std::map<TxnTabletKey, PendingRowsetValue> _pending_rs_map;
    // Multimap for efficient expiration cleanup: expiration_time -> <txn_id, tablet_id>
    std::multimap<int64_t, TxnTabletKey> _expiration_map;
    std::shared_mutex _rwlock;
    std::shared_ptr<Thread> _clean_thread;
    CountDownLatch _stop_latch;
};

} // namespace doris
