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

#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <cstdlib>
#include <string>

#include "common/util.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

int InstanceRecycler::recycle_cluster_snapshots() {
    return snapshot_manager_->recycle_snapshots(this);
}

int InstanceRecycler::recycle_snapshot_meta_and_data(const std::string& resource_id,
                                                     Versionstamp snapshot_version,
                                                     const SnapshotPB& snapshot_pb) {
    auto it = accessor_map_.find(resource_id);
    if (it == accessor_map_.end()) {
        LOG(WARNING) << "no accessor for resource, cannot recycle snapshot data"
                     << ", instance_id=" << instance_id_
                     << ", resource_id=" << instance_info_.resource_ids(0);
        return -1;
    }

    return snapshot_manager_->recycle_snapshot_meta_and_data(
            instance_id_, resource_id, it->second.get(), snapshot_version, snapshot_pb);
}

int InstanceRecycler::has_cluster_snapshots(bool* any) {
    std::string snapshot_key = versioned::snapshot_full_key({instance_id_});
    std::string begin_key = encode_versioned_key(snapshot_key, Versionstamp::min());
    std::string end_key = encode_versioned_key(snapshot_key, Versionstamp::max());

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn. instance_id=" << instance_id_ << ", err=" << err;
        return -1;
    }

    std::unique_ptr<RangeGetIterator> iter;
    err = txn->get(begin_key, end_key, &iter, false, 1);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to get snapshot key. instance_id=" << instance_id_
                     << ", err=" << err;
        return -1;
    }

    *any = iter->has_next();
    return 0;
}

bool InstanceRecycler::should_recycle_versioned_keys() const {
    if (!instance_info_.has_multi_version_status()) {
        return false;
    }

    if (instance_info_.multi_version_status() == MULTI_VERSION_DISABLED) {
        return false;
    }

    // When multi version is write only and snapshot switch is disabled,
    // we do not need to recycle versioned keys. Because there has some
    // keys which are not migrated to versioned keys yet.
    if (instance_info_.multi_version_status() == MULTI_VERSION_WRITE_ONLY &&
        (!instance_info_.has_snapshot_switch_status() ||
         instance_info_.snapshot_switch_status() == SNAPSHOT_SWITCH_DISABLED)) {
        return false;
    }

    return true;
}

} // namespace doris::cloud
