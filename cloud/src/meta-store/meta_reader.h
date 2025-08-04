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

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versionstamp.h"

namespace doris::cloud {

// A versioned meta reader that encapsulates the logic to read versioned metadata.
//
// This class is lightweight and does not hold any state. So constructing multiple instances
// is cheap and does not require any cleanup.
//
// But the caller should ensure that the referenced instance_id and txn_kv are valid
// throughout the lifetime of the MetaReader instance.
class MetaReader {
public:
    MetaReader(std::string_view instance_id, TxnKv* txn_kv)
            : MetaReader(instance_id, txn_kv, Versionstamp::max(), false) {}
    MetaReader(std::string_view instance_id, TxnKv* txn_kv, Versionstamp snapshot_version)
            : MetaReader(instance_id, txn_kv, snapshot_version, false) {}
    MetaReader(std::string_view instance_id, TxnKv* txn_kv, bool snapshot)
            : MetaReader(instance_id, txn_kv, Versionstamp::max(), snapshot) {}
    MetaReader(std::string_view instance_id, TxnKv* txn_kv, Versionstamp snapshot_version,
               bool snapshot)
            : instance_id_(instance_id),
              snapshot_version_(snapshot_version),
              snapshot_(snapshot),
              txn_kv_(txn_kv) {}
    MetaReader(const MetaReader&) = delete;
    MetaReader& operator=(const MetaReader&) = delete;

    // Get the version of the table_version_key with the given table_id.
    TxnErrorCode get_table_version(int64_t table_id, Versionstamp* table_version);
    TxnErrorCode get_table_version(Transaction* txn, int64_t table_id, Versionstamp* table_version);

    // Get the partition version for the given partition
    //
    // If the `version` is not nullptr, it will be filled with the deserialized VersionPB.
    TxnErrorCode get_partition_version(int64_t partition_id, VersionPB* version,
                                       Versionstamp* versionstamp);
    TxnErrorCode get_partition_version(Transaction* txn, int64_t partition_id, VersionPB* version,
                                       Versionstamp* versionstamp);

    // Get the tablet load stats for the given tablet
    //
    // If the `tablet_stats` is not nullptr, it will be filled with the deserialized TabletStatsPB.
    TxnErrorCode get_tablet_load_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                       Versionstamp* versionstamp);
    TxnErrorCode get_tablet_load_stats(Transaction* txn, int64_t tablet_id,
                                       TabletStatsPB* tablet_stats, Versionstamp* versionstamp);

    // Get the tablet meta keys.
    TxnErrorCode get_tablet_meta(int64_t tablet_id, TabletMetaCloudPB* tablet_meta,
                                 Versionstamp* versionstamp);
    TxnErrorCode get_tablet_meta(Transaction* txn, int64_t tablet_id,
                                 TabletMetaCloudPB* tablet_meta, Versionstamp* versionstamp);

private:
    const std::string_view instance_id_;
    const Versionstamp snapshot_version_;
    const bool snapshot_;
    TxnKv* txn_kv_;
};

} // namespace doris::cloud
