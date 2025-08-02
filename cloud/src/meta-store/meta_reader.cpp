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

#include "meta-store/meta_reader.h"

#include <memory>

#include "common/logging.h"
#include "common/util.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"

namespace doris::cloud {

TxnErrorCode MetaReader::get_table_version(int64_t table_id, Versionstamp* table_version) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_table_version(txn.get(), table_id, table_version);
}

TxnErrorCode MetaReader::get_table_version(Transaction* txn, int64_t table_id,
                                           Versionstamp* table_version) {
    std::string table_version_key = versioned::table_version_key({instance_id_, table_id});
    std::string table_version_value;
    return versioned_get(txn, table_version_key, snapshot_version_, table_version,
                         &table_version_value, snapshot_);
}

TxnErrorCode MetaReader::get_partition_version(int64_t partition_id, VersionPB* version,
                                               Versionstamp* partition_version) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    return get_partition_version(txn.get(), partition_id, version, partition_version);
}

TxnErrorCode MetaReader::get_partition_version(Transaction* txn, int64_t partition_id,
                                               VersionPB* version,
                                               Versionstamp* partition_version) {
    std::string partition_version_key =
            versioned::partition_version_key({instance_id_, partition_id});
    std::string partition_version_value;
    TxnErrorCode err = versioned_get(txn, partition_version_key, snapshot_version_,
                                     partition_version, &partition_version_value, snapshot_);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (version && !version->ParseFromString(partition_version_value)) {
        LOG_ERROR("Failed to parse VersionPB")
                .tag("instance_id", instance_id_)
                .tag("partition_id", partition_id)
                .tag("key", hex(partition_version_key));
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_tablet_load_stats(int64_t tablet_id, TabletStatsPB* tablet_stats,
                                               Versionstamp* versionstamp) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return get_tablet_load_stats(txn.get(), tablet_id, tablet_stats, versionstamp);
}

TxnErrorCode MetaReader::get_tablet_load_stats(Transaction* txn, int64_t tablet_id,
                                               TabletStatsPB* tablet_stats,
                                               Versionstamp* versionstamp) {
    std::string tablet_load_stats_key = versioned::tablet_load_stats_key({instance_id_, tablet_id});
    std::string tablet_load_stats_value;
    TxnErrorCode err = versioned_get(txn, tablet_load_stats_key, snapshot_version_, versionstamp,
                                     &tablet_load_stats_value, snapshot_);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (tablet_stats && !tablet_stats->ParseFromString(tablet_load_stats_value)) {
        LOG_ERROR("Failed to parse TabletStatsPB")
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("key", hex(tablet_load_stats_key));
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaReader::get_tablet_meta(int64_t tablet_id, TabletMetaCloudPB* tablet_meta,
                                         Versionstamp* versionstamp) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    return get_tablet_meta(txn.get(), tablet_id, tablet_meta, versionstamp);
}

TxnErrorCode MetaReader::get_tablet_meta(Transaction* txn, int64_t tablet_id,
                                         TabletMetaCloudPB* tablet_meta,
                                         Versionstamp* versionstamp) {
    std::string tablet_meta_key = versioned::meta_tablet_key({instance_id_, tablet_id});
    return versioned::document_get(txn, tablet_meta_key, snapshot_version_, tablet_meta,
                                   versionstamp, snapshot_);
}

} // namespace doris::cloud
