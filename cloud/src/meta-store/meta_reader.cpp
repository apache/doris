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

} // namespace doris::cloud
