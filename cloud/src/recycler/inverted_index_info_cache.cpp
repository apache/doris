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

#include "recycler/inverted_index_info_cache.h"

#include <cpp/sync_point.h>
#include <gen_cpp/olap_file.pb.h>

#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/txn_kv.h"

namespace doris::cloud {

InvertedIndexInfo into_inverted_index_info(const doris::TabletSchemaCloudPB& schema) {
    if (schema.index_size() > 0) {
        auto inverted_index_storage_format = schema.has_inverted_index_storage_format()
                                                     ? schema.inverted_index_storage_format()
                                                     : InvertedIndexStorageFormatPB::V1;
        switch (inverted_index_storage_format) {
        case InvertedIndexStorageFormatPB::V1: {
            InvertedIndexInfoV1 info;
            for (auto&& i : schema.index()) {
                if (i.has_index_type() && i.index_type() == IndexType::INVERTED) {
                    info.emplace_back(i.index_id(), i.index_suffix_name());
                }
            }
            return info;
        }
        case InvertedIndexStorageFormatPB::V2:
            return InvertedIndexInfoV2 {};
        default:
            return UnknownInvertedIndex {};
        }
    }

    return NoInvertedIndex {};
}

InvertedIndexInfoCache::InvertedIndexInfoCache(std::string instance_id,
                                               std::shared_ptr<TxnKv> txn_kv)
        : instance_id_(std::move(instance_id)), txn_kv_(std::move(txn_kv)) {}

InvertedIndexInfoCache::~InvertedIndexInfoCache() = default;

int InvertedIndexInfoCache::get(int64_t index_id, int32_t schema_version, InvertedIndexInfo& res) {
    {
        std::lock_guard lock(mtx_);
        if (schemas_without_inverted_index_.contains({index_id, schema_version})) {
            res = NoInvertedIndex {};
            return 0;
        }
        if (auto it = inverted_index_id_map_.find({index_id, schema_version});
            it != inverted_index_id_map_.end()) {
            res = it->second;
            return 0;
        }
        if (dropped_index_ids_.contains(index_id)) {
            return 1;
        }
    }

    // Get schema from kv
    // TODO(plat1ko): Single flight
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn, err={}", err);
        return -1;
    }

    auto schema_key = meta_schema_key({instance_id_, index_id, schema_version});
    ValueBuf val_buf;
    err = cloud::get(txn.get(), schema_key, &val_buf);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            // Schema not found, this means that the entire materialized index was dropped in normal
            // case, so we cache this `index_id` to avoid futile subsequent get schema kv attempts
            // for that `index_id`.
            {
                std::lock_guard lock(mtx_);
                dropped_index_ids_.insert(index_id);
            }
            LOG_INFO("schema not found").tag("key", hex(schema_key));
            return 1;
        }
        LOG_WARNING("failed to get schema, err={}", err).tag("key", hex(schema_key));
        return -1;
    }

    doris::TabletSchemaCloudPB schema;
    if (!parse_schema_value(val_buf, &schema)) {
        LOG(WARNING) << "malformed schema value, key=" << hex(schema_key);
        return -1;
    }

    res = into_inverted_index_info(schema);
    insert(index_id, schema_version, res);
    return 0;
}

void InvertedIndexInfoCache::insert(int64_t index_id, int32_t schema_version,
                                    InvertedIndexInfo info) {
    if (std::holds_alternative<NoInvertedIndex>(info)) {
        TEST_SYNC_POINT("InvertedIndexInfoCache::insert1");
        std::lock_guard lock(mtx_);
        schemas_without_inverted_index_.insert({index_id, schema_version});
    } else {
        TEST_SYNC_POINT("InvertedIndexInfoCache::insert2");
        std::lock_guard lock(mtx_);
        inverted_index_id_map_.emplace(Key {index_id, schema_version}, std::move(info));
    }
}

} // namespace doris::cloud
