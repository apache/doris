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

#include "meta-service/meta_service_tablet_stats.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv.h"

namespace doris::cloud {

void internal_get_tablet_stats(MetaServiceCode& code, std::string& msg, Transaction* txn,
                               const std::string& instance_id, const TabletIndexPB& idx,
                               TabletStatsPB& stats, TabletStats& detached_stats, bool snapshot) {
    auto begin_key = stats_tablet_key(
            {instance_id, idx.table_id(), idx.index_id(), idx.partition_id(), idx.tablet_id()});
    auto end_key = stats_tablet_key(
            {instance_id, idx.table_id(), idx.index_id(), idx.partition_id(), idx.tablet_id() + 1});
    std::unique_ptr<RangeGetIterator> it;
    TxnErrorCode err = txn->get(begin_key, end_key, &it, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get tablet stats, err={} tablet_id={}", err, idx.tablet_id());
        return;
    }
    if (!it->has_next()) {
        code = MetaServiceCode::TABLET_NOT_FOUND;
        msg = fmt::format("tablet stats not found, tablet_id={}", idx.tablet_id());
        return;
    }
    auto [k, v] = it->next();
    // First key MUST be tablet stats key
    DCHECK(k == begin_key) << hex(k) << " vs " << hex(begin_key);
    if (!stats.ParseFromArray(v.data(), v.size())) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("marformed tablet stats value, key={}", hex(k));
        return;
    }
    // Parse split tablet stats
    int ret = get_detached_tablet_stats(*it, detached_stats);
    if (ret != 0) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("marformed splitted tablet stats kv, key={}", hex(k));
        return;
    }
}

int get_detached_tablet_stats(RangeGetIterator& iter, TabletStats& detached_stats) {
    while (iter.has_next()) {
        auto [k, v] = iter.next();
        int64_t val;
        if (v.size() != sizeof(val)) [[unlikely]] {
            LOG(WARNING) << "malformed tablet stats value. key=" << hex(k);
            return -1;
        }

        // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "data_size"
        k.remove_prefix(1);
        constexpr size_t key_parts = 8;
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        if (decode_key(&k, &out) != 0 || out.size() != key_parts) [[unlikely]] {
            LOG(WARNING) << "malformed tablet stats key. key=" << hex(k);
            return -1;
        }

        auto* suffix = std::get_if<std::string>(&std::get<0>(out.back()));
        if (!suffix) [[unlikely]] {
            LOG(WARNING) << "malformed tablet stats key. key=" << hex(k);
            return -1;
        }

        std::memcpy(&val, v.data(), sizeof(val));
        if constexpr (std::endian::native == std::endian::big) {
            val = bswap_64(val);
        }

        if (*suffix == STATS_KEY_SUFFIX_DATA_SIZE) {
            detached_stats.data_size = val;
        } else if (*suffix == STATS_KEY_SUFFIX_NUM_ROWS) {
            detached_stats.num_rows = val;
        } else if (*suffix == STATS_KEY_SUFFIX_NUM_ROWSETS) {
            detached_stats.num_rowsets = val;
        } else if (*suffix == STATS_KEY_SUFFIX_NUM_SEGS) {
            detached_stats.num_segs = val;
        } else {
            LOG(WARNING) << "unknown suffix=" << *suffix << " key=" << hex(k);
        }
    }

    return 0;
}

void merge_tablet_stats(TabletStatsPB& stats, const TabletStats& detached_stats) {
    stats.set_data_size(stats.data_size() + detached_stats.data_size);
    stats.set_num_rows(stats.num_rows() + detached_stats.num_rows);
    stats.set_num_rowsets(stats.num_rowsets() + detached_stats.num_rowsets);
    stats.set_num_segments(stats.num_segments() + detached_stats.num_segs);
}

void internal_get_tablet_stats(MetaServiceCode& code, std::string& msg, Transaction* txn,
                               const std::string& instance_id, const TabletIndexPB& idx,
                               TabletStatsPB& stats, bool snapshot) {
    TabletStats detached_stats;
    internal_get_tablet_stats(code, msg, txn, instance_id, idx, stats, detached_stats, snapshot);
    merge_tablet_stats(stats, detached_stats);
}

} // namespace doris::cloud
