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
    // clang-format off
    auto begin_key = stats_tablet_key({instance_id, idx.table_id(), idx.index_id(), idx.partition_id(), idx.tablet_id()});
    auto begin_key_check = begin_key;
    auto end_key = stats_tablet_key({instance_id, idx.table_id(), idx.index_id(), idx.partition_id(), idx.tablet_id() + 1});
    // clang-format on
    std::vector<std::pair<std::string, std::string>> stats_kvs;
    stats_kvs.reserve(5); // aggregate + data_size + num_rows + num_rowsets + num_segments

    std::unique_ptr<RangeGetIterator> it;
    do {
        TxnErrorCode err = txn->get(begin_key, end_key, &it, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get tablet stats, err={} tablet_id={}", err,
                              idx.tablet_id());
            return;
        }
        while (it->has_next()) {
            auto [k, v] = it->next();
            stats_kvs.emplace_back(std::string {k.data(), k.size()},
                                   std::string {v.data(), v.size()});
        }
        begin_key = it->next_begin_key();
    } while (it->more());

    if (stats_kvs.empty()) {
        code = MetaServiceCode::TABLET_NOT_FOUND;
        msg = fmt::format("tablet stats not found, tablet_id={}", idx.tablet_id());
        return;
    }

    auto& [first_stats_key, v] = stats_kvs[0];
    // First key MUST be tablet stats key, the original non-detached one
    DCHECK(first_stats_key == begin_key_check)
            << hex(first_stats_key) << " vs " << hex(begin_key_check);
    if (!stats.ParseFromArray(v.data(), v.size())) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("marformed tablet stats value, key={}", hex(first_stats_key));
        return;
    }
    // Parse split tablet stats
    int ret = get_detached_tablet_stats(stats_kvs, detached_stats);

    if (ret != 0) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("marformed splitted tablet stats kv, key={}", hex(first_stats_key));
        return;
    }
}

int get_detached_tablet_stats(const std::vector<std::pair<std::string, std::string>>& stats_kvs,
                              TabletStats& detached_stats) {
    if (stats_kvs.size() != 5 && stats_kvs.size() != 1) {
        LOG(WARNING) << "incorrect tablet stats_kvs, it should be 1 or 5 size=" << stats_kvs.size();
    }
    for (size_t i = 1; i < stats_kvs.size(); ++i) {
        std::string_view k(stats_kvs[i].first), v(stats_kvs[i].second);
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
            return -2;
        }

        int64_t val = 0;
        if (v.size() != sizeof(val)) [[unlikely]] {
            LOG(WARNING) << "malformed tablet stats value v.size=" << v.size() << " key=" << hex(k);
            return -3;
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
