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

#include <fmt/core.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

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
    bool unexpected_size = false;
    // clang-format off
    if (stats_kvs.size() != 5    // aggregated stats and 4 splitted stats: num_rowsets num_segs data_size num_rows
        && stats_kvs.size() != 2 // aggregated stats and 1 splitted stats: num_rowsets
        && stats_kvs.size() != 1 // aggregated stats only (nothing has been imported since created)
        ) {
        unexpected_size = true;
    }
    // clang-format on
    std::stringstream ss;
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

        if (unexpected_size) ss << *suffix << " ";

        if (*suffix == STATS_KEY_SUFFIX_DATA_SIZE) {
            detached_stats.data_size = val;
        } else if (*suffix == STATS_KEY_SUFFIX_NUM_ROWS) {
            detached_stats.num_rows = val;
        } else if (*suffix == STATS_KEY_SUFFIX_NUM_ROWSETS) {
            detached_stats.num_rowsets = val;
        } else if (*suffix == STATS_KEY_SUFFIX_NUM_SEGS) {
            detached_stats.num_segs = val;
        } else {
            LOG(WARNING) << "unknown tablet stats suffix=" << *suffix << " key=" << hex(k);
        }
    }

    if (unexpected_size) {
        LOG_EVERY_N(WARNING, 100) << "unexpected tablet stats_kvs, it should be 1 or 2 or 5, size="
                                  << stats_kvs.size() << " suffix=" << ss.str();
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

MetaServiceResponseStatus parse_fix_tablet_stats_param(
        std::shared_ptr<ResourceManager> resource_mgr, const std::string& table_id_str,
        const std::string& cloud_unique_id_str, int64_t& table_id, std::string& instance_id) {
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);

    // parse params
    try {
        table_id = std::stoll(table_id_str);
    } catch (...) {
        st.set_code(MetaServiceCode::INVALID_ARGUMENT);
        st.set_msg("Invalid table_id, table_id: " + table_id_str);
        return st;
    }

    instance_id = get_instance_id(resource_mgr, cloud_unique_id_str);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id_str;
        st.set_code(code);
        st.set_msg(msg);
        return st;
    }
    return st;
}

MetaServiceResponseStatus fix_tablet_stats_internal(
        std::shared_ptr<TxnKv> txn_kv, std::pair<std::string, std::string>& key_pair,
        std::vector<std::shared_ptr<TabletStatsPB>>& tablet_stat_shared_ptr_vec_batch,
        const std::string& instance_id, size_t batch_size) {
    std::unique_ptr<Transaction> txn;
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);
    MetaServiceCode code = MetaServiceCode::OK;
    std::unique_ptr<RangeGetIterator> it;
    std::vector<std::shared_ptr<TabletStatsPB>> tmp_tablet_stat_vec;

    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        st.set_code(cast_as<ErrCategory::CREATE>(err));
        st.set_msg("failed to create txn");
        return st;
    }

    // read tablet stats
    err = txn->get(key_pair.first, key_pair.second, &it, true);
    if (err != TxnErrorCode::TXN_OK) {
        st.set_code(cast_as<ErrCategory::READ>(err));
        st.set_msg(fmt::format("failed to get tablet stats, err={} ", err));
        return st;
    }

    size_t tablet_cnt = 0;
    while (it->has_next() && tablet_cnt < batch_size) {
        auto [k, v] = it->next();
        key_pair.first = k;
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);

        // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} -> TabletStatsPB
        if (out.size() == 7) {
            tablet_cnt++;
            TabletStatsPB tablet_stat;
            tablet_stat.ParseFromArray(v.data(), v.size());
            tmp_tablet_stat_vec.emplace_back(std::make_shared<TabletStatsPB>(tablet_stat));
        }
    }
    if (it->has_next()) {
        key_pair.first = it->next().first;
    }

    for (const auto& tablet_stat_ptr : tmp_tablet_stat_vec) {
        GetRowsetResponse resp;
        std::string msg;
        // get rowsets in tablet and accumulate disk size
        internal_get_rowset(txn.get(), 0, std::numeric_limits<int64_t>::max() - 1, instance_id,
                            tablet_stat_ptr->idx().tablet_id(), code, msg, &resp);
        if (code != MetaServiceCode::OK) {
            st.set_code(code);
            st.set_msg(msg);
            return st;
        }
        int64_t total_disk_size = 0;
        for (const auto& rs_meta : resp.rowset_meta()) {
            total_disk_size += rs_meta.total_disk_size();
        }

        // set new disk size to tabletPB and write it back
        TabletStatsPB tablet_stat;
        tablet_stat.CopyFrom(*tablet_stat_ptr);
        tablet_stat.set_data_size(total_disk_size);
        // record tablet stats batch
        tablet_stat_shared_ptr_vec_batch.emplace_back(std::make_shared<TabletStatsPB>(tablet_stat));
        std::string tablet_stat_key;
        std::string tablet_stat_value;
        tablet_stat_key = stats_tablet_key(
                {instance_id, tablet_stat.idx().table_id(), tablet_stat.idx().index_id(),
                 tablet_stat.idx().partition_id(), tablet_stat.idx().tablet_id()});
        if (!tablet_stat.SerializeToString(&tablet_stat_value)) {
            st.set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
            st.set_msg("failed to serialize tablet stat");
            return st;
        }
        txn->put(tablet_stat_key, tablet_stat_value);

        // read num segs
        // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "num_segs" -> int64
        std::string tablet_stat_num_segs_key;
        stats_tablet_num_segs_key(
                {instance_id, tablet_stat_ptr->idx().table_id(), tablet_stat_ptr->idx().index_id(),
                 tablet_stat_ptr->idx().partition_id(), tablet_stat_ptr->idx().tablet_id()},
                &tablet_stat_num_segs_key);
        int64_t tablet_stat_num_segs = 0;
        std::string tablet_stat_num_segs_value(sizeof(tablet_stat_num_segs), '\0');
        err = txn->get(tablet_stat_num_segs_key, &tablet_stat_num_segs_value);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            st.set_code(cast_as<ErrCategory::READ>(err));
        }
        if (tablet_stat_num_segs_value.size() != sizeof(tablet_stat_num_segs)) [[unlikely]] {
            LOG(WARNING) << " malformed tablet stats value v.size="
                         << tablet_stat_num_segs_value.size()
                         << " value=" << hex(tablet_stat_num_segs_value);
        }
        std::memcpy(&tablet_stat_num_segs, tablet_stat_num_segs_value.data(),
                    sizeof(tablet_stat_num_segs));
        if constexpr (std::endian::native == std::endian::big) {
            tablet_stat_num_segs = bswap_64(tablet_stat_num_segs);
        }

        if (tablet_stat_num_segs > 0) {
            // set tablet stats data size = 0
            // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "data_size" -> int64
            std::string tablet_stat_data_size_key;
            stats_tablet_data_size_key(
                    {instance_id, tablet_stat.idx().table_id(), tablet_stat.idx().index_id(),
                     tablet_stat.idx().partition_id(), tablet_stat.idx().tablet_id()},
                    &tablet_stat_data_size_key);
            int64_t tablet_stat_data_size = 0;
            std::string tablet_stat_data_size_value(sizeof(tablet_stat_data_size), '\0');
            memcpy(tablet_stat_data_size_value.data(), &tablet_stat_data_size,
                   sizeof(tablet_stat_data_size));
            txn->put(tablet_stat_data_size_key, tablet_stat_data_size_value);
        }
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        st.set_code(cast_as<ErrCategory::COMMIT>(err));
        st.set_msg("failed to commit txn");
        return st;
    }
    return st;
}

MetaServiceResponseStatus check_new_tablet_stats(
        std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
        const std::vector<std::shared_ptr<TabletStatsPB>>& tablet_stat_shared_ptr_vec_batch) {
    std::unique_ptr<Transaction> txn;
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);

    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        st.set_code(cast_as<ErrCategory::CREATE>(err));
        st.set_msg("failed to create txn");
        return st;
    }

    for (const auto& tablet_stat_ptr : tablet_stat_shared_ptr_vec_batch) {
        // check tablet stats
        std::string tablet_stat_key;
        std::string tablet_stat_value;
        tablet_stat_key = stats_tablet_key(
                {instance_id, tablet_stat_ptr->idx().table_id(), tablet_stat_ptr->idx().index_id(),
                 tablet_stat_ptr->idx().partition_id(), tablet_stat_ptr->idx().tablet_id()});
        err = txn->get(tablet_stat_key, &tablet_stat_value);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            st.set_code(cast_as<ErrCategory::READ>(err));
            return st;
        }
        TabletStatsPB tablet_stat_check;
        tablet_stat_check.ParseFromArray(tablet_stat_value.data(), tablet_stat_value.size());
        if (tablet_stat_check.DebugString() != tablet_stat_ptr->DebugString() &&
            // If anyone data size of tablet_stat_check and tablet_stat_ptr is twice bigger than another,
            // we need to rewrite it this tablet_stat.
            (tablet_stat_check.data_size() > 2 * tablet_stat_ptr->data_size() ||
             tablet_stat_ptr->data_size() > 2 * tablet_stat_check.data_size())) {
            LOG_WARNING("[fix tablet stats]:tablet stats check failed")
                    .tag("tablet stat", tablet_stat_ptr->DebugString())
                    .tag("check tabelt stat", tablet_stat_check.DebugString());
        }

        // check data size
        std::string tablet_stat_data_size_key;
        stats_tablet_data_size_key(
                {instance_id, tablet_stat_ptr->idx().table_id(), tablet_stat_ptr->idx().index_id(),
                 tablet_stat_ptr->idx().partition_id(), tablet_stat_ptr->idx().tablet_id()},
                &tablet_stat_data_size_key);
        int64_t tablet_stat_data_size = 0;
        std::string tablet_stat_data_size_value(sizeof(tablet_stat_data_size), '\0');
        err = txn->get(tablet_stat_data_size_key, &tablet_stat_data_size_value);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            st.set_code(cast_as<ErrCategory::READ>(err));
            return st;
        }
        int64_t tablet_stat_data_size_check;

        if (tablet_stat_data_size_value.size() != sizeof(tablet_stat_data_size_check))
                [[unlikely]] {
            LOG(WARNING) << " malformed tablet stats value v.size="
                         << tablet_stat_data_size_value.size()
                         << " value=" << hex(tablet_stat_data_size_value);
        }
        std::memcpy(&tablet_stat_data_size_check, tablet_stat_data_size_value.data(),
                    sizeof(tablet_stat_data_size_check));
        if constexpr (std::endian::native == std::endian::big) {
            tablet_stat_data_size_check = bswap_64(tablet_stat_data_size_check);
        }
        if (tablet_stat_data_size_check != tablet_stat_data_size &&
            // ditto
            (tablet_stat_data_size_check > 2 * tablet_stat_data_size ||
             tablet_stat_data_size > 2 * tablet_stat_data_size_check)) {
            LOG_WARNING("[fix tablet stats]:data size check failed")
                    .tag("data size", tablet_stat_data_size)
                    .tag("check data size", tablet_stat_data_size_check);
        }
    }

    return st;
}

} // namespace doris::cloud
