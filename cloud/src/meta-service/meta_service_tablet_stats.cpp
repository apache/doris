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

MetaServiceResponseStatus fix_tablet_stats_parse_param(
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

MetaServiceResponseStatus read_range_tablet_stats(std::shared_ptr<TxnKv> txn_kv,
                                                  const std::string& instance_id, int64_t table_id,
                                                  const std::string& begin_key,
                                                  const std::string& end_key,
                                                  std::unique_ptr<RangeGetIterator>& it) {
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);
    std::unique_ptr<Transaction> txn;

    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = fmt::format("[fix tablet stat] failed to create txn");
        st.set_code(code);
        st.set_msg(msg);
        return st;
    }

    err = txn->get(begin_key, end_key, &it, true);
    if (err != TxnErrorCode::TXN_OK) {
        st.set_code(cast_as<ErrCategory::READ>(err));
        st.set_msg(fmt::format("failed to get tablet stats, err={} instance_id={} table_id={} ",
                               err, instance_id, table_id));
        return st;
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        st.set_code(cast_as<ErrCategory::COMMIT>(err));
        std::string msg = fmt::format("[fix tablet stats]: failed to commit txn)");
        st.set_msg(msg);
        return st;
    }
    return st;
}

MetaServiceResponseStatus create_txn_if_committed(bool& committed, std::shared_ptr<TxnKv> txn_kv,
                                                  const std::string& msg,
                                                  std::unique_ptr<Transaction>& txn) {
    // read data and write data use one txn
    // check data use one txn
    MetaServiceCode code = MetaServiceCode::OK;
    TxnErrorCode err;
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);
    if (committed) {
        err = txn_kv->create_txn(&txn);
        committed = false;
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            st.set_code(code);
            st.set_msg(msg);
            return st;
        }
    }
    return st;
}

MetaServiceResponseStatus read_and_accumulate_rowset_data(
        const std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>>& out,
        const std::string_view& v, int64_t tablet_cnt, std::unique_ptr<Transaction>& txn,
        const std::string& instance_id, int64_t& total_disk_size, TabletStatsPB& tablet_stat) {
    MetaServiceCode code = MetaServiceCode::OK;
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);

    auto tablet_id = std::get<int64_t>(std::get<0>(out[6]));
    tablet_stat.ParseFromArray(v.data(), v.size());
    LOG(INFO) << fmt::format(
            "[Sub txn id {}, Tablet id {} fix tablet stats phase 2]: read original "
            "tabletPB, tabletPB info: {}",
            tablet_cnt, tablet_id, tablet_stat.DebugString());

    GetRowsetResponse resp;
    std::string msg;
    internal_get_rowset(txn.get(), 0, std::numeric_limits<int64_t>::max() - 1, instance_id,
                        tablet_id, code, msg, &resp, true);
    if (code != MetaServiceCode::OK) {
        st.set_code(code);
        st.set_msg(msg);
        return st;
    }
    total_disk_size = 0;
    for (const auto& rs_meta : resp.rowset_meta()) {
        rs_meta.rowset_id();
        total_disk_size += rs_meta.total_disk_size();
        LOG(INFO) << fmt::format(
                "[Sub txn id {}, Tablet id {} fix tablet stats phase 2]: read "
                "rowsetsPB, total disk size: {}, rowsetPB info: {}",
                tablet_cnt, tablet_id, total_disk_size, rs_meta.DebugString());
    }
    return st;
}

MetaServiceResponseStatus write_tablet_stats_back(
        TabletStatsPB& tablet_stat, int64_t total_disk_size, const std::string& instance_id,
        std::unique_ptr<Transaction>& txn, int64_t tablet_cnt,
        std::vector<std::shared_ptr<TabletStatsPB>>& tablet_stat_shared_ptr_vec) {
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);

    tablet_stat.set_data_size(total_disk_size);
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
    tablet_stat_shared_ptr_vec.emplace_back(std::make_shared<TabletStatsPB>(tablet_stat));
    LOG(INFO) << fmt::format(
            "[Sub txn id{}, Tablet id {} fix tablet stats phase 3]: write new "
            "tabletPB, tabletPB info: {}",
            tablet_cnt, tablet_stat.idx().tablet_id(), tablet_stat.DebugString());
    return st;
}

void set_tablet_data_size_to_zero(TabletStatsPB tablet_stat, std::unique_ptr<Transaction>& txn,
                                  const std::string& instance_id, int64_t tablet_cnt) {
    // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "data_size" -> int64
    std::string tablet_stat_data_size_key;
    stats_tablet_data_size_key(
            {instance_id, tablet_stat.idx().table_id(), tablet_stat.idx().index_id(),
             tablet_stat.idx().partition_id(), tablet_stat.idx().tablet_id()},
            &tablet_stat_data_size_key);
    // set tablet stats data size = 0
    int64_t tablet_stat_data_size = 0;
    std::string tablet_stat_data_size_value(sizeof(tablet_stat_data_size), '\0');
    memcpy(tablet_stat_data_size_value.data(), &tablet_stat_data_size,
           sizeof(tablet_stat_data_size));
    txn->put(tablet_stat_data_size_key, tablet_stat_data_size_value);
    LOG(INFO) << fmt::format(
            "[Sub txn id {} Tablet id {} fix tablet stats phase 4]: set tablet stats "
            "data size = 0, data size : {}",
            tablet_cnt, tablet_stat.idx().tablet_id(), tablet_stat_data_size_value);
}

MetaServiceResponseStatus commit_batch(std::unique_ptr<Transaction>& txn, bool& committed,
                                       int64_t tablet_cnt) {
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);
    TxnErrorCode err;
    if (tablet_cnt % 50 == 0) {
        err = txn->commit();
        committed = true;
        if (err != TxnErrorCode::TXN_OK) {
            st.set_code(cast_as<ErrCategory::COMMIT>(err));
            std::string msg = fmt::format("failed to commit 50 sub txns, sub txn id {}-{}",
                                          tablet_cnt - 50, tablet_cnt);
            st.set_msg(msg);
            return st;
        } else {
            LOG(INFO) << fmt::format(
                    "[fix tablet stats phase ?]: success to commit 50 sub txns, sub txn id "
                    "{}-{}",
                    tablet_cnt - 50, tablet_cnt);
        }
    }
    return st;
}

MetaServiceResponseStatus commit_batch_final(std::unique_ptr<Transaction>& txn, bool& committed,
                                             int64_t tablet_cnt) {
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);
    TxnErrorCode err;
    if (!committed) {
        err = txn->commit();
        committed = true;
        if (err != TxnErrorCode::TXN_OK) {
            st.set_code(cast_as<ErrCategory::COMMIT>(err));
            std::string msg = fmt::format(
                    "[fix tablet stats final commit]: failed to commit remain txns, final sub txn "
                    "id {}",
                    tablet_cnt);
            st.set_msg(msg);
            return st;
        } else {
            LOG(INFO) << fmt::format(
                    "[fix tablet stats final commit]: success to commit remain sub txns, final sub "
                    "txn id {}",
                    tablet_cnt);
        };
    }
    return st;
}

MetaServiceResponseStatus fix_tablet_stats_data(
        std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id, int64_t table_id,
        std::vector<std::shared_ptr<TabletStatsPB>>& tablet_stat_shared_ptr_vec) {
    // fix tablet stats code
    std::string msg;
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);

    std::unique_ptr<Transaction> txn;
    std::string key, val;
    int64_t start = 0;
    int64_t end = std::numeric_limits<int64_t>::max() - 1;
    auto begin_key = stats_tablet_key({instance_id, table_id, start, start, start});
    auto end_key = stats_tablet_key({instance_id, table_id, end, end, end});
    std::unique_ptr<RangeGetIterator> it;
    std::vector<std::pair<std::string, std::string>> stats_kvs;
    int64_t tablet_cnt = 0;
    bool committed = true;

    do {
        // =======================================================
        // phase 0: read all tablet stats by table id
        st = read_range_tablet_stats(txn_kv, instance_id, table_id, begin_key, end_key, it);
        if (st.code() != MetaServiceCode::OK) {
            return st;
        }
        committed = true;

        while (it->has_next()) {
            auto [k, v] = it->next();
            auto k1 = k;
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);

            st = create_txn_if_committed(committed, txn_kv,
                                         "[fix tablet stat] failed to create txn", txn);
            if (st.code() != MetaServiceCode::OK) {
                return st;
            }
            // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} -> TabletStatsPB
            if (out.size() == 7) {
                tablet_cnt++;
                // =======================================================
                // phase 1: read tablet's rowsets data and accumulate them
                int64_t total_disk_size;
                TabletStatsPB tablet_stat;
                st = read_and_accumulate_rowset_data(out, v, tablet_cnt, txn, instance_id,
                                                     total_disk_size, tablet_stat);
                if (st.code() != MetaServiceCode::OK) {
                    return st;
                }

                // =======================================================
                // phase 2: set new data to tabletPB and write it back
                st = write_tablet_stats_back(tablet_stat, total_disk_size, instance_id, txn,
                                             tablet_cnt, tablet_stat_shared_ptr_vec);
                if (st.code() != MetaServiceCode::OK) {
                    return st;
                }

                // =======================================================
                // phase 3: set tablet stats data_size = 0
                set_tablet_data_size_to_zero(tablet_stat, txn, instance_id, tablet_cnt);

                st = commit_batch(txn, committed, tablet_cnt);
                if (st.code() != MetaServiceCode::OK) {
                    return st;
                }
            }
        }
        begin_key = it->next_begin_key();
    } while (it->more());
    st = commit_batch_final(txn, committed, tablet_cnt);
    if (st.code() != MetaServiceCode::OK) {
        return st;
    }

    return st;
}

MetaServiceResponseStatus check_tablet_stats(std::shared_ptr<TabletStatsPB> tablet_stat_ptr,
                                             std::unique_ptr<Transaction>& txn,
                                             const std::string& instance_id, int64_t tablet_cnt) {
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);
    TxnErrorCode err;
    std::string tablet_stat_key;
    std::string tablet_stat_value;
    tablet_stat_key = stats_tablet_key(
            {instance_id, tablet_stat_ptr->idx().table_id(), tablet_stat_ptr->idx().index_id(),
             tablet_stat_ptr->idx().partition_id(), tablet_stat_ptr->idx().tablet_id()});
    err = txn->get(tablet_stat_key, &tablet_stat_value);
    if (err != TxnErrorCode::TXN_OK) {
        st.set_code(cast_as<ErrCategory::READ>(err));

        LOG(INFO) << fmt::format(
                "[Sub txn id {} Tablet id {} fix tablet stats phase 5]: get tablet "
                "stats failed, err {}",
                tablet_cnt, tablet_stat_ptr->idx().tablet_id(), err);
    }
    TabletStatsPB tablet_stat_check;
    tablet_stat_check.ParseFromArray(tablet_stat_value.data(), tablet_stat_value.size());
    LOG(INFO) << fmt::format(
            "[Sub txn id {} Tablet id {} fix tablet stats phase 5]: check correctness "
            "get tabletPB {}",
            tablet_cnt, tablet_stat_ptr->idx().tablet_id(), tablet_stat_check.DebugString());
    if (tablet_stat_check.DebugString() != tablet_stat_ptr->DebugString()) {
        LOG(WARNING) << fmt::format(
                "[Sub txn id {} Tablet id {} fix tablet stats phase 5]: check "
                "correctness get tabletPB failed, tablet_stat {}, tablet_stat_check {}",
                tablet_cnt, tablet_stat_ptr->idx().tablet_id(), tablet_stat_ptr->DebugString(),
                tablet_stat_check.DebugString());
    }
    return st;
}

MetaServiceResponseStatus check_data_size(std::shared_ptr<TabletStatsPB> tablet_stat_ptr,
                                          std::unique_ptr<Transaction>& txn,
                                          const std::string& instance_id, int64_t tablet_cnt) {
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);
    TxnErrorCode err;
    std::string tablet_stat_data_size_key;
    stats_tablet_data_size_key(
            {instance_id, tablet_stat_ptr->idx().table_id(), tablet_stat_ptr->idx().index_id(),
             tablet_stat_ptr->idx().partition_id(), tablet_stat_ptr->idx().tablet_id()},
            &tablet_stat_data_size_key);
    int64_t tablet_stat_data_size = 0;
    std::string tablet_stat_data_size_value(sizeof(tablet_stat_data_size), '\0');
    err = txn->get(tablet_stat_data_size_key, &tablet_stat_data_size_value);
    if (err != TxnErrorCode::TXN_OK) {
        st.set_code(cast_as<ErrCategory::READ>(err));
        LOG(INFO) << fmt::format(
                "[Sub txn id {} Tablet id {} fix tablet stats phase 6]: get tablet "
                "stats data size failed, err {}",
                tablet_cnt, tablet_stat_ptr->idx().tablet_id(), err);
    }
    int64_t tablet_stat_data_size_check;

    if (tablet_stat_data_size_value.size() != sizeof(tablet_stat_data_size_check)) [[unlikely]] {
        LOG(WARNING) << "[Sub txn id " << tablet_cnt << " Tablet id "
                     << tablet_stat_ptr->idx().tablet_id()
                     << " fix tablet stats phase 6]: malformed tablet stats value v.size="
                     << tablet_stat_data_size_value.size()
                     << " value=" << hex(tablet_stat_data_size_value);
    }
    std::memcpy(&tablet_stat_data_size_check, tablet_stat_data_size_value.data(),
                sizeof(tablet_stat_data_size_check));
    if constexpr (std::endian::native == std::endian::big) {
        tablet_stat_data_size_check = bswap_64(tablet_stat_data_size_check);
    }
    LOG(INFO) << fmt::format(
            "[Sub txn id {} Tablet id {} check tablet stats phase 6]: check correctness "
            "get tablet stat data size {}",
            tablet_cnt, tablet_stat_ptr->idx().tablet_id(), tablet_stat_data_size_check);
    if (tablet_stat_data_size_check != tablet_stat_data_size) {
        LOG(WARNING) << fmt::format(
                "[Sub txn id {} Tablet id {} check tablet stats phase 6]: check "
                "correctness get tablet stat data size failed, tablet_stat_data_size "
                "{}, "
                "tablet_stat_data_size_check {}",
                tablet_cnt, tablet_stat_ptr->idx().tablet_id(), tablet_stat_data_size,
                tablet_stat_data_size_check);
    }
    return st;
}

MetaServiceResponseStatus check_tablet_stats_data(
        std::vector<std::shared_ptr<TabletStatsPB>>& tablet_stat_shared_ptr_vec,
        std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id, int64_t table_id) {
    std::string msg;
    MetaServiceResponseStatus st;
    st.set_code(MetaServiceCode::OK);

    std::unique_ptr<Transaction> txn;
    int64_t tablet_cnt = 0;
    bool committed = true;
    for (const auto& tablet_stat_ptr : tablet_stat_shared_ptr_vec) {
        tablet_cnt++;
        // read data and write data use one txn
        // check data use one txn
        st = create_txn_if_committed(committed, txn_kv, "[check tablet stat] failed to create txn",
                                     txn);
        if (st.code() != MetaServiceCode::OK) {
            return st;
        }
        // =======================================================
        // phase 5: get tabletPB to check correctness
        st = check_tablet_stats(tablet_stat_ptr, txn, instance_id, tablet_cnt);
        if (st.code() != MetaServiceCode::OK) {
            return st;
        }

        // =======================================================
        // phase 6: get tablet data size to check correctness
        st = check_data_size(tablet_stat_ptr, txn, instance_id, tablet_cnt);
        if (st.code() != MetaServiceCode::OK) {
            return st;
        }

        st = commit_batch(txn, committed, tablet_cnt);
        if (st.code() != MetaServiceCode::OK) {
            return st;
        }
    }

    st = commit_batch_final(txn, committed, tablet_cnt);
    if (st.code() != MetaServiceCode::OK) {
        return st;
    }
    return st;
}

} // namespace doris::cloud
