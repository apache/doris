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

#include "txn_lazy_committer.h"

#include <chrono>

#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_tablet_stats.h"

using namespace std::chrono;

namespace doris::cloud {

void scan_tmp_rowset(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, int64_t* db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* tmp_rowsets_meta);

void update_tablet_stats(const StatsTabletKeyInfo& info, const TabletStats& stats,
                         std::unique_ptr<Transaction>& txn, MetaServiceCode& code,
                         std::string& msg);

void convert_tmp_rowsets(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, int64_t db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta,
        std::unordered_map<int64_t, TabletIndexPB>& tablet_ids) {
    std::stringstream ss;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    // partition_id -> VersionPB
    std::unordered_map<int64_t, VersionPB> partition_versions;
    // tablet_id -> stats
    std::unordered_map<int64_t, TabletStats> tablet_stats;

    for (auto& [tmp_rowset_key, tmp_rowset_pb] : tmp_rowsets_meta) {
        std::string tmp_rowst_data;
        err = txn->get(tmp_rowset_key, &tmp_rowst_data);
        if (TxnErrorCode::TXN_KEY_NOT_FOUND == err) {
            // the tmp rowset has been converted
            VLOG_DEBUG << "tmp rowset has been converted, key=" << hex(tmp_rowset_key);
            continue;
        }

        if (TxnErrorCode::TXN_OK != err) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get tmp_rowset_key, txn_id=" << txn_id
               << " key=" << hex(tmp_rowset_key) << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        if (!tablet_ids.contains(tmp_rowset_pb.tablet_id())) {
            std::string tablet_idx_key =
                    meta_tablet_idx_key({instance_id, tmp_rowset_pb.tablet_id()});
            std::string tablet_idx_val;
            err = txn->get(tablet_idx_key, &tablet_idx_val, true);
            if (TxnErrorCode::TXN_OK != err) {
                code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                              : cast_as<ErrCategory::READ>(err);
                ss << "failed to get tablet idx, txn_id=" << txn_id
                   << " key=" << hex(tablet_idx_key) << " err=" << err;
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }

            TabletIndexPB tablet_idx_pb;
            if (!tablet_idx_pb.ParseFromString(tablet_idx_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "failed to parse tablet idx pb txn_id=" << txn_id
                   << " key=" << hex(tablet_idx_key);
                msg = ss.str();
                return;
            }
            tablet_ids.emplace(tmp_rowset_pb.tablet_id(), tablet_idx_pb);
        }
        const TabletIndexPB& tablet_idx_pb = tablet_ids[tmp_rowset_pb.tablet_id()];

        if (!partition_versions.contains(tmp_rowset_pb.partition_id())) {
            std::string ver_val;
            std::string ver_key = partition_version_key(
                    {instance_id, db_id, tablet_idx_pb.table_id(), tmp_rowset_pb.partition_id()});
            err = txn->get(ver_key, &ver_val);
            if (TxnErrorCode::TXN_OK != err) {
                code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                              : cast_as<ErrCategory::READ>(err);
                ss << "failed to get partiton version, txn_id=" << txn_id << " key=" << hex(ver_key)
                   << " err=" << err;
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }
            VersionPB version_pb;
            if (!version_pb.ParseFromString(ver_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "failed to parse version pb txn_id=" << txn_id << " key=" << hex(ver_key);
                msg = ss.str();
                return;
            }
            LOG(INFO) << "txn_id=" << txn_id << " key=" << hex(ver_key)
                      << " version_pb:" << version_pb.ShortDebugString();
            partition_versions.emplace(tmp_rowset_pb.partition_id(), version_pb);
        }

        const VersionPB& version_pb = partition_versions[tmp_rowset_pb.partition_id()];
        DCHECK(version_pb.pending_txn_ids_size() > 0);
        DCHECK_EQ(version_pb.pending_txn_ids(0), txn_id);

        int64_t version = version_pb.has_version() ? (version_pb.version() + 1) : 2;

        std::string rowset_key = meta_rowset_key({instance_id, tmp_rowset_pb.tablet_id(), version});
        std::string rowset_val;
        err = txn->get(rowset_key, &rowset_val);
        if (TxnErrorCode::TXN_OK == err) {
            // tmp rowset key has been converted
            continue;
        }

        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get rowset_key, txn_id=" << txn_id << " key=" << hex(rowset_key)
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        DCHECK(err == TxnErrorCode::TXN_KEY_NOT_FOUND);

        tmp_rowset_pb.set_start_version(version);
        tmp_rowset_pb.set_end_version(version);

        rowset_val.clear();
        if (!tmp_rowset_pb.SerializeToString(&rowset_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize rowset_meta, txn_id=" << txn_id
               << " key=" << hex(rowset_key);
            msg = ss.str();
            return;
        }

        txn->put(rowset_key, rowset_val);
        LOG(INFO) << "put rowset_key=" << hex(rowset_key) << " txn_id=" << txn_id
                  << " rowset_size=" << rowset_key.size() + rowset_val.size();

        // Accumulate affected rows
        auto& stats = tablet_stats[tmp_rowset_pb.tablet_id()];
        stats.data_size += tmp_rowset_pb.data_disk_size();
        stats.num_rows += tmp_rowset_pb.num_rows();
        ++stats.num_rowsets;
        stats.num_segs += tmp_rowset_pb.num_segments();
    }

    DCHECK(partition_versions.size() == 1);

    for (auto& [tablet_id, stats] : tablet_stats) {
        DCHECK(tablet_ids.count(tablet_id));
        auto& tablet_idx = tablet_ids[tablet_id];
        StatsTabletKeyInfo info {instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                 tablet_idx.partition_id(), tablet_id};
        update_tablet_stats(info, stats, txn, code, msg);
        if (code != MetaServiceCode::OK) return;
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit kv txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }
}

void make_committed_txn_visible(const std::string& instance_id, int64_t db_id, int64_t txn_id,
                                std::shared_ptr<TxnKv> txn_kv, MetaServiceCode& code,
                                std::string& msg) {
    // 1. visible txn info
    // 2. remove running key and put recycle txn key

    std::stringstream ss;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    std::string info_val;
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_info, txn_id=" << txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    VLOG_DEBUG << "txn_info:" << txn_info.ShortDebugString();
    DCHECK((txn_info.status() == TxnStatusPB::TXN_STATUS_COMMITTED) ||
           (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE));

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_COMMITTED) {
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);

        if (!txn_info.SerializeToString(&info_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        txn->put(info_key, info_val);
        LOG(INFO) << "put info_key=" << hex(info_key) << " txn_id=" << txn_id;

        const std::string running_key = txn_running_key({instance_id, db_id, txn_id});
        LOG(INFO) << "remove running_key=" << hex(running_key) << " txn_id=" << txn_id;
        txn->remove(running_key);

        std::string recycle_val;
        std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        RecycleTxnPB recycle_pb;
        auto now_time = system_clock::now();
        uint64_t visible_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
        recycle_pb.set_creation_time(visible_time);
        recycle_pb.set_label(txn_info.label());

        if (!recycle_pb.SerializeToString(&recycle_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize recycle_pb, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        txn->put(recycle_key, recycle_val);
        LOG(INFO) << "put recycle_key=" << hex(recycle_key) << " txn_id=" << txn_id;

        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to commit kv txn, txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            return;
        }
    }
}

TxnLazyCommitTask::TxnLazyCommitTask(const std::string& instance_id, int64_t txn_id,
                                     std::shared_ptr<TxnKv> txn_kv,
                                     TxnLazyCommitter* txn_lazy_committer)
        : instance_id_(instance_id),
          txn_id_(txn_id),
          txn_kv_(txn_kv),
          txn_lazy_committer_(txn_lazy_committer) {
    DCHECK(txn_id > 0);
}

void TxnLazyCommitTask::commit() {
    std::stringstream ss;
    int retry_times = 0;
    do {
        LOG(INFO) << "lazy task commit txn_id=" << txn_id_ << " retry_times=" << retry_times;
        do {
            code_ = MetaServiceCode::OK;
            msg_.clear();
            int64_t db_id;
            std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> tmp_rowset_metas;
            scan_tmp_rowset(instance_id_, txn_id_, txn_kv_, code_, msg_, &db_id, &tmp_rowset_metas);
            if (code_ != MetaServiceCode::OK) {
                LOG(WARNING) << "scan_tmp_rowset failed, txn_id=" << txn_id_ << " code=" << code_;
                break;
            }

            VLOG_DEBUG << "txn_id=" << txn_id_
                       << " tmp_rowset_metas.size()=" << tmp_rowset_metas.size();
            if (tmp_rowset_metas.size() == 0) {
                LOG(INFO) << "empty tmp_rowset_metas, txn_id=" << txn_id_;
            }

            // <partition_id, tmp_rowsets>
            std::unordered_map<int64_t,
                               std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>>
                    partition_to_tmp_rowset_metas;
            for (auto& [tmp_rowset_key, tmp_rowset_pb] : tmp_rowset_metas) {
                partition_to_tmp_rowset_metas[tmp_rowset_pb.partition_id()].emplace_back();
                partition_to_tmp_rowset_metas[tmp_rowset_pb.partition_id()].back().first =
                        tmp_rowset_key;
                partition_to_tmp_rowset_metas[tmp_rowset_pb.partition_id()].back().second =
                        tmp_rowset_pb;
            }

            // tablet_id -> TabletIndexPB
            std::unordered_map<int64_t, TabletIndexPB> tablet_ids;
            for (auto& [partition_id, tmp_rowset_metas] : partition_to_tmp_rowset_metas) {
                for (size_t i = 0; i < tmp_rowset_metas.size();
                     i += config::txn_lazy_max_rowsets_per_batch) {
                    size_t end =
                            (i + config::txn_lazy_max_rowsets_per_batch) > tmp_rowset_metas.size()
                                    ? tmp_rowset_metas.size()
                                    : i + config::txn_lazy_max_rowsets_per_batch;
                    std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>
                            sub_partition_tmp_rowset_metas(tmp_rowset_metas.begin() + i,
                                                           tmp_rowset_metas.begin() + end);
                    convert_tmp_rowsets(instance_id_, txn_id_, txn_kv_, code_, msg_, db_id,
                                        sub_partition_tmp_rowset_metas, tablet_ids);
                    if (code_ != MetaServiceCode::OK) break;
                }
                if (code_ != MetaServiceCode::OK) break;

                DCHECK(tmp_rowset_metas.size() > 0);
                std::unique_ptr<Transaction> txn;
                TxnErrorCode err = txn_kv_->create_txn(&txn);
                if (err != TxnErrorCode::TXN_OK) {
                    code_ = cast_as<ErrCategory::CREATE>(err);
                    ss << "failed to create txn, txn_id=" << txn_id_ << " err=" << err;
                    msg_ = ss.str();
                    LOG(WARNING) << msg_;
                    break;
                }

                int64_t table_id = -1;
                for (auto& [tmp_rowset_key, tmp_rowset_pb] : tmp_rowset_metas) {
                    if (table_id <= 0) {
                        table_id = tablet_ids[tmp_rowset_pb.tablet_id()].table_id();
                    }
                    txn->remove(tmp_rowset_key);
                    LOG(INFO) << "remove tmp_rowset_key=" << hex(tmp_rowset_key)
                              << " txn_id=" << txn_id_;
                }

                DCHECK(table_id > 0);
                DCHECK(partition_id > 0);

                std::string ver_val;
                std::string ver_key =
                        partition_version_key({instance_id_, db_id, table_id, partition_id});
                err = txn->get(ver_key, &ver_val);
                if (TxnErrorCode::TXN_OK != err) {
                    code_ = err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                    ? MetaServiceCode::TXN_ID_NOT_FOUND
                                    : cast_as<ErrCategory::READ>(err);
                    ss << "failed to get partiton version, txn_id=" << txn_id_
                       << " key=" << hex(ver_key) << " err=" << err;
                    msg_ = ss.str();
                    LOG(WARNING) << msg_;
                    break;
                }
                VersionPB version_pb;
                if (!version_pb.ParseFromString(ver_val)) {
                    code_ = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "failed to parse version pb txn_id=" << txn_id_
                       << " key=" << hex(ver_key);
                    msg_ = ss.str();
                    break;
                }

                if (version_pb.pending_txn_ids_size() > 0 &&
                    version_pb.pending_txn_ids(0) == txn_id_) {
                    DCHECK(version_pb.pending_txn_ids_size() == 1);
                    version_pb.clear_pending_txn_ids();
                    ver_val.clear();

                    if (version_pb.has_version()) {
                        version_pb.set_version(version_pb.version() + 1);
                    } else {
                        // first commit txn version is 2
                        version_pb.set_version(2);
                    }
                    if (!version_pb.SerializeToString(&ver_val)) {
                        code_ = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                        ss << "failed to serialize version_pb when saving, txn_id=" << txn_id_;
                        msg_ = ss.str();
                        return;
                    }
                    txn->put(ver_key, ver_val);
                    LOG(INFO) << "put ver_key=" << hex(ver_key) << " txn_id=" << txn_id_
                              << " version_pb=" << version_pb.ShortDebugString();

                    err = txn->commit();
                    if (err != TxnErrorCode::TXN_OK) {
                        code_ = cast_as<ErrCategory::COMMIT>(err);
                        ss << "failed to commit kv txn, txn_id=" << txn_id_ << " err=" << err;
                        msg_ = ss.str();
                        break;
                    }
                }
            }
            make_committed_txn_visible(instance_id_, db_id, txn_id_, txn_kv_, code_, msg_);
        } while (false);
    } while (code_ == MetaServiceCode::KV_TXN_CONFLICT &&
             retry_times++ < config::txn_store_retry_times);
    {
        std::unique_lock<std::mutex> lock(mutex_);
        this->finished_ = true;
    }
    this->cond_.notify_all();
}

std::pair<MetaServiceCode, std::string> TxnLazyCommitTask::wait() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this]() { return this->finished_ == true; });
    }
    txn_lazy_committer_->remove(txn_id_);
    return std::make_pair(this->code_, this->msg_);
}

TxnLazyCommitter::TxnLazyCommitter(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(txn_kv) {
    worker_pool_ = std::make_unique<SimpleThreadPool>(config::txn_lazy_commit_num_threads);
    worker_pool_->start();
}

std::shared_ptr<TxnLazyCommitTask> TxnLazyCommitter::submit(const std::string& instance_id,
                                                            int64_t txn_id) {
    std::shared_ptr<TxnLazyCommitTask> task;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto iter = running_tasks_.find(txn_id);
        if (iter != running_tasks_.end()) {
            return iter->second;
        }

        task = std::make_shared<TxnLazyCommitTask>(instance_id, txn_id, txn_kv_, this);
        running_tasks_.emplace(txn_id, task);
    }

    worker_pool_->submit([task]() { task->commit(); });
    DCHECK(task != nullptr);
    return task;
}

void TxnLazyCommitter::remove(int64_t txn_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    running_tasks_.erase(txn_id);
}

} // namespace doris::cloud