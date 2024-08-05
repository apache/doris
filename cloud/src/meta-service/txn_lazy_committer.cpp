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

#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"

namespace doris::cloud {

void scan_tmp_rowset(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, int64_t* db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* tmp_rowsets_meta);

void convert_tmp_rowsets(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, int64_t db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta,
        std::unordered_map<int64_t, TabletIndexPB>& tablet_ids);

void make_committed_txn_visible(const std::string& instance_id, int64_t db_id, int64_t txn_id,
                                std::shared_ptr<TxnKv> txn_kv, MetaServiceCode& code,
                                std::string& msg);

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
            break;
        }

        // <partition_id, tmp_rowsets>
        std::unordered_map<int64_t, std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>>
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
                size_t end = (i + config::txn_lazy_max_rowsets_per_batch) > tmp_rowset_metas.size()
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
            }

            DCHECK(table_id > 0);
            DCHECK(partition_id > 0);

            std::string ver_val;
            std::string ver_key =
                    partition_version_key({instance_id_, db_id, table_id, partition_id});
            err = txn->get(ver_key, &ver_val);
            if (TxnErrorCode::TXN_OK != err) {
                code_ = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
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
                ss << "failed to parse version pb txn_id=" << txn_id_ << " key=" << hex(ver_key);
                msg_ = ss.str();
                break;
            }

            if (version_pb.has_txn_id() && version_pb.txn_id() == txn_id_) {
                version_pb.clear_txn_id();
                ver_val.clear();
                if (!version_pb.SerializeToString(&ver_val)) {
                    code_ = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                    ss << "failed to serialize version_pb when saving, txn_id=" << txn_id_;
                    msg_ = ss.str();
                    return;
                }
                txn->put(ver_key, ver_val);
            }

            err = txn->commit();
            if (err != TxnErrorCode::TXN_OK) {
                code_ = cast_as<ErrCategory::COMMIT>(err);
                ss << "failed to commit kv txn, txn_id=" << txn_id_ << " err=" << err;
                msg_ = ss.str();
                break;
            }
        }
        make_committed_txn_visible(instance_id_, db_id, txn_id_, txn_kv_, code_, msg_);
    } while (false);
    {
        std::unique_lock<std::mutex> lock(mutex_);
        this->finished_ = true;
    }
    this->cond_.notify_all();
}

std::pair<MetaServiceCode, std::string> TxnLazyCommitTask::wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return this->finished_ == true; });
    txn_lazy_committer_->remove(txn_id_);
    return std::make_pair(this->code_, this->msg_);
}

TxnLazyCommitter::TxnLazyCommitter(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(txn_kv) {
    worker_pool_ = std::make_unique<SimpleThreadPool>(config::txn_lazy_commit_worker_num);
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