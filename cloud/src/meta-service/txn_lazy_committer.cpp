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

#include "meta-service/txn_lazy_committer.h"

#include <gen_cpp/cloud.pb.h>

#include "common/logging.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"

namespace doris::cloud {

extern void scan_tmp_rowset(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, std::stringstream& ss, int64_t* db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* tmp_rowsets_meta);

extern void convert_tmp_rowsets(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, std::stringstream& ss, int64_t db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta);

TxnLazyCommitTask::TxnLazyCommitTask(const std::string& instance_id, int64_t txn_id,
                                     std::shared_ptr<TxnKv> txn_kv) {
    task_func_ = [=, this]() {
        MetaServiceCode code = MetaServiceCode::OK;
        std::stringstream ss;
        std::string msg;
        int64_t db_id;
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> tmp_rowsets_meta;
        scan_tmp_rowset(instance_id, txn_id, txn_kv, code, msg, ss, &db_id, &tmp_rowsets_meta);

        // <partition_id, tmp_rowsets>
        std::unordered_map<int64_t, std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>>
                partition_to_tmp_rowsets_meta;

        for (auto& [tmp_rowset_key, tmp_rowset_pb] : tmp_rowsets_meta) {
            partition_to_tmp_rowsets_meta[tmp_rowset_pb.partition_id()].emplace_back();
            partition_to_tmp_rowsets_meta[tmp_rowset_pb.partition_id()].back().first =
                    tmp_rowset_key;
            partition_to_tmp_rowsets_meta[tmp_rowset_pb.partition_id()].back().second =
                    tmp_rowset_pb;
        }

        for (auto& [partition_id, sub_tmp_rowsets_meta] : partition_to_tmp_rowsets_meta) {
            convert_tmp_rowsets(instance_id, txn_id, txn_kv, code, msg, ss, db_id,
                                sub_tmp_rowsets_meta);
        }
        this->finished_.store(true);
        this->cond_.notify_all();
    };
}

void TxnLazyCommitTask::wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return this->finished_.load() == true; });
}

TxnLazyCommitter::TxnLazyCommitter() {
    worker_pool_ = std::make_unique<SimpleThreadPool>(config::txn_lazy_commit_worker_num);
    worker_pool_->start();
}

std::shared_ptr<TxnLazyCommitTask> TxnLazyCommitter::submit(const std::string& instance_id,
                                                            int64_t txn_id,
                                                            std::shared_ptr<TxnKv> txn_kv) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto iter = running_tasks_.find(txn_id);
    if (iter != running_tasks_.end()) {
        return iter->second;
    }

    auto task = std::make_shared<TxnLazyCommitTask>(instance_id, txn_id, txn_kv);
    running_tasks_.emplace(txn_id, task);
    worker_pool_->submit(task->task_func_);
    return task;
}

void TxnLazyCommitter::remove(int64_t txn_id) {
    std::unique_lock<std::mutex> lock(mutex_);
}

} // namespace doris::cloud