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

#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <gen_cpp/cloud.pb.h>

#include <memory>

#include "common/simple_thread_pool.h"
#include "meta-store/clone_chain_reader.h"
#include "meta-store/txn_kv.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {

class TxnLazyCommitter;

class TxnLazyCommitTask {
public:
    TxnLazyCommitTask(const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
                      TxnLazyCommitter* txn_lazy_committer);
    void commit();

    std::pair<MetaServiceCode, std::string> wait();

    int64_t txn_id() const { return txn_id_; }

private:
    friend class TxnLazyCommitter;

    std::pair<MetaServiceCode, std::string> commit_partition(
            int64_t db_id, int64_t partition_id,
            const std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowset_metas,
            bool is_versioned_write, bool is_versioned_read,
            bool defer_deleting_pending_delete_bitmaps);

    std::string instance_id_;
    int64_t txn_id_;
    std::shared_ptr<TxnKv> txn_kv_;
    bthread::Mutex mutex_;
    bthread::ConditionVariable cond_;
    bool finished_ = false;
    MetaServiceCode code_ = MetaServiceCode::OK;
    std::string msg_;
    TxnLazyCommitter* txn_lazy_committer_;
};

class TxnLazyCommitter {
public:
    TxnLazyCommitter(std::shared_ptr<TxnKv> txn_kv);
    TxnLazyCommitter(std::shared_ptr<TxnKv> txn_kv, std::shared_ptr<ResourceManager> resource_mgr);
    std::shared_ptr<TxnLazyCommitTask> submit(const std::string& instance_id, int64_t txn_id);
    void remove(int64_t txn_id);

    std::shared_ptr<ResourceManager>& resource_manager() { return resource_mgr_; }
    std::shared_ptr<SimpleThreadPool>& parallel_commit_pool() { return parallel_commit_pool_; }

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::shared_ptr<ResourceManager> resource_mgr_;

    std::unique_ptr<SimpleThreadPool> worker_pool_;
    std::shared_ptr<SimpleThreadPool> parallel_commit_pool_;

    std::mutex mutex_;
    // <txn_id, TxnLazyCommitTask>
    std::unordered_map<int64_t, std::shared_ptr<TxnLazyCommitTask>> running_tasks_;
};
} // namespace doris::cloud