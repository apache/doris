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

#include "common/config.h"
#include "common/simple_thread_pool.h"
#include "meta-service/txn_kv.h"

namespace doris::cloud {

class TxnLazyCommitTask {
public:
    TxnLazyCommitTask(const std::string& instance_id, int64_t txn_id,
                      std::shared_ptr<TxnKv> txn_kv);

    void wait();

private:
    friend class TxnLazyCommitter;
    std::function<void()> task_func_;
    std::mutex mutex_;
    std::condition_variable cond_;
    std::atomic_bool finished_ = false;
};

class TxnLazyCommitter {
public:
    TxnLazyCommitter();
    std::shared_ptr<TxnLazyCommitTask> submit(const std::string& instance_id, int64_t txn_id,
                                              std::shared_ptr<TxnKv> txn_kv);
    void remove(int64_t txn_id);

private:
    std::unique_ptr<SimpleThreadPool> worker_pool_;

    std::mutex mutex_;
    // <txn_id, TxnLazyCommitTask>
    std::unordered_map<int64_t, std::shared_ptr<TxnLazyCommitTask>> running_tasks_;
};
} // namespace doris::cloud