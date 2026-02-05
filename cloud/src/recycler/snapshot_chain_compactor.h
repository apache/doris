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

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "meta-service/txn_lazy_committer.h"
#include "recycler/storage_vault_accessor.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

class InstanceChainCompactor;

class SnapshotChainCompactor {
public:
    explicit SnapshotChainCompactor(std::shared_ptr<TxnKv> txn_kv);
    ~SnapshotChainCompactor();

    // returns 0 for success otherwise error
    int start();

    void stop();

    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

private:
    void scan_instance_loop();
    void compaction_loop();
    void lease_compaction_jobs();

    bool is_snapshot_chain_need_compact(const InstanceInfoPB& instance_info);

    std::shared_ptr<TxnKv> txn_kv_;
    std::atomic_bool stopped_ {false};
    std::string ip_port_;
    std::vector<std::thread> workers_;

    std::mutex mtx_;
    // notify compaction workers
    std::condition_variable pending_instance_cond_;
    std::deque<InstanceInfoPB> pending_instance_queue_;
    std::unordered_set<std::string> pending_instance_set_;
    std::unordered_map<std::string, std::shared_ptr<InstanceChainCompactor>>
            compacting_instance_map_;
    // notify instance scanner and lease thread
    std::condition_variable notifier_;

    std::shared_ptr<TxnLazyCommitter> txn_lazy_committer_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
};

class InstanceChainCompactor {
public:
    InstanceChainCompactor(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance);
    ~InstanceChainCompactor();

    std::string_view instance_id() const { return instance_id_; }
    const InstanceInfoPB& instance_info() const { return instance_info_; }

    // returns 0 for success otherwise error
    int init();

    void stop() { stopped_.store(true, std::memory_order_release); }
    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

    // returns 0 for success otherwise error
    int do_compact();

private:
    // returns 0 for success otherwise error
    int init_obj_store_accessors();

    // returns 0 for success otherwise error
    int init_storage_vault_accessors();

    int handle_compaction_completion();

    std::atomic_bool stopped_ {false};
    std::shared_ptr<TxnKv> txn_kv_;
    std::string instance_id_;
    InstanceInfoPB instance_info_;

    std::unordered_map<std::string, std::shared_ptr<StorageVaultAccessor>> accessor_map_;
};

} // namespace doris::cloud