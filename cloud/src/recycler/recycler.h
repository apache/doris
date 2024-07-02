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

#include <gen_cpp/cloud.pb.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "recycler/storage_vault_accessor.h"
#include "recycler/white_black_list.h"

namespace brpc {
class Server;
} // namespace brpc

namespace doris::cloud {
class TxnKv;
class InstanceRecycler;
class StorageVaultAccessor;
class Checker;

class Recycler {
public:
    explicit Recycler(std::shared_ptr<TxnKv> txn_kv);
    ~Recycler();

    // returns 0 for success otherwise error
    int start(brpc::Server* server);

    void stop();

    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

private:
    void recycle_callback();

    void instance_scanner_callback();

    void lease_recycle_jobs();

    void check_recycle_tasks();

private:
    friend class RecyclerServiceImpl;

    std::shared_ptr<TxnKv> txn_kv_;
    std::atomic_bool stopped_ {false};

    std::vector<std::thread> workers_;

    std::mutex mtx_;
    // notify recycle workers
    std::condition_variable pending_instance_cond_;
    std::deque<InstanceInfoPB> pending_instance_queue_;
    std::unordered_set<std::string> pending_instance_set_;
    std::unordered_map<std::string, std::shared_ptr<InstanceRecycler>> recycling_instance_map_;
    // notify instance scanner and lease thread
    std::condition_variable notifier_;

    std::string ip_port_;

    WhiteBlackList instance_filter_;
    std::unique_ptr<Checker> checker_;
};

class InstanceRecycler {
public:
    explicit InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance);
    ~InstanceRecycler();

    // returns 0 for success otherwise error
    int init();

    void stop() { stopped_.store(true, std::memory_order_release); }
    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

    // returns 0 for success otherwise error
    int do_recycle();

    // remove all kv and data in this instance, ONLY be called when instance has been deleted
    // returns 0 for success otherwise error
    int recycle_deleted_instance();

    // scan and recycle expired indexes
    // returns 0 for success otherwise error
    int recycle_indexes();

    // scan and recycle expired partitions
    // returns 0 for success otherwise error
    int recycle_partitions();

    // scan and recycle expired rowsets
    // returns 0 for success otherwise error
    int recycle_rowsets();

    // scan and recycle expired tmp rowsets
    // returns 0 for success otherwise error
    int recycle_tmp_rowsets();

    /**
     * recycle all tablets belonging to the index specified by `index_id`
     *
     * @param partition_id if positive, only recycle tablets in this partition belonging to the specified index
     * @param is_empty_tablet indicates whether the tablet has object files, can skip delete objects if tablet is empty
     * @return 0 for success otherwise error
     */
    int recycle_tablets(int64_t table_id, int64_t index_id, int64_t partition_id = -1,
                        bool is_empty_tablet = false);

    /**
     * recycle all rowsets belonging to the tablet specified by `tablet_id`
     *
     * @return 0 for success otherwise error
     */
    int recycle_tablet(int64_t tablet_id);

    // scan and recycle useless partition version kv
    int recycle_versions();

    // scan and abort timeout txn label
    // returns 0 for success otherwise error
    int abort_timeout_txn();

    //scan and recycle expire txn label
    // returns 0 for success otherwise error
    int recycle_expired_txn_label();

    // scan and recycle finished or timeout copy jobs
    // returns 0 for success otherwise error
    int recycle_copy_jobs();

    // scan and recycle dropped internal stage
    // returns 0 for success otherwise error
    int recycle_stage();

    // scan and recycle expired stage objects
    // returns 0 for success otherwise error
    int recycle_expired_stage_objects();

    bool check_recycle_tasks();

private:
    // returns 0 for success otherwise error
    int init_obj_store_accessors();

    // returns 0 for success otherwise error
    int init_storage_vault_accessors();

    /**
     * Scan key-value pairs between [`begin`, `end`), and perform `recycle_func` on each key-value pair.
     *
     * @param recycle_func defines how to recycle resources corresponding to a key-value pair. Returns 0 if the recycling is successful.
     * @param loop_done is called after `RangeGetIterator` has no next kv. Usually used to perform a batch recycling. Returns 0 if success. 
     * @return 0 if all corresponding resources are recycled successfully, otherwise non-zero
     */
    int scan_and_recycle(std::string begin, std::string_view end,
                         std::function<int(std::string_view k, std::string_view v)> recycle_func,
                         std::function<int()> loop_done = nullptr);
    // return 0 for success otherwise error
    int delete_rowset_data(const doris::RowsetMetaCloudPB& rs_meta_pb);
    // return 0 for success otherwise error
    // NOTE: this function ONLY be called when the file paths cannot be calculated
    int delete_rowset_data(const std::string& resource_id, int64_t tablet_id,
                           const std::string& rowset_id);
    // return 0 for success otherwise error
    int delete_rowset_data(const std::vector<doris::RowsetMetaCloudPB>& rowsets);

    /**
     * Get stage storage info from instance and init StorageVaultAccessor
     * @return 0 if accessor is successfully inited, 1 if stage not found, negative for error
     */
    int init_copy_job_accessor(const std::string& stage_id, const StagePB::StageType& stage_type,
                               std::shared_ptr<StorageVaultAccessor>* accessor);

    void register_recycle_task(const std::string& task_name, int64_t start_time);

    void unregister_recycle_task(const std::string& task_name);

private:
    std::atomic_bool stopped_ {false};
    std::shared_ptr<TxnKv> txn_kv_;
    std::string instance_id_;
    InstanceInfoPB instance_info_;

    // TODO(plat1ko): Add new accessor to map in runtime for new created storage vaults
    std::unordered_map<std::string, std::shared_ptr<StorageVaultAccessor>> accessor_map_;

    class InvertedIndexIdCache;
    std::unique_ptr<InvertedIndexIdCache> inverted_index_id_cache_;

    std::mutex recycled_tablets_mtx_;
    // Store recycled tablets, we can skip deleting rowset data of these tablets because these data has already been deleted.
    std::unordered_set<int64_t> recycled_tablets_;

    std::mutex recycle_tasks_mutex;
    // <task_name, start_time>>
    std::map<std::string, int64_t> running_recycle_tasks;
};

} // namespace doris::cloud
