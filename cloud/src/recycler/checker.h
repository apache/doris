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

#if defined(USE_LIBCPP) && _LIBCPP_ABI_VERSION <= 1
#define _LIBCPP_ABI_INCOMPLETE_TYPES_IN_DEQUE
#endif
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "recycler/storage_vault_accessor.h"
#include "recycler/white_black_list.h"

namespace doris {
class RowsetMetaCloudPB;
} // namespace doris

namespace doris::cloud {
class StorageVaultAccessor;
class InstanceChecker;
class TxnKv;
class InstanceInfoPB;

class Checker {
public:
    explicit Checker(std::shared_ptr<TxnKv> txn_kv);
    ~Checker();

    int start();

    void stop();
    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

private:
    void lease_check_jobs();
    void inspect_instance_check_interval();
    void do_inspect(const InstanceInfoPB& instance);

private:
    friend class RecyclerServiceImpl;

    std::shared_ptr<TxnKv> txn_kv_;
    std::atomic_bool stopped_ {false};
    std::string ip_port_;
    std::vector<std::thread> workers_;

    std::mutex mtx_;
    // notify check workers
    std::condition_variable pending_instance_cond_;
    std::deque<InstanceInfoPB> pending_instance_queue_;
    // instance_id -> enqueue_timestamp
    std::unordered_map<std::string, long> pending_instance_map_;
    std::unordered_map<std::string, std::shared_ptr<InstanceChecker>> working_instance_map_;
    // notify instance scanner and lease thread
    std::condition_variable notifier_;

    WhiteBlackList instance_filter_;
};

class InstanceChecker {
public:
    explicit InstanceChecker(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id);
    // Return 0 if success, otherwise error
    int init(const InstanceInfoPB& instance);
    // Return 0 if success.
    // Return 1 if data leak is identified.
    // Return negative if a temporary error occurred during the check process.
    int do_inverted_check();

    // Return 0 if success.
    // Return 1 if data loss is identified.
    // Return negative if a temporary error occurred during the check process.
    int do_check();

    // Return 0 if success.
    // Return 1 if delete bitmap leak is identified.
    // Return negative if a temporary error occurred during the check process.
    int do_delete_bitmap_inverted_check();

    // version = 1 : https://github.com/apache/doris/pull/40204
    // checks if https://github.com/apache/doris/pull/40204 works as expected
    // the stale delete bitmap will be cleared in MS when BE delete expired stale rowsets
    // NOTE: stale rowsets will be lost after BE restarts, so there may be some stale delete bitmaps
    // which will not be cleared.
    // version = 2 : https://github.com/apache/doris/pull/49822
    int do_delete_bitmap_storage_optimize_check(int version = 2);

    int do_mow_job_key_check();

    int do_tablet_stats_key_check();

    int do_restore_job_check();

    int do_txn_key_check();

    // check table and partition version key
    // table version should be greater than the versions of all its partitions
    // Return 0 if success, otherwise error
    int do_version_key_check();

    // Return 0 if success.
    // Return 1 if meta rowset key leak or loss is identified.
    // Return negative if a temporary error occurred during the check process.
    int do_meta_rowset_key_check();

    // If there are multiple buckets, return the minimum lifecycle; if there are no buckets (i.e.
    // all accessors are HdfsAccessor), return INT64_MAX.
    // Return 0 if success, otherwise error
    int get_bucket_lifecycle(int64_t* lifecycle_days);
    void stop() { stopped_.store(true, std::memory_order_release); }
    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

private:
    struct RowsetIndexesFormatV1 {
        std::string rowset_id;
        std::unordered_set<int64_t> segment_ids;
        std::unordered_set<std::string> index_ids;
    };

    struct RowsetIndexesFormatV2 {
        std::string rowset_id;
        std::unordered_set<int64_t> segment_ids;
    };

private:
    // returns 0 for success otherwise error
    int init_obj_store_accessors(const InstanceInfoPB& instance);

    // returns 0 for success otherwise error
    int init_storage_vault_accessors(const InstanceInfoPB& instance);

    int traverse_mow_tablet(const std::function<int(int64_t, bool)>& check_func);
    int traverse_rowset_delete_bitmaps(
            int64_t tablet_id, std::string rowset_id,
            const std::function<int(int64_t, std::string_view, int64_t, int64_t)>& callback);
    int collect_tablet_rowsets(
            int64_t tablet_id,
            const std::function<void(const doris::RowsetMetaCloudPB&)>& collect_cb);
    int get_pending_delete_bitmap_keys(int64_t tablet_id,
                                       std::unordered_set<std::string>& pending_delete_bitmaps);
    int check_delete_bitmap_storage_optimize_v2(int64_t tablet_id, bool has_sequence_col,
                                                int64_t& abnormal_rowsets_num);

    int check_inverted_index_file_storage_format_v1(int64_t tablet_id, const std::string& file_path,
                                                    const std::string& rowset_info,
                                                    RowsetIndexesFormatV1& rowset_index_cache_v1);

    int check_inverted_index_file_storage_format_v2(int64_t tablet_id, const std::string& file_path,
                                                    const std::string& rowset_info,
                                                    RowsetIndexesFormatV2& rowset_index_cache_v2);

    // Return 0 if success.
    // Return 1 if key loss is abnormal.
    // Return negative if a temporary error occurred during the check process.
    int check_stats_tablet_key(std::string_view key, std::string_view value);

    // Return 0 if success.
    // Return 1 if key loss is identified.
    // Return negative if a temporary error occurred during the check process.
    int check_stats_tablet_key_exists(std::string_view key, std::string_view value);

    // Return 0 if success.
    // Return 1 if key leak is identified.
    // Return negative if a temporary error occurred during the check process.
    int check_stats_tablet_key_leaked(std::string_view key, std::string_view value);
    int check_txn_info_key(std::string_view key, std::string_view value);

    int check_txn_label_key(std::string_view key, std::string_view value);

    int check_txn_index_key(std::string_view key, std::string_view value);

    int check_txn_running_key(std::string_view key, std::string_view value);

    // Only check whether the meta rowset key is leak
    // in do_inverted_check() function, check whether the key is lost by comparing data file with key
    // Return 0 if success.
    // Return 1 if meta rowset key leak is identified.
    // Return negative if a temporary error occurred during the check process.
    int check_meta_rowset_key(std::string_view key, std::string_view value);

    // if TxnInfoKey's finish time > current time, it should not find tmp rowset
    // Return 0 if success.
    // Return 1 if meta tmp rowset key is abnormal.
    // Return negative if a temporary error occurred during the check process.
    int check_meta_tmp_rowset_key(std::string_view key, std::string_view value);

    /**
     * It is used to scan the key in the range from start_key to end_key 
     * and then perform handle operations on each group of kv
     * 
     * @param start_key Range begining. Note that this function will modify the `start_key`
     * @param end_key Range ending
     * @param handle_kv Operations on kv
     * @return code int 0 for success to scan and hanle, 1 for success to scan but handle abnormally, -1 for failed to handle 
     */
    int scan_and_handle_kv(std::string& start_key, const std::string& end_key,
                           std::function<int(std::string_view, std::string_view)> handle_kv);

    std::atomic_bool stopped_ {false};
    std::shared_ptr<TxnKv> txn_kv_;
    std::string instance_id_;
    // id -> accessor
    std::unordered_map<std::string, std::shared_ptr<StorageVaultAccessor>> accessor_map_;
};

} // namespace doris::cloud
