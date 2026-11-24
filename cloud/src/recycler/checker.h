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

#include "resource-manager/resource_manager.h"
#if defined(USE_LIBCPP) && _LIBCPP_ABI_VERSION <= 1
#define _LIBCPP_ABI_INCOMPLETE_TYPES_IN_DEQUE
#endif
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "meta-store/txn_kv_error.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/white_black_list.h"
#include "snapshot/snapshot_manager.h"

namespace doris {
class RowsetMetaCloudPB;
} // namespace doris

namespace doris::cloud {
class StorageVaultAccessor;
class InstanceChecker;
class TxnKv;
class InstanceInfoPB;
class ObjectStoreInfoPB;
class HdfsVaultInfo;

struct PendingTableStreamDrop {
    int64_t base_db_id;
    int64_t base_table_id;
    int64_t stream_db_id;

    bool matches(int64_t offset_base_db_id, int64_t offset_base_table_id,
                 int64_t offset_stream_db_id) const {
        return base_db_id == offset_base_db_id && base_table_id == offset_base_table_id &&
               stream_db_id == offset_stream_db_id;
    }
};

TxnErrorCode collect_pending_table_stream_drops(
        const std::shared_ptr<TxnKv>& txn_kv, std::string_view instance_id,
        std::unordered_map<int64_t, PendingTableStreamDrop>* pending_drops);

struct DeleteBitmapOrphanCheckResult {
    int64_t scanned_kvs = 0;
    bool range_exhausted = false;
    bool cancelled = false;
    std::string key;
    int64_t tablet_id = 0;
    std::string rowset_id;
    int64_t version = 0;
    int64_t segment_id = 0;
};

// Scan v1 bitmap keys in descending key order and return the first one without a formal v1
// meta rowset. Pending bitmaps and unexpired job tmp rowsets are not excluded, so a match is
// only a suspected orphan. tablet_id == 0 scans the instance; max_scan_keys == 0 is unlimited.
// Cancellation is checked between reads and while iterating bitmap and rowset keys. A read
// already in progress must return or time out before cancellation can be observed.
std::pair<MetaServiceCode, std::string> check_delete_bitmap_orphan(
        const std::shared_ptr<TxnKv>& txn_kv, const std::string& instance_id, int64_t tablet_id,
        int64_t max_scan_keys, DeleteBitmapOrphanCheckResult* result,
        const std::function<bool()>& should_cancel = [] { return false; });

struct SharedResourceOwner {
    std::string instance_id;
    std::string resource_id;
};

// resource id -> all instance/resource pairs that use the same physical storage.
using SharedResourceOwners = std::unordered_map<std::string, std::vector<SharedResourceOwner>>;

// Tells which instance/resource pairs write into the same physical storage.
// Resource IDs are local to an instance, so the physical storage location is
// matched instead: descendants can write into inherited resources even after
// decoupling removes their source_instance_id.
//
// One index is shared by all checkers of a scheduling round. It is loaded
// lazily on the first query, and the result (success or failure) is cached
// until the round ends. A new round must create a new index to see changes.
class SharedStorageIndex {
public:
    // If `instances` is given, it is used as the full instance list instead of
    // scanning all instances from the kv store.
    explicit SharedStorageIndex(
            std::shared_ptr<TxnKv> txn_kv,
            std::optional<std::vector<InstanceInfoPB>> instances = std::nullopt);

    // For every resource of `instance_id`, fill in all owners sharing its physical storage.
    // A resource without identifiable storage location is only owned by itself.
    // Return 0 on success, -1 if the index failed to load or the instance is unknown.
    // Never fills partial owners on failure.
    int get_shared_resource_owners(std::string_view instance_id, SharedResourceOwners* owners);

private:
    // Physical storage location. All fields must match exactly, overlapping
    // prefixes are not merged. Credentials are deliberately not part of it.
    struct StorageLocation {
        std::string type;     // "s3" or "hdfs"
        std::string endpoint; // S3 endpoint, or HDFS fs_name
        std::string bucket;   // empty for HDFS
        std::string prefix;

        bool operator<(const StorageLocation& other) const {
            return std::tie(type, endpoint, bucket, prefix) <
                   std::tie(other.type, other.endpoint, other.bucket, other.prefix);
        }
    };

    // Resources without connection info have no location, so that they never
    // match unrelated resources.
    using ResourceLocations = std::map<std::string, std::optional<StorageLocation>>;

    static std::optional<StorageLocation> s3_location(const ObjectStoreInfoPB& obj_info);
    static std::optional<StorageLocation> hdfs_location(const HdfsVaultInfo& hdfs_info);

    void ensure_loaded();
    int load_all_instances();
    int load_instance(const InstanceInfoPB& instance);
    int load_storage_vaults(const std::string& instance_id);
    void add_resource(const std::string& instance_id, const std::string& resource_id,
                      std::optional<StorageLocation> location);

    std::shared_ptr<TxnKv> txn_kv_;
    // Consumed by the first load.
    std::optional<std::vector<InstanceInfoPB>> preloaded_instances_;

    std::once_flag load_once_;
    int load_status_ = -1;
    // instance_id -> (resource_id -> location)
    std::unordered_map<std::string, ResourceLocations> instance_resource_locations_;
    // location -> all instance/resource pairs located there
    std::map<StorageLocation, std::vector<SharedResourceOwner>> location_to_owners_;
};

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
    std::shared_ptr<ResourceManager> resource_mgr_;
    std::atomic_bool stopped_ {false};
    std::string ip_port_;
    std::vector<std::thread> workers_;

    std::mutex mtx_;
    // notify check workers
    std::condition_variable pending_instance_cond_;
    struct PendingInstance {
        InstanceInfoPB instance;
        std::shared_ptr<SharedStorageIndex> shared_storage_index;
    };
    std::deque<PendingInstance> pending_instance_queue_;
    // instance_id -> enqueue_timestamp
    std::unordered_map<std::string, long> pending_instance_map_;
    std::unordered_map<std::string, std::shared_ptr<InstanceChecker>> working_instance_map_;
    // notify instance scanner and lease thread
    std::condition_variable notifier_;

    WhiteBlackList instance_filter_;
};

class InstanceChecker {
public:
    explicit InstanceChecker(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                             std::shared_ptr<ResourceManager> resource_mgr = nullptr,
                             std::shared_ptr<SharedStorageIndex> shared_storage_index = nullptr);
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

    // Return 0 if success.
    // Return 1 if snapshot key and file leak or loss is identified.
    // Return negative if a temporary error occurred during the check process.
    int do_snapshots_check();

    // Return 0 if success.
    // Return 1 if mvcc meta key and data leak or loss is identified.
    // Return negative if a temporary error occurred during the check process.
    int do_mvcc_meta_key_check();

    // Return 0 if all Table Stream mappings and offsets are consistent.
    // Return 1 if an inconsistent mapping or offset is identified.
    // Return negative if a temporary error occurred during the check process.
    int do_table_stream_check();

    // Return 0 if success.
    // Return 1 if packed file metadata leak or loss is identified.
    // Return negative if a temporary error occurred during the check process.
    int do_packed_file_check();

    StorageVaultAccessor* get_accessor(const std::string& id);

    ResourceManager* resource_mgr() const { return resource_mgr_.get(); }

    SharedStorageIndex* shared_storage_index() const { return shared_storage_index_.get(); }

    void get_all_accessor(std::vector<StorageVaultAccessor*>* accessors);

    std::string_view instance_id() const { return instance_id_; }

    void TEST_add_accessor(std::string_view id, std::shared_ptr<StorageVaultAccessor> accessor) {
        accessor_map_.insert({std::string(id), std::move(accessor)});
    }

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
    int collect_unexpired_job_tmp_rowsets(
            std::unordered_map<int64_t, std::unordered_set<std::string>>& tmp_rowsets);
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
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<ResourceManager> resource_mgr_;
    bool table_stream_versioned_write_ {false};
    std::shared_ptr<SharedStorageIndex> shared_storage_index_;
};

} // namespace doris::cloud
