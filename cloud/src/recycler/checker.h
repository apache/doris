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
#include <memory>
#include <set>
#include <string>
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
    class PackedFileChecker;
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
};

class InstanceChecker::PackedFileChecker {
public:
    /**
         * Creates a checker for the enclosing instance.
         *
         * @param checker Instance whose packed files and metadata are checked.
         */
    explicit PackedFileChecker(InstanceChecker& checker);

    /**
         * Discovers every packed-file candidate and checks its metadata, references, and object.
         *
         * @return 0 if all candidates are consistent, 1 if a mismatch is found, and -1 if the
         *         check is interrupted or encounters a temporary error.
         */
    int run();

private:
    // (tablet_id, rowset_id, txn_id): identifies an owner across visible/tmp/recycle
    // metadata; txn_id is needed to locate the tmp rowset key.
    using RowsetIdentity = std::tuple<int64_t, std::string, int64_t>;
    using BitmapIdentity = std::pair<int64_t, std::string>;
    using BitmapPaths = std::map<BitmapIdentity, std::set<std::string>>;

    // A packed-file path discovered from any independent source. Rowsets seed owner lookup,
    // while object resources ensure orphan objects are checked even without a packed KV.
    struct Candidate {
        std::set<RowsetIdentity> rowsets;
        std::map<RowsetIdentity, std::set<std::string>> visible_rowset_keys;
        std::set<BitmapIdentity> bitmap_owners;
        std::unordered_set<std::string> object_resources;
        std::set<std::string> conflicting_small_paths;
    };

    struct PackedFileMetadata {
        int packed_ret = 1;
        PackedFileInfoPB info;
        std::unordered_set<std::string> resources;
    };

    struct Reference {
        RowsetIdentity owner;
        PackedSliceLocationPB location;
        bool is_delete_bitmap = false;
    };

    struct FileCheckContext {
        /**
             * Creates the state shared by all metadata checks for one packed-file path.
             *
             * @param path Packed-file object path being checked.
             * @param candidate Owners and object resources found during candidate discovery.
             * @param report_mismatches Whether confirmed mismatches update counters and warnings.
             */
        FileCheckContext(const std::string& path, const Candidate& candidate,
                         bool report_mismatches);

        // Packed-file object path checked by this context.
        const std::string& path;
        // False on the initial pass and true on the confirmation pass after a mismatch.
        bool report_mismatches;
        std::unique_ptr<Transaction> txn;
        // Packed-file KV lookup result and resource IDs collected from matching rowsets.
        PackedFileMetadata packed_file_metadata;
        // Rowset identities to resolve from visible, tmp, or recycle metadata.
        std::set<RowsetIdentity> owners;
        // Exact visible-rowset keys found during discovery, grouped by owner identity.
        std::map<RowsetIdentity, std::set<std::string>> visible_rowset_keys;
        // Delete-bitmap owners found by the independent versioned metadata scan.
        std::set<BitmapIdentity> bitmap_owners;
        // Small-file paths whose discovery references require a current-snapshot conflict check.
        std::set<std::string> conflicting_small_paths;
        // Packed KV slices indexed by path; pointers refer into packed_file_metadata.info.
        std::unordered_map<std::string, const PackedSlicePB*> slices;
        // Delete-bitmap slice paths in the packed KV, indexed by tablet and rowset.
        std::map<std::pair<int64_t, std::string>, std::string> bitmap_paths;
        // Current-snapshot references for every observed small-file path.
        std::unordered_map<std::string, Reference> observed_references;
        // Current-snapshot references whose locations point to this packed-file path.
        std::unordered_map<std::string, Reference> references;
        // Cached partition or index recycling state, keyed by its recycle metadata key.
        std::unordered_map<std::string, bool> recycling_states;
        // Number of non-deleted slices in the packed KV.
        int64_t live_slices = 0;
        int result = 0;
    };

    struct Stats {
        long num_scanned_rowsets = 0;
        long num_scanned_packed_files = 0;
        long num_packed_file_loss = 0;
        long num_packed_file_leak = 0;
        long num_packed_file_meta_mismatch = 0;
        long num_ref_count_mismatch = 0;
        long num_small_file_ref_mismatch = 0;

        /**
             * Reports whether any confirmed mismatch counter is nonzero.
             *
             * @return true if the check found a mismatch, otherwise false.
             */
        bool has_mismatch() const;
    };

    /**
     * Collects the union of packed-file paths from rowsets, delete bitmaps, packed KVs, and
     * object listings.
         *
         * @return 0 on success and -1 if a scan fails or is interrupted.
         */
    int collect_candidates();

    int collect_delete_bitmap_candidates(BitmapPaths* bitmap_paths);
    int collect_rowset_candidates(const BitmapPaths& bitmap_paths);

    /**
         * Checks one packed file and coordinates its KV-snapshot and object-store checks.
         *
         * @param path Packed-file object path to check.
         * @param discovered Candidate owners and object resources found by the initial scans.
         * @param report_mismatches Whether confirmed mismatches update counters and warnings.
         * @return 0 if consistent, 1 if a mismatch is found, and -1 on a temporary error.
         */
    int check_file(const std::string& path, const Candidate& discovered, bool report_mismatches);

    /**
         * Reads the packed KV and validates its live slices against rowsets in one KV snapshot.
         *
         * @param context Shared state for the packed file; populated with metadata and references.
         * @return 0 if consistent, 1 if a mismatch is found, and -1 on a temporary error.
         */
    int check_metadata_and_references(FileCheckContext* context);

    /**
         * Reads visible, tmp, and recycle metadata for every known packed-file owner.
         *
         * @param context Shared state containing owners and receiving valid small-file references.
         * @return 0 on success and -1 if a metadata read fails or is interrupted.
         */
    int collect_owner_references(FileCheckContext* context);

    int collect_visible_references(const RowsetIdentity& owner, FileCheckContext* context,
                                   std::unordered_set<std::string>* visible_keys,
                                   bool* owner_found);

    int scan_visible_references(int64_t tablet_id, FileCheckContext* context);

    /**
         * Adds references from one rowset unless its packed data is in a legal recycle transition.
         *
         * @param rowset Rowset metadata whose packed slice locations are inspected.
         * @param recyclable Whether the rowset itself may already be reclaimed.
         * @param context Shared state receiving references, resources, and mismatches.
         * @return 0 on success and -1 if dependent metadata cannot be read.
         */
    int collect_rowset_references(const RowsetMetaCloudPB& rowset, bool recyclable,
                                  FileCheckContext* context);

    void record_reference(const RowsetMetaCloudPB& rowset, const std::string& small_path,
                          const PackedSliceLocationPB& location, bool is_delete_bitmap,
                          FileCheckContext* context);

    /**
         * Compares live slices, valid rowset references, and the packed-file reference count.
         *
         * @param context Shared state containing the sets and counters to compare.
         */
    void check_reference_consistency(FileCheckContext* context);

    /**
         * Checks object existence in every resource inferred from metadata or object listing.
         *
         * @param discovered Candidate object resources found by the initial listing.
         * @param context Shared state containing the path, packed metadata, and reporting mode.
         * @return 0 if consistent, 1 if a mismatch is found, and -1 on an accessor error.
         */
    int check_objects(const Candidate& discovered, FileCheckContext* context);

    /**
         * Reads and parses one protobuf value in the current packed-file KV snapshot.
         *
         * @param context Shared state containing the active transaction.
         * @param key Metadata key to read.
         * @param pb Output protobuf populated when the key exists.
         * @return 0 if read and parsed, 1 if the key is absent, and -1 on a read or parse error.
         */
    template <typename PB>
    int get_and_parse_metadata(FileCheckContext* context, const std::string& key, PB* pb);

    /**
         * Reads and caches whether recycle metadata is in the RECYCLING state.
         *
         * @param context Shared state containing the active transaction and state cache.
         * @param key Recycle metadata key to inspect.
         * @param pb Scratch protobuf used to parse the metadata value.
         * @return 1 if the key exists in RECYCLING state, 0 otherwise, and -1 on an error.
         */
    template <typename PB>
    int is_recycling(FileCheckContext* context, const std::string& key, PB* pb);

    /**
         * Marks the current file inconsistent and records the mismatch when reporting is enabled.
         *
         * @param context Shared state for the current packed file.
         * @param count Counter associated with the mismatch type.
         * @param reason Diagnostic text describing the mismatch.
         */
    void mark_mismatch(FileCheckContext* context, long* count, const std::string& reason);

    InstanceChecker& checker_;
    Stats stats_;
    // Packed-file candidates keyed by object path, unioned from rowsets, delete bitmaps,
    // packed-file KVs, and object listings.
    std::unordered_map<std::string, Candidate> candidates_;
    // References observed across independent discovery scans, grouped by small-file path.
    // They only expand candidates and trigger conflict rechecks; consistency is decided from
    // metadata reread in the FileCheckContext transaction.
    std::unordered_map<std::string, std::vector<Reference>> discovered_references_;
};

} // namespace doris::cloud
