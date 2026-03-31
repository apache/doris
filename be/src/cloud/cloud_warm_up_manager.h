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

#include <bthread/countdown_event.h>
#include <bthread/mutex.h>
#include <gen_cpp/BackendService.h>

#include <array>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "util/threadpool.h"

namespace doris {

struct RecycledRowsets;

enum class DownloadType {
    BE,
    S3,
};

// Filter for event-driven warmup jobs.
// nullopt = cluster-level (no table filter, warm up all tables)
// has_value = table-level filter (only warm up tables in the set)
using EventDrivenJobFilter = std::optional<std::unordered_set<int64_t>>;

struct JobReplicaInfo {
    int64_t job_id;
    TReplicaInfo replica;
};

struct JobMeta {
    JobMeta() = default;
    JobMeta(const TJobMeta& meta);
    DownloadType download_type;
    std::string be_ip;
    int32_t brpc_port;
    std::vector<int64_t> tablet_ids;
};

// Represents a single peer candidate for cross compute group peer read
struct PeerCandidate {
    std::string host;
    int32_t brpc_port {0};
    std::string compute_group_id;
    int64_t last_access_time_ms {0}; // ms since epoch, used for expiry
    int32_t consecutive_rpc_failures {0};
};

// Holds all peer candidates for a single tablet
struct TabletPeerCandidates {
    // candidates[0] is the highest priority (warmup-inserted candidates go to front)
    std::vector<PeerCandidate> candidates;
    // last successful compute group used for this tablet
    std::string last_successful_compute_group_id;
    // singleflight guard: true while an async fetch from FE is in progress
    bool fetching_from_fe {false};
    // Cooldown: consecutive all-miss count and cooldown deadline.
    // When all candidates miss N times in a row, temporarily skip peer for this tablet.
    int32_t consecutive_all_miss {0};
    int64_t cooldown_until_ms {0}; // ms since epoch; 0 = not in cooldown
};

// manager for
// table warm up
// cluster warm up
// balance peer addr cache
class CloudWarmUpManager {
public:
    explicit CloudWarmUpManager(CloudStorageEngine& engine);
    ~CloudWarmUpManager();
    // Set the job id if the id is zero
    Status check_and_set_job_id(int64_t job_id);

    // Set the batch id to record download progress
    Status check_and_set_batch_id(int64_t job_id, int64_t batch_id, bool* retry = nullptr);

    // Add the dowload job
    void add_job(const std::vector<TJobMeta>& job_metas);

#ifdef BE_TEST
    void consumer_job();
#endif

    // Get the job state tuple<cur_job_id, cur_batch_id, pending_job_metas_size, _finish_job_size>
    std::tuple<int64_t, int64_t, int64_t, int64_t> get_current_job_state();

    // Cancel the job
    Status clear_job(int64_t job_id);

    Status set_event(int64_t job_id, TWarmUpEventType::type event, bool clear = false,
                     const std::vector<int64_t>* table_ids = nullptr);

    // If `sync_wait_timeout_ms` <= 0, the function will send the warm-up RPC
    // and return immediately without waiting for the warm-up to complete.
    // If `sync_wait_timeout_ms` > 0, the function will wait for the warm-up
    // to finish or until the specified timeout (in milliseconds) is reached.
    //
    // @param rs_meta Metadata of the rowset to be warmed up.
    // @param sync_wait_timeout_ms Timeout in milliseconds to wait for the warm-up
    //                              to complete. Non-positive value means no waiting.
    void warm_up_rowset(RowsetMeta& rs_meta, int64_t table_id, int64_t sync_wait_timeout_ms = -1);

    void recycle_cache(int64_t tablet_id, const std::vector<RecycledRowsets>& rowsets);

    // Balance warm up cache management methods
    // compute_group_id defaults to "" for backward compatibility
    void record_balanced_tablet(int64_t tablet_id, const std::string& host, int32_t brpc_port,
                                const std::string& compute_group_id = "");
    void remove_balanced_tablet(int64_t tablet_id);
    void remove_balanced_tablets(const std::vector<int64_t>& tablet_ids);

    // Cross compute group peer read candidate management
    void fetch_candidates_from_fe(int64_t tablet_id);
    std::vector<PeerCandidate> get_peer_candidates(int64_t tablet_id);
    void update_peer_candidate_on_success(int64_t tablet_id, const std::string& compute_group_id);
    void update_peer_candidate_on_rpc_failure(int64_t tablet_id, const std::string& host,
                                              int32_t brpc_port);
    // Rotate a cache-miss candidate to the end of the list so next read tries a different one.
    void rotate_peer_candidate_on_cache_miss(int64_t tablet_id, const std::string& host,
                                             int32_t brpc_port);
    // Record that all candidates missed for this tablet in a single race.
    // After consecutive_all_miss reaches threshold, sets a cooldown period during which
    // get_peer_candidates returns empty to avoid wasting peer RPCs.
    void record_peer_all_miss(int64_t tablet_id);
    // Check if peer read is in cooldown for this tablet (all candidates repeatedly missed).
    bool is_peer_cooldown(int64_t tablet_id) const;

    // ---- HTTP debug/admin API ----
    // Read-only snapshot of TabletPeerCandidates for a single tablet.
    // Returns nullopt if the tablet has no peer candidates.
    std::optional<TabletPeerCandidates> get_tablet_peer_info(int64_t tablet_id) const;
    // Snapshot of all tablets with peer candidates. If limit > 0, returns at most that many.
    std::vector<std::pair<int64_t, TabletPeerCandidates>> get_all_peer_info(
            int64_t limit = 0) const;
    // Force-set the full TabletPeerCandidates for a tablet (admin override).
    void set_tablet_peer_candidates(int64_t tablet_id, TabletPeerCandidates candidates);

private:
    struct WarmUpRowsetFailure {
        int code;
        std::string reason;
    };

    static Status _build_warm_up_rowset_result(const std::vector<WarmUpRowsetFailure>& failures,
                                               size_t replica_count, int64_t tablet_id,
                                               int64_t table_id, const std::string& rowset_id);

    void schedule_remove_balanced_tablet(int64_t tablet_id);
    static void clean_up_expired_mappings(void* arg);
    void handle_jobs();
    void run_cleanup_loop();

    Status _do_warm_up_rowset(RowsetMeta& rs_meta, int64_t table_id,
                              std::vector<JobReplicaInfo>& replicas, int64_t sync_wait_timeout_ms,
                              bool skip_existence_check);

    std::vector<JobReplicaInfo> get_replica_info(int64_t tablet_id, int64_t table_id,
                                                 bool bypass_cache, bool& cache_hit);

    void _warm_up_rowset(RowsetMeta& rs_meta, int64_t table_id, int64_t sync_wait_timeout_ms);
    void _recycle_cache(int64_t tablet_id, const std::vector<RecycledRowsets>& rowsets);

    void submit_download_tasks(io::Path path, int64_t file_size, io::FileSystemSPtr file_system,
                               int64_t expiration_time,
                               std::shared_ptr<bthread::CountdownEvent> wait, bool is_index = false,
                               std::function<void(Status)> done_cb = nullptr,
                               int64_t tablet_id = -1);
    std::mutex _mtx;
    std::condition_variable _cond;
    int64_t _cur_job_id {0};
    int64_t _cur_batch_id {-1};
    std::deque<std::shared_ptr<JobMeta>> _pending_job_metas;
    std::vector<std::shared_ptr<JobMeta>> _finish_job;
    std::thread _download_thread;
    bool _closed {false};
    // the attribute for compile in ut
    [[maybe_unused]] CloudStorageEngine& _engine;

    // timestamp, info
    using CacheEntry = std::pair<std::chrono::steady_clock::time_point, TReplicaInfo>;
    // tablet_id -> entry
    using Cache = std::unordered_map<int64_t, CacheEntry>;
    // job_id -> cache
    std::unordered_map<int64_t, Cache> _tablet_replica_cache;
    // job_id -> table filter (nullopt = cluster-level, no filter)
    std::unordered_map<int64_t, EventDrivenJobFilter> _event_driven_filters;
    std::unique_ptr<ThreadPool> _thread_pool;
    std::unique_ptr<ThreadPoolToken> _thread_pool_token;

    // Sharded lock for better performance
    // bthread::Mutex is used because peer read path runs in bthread context
    static constexpr size_t SHARD_COUNT = 10240;
    struct Shard {
        mutable bthread::Mutex mtx;
        std::unordered_map<int64_t, TabletPeerCandidates> tablets;
    };
    std::array<Shard, SHARD_COUNT> _balanced_tablets_shards;

    // Helper methods for shard operations
    size_t get_shard_index(int64_t tablet_id) const {
        return std::hash<int64_t> {}(tablet_id) % SHARD_COUNT;
    }
    Shard& get_shard(int64_t tablet_id) {
        return _balanced_tablets_shards[get_shard_index(tablet_id)];
    }

    const Shard& get_shard(int64_t tablet_id) const {
        return _balanced_tablets_shards[get_shard_index(tablet_id)];
    }

    // Cleanup thread and its synchronization primitives
    std::thread _cleanup_thread;
    std::mutex _cleanup_mtx;
    std::condition_variable _cleanup_cond;
};

} // namespace doris
