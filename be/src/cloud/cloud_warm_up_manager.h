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

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "gen_cpp/BackendService.h"

namespace doris {

enum class DownloadType {
    BE,
    S3,
};

struct JobMeta {
    JobMeta() = default;
    JobMeta(const TJobMeta& meta);
    DownloadType download_type;
    std::string be_ip;
    int32_t brpc_port;
    std::vector<int64_t> tablet_ids;
};

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

    Status set_event(int64_t job_id, TWarmUpEventType::type event, bool clear = false);

    // If `sync_wait_timeout_ms` <= 0, the function will send the warm-up RPC
    // and return immediately without waiting for the warm-up to complete.
    // If `sync_wait_timeout_ms` > 0, the function will wait for the warm-up
    // to finish or until the specified timeout (in milliseconds) is reached.
    //
    // @param rs_meta Metadata of the rowset to be warmed up.
    // @param sync_wait_timeout_ms Timeout in milliseconds to wait for the warm-up
    //                              to complete. Non-positive value means no waiting.
    void warm_up_rowset(RowsetMeta& rs_meta, int64_t sync_wait_timeout_ms = -1);

    void recycle_cache(int64_t tablet_id, const std::vector<RecycledRowsets>& rowsets);

private:
    void handle_jobs();

    std::vector<TReplicaInfo> get_replica_info(int64_t tablet_id);

    void submit_download_tasks(io::Path path, int64_t file_size, io::FileSystemSPtr file_system,
                               int64_t expiration_time,
                               std::shared_ptr<bthread::CountdownEvent> wait,
                               bool is_index = false);
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
};

} // namespace doris
