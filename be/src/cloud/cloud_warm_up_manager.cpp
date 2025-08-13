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

#include "cloud/cloud_warm_up_manager.h"

#include <bvar/bvar.h>
#include <bvar/reducer.h>

#include <algorithm>
#include <cstddef>
#include <tuple>

#include "bvar/bvar.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/logging.h"
#include "io/cache/block_file_cache_downloader.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/brpc_client_cache.h" // BrpcClientCache
#include "util/thrift_rpc_helper.h"
#include "util/time.h"

namespace doris {

bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_requested_segment_size(
        "file_cache_event_driven_warm_up_requested_segment_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_requested_segment_num(
        "file_cache_event_driven_warm_up_requested_segment_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_requested_index_size(
        "file_cache_event_driven_warm_up_requested_index_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_requested_index_num(
        "file_cache_event_driven_warm_up_requested_index_num");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_submitted_tablet_num(
        "file_cache_once_or_periodic_warm_up_submitted_tablet_num");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_finished_tablet_num(
        "file_cache_once_or_periodic_warm_up_finished_tablet_num");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_submitted_segment_size(
        "file_cache_once_or_periodic_warm_up_submitted_segment_size");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_submitted_segment_num(
        "file_cache_once_or_periodic_warm_up_submitted_segment_num");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_submitted_index_size(
        "file_cache_once_or_periodic_warm_up_submitted_index_size");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_submitted_index_num(
        "file_cache_once_or_periodic_warm_up_submitted_index_num");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_finished_segment_size(
        "file_cache_once_or_periodic_warm_up_finished_segment_size");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_finished_segment_num(
        "file_cache_once_or_periodic_warm_up_finished_segment_num");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_finished_index_size(
        "file_cache_once_or_periodic_warm_up_finished_index_size");
bvar::Adder<uint64_t> g_file_cache_once_or_periodic_warm_up_finished_index_num(
        "file_cache_once_or_periodic_warm_up_finished_index_num");
bvar::Adder<uint64_t> g_file_cache_recycle_cache_requested_segment_num(
        "file_cache_recycle_cache_requested_segment_num");
bvar::Adder<uint64_t> g_file_cache_recycle_cache_requested_index_num(
        "file_cache_recycle_cache_requested_index_num");
bvar::Status<int64_t> g_file_cache_warm_up_rowset_last_call_unix_ts(
        "file_cache_warm_up_rowset_last_call_unix_ts", 0);
bvar::Adder<uint64_t> file_cache_warm_up_failed_task_num("file_cache_warm_up", "failed_task_num");

bvar::LatencyRecorder g_file_cache_warm_up_rowset_wait_for_compaction_latency(
        "file_cache_warm_up_rowset_wait_for_compaction_latency");

CloudWarmUpManager::CloudWarmUpManager(CloudStorageEngine& engine) : _engine(engine) {
    _download_thread = std::thread(&CloudWarmUpManager::handle_jobs, this);
}

CloudWarmUpManager::~CloudWarmUpManager() {
    {
        std::lock_guard lock(_mtx);
        _closed = true;
    }
    _cond.notify_all();
    if (_download_thread.joinable()) {
        _download_thread.join();
    }
}

std::unordered_map<std::string, RowsetMetaSharedPtr> snapshot_rs_metas(BaseTablet* tablet) {
    std::unordered_map<std::string, RowsetMetaSharedPtr> id_to_rowset_meta_map;
    auto visitor = [&id_to_rowset_meta_map](const RowsetSharedPtr& r) {
        id_to_rowset_meta_map.emplace(r->rowset_meta()->rowset_id().to_string(), r->rowset_meta());
    };
    constexpr bool include_stale = false;
    tablet->traverse_rowsets(visitor, include_stale);
    return id_to_rowset_meta_map;
}

void CloudWarmUpManager::submit_download_tasks(io::Path path, int64_t file_size,
                                               io::FileSystemSPtr file_system,
                                               int64_t expiration_time,
                                               std::shared_ptr<bthread::CountdownEvent> wait,
                                               bool is_index) {
    if (file_size < 0) {
        auto st = file_system->file_size(path, &file_size);
        if (!st.ok()) [[unlikely]] {
            LOG(WARNING) << "get file size failed: " << path;
            file_cache_warm_up_failed_task_num << 1;
            return;
        }
    }
    if (is_index) {
        g_file_cache_once_or_periodic_warm_up_submitted_index_num << 1;
        g_file_cache_once_or_periodic_warm_up_submitted_index_size << file_size;
    } else {
        g_file_cache_once_or_periodic_warm_up_submitted_segment_num << 1;
        g_file_cache_once_or_periodic_warm_up_submitted_segment_size << file_size;
    }

    const int64_t chunk_size = 10 * 1024 * 1024; // 10MB
    int64_t offset = 0;
    int64_t remaining_size = file_size;

    while (remaining_size > 0) {
        int64_t current_chunk_size = std::min(chunk_size, remaining_size);
        wait->add_count();

        _engine.file_cache_block_downloader().submit_download_task(io::DownloadFileMeta {
                .path = path,
                .file_size = file_size,
                .offset = offset,
                .download_size = current_chunk_size,
                .file_system = file_system,
                .ctx =
                        {
                                .expiration_time = expiration_time,
                                .is_dryrun = config::enable_reader_dryrun_when_download_file_cache,
                        },
                .download_done =
                        [=](Status st) {
                            if (!st) {
                                LOG_WARNING("Warm up error ").error(st);
                            } else if (is_index) {
                                g_file_cache_once_or_periodic_warm_up_finished_index_num
                                        << (offset == 0 ? 1 : 0);
                                g_file_cache_once_or_periodic_warm_up_finished_index_size
                                        << current_chunk_size;
                            } else {
                                g_file_cache_once_or_periodic_warm_up_finished_segment_num
                                        << (offset == 0 ? 1 : 0);
                                g_file_cache_once_or_periodic_warm_up_finished_segment_size
                                        << current_chunk_size;
                            }
                            wait->signal();
                        },
        });

        offset += current_chunk_size;
        remaining_size -= current_chunk_size;
    }
}

void CloudWarmUpManager::handle_jobs() {
#ifndef BE_TEST
    constexpr int WAIT_TIME_SECONDS = 600;
    while (true) {
        std::shared_ptr<JobMeta> cur_job = nullptr;
        {
            std::unique_lock lock(_mtx);
            while (!_closed && _pending_job_metas.empty()) {
                _cond.wait(lock);
            }
            if (_closed) break;
            if (!_pending_job_metas.empty()) {
                cur_job = _pending_job_metas.front();
            }
        }

        if (!cur_job) {
            LOG_WARNING("Warm up job is null");
            continue;
        }

        std::shared_ptr<bthread::CountdownEvent> wait =
                std::make_shared<bthread::CountdownEvent>(0);

        for (int64_t tablet_id : cur_job->tablet_ids) {
            if (_cur_job_id == 0) { // The job is canceled
                break;
            }
            auto res = _engine.tablet_mgr().get_tablet(tablet_id);
            if (!res.has_value()) {
                LOG_WARNING("Warm up error ").tag("tablet_id", tablet_id).error(res.error());
                continue;
            }
            auto tablet = res.value();
            auto st = tablet->sync_rowsets();
            if (!st) {
                LOG_WARNING("Warm up error ").tag("tablet_id", tablet_id).error(st);
                continue;
            }

            auto tablet_meta = tablet->tablet_meta();
            auto rs_metas = snapshot_rs_metas(tablet.get());
            for (auto& [_, rs] : rs_metas) {
                for (int64_t seg_id = 0; seg_id < rs->num_segments(); seg_id++) {
                    auto storage_resource = rs->remote_storage_resource();
                    if (!storage_resource) {
                        LOG(WARNING) << storage_resource.error();
                        continue;
                    }

                    int64_t expiration_time =
                            tablet_meta->ttl_seconds() == 0 || rs->newest_write_timestamp() <= 0
                                    ? 0
                                    : rs->newest_write_timestamp() + tablet_meta->ttl_seconds();
                    if (expiration_time <= UnixSeconds()) {
                        expiration_time = 0;
                    }

                    // 1st. download segment files
                    submit_download_tasks(
                            storage_resource.value()->remote_segment_path(*rs, seg_id),
                            rs->segment_file_size(seg_id), storage_resource.value()->fs,
                            expiration_time, wait);

                    // 2nd. download inverted index files
                    int64_t file_size = -1;
                    auto schema_ptr = rs->tablet_schema();
                    auto idx_version = schema_ptr->get_inverted_index_storage_format();
                    const auto& idx_file_info = rs->inverted_index_file_info(seg_id);
                    if (idx_version == InvertedIndexStorageFormatPB::V1) {
                        auto&& inverted_index_info = rs->inverted_index_file_info(seg_id);
                        std::unordered_map<int64_t, int64_t> index_size_map;
                        for (const auto& info : inverted_index_info.index_info()) {
                            if (info.index_file_size() != -1) {
                                index_size_map[info.index_id()] = info.index_file_size();
                            } else {
                                VLOG_DEBUG << "Invalid index_file_size for segment_id " << seg_id
                                           << ", index_id " << info.index_id();
                            }
                        }
                        for (const auto& index : schema_ptr->inverted_indexes()) {
                            auto idx_path = storage_resource.value()->remote_idx_v1_path(
                                    *rs, seg_id, index->index_id(), index->get_index_suffix());
                            if (idx_file_info.index_info_size() > 0) {
                                for (const auto& idx_info : idx_file_info.index_info()) {
                                    if (index->index_id() == idx_info.index_id() &&
                                        index->get_index_suffix() == idx_info.index_suffix()) {
                                        file_size = idx_info.index_file_size();
                                        break;
                                    }
                                }
                            }
                            submit_download_tasks(idx_path, file_size, storage_resource.value()->fs,
                                                  expiration_time, wait, true);
                        }
                    } else {
                        if (schema_ptr->has_inverted_index()) {
                            auto idx_path =
                                    storage_resource.value()->remote_idx_v2_path(*rs, seg_id);
                            file_size = idx_file_info.has_index_size() ? idx_file_info.index_size()
                                                                       : -1;
                            submit_download_tasks(idx_path, file_size, storage_resource.value()->fs,
                                                  expiration_time, wait, true);
                        }
                    }
                }
            }
            g_file_cache_once_or_periodic_warm_up_finished_tablet_num << 1;
        }

        timespec time;
        time.tv_sec = UnixSeconds() + WAIT_TIME_SECONDS;
        if (wait->timed_wait(time)) {
            LOG_WARNING("Warm up {} tablets take a long time", cur_job->tablet_ids.size());
        }
        {
            std::unique_lock lock(_mtx);
            _finish_job.push_back(cur_job);
            // _pending_job_metas may be cleared by a CLEAR_JOB request
            // so we need to check it again.
            if (!_pending_job_metas.empty()) {
                // We can not call pop_front before the job is finished,
                // because GET_CURRENT_JOB_STATE_AND_LEASE is relying on the pending job size.
                _pending_job_metas.pop_front();
            }
        }
    }
#endif
}

JobMeta::JobMeta(const TJobMeta& meta)
        : be_ip(meta.be_ip), brpc_port(meta.brpc_port), tablet_ids(meta.tablet_ids) {
    switch (meta.download_type) {
    case TDownloadType::BE:
        download_type = DownloadType::BE;
        break;
    case TDownloadType::S3:
        download_type = DownloadType::S3;
        break;
    }
}

Status CloudWarmUpManager::check_and_set_job_id(int64_t job_id) {
    std::lock_guard lock(_mtx);
    if (_cur_job_id == 0) {
        _cur_job_id = job_id;
    }
    Status st = Status::OK();
    if (_cur_job_id != job_id) {
        st = Status::InternalError("The job {} is running", _cur_job_id);
    }
    return st;
}

Status CloudWarmUpManager::check_and_set_batch_id(int64_t job_id, int64_t batch_id, bool* retry) {
    std::lock_guard lock(_mtx);
    Status st = Status::OK();
    if (_cur_job_id != 0 && _cur_job_id != job_id) {
        st = Status::InternalError("The job {} is not current job, current job is {}", job_id,
                                   _cur_job_id);
        return st;
    }
    if (_cur_job_id == 0) {
        _cur_job_id = job_id;
    }
    if (_cur_batch_id == batch_id) {
        *retry = true;
        return st;
    }
    if (_pending_job_metas.empty()) {
        _cur_batch_id = batch_id;
    } else {
        st = Status::InternalError("The batch {} is not finish", _cur_batch_id);
    }
    return st;
}

void CloudWarmUpManager::add_job(const std::vector<TJobMeta>& job_metas) {
    {
        std::lock_guard lock(_mtx);
        std::for_each(job_metas.begin(), job_metas.end(), [this](const TJobMeta& meta) {
            _pending_job_metas.emplace_back(std::make_shared<JobMeta>(meta));
            g_file_cache_once_or_periodic_warm_up_submitted_tablet_num << meta.tablet_ids.size();
        });
    }
    _cond.notify_all();
}

#ifdef BE_TEST
void CloudWarmUpManager::consumer_job() {
    {
        std::unique_lock lock(_mtx);
        _finish_job.push_back(_pending_job_metas.front());
        _pending_job_metas.pop_front();
    }
}

#endif

std::tuple<int64_t, int64_t, int64_t, int64_t> CloudWarmUpManager::get_current_job_state() {
    std::lock_guard lock(_mtx);
    return std::make_tuple(_cur_job_id, _cur_batch_id, _pending_job_metas.size(),
                           _finish_job.size());
}

Status CloudWarmUpManager::clear_job(int64_t job_id) {
    std::lock_guard lock(_mtx);
    Status st = Status::OK();
    if (job_id == _cur_job_id) {
        _cur_job_id = 0;
        _cur_batch_id = -1;
        _pending_job_metas.clear();
        _finish_job.clear();
    } else {
        st = Status::InternalError("The job {} is not current job, current job is {}", job_id,
                                   _cur_job_id);
    }
    return st;
}

Status CloudWarmUpManager::set_event(int64_t job_id, TWarmUpEventType::type event, bool clear) {
    DBUG_EXECUTE_IF("CloudWarmUpManager.set_event.ignore_all", {
        LOG(INFO) << "Ignore set_event request, job_id=" << job_id << ", event=" << event
                  << ", clear=" << clear;
        return Status::OK();
    });
    std::lock_guard lock(_mtx);
    Status st = Status::OK();
    if (event == TWarmUpEventType::type::LOAD) {
        if (clear) {
            _tablet_replica_cache.erase(job_id);
            LOG(INFO) << "Clear event driven sync, job_id=" << job_id << ", event=" << event;
        } else if (!_tablet_replica_cache.contains(job_id)) {
            static_cast<void>(_tablet_replica_cache[job_id]);
            LOG(INFO) << "Set event driven sync, job_id=" << job_id << ", event=" << event;
        }
    } else {
        st = Status::InternalError("The event {} is not supported yet", event);
    }
    return st;
}

std::vector<TReplicaInfo> CloudWarmUpManager::get_replica_info(int64_t tablet_id) {
    std::vector<TReplicaInfo> replicas;
    std::vector<int64_t> cancelled_jobs;
    std::lock_guard<std::mutex> lock(_mtx);
    for (auto& [job_id, cache] : _tablet_replica_cache) {
        auto it = cache.find(tablet_id);
        if (it != cache.end()) {
            // check ttl expire
            auto now = std::chrono::steady_clock::now();
            auto sec = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.first);
            if (sec.count() < config::warmup_tablet_replica_info_cache_ttl_sec) {
                replicas.push_back(it->second.second);
                LOG(INFO) << "get_replica_info: cache hit, tablet_id=" << tablet_id
                          << ", job_id=" << job_id;
                continue;
            } else {
                LOG(INFO) << "get_replica_info: cache expired, tablet_id=" << tablet_id
                          << ", job_id=" << job_id;
                cache.erase(it);
            }
        }
        LOG(INFO) << "get_replica_info: cache miss, tablet_id=" << tablet_id
                  << ", job_id=" << job_id;

        ClusterInfo* cluster_info = ExecEnv::GetInstance()->cluster_info();
        if (cluster_info == nullptr) {
            LOG(WARNING) << "get_replica_info: have not get FE Master heartbeat yet, job_id="
                         << job_id;
            continue;
        }
        TNetworkAddress master_addr = cluster_info->master_fe_addr;
        if (master_addr.hostname == "" || master_addr.port == 0) {
            LOG(WARNING) << "get_replica_info: have not get FE Master heartbeat yet, job_id="
                         << job_id;
            continue;
        }

        TGetTabletReplicaInfosRequest request;
        TGetTabletReplicaInfosResult result;
        request.warm_up_job_id = job_id;
        request.__isset.warm_up_job_id = true;
        request.tablet_ids.emplace_back(tablet_id);
        Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->getTabletReplicaInfos(result, request);
                });

        if (!rpc_st.ok()) {
            LOG(WARNING) << "get_replica_info: rpc failed error=" << rpc_st
                         << ", tablet id=" << tablet_id << ", job_id=" << job_id;
            continue;
        }

        auto st = Status::create(result.status);
        if (!st.ok()) {
            if (st.is<CANCELED>()) {
                LOG(INFO) << "get_replica_info: warm up job cancelled, tablet_id=" << tablet_id
                          << ", job_id=" << job_id;
                cancelled_jobs.push_back(job_id);
            } else {
                LOG(WARNING) << "get_replica_info: failed status=" << st
                             << ", tablet id=" << tablet_id << ", job_id=" << job_id;
            }
            continue;
        }
        VLOG_DEBUG << "get_replica_info: got " << result.tablet_replica_infos.size()
                   << " tablets, tablet id=" << tablet_id << ", job_id=" << job_id;

        for (const auto& it : result.tablet_replica_infos) {
            auto tablet_id = it.first;
            VLOG_DEBUG << "get_replica_info: got " << it.second.size()
                       << " replica_infos, tablet id=" << tablet_id << ", job_id=" << job_id;
            for (const auto& replica : it.second) {
                cache[tablet_id] = std::make_pair(std::chrono::steady_clock::now(), replica);
                replicas.push_back(replica);
                LOG(INFO) << "get_replica_info: cache add, tablet_id=" << tablet_id
                          << ", job_id=" << job_id;
            }
        }
    }
    for (auto job_id : cancelled_jobs) {
        LOG(INFO) << "get_replica_info: erasing cancelled job, job_id=" << job_id;
        _tablet_replica_cache.erase(job_id);
    }
    VLOG_DEBUG << "get_replica_info: return " << replicas.size()
               << " replicas, tablet id=" << tablet_id;
    return replicas;
}

void CloudWarmUpManager::warm_up_rowset(RowsetMeta& rs_meta, int64_t sync_wait_timeout_ms) {
    auto tablet_id = rs_meta.tablet_id();
    auto replicas = get_replica_info(tablet_id);
    if (replicas.empty()) {
        LOG(INFO) << "There is no need to warmup tablet=" << rs_meta.tablet_id()
                  << ", skipping rowset=" << rs_meta.rowset_id().to_string();
        return;
    }
    int64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    g_file_cache_warm_up_rowset_last_call_unix_ts.set_value(now_ts);

    PWarmUpRowsetRequest request;
    request.add_rowset_metas()->CopyFrom(rs_meta.get_rowset_pb());
    request.set_unix_ts_us(now_ts);
    request.set_sync_wait_timeout_ms(sync_wait_timeout_ms);
    for (auto& replica : replicas) {
        // send sync request
        std::string host = replica.host;
        auto dns_cache = ExecEnv::GetInstance()->dns_cache();
        if (dns_cache == nullptr) {
            LOG(WARNING) << "DNS cache is not initialized, skipping hostname resolve";
        } else if (!is_valid_ip(replica.host)) {
            Status status = dns_cache->get(replica.host, &host);
            if (!status.ok()) {
                LOG(WARNING) << "failed to get ip from host " << replica.host << ": "
                             << status.to_string();
                return;
            }
        }
        std::string brpc_addr = get_host_port(host, replica.brpc_port);
        Status st = Status::OK();
        std::shared_ptr<PBackendService_Stub> brpc_stub =
                ExecEnv::GetInstance()->brpc_internal_client_cache()->get_new_client_no_cache(
                        brpc_addr);
        if (!brpc_stub) {
            st = Status::RpcError("Address {} is wrong", brpc_addr);
            continue;
        }

        // update metrics
        auto schema_ptr = rs_meta.tablet_schema();
        bool has_inverted_index = schema_ptr->has_inverted_index();
        auto idx_version = schema_ptr->get_inverted_index_storage_format();
        for (int64_t segment_id = 0; segment_id < rs_meta.num_segments(); segment_id++) {
            g_file_cache_event_driven_warm_up_requested_segment_num << 1;
            g_file_cache_event_driven_warm_up_requested_segment_size
                    << rs_meta.segment_file_size(segment_id);

            if (has_inverted_index) {
                if (idx_version == InvertedIndexStorageFormatPB::V1) {
                    auto&& inverted_index_info = rs_meta.inverted_index_file_info(segment_id);
                    if (inverted_index_info.index_info().empty()) {
                        VLOG_DEBUG << "No index info available for segment " << segment_id;
                        continue;
                    }
                    for (const auto& info : inverted_index_info.index_info()) {
                        g_file_cache_event_driven_warm_up_requested_index_num << 1;
                        if (info.index_file_size() != -1) {
                            g_file_cache_event_driven_warm_up_requested_index_size
                                    << info.index_file_size();
                        } else {
                            VLOG_DEBUG << "Invalid index_file_size for segment_id " << segment_id
                                       << ", index_id " << info.index_id();
                        }
                    }
                } else { // InvertedIndexStorageFormatPB::V2
                    auto&& inverted_index_info = rs_meta.inverted_index_file_info(segment_id);
                    g_file_cache_event_driven_warm_up_requested_index_num << 1;
                    if (inverted_index_info.has_index_size()) {
                        g_file_cache_event_driven_warm_up_requested_index_size
                                << inverted_index_info.index_size();
                    } else {
                        VLOG_DEBUG << "index_size is not set for segment " << segment_id;
                    }
                }
            }
        }

        brpc::Controller cntl;
        if (sync_wait_timeout_ms > 0) {
            cntl.set_timeout_ms(sync_wait_timeout_ms + 1000);
        }
        PWarmUpRowsetResponse response;
        MonotonicStopWatch watch;
        watch.start();
        brpc_stub->warm_up_rowset(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG_WARNING("warm up rowset {} for tablet {} failed, rpc error: {}",
                        rs_meta.rowset_id().to_string(), tablet_id, cntl.ErrorText());
            return;
        }
        if (sync_wait_timeout_ms > 0) {
            auto cost_us = watch.elapsed_time_microseconds();
            VLOG_DEBUG << "warm up rowset wait for compaction: " << cost_us << " us";
            if (cost_us / 1000 > sync_wait_timeout_ms) {
                LOG_WARNING(
                        "Warm up rowset {} for tabelt {} wait for compaction timeout, takes {} ms",
                        rs_meta.rowset_id().to_string(), tablet_id, cost_us / 1000);
            }
            g_file_cache_warm_up_rowset_wait_for_compaction_latency << cost_us;
        }
    }
}

void CloudWarmUpManager::recycle_cache(int64_t tablet_id,
                                       const std::vector<RecycledRowsets>& rowsets) {
    LOG(INFO) << "recycle_cache: tablet_id=" << tablet_id << ", num_rowsets=" << rowsets.size();
    auto replicas = get_replica_info(tablet_id);
    if (replicas.empty()) {
        return;
    }

    PRecycleCacheRequest request;
    for (const auto& rowset : rowsets) {
        RecycleCacheMeta* meta = request.add_cache_metas();
        meta->set_tablet_id(tablet_id);
        meta->set_rowset_id(rowset.rowset_id.to_string());
        meta->set_num_segments(rowset.num_segments);
        for (const auto& name : rowset.index_file_names) {
            meta->add_index_file_names(name);
        }
        g_file_cache_recycle_cache_requested_segment_num << rowset.num_segments;
        g_file_cache_recycle_cache_requested_index_num << rowset.index_file_names.size();
    }
    auto dns_cache = ExecEnv::GetInstance()->dns_cache();
    for (auto& replica : replicas) {
        // send sync request
        std::string host = replica.host;
        if (dns_cache == nullptr) {
            LOG(WARNING) << "DNS cache is not initialized, skipping hostname resolve";
        } else if (!is_valid_ip(replica.host)) {
            Status status = dns_cache->get(replica.host, &host);
            if (!status.ok()) {
                LOG(WARNING) << "failed to get ip from host " << replica.host << ": "
                             << status.to_string();
                return;
            }
        }
        std::string brpc_addr = get_host_port(host, replica.brpc_port);
        Status st = Status::OK();
        std::shared_ptr<PBackendService_Stub> brpc_stub =
                ExecEnv::GetInstance()->brpc_internal_client_cache()->get_new_client_no_cache(
                        brpc_addr);
        if (!brpc_stub) {
            st = Status::RpcError("Address {} is wrong", brpc_addr);
            continue;
        }
        brpc::Controller cntl;
        PRecycleCacheResponse response;
        brpc_stub->recycle_cache(&cntl, &request, &response, nullptr);
    }
}

} // namespace doris
