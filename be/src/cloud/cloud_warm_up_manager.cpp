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

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <bthread/unstable.h>
#include <butil/time.h>
#include <bvar/bvar.h>
#include <bvar/reducer.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <list>
#include <string>
#include <tuple>
#include <vector>

#include "bthread/mutex.h"
#include "bvar/bvar.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/check.h"
#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_downloader.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/tablet/tablet.h"
#include "util/brpc_client_cache.h" // BrpcClientCache
#include "util/bvar_windowed_adder.h"
#include "util/client_cache.h"
#include "util/defer_op.h"
#include "util/stack_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"

namespace doris {

// Peer candidate management statistics
bvar::Adder<uint64_t> g_peer_candidate_cache_hit("peer_candidate_cache_hit");
bvar::Adder<uint64_t> g_peer_candidate_cache_miss("peer_candidate_cache_miss");
bvar::Adder<uint64_t> g_peer_lazy_fetch_total("peer_lazy_fetch_total");
bvar::Adder<uint64_t> g_peer_lazy_fetch_success("peer_lazy_fetch_success");
bvar::Adder<uint64_t> g_peer_lazy_fetch_failed("peer_lazy_fetch_failed");
bvar::LatencyRecorder g_peer_lazy_fetch_latency("peer_lazy_fetch_latency");
bvar::Adder<uint64_t> g_peer_rpc_failure_eviction("peer_rpc_failure_eviction");
bvar::Adder<uint64_t> g_peer_candidate_expiry_eviction("peer_candidate_expiry_eviction");
bvar::Adder<uint64_t> g_peer_candidate_rotate("peer_candidate_rotate");
bvar::Adder<uint64_t> g_peer_tablet_cooldown_entered("peer_tablet_cooldown_entered");
bvar::Adder<uint64_t> g_peer_tablet_cooldown_skipped("peer_tablet_cooldown_skipped");

bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_skipped_rowset_num(
        "file_cache_event_driven_warm_up_skipped_rowset_num");
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
bvar::Adder<int64_t> g_balance_tablet_be_mapping_size("balance_tablet_be_mapping_size");

bvar::LatencyRecorder g_file_cache_warm_up_rowset_wait_for_compaction_latency(
        "file_cache_warm_up_rowset_wait_for_compaction_latency");

// Per-job windowed metrics for source BE
// bvar::Window enforces MAX_SECONDS_LIMIT = 3600, so the longest window is 1h.
static constexpr int WINDOW_5M = 300;
static constexpr int WINDOW_30M = 1800;
static constexpr int WINDOW_1H = 3600;

MBvarWindowedAdder g_warmup_ed_requested_segment_num("warmup_ed_requested_segment_num", {"job_id"},
                                                     {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_requested_segment_size("warmup_ed_requested_segment_size",
                                                      {"job_id"},
                                                      {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_requested_index_num("warmup_ed_requested_index_num", {"job_id"},
                                                   {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
MBvarWindowedAdder g_warmup_ed_requested_index_size("warmup_ed_requested_index_size", {"job_id"},
                                                    {WINDOW_5M, WINDOW_30M, WINDOW_1H}, false);
bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_trigger_ts({"job_id"});

CloudWarmUpManager::CloudWarmUpManager(CloudStorageEngine& engine) : _engine(engine) {
    auto st = ThreadPoolBuilder("CloudWarmUpManagerThreadPool")
                      .set_min_threads(config::warm_up_manager_thread_pool_size)
                      .set_max_threads(config::warm_up_manager_thread_pool_size)
                      .build(&_thread_pool);
    DORIS_CHECK(st.ok()) << st;
    _thread_pool_token = _thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    DORIS_CHECK(_thread_pool_token != nullptr);
    _download_thread = std::thread(&CloudWarmUpManager::handle_jobs, this);
    _cleanup_thread = std::thread(&CloudWarmUpManager::run_cleanup_loop, this);
}

CloudWarmUpManager::~CloudWarmUpManager() {
    {
        // Set _closed under both mutexes so that both threads' wait predicates see it.
        std::lock_guard lock(_mtx);
        std::lock_guard<std::mutex> cleanup_lock(_cleanup_mtx);
        _closed = true;
    }
    _cond.notify_all();
    _cleanup_cond.notify_all();
    if (_download_thread.joinable()) {
        _download_thread.join();
    }
    if (_cleanup_thread.joinable()) {
        _cleanup_thread.join();
    }

    _thread_pool_token->shutdown();
    _thread_pool_token.reset();
    _thread_pool->shutdown();
    _thread_pool.reset();

    for (auto& shard : _balanced_tablets_shards) {
        std::unique_lock<bthread::Mutex> lock(shard.mtx);
        shard.tablets.clear();
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
                                               bool is_index, std::function<void(Status)> done_cb,
                                               int64_t tablet_id) {
    VLOG_DEBUG << "submit warm up task for file: " << path << ", file_size: " << file_size
               << ", expiration_time: " << expiration_time
               << ", is_index: " << (is_index ? "true" : "false");
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
                .ctx = {.expiration_time = expiration_time,
                        .is_dryrun = config::enable_reader_dryrun_when_download_file_cache,
                        .is_warmup = true,
                        .table_name = "",
                        .partition_name = ""},
                .download_done =
                        [=, done_cb = std::move(done_cb)](Status st) {
                            if (done_cb) done_cb(st);
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
                .tablet_id = tablet_id,
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
            VLOG_DEBUG << "Warm up tablet " << tablet_id << " stack: " << get_stack_trace();
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
                auto storage_resource = rs->remote_storage_resource();
                if (!storage_resource) {
                    LOG(WARNING) << storage_resource.error();
                    continue;
                }

                int64_t expiration_time = tablet_meta->ttl_seconds();
                if (!tablet->add_rowset_warmup_state(*rs, WarmUpTriggerSource::JOB)) {
                    LOG(INFO) << "found duplicate warmup task for rowset " << rs->rowset_id()
                              << ", skip it";
                    continue;
                }
                for (int64_t seg_id = 0; seg_id < rs->num_segments(); seg_id++) {
                    // 1st. download segment files
                    // Use rs->fs() instead of storage_resource.value()->fs to support packed
                    // files. PackedFileSystem wrapper in RowsetMeta::fs() handles the index_map
                    // lookup and reads from the correct packed file.
                    if (!config::file_cache_enable_only_warm_up_idx) {
                        submit_download_tasks(
                                storage_resource.value()->remote_segment_path(*rs, seg_id),
                                rs->segment_file_size(cast_set<int>(seg_id)), rs->fs(),
                                expiration_time, wait, false,
                                [tablet, rs, seg_id](Status st) {
                                    VLOG_DEBUG << "warmup rowset " << rs->version() << " segment "
                                               << seg_id << " completed";
                                    if (tablet->complete_rowset_segment_warmup(
                                                      WarmUpTriggerSource::JOB, rs->rowset_id(), st,
                                                      1, 0)
                                                .trigger_source == WarmUpTriggerSource::JOB) {
                                        VLOG_DEBUG << "warmup rowset " << rs->version()
                                                   << " completed";
                                    }
                                },
                                tablet_id);
                    }

                    // 2nd. download inverted index files
                    int64_t file_size = -1;
                    auto schema_ptr = rs->tablet_schema();
                    auto idx_version = schema_ptr->get_inverted_index_storage_format();
                    const auto& idx_file_info = rs->inverted_index_file_info(cast_set<int>(seg_id));
                    if (idx_version == InvertedIndexStorageFormatPB::V1) {
                        auto&& inverted_index_info =
                                rs->inverted_index_file_info(cast_set<int>(seg_id));
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
                            tablet->update_rowset_warmup_state_inverted_idx_num(
                                    WarmUpTriggerSource::JOB, rs->rowset_id(), 1);
                            submit_download_tasks(
                                    idx_path, file_size, rs->fs(), expiration_time, wait, true,
                                    [=](Status st) {
                                        VLOG_DEBUG << "warmup rowset " << rs->version()
                                                   << " segment " << seg_id
                                                   << "inverted idx:" << idx_path << " completed";
                                        if (tablet->complete_rowset_segment_warmup(
                                                          WarmUpTriggerSource::JOB, rs->rowset_id(),
                                                          st, 0, 1)
                                                    .trigger_source == WarmUpTriggerSource::JOB) {
                                            VLOG_DEBUG << "warmup rowset " << rs->version()
                                                       << " completed";
                                        }
                                    },
                                    tablet_id);
                        }
                    } else {
                        if (schema_ptr->has_inverted_index() || schema_ptr->has_ann_index()) {
                            auto idx_path =
                                    storage_resource.value()->remote_idx_v2_path(*rs, seg_id);
                            file_size = idx_file_info.has_index_size() ? idx_file_info.index_size()
                                                                       : -1;
                            tablet->update_rowset_warmup_state_inverted_idx_num(
                                    WarmUpTriggerSource::JOB, rs->rowset_id(), 1);
                            submit_download_tasks(
                                    idx_path, file_size, rs->fs(), expiration_time, wait, true,
                                    [=](Status st) {
                                        VLOG_DEBUG << "warmup rowset " << rs->version()
                                                   << " segment " << seg_id
                                                   << "inverted idx:" << idx_path << " completed";
                                        if (tablet->complete_rowset_segment_warmup(
                                                          WarmUpTriggerSource::JOB, rs->rowset_id(),
                                                          st, 0, 1)
                                                    .trigger_source == WarmUpTriggerSource::JOB) {
                                            VLOG_DEBUG << "warmup rowset " << rs->version()
                                                       << " completed";
                                        }
                                    },
                                    tablet_id);
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

Status CloudWarmUpManager::set_event(int64_t job_id, TWarmUpEventType::type event, bool clear,
                                     const std::vector<int64_t>* table_ids) {
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
            _event_driven_filters.erase(job_id);
            LOG(INFO) << "Clear event driven sync, job_id=" << job_id << ", event=" << event;
        } else if (!_tablet_replica_cache.contains(job_id)) {
            static_cast<void>(_tablet_replica_cache[job_id]);
            if (table_ids != nullptr) {
                // table-level filter: set to the given table_id set (may be empty,
                // meaning all matched tables were deleted — warm up nothing)
                _event_driven_filters[job_id] =
                        std::unordered_set<int64_t>(table_ids->begin(), table_ids->end());
                LOG(INFO) << "Set event driven sync with table filter, job_id=" << job_id
                          << ", event=" << event << ", table_ids_size=" << table_ids->size();
            } else {
                // cluster-level: no filter, warm up all tables
                _event_driven_filters[job_id] = std::nullopt;
                LOG(INFO) << "Set event driven sync, job_id=" << job_id << ", event=" << event;
            }
        } else if (table_ids != nullptr) {
            // Update table_ids for an existing job (may be empty)
            _event_driven_filters[job_id] =
                    std::unordered_set<int64_t>(table_ids->begin(), table_ids->end());
            LOG(INFO) << "Updated table filter for event driven sync, job_id=" << job_id
                      << ", table_ids_size=" << table_ids->size();
        }
    } else {
        st = Status::InternalError("The event {} is not supported yet", event);
    }
    return st;
}

std::vector<JobReplicaInfo> CloudWarmUpManager::get_replica_info(int64_t tablet_id,
                                                                 int64_t table_id,
                                                                 bool bypass_cache,
                                                                 bool& cache_hit) {
    std::vector<JobReplicaInfo> replicas;
    std::vector<int64_t> cancelled_jobs;
    std::lock_guard<std::mutex> lock(_mtx);
    cache_hit = false;
    for (auto& [job_id, cache] : _tablet_replica_cache) {
        // Check table-level filter: skip this job if table_id doesn't match
        // table_id == 0 means the caller doesn't have table context (e.g., recycle_cache),
        // so skip filtering
        if (table_id != 0) {
            auto filter_it = _event_driven_filters.find(job_id);
            if (filter_it != _event_driven_filters.end() && filter_it->second.has_value()) {
                if (filter_it->second->find(table_id) == filter_it->second->end()) {
                    VLOG_DEBUG << "get_replica_info: table_id=" << table_id
                               << " not in filter for job_id=" << job_id << ", skipping";
                    continue;
                }
            }
        }

        if (!bypass_cache) {
            auto it = cache.find(tablet_id);
            if (it != cache.end()) {
                // check ttl expire
                auto now = std::chrono::steady_clock::now();
                auto sec = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.first);
                if (sec.count() < config::warmup_tablet_replica_info_cache_ttl_sec) {
                    replicas.push_back(JobReplicaInfo {job_id, it->second.second});
                    VLOG_DEBUG << "get_replica_info: cache hit, tablet_id=" << tablet_id
                               << ", job_id=" << job_id;
                    cache_hit = true;
                    continue;
                } else {
                    VLOG_DEBUG << "get_replica_info: cache expired, tablet_id=" << tablet_id
                               << ", job_id=" << job_id;
                    cache.erase(it);
                }
            }
            VLOG_DEBUG << "get_replica_info: cache miss, tablet_id=" << tablet_id
                       << ", job_id=" << job_id;
        }

        if (!cache_hit) {
            // We are trying to save one retry by refresh all the remaining caches
            bypass_cache = true;
        }
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

        auto st = Status::create<false>(result.status);
        if (!st.ok()) {
            if (st.is<ErrorCode::CANCELLED>()) {
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
            auto tid = it.first;
            VLOG_DEBUG << "get_replica_info: got " << it.second.size()
                       << " replica_infos, tablet id=" << tid << ", job_id=" << job_id;
            for (const auto& replica : it.second) {
                cache[tid] = std::make_pair(std::chrono::steady_clock::now(), replica);
                replicas.push_back(JobReplicaInfo {job_id, replica});
                LOG(INFO) << "get_replica_info: cache add, tablet_id=" << tid
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

void CloudWarmUpManager::warm_up_rowset(RowsetMeta& rs_meta, int64_t table_id,
                                        int64_t sync_wait_timeout_ms) {
    if (sync_wait_timeout_ms <= 0) {
        auto rs_meta_pb = std::make_shared<RowsetMetaPB>(rs_meta.get_rowset_pb());
        auto st = _thread_pool_token->submit_func([this, rs_meta_pb, table_id,
                                                   sync_wait_timeout_ms]() {
            RowsetMeta async_rs_meta;
            bool init_succeed = async_rs_meta.init_from_pb(*rs_meta_pb);
            TEST_SYNC_POINT_CALLBACK("CloudWarmUpManager::warm_up_rowset.async_init_from_pb",
                                     &init_succeed);
            if (!init_succeed) {
                LOG(WARNING) << "Failed to init rowset meta when warming up rowset asynchronously";
                return;
            }
            _warm_up_rowset(async_rs_meta, table_id, sync_wait_timeout_ms);
        });
        if (!st.ok()) {
            LOG(WARNING) << "Failed to submit warm up rowset task: " << st;
            file_cache_warm_up_failed_task_num << 1;
        }
        return;
    }

    bthread::Mutex mu;
    bthread::ConditionVariable cv;
    bool finished = false;
    std::unique_lock<bthread::Mutex> lock(mu);
    auto st = _thread_pool_token->submit_func([&, this]() {
        _warm_up_rowset(rs_meta, table_id, sync_wait_timeout_ms);
        std::unique_lock<bthread::Mutex> l(mu);
        finished = true;
        cv.notify_one();
    });
    if (!st.ok()) {
        LOG(WARNING) << "Failed to submit warm up rowset task: " << st;
        file_cache_warm_up_failed_task_num << 1;
    } else {
        while (!finished) {
            TEST_SYNC_POINT_CALLBACK("CloudWarmUpManager::warm_up_rowset.before_wait", &cv);
            cv.wait(lock);
        }
    }
}

void CloudWarmUpManager::_warm_up_rowset(RowsetMeta& rs_meta, int64_t table_id,
                                         int64_t sync_wait_timeout_ms) {
    TEST_SYNC_POINT_CALLBACK("CloudWarmUpManager::_warm_up_rowset.enter", &rs_meta,
                             &sync_wait_timeout_ms);
    bool cache_hit = false;
    auto replicas = get_replica_info(rs_meta.tablet_id(), table_id, false, cache_hit);
    if (replicas.empty()) {
        VLOG_DEBUG << "There is no need to warmup tablet=" << rs_meta.tablet_id()
                   << ", skipping rowset=" << rs_meta.rowset_id().to_string();
        g_file_cache_event_driven_warm_up_skipped_rowset_num << 1;
        return;
    }
    Status st = _do_warm_up_rowset(rs_meta, table_id, replicas, sync_wait_timeout_ms, !cache_hit);
    if (cache_hit && !st.ok() && st.is<ErrorCode::TABLE_NOT_FOUND>()) {
        replicas = get_replica_info(rs_meta.tablet_id(), table_id, true, cache_hit);
        st = _do_warm_up_rowset(rs_meta, table_id, replicas, sync_wait_timeout_ms, true);
    }
    if (!st.ok()) {
        LOG(WARNING) << "Failed to warm up rowset, tablet_id=" << rs_meta.tablet_id()
                     << ", rowset_id=" << rs_meta.rowset_id().to_string() << ", status=" << st;
    }
}

Status CloudWarmUpManager::_build_warm_up_rowset_result(
        const std::vector<WarmUpRowsetFailure>& failures, size_t replica_count, int64_t tablet_id,
        int64_t table_id, const std::string& rowset_id) {
    if (failures.empty()) {
        return Status::OK();
    }

    int code = failures.front().code;
    std::string failure_msg;
    for (size_t i = 0; i < failures.size(); ++i) {
        if (failures[i].code == ErrorCode::TABLE_NOT_FOUND) {
            code = ErrorCode::TABLE_NOT_FOUND;
        }
        if (i > 0) {
            failure_msg.append("; ");
        }
        failure_msg.append(failures[i].reason);
    }

    return Status::Error(code,
                         "warm up rowset failed on {}/{} replicas, tablet_id={}, table_id={}, "
                         "rowset_id={}, failures=[{}]",
                         failures.size(), replica_count, tablet_id, table_id, rowset_id,
                         failure_msg);
}

Status CloudWarmUpManager::_do_warm_up_rowset(RowsetMeta& rs_meta, int64_t table_id,
                                              std::vector<JobReplicaInfo>& replicas,
                                              int64_t sync_wait_timeout_ms,
                                              bool skip_existence_check) {
    auto tablet_id = rs_meta.tablet_id();
    int64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    g_file_cache_warm_up_rowset_last_call_unix_ts.set_value(now_ts);
    std::vector<WarmUpRowsetFailure> failures;
    auto add_failure = [&failures](const JobReplicaInfo& info, const std::string& target,
                                   const Status& st) {
        failures.push_back(WarmUpRowsetFailure {
                .code = st.code(),
                .reason = "job_id=" + std::to_string(info.job_id) +
                          ", backend_id=" + std::to_string(info.replica.backend_id) +
                          ", target=" + target + ", status=" + st.to_string_no_stack()});
    };

    for (auto& info : replicas) {
        std::string job_id_str = std::to_string(info.job_id);
        std::string target = get_host_port(info.replica.host, info.replica.brpc_port);
        int64_t trigger_ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();

        PWarmUpRowsetRequest request;
        request.add_rowset_metas()->CopyFrom(rs_meta.get_rowset_pb());
        request.set_unix_ts_us(now_ts);
        request.set_sync_wait_timeout_ms(sync_wait_timeout_ms);
        request.set_skip_existence_check(skip_existence_check);
        request.set_job_id(info.job_id);
        request.set_upstream_trigger_ts_ms(trigger_ts_ms);

        // send sync request
        std::string host = info.replica.host;
        auto dns_cache = ExecEnv::GetInstance()->dns_cache();
        if (dns_cache == nullptr) {
            LOG(WARNING) << "DNS cache is not initialized, skipping hostname resolve";
        } else if (!is_valid_ip(info.replica.host)) {
            Status status = dns_cache->get(info.replica.host, &host);
            if (!status.ok()) {
                LOG(WARNING) << "failed to get ip from host " << info.replica.host << ": "
                             << status.to_string();
                add_failure(info, target, status);
                continue;
            }
        }
        std::string brpc_addr = get_host_port(host, info.replica.brpc_port);
        Status st = Status::OK();
        std::shared_ptr<PBackendService_Stub> brpc_stub =
                ExecEnv::GetInstance()->brpc_internal_client_cache()->get_new_client_no_cache(
                        brpc_addr);
        if (!brpc_stub) {
            st = Status::RpcError("Address {} is wrong", brpc_addr);
            add_failure(info, target, st);
            continue;
        }

        // update metrics
        auto schema_ptr = rs_meta.tablet_schema();
        auto idx_version = schema_ptr->get_inverted_index_storage_format();
        for (int64_t segment_id = 0; segment_id < rs_meta.num_segments(); segment_id++) {
            auto seg_size = rs_meta.segment_file_size(cast_set<int>(segment_id));

            g_file_cache_event_driven_warm_up_requested_segment_num << 1;
            g_warmup_ed_requested_segment_num.put({job_id_str}, 1);

            g_file_cache_event_driven_warm_up_requested_segment_size << seg_size;
            g_warmup_ed_requested_segment_size.put({job_id_str}, seg_size);

            if (schema_ptr->has_inverted_index() || schema_ptr->has_ann_index()) {
                if (idx_version == InvertedIndexStorageFormatPB::V1) {
                    auto&& inverted_index_info =
                            rs_meta.inverted_index_file_info(cast_set<int>(segment_id));
                    if (inverted_index_info.index_info().empty()) {
                        VLOG_DEBUG << "No index info available for segment " << segment_id;
                        continue;
                    }
                    for (const auto& idx_info : inverted_index_info.index_info()) {
                        g_file_cache_event_driven_warm_up_requested_index_num << 1;
                        g_warmup_ed_requested_index_num.put({job_id_str}, 1);

                        if (idx_info.index_file_size() != -1) {
                            g_file_cache_event_driven_warm_up_requested_index_size
                                    << idx_info.index_file_size();
                            g_warmup_ed_requested_index_size.put({job_id_str},
                                                                 idx_info.index_file_size());
                        } else {
                            VLOG_DEBUG << "Invalid index_file_size for segment_id " << segment_id
                                       << ", index_id " << idx_info.index_id();
                        }
                    }
                } else { // InvertedIndexStorageFormatPB::V2
                    auto&& inverted_index_info =
                            rs_meta.inverted_index_file_info(cast_set<int>(segment_id));
                    g_file_cache_event_driven_warm_up_requested_index_num << 1;
                    g_warmup_ed_requested_index_num.put({job_id_str}, 1);

                    if (inverted_index_info.has_index_size()) {
                        g_file_cache_event_driven_warm_up_requested_index_size
                                << inverted_index_info.index_size();
                        g_warmup_ed_requested_index_size.put({job_id_str},
                                                             inverted_index_info.index_size());
                    } else {
                        VLOG_DEBUG << "index_size is not set for segment " << segment_id;
                    }
                }
            }
        }

        // Update last trigger timestamp
        auto* trigger_ts =
                g_warmup_ed_last_trigger_ts.get_stats(std::list<std::string> {job_id_str});
        if (trigger_ts) {
            trigger_ts->set_value(trigger_ts_ms);
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
            add_failure(info, target, Status::RpcError(cntl.ErrorText()));
            continue;
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
        auto status = Status::create<false>(response.status());
        if (response.has_status() && !status.ok()) {
            LOG(INFO) << "warm_up_rowset failed, tablet_id=" << rs_meta.tablet_id()
                      << ", rowset_id=" << rs_meta.rowset_id().to_string()
                      << ", target=" << info.replica.host << ", skip_existence_check"
                      << skip_existence_check << ", status=" << status;
            add_failure(info, target, status);
        }
    }
    return _build_warm_up_rowset_result(failures, replicas.size(), tablet_id, table_id,
                                        rs_meta.rowset_id().to_string());
}

void CloudWarmUpManager::recycle_cache(int64_t tablet_id,
                                       const std::vector<RecycledRowsets>& rowsets) {
    bthread::Mutex mu;
    bthread::ConditionVariable cv;
    std::unique_lock<bthread::Mutex> lock(mu);
    auto st = _thread_pool_token->submit_func([&, this]() {
        std::unique_lock<bthread::Mutex> l(mu);
        _recycle_cache(tablet_id, rowsets);
        cv.notify_one();
    });
    if (!st.ok()) {
        LOG(WARNING) << "Failed to submit recycle cache task, tablet_id=" << tablet_id
                     << ", error=" << st;
    } else {
        cv.wait(lock);
    }
}

void CloudWarmUpManager::_recycle_cache(int64_t tablet_id,
                                        const std::vector<RecycledRowsets>& rowsets) {
    LOG(INFO) << "recycle_cache: tablet_id=" << tablet_id << ", num_rowsets=" << rowsets.size();
    bool cache_hit = false;
    auto replicas = get_replica_info(tablet_id, /*table_id=*/0, false, cache_hit);
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
        std::string host = replica.replica.host;
        if (dns_cache == nullptr) {
            LOG(WARNING) << "DNS cache is not initialized, skipping hostname resolve";
        } else if (!is_valid_ip(replica.replica.host)) {
            Status status = dns_cache->get(replica.replica.host, &host);
            if (!status.ok()) {
                LOG(WARNING) << "failed to get ip from host " << replica.replica.host << ": "
                             << status.to_string();
                return;
            }
        }
        std::string brpc_addr = get_host_port(host, replica.replica.brpc_port);
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

// Balance warm up cache management methods implementation
void CloudWarmUpManager::record_balanced_tablet(int64_t tablet_id, const std::string& host,
                                                int32_t brpc_port,
                                                const std::string& compute_group_id) {
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();

    PeerCandidate candidate;
    candidate.host = host;
    candidate.brpc_port = brpc_port;
    candidate.compute_group_id = compute_group_id;
    candidate.last_access_time_ms = now_ms;
    candidate.consecutive_rpc_failures = 0;

    auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);

    auto [it, inserted] = shard.tablets.try_emplace(tablet_id);
    if (inserted) {
        // Only increment the gauge counter on first insertion.
        g_balance_tablet_be_mapping_size << 1;
    }

    auto& cands = it->second.candidates;
    // Warmup rebalance: a tablet has at most one warm-up peer (the current rebalance source).
    // Upsert: replace existing same-CG entry if present, otherwise prepend.
    auto same_cg_it = std::find_if(cands.begin(), cands.end(), [&](const PeerCandidate& c) {
        return c.compute_group_id == compute_group_id;
    });

    if (same_cg_it != cands.end()) {
        // Update in-place, preserve position (already at or near front from prior insert).
        same_cg_it->host = std::move(candidate.host);
        same_cg_it->brpc_port = candidate.brpc_port;
        same_cg_it->last_access_time_ms = candidate.last_access_time_ms;
        same_cg_it->consecutive_rpc_failures = 0;
    } else {
        // New CG entry: insert at front (warmup has highest priority).
        cands.insert(cands.begin(), std::move(candidate));
    }

    VLOG_DEBUG << "Recorded balanced warm up cache tablet: tablet_id=" << tablet_id
               << ", host=" << host << ":" << brpc_port
               << ", compute_group_id=" << compute_group_id;
}

void CloudWarmUpManager::remove_balanced_tablet(int64_t tablet_id) {
    auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto it = shard.tablets.find(tablet_id);
    if (it != shard.tablets.end()) {
        shard.tablets.erase(it);
        g_balance_tablet_be_mapping_size << -1;
        VLOG_DEBUG << "Removed balanced warm up cache tablet by timer, tablet_id=" << tablet_id;
    }
}

void CloudWarmUpManager::remove_balanced_tablets(const std::vector<int64_t>& tablet_ids) {
    // Group tablet_ids by shard to minimize lock contention
    std::array<std::vector<int64_t>, SHARD_COUNT> shard_groups;
    for (int64_t tablet_id : tablet_ids) {
        shard_groups[get_shard_index(tablet_id)].push_back(tablet_id);
    }

    // Process each shard
    for (size_t i = 0; i < SHARD_COUNT; ++i) {
        if (shard_groups[i].empty()) continue;

        auto& shard = _balanced_tablets_shards[i];
        std::unique_lock<bthread::Mutex> lock(shard.mtx);
        for (int64_t tablet_id : shard_groups[i]) {
            auto it = shard.tablets.find(tablet_id);
            if (it != shard.tablets.end()) {
                shard.tablets.erase(it);
                g_balance_tablet_be_mapping_size << -1;
                VLOG_DEBUG << "Removed balanced warm up cache tablet: tablet_id=" << tablet_id;
            }
        }
    }
}

// Cleanup loop: runs on a dedicated pthread, wakes up periodically to evict
// expired peer candidates and empty tablet entries.
void CloudWarmUpManager::run_cleanup_loop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(_cleanup_mtx);
            _cleanup_cond.wait_for(lock,
                                   std::chrono::seconds(config::peer_candidate_cleanup_interval_s),
                                   [this]() { return _closed; });
            if (_closed) break;
        }

        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        int64_t expiry_ms = config::peer_candidate_expiry_s * 1000LL;

        for (auto& shard : _balanced_tablets_shards) {
            std::unique_lock<bthread::Mutex> lock(shard.mtx);
            auto tablet_it = shard.tablets.begin();
            while (tablet_it != shard.tablets.end()) {
                auto& tpc = tablet_it->second;
                // Remove expired candidates
                auto& cands = tpc.candidates;
                size_t cands_before = cands.size();
                cands.erase(std::remove_if(cands.begin(), cands.end(),
                                           [&](const PeerCandidate& c) {
                                               return (now_ms - c.last_access_time_ms) >= expiry_ms;
                                           }),
                            cands.end());
                size_t removed = cands_before - cands.size();
                if (removed > 0) {
                    g_peer_candidate_expiry_eviction << removed;
                }
                // Remove the tablet entry if no candidates remain
                if (cands.empty()) {
                    tablet_it = shard.tablets.erase(tablet_it);
                    g_balance_tablet_be_mapping_size << -1;
                } else {
                    ++tablet_it;
                }
            }
        }
    }
}

// fetch_candidates_from_fe: lazy fetch path — appends candidates to the end
// (lower priority than warmup-inserted ones).  Uses singleflight to avoid
// duplicate concurrent RPCs for the same tablet.
void CloudWarmUpManager::fetch_candidates_from_fe(int64_t tablet_id) {
    // --- singleflight check ---
    {
        auto& shard = get_shard(tablet_id);
        std::unique_lock<bthread::Mutex> lock(shard.mtx);
        auto it = shard.tablets.find(tablet_id);
        if (it != shard.tablets.end() && it->second.fetching_from_fe) {
            return; // another fetch is already in flight
        }
        // Increment gauge when we create a genuinely new tablet entry
        if (it == shard.tablets.end()) {
            g_balance_tablet_be_mapping_size << 1;
        }
        // Mark as fetching (creates entry if not present).
        shard.tablets[tablet_id].fetching_from_fe = true;
    }

    // Use Defer to absolutely guarantee we reset the fetching flag on return
    Defer defer_fetching_reset {[this, tablet_id]() {
        auto& shard = get_shard(tablet_id);
        std::unique_lock<bthread::Mutex> lock(shard.mtx);
        auto it = shard.tablets.find(tablet_id);
        if (it != shard.tablets.end()) {
            it->second.fetching_from_fe = false;
        }
    }};

    // --- RPC to FE (without warm_up_job_id) ---
    ClusterInfo* cluster_info = ExecEnv::GetInstance()->cluster_info();
    if (cluster_info == nullptr) {
        LOG(WARNING) << "fetch_candidates_from_fe: have not got FE Master heartbeat yet"
                     << ", tablet_id=" << tablet_id;
        return;
    }
    TNetworkAddress master_addr = cluster_info->master_fe_addr;
    if (master_addr.hostname.empty() || master_addr.port == 0) {
        LOG(WARNING) << "fetch_candidates_from_fe: FE master address unknown"
                     << ", tablet_id=" << tablet_id;
        return;
    }

    TGetTabletReplicaInfosRequest request;
    TGetTabletReplicaInfosResult result;
    // No warm_up_job_id — lazy fetch path
    request.tablet_ids.emplace_back(tablet_id);

    g_peer_lazy_fetch_total << 1;
    const auto rpc_start = std::chrono::steady_clock::now();
    Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->getTabletReplicaInfos(result, request);
            });
    g_peer_lazy_fetch_latency << std::chrono::duration_cast<std::chrono::microseconds>(
                                         std::chrono::steady_clock::now() - rpc_start)
                                         .count();

    if (!rpc_st.ok()) {
        LOG(WARNING) << "fetch_candidates_from_fe: rpc failed, tablet_id=" << tablet_id
                     << ", error=" << rpc_st;
        g_peer_lazy_fetch_failed << 1;
        return;
    }

    auto st = Status::create<false>(result.status);
    if (!st.ok()) {
        LOG(WARNING) << "fetch_candidates_from_fe: FE returned error, tablet_id=" << tablet_id
                     << ", status=" << st;
        g_peer_lazy_fetch_failed << 1;
        return;
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();

    // Parse the results OUTSIDE the lock
    std::vector<PeerCandidate> new_candidates;
    const std::string& self_host = BackendOptions::get_localhost();
    const int32_t self_brpc_port = config::brpc_port;

    auto it_res = result.tablet_replica_infos.find(tablet_id);
    if (it_res != result.tablet_replica_infos.end()) {
        const auto& replicas = it_res->second;
        // Pre-allocate memory since we know the upper bound of candidates
        new_candidates.reserve(replicas.size());

        for (const auto& replica : replicas) {
            // Skip self: a BE must not peer-read from its own file cache
            if (replica.host == self_host && replica.brpc_port == self_brpc_port) {
                VLOG_DEBUG << "fetch_candidates_from_fe: skipping self candidate " << replica.host
                           << ":" << replica.brpc_port << " for tablet_id=" << tablet_id;
                continue;
            }

            PeerCandidate& candidate = new_candidates.emplace_back();
            candidate.host = replica.host;
            candidate.brpc_port = replica.brpc_port;
            if (replica.__isset.cloud_compute_group_id) {
                candidate.compute_group_id = replica.cloud_compute_group_id;
            }
            candidate.last_access_time_ms = now_ms;
            candidate.consecutive_rpc_failures = 0;
        }
    }

    g_peer_lazy_fetch_success << 1;

    // --- Merge results back into shard ---
    // Acquire lock only to append to the candidates vector
    {
        auto& shard = get_shard(tablet_id);
        std::unique_lock<bthread::Mutex> lock(shard.mtx);
        auto it = shard.tablets.find(tablet_id);
        // Safely check if tablet is still there
        if (it != shard.tablets.end()) {
            auto& tpc = it->second;
            tpc.candidates.insert(tpc.candidates.end(),
                                  std::make_move_iterator(new_candidates.begin()),
                                  std::make_move_iterator(new_candidates.end()));
            LOG(INFO) << "fetch_candidates_from_fe: tablet_id=" << tablet_id << " got "
                      << tpc.candidates.size() << " total candidates from FE";
            VLOG_DEBUG << "fetch_candidates_from_fe: added " << new_candidates.size()
                       << " candidates for tablet_id=" << tablet_id;
        }
    }
}

std::vector<PeerCandidate> CloudWarmUpManager::get_peer_candidates(int64_t tablet_id) {
    auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto it = shard.tablets.find(tablet_id);
    if (it == shard.tablets.end()) {
        g_peer_candidate_cache_miss << 1;
        return {};
    }
    // Update last_access_time_ms for all candidates to keep them alive
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    for (auto& c : it->second.candidates) {
        c.last_access_time_ms = now_ms;
    }
    auto& tpc = it->second;
    // Cooldown check: if this tablet is in cooldown, return empty to skip peer.
    if (tpc.cooldown_until_ms > 0 && now_ms < tpc.cooldown_until_ms) {
        g_peer_tablet_cooldown_skipped << 1;
        return {};
    }
    // Cooldown expired — reset for next cycle.
    if (tpc.cooldown_until_ms > 0) {
        tpc.cooldown_until_ms = 0;
        tpc.consecutive_all_miss = 0;
    }
    auto result = tpc.candidates;
    if (result.empty()) {
        g_peer_candidate_cache_miss << 1;
    } else {
        g_peer_candidate_cache_hit << 1;
        // Apply compute group affinity: if a previous read succeeded from a particular
        // compute group, move its candidates to the front so the next read tries it first.
        // stable_partition preserves relative order within each group.
        //
        // Example:
        // Candidates: [A(CG1), B(CG2), C(CG1), D(CG3)]
        // pref = "CG1"
        // After stable_partition: [A(CG1), C(CG1), B(CG2), D(CG3)]
        // (A remains before C, and B remains before D)
        if (!tpc.last_successful_compute_group_id.empty()) {
            const std::string& pref = tpc.last_successful_compute_group_id;
            std::stable_partition(result.begin(), result.end(), [&pref](const PeerCandidate& c) {
                return c.compute_group_id == pref;
            });
        }
    }
    return result;
}

void CloudWarmUpManager::update_peer_candidate_on_success(int64_t tablet_id,
                                                          const std::string& compute_group_id) {
    auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto it = shard.tablets.find(tablet_id);
    if (it == shard.tablets.end()) {
        return;
    }
    it->second.last_successful_compute_group_id = compute_group_id;
    it->second.consecutive_all_miss = 0;
    it->second.cooldown_until_ms = 0;
}

void CloudWarmUpManager::update_peer_candidate_on_rpc_failure(int64_t tablet_id,
                                                              const std::string& host,
                                                              int32_t brpc_port) {
    auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto it = shard.tablets.find(tablet_id);
    if (it == shard.tablets.end()) {
        return;
    }
    auto& cands = it->second.candidates;
    for (auto cit = cands.begin(); cit != cands.end(); ++cit) {
        if (cit->host == host && cit->brpc_port == brpc_port) {
            ++cit->consecutive_rpc_failures;
            if (cit->consecutive_rpc_failures >= config::peer_rpc_failure_eviction_threshold) {
                LOG(INFO) << "Evicting peer candidate due to consecutive RPC failures"
                          << ", tablet_id=" << tablet_id << ", host=" << host << ":" << brpc_port
                          << ", failures=" << cit->consecutive_rpc_failures;
                g_peer_rpc_failure_eviction << 1;
                cands.erase(cit);
                // If all candidates have been evicted, remove the tablet entry
                // entirely so that the gauge stays accurate.
                if (cands.empty()) {
                    shard.tablets.erase(it);
                    g_balance_tablet_be_mapping_size << -1;
                }
            }
            break;
        }
    }
}

void CloudWarmUpManager::rotate_peer_candidate_on_cache_miss(int64_t tablet_id,
                                                             const std::string& host,
                                                             int32_t brpc_port) {
    auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto it = shard.tablets.find(tablet_id);
    if (it == shard.tablets.end()) {
        return;
    }
    auto& cands = it->second.candidates;
    auto cit = std::find_if(cands.begin(), cands.end(), [&](const PeerCandidate& c) {
        return c.host == host && c.brpc_port == brpc_port;
    });
    if (cit != cands.end() && std::next(cit) != cands.end()) {
        // Move this candidate to the end so the next read tries a different one.
        // This ensures that if the first N candidates are all cache-miss, the system
        // gradually converges to whichever compute group actually has the data.
        //
        // Example:
        // cands: [B, C, D],  cit points to B (front, cache miss)
        // std::rotate(B, C, end) → [C, D, B]
        // Next read tries C first instead of B.
        //
        // Also clear affinity if the rotated candidate belongs to the currently preferred
        // compute group.  Without this, get_peer_candidates() would stable_partition that
        // CG back to the front on the very next call — completely undoing the rotate.
        if (it->second.last_successful_compute_group_id == cit->compute_group_id) {
            it->second.last_successful_compute_group_id.clear();
        }
        std::rotate(cit, std::next(cit), cands.end());
    }
    // Always count the metric when the candidate is found, even if it is the
    // last (or only) element where rotation is a no-op.
    if (cit != cands.end()) {
        g_peer_candidate_rotate << 1;
    }
}

bool CloudWarmUpManager::is_peer_cooldown(int64_t tablet_id) const {
    const auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto it = shard.tablets.find(tablet_id);
    if (it == shard.tablets.end()) {
        return false;
    }
    if (it->second.cooldown_until_ms <= 0) {
        return false;
    }
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    return now_ms < it->second.cooldown_until_ms;
}

void CloudWarmUpManager::record_peer_all_miss(int64_t tablet_id) {
    auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto it = shard.tablets.find(tablet_id);
    if (it == shard.tablets.end()) {
        return;
    }
    auto& tpc = it->second;
    tpc.consecutive_all_miss++;
    if (tpc.consecutive_all_miss >= config::peer_all_miss_cooldown_threshold) {
        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        tpc.cooldown_until_ms = now_ms + config::peer_all_miss_cooldown_duration_s * 1000;
        g_peer_tablet_cooldown_entered << 1;
        LOG(INFO) << "Peer read cooldown entered for tablet_id=" << tablet_id << " after "
                  << tpc.consecutive_all_miss << " consecutive all-miss races"
                  << ", cooldown_duration_s=" << config::peer_all_miss_cooldown_duration_s;
    }
}

std::optional<TabletPeerCandidates> CloudWarmUpManager::get_tablet_peer_info(
        int64_t tablet_id) const {
    const auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto it = shard.tablets.find(tablet_id);
    if (it == shard.tablets.end()) {
        return std::nullopt;
    }
    return it->second; // copy under lock
}

std::vector<std::pair<int64_t, TabletPeerCandidates>> CloudWarmUpManager::get_all_peer_info(
        int64_t limit) const {
    std::vector<std::pair<int64_t, TabletPeerCandidates>> result;
    for (size_t i = 0; i < SHARD_COUNT; ++i) {
        const auto& shard = _balanced_tablets_shards[i];
        std::unique_lock<bthread::Mutex> lock(shard.mtx);
        for (const auto& [tid, tpc] : shard.tablets) {
            result.emplace_back(tid, tpc);
            if (limit > 0 && static_cast<int64_t>(result.size()) >= limit) {
                return result;
            }
        }
    }
    return result;
}

void CloudWarmUpManager::set_tablet_peer_candidates(int64_t tablet_id,
                                                    TabletPeerCandidates candidates) {
    auto& shard = get_shard(tablet_id);
    std::unique_lock<bthread::Mutex> lock(shard.mtx);
    auto [it, inserted] = shard.tablets.insert_or_assign(tablet_id, std::move(candidates));
    if (inserted) {
        g_balance_tablet_be_mapping_size << 1;
    }
}

} // namespace doris
