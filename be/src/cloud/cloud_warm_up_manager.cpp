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

#include <bthread/countdown_event.h>

#include <algorithm>
#include <cstddef>
#include <tuple>

#include "cloud/cloud_tablet_mgr.h"
#include "common/logging.h"
#include "io/cache/block_file_cache_downloader.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet.h"
#include "runtime/exec_env.h"
#include "util/time.h"

namespace doris {

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

void CloudWarmUpManager::handle_jobs() {
#ifndef BE_TEST
    while (true) {
        JobMeta* cur_job = nullptr;
        {
            std::unique_lock lock(_mtx);
            _cond.wait(lock, [this]() { return _closed || !_pending_job_metas.empty(); });
            if (_closed) break;
            cur_job = &_pending_job_metas.front();
        }
        for (int64_t tablet_id : cur_job->tablet_ids) {
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
            std::shared_ptr<bthread::CountdownEvent> wait =
                    std::make_shared<bthread::CountdownEvent>(0);
            auto tablet_meta = tablet->tablet_meta();
            auto rs_metas = tablet_meta->snapshot_rs_metas();
            for (auto& [_, rs] : rs_metas) {
                for (int64_t seg_id = 0; seg_id < rs->num_segments(); seg_id++) {
                    auto fs = rs->fs();
                    if (!fs) {
                        LOG(WARNING) << "failed to get fs. tablet_id=" << tablet_id
                                     << " rowset_id=" << rs->rowset_id()
                                     << " resource_id=" << rs->resource_id();
                        continue;
                    }

                    int64_t expiration_time =
                            tablet_meta->ttl_seconds() == 0 || rs->newest_write_timestamp() <= 0
                                    ? 0
                                    : rs->newest_write_timestamp() + tablet_meta->ttl_seconds();
                    if (expiration_time <= UnixSeconds()) {
                        expiration_time = 0;
                    }

                    wait->add_count();
                    _engine.file_cache_block_downloader().submit_download_task(
                            io::DownloadFileMeta {
                                    .path = BetaRowset::remote_segment_path(
                                            rs->tablet_id(), rs->rowset_id(), seg_id),
                                    .file_size = rs->segment_file_size(seg_id),
                                    .file_system = std::move(fs),
                                    .ctx =
                                            {
                                                    .expiration_time = expiration_time,
                                            },
                                    .download_done =
                                            [wait](Status st) {
                                                if (!st) {
                                                    LOG_WARNING("Warm up error ").error(st);
                                                }
                                                wait->signal();
                                            },
                            });
                }
            }
            if (!wait->wait()) {
                LOG_WARNING("Warm up tablet {} take a long time", tablet_meta->tablet_id());
            }
        }
        {
            std::unique_lock lock(_mtx);
            _finish_job.push_back(std::move(*cur_job));
            _pending_job_metas.pop_front();
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
        std::for_each(job_metas.begin(), job_metas.end(),
                      [this](const TJobMeta& meta) { _pending_job_metas.emplace_back(meta); });
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

} // namespace doris
