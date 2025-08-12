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

#include "cloud/cloud_internal_service.h"

#include <bthread/countdown_event.h>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_downloader.h"
#include "io/cache/block_file_cache_factory.h"

namespace doris {
#include "common/compile_check_begin.h"

CloudInternalServiceImpl::CloudInternalServiceImpl(CloudStorageEngine& engine, ExecEnv* exec_env)
        : PInternalService(exec_env), _engine(engine) {}

CloudInternalServiceImpl::~CloudInternalServiceImpl() = default;

void CloudInternalServiceImpl::alter_vault_sync(google::protobuf::RpcController* controller,
                                                const doris::PAlterVaultSyncRequest* request,
                                                PAlterVaultSyncResponse* response,
                                                google::protobuf::Closure* done) {
    LOG(INFO) << "alter be to sync vault info from Meta Service";
    // If the vaults containing hdfs vault then it would try to create hdfs connection using jni
    // which would acuiqre one thread local jniEnv. But bthread context can't guarantee that the brpc
    // worker thread wouldn't do bthread switch between worker threads.
    bool ret = _heavy_work_pool.try_offer([this, done]() {
        brpc::ClosureGuard closure_guard(done);
        _engine.sync_storage_vault();
    });
    if (!ret) {
        brpc::ClosureGuard closure_guard(done);
        LOG(WARNING) << "fail to offer alter_vault_sync request to the work pool, pool="
                     << _heavy_work_pool.get_info();
    }
}

FileCacheType cache_type_to_pb(io::FileCacheType type) {
    switch (type) {
    case io::FileCacheType::TTL:
        return FileCacheType::TTL;
    case io::FileCacheType::INDEX:
        return FileCacheType::INDEX;
    case io::FileCacheType::NORMAL:
        return FileCacheType::NORMAL;
    default:
        DCHECK(false);
    }
    return FileCacheType::NORMAL;
}

void CloudInternalServiceImpl::get_file_cache_meta_by_tablet_id(
        google::protobuf::RpcController* controller [[maybe_unused]],
        const PGetFileCacheMetaRequest* request, PGetFileCacheMetaResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    if (!config::enable_file_cache) {
        LOG_WARNING("try to access tablet file cache meta, but file cache not enabled");
        return;
    }
    LOG(INFO) << "warm up get meta from this be, tablets num=" << request->tablet_ids().size();
    for (const auto& tablet_id : request->tablet_ids()) {
        auto res = _engine.tablet_mgr().get_tablet(tablet_id);
        if (!res.has_value()) {
            LOG(ERROR) << "failed to get tablet: " << tablet_id
                       << " err msg: " << res.error().msg();
            return;
        }
        CloudTabletSPtr tablet = std::move(res.value());
        auto st = tablet->sync_rowsets();
        if (!st) {
            // just log failed, try it best
            LOG(WARNING) << "failed to sync rowsets: " << tablet_id
                         << " err msg: " << st.to_string();
        }
        auto rowsets = tablet->get_snapshot_rowset();
        std::for_each(rowsets.cbegin(), rowsets.cend(), [&](const RowsetSharedPtr& rowset) {
            std::string rowset_id = rowset->rowset_id().to_string();
            for (int32_t segment_id = 0; segment_id < rowset->num_segments(); segment_id++) {
                std::string file_name = fmt::format("{}_{}.dat", rowset_id, segment_id);
                auto cache_key = io::BlockFileCache::hash(file_name);
                auto* cache = io::FileCacheFactory::instance()->get_by_path(cache_key);

                auto segments_meta = cache->get_hot_blocks_meta(cache_key);
                for (const auto& tuple : segments_meta) {
                    FileCacheBlockMeta* meta = response->add_file_cache_block_metas();
                    meta->set_tablet_id(tablet_id);
                    meta->set_rowset_id(rowset_id);
                    meta->set_segment_id(segment_id);
                    meta->set_file_name(file_name);
                    meta->set_file_size(rowset->rowset_meta()->segment_file_size(segment_id));
                    meta->set_offset(std::get<0>(tuple));
                    meta->set_size(std::get<1>(tuple));
                    meta->set_cache_type(cache_type_to_pb(std::get<2>(tuple)));
                    meta->set_expiration_time(std::get<3>(tuple));
                }
            }
        });
    }
    VLOG_DEBUG << "warm up get meta request=" << request->DebugString()
               << ", response=" << response->DebugString();
}
#include "common/compile_check_end.h"

bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_submitted_segment_num(
        "file_cache_event_driven_warm_up_submitted_segment_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_finished_segment_num(
        "file_cache_event_driven_warm_up_finished_segment_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_failed_segment_num(
        "file_cache_event_driven_warm_up_failed_segment_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_submitted_segment_size(
        "file_cache_event_driven_warm_up_submitted_segment_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_finished_segment_size(
        "file_cache_event_driven_warm_up_finished_segment_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_failed_segment_size(
        "file_cache_event_driven_warm_up_failed_segment_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_submitted_index_num(
        "file_cache_event_driven_warm_up_submitted_index_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_finished_index_num(
        "file_cache_event_driven_warm_up_finished_index_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_failed_index_num(
        "file_cache_event_driven_warm_up_failed_index_num");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_submitted_index_size(
        "file_cache_event_driven_warm_up_submitted_index_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_finished_index_size(
        "file_cache_event_driven_warm_up_finished_index_size");
bvar::Adder<uint64_t> g_file_cache_event_driven_warm_up_failed_index_size(
        "file_cache_event_driven_warm_up_failed_index_size");
bvar::Status<int64_t> g_file_cache_warm_up_rowset_last_handle_unix_ts(
        "file_cache_warm_up_rowset_last_handle_unix_ts", 0);
bvar::Status<int64_t> g_file_cache_warm_up_rowset_last_finish_unix_ts(
        "file_cache_warm_up_rowset_last_finish_unix_ts", 0);
bvar::LatencyRecorder g_file_cache_warm_up_rowset_latency("file_cache_warm_up_rowset_latency");
bvar::LatencyRecorder g_file_cache_warm_up_rowset_request_to_handle_latency(
        "file_cache_warm_up_rowset_request_to_handle_latency");
bvar::LatencyRecorder g_file_cache_warm_up_rowset_handle_to_finish_latency(
        "file_cache_warm_up_rowset_handle_to_finish_latency");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_slow_count(
        "file_cache_warm_up_rowset_slow_count");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_request_to_handle_slow_count(
        "file_cache_warm_up_rowset_request_to_handle_slow_count");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_handle_to_finish_slow_count(
        "file_cache_warm_up_rowset_handle_to_finish_slow_count");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_wait_for_compaction_num(
        "file_cache_warm_up_rowset_wait_for_compaction_num");
bvar::Adder<uint64_t> g_file_cache_warm_up_rowset_wait_for_compaction_timeout_num(
        "file_cache_warm_up_rowset_wait_for_compaction_timeout_num");

void CloudInternalServiceImpl::warm_up_rowset(google::protobuf::RpcController* controller
                                              [[maybe_unused]],
                                              const PWarmUpRowsetRequest* request,
                                              PWarmUpRowsetResponse* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    std::shared_ptr<bthread::CountdownEvent> wait = nullptr;
    timespec due_time;
    if (request->has_sync_wait_timeout_ms() && request->sync_wait_timeout_ms() > 0) {
        g_file_cache_warm_up_rowset_wait_for_compaction_num << 1;
        wait = std::make_shared<bthread::CountdownEvent>(0);
        VLOG_DEBUG << "sync_wait_timeout: " << request->sync_wait_timeout_ms() << " ms";
        due_time = butil::milliseconds_from_now(request->sync_wait_timeout_ms());
    }

    for (auto& rs_meta_pb : request->rowset_metas()) {
        RowsetMeta rs_meta;
        rs_meta.init_from_pb(rs_meta_pb);
        auto storage_resource = rs_meta.remote_storage_resource();
        if (!storage_resource) {
            LOG(WARNING) << storage_resource.error();
            continue;
        }
        int64_t tablet_id = rs_meta.tablet_id();
        auto res = _engine.tablet_mgr().get_tablet(tablet_id, /* warmup_data = */ false,
                                                   /* sync_delete_bitmap = */ true,
                                                   /* sync_stats = */ nullptr,
                                                   /* local_only = */ true);
        if (!res.has_value()) {
            LOG_WARNING("Warm up error ").tag("tablet_id", tablet_id).error(res.error());
            continue;
        }
        auto tablet = res.value();
        auto tablet_meta = tablet->tablet_meta();

        int64_t handle_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
        g_file_cache_warm_up_rowset_last_handle_unix_ts.set_value(handle_ts);
        int64_t request_ts = request->has_unix_ts_us() ? request->unix_ts_us() : 0;
        g_file_cache_warm_up_rowset_request_to_handle_latency << (handle_ts - request_ts);
        if (request_ts > 0 && handle_ts - request_ts > config::warm_up_rowset_slow_log_ms * 1000) {
            g_file_cache_warm_up_rowset_request_to_handle_slow_count << 1;
            LOG(INFO) << "warm up rowset (request to handle) took " << handle_ts - request_ts
                      << " us, tablet_id: " << rs_meta.tablet_id()
                      << ", rowset_id: " << rs_meta.rowset_id().to_string();
        }
        int64_t expiration_time =
                tablet_meta->ttl_seconds() == 0 || rs_meta.newest_write_timestamp() <= 0
                        ? 0
                        : rs_meta.newest_write_timestamp() + tablet_meta->ttl_seconds();
        if (expiration_time <= UnixSeconds()) {
            expiration_time = 0;
        }

        for (int64_t segment_id = 0; segment_id < rs_meta.num_segments(); segment_id++) {
            auto download_done = [&, tablet_id = rs_meta.tablet_id(),
                                  rowset_id = rs_meta.rowset_id().to_string(),
                                  segment_size = rs_meta.segment_file_size(segment_id),
                                  wait](Status st) {
                if (st.ok()) {
                    g_file_cache_event_driven_warm_up_finished_segment_num << 1;
                    g_file_cache_event_driven_warm_up_finished_segment_size << segment_size;
                    int64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                             std::chrono::system_clock::now().time_since_epoch())
                                             .count();
                    g_file_cache_warm_up_rowset_last_finish_unix_ts.set_value(now_ts);
                    g_file_cache_warm_up_rowset_latency << (now_ts - request_ts);
                    g_file_cache_warm_up_rowset_handle_to_finish_latency << (now_ts - handle_ts);
                    if (request_ts > 0 &&
                        now_ts - request_ts > config::warm_up_rowset_slow_log_ms * 1000) {
                        g_file_cache_warm_up_rowset_slow_count << 1;
                        LOG(INFO) << "warm up rowset took " << now_ts - request_ts
                                  << " us, tablet_id: " << tablet_id << ", rowset_id: " << rowset_id
                                  << ", segment_id: " << segment_id;
                    }
                    if (now_ts - handle_ts > config::warm_up_rowset_slow_log_ms * 1000) {
                        g_file_cache_warm_up_rowset_handle_to_finish_slow_count << 1;
                        LOG(INFO) << "warm up rowset (handle to finish) took " << now_ts - handle_ts
                                  << " us, tablet_id: " << tablet_id << ", rowset_id: " << rowset_id
                                  << ", segment_id: " << segment_id;
                    }
                } else {
                    g_file_cache_event_driven_warm_up_failed_segment_num << 1;
                    g_file_cache_event_driven_warm_up_failed_segment_size << segment_size;
                    LOG(WARNING) << "download segment failed, tablet_id: " << tablet_id
                                 << " rowset_id: " << rowset_id << ", error: " << st;
                }
                if (wait) {
                    wait->signal();
                }
            };

            io::DownloadFileMeta download_meta {
                    .path = storage_resource.value()->remote_segment_path(rs_meta, segment_id),
                    .file_size = rs_meta.segment_file_size(segment_id),
                    .offset = 0,
                    .download_size = rs_meta.segment_file_size(segment_id),
                    .file_system = storage_resource.value()->fs,
                    .ctx =
                            {
                                    .is_index_data = false,
                                    .expiration_time = expiration_time,
                                    .is_dryrun =
                                            config::enable_reader_dryrun_when_download_file_cache,
                            },
                    .download_done = std::move(download_done),
            };
            g_file_cache_event_driven_warm_up_submitted_segment_num << 1;
            g_file_cache_event_driven_warm_up_submitted_segment_size
                    << rs_meta.segment_file_size(segment_id);
            if (wait) {
                wait->add_count();
            }
            _engine.file_cache_block_downloader().submit_download_task(download_meta);

            auto download_inverted_index = [&](std::string index_path, uint64_t idx_size) {
                auto storage_resource = rs_meta.remote_storage_resource();
                auto download_done = [=, tablet_id = rs_meta.tablet_id(),
                                      rowset_id = rs_meta.rowset_id().to_string()](Status st) {
                    if (st.ok()) {
                        g_file_cache_event_driven_warm_up_finished_index_num << 1;
                        g_file_cache_event_driven_warm_up_finished_index_size << idx_size;
                        int64_t now_ts =
                                std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();
                        g_file_cache_warm_up_rowset_last_finish_unix_ts.set_value(now_ts);
                        g_file_cache_warm_up_rowset_latency << (now_ts - request_ts);
                        g_file_cache_warm_up_rowset_handle_to_finish_latency
                                << (now_ts - handle_ts);
                        if (request_ts > 0 &&
                            now_ts - request_ts > config::warm_up_rowset_slow_log_ms * 1000) {
                            g_file_cache_warm_up_rowset_slow_count << 1;
                            LOG(INFO) << "warm up rowset took " << now_ts - request_ts
                                      << " us, tablet_id: " << tablet_id
                                      << ", rowset_id: " << rowset_id
                                      << ", segment_id: " << segment_id;
                        }
                        if (now_ts - handle_ts > config::warm_up_rowset_slow_log_ms * 1000) {
                            g_file_cache_warm_up_rowset_handle_to_finish_slow_count << 1;
                            LOG(INFO) << "warm up rowset (handle to finish) took "
                                      << now_ts - handle_ts << " us, tablet_id: " << tablet_id
                                      << ", rowset_id: " << rowset_id
                                      << ", segment_id: " << segment_id;
                        }
                    } else {
                        g_file_cache_event_driven_warm_up_failed_index_num << 1;
                        g_file_cache_event_driven_warm_up_failed_index_size << idx_size;
                        LOG(WARNING) << "download inverted index failed, tablet_id: " << tablet_id
                                     << " rowset_id: " << rowset_id << ", error: " << st;
                    }
                    if (wait) {
                        wait->signal();
                    }
                };
                io::DownloadFileMeta download_meta {
                        .path = io::Path(index_path),
                        .file_size = static_cast<int64_t>(idx_size),
                        .file_system = storage_resource.value()->fs,
                        .ctx =
                                {
                                        .is_index_data = false, // DORIS-20877
                                        .expiration_time = expiration_time,
                                        .is_dryrun = config::
                                                enable_reader_dryrun_when_download_file_cache,
                                },
                        .download_done = std::move(download_done),
                };
                g_file_cache_event_driven_warm_up_submitted_index_num << 1;
                g_file_cache_event_driven_warm_up_submitted_index_size << idx_size;

                if (wait) {
                    wait->add_count();
                }
                _engine.file_cache_block_downloader().submit_download_task(download_meta);
            };

            // inverted index
            auto schema_ptr = rs_meta.tablet_schema();
            auto idx_version = schema_ptr->get_inverted_index_storage_format();
            bool has_inverted_index = schema_ptr->has_inverted_index();

            if (has_inverted_index) {
                if (idx_version == InvertedIndexStorageFormatPB::V1) {
                    auto&& inverted_index_info = rs_meta.inverted_index_file_info(segment_id);
                    std::unordered_map<int64_t, int64_t> index_size_map;
                    for (const auto& info : inverted_index_info.index_info()) {
                        if (info.index_file_size() != -1) {
                            index_size_map[info.index_id()] = info.index_file_size();
                        } else {
                            VLOG_DEBUG << "Invalid index_file_size for segment_id " << segment_id
                                       << ", index_id " << info.index_id();
                        }
                    }
                    for (const auto& index : schema_ptr->inverted_indexes()) {
                        auto idx_path = storage_resource.value()->remote_idx_v1_path(
                                rs_meta, segment_id, index->index_id(), index->get_index_suffix());
                        download_inverted_index(idx_path, index_size_map[index->index_id()]);
                    }
                } else { // InvertedIndexStorageFormatPB::V2
                    auto&& inverted_index_info = rs_meta.inverted_index_file_info(segment_id);
                    int64_t idx_size = 0;
                    if (inverted_index_info.has_index_size()) {
                        idx_size = inverted_index_info.index_size();
                    } else {
                        VLOG_DEBUG << "index_size is not set for segment " << segment_id;
                    }
                    auto idx_path =
                            storage_resource.value()->remote_idx_v2_path(rs_meta, segment_id);
                    download_inverted_index(idx_path, idx_size);
                }
            }
        }
    }
    if (wait && wait->timed_wait(due_time)) {
        g_file_cache_warm_up_rowset_wait_for_compaction_timeout_num << 1;
        LOG_WARNING("the time spent warming up {} rowsets exceeded {} ms",
                    request->rowset_metas().size(), request->sync_wait_timeout_ms());
    }
}

bvar::Adder<uint64_t> g_file_cache_recycle_cache_finished_segment_num(
        "file_cache_recycle_cache_finished_segment_num");
bvar::Adder<uint64_t> g_file_cache_recycle_cache_finished_index_num(
        "file_cache_recycle_cache_finished_index_num");

void CloudInternalServiceImpl::recycle_cache(google::protobuf::RpcController* controller
                                             [[maybe_unused]],
                                             const PRecycleCacheRequest* request,
                                             PRecycleCacheResponse* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);

    if (!config::enable_file_cache) {
        return;
    }
    for (const auto& meta : request->cache_metas()) {
        for (int64_t segment_id = 0; segment_id < meta.num_segments(); segment_id++) {
            auto file_key = Segment::file_cache_key(meta.rowset_id(), segment_id);
            auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached_async(file_key);
            g_file_cache_recycle_cache_finished_segment_num << 1;
        }

        // inverted index
        for (const auto& file_name : meta.index_file_names()) {
            auto file_key = io::BlockFileCache::hash(file_name);
            auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached_async(file_key);
            g_file_cache_recycle_cache_finished_index_num << 1;
        }
    }
}

} // namespace doris
