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

#include <bvar/bvar.h>

#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/config.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/time.h"

namespace doris {
namespace {
bvar::Adder<int64_t> ttl_discovered("row_binlog_ttl_discovered");
bvar::Adder<int64_t> ttl_submitted("row_binlog_ttl_submitted");
bvar::Adder<int64_t> ttl_failed("row_binlog_ttl_failed");
constexpr int kScanBatchSize = 64;

Status prepare_row_binlog_ttl(BaseStorageEngine& engine, int64_t tablet_id, bool refresh_cloud_meta,
                              bool is_cloud) {
    // Cloud catalog discovery and already queued work must also honor runtime switches.
    if (!config::enable_feature_binlog || config::disable_auto_compaction) {
        return Status::OK();
    }
    auto tablet = DORIS_TRY(engine.get_tablet(tablet_id));
    if (is_cloud && refresh_cloud_meta) {
        auto cloud_tablet = std::static_pointer_cast<CloudTablet>(tablet);
        RETURN_IF_ERROR(cloud_tablet->sync_meta());
        RETURN_IF_ERROR(cloud_tablet->sync_rowsets());
    }
    if (tablet->tablet_state() != TABLET_RUNNING || !tablet->scan_expired_row_binlog_rowsets()) {
        return Status::OK();
    }
    ttl_discovered << 1;
    const auto last_failure =
            is_cloud
                    ? std::static_pointer_cast<CloudTablet>(tablet)
                              ->last_cumu_compaction_failure_time()
                    : std::static_pointer_cast<Tablet>(tablet)->last_cumu_compaction_failure_time();
    if (UnixMillis() - last_failure < config::min_compaction_failure_interval_ms) {
        return Status::OK();
    }
    if (is_cloud) {
        auto cloud_tablet = std::static_pointer_cast<CloudTablet>(tablet);
        auto* cluster = static_cast<CloudClusterInfo*>(ExecEnv::GetInstance()->cluster_info());
        if (cluster->should_skip_compaction(cloud_tablet.get()) || cluster->is_in_standby()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(engine.to_cloud().submit_compaction_task(
                cloud_tablet, CompactionType::CUMU_BINLOG_COMPACTION));
    } else {
        RETURN_IF_ERROR(engine.to_local().submit_compaction_task(
                std::static_pointer_cast<Tablet>(tablet), CompactionType::CUMU_BINLOG_COMPACTION,
                false));
    }
    ttl_submitted << 1;
    return Status::OK();
}
} // namespace

void BaseStorageEngine::register_row_binlog_tablet(const BaseTabletSPtr& tablet) {
    if (tablet->is_row_binlog_tablet()) {
        std::lock_guard lock(_row_binlog_ttl_mutex);
        _row_binlog_ttl_tablets[tablet->tablet_id()] = tablet;
    }
}

Status BaseStorageEngine::submit_row_binlog_ttl(int64_t tablet_id, bool refresh_cloud_meta) {
    if (ExecEnv::GetInstance()->cluster_info()->row_binlog_ttl_reference_tso() <= 0) {
        return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>("No valid ROW binlog TTL reference");
    }
    std::lock_guard lock(_row_binlog_ttl_mutex);
    if (!_row_binlog_ttl_prepare_pool || _stop_background_threads_latch.count() == 0) {
        return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                "ROW binlog TTL scanner is not running");
    }
    if (!_row_binlog_ttl_pending.insert(tablet_id).second) {
        return Status::AlreadyExist("ROW binlog TTL task already queued for {}", tablet_id);
    }
    auto st = _row_binlog_ttl_prepare_pool->submit_func([this, tablet_id, refresh_cloud_meta]() {
        SCOPED_INIT_THREAD_CONTEXT();
        Defer clear_pending([this, tablet_id] {
            std::lock_guard lock(_row_binlog_ttl_mutex);
            _row_binlog_ttl_pending.erase(tablet_id);
        });
        auto result =
                prepare_row_binlog_ttl(*this, tablet_id, refresh_cloud_meta, _type == Type::CLOUD);
        if (!result.ok()) {
            ttl_failed << 1;
            LOG_EVERY_N(WARNING, 100)
                    << "ROW binlog TTL task failed, tablet=" << tablet_id << ", status=" << result;
        }
    });
    if (!st.ok()) {
        _row_binlog_ttl_pending.erase(tablet_id);
    }
    return st;
}

Status BaseStorageEngine::_start_row_binlog_ttl_scanner() {
    RETURN_IF_ERROR(ThreadPoolBuilder("RowBinlogTtlPrepare")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .set_max_queue_size(kScanBatchSize)
                            .build(&_row_binlog_ttl_prepare_pool));
    return Thread::create(
            "StorageEngine", "row_binlog_ttl_scanner",
            [this] {
                SCOPED_INIT_THREAD_CONTEXT();
                int64_t cursor = 0;
                do {
                    if (!config::enable_feature_binlog || config::disable_auto_compaction ||
                        ExecEnv::GetInstance()->cluster_info()->row_binlog_ttl_reference_tso() <=
                                0) {
                        continue;
                    }
                    std::vector<int64_t> batch;
                    {
                        std::lock_guard lock(_row_binlog_ttl_mutex);
                        auto it = _row_binlog_ttl_tablets.upper_bound(cursor);
                        for (int n = 0; n < kScanBatchSize && it != _row_binlog_ttl_tablets.end();
                             ++n) {
                            if (it->second.expired()) {
                                it = _row_binlog_ttl_tablets.erase(it);
                            } else {
                                batch.push_back(it->first);
                                ++it;
                            }
                        }
                        if (batch.empty() && it == _row_binlog_ttl_tablets.end()) {
                            cursor = 0;
                        }
                    }
                    for (auto tablet_id : batch) {
                        auto st = submit_row_binlog_ttl(tablet_id);
                        if (!st.ok() && !st.is<ErrorCode::ALREADY_EXIST>()) {
                            ttl_failed << 1;
                            break; // Resume here next round; a full queue must not starve the tail.
                        }
                        cursor = tablet_id;
                    }
                } while (!_stop_background_threads_latch.wait_for(std::chrono::milliseconds(1000)));
            },
            &_row_binlog_ttl_scan_thread);
}

void BaseStorageEngine::_stop_row_binlog_ttl_scanner() {
    if (_row_binlog_ttl_scan_thread) {
        _row_binlog_ttl_scan_thread->join();
    }
    if (_row_binlog_ttl_prepare_pool) {
        _row_binlog_ttl_prepare_pool->shutdown();
    }
}
} // namespace doris
