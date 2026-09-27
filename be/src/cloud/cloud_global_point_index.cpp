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

#include "cloud/cloud_global_point_index.h"

#include <atomic>
#include <memory>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/config.h"
#include "common/status.h"
#include "io/io_common.h"
#include "runtime/thread_context.h"
#include "storage/index/global_point/global_point_index_reader.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet/base_tablet.h"
#include "util/threadpool.h"

namespace doris {

namespace {

// Outcome of one tablet, shared by the concurrent tasks that read its rowset blooms.
struct TabletPruneState {
    int64_t tablet_id;
    std::vector<RowsetSharedPtr> rowsets; // only rowsets with rows
    // Set by the first rowset that decides the tablet (a hit, or a bloom that cannot be used);
    // the remaining rowsets of the tablet are then skipped.
    std::atomic<bool> decided {false};
    std::atomic<bool> any_hit {false};
    std::atomic<bool> any_degraded {false};
    std::atomic<int64_t> probed_blooms {0};
};

// Shared by all prune requests on this BE, so global_point_index_prune_io_max_threads bounds the
// total number of concurrent .gpidx reads. The RPC itself runs on the light work pool, which must
// not block on remote IO.
ThreadPool* prune_io_pool() {
    static std::unique_ptr<ThreadPool> pool = [] {
        std::unique_ptr<ThreadPool> p;
        Status st = ThreadPoolBuilder("gp_idx_prune_io")
                            .set_min_threads(0)
                            .set_max_threads(config::global_point_index_prune_io_max_threads)
                            .set_max_queue_size(10240)
                            .build(&p);
        if (!st.ok()) {
            LOG(FATAL) << "failed to create gp_idx_prune_io thread pool: " << st;
        }
        return p;
    }();
    return pool.get();
}

void mark_decided(TabletPruneState* state, bool hit, bool degraded) {
    if (hit) {
        state->any_hit.store(true, std::memory_order_release);
    }
    if (degraded) {
        state->any_degraded.store(true, std::memory_order_release);
    }
    state->decided.store(true, std::memory_order_release);
}

// Tests one rowset's bloom and records the outcome in `state`. A clean miss decides nothing: the
// other rowsets of the tablet still have to be checked.
void probe_rowset(TabletPruneState* state, const RowsetSharedPtr& rowset, int32_t col_unique_id,
                  const PGlobalPointIndexPruneRequest& request) {
    const ColumnPointIndexPB* desc = nullptr;
    for (const auto& d : rowset->rowset_meta()->point_query_indexes()) {
        if (d.column_unique_id() == col_unique_id) {
            desc = &d;
            break;
        }
    }
    if (desc == nullptr) {
        // The rowset has rows but no bloom (written before the index, or not rebuilt yet).
        mark_decided(state, false, true);
        return;
    }
    auto path_result = rowset->global_point_index_path(col_unique_id);
    if (!path_result.has_value()) {
        mark_decided(state, false, true);
        return;
    }

    std::unique_ptr<segment_v2::BloomFilter> bloom;
    int64_t bytes_read = 0;
    io::IOContext io_ctx;
    // Read through the INDEX queue of the file cache, like the scan-time gate.
    io_ctx.is_index_data = true;
    auto reader_opts = rowset->global_point_index_reader_options(*desc);
    Status st = segment_v2::try_load_global_point_index(rowset->rowset_meta()->fs(),
                                                        path_result.value(), *desc, &io_ctx, &bloom,
                                                        &bytes_read, &reader_opts);
    if (!st.ok() || bloom == nullptr) {
        mark_decided(state, false, true);
        return;
    }
    // A bloom that was never fed is a valid, all-zero file and would answer "absent" for every
    // value. If the rowset has rows but the descriptor recorded neither a value nor a null, do
    // not trust it.
    if (desc->total_rows() == 0 && !bloom->has_null() && rowset->rowset_meta()->num_rows() > 0) {
        mark_decided(state, false, true);
        return;
    }
    state->probed_blooms.fetch_add(1, std::memory_order_relaxed);
    for (const auto& probe_value : request.probe_values()) {
        if (bloom->test_bytes(probe_value.data(), probe_value.size())) {
            mark_decided(state, true, false);
            return;
        }
    }
}

} // namespace

void handle_global_point_index_prune(CloudStorageEngine& engine,
                                     const PGlobalPointIndexPruneRequest& request,
                                     PGlobalPointIndexPruneResponse* response) {
    if (!request.has_table_id() || !request.has_column_unique_id() ||
        request.probe_values_size() == 0 || request.tablets_size() == 0) {
        Status::InvalidArgument("missing params table_id/column_unique_id/probe_values/tablets")
                .to_protobuf(response->mutable_status());
        return;
    }
    const int32_t col_unique_id = request.column_unique_id();
    int64_t degraded_tablets = 0;

    // Pass 1, in memory only: the visible rowsets of each tablet at its snapshot version.
    std::vector<std::unique_ptr<TabletPruneState>> states;
    states.reserve(request.tablets_size());
    for (const auto& gp_tablet : request.tablets()) {
        if (!gp_tablet.has_tablet_id()) {
            continue;
        }
        const int64_t tablet_id = gp_tablet.tablet_id();
        auto keep = [&]() {
            response->add_candidate_tablet_ids(tablet_id);
            degraded_tablets++;
        };
        if (!gp_tablet.has_snapshot_version()) {
            keep();
            continue;
        }
        // Never sync from the meta service here: an uncached tablet is simply kept.
        auto tablet = engine.tablet_mgr().get_tablet_if_cached(tablet_id);
        if (tablet == nullptr) {
            keep();
            continue;
        }
        // The default options capture exactly [0, snapshot_version], and fail if the cached
        // rowsets have not reached it, instead of returning an older, smaller set.
        Result<CaptureRowsetResult> captured;
        {
            std::shared_lock lock(tablet->get_header_lock());
            captured = tablet->capture_consistent_rowsets_unlocked(
                    {0, gp_tablet.snapshot_version()}, CaptureRowsetOps {});
        }
        if (!captured) {
            keep();
            continue;
        }
        auto state = std::make_unique<TabletPruneState>();
        state->tablet_id = tablet_id;
        for (auto& rowset : captured.value().rowsets) {
            // Empty rowsets, such as the initial [0-1] rowset, cannot contain the value.
            if (rowset->rowset_meta()->num_rows() > 0) {
                state->rowsets.push_back(rowset);
            }
        }
        states.push_back(std::move(state));
    }

    // Pass 2: read and test every (tablet, rowset) bloom concurrently.
    auto token = prune_io_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    for (auto& state : states) {
        TabletPruneState* state_ptr = state.get();
        for (const auto& rowset : state_ptr->rowsets) {
            Status submit_st = token->submit_func([state_ptr, rowset, col_unique_id, &request]() {
                if (state_ptr->decided.load(std::memory_order_acquire)) {
                    return;
                }
                SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
                probe_rowset(state_ptr, rowset, col_unique_id, request);
            });
            if (!submit_st.ok()) {
                // This bloom will never be tested, so the tablet must be kept.
                mark_decided(state_ptr, false, true);
            }
        }
    }
    token->wait();

    // Pass 3: a tablet is a candidate unless every bloom was tested and none matched.
    int64_t probed_blooms = 0;
    for (const auto& state : states) {
        probed_blooms += state->probed_blooms.load(std::memory_order_relaxed);
        bool any_hit = state->any_hit.load(std::memory_order_relaxed);
        bool any_degraded = state->any_degraded.load(std::memory_order_relaxed);
        if (any_hit || any_degraded) {
            response->add_candidate_tablet_ids(state->tablet_id);
            if (any_degraded) {
                degraded_tablets++;
            }
        }
    }

    response->set_probed_blooms(probed_blooms);
    response->set_degraded_tablets(degraded_tablets);
    Status::OK().to_protobuf(response->mutable_status());
}

} // namespace doris
