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
#include <mutex>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/io_common.h"
#include "runtime/thread_context.h"
#include "storage/index/global_point/global_point_index_reader.h"
#include "storage/index/global_point/global_point_index_writer.h"
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

// Separate from the prune pool: warm-up is bulk IO right after a BE starts, and must not delay
// prune requests, which are on the planning path of queries. The queue holds a whole BE's tablets
// (the FE sends one task per tablet); a full queue is reported back as rejected tablets.
ThreadPool* warm_up_io_pool() {
    static std::unique_ptr<ThreadPool> pool = [] {
        std::unique_ptr<ThreadPool> p;
        Status st = ThreadPoolBuilder("gp_idx_warmup_io")
                            .set_min_threads(0)
                            .set_max_threads(config::global_point_index_warmup_io_max_threads)
                            .set_max_queue_size(65536)
                            .build(&p);
        if (!st.ok()) {
            LOG(FATAL) << "failed to create gp_idx_warmup_io thread pool: " << st;
        }
        return p;
    }();
    return pool.get();
}

// Totals of one warm-up request, updated concurrently by its tablet tasks.
struct SweepStats {
    std::atomic<int64_t> checked {0};
    std::atomic<int64_t> resident {0};
    std::atomic<int64_t> repaired {0};
    std::atomic<int64_t> failed {0};
    std::atomic<int64_t> blooms_seen {0};
    std::atomic<int64_t> blooms_undersized {0};
    std::atomic<int64_t> blooms_empty {0};
    std::atomic<int64_t> blooms_missing {0};
};

// Totals of the most recently completed sweep, reported by the next warm-up request.
struct PrevSweepStats {
    std::mutex mtx;
    bool has_value = false;
    PGpIdxWarmUpResponse values;
};

PrevSweepStats& prev_sweep_stats() {
    static PrevSweepStats stats;
    return stats;
}

// Called exactly once per request, by whoever finishes last, so the cumulative metrics are not
// counted twice.
void publish_sweep(const SweepStats& sweep) {
    int64_t checked = sweep.checked.load(std::memory_order_relaxed);
    int64_t blooms_seen = sweep.blooms_seen.load(std::memory_order_relaxed);
    // An empty sweep would overwrite the previous numbers with zeros. The sizing check counts
    // descriptors even where nothing is cacheable, so either counter makes the sweep real.
    if (checked == 0 && blooms_seen == 0) {
        return;
    }
    PGpIdxWarmUpResponse values;
    values.set_prev_sweep_checked(checked);
    values.set_prev_sweep_resident(sweep.resident.load(std::memory_order_relaxed));
    values.set_prev_sweep_repaired(sweep.repaired.load(std::memory_order_relaxed));
    values.set_prev_sweep_failed(sweep.failed.load(std::memory_order_relaxed));
    values.set_prev_sweep_blooms_seen(blooms_seen);
    values.set_prev_sweep_blooms_undersized(
            sweep.blooms_undersized.load(std::memory_order_relaxed));
    values.set_prev_sweep_blooms_empty(sweep.blooms_empty.load(std::memory_order_relaxed));
    values.set_prev_sweep_blooms_missing(sweep.blooms_missing.load(std::memory_order_relaxed));
    {
        PrevSweepStats& prev = prev_sweep_stats();
        std::lock_guard<std::mutex> lock(prev.mtx);
        prev.has_value = true;
        prev.values = values;
    }
    auto* metrics = DorisMetrics::instance();
    metrics->global_point_index_warmup_checked_total->increment(values.prev_sweep_checked());
    metrics->global_point_index_warmup_resident_total->increment(values.prev_sweep_resident());
    metrics->global_point_index_warmup_repaired_total->increment(values.prev_sweep_repaired());
    metrics->global_point_index_warmup_failed_total->increment(values.prev_sweep_failed());
    metrics->global_point_index_blooms_undersized_total->increment(
            values.prev_sweep_blooms_undersized());
    metrics->global_point_index_blooms_empty_total->increment(values.prev_sweep_blooms_empty());
    metrics->global_point_index_blooms_missing_total->increment(values.prev_sweep_blooms_missing());
}

enum class Residency { MISSING, WRONG_QUEUE, RESIDENT };

// Residency of one .gpidx file, from the cache's in-memory block map only. probe() neither creates
// cache cells nor touches LRU state.
Residency probe_residency(io::BlockFileCache* file_cache, const io::UInt128Wrapper& hash,
                          int64_t file_size, std::vector<io::FileBlockSPtr>* blocks) {
    io::IOContext io_ctx;
    io_ctx.is_index_data = true;
    io::CacheContext cache_ctx(&io_ctx);
    auto probe = file_cache->probe(hash, 0, static_cast<size_t>(file_size), cache_ctx);
    bool wrong_queue = false;
    for (const auto& block : probe.file_blocks) {
        if (block == nullptr || block->state() != io::FileBlock::State::DOWNLOADED) {
            return Residency::MISSING;
        }
        // TTL blocks (a TTL table's compaction output) are protected as well; leave them.
        if (block->cache_type() == io::FileCacheType::NORMAL ||
            block->cache_type() == io::FileCacheType::DISPOSABLE) {
            wrong_queue = true;
        }
    }
    *blocks = std::move(probe.file_blocks);
    return wrong_queue ? Residency::WRONG_QUEUE : Residency::RESIDENT;
}

void warm_up_tablet(CloudStorageEngine& engine, int64_t tablet_id, bool is_repair_sweep,
                    SweepStats* stats) {
    SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
    // Unlike pruning, warm-up may load the tablet from the meta service: the BE just started.
    auto maybe_tablet = engine.tablet_mgr().get_tablet(tablet_id, /*warmup_data=*/false,
                                                       /*sync_delete_bitmap=*/false);
    if (!maybe_tablet) {
        LOG(WARNING) << "GLOBAL_POINT warm-up: get_tablet failed, tablet_id=" << tablet_id << ": "
                     << maybe_tablet.error();
        return;
    }
    auto tablet = maybe_tablet.value();
    if (!is_repair_sweep) {
        Status sync_st = tablet->sync_rowsets();
        if (!sync_st.ok()) {
            // Warm what is known; warm-up never affects correctness.
            LOG(WARNING) << "GLOBAL_POINT warm-up: sync_rowsets failed, tablet_id=" << tablet_id
                         << ": " << sync_st;
        }
    }
    std::vector<RowsetSharedPtr> rowsets;
    {
        std::shared_lock lock(tablet->get_header_lock());
        auto captured = tablet->capture_consistent_rowsets_unlocked(
                {0, tablet->max_version_unlocked()}, CaptureRowsetOps {});
        if (!captured) {
            LOG(WARNING) << "GLOBAL_POINT warm-up: capture rowsets failed, tablet_id=" << tablet_id;
            return;
        }
        rowsets = std::move(captured.value().rowsets);
    }
    // The current schema, because "missing" means a rowset that should have a bloom by now.
    TabletSchemaSPtr tablet_schema = tablet->tablet_schema();
    for (const auto& rowset : rowsets) {
        if (rowset->rowset_meta()->num_rows() == 0) {
            continue;
        }
        for (const auto* tablet_index : tablet_schema->global_point_indexes()) {
            if (tablet_index->col_unique_ids().empty()) {
                continue;
            }
            int32_t col_unique_id = tablet_index->col_unique_ids()[0];
            bool present = false;
            for (const auto& d : rowset->rowset_meta()->point_query_indexes()) {
                present = present || d.column_unique_id() == col_unique_id;
            }
            if (!present) {
                stats->blooms_missing.fetch_add(1, std::memory_order_relaxed);
            }
        }
        for (const auto& desc : rowset->rowset_meta()->point_query_indexes()) {
            // The sizing check only reads the descriptor, so it runs whether or not the file can
            // be cached. A saturated bloom is otherwise invisible: queries stay correct and
            // nothing is reported as degraded.
            stats->blooms_seen.fetch_add(1, std::memory_order_relaxed);
            switch (segment_v2::check_global_point_index_health(
                    desc.size(), desc.total_rows(), desc.fpp(),
                    config::global_point_index_bloom_size_slack_percent)) {
            case segment_v2::GlobalPointIndexHealth::UNDERSIZED:
                stats->blooms_undersized.fetch_add(1, std::memory_order_relaxed);
                break;
            case segment_v2::GlobalPointIndexHealth::EMPTY:
                stats->blooms_empty.fetch_add(1, std::memory_order_relaxed);
                break;
            case segment_v2::GlobalPointIndexHealth::OK:
                break;
            }

            auto path_result = rowset->global_point_index_path(desc.column_unique_id());
            if (!path_result.has_value()) {
                continue;
            }
            auto reader_opts = rowset->global_point_index_reader_options(desc);
            if (reader_opts.cache_type != io::FileCachePolicy::FILE_BLOCK_CACHE ||
                reader_opts.file_size <= 0) {
                continue;
            }
            stats->checked.fetch_add(1, std::memory_order_relaxed);

            auto hash = io::BlockFileCache::hash(io::Path(path_result.value()).filename().native());
            io::BlockFileCache* file_cache = io::FileCacheFactory::instance()->get_by_path(hash);
            std::vector<io::FileBlockSPtr> blocks;
            Residency residency = probe_residency(file_cache, hash, reader_opts.file_size, &blocks);
            if (residency == Residency::RESIDENT) {
                stats->resident.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            if (residency == Residency::WRONG_QUEUE) {
                // Already downloaded: move the blocks instead of reading them again.
                bool ok = true;
                for (const auto& block : blocks) {
                    if (block->cache_type() == io::FileCacheType::NORMAL ||
                        block->cache_type() == io::FileCacheType::DISPOSABLE) {
                        Status st = block->change_cache_type(io::FileCacheType::INDEX);
                        if (!st.ok()) {
                            LOG(WARNING)
                                    << "GLOBAL_POINT warm-up: moving a block to the INDEX "
                                    << "queue failed, path=" << path_result.value() << ": " << st;
                            ok = false;
                        }
                    }
                }
                (ok ? stats->repaired : stats->failed).fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            // Not cached: the only case that reads remote storage. Reading through the cached
            // reader fills the cache (INDEX queue, like the read paths) and validates the file.
            io::IOContext io_ctx;
            io_ctx.is_index_data = true;
            io_ctx.is_warmup = true;
            std::unique_ptr<segment_v2::BloomFilter> bloom;
            int64_t bytes_read = 0;
            Status st = segment_v2::try_load_global_point_index(rowset->rowset_meta()->fs(),
                                                                path_result.value(), desc, &io_ctx,
                                                                &bloom, &bytes_read, &reader_opts);
            (st.ok() && bloom != nullptr ? stats->repaired : stats->failed)
                    .fetch_add(1, std::memory_order_relaxed);
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

void handle_global_point_index_warm_up(CloudStorageEngine& engine,
                                       const PGpIdxWarmUpRequest& request,
                                       PGpIdxWarmUpResponse* response) {
    {
        PrevSweepStats& prev = prev_sweep_stats();
        std::lock_guard<std::mutex> lock(prev.mtx);
        if (prev.has_value) {
            response->MergeFrom(prev.values);
        }
    }
    const bool is_repair_sweep = request.is_repair_sweep();
    auto sweep_stats = std::make_shared<SweepStats>();
    // Starts at 1: this loop holds a reference until it has queued every task. Otherwise tasks
    // that finish while the loop is still queuing could bring it to 0 and publish a partial sweep
    // more than once.
    auto remaining = std::make_shared<std::atomic<int64_t>>(1);
    int64_t accepted = 0;
    int64_t rejected = 0;
    for (int64_t tablet_id : request.tablet_ids()) {
        remaining->fetch_add(1, std::memory_order_relaxed);
        Status submit_st = warm_up_io_pool()->submit_func(
                [&engine, tablet_id, is_repair_sweep, sweep_stats, remaining]() {
                    warm_up_tablet(engine, tablet_id, is_repair_sweep, sweep_stats.get());
                    if (remaining->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                        publish_sweep(*sweep_stats);
                    }
                });
        if (submit_st.ok()) {
            accepted++;
        } else {
            rejected++;
            remaining->fetch_sub(1, std::memory_order_acq_rel);
        }
    }
    if (remaining->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        publish_sweep(*sweep_stats);
    }
    if (rejected > 0) {
        LOG(WARNING) << "GLOBAL_POINT warm-up: queue full, rejected " << rejected << "/"
                     << request.tablet_ids_size() << " tablets";
    }
    response->set_accepted_tablets(accepted);
    response->set_rejected_tablets(rejected);
    Status::OK().to_protobuf(response->mutable_status());
}

} // namespace doris
