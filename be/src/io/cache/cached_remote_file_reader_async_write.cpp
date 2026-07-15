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
#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "io/cache/async_cache_write_service.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "util/time.h"

namespace doris::io {

// These counters are shared by the synchronous and asynchronous indirect-read paths, so their
// definitions remain in cached_remote_file_reader.cpp.
extern bvar::Adder<uint64_t> g_read_cache_indirect_num;
extern bvar::Adder<uint64_t> g_read_cache_indirect_bytes;
extern bvar::Adder<uint64_t> g_read_cache_indirect_total_bytes;
extern bvar::Adder<uint64_t> g_read_cache_self_heal_on_not_found;

namespace {

bvar::Adder<uint64_t> g_cached_remote_reader_probe_total("cached_remote_file_reader_probe_total");
bvar::Adder<uint64_t> g_cached_remote_reader_probe_downloaded(
        "cached_remote_file_reader_probe_hit_downloaded_total");
bvar::Adder<uint64_t> g_cached_remote_reader_probe_downloading(
        "cached_remote_file_reader_probe_hit_downloading_total");
bvar::Adder<uint64_t> g_cached_remote_reader_probe_miss(
        "cached_remote_file_reader_probe_miss_total");
bvar::Adder<uint64_t> g_cached_remote_reader_inflight_hit(
        "cached_remote_file_reader_inflight_write_buffer_hit_total");
bvar::Adder<uint64_t> g_cached_remote_reader_async_skip_existing(
        "cached_remote_file_reader_async_write_skip_inflight_existing_total");
bvar::Adder<uint64_t> g_cached_remote_reader_block_wait(
        "cached_remote_file_reader_block_wait_total");
bvar::Adder<uint64_t> g_cached_remote_reader_block_wait_timeout(
        "cached_remote_file_reader_block_wait_timeout_total");
bvar::Adder<uint64_t> g_cached_remote_reader_remote_after_dedup_miss(
        "cached_remote_file_reader_remote_read_after_all_dedup_miss_total");
bvar::Adder<uint64_t> g_cached_remote_reader_middle_span_read_bytes(
        "cached_remote_file_reader_middle_span_read_bytes");
bvar::Adder<uint64_t> g_cached_remote_reader_middle_span_miss_bytes(
        "cached_remote_file_reader_middle_span_miss_bytes");

} // namespace

// One block-aligned ownership decision. Only MISS entries can create a new async write task.
struct CachedRemoteFileReader::AsyncBlockCoverage {
    enum class Source {
        INFLIGHT,
        DOWNLOADED,
        DOWNLOADING,
        MISS,
    };

    explicit AsyncBlockCoverage(FileBlock::Range range_) : range(range_) {}

    FileBlock::Range range;
    Source source {Source::MISS};
    std::shared_ptr<InflightWriteBufferEntry> inflight_entry;
    std::vector<FileBlockSPtr> probe_blocks;
};

// Probe holders must outlive copied block references. Declaration order makes coverage destruct
// first, before FileBlocksHolder performs EMPTY/deleting cleanup.
struct CachedRemoteFileReader::AsyncReadPlan {
    uint64_t write_epoch {0};
    size_t user_left {0};
    size_t user_right {0};
    std::vector<FileBlocksProbeResult> probe_results;
    std::vector<AsyncBlockCoverage> coverage;
};

// The one aligned remote GET left after consuming the maximum covered prefix and suffix.
struct CachedRemoteFileReader::AsyncRemoteRange {
    size_t left {0};
    size_t size {0};
};

// Resolve mode on every read so online configuration changes affect existing readers.
CacheWriteMode CachedRemoteFileReader::_resolve_cache_write_mode(const IOContext* io_ctx) const {
    if (io_ctx->is_dryrun || io_ctx->is_warmup || _should_read_from_peer(io_ctx)) {
        return CacheWriteMode::SYNC_WRITE;
    }
    if (io_ctx->cache_write_mode_override.has_value()) {
        return *io_ctx->cache_write_mode_override;
    }
    if (_cache_write_mode != CacheWriteMode::DEFAULT) {
        return _cache_write_mode;
    }
    return config::enable_async_file_cache_write ? CacheWriteMode::ASYNC_WRITE
                                                 : CacheWriteMode::SYNC_WRITE;
}

// Build per-block coverage with the required priority: inflight memory first, then read-only
// probing only for consecutive runs that remain uncovered.
void CachedRemoteFileReader::_build_async_read_plan(size_t remaining_offset, size_t remaining_size,
                                                    uint64_t write_epoch, const IOContext* io_ctx,
                                                    ReadStatistics& stats, AsyncReadPlan* plan) {
    DORIS_CHECK(plan != nullptr);
    DORIS_CHECK(plan->probe_results.empty());
    DORIS_CHECK(plan->coverage.empty());

    plan->write_epoch = write_epoch;
    plan->user_left = remaining_offset;
    plan->user_right = remaining_offset + remaining_size - 1;

    const auto [align_left, align_size] = s_align_size(remaining_offset, remaining_size, size());
    const size_t cache_block_size = static_cast<size_t>(config::file_cache_each_block_size);
    DORIS_CHECK(cache_block_size > 0);

    std::vector<size_t> block_offsets;
    const size_t align_end = align_left + align_size;
    for (size_t block_offset = align_left; block_offset < align_end;
         block_offset += cache_block_size) {
        const size_t block_size = std::min(cache_block_size, size() - block_offset);
        DORIS_CHECK(block_size > 0);
        plan->coverage.emplace_back(FileBlock::Range(block_offset, block_offset + block_size - 1));
        block_offsets.emplace_back(block_offset);
    }

    auto* inflight_index = _cache->inflight_write_buffer_index();
    DORIS_CHECK(inflight_index != nullptr);
    if (config::enable_inflight_write_buffer_index) {
        auto lookup_results = inflight_index->lookup_all(_cache_hash, block_offsets, write_epoch);
        DORIS_CHECK(lookup_results.size() == plan->coverage.size());
        for (size_t index = 0; index < lookup_results.size(); ++index) {
            auto& lookup_result = lookup_results[index];
            if (!lookup_result.entry) {
                ++stats.inflight_write_buffer_index_miss;
                continue;
            }

            auto& coverage = plan->coverage[index];
            DORIS_CHECK(lookup_result.entry->buffer != nullptr);
            DORIS_CHECK(lookup_result.entry->buffer_offset <= coverage.range.left);
            DORIS_CHECK(lookup_result.entry->buffer_offset + lookup_result.entry->buffer_size >
                        coverage.range.right);
            coverage.source = AsyncBlockCoverage::Source::INFLIGHT;
            coverage.inflight_entry = std::move(lookup_result.entry);
            ++stats.inflight_write_buffer_index_hit;
            g_cached_remote_reader_inflight_hit << 1;
        }
    }

    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    size_t index = 0;
    while (index < plan->coverage.size()) {
        while (index < plan->coverage.size() &&
               plan->coverage[index].source == AsyncBlockCoverage::Source::INFLIGHT) {
            ++index;
        }
        if (index == plan->coverage.size()) {
            break;
        }

        const size_t begin_index = index;
        while (index < plan->coverage.size() &&
               plan->coverage[index].source != AsyncBlockCoverage::Source::INFLIGHT) {
            ++index;
        }
        const size_t end_index = index;
        const size_t probe_left = plan->coverage[begin_index].range.left;
        const size_t probe_right = plan->coverage[end_index - 1].range.right;
        plan->probe_results.emplace_back(_cache->probe(
                _cache_hash, probe_left, probe_right - probe_left + 1, cache_context));
        g_cached_remote_reader_probe_total << 1;
        _classify_async_probe_result(plan, begin_index, end_index, plan->probe_results.back(),
                                     stats);
    }
}

// Classify a consecutive probe run with two monotonic cursors. Each cache block/gap is considered
// only while it can overlap the current aligned coverage entry.
void CachedRemoteFileReader::_classify_async_probe_result(AsyncReadPlan* plan, size_t begin_index,
                                                          size_t end_index,
                                                          const FileBlocksProbeResult& probe_result,
                                                          ReadStatistics& stats) {
    DORIS_CHECK(plan != nullptr);
    DORIS_CHECK(begin_index < end_index);
    DORIS_CHECK(end_index <= plan->coverage.size());

    const auto& cache_blocks = probe_result.holder.file_blocks;
    auto cache_block = cache_blocks.begin();
    size_t gap_index = 0;
    for (size_t index = begin_index; index < end_index; ++index) {
        auto& coverage = plan->coverage[index];
        DORIS_CHECK(coverage.source == AsyncBlockCoverage::Source::MISS);

        while (cache_block != cache_blocks.end() &&
               (*cache_block)->range().right < coverage.range.left) {
            ++cache_block;
        }
        while (gap_index < probe_result.gaps.size() &&
               probe_result.gaps[gap_index].right < coverage.range.left) {
            ++gap_index;
        }

        bool has_miss = gap_index < probe_result.gaps.size() &&
                        probe_result.gaps[gap_index].left <= coverage.range.right;
        bool has_downloading = false;
        auto overlapping_block = cache_block;
        while (overlapping_block != cache_blocks.end() &&
               (*overlapping_block)->range().left <= coverage.range.right) {
            const auto& block = *overlapping_block;
            coverage.probe_blocks.emplace_back(block);
            if (_cache->is_block_deleting(block)) {
                has_miss = true;
            } else {
                const FileBlock::State state = block->state();
                has_miss = has_miss || state == FileBlock::State::EMPTY ||
                           state == FileBlock::State::SKIP_CACHE;
                has_downloading = has_downloading || state == FileBlock::State::DOWNLOADING;
            }
            ++overlapping_block;
        }

        if (has_miss) {
            ++stats.probe_miss;
            g_cached_remote_reader_probe_miss << 1;
            continue;
        }

        DORIS_CHECK(!coverage.probe_blocks.empty());
        if (has_downloading) {
            coverage.source = AsyncBlockCoverage::Source::DOWNLOADING;
            ++stats.probe_downloading_hit;
            g_cached_remote_reader_probe_downloading << 1;
        } else {
            coverage.source = AsyncBlockCoverage::Source::DOWNLOADED;
            ++stats.probe_downloaded_hit;
            g_cached_remote_reader_probe_downloaded << 1;
        }
    }
}

// Materialize a single side entry. The caller deliberately never invokes this for entries already
// enclosed by the middle span, which keeps waits and local reads out of that span.
bool CachedRemoteFileReader::_materialize_async_coverage(
        AsyncBlockCoverage* coverage, size_t user_offset, Slice result, size_t user_left,
        size_t user_right, const CacheContext& cache_context, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, size_t* indirect_read_bytes,
        bool* need_self_heal) {
    DORIS_CHECK(coverage != nullptr);
    DORIS_CHECK(indirect_read_bytes != nullptr);
    DORIS_CHECK(need_self_heal != nullptr);
    if (coverage->source == AsyncBlockCoverage::Source::MISS) {
        return false;
    }

    const size_t copy_left = std::max(coverage->range.left, user_left);
    const size_t copy_right = std::min(coverage->range.right, user_right);
    const bool has_user_bytes = copy_left <= copy_right;
    if (coverage->source == AsyncBlockCoverage::Source::INFLIGHT) {
        if (has_user_bytes) {
            const auto& entry = coverage->inflight_entry;
            DORIS_CHECK(entry != nullptr);
            const size_t entry_offset = copy_left - entry->buffer_offset;
            const size_t copy_size = copy_right - copy_left + 1;
            DORIS_CHECK(entry_offset + copy_size <= entry->buffer_size);
            memcpy(result.data + (copy_left - user_offset), entry->buffer->data() + entry_offset,
                   copy_size);
            *indirect_read_bytes += copy_size;
            source_read_breakdown.local_bytes += copy_size;
        }
        return true;
    }

    DORIS_CHECK(!coverage->probe_blocks.empty());
    size_t local_read_bytes = 0;
    std::vector<FileBlockSPtr> consumed_blocks;
    for (const auto& block : coverage->probe_blocks) {
        if (_cache->is_block_deleting(block)) {
            coverage->source = AsyncBlockCoverage::Source::MISS;
            return false;
        }

        FileBlock::State state = block->state();
        if (state == FileBlock::State::DOWNLOADING) {
            DORIS_CHECK(coverage->source == AsyncBlockCoverage::Source::DOWNLOADING);
            {
                SCOPED_RAW_TIMER(&stats.remote_wait_timer);
                state = block->wait();
            }
            if (state != FileBlock::State::DOWNLOADED) {
                ++stats.block_wait_timeout;
                g_cached_remote_reader_block_wait_timeout << 1;
                coverage->source = AsyncBlockCoverage::Source::MISS;
                return false;
            }
            ++stats.block_wait_success;
            g_cached_remote_reader_block_wait << 1;
        }
        if (state != FileBlock::State::DOWNLOADED) {
            coverage->source = AsyncBlockCoverage::Source::MISS;
            return false;
        }
        if (!has_user_bytes || block->range().right < copy_left ||
            block->range().left > copy_right) {
            continue;
        }

        const size_t read_left = std::max(block->range().left, copy_left);
        const size_t read_right = std::min(block->range().right, copy_right);
        const size_t read_size = read_right - read_left + 1;
        Status status;
        {
            SCOPED_RAW_TIMER(&stats.local_read_timer);
            status = block->read(Slice(result.data + (read_left - user_offset), read_size),
                                 read_left - block->range().left);
        }
        if (!status.ok()) {
            if (status.is<ErrorCode::NOT_FOUND>()) {
                *need_self_heal = true;
                g_read_cache_self_heal_on_not_found << 1;
            }
            LOG_EVERY_N(WARNING, 100)
                    << "Read probed file cache block failed, falling back to remote. path="
                    << path().native() << ", hash=" << _cache_hash.to_string()
                    << ", offset=" << block->offset() << ", status=" << status;
            coverage->source = AsyncBlockCoverage::Source::MISS;
            return false;
        }
        local_read_bytes += read_size;
        consumed_blocks.emplace_back(block);
    }

    for (const auto& block : consumed_blocks) {
        _cache->touch_probe_block_if_cached(block, cache_context);
    }
    coverage->source = AsyncBlockCoverage::Source::DOWNLOADED;
    *indirect_read_bytes += local_read_bytes;
    source_read_breakdown.local_bytes += local_read_bytes;
    return true;
}

// Consume only boundary entries. Once either side reaches a MISS, everything between the two
// boundaries is intentionally covered by one remote GET without further waits or local reads.
bool CachedRemoteFileReader::_materialize_async_covered_sides(
        AsyncReadPlan* plan, size_t user_offset, Slice result, const CacheContext& cache_context,
        ReadStatistics& stats, SourceReadBreakdown& source_read_breakdown,
        size_t* indirect_read_bytes, bool* need_self_heal, AsyncRemoteRange* remote_range) {
    DORIS_CHECK(plan != nullptr);
    DORIS_CHECK(remote_range != nullptr);

    size_t middle_begin = 0;
    while (middle_begin < plan->coverage.size() &&
           _materialize_async_coverage(&plan->coverage[middle_begin], user_offset, result,
                                       plan->user_left, plan->user_right, cache_context, stats,
                                       source_read_breakdown, indirect_read_bytes,
                                       need_self_heal)) {
        ++middle_begin;
    }

    size_t middle_end = plan->coverage.size();
    while (middle_end > middle_begin &&
           _materialize_async_coverage(&plan->coverage[middle_end - 1], user_offset, result,
                                       plan->user_left, plan->user_right, cache_context, stats,
                                       source_read_breakdown, indirect_read_bytes,
                                       need_self_heal)) {
        --middle_end;
    }
    if (middle_begin == middle_end) {
        return false;
    }

    remote_range->left = plan->coverage[middle_begin].range.left;
    remote_range->size = plan->coverage[middle_end - 1].range.right - remote_range->left + 1;
    size_t miss_bytes = 0;
    for (size_t index = middle_begin; index < middle_end; ++index) {
        if (plan->coverage[index].source == AsyncBlockCoverage::Source::MISS) {
            miss_bytes += plan->coverage[index].range.size();
        }
    }
    DORIS_CHECK(miss_bytes > 0);
    g_cached_remote_reader_remote_after_dedup_miss << 1;
    g_cached_remote_reader_middle_span_read_bytes << remote_range->size;
    g_cached_remote_reader_middle_span_miss_bytes << miss_bytes;
    return true;
}

// Execute one middle-span remote read and expose only the requested user overlap. The full aligned
// payload stays alive for per-block task construction in the next stage.
Status CachedRemoteFileReader::_read_async_remote_range(
        const AsyncReadPlan& plan, const AsyncRemoteRange& remote_range, size_t user_offset,
        Slice result, bool need_self_heal, const IOContext* io_ctx, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, size_t* indirect_read_bytes,
        std::unique_ptr<char[]>* remote_buffer) {
    DORIS_CHECK(indirect_read_bytes != nullptr);
    DORIS_CHECK(remote_buffer != nullptr);
    DORIS_CHECK(remote_range.size > 0);

    stats.hit_cache = false;
    stats.from_peer_cache = false;
    if (need_self_heal) {
        _cache->remove_if_cached_async(_cache_hash);
    }

    const std::vector<FileBlockSPtr> no_peer_blocks;
    RETURN_IF_ERROR(_execute_remote_read(no_peer_blocks, remote_range.left, remote_range.size,
                                         *remote_buffer, nullptr, stats, io_ctx));
    DORIS_CHECK(*remote_buffer != nullptr);

    const size_t remote_right = remote_range.left + remote_range.size - 1;
    const size_t copy_left = std::max(remote_range.left, plan.user_left);
    const size_t copy_right = std::min(remote_right, plan.user_right);
    if (copy_left <= copy_right) {
        const size_t copy_size = copy_right - copy_left + 1;
        memcpy(result.data + (copy_left - user_offset),
               remote_buffer->get() + (copy_left - remote_range.left), copy_size);
        *indirect_read_bytes += copy_size;
        source_read_breakdown.remote_bytes += copy_size;
    }
    return Status::OK();
}

// Submit exactly the MISS entries contained in the remote span. Inflight insertion happens after
// remote IO and immediately before enqueueing, so a concurrent owner wins without duplicate work.
void CachedRemoteFileReader::_submit_async_write_tasks(const AsyncReadPlan& plan,
                                                       const AsyncRemoteRange& remote_range,
                                                       const std::unique_ptr<char[]>& remote_buffer,
                                                       const IOContext* io_ctx,
                                                       ReadStatistics& stats) {
    auto* service = _cache->async_write_service();
    auto* inflight_index = _cache->inflight_write_buffer_index();
    DORIS_CHECK(service != nullptr);
    DORIS_CHECK(inflight_index != nullptr);
    DORIS_CHECK(remote_buffer != nullptr);

    CacheContext cache_context(io_ctx);
    CacheAdmissionContext admission_context {
            .query_id = cache_context.query_id,
            .cache_type = cache_context.cache_type,
            .expiration_time = cache_context.expiration_time,
            .tablet_id = _tablet_id,
            .is_warmup = cache_context.is_warmup,
    };
    const size_t remote_right = remote_range.left + remote_range.size - 1;
    for (const auto& coverage : plan.coverage) {
        if (coverage.source != AsyncBlockCoverage::Source::MISS ||
            coverage.range.left < remote_range.left || coverage.range.right > remote_right) {
            continue;
        }
        if (!service->is_current_write_epoch(plan.write_epoch)) {
            ++stats.async_cache_write_drop_stale_epoch;
            continue;
        }

        AsyncCacheWriteBufferPtr tracked_buffer;
        Status status = service->allocate_tracked_buffer(coverage.range.size(), &tracked_buffer);
        if (!status.ok()) {
            ++stats.async_cache_write_buffer_alloc_fail;
            continue;
        }
        memcpy(tracked_buffer->data(),
               remote_buffer.get() + (coverage.range.left - remote_range.left),
               coverage.range.size());

        AsyncCacheWriteTask task {
                .cache_hash = _cache_hash,
                .file_offset = coverage.range.left,
                .file_size = coverage.range.size(),
                .buffer = tracked_buffer,
                .admission_ctx = admission_context,
                .submit_ts_us = MonotonicMicros(),
                .write_epoch = plan.write_epoch,
                .on_finalized = nullptr,
        };
        std::shared_ptr<InflightWriteBufferEntry> entry;
        if (config::enable_inflight_write_buffer_index) {
            entry = std::make_shared<InflightWriteBufferEntry>(tracked_buffer, coverage.range.left,
                                                               coverage.range.size(),
                                                               task.submit_ts_us, plan.write_epoch);
            auto existing =
                    inflight_index->insert_if_absent(_cache_hash, coverage.range.left, entry);
            if (existing) {
                g_cached_remote_reader_async_skip_existing << 1;
                continue;
            }
            task.on_finalized = [cache_hash = _cache_hash, offset = coverage.range.left,
                                 inflight_index, entry](const AsyncCacheWriteTask&) {
                inflight_index->remove_if(cache_hash, offset, entry);
            };
        }

        if (!service->try_submit(std::move(task))) {
            if (entry) {
                inflight_index->remove_if(_cache_hash, coverage.range.left, entry);
                inflight_index->record_backpressure_rollback();
            }
            ++stats.async_cache_write_rejected;
        } else {
            ++stats.async_cache_write_submitted;
        }
    }
}

// Orchestrate the async-write path while keeping ownership decisions inside the stage helpers.
Status CachedRemoteFileReader::_read_async_write_path(size_t offset, Slice result, size_t bytes_req,
                                                      size_t already_read, size_t* bytes_read,
                                                      ReadStatistics& stats,
                                                      SourceReadBreakdown& source_read_breakdown,
                                                      const IOContext* io_ctx) {
    DORIS_CHECK(_cache_align_mode == CacheAlignMode::ALIGN_TO_BLOCK);
    DORIS_CHECK(!io_ctx->is_dryrun);
    DORIS_CHECK(result.data != nullptr);
    g_read_cache_indirect_num << 1;
    if (already_read == bytes_req) {
        *bytes_read = bytes_req;
        return Status::OK();
    }

    auto* service = _cache->async_write_service();
    DORIS_CHECK(service != nullptr);
    AsyncReadPlan plan;
    _build_async_read_plan(offset + already_read, bytes_req - already_read,
                           service->current_write_epoch(), io_ctx, stats, &plan);

    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    size_t indirect_read_bytes = 0;
    bool need_self_heal = false;
    AsyncRemoteRange remote_range;
    if (_materialize_async_covered_sides(&plan, offset, result, cache_context, stats,
                                         source_read_breakdown, &indirect_read_bytes,
                                         &need_self_heal, &remote_range)) {
        std::unique_ptr<char[]> remote_buffer;
        RETURN_IF_ERROR(_read_async_remote_range(plan, remote_range, offset, result, need_self_heal,
                                                 io_ctx, stats, source_read_breakdown,
                                                 &indirect_read_bytes, &remote_buffer));
        _submit_async_write_tasks(plan, remote_range, remote_buffer, io_ctx, stats);
    }

    *bytes_read = bytes_req;
    g_read_cache_indirect_bytes << indirect_read_bytes;
    g_read_cache_indirect_total_bytes << bytes_req;
    return Status::OK();
}

} // namespace doris::io
