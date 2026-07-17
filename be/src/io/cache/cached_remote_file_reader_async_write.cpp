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
#include <optional>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/cache/async_cache_write_service.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "util/defer_op.h"
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
bvar::LatencyRecorder g_cached_remote_reader_async_read_plan_latency(
        "cached_remote_file_reader_async_read_plan_latency_us");
bvar::LatencyRecorder g_cached_remote_reader_async_write_submission_latency(
        "cached_remote_file_reader_async_write_submission_latency_us");

} // namespace

// One aligned cache block in the simplified read plan. REMOTE blocks delimit the single remote
// range; submit_write distinguishes real cache misses from blocks already being downloaded.
struct CachedRemoteFileReader::AsyncReadBlock {
    enum class Source {
        INFLIGHT,
        CACHE,
        DOWNLOADING,
        REMOTE,
    };

    explicit AsyncReadBlock(FileBlock::Range range_) : range(range_) {}

    FileBlock::Range range;
    Source source {Source::REMOTE};
    bool submit_write {false};
    std::shared_ptr<InflightWriteBufferEntry> inflight_entry;
};

// A read first checks inflight buffers and owns a cache probe result only when that lookup leaves
// uncovered blocks. first_remote_block and remote_block_count identify the inclusive first-to-last
// uncovered span, including any cache hits between those boundaries.
struct CachedRemoteFileReader::AsyncReadPlan {
    AsyncReadPlan(uint64_t write_epoch_, size_t user_left_, size_t user_right_)
            : write_epoch(write_epoch_), user_left(user_left_), user_right(user_right_) {}

    uint64_t write_epoch {0};
    size_t user_left {0};
    size_t user_right {0};
    std::optional<FileBlocksProbeResult> probe_result;
    std::vector<AsyncReadBlock> blocks;
    size_t first_remote_block {0};
    size_t remote_block_count {0};
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

// Build a deliberately small plan. The inflight index is checked first so a fully covered read
// avoids BlockFileCache::probe and its cache mutex. Only an incomplete inflight lookup performs one
// whole-range probe whose result entries map directly to the logical plan blocks.
CachedRemoteFileReader::AsyncReadPlan CachedRemoteFileReader::_build_async_read_plan(
        size_t remaining_offset, size_t remaining_size, uint64_t write_epoch,
        const IOContext* io_ctx, ReadStatistics& stats) {
    const int64_t plan_start_us = MonotonicMicros();
    Defer record_plan_latency {[&]() {
        g_cached_remote_reader_async_read_plan_latency << (MonotonicMicros() - plan_start_us);
    }};
    DORIS_CHECK(remaining_size > 0);
    const auto [align_left, align_size] = s_align_size(remaining_offset, remaining_size, size());
    const size_t cache_block_size = static_cast<size_t>(config::file_cache_each_block_size);
    DORIS_CHECK(cache_block_size > 0);

    AsyncReadPlan plan(write_epoch, remaining_offset, remaining_offset + remaining_size - 1);

    std::vector<size_t> block_offsets;
    const size_t align_end = align_left + align_size;
    for (size_t block_offset = align_left; block_offset < align_end;
         block_offset += cache_block_size) {
        const size_t block_size = std::min(cache_block_size, size() - block_offset);
        DORIS_CHECK(block_size > 0);
        const FileBlock::Range block_range(block_offset, block_offset + block_size - 1);
        plan.blocks.emplace_back(block_range);
        block_offsets.emplace_back(block_offset);
    }
    DORIS_CHECK(!plan.blocks.empty());

    const bool inflight_index_enabled =
            config::enable_async_file_cache_write_inflight_write_buffer_index;
    bool all_blocks_inflight = inflight_index_enabled;
    if (inflight_index_enabled) {
        auto* inflight_index = _cache->inflight_write_buffer_index();
        DORIS_CHECK(inflight_index != nullptr);
        auto inflight_results = inflight_index->lookup_all(_cache_hash, block_offsets, write_epoch);
        DORIS_CHECK(inflight_results.size() == plan.blocks.size());
        for (size_t index = 0; index < plan.blocks.size(); ++index) {
            auto& entry = inflight_results[index].entry;
            if (!entry) {
                all_blocks_inflight = false;
                ++stats.inflight_write_buffer_index_miss;
                continue;
            }

            auto& read_block = plan.blocks[index];
            DORIS_CHECK(entry->buffer != nullptr);
            DORIS_CHECK(entry->buffer_offset <= read_block.range.left);
            DORIS_CHECK(entry->buffer_offset + entry->buffer_size > read_block.range.right);
            read_block.source = AsyncReadBlock::Source::INFLIGHT;
            read_block.inflight_entry = std::move(entry);
            ++stats.inflight_write_buffer_index_hit;
            g_cached_remote_reader_inflight_hit << 1;
        }
    }
    if (all_blocks_inflight) {
        return plan;
    }

    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    plan.probe_result.emplace(_cache->probe(_cache_hash, align_left, align_size, cache_context));
    g_cached_remote_reader_probe_total << 1;
    const auto& probe_result = *plan.probe_result;
    DORIS_CHECK(probe_result.file_blocks.size() == plan.blocks.size());

    for (size_t index = 0; index < plan.blocks.size(); ++index) {
        auto& read_block = plan.blocks[index];
        if (read_block.source == AsyncReadBlock::Source::INFLIGHT) {
            continue;
        }

        const auto& file_block = probe_result.file_blocks[index];
        bool is_miss = file_block == nullptr;
        bool is_downloading = false;
        if (file_block != nullptr) {
            DORIS_CHECK(file_block->range().left == read_block.range.left);
            DORIS_CHECK(file_block->range().right == read_block.range.right);
            if (_cache->is_block_deleting(file_block)) {
                is_miss = true;
            } else {
                switch (file_block->state()) {
                case FileBlock::State::DOWNLOADED:
                    break;
                case FileBlock::State::DOWNLOADING:
                    is_downloading = true;
                    break;
                case FileBlock::State::EMPTY:
                case FileBlock::State::SKIP_CACHE:
                    is_miss = true;
                    break;
                }
            }
        }

        if (is_miss) {
            read_block.submit_write = true;
            ++stats.probe_miss;
            g_cached_remote_reader_probe_miss << 1;
        } else if (is_downloading) {
            read_block.source = AsyncReadBlock::Source::DOWNLOADING;
            ++stats.probe_downloading_hit;
            g_cached_remote_reader_probe_downloading << 1;
            continue;
        } else {
            read_block.source = AsyncReadBlock::Source::CACHE;
            ++stats.probe_downloaded_hit;
            g_cached_remote_reader_probe_downloaded << 1;
            continue;
        }

        if (plan.remote_block_count == 0) {
            plan.first_remote_block = index;
        }
        plan.remote_block_count = index - plan.first_remote_block + 1;
    }
    return plan;
}

// Copy one block available from inflight memory or cache. DOWNLOADING blocks outside the remote
// span retain the existing wait semantics. Any race or read failure asks the caller to replace the
// whole planned request with one remote read instead of incrementally repairing the range.
bool CachedRemoteFileReader::_materialize_async_block(const AsyncReadPlan& plan, size_t block_index,
                                                      size_t user_offset, Slice result,
                                                      const CacheContext& cache_context,
                                                      ReadStatistics& stats,
                                                      size_t* materialized_bytes,
                                                      bool* need_self_heal) {
    DORIS_CHECK(block_index < plan.blocks.size());
    const auto& read_block = plan.blocks[block_index];
    DORIS_CHECK(read_block.source != AsyncReadBlock::Source::REMOTE);
    DORIS_CHECK(materialized_bytes != nullptr);
    DORIS_CHECK(need_self_heal != nullptr);

    const size_t copy_left = std::max(read_block.range.left, plan.user_left);
    const size_t copy_right = std::min(read_block.range.right, plan.user_right);
    if (copy_left > copy_right) {
        return true;
    }
    const size_t copy_size = copy_right - copy_left + 1;

    if (read_block.source == AsyncReadBlock::Source::INFLIGHT) {
        const auto& entry = read_block.inflight_entry;
        DORIS_CHECK(entry != nullptr);
        const size_t entry_offset = copy_left - entry->buffer_offset;
        DORIS_CHECK(entry_offset + copy_size <= entry->buffer_size);
        memcpy(result.data + (copy_left - user_offset), entry->buffer->data() + entry_offset,
               copy_size);
        *materialized_bytes += copy_size;
        return true;
    }

    DORIS_CHECK(plan.probe_result.has_value());
    DORIS_CHECK(block_index < plan.probe_result->file_blocks.size());
    const auto& file_block = plan.probe_result->file_blocks[block_index];
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(file_block->range().left == read_block.range.left);
    DORIS_CHECK(file_block->range().right == read_block.range.right);
    if (_cache->is_block_deleting(file_block)) {
        return false;
    }

    FileBlock::State state = file_block->state();
    if (state == FileBlock::State::DOWNLOADING) {
        DORIS_CHECK(read_block.source == AsyncReadBlock::Source::DOWNLOADING);
        {
            SCOPED_RAW_TIMER(&stats.remote_wait_timer);
            state = file_block->wait();
        }
        if (state != FileBlock::State::DOWNLOADED) {
            ++stats.block_wait_timeout;
            g_cached_remote_reader_block_wait_timeout << 1;
            return false;
        }
        ++stats.block_wait_success;
        g_cached_remote_reader_block_wait << 1;
    }
    if (state != FileBlock::State::DOWNLOADED) {
        return false;
    }

    Status status;
    {
        SCOPED_RAW_TIMER(&stats.local_read_timer);
        status = file_block->read(Slice(result.data + (copy_left - user_offset), copy_size),
                                  copy_left - file_block->range().left);
    }
    if (!status.ok()) {
        if (status.is<ErrorCode::NOT_FOUND>()) {
            *need_self_heal = true;
            g_read_cache_self_heal_on_not_found << 1;
        }
        LOG_EVERY_N(WARNING, 100)
                << "Read probed file cache block failed, falling back to remote. path="
                << path().native() << ", hash=" << _cache_hash.to_string()
                << ", offset=" << file_block->offset() << ", status=" << status;
        return false;
    }

    _cache->touch_probe_block_if_cached(file_block, cache_context);
    *materialized_bytes += copy_size;
    return true;
}

// Read cache/inflight blocks outside the first-to-last remote span. A side failure returns false;
// the caller then performs one remote read for the whole aligned request.
bool CachedRemoteFileReader::_materialize_async_cached_sides(
        const AsyncReadPlan& plan, size_t user_offset, Slice result,
        const CacheContext& cache_context, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, size_t* indirect_read_bytes,
        bool* need_self_heal) {
    DORIS_CHECK(indirect_read_bytes != nullptr);
    DORIS_CHECK(need_self_heal != nullptr);

    size_t materialized_bytes = 0;
    const auto materialize_range = [&](size_t begin, size_t end) {
        for (size_t index = begin; index < end; ++index) {
            if (!_materialize_async_block(plan, index, user_offset, result, cache_context, stats,
                                          &materialized_bytes, need_self_heal)) {
                return false;
            }
        }
        return true;
    };

    const size_t prefix_end =
            plan.remote_block_count == 0 ? plan.blocks.size() : plan.first_remote_block;
    if (!materialize_range(0, prefix_end)) {
        return false;
    }
    if (plan.remote_block_count != 0) {
        const size_t suffix_begin = plan.first_remote_block + plan.remote_block_count;
        DORIS_CHECK(suffix_begin <= plan.blocks.size());
        if (!materialize_range(suffix_begin, plan.blocks.size())) {
            return false;
        }
    }

    *indirect_read_bytes += materialized_bytes;
    source_read_breakdown.local_bytes += materialized_bytes;
    return true;
}

// Read the single aligned range from the first REMOTE block through the last REMOTE block. Cache
// hits inside that span are intentionally reread to keep one straightforward remote operation.
Status CachedRemoteFileReader::_read_async_remote_range(
        const AsyncReadPlan& plan, size_t user_offset, Slice result, bool need_self_heal,
        const IOContext* io_ctx, ReadStatistics& stats, SourceReadBreakdown& source_read_breakdown,
        size_t* indirect_read_bytes, std::unique_ptr<char[]>* remote_buffer) {
    DORIS_CHECK(indirect_read_bytes != nullptr);
    DORIS_CHECK(remote_buffer != nullptr);
    DORIS_CHECK(plan.remote_block_count > 0);
    const size_t remote_end = plan.first_remote_block + plan.remote_block_count;
    DORIS_CHECK(remote_end <= plan.blocks.size());
    const size_t remote_left = plan.blocks[plan.first_remote_block].range.left;
    const size_t remote_right = plan.blocks[remote_end - 1].range.right;
    const size_t remote_size = remote_right - remote_left + 1;

    stats.hit_cache = false;
    stats.from_peer_cache = false;
    if (need_self_heal) {
        _cache->remove_if_cached_async(_cache_hash);
    }

    const std::vector<FileBlockSPtr> no_peer_blocks;
    RETURN_IF_ERROR(_execute_remote_read(no_peer_blocks, remote_left, remote_size, *remote_buffer,
                                         nullptr, stats, io_ctx));
    DORIS_CHECK(*remote_buffer != nullptr);

    const size_t copy_left = std::max(remote_left, plan.user_left);
    const size_t copy_right = std::min(remote_right, plan.user_right);
    if (copy_left <= copy_right) {
        const size_t copy_size = copy_right - copy_left + 1;
        memcpy(result.data + (copy_left - user_offset),
               remote_buffer->get() + (copy_left - remote_left), copy_size);
        *indirect_read_bytes += copy_size;
        source_read_breakdown.remote_bytes += copy_size;
    }

    size_t miss_bytes = 0;
    for (size_t index = plan.first_remote_block; index < remote_end; ++index) {
        if (plan.blocks[index].submit_write) {
            miss_bytes += plan.blocks[index].range.size();
        }
    }
    if (miss_bytes > 0) {
        g_cached_remote_reader_remote_after_dedup_miss << 1;
    }
    g_cached_remote_reader_middle_span_read_bytes << remote_size;
    g_cached_remote_reader_middle_span_miss_bytes << miss_bytes;
    return Status::OK();
}

// Submit exactly the real cache misses contained in the remote span. Inflight insertion happens after
// remote IO and immediately before enqueueing, so a concurrent owner wins without duplicate work.
void CachedRemoteFileReader::_submit_async_write_tasks(const AsyncReadPlan& plan,
                                                       const std::unique_ptr<char[]>& remote_buffer,
                                                       const IOContext* io_ctx,
                                                       ReadStatistics& stats) {
    const int64_t submission_start_us = MonotonicMicros();
    Defer record_submission_latency {[&]() {
        g_cached_remote_reader_async_write_submission_latency
                << (MonotonicMicros() - submission_start_us);
    }};
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
    DORIS_CHECK(plan.remote_block_count > 0);
    const size_t remote_end = plan.first_remote_block + plan.remote_block_count;
    DORIS_CHECK(remote_end <= plan.blocks.size());
    const size_t remote_left = plan.blocks[plan.first_remote_block].range.left;
    for (size_t index = plan.first_remote_block; index < remote_end; ++index) {
        const auto& read_block = plan.blocks[index];
        if (!read_block.submit_write) {
            continue;
        }
        DORIS_CHECK(read_block.source == AsyncReadBlock::Source::REMOTE);
        if (!service->is_current_write_epoch(plan.write_epoch)) {
            ++stats.async_cache_write_drop_stale_epoch;
            continue;
        }

        AsyncCacheWriteBufferPtr tracked_buffer;
        Status status = service->allocate_tracked_buffer(read_block.range.size(), &tracked_buffer);
        if (!status.ok()) {
            ++stats.async_cache_write_buffer_alloc_fail;
            continue;
        }
        memcpy(tracked_buffer->data(), remote_buffer.get() + (read_block.range.left - remote_left),
               read_block.range.size());

        AsyncCacheWriteTask task {
                .cache_hash = _cache_hash,
                .file_offset = read_block.range.left,
                .file_size = read_block.range.size(),
                .buffer = tracked_buffer,
                .admission_ctx = admission_context,
                .submit_ts_us = MonotonicMicros(),
                .write_epoch = plan.write_epoch,
                .on_finalized = nullptr,
        };
        std::shared_ptr<InflightWriteBufferEntry> entry;
        if (config::enable_async_file_cache_write_inflight_write_buffer_index) {
            entry = std::make_shared<InflightWriteBufferEntry>(
                    tracked_buffer, read_block.range.left, read_block.range.size(),
                    task.submit_ts_us, plan.write_epoch);
            TEST_SYNC_POINT_CALLBACK(
                    "CachedRemoteFileReader::_submit_async_write_tasks:before_inflight_insert",
                    &task);
            auto existing =
                    inflight_index->insert_if_absent(_cache_hash, read_block.range.left, entry);
            if (existing) {
                g_cached_remote_reader_async_skip_existing << 1;
                continue;
            }
            task.on_finalized = [cache_hash = _cache_hash, offset = read_block.range.left,
                                 inflight_index, entry](const AsyncCacheWriteTask&) {
                inflight_index->remove_if(cache_hash, offset, entry);
            };
        }

        if (!service->try_submit(std::move(task))) {
            if (entry) {
                inflight_index->remove_if(_cache_hash, read_block.range.left, entry);
                inflight_index->record_backpressure_rollback();
            }
            ++stats.async_cache_write_rejected;
        } else {
            ++stats.async_cache_write_submitted;
        }
    }
}

// Orchestrate one simple plan: read cache/inflight blocks outside the remote span, issue at most
// one remote read, then enqueue only blocks that were actual cache misses.
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
    auto plan = _build_async_read_plan(offset + already_read, bytes_req - already_read,
                                       service->current_write_epoch(), io_ctx, stats);

    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    size_t indirect_read_bytes = 0;
    bool need_self_heal = false;
    if (!_materialize_async_cached_sides(plan, offset, result, cache_context, stats,
                                         source_read_breakdown, &indirect_read_bytes,
                                         &need_self_heal)) {
        plan.first_remote_block = 0;
        plan.remote_block_count = plan.blocks.size();
    }
    if (plan.remote_block_count > 0) {
        std::unique_ptr<char[]> remote_buffer;
        RETURN_IF_ERROR(_read_async_remote_range(plan, offset, result, need_self_heal, io_ctx,
                                                 stats, source_read_breakdown, &indirect_read_bytes,
                                                 &remote_buffer));
        _submit_async_write_tasks(plan, remote_buffer, io_ctx, stats);
    }

    *bytes_read = bytes_req;
    g_read_cache_indirect_bytes << indirect_read_bytes;
    g_read_cache_indirect_total_bytes << bytes_req;
    return Status::OK();
}

} // namespace doris::io
