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

#include <butil/iobuf.h>
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
#include "io/cache/async_cache_write_manager.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "io/cache/peer_file_cache_reader.h"
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

// Async-path remote spans that may use peer read (peer read enabled and not bypassed by the
// caller). Every eligible span ends up in exactly one of:
// - a span made purely of misses, sent to peer as is (not counted below);
// - a sparse read: hits inside the span are read locally and peer gets only the misses (not
//   counted below);
// - no_candidate: a span with hits whose tablet has no usable candidate (none yet, or in
//   cooldown). It is reread from storage and a background candidate fetch is started. This takes
//   precedence over the span shape, so a span with a DOWNLOADING block and no candidate is
//   no_candidate, not bypass;
// - bypass: a span with hits that has candidates but gives up peer because of its shape: a
//   DOWNLOADING block inside the span, the whole-range fallback after a side race, or a failed
//   local read of a cached/inflight block inside the span.
// Spans that try peer but end on remote storage (peer miss or failure, S3 winning the race) are
// in neither counter; for a sparse span the storage read then supplies only the misses.
bvar::Adder<uint64_t> g_async_peer_eligible_span_count(
        "cached_remote_file_reader_async_peer_eligible_span_count");
bvar::Adder<uint64_t> g_async_peer_bypass_span_count(
        "cached_remote_file_reader_async_peer_bypass_span_count");
bvar::Adder<uint64_t> g_async_peer_no_candidate_span_count(
        "cached_remote_file_reader_async_peer_no_candidate_span_count");

namespace {

bvar::Adder<uint64_t> g_cached_remote_reader_probe_total("cached_remote_file_reader_probe_count");
bvar::Adder<uint64_t> g_cached_remote_reader_probe_downloaded(
        "cached_remote_file_reader_probe_hit_downloaded_count");
bvar::Adder<uint64_t> g_cached_remote_reader_probe_downloading(
        "cached_remote_file_reader_probe_hit_downloading_count");
bvar::Adder<uint64_t> g_cached_remote_reader_probe_miss(
        "cached_remote_file_reader_probe_miss_count");

// Copy the peer payload that overlaps file range [copy_left, copy_right] into `result`, whose first
// byte is file offset `user_offset`, and regroup the chunks by reference into one IOBuf per
// requested range. Chunks may split or reorder ranges; block_offset is a file offset and
// block_index indexes `ranges`. Add the bytes copied into `result` to `copied_bytes`.
std::vector<butil::IOBuf> take_peer_span(const PeerFetchResult& peer_result,
                                         const std::vector<FileBlock::Range>& ranges,
                                         size_t copy_left, size_t copy_right, size_t user_offset,
                                         Slice result, size_t* copied_bytes) {
    std::vector<const PeerFetchChunk*> chunks;
    chunks.reserve(peer_result.chunks.size());
    for (const auto& chunk : peer_result.chunks) {
        chunks.push_back(&chunk);
    }
    std::sort(chunks.begin(), chunks.end(), [](const auto* lhs, const auto* rhs) {
        return lhs->block_offset < rhs->block_offset;
    });
    std::vector<butil::IOBuf> peer_blocks(ranges.size());
    for (const auto* chunk : chunks) {
        DORIS_CHECK(chunk->block_index < ranges.size());
        const size_t chunk_size = chunk->payload.length();
        peer_blocks[chunk->block_index].append(chunk->payload); // shares blocks
        if (chunk_size == 0) {
            continue;
        }
        const size_t chunk_copy_left = std::max(copy_left, chunk->block_offset);
        const size_t chunk_copy_right = std::min(copy_right, chunk->block_offset + chunk_size - 1);
        if (chunk_copy_left > chunk_copy_right) {
            continue;
        }
        const size_t copy_size = chunk_copy_right - chunk_copy_left + 1;
        const size_t result_offset = chunk_copy_left - user_offset;
        DORIS_CHECK(copy_size <= result.size - result_offset);
        const size_t copied = chunk->payload.copy_to(result.data + result_offset, copy_size,
                                                     chunk_copy_left - chunk->block_offset);
        DORIS_CHECK(copied == copy_size);
        *copied_bytes += copy_size;
    }
    for (size_t index = 0; index < ranges.size(); ++index) {
        DORIS_CHECK(peer_blocks[index].length() == ranges[index].size());
    }
    return peer_blocks;
}

// Build the payload of one async write task. A peer block is wrapped without copying; storage bytes
// are copied into a tracked buffer. Return nullptr when the tracked allocation fails.
AsyncCacheWriteBufferPtr make_task_buffer(AsyncCacheWriteManager* manager,
                                          const butil::IOBuf* peer_block, const char* storage_data,
                                          size_t size, size_t capacity) {
    if (peer_block != nullptr) {
        return manager->wrap_iobuf(capacity, *peer_block); // copying an IOBuf shares its blocks
    }
    AsyncCacheWriteBufferPtr buffer;
    if (!manager->allocate_tracked_buffer(capacity, &buffer).ok()) {
        return nullptr;
    }
    memcpy(buffer->data(), storage_data, size);
    return buffer;
}

} // namespace

// One logical cache-block slot in the read plan.
//
// Contract after plan construction:
// - `range.left` is cache-block aligned and ranges are ordered, gap-free, and non-overlapping.
// - `range.size()` is one cache block except for the physical EOF slot, which may be shorter.
// - `source` identifies how this slot can be materialized before the remote read. REMOTE means the
//   probe found a real miss; CACHE and DOWNLOADING retain the corresponding probe result by index.
// - `submit_write` is true exactly for REMOTE misses. A CACHE hit lying between the first and last
//   miss is reread by the single remote operation, but is deliberately not submitted for writing.
// - `inflight_entry` is non-null exactly when `source` is INFLIGHT and covers the whole slot.
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

// Immutable logical partition plus the source classification used by one async read.
//
// `blocks` is an index-stable partition of the aligned read interval: the same index is used for
// inflight lookup, BlockFileCache::probe(), side materialization, remote-buffer slicing, and async
// task creation. The user interval [user_left, user_right] is fully contained by that partition.
//
// `first_remote_block` and `remote_block_count` describe the minimal first-miss-to-last-miss span.
// It may contain CACHE/INFLIGHT hits between misses so the read path issues at most one remote IO.
// When peer read applies, those hits are read locally first and peer serves only the misses.
// `whole_range_fallback` marks the span widened to every block after a side race.
// `probe_result` is absent only when inflight buffers covered every block and probe was skipped.
// `write_epoch` fences only later persistence: a cache hash identifies immutable file content, so
// an inflight payload remains readable after its original write task is invalidated.
struct CachedRemoteFileReader::AsyncReadPlan {
    AsyncReadPlan(AsyncCacheWriteEpoch write_epoch_, size_t user_left_, size_t user_right_)
            : write_epoch(std::move(write_epoch_)),
              user_left(user_left_),
              user_right(user_right_) {}

    AsyncCacheWriteEpoch write_epoch;
    size_t user_left {0};
    size_t user_right {0};
    std::optional<FileBlocksProbeResult> probe_result;
    std::vector<AsyncReadBlock> blocks;
    size_t first_remote_block {0};
    size_t remote_block_count {0};
    bool whole_range_fallback {false};

    // These are construction-time contracts rather than runtime error handling. Keep them as
    // debug checks so validating every logical block does not add work to each production read.
    void check_layout(size_t file_size, size_t cache_block_size) const {
        DCHECK_GT(file_size, 0);
        DCHECK_GT(cache_block_size, 0);
        DCHECK_LE(user_left, user_right);
        DCHECK_LT(user_right, file_size);
        DCHECK(!blocks.empty());
        DCHECK_LE(blocks.front().range.left, user_left);
        DCHECK_GE(blocks.back().range.right, user_right);

        for (size_t index = 0; index < blocks.size(); ++index) {
            const auto& range = blocks[index].range;
            DCHECK_LE(range.left, range.right);
            DCHECK_EQ(range.left % cache_block_size, 0);
            DCHECK_LT(range.right, file_size);
            if (index > 0) {
                DCHECK_EQ(blocks[index - 1].range.right + 1, range.left);
            }
            if (range.right + 1 < file_size) {
                DCHECK_EQ(range.size(), cache_block_size);
            } else {
                DCHECK_LE(range.size(), cache_block_size);
            }
        }
    }

    void check_built_sources() const {
        size_t first_miss = blocks.size();
        size_t last_miss = 0;
        for (size_t index = 0; index < blocks.size(); ++index) {
            const auto& block = blocks[index];
            const bool is_inflight = block.source == AsyncReadBlock::Source::INFLIGHT;
            const bool is_remote = block.source == AsyncReadBlock::Source::REMOTE;
            DCHECK_EQ(block.inflight_entry != nullptr, is_inflight);
            DCHECK_EQ(block.submit_write, is_remote);
            if (block.submit_write) {
                if (first_miss == blocks.size()) {
                    first_miss = index;
                }
                last_miss = index;
            }
        }

        if (first_miss == blocks.size()) {
            DCHECK_EQ(remote_block_count, 0);
        } else {
            DCHECK_EQ(first_remote_block, first_miss);
            DCHECK_EQ(remote_block_count, last_miss - first_miss + 1);
        }
        if (probe_result.has_value()) {
            DCHECK_EQ(probe_result->file_blocks.size(), blocks.size());
        } else {
            DCHECK(std::all_of(blocks.begin(), blocks.end(), [](const auto& block) {
                return block.source == AsyncReadBlock::Source::INFLIGHT;
            }));
        }
    }
};

// Resolve mode on every read so online configuration changes affect existing readers.
CacheWriteMode CachedRemoteFileReader::_resolve_cache_write_mode(const IOContext* io_ctx) const {
    if (io_ctx->is_dryrun || io_ctx->is_warmup) {
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
        size_t remaining_offset, size_t remaining_size, AsyncCacheWriteEpoch write_epoch,
        const IOContext* io_ctx, ReadStatistics& stats) {
    DORIS_CHECK(io_ctx != nullptr);
    DORIS_CHECK(remaining_offset < size());
    DORIS_CHECK(remaining_size > 0);
    DORIS_CHECK(remaining_size <= size() - remaining_offset);
    const auto [align_left, align_size] = s_align_size(remaining_offset, remaining_size, size());
    const size_t cache_block_size = static_cast<size_t>(config::file_cache_each_block_size);
    DORIS_CHECK(cache_block_size > 0);
    DORIS_CHECK(align_left % cache_block_size == 0);
    DORIS_CHECK(align_left <= remaining_offset);
    DORIS_CHECK(align_size > 0);
    DORIS_CHECK(align_size <= size() - align_left);

    AsyncReadPlan plan(std::move(write_epoch), remaining_offset,
                       remaining_offset + remaining_size - 1);

    std::vector<size_t> block_offsets;
    const size_t align_end = align_left + align_size;
    for (size_t block_offset = align_left; block_offset < align_end;) {
        const size_t block_size = std::min(cache_block_size, align_end - block_offset);
        DORIS_CHECK(block_size > 0);
        const FileBlock::Range block_range(block_offset, block_offset + block_size - 1);
        plan.blocks.emplace_back(block_range);
        block_offsets.emplace_back(block_offset);
        block_offset += block_size;
    }
    DORIS_CHECK(!plan.blocks.empty());
    DORIS_CHECK(plan.blocks.front().range.left == align_left);
    DORIS_CHECK(plan.blocks.back().range.right == align_end - 1);
    plan.check_layout(size(), cache_block_size);

    const bool inflight_index_enabled =
            config::enable_async_file_cache_write_inflight_write_buffer_index;
    bool all_blocks_inflight = inflight_index_enabled;
    if (inflight_index_enabled) {
        auto* inflight_index = _cache->inflight_write_buffer_index();
        DORIS_CHECK(inflight_index != nullptr);
        auto inflight_results = inflight_index->lookup_all(_cache_hash, block_offsets);
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
        }
    }
    if (all_blocks_inflight) {
        plan.check_built_sources();
        return plan;
    }

    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    plan.probe_result.emplace(_cache->probe(_cache_hash, align_left, align_size, cache_context));
    g_cached_remote_reader_probe_total << 1;
    const auto& probe_result = *plan.probe_result;
    DORIS_CHECK(probe_result.file_blocks.size() == plan.blocks.size());

    // probe() validates slot coverage while holding the cache mutex. Use the immutable logical
    // plan ranges after it returns: a concurrent file writer may shrink a preallocated EOF block
    // during finalize().
    for (size_t index = 0; index < plan.blocks.size(); ++index) {
        auto& read_block = plan.blocks[index];
        if (read_block.source == AsyncReadBlock::Source::INFLIGHT) {
            continue;
        }

        const auto& file_block = probe_result.file_blocks[index];
        bool is_miss = file_block == nullptr;
        bool is_downloading = false;
        if (file_block != nullptr) {
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
    plan.check_built_sources();
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
    DORIS_CHECK(user_offset <= plan.user_left);
    DORIS_CHECK(plan.user_right - user_offset < result.size);
    DORIS_CHECK(materialized_bytes != nullptr);
    DORIS_CHECK(need_self_heal != nullptr);

    const size_t copy_left = std::max(read_block.range.left, plan.user_left);
    const size_t copy_right = std::min(read_block.range.right, plan.user_right);
    if (copy_left > copy_right) {
        return true;
    }
    const size_t copy_size = copy_right - copy_left + 1;
    const size_t result_offset = copy_left - user_offset;
    DORIS_CHECK(copy_size <= result.size - result_offset);

    if (read_block.source == AsyncReadBlock::Source::INFLIGHT) {
        const auto& entry = read_block.inflight_entry;
        DORIS_CHECK(entry != nullptr);
        const size_t entry_offset = copy_left - entry->buffer_offset;
        DORIS_CHECK(entry_offset + copy_size <= entry->buffer_size);
        entry->buffer->copy_to(entry_offset, copy_size, result.data + result_offset);
        *materialized_bytes += copy_size;
        return true;
    }

    DORIS_CHECK(plan.probe_result.has_value());
    DORIS_CHECK(block_index < plan.probe_result->file_blocks.size());
    const auto& file_block = plan.probe_result->file_blocks[block_index];
    DORIS_CHECK(file_block != nullptr);
    if (_cache->is_block_deleting(file_block)) {
        return false;
    }

    FileBlock::State state = file_block->state();
    if (state == FileBlock::State::DOWNLOADING) {
        DORIS_CHECK(read_block.source == AsyncReadBlock::Source::DOWNLOADING);
        TEST_SYNC_POINT("CachedRemoteFileReader::_materialize_async_block:before_wait");
        {
            SCOPED_RAW_TIMER(&stats.remote_wait_timer);
            state = file_block->wait();
        }
        if (state != FileBlock::State::DOWNLOADED) {
            ++stats.block_wait_timeout;
            return false;
        }
        ++stats.block_wait_success;
    }
    if (state != FileBlock::State::DOWNLOADED) {
        return false;
    }

    Status status;
    {
        SCOPED_RAW_TIMER(&stats.local_read_timer);
        status = file_block->read(Slice(result.data + result_offset, copy_size),
                                  copy_left - read_block.range.left);
    }
    if (!status.ok()) {
        if (status.is<ErrorCode::NOT_FOUND>()) {
            *need_self_heal = true;
            g_read_cache_self_heal_on_not_found << 1;
        }
        LOG_EVERY_N(WARNING, 100)
                << "Read probed file cache block failed, falling back to remote. path="
                << path().native() << ", hash=" << _cache_hash.to_string()
                << ", offset=" << read_block.range.left << ", status=" << status;
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
    DORIS_CHECK(plan.first_remote_block <= plan.blocks.size());
    DORIS_CHECK(plan.remote_block_count <= plan.blocks.size() - plan.first_remote_block);

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

// Payload of the single remote operation: storage returns one contiguous buffer for the span; peer
// returns one IOBuf per span block, after fetch_blocks() verified exact coverage. For a sparse peer
// read, blocks that were not requested from peer keep an empty IOBuf and are never submitted.
struct CachedRemoteFileReader::AsyncRemoteSpan {
    std::unique_ptr<char[]> buffer;
    std::vector<butil::IOBuf> peer_blocks;
};

// Read the single aligned range from the first REMOTE block through the last REMOTE block. Without
// peer, cache hits inside that span are intentionally reread to keep one straightforward remote
// operation. With peer, a peer fails the whole RPC when it misses any requested block, so hits
// inside the span are read locally first and peer is asked only for the misses.
Status CachedRemoteFileReader::_read_async_remote_range(
        const AsyncReadPlan& plan, size_t user_offset, Slice result, bool need_self_heal,
        const IOContext* io_ctx, ReadStatistics& stats, SourceReadBreakdown& source_read_breakdown,
        size_t* indirect_read_bytes, AsyncRemoteSpan* span) {
    DORIS_CHECK(indirect_read_bytes != nullptr);
    DORIS_CHECK(span != nullptr);
    DORIS_CHECK(plan.remote_block_count > 0);
    DORIS_CHECK(plan.first_remote_block < plan.blocks.size());
    const size_t remote_end = plan.first_remote_block + plan.remote_block_count;
    DORIS_CHECK(remote_end <= plan.blocks.size());
    const size_t remote_left = plan.blocks[plan.first_remote_block].range.left;
    const size_t remote_right = plan.blocks[remote_end - 1].range.right;
    DORIS_CHECK(remote_left <= remote_right);
    const size_t remote_size = remote_right - remote_left + 1;
    DORIS_CHECK(user_offset <= plan.user_left);
    DORIS_CHECK(plan.user_right - user_offset < result.size);

    stats.hit_cache = false;
    stats.from_peer_cache = false;

    bool all_misses = true;
    for (size_t index = plan.first_remote_block; index < remote_end; ++index) {
        all_misses = all_misses && plan.blocks[index].source == AsyncReadBlock::Source::REMOTE;
    }
    size_t sparse_local_bytes = 0;
    const bool sparse_peer = _prepare_async_sparse_peer(plan, user_offset, result, io_ctx, stats,
                                                        all_misses, &sparse_local_bytes,
                                                        &need_self_heal);
    if (need_self_heal) {
        _cache->remove_if_cached_async(_cache_hash);
    }
    if (sparse_peer) {
        return _read_async_sparse_peer_span(plan, user_offset, result, io_ctx, stats,
                                            sparse_local_bytes, source_read_breakdown,
                                            indirect_read_bytes, span);
    }

    // Peer fails the whole RPC when it misses any requested block, so a span that is not purely
    // real misses and was not read sparsely above stays on direct remote storage.
    std::vector<FileBlock::Range> remote_ranges;
    remote_ranges.reserve(plan.remote_block_count);
    for (size_t index = plan.first_remote_block; index < remote_end; ++index) {
        remote_ranges.push_back(plan.blocks[index].range);
    }
    IOContext remote_io_ctx = *io_ctx;
    remote_io_ctx.bypass_peer_read = remote_io_ctx.bypass_peer_read || !all_misses;

    PeerFetchResult peer_result;
    RETURN_IF_ERROR(_execute_remote_read(remote_ranges, remote_left, remote_size, span->buffer,
                                         &peer_result, stats, &remote_io_ctx));

    SCOPED_RAW_TIMER(&stats.async_copy_to_user_timer);
    const size_t copy_left = std::max(remote_left, plan.user_left);
    const size_t copy_right = std::min(remote_right, plan.user_right);
    if (span->buffer != nullptr) {
        if (copy_left <= copy_right) {
            const size_t copy_size = copy_right - copy_left + 1;
            const size_t result_offset = copy_left - user_offset;
            const size_t remote_buffer_offset = copy_left - remote_left;
            DORIS_CHECK(copy_size <= result.size - result_offset);
            DORIS_CHECK(copy_size <= remote_size - remote_buffer_offset);
            memcpy(result.data + result_offset, span->buffer.get() + remote_buffer_offset,
                   copy_size);
            *indirect_read_bytes += copy_size;
            source_read_breakdown.remote_bytes += copy_size;
        }
        return Status::OK();
    }

    // Peer won: fetch_blocks() verified that the chunks cover every requested byte exactly once.
    DORIS_CHECK(stats.from_peer_cache);
    DORIS_CHECK(peer_result.bytes_read == remote_size);
    size_t copied = 0;
    span->peer_blocks = take_peer_span(peer_result, remote_ranges, copy_left, copy_right,
                                       user_offset, result, &copied);
    *indirect_read_bytes += copied;
    source_read_breakdown.peer_bytes += copied;
    return Status::OK();
}

// Decide whether an eligible span is read sparsely and count the outcome (see the counters above).
// Without a candidate the local reads would be wasted before the storage read, so they are skipped
// and a candidate fetch is started. Candidates may change before the read; that only falls back
// to storage, as either path already can.
bool CachedRemoteFileReader::_prepare_async_sparse_peer(const AsyncReadPlan& plan,
                                                        size_t user_offset, Slice result,
                                                        const IOContext* io_ctx,
                                                        ReadStatistics& stats, bool all_misses,
                                                        size_t* local_bytes, bool* need_self_heal) {
    if (!_should_read_from_peer(io_ctx)) {
        return false;
    }
    g_async_peer_eligible_span_count << 1;
    if (all_misses) {
        return false;
    }
    if (!_has_peer_candidates()) {
        g_async_peer_no_candidate_span_count << 1;
        _fetch_peer_candidates_unless_cooldown();
        return false;
    }
    if (!_materialize_async_span_hits(plan, user_offset, result, io_ctx, stats, local_bytes,
                                      need_self_heal)) {
        g_async_peer_bypass_span_count << 1;
        return false;
    }
    return true;
}

// Read the cached/inflight blocks inside the remote span so peer can be asked for the misses only.
// DOWNLOADING blocks keep the span on remote storage so the read never waits on another
// downloader, and the whole-range fallback has already failed a local read. Return false when the
// span must instead be reread from storage; bytes copied before a failure are not counted.
// NOLINTNEXTLINE(readability-make-member-function-const): calls non-const _materialize_async_block
bool CachedRemoteFileReader::_materialize_async_span_hits(
        const AsyncReadPlan& plan, size_t user_offset, Slice result, const IOContext* io_ctx,
        ReadStatistics& stats, size_t* local_bytes, bool* need_self_heal) {
    const size_t remote_end = plan.first_remote_block + plan.remote_block_count;
    if (plan.whole_range_fallback ||
        std::any_of(plan.blocks.begin() + plan.first_remote_block, plan.blocks.begin() + remote_end,
                    [](const auto& block) {
                        return block.source == AsyncReadBlock::Source::DOWNLOADING;
                    })) {
        return false;
    }
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    for (size_t index = plan.first_remote_block; index < remote_end; ++index) {
        if (plan.blocks[index].source != AsyncReadBlock::Source::REMOTE &&
            !_materialize_async_block(plan, index, user_offset, result, cache_context, stats,
                                      local_bytes, need_self_heal)) {
            return false;
        }
    }
    return true;
}

// Peer receives only the misses and fetch_blocks() verifies it covered them exactly. A storage
// fallback (no candidate, peer failure, or S3 winning the race) still reads the whole span, from
// which only the misses are copied because the other blocks were already read locally.
Status CachedRemoteFileReader::_read_async_sparse_peer_span(
        const AsyncReadPlan& plan, size_t user_offset, Slice result, const IOContext* io_ctx,
        ReadStatistics& stats, size_t local_bytes, SourceReadBreakdown& source_read_breakdown,
        size_t* indirect_read_bytes, AsyncRemoteSpan* span) {
    *indirect_read_bytes += local_bytes;
    source_read_breakdown.local_bytes += local_bytes;
    const size_t remote_end = plan.first_remote_block + plan.remote_block_count;
    const size_t remote_left = plan.blocks[plan.first_remote_block].range.left;
    const size_t remote_right = plan.blocks[remote_end - 1].range.right;
    const size_t remote_size = remote_right - remote_left + 1;

    std::vector<FileBlock::Range> miss_ranges;
    std::vector<size_t> miss_span_indexes;
    size_t miss_bytes = 0;
    for (size_t index = plan.first_remote_block; index < remote_end; ++index) {
        if (plan.blocks[index].source == AsyncReadBlock::Source::REMOTE) {
            miss_ranges.push_back(plan.blocks[index].range);
            miss_span_indexes.push_back(index - plan.first_remote_block);
            miss_bytes += plan.blocks[index].range.size();
        }
    }

    PeerFetchResult peer_result;
    RETURN_IF_ERROR(_execute_remote_read(miss_ranges, remote_left, remote_size, span->buffer,
                                         &peer_result, stats, io_ctx));

    SCOPED_RAW_TIMER(&stats.async_copy_to_user_timer);
    const size_t copy_left = std::max(remote_left, plan.user_left);
    const size_t copy_right = std::min(remote_right, plan.user_right);
    if (span->buffer != nullptr) {
        for (const auto& range : miss_ranges) {
            const size_t block_copy_left = std::max(copy_left, range.left);
            const size_t block_copy_right = std::min(copy_right, range.right);
            if (block_copy_left > block_copy_right) {
                continue;
            }
            const size_t copy_size = block_copy_right - block_copy_left + 1;
            const size_t result_offset = block_copy_left - user_offset;
            DORIS_CHECK(copy_size <= result.size - result_offset);
            DORIS_CHECK(copy_size <= remote_size - (block_copy_left - remote_left));
            memcpy(result.data + result_offset,
                   span->buffer.get() + (block_copy_left - remote_left), copy_size);
            *indirect_read_bytes += copy_size;
            source_read_breakdown.remote_bytes += copy_size;
        }
        return Status::OK();
    }

    DORIS_CHECK(stats.from_peer_cache);
    DORIS_CHECK(peer_result.bytes_read == miss_bytes);
    size_t copied = 0;
    auto miss_blocks = take_peer_span(peer_result, miss_ranges, copy_left, copy_right, user_offset,
                                      result, &copied);
    span->peer_blocks.resize(plan.remote_block_count);
    for (size_t index = 0; index < miss_blocks.size(); ++index) {
        span->peer_blocks[miss_span_indexes[index]] = std::move(miss_blocks[index]);
    }
    *indirect_read_bytes += copied;
    source_read_breakdown.peer_bytes += copied;
    return Status::OK();
}

// Submit exactly the real cache misses contained in the remote span. Inflight insertion happens after
// remote IO and immediately before enqueueing, so a concurrent owner wins without duplicate work.
void CachedRemoteFileReader::_submit_async_write_tasks(const AsyncReadPlan& plan,
                                                       const AsyncRemoteSpan& span,
                                                       const IOContext* io_ctx,
                                                       ReadStatistics& stats) {
    auto* manager = _cache->async_write_manager();
    auto* inflight_index = _cache->inflight_write_buffer_index();
    DORIS_CHECK(manager != nullptr);
    DORIS_CHECK(inflight_index != nullptr);
    DORIS_CHECK((span.buffer != nullptr) != !span.peer_blocks.empty());

    CacheContext cache_context(io_ctx);
    const CacheAdmissionContext admission_context =
            CacheAdmissionContext::from_cache_context(cache_context, _tablet_id);
    DORIS_CHECK(plan.remote_block_count > 0);
    DORIS_CHECK(plan.first_remote_block < plan.blocks.size());
    const size_t remote_end = plan.first_remote_block + plan.remote_block_count;
    DORIS_CHECK(remote_end <= plan.blocks.size());
    const size_t remote_left = plan.blocks[plan.first_remote_block].range.left;
    const size_t remote_right = plan.blocks[remote_end - 1].range.right;
    const size_t remote_size = remote_right - remote_left + 1;
    const size_t cache_block_size = static_cast<size_t>(config::file_cache_each_block_size);
    DORIS_CHECK(cache_block_size > 0);
    for (size_t index = plan.first_remote_block; index < remote_end; ++index) {
        const auto& read_block = plan.blocks[index];
        if (!read_block.submit_write) {
            continue;
        }
        DORIS_CHECK(read_block.source == AsyncReadBlock::Source::REMOTE);
        DORIS_CHECK(read_block.range.left % cache_block_size == 0);
        DORIS_CHECK(read_block.range.size() <= cache_block_size);
        DORIS_CHECK(read_block.range.left >= remote_left);
        DORIS_CHECK(read_block.range.right <= remote_right);
        if (!manager->check_write_epoch(plan.write_epoch)) {
            ++stats.async_cache_write_drop_stale_epoch;
            continue;
        }

        const size_t remote_buffer_offset = read_block.range.left - remote_left;
        DORIS_CHECK(remote_buffer_offset <= remote_size);
        DORIS_CHECK(read_block.range.size() <= remote_size - remote_buffer_offset);
        const bool from_peer = span.buffer == nullptr;
        AsyncCacheWriteBufferPtr tracked_buffer = make_task_buffer(
                manager, from_peer ? &span.peer_blocks[index - plan.first_remote_block] : nullptr,
                from_peer ? nullptr : span.buffer.get() + remote_buffer_offset,
                read_block.range.size(), cache_block_size);
        if (tracked_buffer == nullptr) {
            ++stats.async_cache_write_buffer_alloc_fail;
            continue;
        }
        DORIS_CHECK(tracked_buffer->size() == cache_block_size);
        DORIS_CHECK(read_block.range.size() <= tracked_buffer->size());

        AsyncCacheWriteTask task {
                .cache_hash = _cache_hash,
                .file_offset = read_block.range.left,
                .write_size = read_block.range.size(),
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
                    task.submit_ts_us);
            TEST_SYNC_POINT_CALLBACK(
                    "CachedRemoteFileReader::_submit_async_write_tasks:before_inflight_insert",
                    &task);
            auto existing =
                    inflight_index->insert_if_absent(_cache_hash, read_block.range.left, entry);
            if (existing) {
                continue;
            }
            task.on_finalized = [cache_hash = _cache_hash, offset = read_block.range.left,
                                 inflight_index, entry](const AsyncCacheWriteTask&) {
                inflight_index->remove_if(cache_hash, offset, entry);
            };
        }

        if (!manager->try_submit(std::move(task))) {
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
    DORIS_CHECK(io_ctx != nullptr);
    DORIS_CHECK(!io_ctx->is_dryrun);
    DORIS_CHECK(result.data != nullptr);
    DORIS_CHECK(bytes_read != nullptr);
    DORIS_CHECK(already_read <= bytes_req);
    DORIS_CHECK(bytes_req <= result.size);
    DORIS_CHECK(offset <= size());
    DORIS_CHECK(bytes_req <= size() - offset);

    g_read_cache_indirect_num << 1;
    if (already_read == bytes_req) {
        *bytes_read = bytes_req;
        return Status::OK();
    }

    auto* manager = _cache->async_write_manager();
    DORIS_CHECK(manager != nullptr);
    auto plan = _build_async_read_plan(offset + already_read, bytes_req - already_read,
                                       manager->current_write_epoch(_cache_hash), io_ctx, stats);

    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    size_t indirect_read_bytes = 0;
    bool need_self_heal = false;
    if (!_materialize_async_cached_sides(plan, offset, result, cache_context, stats,
                                         source_read_breakdown, &indirect_read_bytes,
                                         &need_self_heal)) {
        // A side race invalidates incremental materialization. Reread the whole aligned partition
        // remotely, but retain the original `submit_write` bits so only original probe misses are
        // published as async writes.
        plan.first_remote_block = 0;
        plan.remote_block_count = plan.blocks.size();
        plan.whole_range_fallback = true;
    }
    if (plan.remote_block_count > 0) {
        AsyncRemoteSpan remote_span;
        RETURN_IF_ERROR(_read_async_remote_range(plan, offset, result, need_self_heal, io_ctx,
                                                 stats, source_read_breakdown, &indirect_read_bytes,
                                                 &remote_span));
        SCOPED_RAW_TIMER(&stats.async_submit_write_timer);
        _submit_async_write_tasks(plan, remote_span, io_ctx, stats);
    }

    DORIS_CHECK(indirect_read_bytes == bytes_req - already_read);
    *bytes_read = bytes_req;
    g_read_cache_indirect_bytes << indirect_read_bytes;
    g_read_cache_indirect_total_bytes << bytes_req;
    return Status::OK();
}

} // namespace doris::io
