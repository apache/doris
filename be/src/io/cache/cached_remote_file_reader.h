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

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <shared_mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {
struct IOContext;
struct FileCacheStatistics;
struct PeerFetchResult;

} // namespace doris::io

namespace doris {
struct PeerCandidate;
}

namespace doris::io {
struct SourceReadBreakdown {
    int64_t local_bytes = 0;
    int64_t remote_bytes = 0;
    int64_t peer_bytes = 0;
};
using PeerFetchedBlockSet = std::unordered_set<const FileBlock*>;

class CachedRemoteFileReader final : public FileReader,
                                     public std::enable_shared_from_this<CachedRemoteFileReader> {
public:
    /// Construct a cached reader on top of a remote reader.
    /// @param[in] remote_file_reader Underlying reader used for remote/peer fallback reads.
    /// @param[in] opts File reader options used to initialize cache identity and policy.
    /// @return None.
    CachedRemoteFileReader(FileReaderSPtr remote_file_reader, const FileReaderOptions& opts);

    /// Destroy the cached reader and release direct cache-file ownership tracked by this reader.
    /// @return None.
    ~CachedRemoteFileReader() override;

    /// Close the underlying remote reader.
    /// @return OK on success; otherwise the close error from the underlying reader.
    Status close() override;

    /// Get the path of the underlying file.
    /// @return Reference to the remote reader path.
    const Path& path() const override { return _remote_file_reader->path(); }

    /// Get the logical size of the underlying file.
    /// @return File size in bytes.
    size_t size() const override { return _remote_file_reader->size(); }

    /// Check whether the underlying reader has been closed.
    /// @return true if the underlying reader is closed; otherwise false.
    bool closed() const override { return _remote_file_reader->closed(); }

    /// Expose the wrapped remote reader.
    /// @return Raw pointer to the underlying reader owned by this object.
    FileReader* get_remote_reader() { return _remote_file_reader.get(); }

    /// Align a read range to file-cache block boundaries.
    /// @param[in] offset Requested read offset in bytes.
    /// @param[in] size Requested read size in bytes.
    /// @param[in] length Total file length in bytes.
    /// @return Pair of aligned start offset and aligned size.
    static std::pair<size_t, size_t> s_align_size(size_t offset, size_t size, size_t length);

    int64_t mtime() const override { return _remote_file_reader->mtime(); }

    // Asynchronously prefetch a range of file cache blocks.
    // This method triggers read file cache in dryrun mode to warm up the cache
    // without actually reading the data into user buffers.
    //
    // Parameters:
    //   offset: Starting offset in the file
    //   size: Number of bytes to prefetch
    //   io_ctx: IO context (can be nullptr, will create a dryrun context internally)
    //
    // Note: This is a best-effort operation. Errors are logged but not returned.
    void prefetch_range(size_t offset, size_t size, const IOContext* io_ctx = nullptr);

protected:
    /// Read bytes from cache when possible and fall back to peer/S3 when needed.
    /// @param[in] offset Start offset in the file.
    /// @param[out] result Destination buffer for the requested bytes.
    /// @param[out] bytes_read Number of bytes copied into result.
    /// @param[in] io_ctx IO context carrying dry-run, warmup and statistics options.
    /// @return OK on success; otherwise an error from cache lookup or remote read.
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    enum class FileCacheReadType {
        DATA,
        INVERTED_INDEX,
        SEGMENT_FOOTER_INDEX,
    };

    /// Initialize cache metadata for Doris-table files.
    /// @return None.
    void _init_doris_table_cache();

    /// Initialize cache metadata for external-table files.
    /// @param[in] opts Reader options used to choose cache key and cache base path.
    /// @return None.
    void _init_external_table_cache(const FileReaderOptions& opts);

    /// Check whether this reader can read cache files directly without get_or_set.
    /// @return true when direct cache-file reads are enabled for Doris-table files.
    bool _can_read_cache_file_directly() const;

    /// Decide whether remote cache miss reads should try peer cache first.
    /// @param[in] io_ctx IO context for warmup and request-mode checks.
    /// @return true if peer read is enabled for this request; otherwise false.
    bool _should_read_from_peer(const IOContext* io_ctx) const;

    /// Register a downloaded block in the direct-read map owned by this reader.
    /// @param[in] file_block Downloaded cache block to insert.
    /// @return None.
    void _insert_file_reader(FileBlockSPtr file_block);

    /// Try to satisfy the request by reading already downloaded cache files directly.
    /// @param[in] offset Requested file offset.
    /// @param[out] result Destination buffer for the request.
    /// @param[in] bytes_req Requested byte count.
    /// @param[in] is_dryrun True if local cache IO should be skipped.
    /// @param[in,out] stats Read statistics updated during the attempt.
    /// @param[in,out] already_read Bytes already filled into result by direct cache reads.
    /// @param[out] bytes_read Total bytes read when the whole request is satisfied directly.
    /// @return true if the whole request is completed by direct cache-file reads; otherwise false.
    bool _try_read_from_cached_files_directly(size_t offset, Slice result, size_t bytes_req,
                                              bool is_dryrun, ReadStatistics& stats,
                                              SourceReadBreakdown& source_read_breakdown,
                                              size_t& already_read, size_t* bytes_read);

    /// Collect blocks that still need remote data and update cache-hit statistics.
    /// @param[in] holder Cache blocks covering the aligned request range.
    /// @param[in,out] stats Read statistics updated according to block states.
    /// @return Blocks that should be fetched from peer/S3 by the current reader.
    std::vector<FileBlockSPtr> _collect_remote_read_blocks(const FileBlocksHolder& holder,
                                                           ReadStatistics& stats);

    /// Fetch missing blocks from peer/S3, write them into cache, and copy the overlap to result.
    /// @param[in] empty_blocks Blocks selected for remote fetch.
    /// @param[in] offset Original request offset.
    /// @param[in] bytes_req Original request size.
    /// @param[in] already_read Bytes already produced before this step.
    /// @param[out] result Destination buffer for the original request.
    /// @param[in] is_dryrun True if cache-file writes and local buffer copies should be skipped.
    /// @param[in,out] stats Read statistics updated for remote and local cache work.
    /// @param[in] io_ctx IO context passed to peer/S3 reads.
    /// @param[in,out] indirect_read_bytes Bytes copied into result through the indirect path.
    /// @param[out] empty_start Left boundary of the fetched contiguous empty range.
    /// @param[out] empty_end Right boundary of the fetched contiguous empty range.
    /// @param[out] peer_fetched_blocks Exact blocks fetched by peer in sparse mode; empty for S3.
    /// @return OK on success; otherwise an error from peer/S3 read.
    Status _read_remote_blocks_into_cache(const std::vector<FileBlockSPtr>& empty_blocks,
                                          size_t offset, size_t bytes_req, size_t already_read,
                                          Slice result, bool is_dryrun, ReadStatistics& stats,
                                          SourceReadBreakdown& source_read_breakdown,
                                          const IOContext* io_ctx, size_t& indirect_read_bytes,
                                          size_t& empty_start, size_t& empty_end,
                                          PeerFetchedBlockSet& peer_fetched_blocks);

    /// Read cached blocks that were not covered by the remote-fetch range, with remote fallback.
    /// @param[in] holder Cache blocks covering the aligned request range.
    /// @param[in] offset Original request offset.
    /// @param[in] bytes_req Original request size.
    /// @param[out] result Destination buffer for the original request.
    /// @param[in] is_dryrun True if local cache IO should be skipped.
    /// @param[in] empty_start Left boundary of the range already handled by remote fetch.
    /// @param[in] empty_end Right boundary of the range already handled by remote fetch.
    /// @param[in] peer_fetched_blocks Exact blocks already filled by peer; empty for S3 path.
    /// @param[in,out] stats Read statistics updated for wait, cache, and remote fallback paths.
    /// @param[in,out] indirect_read_bytes Bytes copied into result through this indirect stage.
    /// @param[out] bytes_read Total bytes covered for the original request after this stage.
    /// @return OK on success; otherwise an error from cache read or remote fallback read.
    Status _read_remaining_blocks_from_cache(const FileBlocksHolder& holder, size_t offset,
                                             size_t bytes_req, Slice result, bool is_dryrun,
                                             size_t empty_start, size_t empty_end,
                                             const PeerFetchedBlockSet& peer_fetched_blocks,
                                             ReadStatistics& stats,
                                             SourceReadBreakdown& source_read_breakdown,
                                             size_t& indirect_read_bytes, size_t* bytes_read,
                                             const IOContext* io_ctx);

    /// Read through the block-cache metadata path when direct cache-file reads are insufficient.
    /// @param[in] offset Original request offset.
    /// @param[out] result Destination buffer for the original request.
    /// @param[in] bytes_req Original request size.
    /// @param[in] already_read Bytes already produced by direct cache-file reads.
    /// @param[in] is_dryrun True if local cache IO should be skipped.
    /// @param[out] bytes_read Total bytes read for the request.
    /// @param[in,out] stats Read statistics updated across the indirect path.
    /// @param[in] io_ctx IO context passed to cache lookup and remote read.
    /// @return OK on success; otherwise an error from cache lookup or remote read.
    Status _read_from_indirect_cache(size_t offset, Slice result, size_t bytes_req,
                                     size_t already_read, bool is_dryrun, size_t* bytes_read,
                                     ReadStatistics& stats,
                                     SourceReadBreakdown& source_read_breakdown,
                                     const IOContext* io_ctx);

    /// Fall back to S3: clear peer_result, allocate buffer, and read from remote storage.
    /// @param[in] empty_start Start offset of the contiguous remote-read range.
    /// @param[in] span_size Size of the remote-read range in bytes.
    /// @param[in,out] buffer Span buffer that receives S3 data.
    /// @param[in,out] peer_result Cleared when non-null.
    /// @param[in,out] stats Read statistics updated for S3 execution.
    /// @param[in] io_ctx IO context passed to the remote reader.
    /// @return OK on success; otherwise the S3 read error.
    Status _execute_s3_fallback(size_t empty_start, size_t span_size,
                                std::unique_ptr<char[]>& buffer, PeerFetchResult* peer_result,
                                ReadStatistics& stats, const IOContext* io_ctx);

    /// Sequential peer-then-S3 fallback: try the best peer candidate, update affinity on
    /// success/failure, and fall back to S3 if peer fails.
    /// @param[in] empty_blocks Blocks whose data is missing from local cache.
    /// @param[in] empty_start Start offset of the contiguous remote-read range.
    /// @param[in] span_size Size of the remote-read range in bytes.
    /// @param[in,out] buffer Span buffer that receives S3 data on S3 fallback.
    /// @param[in,out] peer_result Peer payloads populated on peer success.
    /// @param[in,out] stats Read statistics updated for peer/S3 execution.
    /// @param[in] io_ctx IO context passed to the remote reader.
    /// @param[in] candidates Peer candidates sorted by affinity.
    /// @param[in] tablet_id Tablet ID for affinity tracking.
    /// @return OK on success; otherwise the S3 read error.
    Status _execute_sequential_peer_read(const std::vector<FileBlockSPtr>& empty_blocks,
                                         size_t empty_start, size_t span_size,
                                         std::unique_ptr<char[]>& buffer,
                                         PeerFetchResult* peer_result, ReadStatistics& stats,
                                         const IOContext* io_ctx,
                                         const std::vector<doris::PeerCandidate>& candidates,
                                         int64_t tablet_id);

    /// Execute a remote fetch for the contiguous empty range, trying peer first when enabled.
    /// @param[in] empty_blocks Blocks whose data is missing from local cache.
    /// @param[in] empty_start Start offset of the contiguous remote-read range for S3 fallback.
    /// @param[in] span_size Size of the enclosing contiguous remote-read range for S3 fallback.
    /// @param[in,out] buffer Temporary span buffer receiving S3 data.
    /// @param[in,out] peer_result Segmented peer payloads when the peer path succeeds.
    /// @param[in,out] stats Read statistics updated for peer/S3 execution.
    /// @param[in] io_ctx IO context passed to the remote reader.
    /// @return OK on success; otherwise the peer/S3 read error.
    Status _execute_remote_read(const std::vector<FileBlockSPtr>& empty_blocks, size_t empty_start,
                                size_t span_size, std::unique_ptr<char[]>& buffer,
                                PeerFetchResult* peer_result, ReadStatistics& stats,
                                const IOContext* io_ctx);

    /// Execute a winner race between peer read and S3 read for cross compute group scenarios.
    /// Launches both peer and S3 reads concurrently in bthreads and returns the first successful
    /// result. Uses bthread::Mutex and bthread::ConditionVariable for synchronization.
    /// @param[in] empty_blocks Blocks whose data is missing from local cache.
    /// @param[in] empty_start Start offset of the contiguous remote-read range.
    /// @param[in] span_size Size of the contiguous range for the S3 fallback read.
    /// @param[in,out] buffer Temporary span buffer that receives S3 data on S3 win.
    /// @param[out] peer_result Peer fetch payloads populated when peer wins.
    /// @param[in,out] stats Read statistics updated for the winning path.
    /// @param[in] io_ctx IO context passed to both peer and S3 reads.
    /// @param[in] candidates All peer candidates for the tablet.
    /// @return OK on success with buffer or peer_result populated; otherwise an error.
    Status _execute_winner_race(const std::vector<FileBlockSPtr>& empty_blocks, size_t empty_start,
                                size_t span_size, std::unique_ptr<char[]>& buffer,
                                PeerFetchResult* peer_result, ReadStatistics& stats,
                                const IOContext* io_ctx,
                                const std::vector<doris::PeerCandidate>& candidates,
                                int64_t tablet_id);

    /// Merge per-read statistics into the external file-cache statistics accumulator.
    /// @param[in] stats Statistics produced by the current read.
    /// @param[in,out] state Destination statistics accumulator; ignored when null.
    /// @param[in] read_type Logical file-cache read type used for fine-grained profile counters.
    /// @return None.
    void _update_stats(const ReadStatistics& stats,
                       const SourceReadBreakdown& source_read_breakdown, FileCacheStatistics* state,
                       FileCacheReadType read_type) const;

    bool _is_doris_table = false;
    int64_t _tablet_id = -1;
    std::string _storage_resource_id;
    FileReaderSPtr _remote_file_reader;
    UInt128Wrapper _cache_hash;
    BlockFileCache* _cache = nullptr;
    std::shared_mutex _mtx;
    std::map<size_t, FileBlockSPtr> _cache_file_readers;
};

} // namespace doris::io
