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
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris::io {

struct IOContext;

enum class CacheBlockReadDirection : uint8_t {
    FORWARD = 0,
    BACKWARD = 1,
};

// One caller-visible data range in the underlying remote file.
//
// Segment readers build these ranges from data page pointers in ordinal indexes.
// The reader uses `offset` both as the remote-file range start and as the trigger
// position for prefetch progress, so callers do not need to pass segment ordinals
// into the IO layer.
struct FileAccessRange {
    size_t offset = 0;
    size_t size = 0;
};

struct CacheBlockReadPattern {
    CacheBlockReadDirection direction = CacheBlockReadDirection::FORWARD;
    std::vector<FileAccessRange> ranges;
};

struct CacheBlockPrefetchPolicy {
    // Target number of file cache blocks kept in the prefetch window, including
    // the block that contains the current read position. This is a soft cap: a
    // single file access range is never split even when it spans more blocks.
    size_t max_prefetch_blocks = 0;

    // The block size used to convert access ranges to file cache blocks. This
    // should normally be config::file_cache_each_block_size.
    size_t cache_block_size = 0;
};

struct CacheBlockRange {
    size_t offset = 0;
    size_t size = 0;
};

// Cached remote reader with cache-block-aware prefetch scheduling.
//
// Purpose:
//   CachedRemoteFileReader already knows how to warm a file cache block by
//   reading it in dry-run mode. This class adds an explicit read-pattern layer
//   above that primitive: callers describe the future file access ranges and a
//   prefetch policy. The reader translates the pattern to file cache blocks,
//   keeps a sliding window of blocks ahead of the current read_at() offset, and
//   submits one async prefetch task per cache block through
//   CachedRemoteFileReader::prefetch_range.
//
// Interface usage:
//   1. Build a CacheBlockReadPattern from higher-level metadata. For segment
//      scans, the segment layer converts selected row ids through the ordinal
//      index into FileAccessRange entries for data pages.
//   2. Give each physical column iterator its own
//      CacheBlockAwarePrefetchRemoteReader. A reader owns at most one pattern,
//      so multiple independently monotonic scan streams should not share this
//      object.
//   3. Call set_read_pattern() with a CacheBlockPrefetchPolicy before scanning.
//      Afterwards callers use the normal FileReader::read_at() API. Each read
//      advances the pattern by file offset and warms cache blocks until the
//      configured window is full. If one file range spans more cache blocks than
//      the window, the whole range is still prefetched so large data pages and
//      pages that cross block boundaries are not split.
//
// Usage example:
//   See BlockFileCacheTest.usage_example_read_at_automatically_prefetches_single_pattern in
//   be/test/io/cache/cache_block_aware_prefetch_remote_reader_test.cpp.
//
// This optimization intentionally spends more object-storage IOPS to expose more
// parallelism and therefore more bandwidth: instead of a scanner fetching a large
// cold segment serially, many independent S3 range reads are issued at the file
// cache block granularity. On cold scans where bandwidth is the bottleneck and
// IOPS headroom exists, this trades S3 IOPS for higher aggregate throughput.
class CacheBlockAwarePrefetchRemoteReader final : public CachedRemoteFileReader {
public:
    CacheBlockAwarePrefetchRemoteReader(FileReaderSPtr remote_file_reader,
                                        const FileReaderOptions& opts);

    ~CacheBlockAwarePrefetchRemoteReader() override = default;

    Status set_read_pattern(CacheBlockReadPattern pattern, const CacheBlockPrefetchPolicy& policy);

    void clear_read_pattern();

    bool has_read_pattern() const;

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    struct CacheBlockInfo {
        size_t block_id = 0;
        size_t trigger_offset = 0;
    };

    struct ReadPatternState {
        CacheBlockReadDirection direction = CacheBlockReadDirection::FORWARD;
        CacheBlockPrefetchPolicy policy;
        std::vector<CacheBlockInfo> block_sequence;
        int prefetched_index = -1;
        int current_block_index = 0;

        int window_size() const { return prefetched_index - current_block_index + 1; }
    };

    static std::vector<CacheBlockInfo> _build_block_sequence(CacheBlockReadPattern pattern,
                                                             size_t cache_block_size);

    static CacheBlockRange _block_id_to_range(size_t block_id, size_t cache_block_size) {
        return {block_id * cache_block_size, cache_block_size};
    }

    static std::vector<CacheBlockRange> _next_prefetch_ranges(ReadPatternState* state,
                                                              size_t current_file_offset);

    void _prefetch(size_t current_file_offset, const IOContext* io_ctx);

    mutable std::mutex _pattern_mutex;
    bool _has_pattern = false;
    ReadPatternState _pattern;
};

} // namespace doris::io
