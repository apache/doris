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
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "io/cache/file_range_planner.h"
#include "io/fs/file_range_read_scheduler.h"
#include "io/fs/file_reader.h"
#include "storage/segment/column_read_ahead.h"

namespace doris {

class ExecEnv;
class StorageReadOptions;

namespace segment_v2 {

class ColumnIterator;

/// Invoked with the whole physical range at most once, when any registered page successfully
/// consumes bytes from it.
using ReadAheadRangeConsumer = std::function<void(const io::FileRangeRead&)>;
/// Creates submission-scoped state immediately before asynchronous range tasks are admitted.
using ReadAheadRangeConsumerFactory = std::function<ReadAheadRangeConsumer()>;
/// Returns true when the exact compressed data-page range is already in StoragePageCache.
using ReadAheadPageCacheProbe = std::function<bool(const io::FileRange& page_range)>;

struct SegmentReadAheadOptions {
    io::FileRangePlanOptions range_plan;
    ReadAheadPageCacheProbe page_cache_probe;
    /// Capture per-submission writeback state immediately before asynchronous IO starts. The
    /// returned consumer is retained only by ranges accepted in that submission.
    ReadAheadRangeConsumerFactory range_consumer_factory;
};

struct SegmentReadAheadResult {
    /// Data pages newly requested by all column windows before the page-cache probe.
    size_t new_pages {0};
    /// Newly requested pages already available from StoragePageCache.
    size_t page_cache_hits {0};
    /// Physical range handles returned for an accepted scheduler batch.
    size_t submitted_ranges {0};
    /// Sum of that batch's range sizes, including coalescing and block-fill bytes.
    size_t submitted_bytes {0};
    /// Batch-level scheduler rejection; NONE also covers plans that required no source reads.
    io::FileRangeReadRejectReason reject_reason {io::FileRangeReadRejectReason::NONE};
    /// Planning or batch-submission status. Per-range execution failures surface on page reads.
    Status status;

    bool accepted() const {
        return reject_reason == io::FileRangeReadRejectReason::NONE && status.ok();
    }
};

/// FileReader view used by PageIO. Exact data-page reads reuse an asynchronous range buffer;
/// unrelated reads and failed prefetches transparently use the original reader.
class SegmentReadAheadFileReader final : public io::FileReader {
public:
    Status close() override { return _inner->close(); }
    const io::Path& path() const override { return _inner->path(); }
    size_t size() const override { return _inner->size(); }
    bool closed() const override { return _inner->closed(); }
    int64_t mtime() const override { return _inner->mtime(); }
    const std::string& get_data_dir_path() override { return _inner->get_data_dir_path(); }

    const io::FileReaderSPtr& inner_reader() const { return _inner; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override;
    Status read_at_iobuf_impl(size_t offset, size_t bytes_req, butil::IOBuf* out,
                              size_t* bytes_read, const io::IOContext* io_ctx) override;
    void _collect_profile_at_runtime() override { _inner->collect_profile_at_runtime(); }
    void _collect_profile_before_close() override { _inner->collect_profile_before_close(); }

private:
    friend class SegmentReadAhead;

    struct PageKey {
        size_t offset {0};
        size_t size {0};

        bool operator<(const PageKey& other) const {
            return offset < other.offset || (offset == other.offset && size < other.size);
        }
    };

    struct PageOwner {
        ColumnReadAhead* column {nullptr};
        int32_t page_index {0};

        bool operator==(const PageOwner&) const = default;
    };

    struct BufferedRange {
        std::shared_ptr<io::FileRangeRead> read;
        ReadAheadRangeConsumer consumer;
        /// Guards the range-level consumer when several registered pages share this buffer.
        bool consumed {false};
    };

    struct PageSlot {
        std::vector<PageOwner> owners;
        std::shared_ptr<BufferedRange> range;
        size_t buffer_offset {0};
    };

    explicit SegmentReadAheadFileReader(io::FileReaderSPtr inner);
    /// Associate one exact page read with its location in a scheduled physical range.
    void _register_page(ColumnReadAhead* column, const ColumnReadAheadPage& page,
                        std::shared_ptr<BufferedRange> range, size_t buffer_offset);
    /// Drop one column's prediction after its scan has passed the page.
    void _release_page(ColumnReadAhead* column, const ColumnReadAheadPage& page);
    /// Serve an exact registered page, waiting for its range when necessary. A true return means
    /// the key was registered; `status` distinguishes successful buffer reuse from fallback.
    bool _try_read(const PageKey& key, Slice output, Status* status);
    /// Remove a page from every owning column and invoke the range consumer once after a successful
    /// read. Callbacks run without `_mutex`.
    void _finish_page(const PageKey& key, bool consumed);

    const io::FileReaderSPtr _inner;
    std::mutex _mutex;
    std::map<PageKey, PageSlot> _pages;
};

/// Coordinates the newly added pages of all physical columns in one segment submission. It does
/// not decide column roles or window sizes and does not own the BE-level asynchronous reader.
class SegmentReadAhead final {
public:
    SegmentReadAhead(io::FileReaderSPtr source_reader, io::FileRangeReadScheduler* scheduler,
                     std::shared_ptr<io::FileRangeReadContext> context,
                     io::FileRangeReadIOContext io_context, SegmentReadAheadOptions options,
                     ColumnReadAheadOptions eager_options = {},
                     ColumnReadAheadOptions lazy_options = {});

    /// Read the dynamic query read-ahead switch and cloud-mode requirement.
    static bool enabled();

    /// Create query read-ahead state when enabled for a query reader. On an inapplicable request,
    /// return OK with `output` cleared so callers continue with the original reader.
    static Status create_for_query(io::FileReaderSPtr source_reader, ExecEnv* exec_env,
                                   std::shared_ptr<io::FileRangeReadContext> context,
                                   const StorageReadOptions& read_options,
                                   std::unique_ptr<SegmentReadAhead>* output);

    SegmentReadAhead(const SegmentReadAhead&) = delete;
    SegmentReadAhead& operator=(const SegmentReadAhead&) = delete;

    /// Apply released pages first, probe new pages, then coalesce and submit all misses together.
    /// Planning or admission failure completes those pages and leaves PageIO to read them through
    /// the original reader.
    SegmentReadAheadResult apply_plans(std::vector<ColumnReadAheadPlan> plans);

    const std::shared_ptr<SegmentReadAheadFileReader>& file_reader() const { return _reader; }

    const ColumnReadAheadContext& column_context() const { return _column_context; }

private:
    const io::FileReaderSPtr _source_reader;
    io::FileRangeReadScheduler* const _scheduler;
    const std::shared_ptr<io::FileRangeReadContext> _context;
    const io::FileRangeReadIOContext _io_context;
    const SegmentReadAheadOptions _options;
    const std::shared_ptr<SegmentReadAheadFileReader> _reader;
    const ColumnReadAheadContext _column_context;
};

ReadAheadPageCacheProbe make_storage_page_cache_probe(const io::FileReaderSPtr& file_reader);

} // namespace segment_v2
} // namespace doris
