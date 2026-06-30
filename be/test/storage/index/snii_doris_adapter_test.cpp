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

#include "storage/index/snii/snii_doris_adapter.h"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "io/io_common.h"
#include "snii/format/per_index_meta.h"
#include "snii/io/file_reader.h"
#include "util/slice.h"
#include "util/threadpool.h"

namespace doris::segment_v2::snii_doris {
namespace {

struct CapturedIOContext {
    bool has_ctx = false;
    bool is_inverted_index = false;
    bool is_index_data = false;
    bool read_file_cache = true;
    bool is_disposable = false;
    int64_t expiration_time = 0;
    io::FileCacheStatistics* file_cache_stats = nullptr;
};

struct CapturedRead {
    size_t offset = 0;
    size_t len = 0;
    CapturedIOContext io_ctx;
    // Destination buffer the read wrote into; used to prove single-segment reads
    // land directly in the caller's output (no temp/double buffer).
    const void* dst = nullptr;
    // Worker thread that served the read; supports parallel-path assertions.
    std::thread::id thread_id;
};

class RecordingFileReader final : public io::FileReader {
public:
    explicit RecordingFileReader(std::string data) : _data(std::move(data)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const io::Path& path() const override { return _path; }
    size_t size() const override { return _data.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    // Safe to read after read_batch returns: all worker reads happen-before the
    // caller (latch join) and writes are guarded by _reads_mu.
    const std::vector<CapturedRead>& reads() const { return _reads; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        CapturedRead read;
        read.offset = offset;
        read.len = result.size;
        read.dst = result.data;
        read.thread_id = std::this_thread::get_id();
        if (io_ctx != nullptr) {
            read.io_ctx.has_ctx = true;
            read.io_ctx.is_inverted_index = io_ctx->is_inverted_index;
            read.io_ctx.is_index_data = io_ctx->is_index_data;
            read.io_ctx.read_file_cache = io_ctx->read_file_cache;
            read.io_ctx.is_disposable = io_ctx->is_disposable;
            read.io_ctx.expiration_time = io_ctx->expiration_time;
            read.io_ctx.file_cache_stats = io_ctx->file_cache_stats;
        }

        if (result.size > 0) {
            std::memcpy(result.data, _data.data() + offset, result.size);
        }
        *bytes_read = result.size;

        // Parallel batch reads may invoke this from several pool threads at once;
        // guard the capture log so the test infrastructure is race-free.
        std::lock_guard<std::mutex> guard(_reads_mu);
        _reads.push_back(read);
        return Status::OK();
    }

private:
    std::string _data;
    io::Path _path = "/tmp/snii_doris_adapter_test.idx";
    bool _closed = false;
    mutable std::mutex _reads_mu;
    std::vector<CapturedRead> _reads;
};

// Deterministic, position-dependent byte pattern so every range's expected bytes
// are just `pattern.substr(offset, len)`.
std::string make_pattern(size_t n) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i) {
        s[i] = static_cast<char>('a' + static_cast<int>(i % 26));
    }
    return s;
}

std::string as_string(const std::vector<uint8_t>& v) {
    return std::string(v.begin(), v.end());
}

// Restores the batch-read executor seam on scope exit so an injected pool never
// leaks into sibling tests, even if an assertion returns early.
struct IoPoolSeamGuard {
    explicit IoPoolSeamGuard(ThreadPool* pool) {
        DorisSniiFileReader::set_io_thread_pool_for_test(pool);
    }
    ~IoPoolSeamGuard() { DorisSniiFileReader::set_io_thread_pool_for_test(nullptr); }
    IoPoolSeamGuard(const IoPoolSeamGuard&) = delete;
    IoPoolSeamGuard& operator=(const IoPoolSeamGuard&) = delete;
};

} // namespace

TEST(DorisSniiFileReaderTest, ReadAtPropagatesIndexIOContextAndRecordsStats) {
    auto recording_reader = std::make_shared<RecordingFileReader>("0123456789abcdef");
    DorisSniiFileReader reader(recording_reader);

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.is_disposable = true;
    io_ctx.is_index_data = true;
    io_ctx.read_file_cache = false;
    io_ctx.file_cache_stats = &stats;

    std::vector<uint8_t> out;
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        auto status = reader.read_at(2, 5, &out);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    ASSERT_EQ(out.size(), 5);
    EXPECT_EQ(std::string(out.begin(), out.end()), "23456");
    ASSERT_EQ(recording_reader->reads().size(), 1);
    const auto& captured = recording_reader->reads()[0].io_ctx;
    EXPECT_TRUE(captured.has_ctx);
    EXPECT_TRUE(captured.is_inverted_index);
    EXPECT_TRUE(captured.is_index_data);
    EXPECT_FALSE(captured.read_file_cache);
    EXPECT_TRUE(captured.is_disposable);
    EXPECT_EQ(captured.file_cache_stats, &stats);

    EXPECT_EQ(stats.inverted_index_request_bytes, 5);
    EXPECT_EQ(stats.inverted_index_read_bytes, 5);
    EXPECT_EQ(stats.inverted_index_range_read_count, 1);
    EXPECT_EQ(stats.inverted_index_serial_read_rounds, 1);
}

TEST(DorisSniiFileReaderTest, ReadBatchRecordsLogicalAndCoalescedPhysicalIO) {
    auto recording_reader =
            std::make_shared<RecordingFileReader>("0123456789abcdefghijklmnopqrstuvwxyz");
    DorisSniiFileReader reader(recording_reader);

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.file_cache_stats = &stats;

    std::vector<std::vector<uint8_t>> outs;
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        std::vector<::snii::io::Range> ranges {{0, 4}, {6, 3}, {20, 2}};
        auto status = reader.read_batch(ranges, &outs);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    ASSERT_EQ(outs.size(), 3);
    EXPECT_EQ(std::string(outs[0].begin(), outs[0].end()), "0123");
    EXPECT_EQ(std::string(outs[1].begin(), outs[1].end()), "678");
    EXPECT_EQ(std::string(outs[2].begin(), outs[2].end()), "kl");

    ASSERT_EQ(recording_reader->reads().size(), 1);
    EXPECT_EQ(recording_reader->reads()[0].offset, 0);
    EXPECT_EQ(recording_reader->reads()[0].len, 22);

    EXPECT_EQ(stats.inverted_index_request_bytes, 9);
    EXPECT_EQ(stats.inverted_index_read_bytes, 22);
    EXPECT_EQ(stats.inverted_index_range_read_count, 1);
    EXPECT_EQ(stats.inverted_index_serial_read_rounds, 1);
}

// FB-02: three ranges spaced >4096 apart form three disjoint physical segments.
// Each is still a separate physical read, but the batch is one concurrent round
// (F19): serial_read_rounds drops from K(==3) to 1.
TEST(DorisSniiFileReaderTest, ReadBatchIssuesSingleSerialRoundForDisjointSegments) {
    const std::string data = make_pattern(20000);
    auto recording_reader = std::make_shared<RecordingFileReader>(data);
    DorisSniiFileReader reader(recording_reader);

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.file_cache_stats = &stats;

    std::vector<std::vector<uint8_t>> outs;
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        std::vector<::snii::io::Range> ranges {{0, 4}, {8192, 4}, {16384, 4}};
        auto status = reader.read_batch(ranges, &outs);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    ASSERT_EQ(outs.size(), 3);
    EXPECT_EQ(as_string(outs[0]), data.substr(0, 4));
    EXPECT_EQ(as_string(outs[1]), data.substr(8192, 4));
    EXPECT_EQ(as_string(outs[2]), data.substr(16384, 4));

    EXPECT_EQ(recording_reader->reads().size(), 3);
    EXPECT_EQ(stats.inverted_index_request_bytes, 12);
    EXPECT_EQ(stats.inverted_index_read_bytes, 12);
    EXPECT_EQ(stats.inverted_index_range_read_count, 3);
    EXPECT_EQ(stats.inverted_index_serial_read_rounds, 1);
}

// F27: a single-range group reads straight into the caller's output slot, with no
// temporary buffer and no second memcpy. Proven by destination-pointer identity.
TEST(DorisSniiFileReaderTest, ReadBatchSingleSegmentReadsInPlace) {
    const std::string data = make_pattern(20000);
    auto recording_reader = std::make_shared<RecordingFileReader>(data);
    DorisSniiFileReader reader(recording_reader);

    std::vector<std::vector<uint8_t>> outs;
    std::vector<::snii::io::Range> ranges {{100, 8}};
    auto status = reader.read_batch(ranges, &outs);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(outs.size(), 1);
    EXPECT_EQ(as_string(outs[0]), data.substr(100, 8));
    ASSERT_EQ(recording_reader->reads().size(), 1);
    // The read wrote directly into outs[0]'s storage (no double buffer).
    EXPECT_EQ(recording_reader->reads()[0].dst, outs[0].data());
}

// FB-03: a batch mixing one coalesced group (temp + scatter) and one single-range
// group (direct read). Both branches produce correct bytes.
TEST(DorisSniiFileReaderTest, ReadBatchMixedSingleAndCoalescedGroups) {
    const std::string data = make_pattern(20000);
    auto recording_reader = std::make_shared<RecordingFileReader>(data);
    DorisSniiFileReader reader(recording_reader);

    std::vector<std::vector<uint8_t>> outs;
    std::vector<::snii::io::Range> ranges {{0, 4}, {4, 4}, {9000, 4}};
    auto status = reader.read_batch(ranges, &outs);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(outs.size(), 3);
    EXPECT_EQ(as_string(outs[0]), data.substr(0, 4));
    EXPECT_EQ(as_string(outs[1]), data.substr(4, 4));
    EXPECT_EQ(as_string(outs[2]), data.substr(9000, 4));

    // Two physical segments: the coalesced [0,8) group and the single [9000,9004).
    ASSERT_EQ(recording_reader->reads().size(), 2);
    // The single-range group [9000,9004) read directly into outs[2].
    const auto& reads = recording_reader->reads();
    bool single_in_place = false;
    for (const auto& r : reads) {
        if (r.offset == 9000) {
            single_in_place = (r.dst == outs[2].data());
        }
    }
    EXPECT_TRUE(single_in_place);
}

// FB-04: empty batch and zero-length ranges. No physical reads; zero-length slots
// stay empty; outs aligns 1:1 with the input.
TEST(DorisSniiFileReaderTest, ReadBatchHandlesEmptyAndZeroLengthRanges) {
    auto recording_reader = std::make_shared<RecordingFileReader>(make_pattern(64));
    DorisSniiFileReader reader(recording_reader);

    std::vector<std::vector<uint8_t>> empty_outs;
    auto empty_status = reader.read_batch({}, &empty_outs);
    ASSERT_TRUE(empty_status.ok()) << empty_status.to_string();
    EXPECT_TRUE(empty_outs.empty());
    EXPECT_EQ(recording_reader->reads().size(), 0);

    std::vector<std::vector<uint8_t>> outs;
    std::vector<::snii::io::Range> ranges {{5, 0}};
    auto status = reader.read_batch(ranges, &outs);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(outs.size(), 1);
    EXPECT_TRUE(outs[0].empty());
    EXPECT_EQ(recording_reader->reads().size(), 0);
}

// FB-05: an out-of-range request surfaces a corruption error and does not crash.
TEST(DorisSniiFileReaderTest, ReadBatchReturnsErrorForOutOfRange) {
    auto recording_reader = std::make_shared<RecordingFileReader>(make_pattern(64));
    DorisSniiFileReader reader(recording_reader);

    std::vector<std::vector<uint8_t>> outs;
    std::vector<::snii::io::Range> ranges {{63, 100}};
    auto status = reader.read_batch(ranges, &outs);
    EXPECT_FALSE(status.ok());
}

// FB-06: unsorted input still produces outputs in the caller's original index
// order (the skip-sort guard only avoids the sort when already sorted).
TEST(DorisSniiFileReaderTest, ReadBatchPreservesOriginalOrderForUnsortedInput) {
    const std::string data = make_pattern(20000);
    auto recording_reader = std::make_shared<RecordingFileReader>(data);
    DorisSniiFileReader reader(recording_reader);

    std::vector<std::vector<uint8_t>> outs;
    std::vector<::snii::io::Range> ranges {{16384, 2}, {0, 4}, {8192, 3}};
    auto status = reader.read_batch(ranges, &outs);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(outs.size(), 3);
    EXPECT_EQ(as_string(outs[0]), data.substr(16384, 2));
    EXPECT_EQ(as_string(outs[1]), data.substr(0, 4));
    EXPECT_EQ(as_string(outs[2]), data.substr(8192, 3));
}

// IOContext passthrough: every per-segment read sees the caller's flags. Each
// segment is routed through a private FileCacheStatistics slot (so disjoint reads
// never race), which is then merged back into the caller's sink.
TEST(DorisSniiFileReaderTest, ReadBatchPropagatesIOContextFlagsPerSegment) {
    const std::string data = make_pattern(20000);
    auto recording_reader = std::make_shared<RecordingFileReader>(data);
    DorisSniiFileReader reader(recording_reader);

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.is_index_data = true;
    io_ctx.read_file_cache = false;
    io_ctx.is_disposable = true;
    io_ctx.expiration_time = 123;
    io_ctx.file_cache_stats = &stats;

    std::vector<std::vector<uint8_t>> outs;
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        std::vector<::snii::io::Range> ranges {{0, 4}, {8192, 4}, {16384, 4}};
        auto status = reader.read_batch(ranges, &outs);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    const auto& reads = recording_reader->reads();
    ASSERT_EQ(reads.size(), 3);
    for (const auto& r : reads) {
        EXPECT_TRUE(r.io_ctx.has_ctx);
        EXPECT_TRUE(r.io_ctx.is_inverted_index);
        EXPECT_TRUE(r.io_ctx.is_index_data);
        EXPECT_FALSE(r.io_ctx.read_file_cache);
        EXPECT_TRUE(r.io_ctx.is_disposable);
        EXPECT_EQ(r.io_ctx.expiration_time, 123);
        // Per-segment private stats slot, not the caller's sink directly.
        EXPECT_NE(r.io_ctx.file_cache_stats, nullptr);
        EXPECT_NE(r.io_ctx.file_cache_stats, &stats);
    }
    // Aggregate counters still land on the caller's sink after the merge.
    EXPECT_EQ(stats.inverted_index_range_read_count, 3);
    EXPECT_EQ(stats.inverted_index_serial_read_rounds, 1);
}

// FB-08: with a real injected pool, eight disjoint segments are read in parallel.
// All bytes are correct, every segment is a distinct physical read, and the batch
// is one concurrent round. Run under TSAN to prove thread-safety.
TEST(DorisSniiFileReaderConcurrencyTest, ParallelSegmentReadsAreThreadSafe) {
    const std::string data = make_pattern(60000);
    auto recording_reader = std::make_shared<RecordingFileReader>(data);
    DorisSniiFileReader reader(recording_reader);

    std::unique_ptr<ThreadPool> pool;
    auto pool_st =
            ThreadPoolBuilder("snii_batch_test").set_min_threads(2).set_max_threads(4).build(&pool);
    ASSERT_TRUE(pool_st.ok()) << pool_st.to_string();
    IoPoolSeamGuard seam(pool.get());

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.file_cache_stats = &stats;

    std::vector<::snii::io::Range> ranges;
    for (size_t i = 0; i < 8; ++i) {
        ranges.push_back({static_cast<uint64_t>(i) * 8192, 4});
    }

    std::vector<std::vector<uint8_t>> outs;
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        auto status = reader.read_batch(ranges, &outs);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    ASSERT_EQ(outs.size(), 8);
    for (size_t i = 0; i < 8; ++i) {
        EXPECT_EQ(as_string(outs[i]), data.substr(i * 8192, 4));
    }
    EXPECT_EQ(recording_reader->reads().size(), 8);
    EXPECT_EQ(stats.inverted_index_range_read_count, 8);
    EXPECT_EQ(stats.inverted_index_serial_read_rounds, 1);
}

// FB-07: the injected-pool (parallel) path and the seam-nullptr path both produce
// the exact same outputs (and match ground truth) -- parallelism is invisible.
TEST(DorisSniiFileReaderConcurrencyTest, ParallelPathMatchesSerialPath) {
    const std::string data = make_pattern(60000);
    auto recording_reader = std::make_shared<RecordingFileReader>(data);
    DorisSniiFileReader reader(recording_reader);

    std::vector<::snii::io::Range> ranges;
    std::vector<std::vector<uint8_t>> expected;
    for (size_t i = 0; i < 8; ++i) {
        const uint64_t off = static_cast<uint64_t>(i) * 8192;
        ranges.push_back({off, 4});
        const std::string chunk = data.substr(off, 4);
        expected.emplace_back(chunk.begin(), chunk.end());
    }

    std::vector<std::vector<uint8_t>> serial_outs;
    {
        IoPoolSeamGuard seam(nullptr); // no injected pool
        auto status = reader.read_batch(ranges, &serial_outs);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    std::unique_ptr<ThreadPool> pool;
    auto pool_st =
            ThreadPoolBuilder("snii_batch_test").set_min_threads(2).set_max_threads(4).build(&pool);
    ASSERT_TRUE(pool_st.ok()) << pool_st.to_string();

    std::vector<std::vector<uint8_t>> parallel_outs;
    {
        IoPoolSeamGuard seam(pool.get());
        auto status = reader.read_batch(ranges, &parallel_outs);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    EXPECT_EQ(serial_outs, expected);
    EXPECT_EQ(parallel_outs, expected);
    EXPECT_EQ(serial_outs, parallel_outs);
}

} // namespace doris::segment_v2::snii_doris
