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

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/index/snii_query_test_util.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
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

    void xor_byte(size_t offset, uint8_t mask) { _data[offset] ^= static_cast<char>(mask); }

    void block_once_on_range_containing(size_t offset) {
        std::lock_guard<std::mutex> guard(_control_mu);
        _block_offset = offset;
        _block_armed = true;
        _blocked = false;
        _released = false;
        _matching_reads = 0;
    }

    bool wait_for_blocked_read(std::chrono::seconds timeout) {
        std::unique_lock<std::mutex> lock(_control_mu);
        return _control_cv.wait_for(lock, timeout, [&] { return _blocked; });
    }

    void release_blocked_read() {
        std::lock_guard<std::mutex> guard(_control_mu);
        _released = true;
        _control_cv.notify_all();
    }

    size_t matching_read_count() const {
        std::lock_guard<std::mutex> guard(_control_mu);
        return _matching_reads;
    }

    void add_file_cache_stats_sentinel(size_t offset, int64_t sentinel) {
        _file_cache_stats_sentinels.emplace_back(offset, sentinel);
    }

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

        {
            std::unique_lock<std::mutex> lock(_control_mu);
            if (_block_offset >= offset && _block_offset < offset + result.size) {
                ++_matching_reads;
                if (_block_armed) {
                    _block_armed = false;
                    _blocked = true;
                    _control_cv.notify_all();
                    _control_cv.wait(lock, [&] { return _released; });
                }
            }
        }

        if (result.size > 0) {
            std::memcpy(result.data, _data.data() + offset, result.size);
        }
        *bytes_read = result.size;

        // Parallel batch reads may invoke this from several pool threads at once;
        // guard the capture log so the test infrastructure is race-free.
        {
            std::lock_guard<std::mutex> guard(_reads_mu);
            _reads.push_back(read);
        }
        // Configured before batch workers start and read-only thereafter. Each
        // physical segment also gets its own stats slot, so these writes do not race.
        if (io_ctx != nullptr && io_ctx->file_cache_stats != nullptr) {
            for (const auto& [stats_offset, sentinel] : _file_cache_stats_sentinels) {
                if (stats_offset == offset) {
                    io_ctx->file_cache_stats->inverted_index_num_remote_io_total += sentinel;
                }
            }
        }
        if (_fail_offset.has_value() && *_fail_offset >= offset &&
            *_fail_offset < offset + result.size) {
            return Status::IOError("injected read failure at offset {}", *_fail_offset);
        }
        return Status::OK();
    }

public:
    // Fail any read whose range covers `offset` (after capturing it), so batch
    // tests can make exactly one physical segment error out.
    void set_fail_offset(size_t offset) { _fail_offset = offset; }

private:
    std::string _data;
    io::Path _path = "/tmp/snii_doris_adapter_test.idx";
    bool _closed = false;
    std::optional<size_t> _fail_offset;
    std::vector<std::pair<size_t, int64_t>> _file_cache_stats_sentinels;
    mutable std::mutex _reads_mu;
    std::vector<CapturedRead> _reads;
    mutable std::mutex _control_mu;
    std::condition_variable _control_cv;
    size_t _block_offset = std::numeric_limits<size_t>::max();
    bool _block_armed = false;
    bool _blocked = false;
    bool _released = false;
    size_t _matching_reads = 0;
};

class FollowerJoinedLatch {
public:
    static void notify(void* opaque) noexcept {
        auto* latch = static_cast<FollowerJoinedLatch*>(opaque);
        {
            std::lock_guard<std::mutex> guard(latch->_mutex);
            latch->_joined = true;
        }
        latch->_cv.notify_all();
    }

    bool wait_for(std::chrono::seconds timeout) {
        std::unique_lock<std::mutex> lock(_mutex);
        return _cv.wait_for(lock, timeout, [&] { return _joined; });
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _joined = false;
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

using PrxStatsSnapshot = std::array<int64_t, 11>;

PrxStatsSnapshot prx_stats_snapshot(const OlapReaderStatistics& stats) {
    return {stats.snii_stats.prx_raw_frames,      stats.snii_stats.prx_zstd_frames,
            stats.snii_stats.prx_pfor_frames,     stats.snii_stats.prx_plaintext_bytes,
            stats.snii_stats.prx_total_docs,      stats.snii_stats.prx_selected_docs,
            stats.snii_stats.prx_total_positions, stats.snii_stats.prx_selected_positions,
            stats.snii_stats.prx_fetch_ns,        stats.snii_stats.prx_decode_ns,
            stats.snii_stats.prx_phrase_verify_ns};
}

void set_prx_stats_sentinel(OlapReaderStatistics* stats) {
    stats->snii_stats.prx_raw_frames = 101;
    stats->snii_stats.prx_zstd_frames = 102;
    stats->snii_stats.prx_pfor_frames = 103;
    stats->snii_stats.prx_plaintext_bytes = 104;
    stats->snii_stats.prx_total_docs = 105;
    stats->snii_stats.prx_selected_docs = 106;
    stats->snii_stats.prx_total_positions = 107;
    stats->snii_stats.prx_selected_positions = 108;
    stats->snii_stats.prx_fetch_ns = 109;
    stats->snii_stats.prx_decode_ns = 110;
    stats->snii_stats.prx_phrase_verify_ns = 111;
}

std::vector<uint32_t> bitmap_docids(const roaring::Roaring& bitmap) {
    return {bitmap.begin(), bitmap.end()};
}

struct ActualQueryExecutionContext {
    ActualQueryExecutionContext() {
        TQueryOptions options;
        options.enable_inverted_index_query_cache = false;
        options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(options);
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
    }

    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
};

void init_actual_path_index_meta(TabletIndex* meta) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(31);
    pb.set_index_name("snii_doris_adapter_actual_path_idx");
    pb.add_col_unique_id(0);
    pb.mutable_properties()->insert({"parser", "english"});
    pb.mutable_properties()->insert({"lower_case", "true"});
    pb.mutable_properties()->insert({"support_phrase", "true"});
    meta->init_from_pb(pb);
}

Status resolve_prx_range(const doris::snii::reader::LogicalIndexReader& reader,
                         std::string_view term, doris::snii::io::Range* range) {
    bool found = false;
    doris::snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(reader.lookup(term, &found, &entry, &frq_base, &prx_base));
    if (!found) {
        return Status::NotFound("term {} missing from actual-path fixture", term);
    }
    doris::snii::format::FrqPreludeReader prelude;
    RETURN_IF_ERROR(doris::snii::reader::fetch_windowed_prelude(reader, entry, frq_base, &prelude));
    doris::snii::reader::WindowAbsRange window_range;
    RETURN_IF_ERROR(doris::snii::reader::windowed_window_range(reader, entry, frq_base, prx_base,
                                                               prelude, 0, /*want_positions=*/true,
                                                               /*want_freq=*/false, &window_range));
    range->offset = window_range.prx_off;
    range->len = window_range.prx_len;
    return Status::OK();
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

class SniiIndexReaderActualPathTest : public testing::Test {
protected:
    void SetUp() override {
        init_actual_path_index_meta(&_meta);
        constexpr uint32_t kDocCount = 9000;
        doris::snii::snii_test::MemoryFile memory_file;
        doris::snii::writer::SniiIndexInput input;
        input.index_id = 31;
        input.config = doris::snii::format::IndexConfig::kDocsPositions;
        input.doc_count = kDocCount;
        input.terms = {
                doris::snii::snii_test::make_term(
                        "failed", doris::snii::snii_test::docs_with_one_position(0, kDocCount, 0)),
                doris::snii::snii_test::make_term(
                        "order", doris::snii::snii_test::docs_with_one_position(0, kDocCount, 1))};
        doris::snii::writer::SniiCompoundWriter writer(&memory_file);
        const Status add_status = writer.add_logical_index(input);
        ASSERT_TRUE(add_status.ok()) << add_status.to_string();
        const Status finish_status = writer.finish();
        ASSERT_TRUE(finish_status.ok()) << finish_status.to_string();
        const auto& bytes = memory_file.data();
        std::string data(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        _recording_reader = std::make_shared<RecordingFileReader>(std::move(data));
        _adapter_reader = std::make_shared<DorisSniiFileReader>(_recording_reader);

        auto segment_reader = std::make_unique<doris::snii::reader::SniiSegmentReader>();
        const Status open_status = doris::snii::reader::SniiSegmentReader::open(
                _adapter_reader.get(), segment_reader.get());
        ASSERT_TRUE(open_status.ok()) << open_status.to_string();

        _file_reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                         "/tmp/snii_doris_adapter_actual_path",
                                                         InvertedIndexStorageFormatPB::SNII);
        _file_reader->_snii_file_reader = _adapter_reader;
        _file_reader->_snii_segment_reader = std::move(segment_reader);
        _file_reader->_inited = true;
        _index_reader = SniiIndexReader::create_shared(&_meta, _file_reader,
                                                       InvertedIndexReaderType::FULLTEXT);

        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());
    }

    void TearDown() override {
        _index_reader.reset();
        _file_reader.reset();
        _adapter_reader.reset();
        _recording_reader.reset();
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);
        _query_cache.reset();
    }

    doris::snii::io::Range prx_range(std::string_view term) {
        auto logical_reader = _file_reader->open_snii_index(&_meta);
        EXPECT_TRUE(logical_reader.has_value()) << logical_reader.error();
        doris::snii::io::Range range;
        if (logical_reader.has_value()) {
            const Status status = resolve_prx_range(*logical_reader.value(), term, &range);
            EXPECT_TRUE(status.ok()) << status.to_string();
        }
        return range;
    }

    Status run_phrase(const IndexQueryContextPtr& context, std::string_view column,
                      std::shared_ptr<roaring::Roaring>* bitmap) {
        Field query_value = Field::create_field<TYPE_STRING>(std::string("failed order"));
        return _index_reader->query(context, std::string(column), query_value,
                                    InvertedIndexQueryType::MATCH_PHRASE_QUERY, *bitmap);
    }

    TabletIndex _meta;
    std::shared_ptr<RecordingFileReader> _recording_reader;
    std::shared_ptr<DorisSniiFileReader> _adapter_reader;
    std::shared_ptr<IndexFileReader> _file_reader;
    std::shared_ptr<SniiIndexReader> _index_reader;
    InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

TEST_F(SniiIndexReaderActualPathTest, SingleFlightFollowerLeavesPrxStatsUnchanged) {
    const doris::snii::io::Range first_prx = prx_range("failed");
    ASSERT_GT(first_prx.len, 0U);
    _recording_reader->block_once_on_range_containing(first_prx.offset);

    ActualQueryExecutionContext leader;
    ActualQueryExecutionContext follower;
    set_prx_stats_sentinel(&follower.stats);
    const PrxStatsSnapshot follower_before = prx_stats_snapshot(follower.stats);
    std::shared_ptr<roaring::Roaring> leader_bitmap;
    std::shared_ptr<roaring::Roaring> follower_bitmap;
    FollowerJoinedLatch follower_joined;
    _index_reader->set_single_flight_follower_joined_observer_for_test(&FollowerJoinedLatch::notify,
                                                                       &follower_joined);

    auto leader_future = std::async(std::launch::async, [&] {
        return run_phrase(leader.context, "single_flight_content", &leader_bitmap);
    });
    const bool leader_blocked = _recording_reader->wait_for_blocked_read(std::chrono::seconds(5));
    if (!leader_blocked) {
        _recording_reader->release_blocked_read();
        const Status leader_status = leader_future.get();
        FAIL() << "leader did not reach configured PRX range: " << leader_status.to_string();
    }

    auto follower_future = std::async(std::launch::async, [&] {
        return run_phrase(follower.context, "single_flight_content", &follower_bitmap);
    });
    const bool follower_did_join = follower_joined.wait_for(std::chrono::seconds(5));
    if (!follower_did_join) {
        _recording_reader->release_blocked_read();
        const Status leader_status = leader_future.get();
        const Status follower_status = follower_future.get();
        FAIL() << "follower did not join single-flight: leader=" << leader_status.to_string()
               << ", follower=" << follower_status.to_string();
    }

    _recording_reader->release_blocked_read();
    const Status leader_status = leader_future.get();
    const Status follower_status = follower_future.get();
    ASSERT_TRUE(leader_status.ok()) << leader_status.to_string();
    ASSERT_TRUE(follower_status.ok()) << follower_status.to_string();
    ASSERT_NE(leader_bitmap, nullptr);
    ASSERT_NE(follower_bitmap, nullptr);
    EXPECT_EQ(bitmap_docids(*leader_bitmap), bitmap_docids(*follower_bitmap));
    EXPECT_GT(leader.stats.snii_stats.prx_plaintext_bytes, 0);
    EXPECT_EQ(prx_stats_snapshot(follower.stats), follower_before);
    EXPECT_EQ(_recording_reader->matching_read_count(), 1U);
}

TEST_F(SniiIndexReaderActualPathTest, LaterCorruptFrameFlushesEarlierSuccessfulFrame) {
    const doris::snii::io::Range first_prx = prx_range("failed");
    const doris::snii::io::Range second_prx = prx_range("order");
    ASSERT_GT(first_prx.len, 0U);
    ASSERT_GT(second_prx.len, 0U);
    ASSERT_NE(first_prx.offset, second_prx.offset);
    const uint64_t second_crc_byte = second_prx.offset + second_prx.len - 1;
    ASSERT_LT(second_crc_byte, _recording_reader->size());
    _recording_reader->xor_byte(static_cast<size_t>(second_crc_byte), 0x80);

    ActualQueryExecutionContext execution;
    std::shared_ptr<roaring::Roaring> bitmap;
    const Status status = run_phrase(execution.context, "late_error_content", &bitmap);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(execution.stats.snii_stats.prx_raw_frames +
                      execution.stats.snii_stats.prx_zstd_frames +
                      execution.stats.snii_stats.prx_pfor_frames,
              1);
    EXPECT_GT(execution.stats.snii_stats.prx_plaintext_bytes, 0);
    EXPECT_GT(execution.stats.snii_stats.prx_total_docs, 0);
    EXPECT_EQ(execution.stats.snii_stats.prx_phrase_verify_ns, 0);
}

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
        std::vector<::doris::snii::io::Range> ranges {{0, 4}, {6, 3}, {20, 2}};
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

// A direct-remote (NO_CACHE bypass) reader has no CachedRemoteFileReader below
// it, so it must count its own reads as physical remote bytes; a default reader
// must not (its wrapper does, or the bytes are local).
TEST(DorisSniiFileReaderTest, DirectRemoteReaderCountsPhysicalRemoteBytes) {
    auto recording_reader = std::make_shared<RecordingFileReader>("0123456789abcdef");
    DorisSniiFileReader direct_reader(recording_reader, /*io_ctx=*/nullptr,
                                      /*direct_remote_io=*/true);

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.file_cache_stats = &stats;

    std::vector<uint8_t> out;
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        auto status = direct_reader.read_at(2, 5, &out);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    EXPECT_EQ(stats.inverted_index_read_bytes, 5);
    EXPECT_EQ(stats.inverted_index_remote_physical_read_bytes, 5);

    io::FileCacheStatistics cached_stats;
    io::IOContext cached_io_ctx;
    cached_io_ctx.file_cache_stats = &cached_stats;
    DorisSniiFileReader cached_reader(recording_reader);
    {
        DorisSniiFileReader::ScopedIOContext scope(&cached_io_ctx);
        auto status = cached_reader.read_at(2, 5, &out);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    EXPECT_EQ(cached_stats.inverted_index_read_bytes, 5);
    EXPECT_EQ(cached_stats.inverted_index_remote_physical_read_bytes, 0);
}

// When one physical segment of a batch fails, lower-layer stats from every
// attempted segment must reach the caller, while logical counters record only
// what completed (including direct-remote physical bytes).
TEST(DorisSniiFileReaderTest, ReadBatchKeepsCompletedStatsWhenOneSegmentFails) {
    // Two ranges spaced far beyond the coalescing gap form two physical segments.
    constexpr int64_t successful_segment_sentinel = 101;
    constexpr int64_t failed_segment_sentinel = 1009;
    std::string data(8192, 'x');
    auto recording_reader = std::make_shared<RecordingFileReader>(std::move(data));
    recording_reader->add_file_cache_stats_sentinel(0, successful_segment_sentinel);
    recording_reader->add_file_cache_stats_sentinel(6000, failed_segment_sentinel);
    recording_reader->set_fail_offset(6000);
    DorisSniiFileReader reader(recording_reader, /*io_ctx=*/nullptr,
                               /*direct_remote_io=*/true);

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.file_cache_stats = &stats;

    std::vector<std::vector<uint8_t>> outs;
    Status status;
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        std::vector<::doris::snii::io::Range> ranges {{.offset = 0, .len = 4},
                                                      {.offset = 6000, .len = 8}};
        status = reader.read_batch(ranges, &outs);
    }
    EXPECT_FALSE(status.ok());
    const auto& reads = recording_reader->reads();
    ASSERT_EQ(reads.size(), 2);
    const auto successful_read =
            std::ranges::find_if(reads, [](const CapturedRead& read) { return read.offset == 0; });
    const auto failed_read = std::ranges::find_if(
            reads, [](const CapturedRead& read) { return read.offset == 6000; });
    ASSERT_NE(successful_read, reads.end());
    ASSERT_NE(failed_read, reads.end());
    ASSERT_NE(successful_read->io_ctx.file_cache_stats, nullptr);
    ASSERT_NE(failed_read->io_ctx.file_cache_stats, nullptr);
    EXPECT_NE(successful_read->io_ctx.file_cache_stats, &stats);
    EXPECT_NE(failed_read->io_ctx.file_cache_stats, &stats);
    EXPECT_NE(successful_read->io_ctx.file_cache_stats, failed_read->io_ctx.file_cache_stats);
    // Logical counters include only the completed segment. Lower-layer stats
    // written before either segment returns, including the failed one, are all merged.
    EXPECT_EQ(stats.inverted_index_request_bytes, 4);
    EXPECT_EQ(stats.inverted_index_read_bytes, 4);
    EXPECT_EQ(stats.inverted_index_remote_physical_read_bytes, 4);
    EXPECT_EQ(stats.inverted_index_range_read_count, 2);
    EXPECT_EQ(stats.inverted_index_serial_read_rounds, 1);
    EXPECT_EQ(stats.inverted_index_num_remote_io_total,
              successful_segment_sentinel + failed_segment_sentinel);
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
        std::vector<::doris::snii::io::Range> ranges {{0, 4}, {8192, 4}, {16384, 4}};
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
    std::vector<::doris::snii::io::Range> ranges {{100, 8}};
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
    std::vector<::doris::snii::io::Range> ranges {{0, 4}, {4, 4}, {9000, 4}};
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
    std::vector<::doris::snii::io::Range> ranges {{5, 0}};
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
    std::vector<::doris::snii::io::Range> ranges {{63, 100}};
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
    std::vector<::doris::snii::io::Range> ranges {{16384, 2}, {0, 4}, {8192, 3}};
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
        std::vector<::doris::snii::io::Range> ranges {{0, 4}, {8192, 4}, {16384, 4}};
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

    std::vector<::doris::snii::io::Range> ranges;
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

    std::vector<::doris::snii::io::Range> ranges;
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
