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

#include "storage/segment/segment_read_ahead.h"

#include <butil/iobuf.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "util/threadpool.h"

namespace doris::segment_v2 {
namespace {

class TestFileReader final : public io::FileReader {
public:
    explicit TestFileReader(std::string data) : _data(std::move(data)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _data.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    void fail_next_read() { _fail_next_read = true; }
    size_t read_calls() const { return _read_calls; }
    const std::vector<io::FileRange>& reads() const { return _reads; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext*) override {
        std::lock_guard lock(_mutex);
        ++_read_calls;
        _reads.push_back({.offset = offset, .size = result.size});
        if (_fail_next_read.exchange(false)) {
            *bytes_read = 0;
            return Status::IOError("injected read failure");
        }
        DORIS_CHECK(offset <= _data.size());
        const size_t length = std::min(result.size, _data.size() - offset);
        std::memcpy(result.data, _data.data() + offset, length);
        *bytes_read = length;
        return Status::OK();
    }

private:
    const io::Path _path {"test-segment.dat"};
    const std::string _data;
    mutable std::mutex _mutex;
    std::vector<io::FileRange> _reads;
    std::atomic<bool> _fail_next_read {false};
    size_t _read_calls {0};
    bool _closed {false};
};

std::vector<ColumnReadAheadPage> make_pages(size_t page_count, size_t page_size,
                                            size_t first_offset = 0) {
    std::vector<ColumnReadAheadPage> pages;
    for (size_t index = 0; index < page_count; ++index) {
        pages.push_back({.page_index = cast_set<int32_t>(index),
                         .first_ordinal = cast_set<ordinal_t>(index * 100),
                         .last_ordinal = cast_set<ordinal_t>((index + 1) * 100 - 1),
                         .range = {.offset = first_offset + index * page_size, .size = page_size}});
    }
    return pages;
}

std::unique_ptr<ColumnReadAhead> make_window(size_t page_count, size_t page_size,
                                             size_t first_offset = 0) {
    std::unique_ptr<ColumnReadAhead> window;
    EXPECT_TRUE(ColumnReadAhead::create(make_pages(page_count, page_size, first_offset),
                                        {.high_watermark_bytes = page_size * page_count,
                                         .low_watermark_bytes = page_count == 1 ? 0 : page_size},
                                        false, &window)
                        .ok());
    return window;
}

ColumnReadAheadPlan plan_window(ColumnReadAhead* window, const roaring::Roaring& rows,
                                rowid_t current) {
    ColumnReadAheadPlan plan;
    window->plan(&current, 1, rows, &plan);
    return plan;
}

io::FileRangePlanOptions plan_options(size_t max_gap = 16) {
    return {.coalesce_options = {.max_gap_bytes = max_gap,
                                 .max_range_bytes = 1024,
                                 .max_read_amplification_ratio = 2.0},
            .cache_block_size = 1024,
            .block_fill_min_coverage = 1.0};
}

ThreadPool* read_ahead_test_executor() {
    static std::unique_ptr<ThreadPool> executor = []() {
        std::unique_ptr<ThreadPool> result;
        Status status = ThreadPoolBuilder("SegmentReadAheadTest")
                                .set_min_threads(4)
                                .set_max_threads(16)
                                .build(&result);
        DORIS_CHECK(status.ok());
        return result;
    }();
    return executor.get();
}

std::unique_ptr<io::FileRangeReadScheduler> make_scheduler(size_t max_bytes_per_query = 4096) {
    std::unique_ptr<io::FileRangeReadScheduler> scheduler;
    EXPECT_TRUE(io::FileRangeReadScheduler::create(
                        {.max_bytes_per_query = max_bytes_per_query, .max_bytes_per_be = 64 * 1024},
                        read_ahead_test_executor(), &scheduler)
                        .ok());
    return scheduler;
}

std::unique_ptr<SegmentReadAhead> make_segment_read_ahead(const io::FileReaderSPtr& source,
                                                          io::FileRangeReadScheduler* scheduler,
                                                          SegmentReadAheadOptions options) {
    auto context = scheduler->create_context();
    return std::make_unique<SegmentReadAhead>(source, scheduler, std::move(context),
                                              io::FileRangeReadIOContext {}, std::move(options));
}

roaring::Roaring rows(size_t count) {
    roaring::Roaring result;
    result.addRange(0, count);
    return result;
}

TEST(SegmentReadAheadTest, CoalescesColumnsAndServesExactPageSlices) {
    auto source = std::make_shared<TestFileReader>(std::string(256, 'x'));
    auto scheduler = make_scheduler();
    size_t consumer_factory_calls = 0;
    std::vector<io::FileRange> consumed_ranges;
    auto read_ahead = make_segment_read_ahead(
            source, scheduler.get(),
            {.range_plan = plan_options(), .page_cache_probe = {}, .range_consumer_factory = [&]() {
                 ++consumer_factory_calls;
                 return [&](const io::FileRange& range, Slice) {
                     consumed_ranges.push_back(range);
                 };
             }});
    auto first = make_window(1, 16, 0);
    auto second = make_window(1, 16, 24);
    const auto scan_rows = rows(100);
    auto first_plan = plan_window(first.get(), scan_rows, 0);
    auto second_plan = plan_window(second.get(), scan_rows, 0);

    const auto result = read_ahead->apply_plans({std::move(second_plan), std::move(first_plan)});

    ASSERT_TRUE(result.accepted()) << result.status;
    EXPECT_EQ(consumer_factory_calls, 1);
    EXPECT_EQ(result.submitted_ranges, 1);
    EXPECT_EQ(result.submitted_bytes, 40);
    char first_data[16] {};
    size_t bytes_read = 0;
    ASSERT_TRUE(read_ahead->file_reader()
                        ->read_at(0, Slice(first_data, sizeof(first_data)), &bytes_read)
                        .ok());
    EXPECT_EQ(bytes_read, sizeof(first_data));
    ASSERT_EQ(consumed_ranges.size(), 1);
    EXPECT_EQ(consumed_ranges[0], (io::FileRange {.offset = 0, .size = 40}));

    char second_data[16] {};
    ASSERT_TRUE(read_ahead->file_reader()
                        ->read_at(24, Slice(second_data, sizeof(second_data)), &bytes_read)
                        .ok());
    ASSERT_EQ(consumed_ranges.size(), 1);
    EXPECT_EQ(source->read_calls(), 1);
    EXPECT_EQ(source->reads(), (std::vector<io::FileRange> {{.offset = 0, .size = 40}}));
}

TEST(SegmentReadAheadTest, SharesOnePhysicalPageAcrossColumnOwners) {
    auto source = std::make_shared<TestFileReader>(std::string(64, 'x'));
    auto scheduler = make_scheduler();
    auto read_ahead = make_segment_read_ahead(
            source, scheduler.get(),
            {.range_plan = plan_options(), .page_cache_probe = {}, .range_consumer_factory = {}});
    auto first = make_window(1, 16, 8);
    auto second = make_window(1, 16, 8);
    const auto scan_rows = rows(100);
    auto first_plan = plan_window(first.get(), scan_rows, 0);
    auto second_plan = plan_window(second.get(), scan_rows, 0);

    const auto result = read_ahead->apply_plans({std::move(first_plan), std::move(second_plan)});

    ASSERT_TRUE(result.accepted()) << result.status;
    EXPECT_EQ(result.submitted_ranges, 1);
    EXPECT_EQ(result.submitted_bytes, 16);
    char data[16] {};
    size_t bytes_read = 0;
    ASSERT_TRUE(read_ahead->file_reader()->read_at(8, Slice(data, sizeof(data)), &bytes_read).ok());
    EXPECT_FALSE(first->pending(0));
    EXPECT_FALSE(second->pending(0));
    EXPECT_EQ(source->reads(), (std::vector<io::FileRange> {{.offset = 8, .size = 16}}));
}

TEST(SegmentReadAheadTest, UpdatesColumnsInOneCoalescedSubmission) {
    auto source = std::make_shared<TestFileReader>(std::string(256, 's'));
    auto scheduler = make_scheduler();
    auto read_ahead = make_segment_read_ahead(
            source, scheduler.get(),
            {.range_plan = plan_options(), .page_cache_probe = {}, .range_consumer_factory = {}});
    auto first = make_window(1, 16, 0);
    auto second = make_window(1, 16, 24);
    const auto scan_rows = rows(100);
    std::vector<ColumnReadAheadPlan> plans;
    plans.push_back(plan_window(first.get(), scan_rows, 0));
    plans.push_back(plan_window(second.get(), scan_rows, 0));

    static_cast<void>(read_ahead->apply_plans(std::move(plans)));

    char first_data[16] {};
    char second_data[16] {};
    size_t bytes_read = 0;
    ASSERT_TRUE(read_ahead->file_reader()
                        ->read_at(0, Slice(first_data, sizeof(first_data)), &bytes_read)
                        .ok());
    ASSERT_TRUE(read_ahead->file_reader()
                        ->read_at(24, Slice(second_data, sizeof(second_data)), &bytes_read)
                        .ok());
    EXPECT_EQ(source->reads(), (std::vector<io::FileRange> {{.offset = 0, .size = 40}}));
}

TEST(SegmentReadAheadTest, PageCacheHitCompletesWindowWithoutSubmission) {
    auto source = std::make_shared<TestFileReader>(std::string(64, 'p'));
    auto scheduler = make_scheduler();
    auto read_ahead =
            make_segment_read_ahead(source, scheduler.get(),
                                    {.range_plan = plan_options(),
                                     .page_cache_probe = [](const io::FileRange&) { return true; },
                                     .range_consumer_factory = {}});
    auto window = make_window(1, 16);
    const auto scan_rows = rows(100);
    auto plan = plan_window(window.get(), scan_rows, 0);

    const auto result = read_ahead->apply_plans({std::move(plan)});

    EXPECT_TRUE(result.accepted());
    EXPECT_EQ(result.page_cache_hits, 1);
    EXPECT_EQ(result.submitted_ranges, 0);
    EXPECT_EQ(window->pending_bytes(), 0);
    EXPECT_FALSE(window->pending(0));
}

TEST(SegmentReadAheadTest, AdmissionRejectionFallsBackWithoutWaiting) {
    auto source = std::make_shared<TestFileReader>(std::string(64, 'r'));
    auto scheduler = make_scheduler(8);
    auto read_ahead = make_segment_read_ahead(
            source, scheduler.get(),
            {.range_plan = plan_options(), .page_cache_probe = {}, .range_consumer_factory = {}});
    auto window = make_window(1, 16);
    const auto scan_rows = rows(100);
    auto plan = plan_window(window.get(), scan_rows, 0);

    const auto result = read_ahead->apply_plans({std::move(plan)});

    EXPECT_FALSE(result.accepted());
    EXPECT_EQ(result.reject_reason, io::FileRangeReadRejectReason::QUERY_BYTE_LIMIT);
    EXPECT_FALSE(window->pending(0));
    EXPECT_EQ(window->pending_bytes(), 0);
    char data[16] {};
    size_t bytes_read = 0;
    EXPECT_TRUE(read_ahead->file_reader()->read_at(0, Slice(data, sizeof(data)), &bytes_read).ok());
    EXPECT_EQ(source->read_calls(), 1);
}

TEST(SegmentReadAheadTest, AsyncReadFailureUsesOriginalReader) {
    auto source = std::make_shared<TestFileReader>(std::string(64, 'f'));
    source->fail_next_read();
    auto scheduler = make_scheduler();
    auto read_ahead = make_segment_read_ahead(
            source, scheduler.get(),
            {.range_plan = plan_options(), .page_cache_probe = {}, .range_consumer_factory = {}});
    auto window = make_window(1, 16);
    const auto scan_rows = rows(100);
    auto plan = plan_window(window.get(), scan_rows, 0);
    ASSERT_TRUE(read_ahead->apply_plans({std::move(plan)}).accepted());

    char data[16] {};
    size_t bytes_read = 0;
    const auto status =
            read_ahead->file_reader()->read_at(0, Slice(data, sizeof(data)), &bytes_read);

    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(bytes_read, sizeof(data));
    EXPECT_EQ(source->read_calls(), 2);
    EXPECT_FALSE(window->pending(0));
}

TEST(SegmentReadAheadTest, DiscardedUnusedRangeIsNotPublished) {
    auto source = std::make_shared<TestFileReader>(std::string(64, 'd'));
    auto scheduler = make_scheduler();
    size_t published = 0;
    auto read_ahead = make_segment_read_ahead(
            source, scheduler.get(),
            {.range_plan = plan_options(), .page_cache_probe = {}, .range_consumer_factory = [&]() {
                 return [&](const io::FileRange&, Slice) { ++published; };
             }});
    auto window = make_window(2, 16);
    const auto scan_rows = rows(200);
    auto first_plan = plan_window(window.get(), scan_rows, 0);
    ASSERT_TRUE(read_ahead->apply_plans({std::move(first_plan)}).accepted());

    const rowid_t later = 199;
    ColumnReadAheadPlan discard_plan;
    window->plan(&later, 1, scan_rows, &discard_plan);
    static_cast<void>(read_ahead->apply_plans({std::move(discard_plan)}));

    EXPECT_EQ(published, 0);
}

} // namespace
} // namespace doris::segment_v2
