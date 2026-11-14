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

#include "io/fs/read_ahead_range_buffer.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <string>

#include "io/fs/file_range_read_scheduler.h"
#include "io/fs/read_ahead_metrics.h"
#include "util/threadpool.h"

namespace doris::io {
namespace {

class RangeBufferFileReader final : public FileReader {
public:
    Status close() override { return Status::OK(); }
    const Path& path() const override { return _path; }
    size_t size() const override { return 128; }
    bool closed() const override { return false; }
    int64_t mtime() const override { return 0; }
    std::atomic<bool> fail {false};
    std::atomic<size_t> reads {0};

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext*) override {
        ++reads;
        if (fail) {
            return Status::IOError("injected range failure");
        }
        for (size_t i = 0; i < result.size; ++i) {
            result.data[i] = static_cast<char>(offset + i);
        }
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    const Path _path {"range-buffer-test"};
};

class ReadAheadRangeBufferTest : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(ThreadPoolBuilder("RangeBufferTest")
                            .set_min_threads(2)
                            .set_max_threads(4)
                            .build(&_pool)
                            .ok());
        ASSERT_TRUE(
                FileRangeReadScheduler::create({.max_bytes_per_query = 24, .max_bytes_per_be = 64},
                                               _pool.get(), &_scheduler)
                        .ok());
        _context = _scheduler->create_context();
        _statistics = std::make_shared<ReadAheadStatistics>();
        _source = std::make_shared<RangeBufferFileReader>();
    }
    void TearDown() override {
        _scheduler->shutdown();
        _pool->wait();
    }
    FileRangeReadSubmitResult submit(std::vector<FileRange> ranges) {
        FileRangeReadIOContext io_context;
        io_context.statistics = _statistics;
        return _scheduler->try_submit(ranges, _source, std::move(io_context), _context);
    }
    std::unique_ptr<ThreadPool> _pool;
    std::unique_ptr<FileRangeReadScheduler> _scheduler;
    std::shared_ptr<FileRangeReadContext> _context;
    std::shared_ptr<ReadAheadStatistics> _statistics;
    std::shared_ptr<RangeBufferFileReader> _source;
};

TEST_F(ReadAheadRangeBufferTest, ServesOriginalSlicesAndConsumesWholeRangeOnce) {
    auto submitted = submit({{.offset = 8, .size = 24}});
    ASSERT_TRUE(submitted.accepted());
    size_t consumed = 0;
    auto range = std::make_shared<ReadAheadRangeBuffer::Range>(ReadAheadRangeBuffer::Range {
            .read = submitted.reads[0], .consumer = [&](const FileRangeRead& read) {
                ++consumed;
                EXPECT_EQ(read.range(), (FileRange {.offset = 8, .size = 24}));
            }});
    ReadAheadRangeBuffer buffer(_statistics);
    buffer.register_slice({.offset = 8, .size = 8}, range, 0);
    buffer.register_slice({.offset = 24, .size = 8}, range, 16);
    char output[8];
    Status status;
    EXPECT_FALSE(buffer.try_read(9, Slice(output, 8), &status));
    EXPECT_FALSE(buffer.try_read(8, Slice(output, 7), &status));
    ASSERT_TRUE(buffer.try_read(24, Slice(output, 8), &status));
    ASSERT_TRUE(status.ok()) << status;
    for (size_t i = 0; i < 8; ++i) EXPECT_EQ(output[i], 24 + i);
    ASSERT_TRUE(buffer.try_read(8, Slice(output, 8), &status));
    ASSERT_TRUE(status.ok()) << status;
    for (size_t i = 0; i < 8; ++i) EXPECT_EQ(output[i], 8 + i);
    EXPECT_FALSE(buffer.contains({.offset = 8, .size = 8}));
    EXPECT_EQ(consumed, 1);
    EXPECT_EQ(_source->reads, 1);
    EXPECT_EQ(_statistics->consumed_page_bytes.value(), 16);
    EXPECT_EQ(_statistics->consumed_range_bytes.value(), 24);
}

TEST_F(ReadAheadRangeBufferTest, ReleaseAndDestructionDoNotPublishUnusedRanges) {
    auto submitted = submit({{.offset = 0, .size = 16}});
    ASSERT_TRUE(submitted.accepted());
    size_t consumed = 0;
    {
        ReadAheadRangeBuffer buffer(_statistics);
        auto range = std::make_shared<ReadAheadRangeBuffer::Range>(ReadAheadRangeBuffer::Range {
                .read = submitted.reads[0], .consumer = [&](const FileRangeRead&) { ++consumed; }});
        buffer.register_slice({.offset = 0, .size = 8}, range, 0);
        buffer.register_slice({.offset = 8, .size = 8}, range, 8);
        buffer.release({.offset = 0, .size = 8});
        EXPECT_FALSE(buffer.contains({.offset = 0, .size = 8}));
    }
    submitted.reads.clear();
    _scheduler->shutdown();
    _pool->wait();
    EXPECT_EQ(consumed, 0);
    EXPECT_EQ(_context->resident_bytes(), 0);
}

TEST_F(ReadAheadRangeBufferTest, FailedReadRetiresSliceWithoutPublishing) {
    _source->fail = true;
    auto submitted = submit({{.offset = 0, .size = 8}});
    ASSERT_TRUE(submitted.accepted());
    size_t consumed = 0;
    ReadAheadRangeBuffer buffer(_statistics);
    buffer.register_slice(
            {.offset = 0, .size = 8},
            std::make_shared<ReadAheadRangeBuffer::Range>(ReadAheadRangeBuffer::Range {
                    .read = submitted.reads[0],
                    .consumer = [&](const FileRangeRead&) { ++consumed; }}),
            0);
    char output[8];
    Status status;
    ASSERT_TRUE(buffer.try_read(0, Slice(output, 8), &status));
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(buffer.contains({.offset = 0, .size = 8}));
    EXPECT_EQ(consumed, 0);
}

TEST_F(ReadAheadRangeBufferTest, PartialAdmissionPreservesInputRangeMapping) {
    auto submitted = submit(
            {{.offset = 0, .size = 16}, {.offset = 32, .size = 16}, {.offset = 64, .size = 8}});
    ASSERT_TRUE(submitted.accepted());
    ASSERT_EQ(submitted.reads.size(), 3);
    ASSERT_NE(submitted.reads[0], nullptr);
    EXPECT_EQ(submitted.reads[1], nullptr);
    ASSERT_NE(submitted.reads[2], nullptr);
    ReadAheadRangeBuffer buffer(_statistics);
    buffer.register_slice(
            {.offset = 8, .size = 8},
            std::make_shared<ReadAheadRangeBuffer::Range>(
                    ReadAheadRangeBuffer::Range {.read = submitted.reads[0], .consumer = {}}),
            8);
    buffer.register_slice(
            {.offset = 64, .size = 8},
            std::make_shared<ReadAheadRangeBuffer::Range>(
                    ReadAheadRangeBuffer::Range {.read = submitted.reads[2], .consumer = {}}),
            0);
    char output[8];
    Status status;
    EXPECT_FALSE(buffer.try_read(32, Slice(output, 8), &status));
    ASSERT_TRUE(buffer.try_read(64, Slice(output, 8), &status));
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(output[0], 64);
    ASSERT_TRUE(buffer.try_read(8, Slice(output, 8), &status));
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(output[0], 8);
}

TEST_F(ReadAheadRangeBufferTest, IndependentBuffersShareQueryBudgetAndCancellation) {
    auto first = submit({{.offset = 0, .size = 16}});
    ASSERT_TRUE(first.accepted());
    auto second = submit({{.offset = 32, .size = 16}});
    EXPECT_FALSE(second.accepted());
    EXPECT_EQ(second.reject_reason, FileRangeReadRejectReason::QUERY_BYTE_LIMIT);
    EXPECT_FALSE(_context->cancelled());
    {
        ReadAheadRangeBuffer first_buffer(_statistics);
        first_buffer.register_slice(
                {.offset = 0, .size = 16},
                std::make_shared<ReadAheadRangeBuffer::Range>(
                        ReadAheadRangeBuffer::Range {.read = first.reads[0], .consumer = {}}),
                0);
    }
    // Destroying one column's registry does not cancel another column's query context.
    EXPECT_FALSE(_context->cancelled());
    auto smaller = submit({{.offset = 64, .size = 8}});
    ASSERT_TRUE(smaller.accepted());
    _context->cancel();
    EXPECT_TRUE(_context->cancelled());
    auto rejected = submit({{.offset = 80, .size = 8}});
    EXPECT_EQ(rejected.reject_reason, FileRangeReadRejectReason::QUERY_CANCELLED);
}

} // namespace
} // namespace doris::io
