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
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "io/io_common.h"
#include "snii/io/file_reader.h"
#include "util/slice.h"

namespace doris::segment_v2::snii_doris {
namespace {

struct CapturedIOContext {
    bool has_ctx = false;
    bool is_inverted_index = false;
    bool is_index_data = false;
    bool read_file_cache = true;
    bool is_disposable = false;
    io::FileCacheStatistics* file_cache_stats = nullptr;
};

struct CapturedRead {
    size_t offset = 0;
    size_t len = 0;
    CapturedIOContext io_ctx;
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

    const std::vector<CapturedRead>& reads() const { return _reads; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        CapturedRead read;
        read.offset = offset;
        read.len = result.size;
        if (io_ctx != nullptr) {
            read.io_ctx.has_ctx = true;
            read.io_ctx.is_inverted_index = io_ctx->is_inverted_index;
            read.io_ctx.is_index_data = io_ctx->is_index_data;
            read.io_ctx.read_file_cache = io_ctx->read_file_cache;
            read.io_ctx.is_disposable = io_ctx->is_disposable;
            read.io_ctx.file_cache_stats = io_ctx->file_cache_stats;
        }
        _reads.push_back(read);

        if (result.size > 0) {
            std::memcpy(result.data, _data.data() + offset, result.size);
        }
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    std::string _data;
    io::Path _path = "/tmp/snii_doris_adapter_test.idx";
    bool _closed = false;
    std::vector<CapturedRead> _reads;
};

} // namespace

TEST(DorisSniiFileReaderTest, ReadAtPropagatesIndexIOContextAndRecordsStats) {
    auto recording_reader = std::make_shared<RecordingFileReader>("0123456789abcdef");
    DorisSniiFileReader reader(recording_reader);

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.is_disposable = true;
    io_ctx.is_index_data = false;
    io_ctx.read_file_cache = false;
    io_ctx.file_cache_stats = &stats;

    std::vector<uint8_t> out;
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        auto status = reader.read_at(2, 5, &out);
        ASSERT_TRUE(status.ok()) << status.message();
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
        ASSERT_TRUE(status.ok()) << status.message();
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

} // namespace doris::segment_v2::snii_doris
