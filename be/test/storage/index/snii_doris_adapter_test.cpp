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
#include "snii/format/per_index_meta.h"
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
    int64_t expiration_time = 0;
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
            read.io_ctx.expiration_time = io_ctx->expiration_time;
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

void assert_read_ok(DorisSniiFileReader* reader, uint64_t offset, std::vector<uint8_t>* out) {
    auto status = reader->read_at(offset, 1, out);
    ASSERT_TRUE(status.ok()) << status.message();
}

void expect_captured_io_context_eq(const CapturedIOContext& actual,
                                   const CapturedIOContext& expected) {
    EXPECT_EQ(actual.has_ctx, expected.has_ctx);
    EXPECT_EQ(actual.is_inverted_index, expected.is_inverted_index);
    EXPECT_EQ(actual.is_index_data, expected.is_index_data);
    EXPECT_EQ(actual.read_file_cache, expected.read_file_cache);
    EXPECT_EQ(actual.is_disposable, expected.is_disposable);
    EXPECT_EQ(actual.expiration_time, expected.expiration_time);
    EXPECT_EQ(actual.file_cache_stats, expected.file_cache_stats);
}

} // namespace

TEST(DorisSniiFileReaderTest, ReadAtPropagatesIndexIOContextAndRecordsStats) {
    auto recording_reader = std::make_shared<RecordingFileReader>("0123456789abcdef");
    DorisSniiFileReader reader(recording_reader);

    io::FileCacheStatistics stats;
    io::IOContext io_ctx;
    io_ctx.is_disposable = true;
    io_ctx.is_index_data = false;
    io_ctx.read_file_cache = false;
    io_ctx.snii_section_type = io::SNII_SECTION_META;
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

TEST(DorisSniiFileReaderTest, SectionIOContextRoutesOnlyMetaToIndexQueue) {
    auto recording_reader = std::make_shared<RecordingFileReader>(
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
    DorisSniiFileReader reader(recording_reader);

    snii::format::SectionRefs refs;
    refs.dict_region = {.offset = 10, .length = 2};
    refs.posting_region = {.offset = 20, .length = 2};
    refs.bsbf = {.offset = 30, .length = 2};
    refs.norms = {.offset = 40, .length = 2};
    refs.null_bitmap = {.offset = 50, .length = 2};
    reader.register_section_refs(refs);

    io::IOContext io_ctx;
    io_ctx.is_index_data = false;
    io_ctx.read_file_cache = true;
    io_ctx.snii_section_type = io::SNII_SECTION_META;

    std::vector<uint8_t> out;
    const std::vector<uint64_t> offsets {0,
                                         refs.dict_region.offset,
                                         refs.posting_region.offset,
                                         refs.bsbf.offset,
                                         refs.norms.offset,
                                         refs.null_bitmap.offset};
    {
        DorisSniiFileReader::ScopedIOContext scope(&io_ctx);
        for (uint64_t offset : offsets) {
            assert_read_ok(&reader, offset, &out);
        }
    }

    ASSERT_EQ(recording_reader->reads().size(), offsets.size());
    expect_captured_io_context_eq(recording_reader->reads()[0].io_ctx, {.has_ctx = true,
                                                                        .is_inverted_index = true,
                                                                        .is_index_data = true,
                                                                        .read_file_cache = true});

    for (size_t i = 1; i < recording_reader->reads().size(); ++i) {
        expect_captured_io_context_eq(recording_reader->reads()[i].io_ctx,
                                      {.has_ctx = true,
                                       .is_inverted_index = true,
                                       .is_index_data = false,
                                       .read_file_cache = true});
    }
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
