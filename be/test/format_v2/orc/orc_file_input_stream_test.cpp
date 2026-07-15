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

#include "format_v2/orc/orc_file_input_stream.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "runtime/runtime_profile.h"

namespace doris::format::orc {
namespace {

struct TestStream {
    uint64_t column_id;
    ::orc::StreamKind kind;
    uint64_t length;
};

class RecordingFileReader final : public io::FileReader {
public:
    explicit RecordingFileReader(size_t size) : _data(size) {
        for (size_t offset = 0; offset < size; ++offset) {
            _data[offset] = static_cast<char>(offset % 251);
        }
    }

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _data.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    const std::vector<io::PrefetchRange>& reads() const { return _reads; }
    char value_at(size_t offset) const { return _data[offset]; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        _reads.emplace_back(offset, std::min(offset + result.size, _data.size()));
        *bytes_read = std::min(result.size, _data.size() - offset);
        std::memcpy(result.data, _data.data() + offset, *bytes_read);
        return Status::OK();
    }

private:
    std::vector<char> _data;
    std::vector<io::PrefetchRange> _reads;
    bool _closed = false;
    io::Path _path = "/tmp/orc_v2_input_stream";
};

class TestStreamInformation final : public ::orc::StreamInformation {
public:
    TestStreamInformation(TestStream stream, uint64_t offset) : _stream(stream), _offset(offset) {}

    ::orc::StreamKind getKind() const override { return _stream.kind; }
    uint64_t getColumnId() const override { return _stream.column_id; }
    uint64_t getOffset() const override { return _offset; }
    uint64_t getLength() const override { return _stream.length; }

private:
    TestStream _stream;
    uint64_t _offset;
};

class TestStripeInformation final : public ::orc::StripeInformation {
public:
    TestStripeInformation(uint64_t offset, std::vector<TestStream> streams)
            : _offset(offset), _streams(std::move(streams)) {}

    uint64_t getOffset() const override { return _offset; }
    uint64_t getLength() const override {
        uint64_t length = 0;
        for (const auto& stream : _streams) {
            length += stream.length;
        }
        return length;
    }
    uint64_t getIndexLength() const override { return 0; }
    uint64_t getDataLength() const override { return getLength(); }
    uint64_t getFooterLength() const override { return 0; }
    uint64_t getNumberOfRows() const override { return 1; }
    uint64_t getNumberOfStreams() const override { return _streams.size(); }

    std::unique_ptr<::orc::StreamInformation> getStreamInformation(
            uint64_t stream_id) const override {
        uint64_t offset = _offset;
        for (uint64_t index = 0; index < stream_id; ++index) {
            offset += _streams[index].length;
        }
        return std::make_unique<TestStreamInformation>(_streams[stream_id], offset);
    }

    ::orc::ColumnEncodingKind getColumnEncoding(uint64_t col_id) const override {
        return ::orc::ColumnEncodingKind_DIRECT;
    }
    uint64_t getDictionarySize(uint64_t col_id) const override { return 0; }
    const std::string& getWriterTimezone() const override { return _timezone; }

private:
    uint64_t _offset;
    std::vector<TestStream> _streams;
    std::string _timezone = "UTC";
};

using StripeStreamMap = std::unordered_map<::orc::StreamId, std::shared_ptr<::orc::InputStream>>;

std::shared_ptr<::orc::InputStream> find_stream(const StripeStreamMap& streams, uint64_t column_id,
                                                ::orc::StreamKind kind) {
    auto it = streams.find(::orc::StreamId(column_id, kind));
    EXPECT_NE(it, streams.end());
    return it == streams.end() ? nullptr : it->second;
}

std::vector<bool> selected_columns(std::initializer_list<uint64_t> columns) {
    std::vector<bool> selected(8, false);
    for (uint64_t column : columns) {
        selected[column] = true;
    }
    return selected;
}

TEST(OrcFileInputStreamTest, MergesAdjacentSelectedStreams) {
    auto reader = std::make_shared<RecordingFileReader>(256);
    OrcFileInputStream input("test.orc", reader, nullptr, nullptr,
                             {.once_max_read_bytes = 16, .max_merge_distance_bytes = 0});
    StripeStreamMap streams;
    input.beforeReadStripe(std::make_unique<TestStripeInformation>(
                                   100, std::vector<TestStream> {{1, ::orc::StreamKind_DATA, 4},
                                                                 {2, ::orc::StreamKind_DATA, 4}}),
                           selected_columns({1, 2}), streams);

    auto second = find_stream(streams, 2, ::orc::StreamKind_DATA);
    ASSERT_NE(second, nullptr);
    std::array<char, 4> second_data {};
    second->read(second_data.data(), second_data.size(), 104);

    auto first = find_stream(streams, 1, ::orc::StreamKind_DATA);
    ASSERT_NE(first, nullptr);
    std::array<char, 4> first_data {};
    first->read(first_data.data(), first_data.size(), 100);

    ASSERT_EQ(reader->reads().size(), 1);
    EXPECT_EQ(reader->reads().front(), io::PrefetchRange(100, 108));
    EXPECT_EQ(second_data[0], reader->value_at(104));
    EXPECT_EQ(first_data[0], reader->value_at(100));
}

TEST(OrcFileInputStreamTest, SupportsRepeatedAndBackwardClusterReads) {
    auto reader = std::make_shared<RecordingFileReader>(256);
    OrcFileInputStream input("test.orc", reader, nullptr, nullptr,
                             {.once_max_read_bytes = 32, .max_merge_distance_bytes = 8});
    StripeStreamMap streams;
    input.beforeReadStripe(std::make_unique<TestStripeInformation>(
                                   40, std::vector<TestStream> {{1, ::orc::StreamKind_DATA, 4},
                                                                {0, ::orc::StreamKind_DATA, 3},
                                                                {2, ::orc::StreamKind_LENGTH, 5}}),
                           selected_columns({1, 2}), streams);

    auto later = find_stream(streams, 2, ::orc::StreamKind_LENGTH);
    auto earlier = find_stream(streams, 1, ::orc::StreamKind_DATA);
    ASSERT_NE(later, nullptr);
    ASSERT_NE(earlier, nullptr);

    std::array<char, 5> later_data {};
    later->read(later_data.data(), later_data.size(), 47);
    std::array<char, 2> repeated_data {};
    later->read(repeated_data.data(), repeated_data.size(), 48);
    std::array<char, 4> earlier_data {};
    earlier->read(earlier_data.data(), earlier_data.size(), 40);

    ASSERT_EQ(reader->reads().size(), 1);
    EXPECT_EQ(reader->reads().front(), io::PrefetchRange(40, 52));
    EXPECT_EQ(repeated_data[0], reader->value_at(48));
    EXPECT_EQ(earlier_data[0], reader->value_at(40));
}

TEST(OrcFileInputStreamTest, RespectsMergeDistanceAndMaximumSpan) {
    auto distance_reader = std::make_shared<RecordingFileReader>(256);
    OrcFileInputStream distance_input("test.orc", distance_reader, nullptr, nullptr,
                                      {.once_max_read_bytes = 32, .max_merge_distance_bytes = 2});
    StripeStreamMap distance_streams;
    distance_input.beforeReadStripe(
            std::make_unique<TestStripeInformation>(
                    0, std::vector<TestStream> {{1, ::orc::StreamKind_DATA, 4},
                                                {0, ::orc::StreamKind_DATA, 3},
                                                {2, ::orc::StreamKind_DATA, 4}}),
            selected_columns({1, 2}), distance_streams);
    std::array<char, 4> buffer {};
    find_stream(distance_streams, 1, ::orc::StreamKind_DATA)->read(buffer.data(), buffer.size(), 0);
    find_stream(distance_streams, 2, ::orc::StreamKind_DATA)->read(buffer.data(), buffer.size(), 7);
    ASSERT_EQ(distance_reader->reads().size(), 2);
    EXPECT_EQ(distance_reader->reads()[0], io::PrefetchRange(0, 4));
    EXPECT_EQ(distance_reader->reads()[1], io::PrefetchRange(7, 11));

    auto span_reader = std::make_shared<RecordingFileReader>(256);
    OrcFileInputStream span_input("test.orc", span_reader, nullptr, nullptr,
                                  {.once_max_read_bytes = 8, .max_merge_distance_bytes = 0});
    StripeStreamMap span_streams;
    span_input.beforeReadStripe(
            std::make_unique<TestStripeInformation>(
                    20, std::vector<TestStream> {{1, ::orc::StreamKind_DATA, 4},
                                                 {2, ::orc::StreamKind_DATA, 5}}),
            selected_columns({1, 2}), span_streams);
    find_stream(span_streams, 1, ::orc::StreamKind_DATA)->read(buffer.data(), buffer.size(), 20);
    std::array<char, 5> second_buffer {};
    find_stream(span_streams, 2, ::orc::StreamKind_DATA)
            ->read(second_buffer.data(), second_buffer.size(), 24);
    ASSERT_EQ(span_reader->reads().size(), 2);
    EXPECT_EQ(span_reader->reads()[0], io::PrefetchRange(20, 24));
    EXPECT_EQ(span_reader->reads()[1], io::PrefetchRange(24, 29));
}

TEST(OrcFileInputStreamTest, KeepsLargeAndSingleStreamsDirect) {
    auto reader = std::make_shared<RecordingFileReader>(256);
    OrcFileInputStream input("test.orc", reader, nullptr, nullptr,
                             {.once_max_read_bytes = 8, .max_merge_distance_bytes = 8});
    StripeStreamMap streams;
    input.beforeReadStripe(std::make_unique<TestStripeInformation>(
                                   60, std::vector<TestStream> {{1, ::orc::StreamKind_DATA, 9},
                                                                {2, ::orc::StreamKind_DATA, 4}}),
                           selected_columns({1, 2}), streams);

    std::array<char, 3> large_data {};
    find_stream(streams, 1, ::orc::StreamKind_DATA)->read(large_data.data(), large_data.size(), 62);
    std::array<char, 4> single_data {};
    find_stream(streams, 2, ::orc::StreamKind_DATA)
            ->read(single_data.data(), single_data.size(), 69);

    ASSERT_EQ(reader->reads().size(), 2);
    EXPECT_EQ(reader->reads()[0], io::PrefetchRange(62, 65));
    EXPECT_EQ(reader->reads()[1], io::PrefetchRange(69, 73));
}

TEST(OrcFileInputStreamTest, ZeroOnceMaxReadBytesKeepsStreamsDirect) {
    auto reader = std::make_shared<RecordingFileReader>(256);
    OrcFileInputStream input("test.orc", reader, nullptr, nullptr,
                             {.once_max_read_bytes = 0, .max_merge_distance_bytes = 16});
    StripeStreamMap streams;
    input.beforeReadStripe(std::make_unique<TestStripeInformation>(
                                   120, std::vector<TestStream> {{1, ::orc::StreamKind_DATA, 4},
                                                                 {2, ::orc::StreamKind_DATA, 4}}),
                           selected_columns({1, 2}), streams);

    std::array<char, 4> first_data {};
    find_stream(streams, 1, ::orc::StreamKind_DATA)
            ->read(first_data.data(), first_data.size(), 120);
    std::array<char, 4> second_data {};
    find_stream(streams, 2, ::orc::StreamKind_DATA)
            ->read(second_data.data(), second_data.size(), 124);

    ASSERT_EQ(reader->reads().size(), 2);
    EXPECT_EQ(reader->reads()[0], io::PrefetchRange(120, 124));
    EXPECT_EQ(reader->reads()[1], io::PrefetchRange(124, 128));
}

TEST(OrcFileInputStreamTest, SkipsUnselectedStreams) {
    auto reader = std::make_shared<RecordingFileReader>(256);
    OrcFileInputStream input("test.orc", reader, nullptr, nullptr,
                             {.once_max_read_bytes = 16, .max_merge_distance_bytes = 16});
    StripeStreamMap streams;
    input.beforeReadStripe(std::make_unique<TestStripeInformation>(
                                   80, std::vector<TestStream> {{1, ::orc::StreamKind_DATA, 4},
                                                                {2, ::orc::StreamKind_DATA, 4}}),
                           selected_columns({2}), streams);

    EXPECT_EQ(streams.size(), 1);
    EXPECT_EQ(streams.count(::orc::StreamId(1, ::orc::StreamKind_DATA)), 0);
    EXPECT_EQ(streams.count(::orc::StreamId(2, ::orc::StreamKind_DATA)), 1);
}

TEST(OrcFileInputStreamTest, RejectsInvalidStreamColumnId) {
    auto reader = std::make_shared<RecordingFileReader>(256);
    OrcFileInputStream input("test.orc", reader, nullptr, nullptr,
                             {.once_max_read_bytes = 16, .max_merge_distance_bytes = 16});
    StripeStreamMap streams;

    EXPECT_THROW(input.beforeReadStripe(
                         std::make_unique<TestStripeInformation>(
                                 80, std::vector<TestStream> {{9, ::orc::StreamKind_DATA, 4}}),
                         selected_columns({1, 2}), streams),
                 ::orc::ParseError);
}

TEST(OrcFileInputStreamTest, PublishesClusterProfileExactlyOnce) {
    auto reader = std::make_shared<RecordingFileReader>(256);
    RuntimeProfile profile("orc_v2_input_stream");
    {
        OrcFileInputStream input("test.orc", reader, nullptr, &profile,
                                 {.once_max_read_bytes = 16, .max_merge_distance_bytes = 0});
        StripeStreamMap streams;
        input.beforeReadStripe(
                std::make_unique<TestStripeInformation>(
                        100, std::vector<TestStream> {{1, ::orc::StreamKind_DATA, 4},
                                                      {2, ::orc::StreamKind_DATA, 4}}),
                selected_columns({1, 2}), streams);

        std::array<char, 4> data {};
        find_stream(streams, 2, ::orc::StreamKind_DATA)->read(data.data(), data.size(), 104);
        find_stream(streams, 1, ::orc::StreamKind_DATA)->read(data.data(), data.size(), 100);

        StripeStreamMap next_streams;
        input.beforeReadStripe(
                std::make_unique<TestStripeInformation>(200, std::vector<TestStream> {}),
                selected_columns({}), next_streams);
        ASSERT_NE(profile.get_counter("RequestIO"), nullptr);
        ASSERT_NE(profile.get_counter("MergedIO"), nullptr);
        EXPECT_EQ(profile.get_counter("RequestIO")->value(), 2);
        EXPECT_EQ(profile.get_counter("MergedIO")->value(), 1);
    }

    EXPECT_EQ(profile.get_counter("RequestIO")->value(), 2);
    EXPECT_EQ(profile.get_counter("MergedIO")->value(), 1);
}

} // namespace
} // namespace doris::format::orc
