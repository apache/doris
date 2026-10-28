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

#include "format_v2/blob/blob_file_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "format_v2/blob/blob_test_util.h"
#include "io/io_common.h"

namespace doris::format {
namespace {

using blob_test::encode_record_lengths;
using blob_test::finish_blob_file;
using blob_test::make_blob_file;

class MemoryBlobFile final : public io::FileReader {
public:
    // A nonzero offset represents a sparse file: only its index/footer are backed by memory.
    explicit MemoryBlobFile(std::string bytes, size_t content_offset = 0)
            : _bytes(std::move(bytes)), _content_offset(content_offset) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _content_offset + _bytes.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    std::vector<std::pair<size_t, size_t>> read_requests;
    std::optional<size_t> short_read_offset;
    std::optional<size_t> error_offset;

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext*) override {
        read_requests.emplace_back(offset, result.size);
        if (error_offset == offset || offset < _content_offset) {
            return Status::IOError("Injected BLOB read failure");
        }
        *bytes_read = std::min(result.size, size() - offset);
        if (short_read_offset == offset && *bytes_read > 0) {
            --*bytes_read;
        }
        if (*bytes_read > 0) {
            std::memcpy(result.data, _bytes.data() + offset - _content_offset, *bytes_read);
        }
        return Status::OK();
    }

private:
    io::Path _path {"memory.blob"};
    std::string _bytes;
    size_t _content_offset;
    bool _closed = false;
};

TEST(BlobFileReaderTest, ReadsPaimon142WriterBytes) {
    // Generated with Paimon 1.4.2 BlobFormatWriter for ["hello", NULL, empty, 00FF].
    // Fixed bytes independently pin little-endian framing, CRC32 and NULL/empty index deltas.
    const std::string bytes = std::string(
            "\xcf\x11\x4e\x58hello\x15\x00\x00\x00\x00\x00\x00\x00"
            "\xc8\xf6\xa7\x51\xcf\x11\x4e\x58\x10\x00\x00\x00"
            "\x00\x00\x00\x00\x53\x7f\xdf\x03\xcf\x11\x4e\x58"
            "\x00\xff\x12\x00\x00\x00\x00\x00\x00\x00\xef\xc8"
            "\x8a\x40\x2a\x2b\x22\x04\x04\x00\x00\x00\x01",
            64);
    EXPECT_EQ(bytes, make_blob_file({"hello", std::nullopt, "", std::string("\0\xff", 2)}));
    auto file = std::make_shared<MemoryBlobFile>(bytes);
    BlobFileReader reader;
    ASSERT_TRUE(reader.open(file, 4, nullptr).ok());

    // Descriptor access reads only the footer and index, never record bytes.
    ASSERT_EQ(file->read_requests.size(), 2);
    EXPECT_EQ(file->read_requests[0], std::make_pair(size_t {59}, size_t {5}));
    EXPECT_EQ(file->read_requests[1], std::make_pair(size_t {55}, size_t {4}));
    std::optional<BlobFileReader::ByteRange> range;
    ASSERT_TRUE(reader.get_range(0, &range).ok());
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(range->offset, 4);
    EXPECT_EQ(range->size, 5);
    PODArray<char> payload;
    ASSERT_TRUE(reader.read_payload(*range, &payload, nullptr).ok());
    EXPECT_EQ(std::string(payload.data(), payload.size()), "hello");

    ASSERT_TRUE(reader.get_range(1, &range).ok());
    EXPECT_FALSE(range.has_value());
    ASSERT_TRUE(reader.get_range(2, &range).ok());
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(range->offset, 25);
    EXPECT_EQ(range->size, 0);
    ASSERT_TRUE(reader.read_payload(*range, &payload, nullptr).ok());
    EXPECT_TRUE(payload.empty());
    ASSERT_TRUE(reader.get_range(3, &range).ok());
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(range->offset, 41);
    EXPECT_EQ(range->size, 2);
    ASSERT_TRUE(reader.read_payload(*range, &payload, nullptr).ok());
    EXPECT_EQ(std::string(payload.data(), payload.size()), std::string("\0\xff", 2));
}

TEST(BlobFileReaderTest, BinaryPayloadsAndVarintLengthTransitions) {
    std::vector<std::optional<std::string>> values {std::nullopt, ""};
    for (const size_t size : {47, 48, 49, 111, 112, 113, 127, 128, 129, 8175, 8176, 8177}) {
        values.emplace_back(std::string(size, 'x'));
    }
    values.emplace_back(std::nullopt);
    values.emplace_back("");
    values.emplace_back("short");
    std::string all_bytes;
    for (int byte = 0; byte < 256; ++byte) {
        all_bytes.push_back(static_cast<char>(byte));
    }
    values.emplace_back(all_bytes);
    values.emplace_back("你好Doris🌍");
    BlobFileReader reader;
    ASSERT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(make_blob_file(values)),
                            static_cast<int64_t>(values.size()), nullptr)
                        .ok());
    PODArray<char> payload;
    int64_t record_offset = 0;
    for (size_t row = 0; row < values.size(); ++row) {
        SCOPED_TRACE(row);
        std::optional<BlobFileReader::ByteRange> range;
        ASSERT_TRUE(reader.get_range(static_cast<int64_t>(row), &range).ok());
        if (!values[row]) {
            EXPECT_FALSE(range.has_value());
            continue;
        }
        ASSERT_TRUE(range.has_value());
        EXPECT_EQ(range->offset, record_offset + 4);
        EXPECT_EQ(range->size, values[row]->size());
        ASSERT_TRUE(reader.read_payload(*range, &payload, nullptr).ok());
        EXPECT_EQ(std::string(payload.data(), payload.size()), *values[row]);
        record_offset += static_cast<int64_t>(values[row]->size()) + 16;
    }
}

TEST(BlobFileReaderTest, EmptyAndAllNullFiles) {
    for (const size_t row_count : {0, 4}) {
        SCOPED_TRACE(row_count);
        auto file = std::make_shared<MemoryBlobFile>(
                make_blob_file(std::vector<std::optional<std::string>>(row_count)));
        BlobFileReader reader;
        ASSERT_TRUE(reader.open(file, static_cast<int64_t>(row_count), nullptr).ok());
        for (size_t row = 0; row < row_count; ++row) {
            std::optional<BlobFileReader::ByteRange> range;
            ASSERT_TRUE(reader.get_range(static_cast<int64_t>(row), &range).ok());
            EXPECT_FALSE(range.has_value());
        }
        EXPECT_EQ(file->read_requests.size(), 2);
    }
}

TEST(BlobFileReaderTest, ReopeningClearsPreviousIndex) {
    BlobFileReader reader;
    ASSERT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(make_blob_file({"old", "", "end"})), 3,
                            nullptr)
                        .ok());
    ASSERT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(make_blob_file({"new"})), 1, nullptr)
                        .ok());
    std::optional<BlobFileReader::ByteRange> range;
    EXPECT_TRUE(reader.get_range(1, &range).is<ErrorCode::CORRUPTION>());
    ASSERT_TRUE(reader.get_range(0, &range).ok());
    ASSERT_TRUE(range.has_value());
    PODArray<char> payload;
    ASSERT_TRUE(reader.read_payload(*range, &payload, nullptr).ok());
    EXPECT_EQ(std::string(payload.data(), payload.size()), "new");
}

TEST(BlobFileReaderTest, RejectsOutOfBoundsRows) {
    BlobFileReader reader;
    ASSERT_TRUE(
            reader.open(std::make_shared<MemoryBlobFile>(make_blob_file({"a"})), 1, nullptr).ok());
    for (const int64_t row : {int64_t {-1}, int64_t {1}, std::numeric_limits<int64_t>::max()}) {
        std::optional<BlobFileReader::ByteRange> range;
        EXPECT_TRUE(reader.get_range(row, &range).is<ErrorCode::CORRUPTION>());
    }
}

TEST(BlobFileReaderTest, RejectsInvalidFileSizeAndRowCount) {
    BlobFileReader reader;
    EXPECT_TRUE(reader.open(nullptr, 1, nullptr).is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(reader.open(std::make_shared<MemoryBlobFile>("1234"), 0, nullptr)
                        .is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(make_blob_file({"a"})), -1, nullptr)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(BlobFileReaderTest, RejectsUnknownVersion) {
    std::string bytes = make_blob_file({"a"});
    bytes.back() = '\2';
    BlobFileReader reader;
    const auto status = reader.open(std::make_shared<MemoryBlobFile>(bytes), 1, nullptr);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
}

TEST(BlobFileReaderTest, RejectsInvalidIndexLength) {
    for (const uint32_t index_size : {0U, 100U, std::numeric_limits<uint32_t>::max()}) {
        SCOPED_TRACE(index_size);
        std::string bytes = make_blob_file({"a"});
        bytes.resize(bytes.size() - 5);
        blob_test::append_little_endian(&bytes, index_size, 4);
        bytes.push_back('\1');
        BlobFileReader reader;
        EXPECT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(bytes), 1, nullptr)
                            .is<ErrorCode::CORRUPTION>());
    }
}

TEST(BlobFileReaderTest, RejectsTruncatedAndOverflowingVarints) {
    const std::vector<std::string> indexes {std::string(1, '\x80'), std::string(9, '\x80') + '\x02',
                                            std::string(10, '\x80')};
    for (const auto& index : indexes) {
        BlobFileReader reader;
        const auto status = reader.open(
                std::make_shared<MemoryBlobFile>(finish_blob_file("", index)), 1, nullptr);
        EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    }
}

TEST(BlobFileReaderTest, RejectsInvalidRecordLengths) {
    for (const int64_t length : {-2, 0, 15, 17}) {
        SCOPED_TRACE(length);
        BlobFileReader reader;
        const auto status =
                reader.open(std::make_shared<MemoryBlobFile>(finish_blob_file(
                                    std::string(16, '\0'), encode_record_lengths({length}))),
                            1, nullptr);
        EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    }
    // The second delta would overflow the signed record length when added to the first.
    const std::string index = encode_record_lengths({16}) +
                              encode_record_lengths({std::numeric_limits<int64_t>::max()});
    BlobFileReader reader;
    EXPECT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(
                                    finish_blob_file(std::string(16, '\0'), index)),
                            2, nullptr)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(BlobFileReaderTest, RejectsManifestRowCountAndUnindexedData) {
    const auto bytes = make_blob_file({"a", std::nullopt, ""});
    for (const int64_t rows : {0, 2, 4}) {
        BlobFileReader reader;
        EXPECT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(bytes), rows, nullptr)
                            .is<ErrorCode::CORRUPTION>());
    }
    BlobFileReader reader;
    EXPECT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(finish_blob_file(
                                    std::string(17, '\0'), encode_record_lengths({16}))),
                            1, nullptr)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(BlobFileReaderTest, ValidatesMagicStoredLengthPayloadAndChecksum) {
    // Corrupt one field at a time. Opening and descriptor access still succeed: records
    // are deliberately read and validated only when inline output is requested.
    for (const size_t corrupt_offset : {0, 9, 4, 20}) {
        SCOPED_TRACE(corrupt_offset);
        auto bytes = make_blob_file({"hello"});
        bytes[corrupt_offset] ^= 1;
        BlobFileReader reader;
        ASSERT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(bytes), 1, nullptr).ok());
        std::optional<BlobFileReader::ByteRange> range;
        ASSERT_TRUE(reader.get_range(0, &range).ok());
        ASSERT_TRUE(range.has_value());
        PODArray<char> payload;
        const auto status = reader.read_payload(*range, &payload, nullptr);
        EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
        EXPECT_NE(status.to_string().find(corrupt_offset == 0 || corrupt_offset == 9 ? "framing"
                                                                                     : "checksum"),
                  std::string::npos);
    }
}

TEST(BlobFileReaderTest, DetectsShortFooterIndexAndRecordReads) {
    const auto bytes = make_blob_file({"hello"});
    for (const size_t offset : {bytes.size() - 5, bytes.size() - 6, size_t {0}}) {
        SCOPED_TRACE(offset);
        auto file = std::make_shared<MemoryBlobFile>(bytes);
        file->short_read_offset = offset;
        BlobFileReader reader;
        auto status = reader.open(file, 1, nullptr);
        if (offset == 0) {
            ASSERT_TRUE(status.ok());
            std::optional<BlobFileReader::ByteRange> range;
            ASSERT_TRUE(reader.get_range(0, &range).ok());
            ASSERT_TRUE(range.has_value());
            PODArray<char> payload;
            status = reader.read_payload(*range, &payload, nullptr);
        }
        EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
        EXPECT_NE(status.to_string().find("Truncated BLOB file"), std::string::npos);
    }
}

TEST(BlobFileReaderTest, PropagatesReadErrorsAndCancellation) {
    const auto bytes = make_blob_file({"hello"});
    auto file = std::make_shared<MemoryBlobFile>(bytes);
    file->error_offset = bytes.size() - 5;
    BlobFileReader reader;
    EXPECT_TRUE(reader.open(file, 1, nullptr).is<ErrorCode::IO_ERROR>());
    io::IOContext context;
    context.should_stop = true;
    EXPECT_TRUE(reader.open(std::make_shared<MemoryBlobFile>(bytes), 1, &context)
                        .is<ErrorCode::CANCELLED>());
}

TEST(BlobFileReaderTest, DescribesOversizedPayloadWithoutAllocatingIt) {
    const int64_t payload_size = int64_t {std::numeric_limits<int32_t>::max()} + 1;
    const auto record_size = static_cast<size_t>(payload_size + 16);
    // Only the tiny tail exists in memory. Any attempted payload read fails the fake reader.
    auto file = std::make_shared<MemoryBlobFile>(
            finish_blob_file("", encode_record_lengths({payload_size + 16})), record_size);
    BlobFileReader reader;
    ASSERT_TRUE(reader.open(file, 1, nullptr).ok());
    std::optional<BlobFileReader::ByteRange> range;
    ASSERT_TRUE(reader.get_range(0, &range).ok());
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(range->offset, 4);
    EXPECT_EQ(range->size, payload_size);
    PODArray<char> payload;
    EXPECT_TRUE(
            reader.read_payload(*range, &payload, nullptr).is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    EXPECT_TRUE(payload.empty());
    EXPECT_EQ(file->read_requests.size(), 2);
}

} // namespace
} // namespace doris::format
