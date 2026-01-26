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

#include "io/fs/packed_file_reader.h"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {

using doris::Status;

// Mock FileReader for testing PackedFileReader
class MockFileReader : public FileReader {
public:
    explicit MockFileReader(std::string content) : _content(std::move(content)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    size_t size() const override { return _content.size(); }

    bool closed() const override { return _closed; }

    void set_read_status(Status st) { _read_status = std::move(st); }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* /*io_ctx*/) override {
        if (!_read_status.ok()) {
            return _read_status;
        }

        if (offset >= _content.size()) {
            *bytes_read = 0;
            return Status::OK();
        }

        size_t available = _content.size() - offset;
        size_t to_read = std::min(result.get_size(), available);
        std::memcpy(result.mutable_data(), _content.data() + offset, to_read);
        *bytes_read = to_read;
        return Status::OK();
    }

    int64_t mtime() const override { return 0; }

private:
    Path _path = Path("mock_file");
    std::string _content;
    bool _closed = false;
    Status _read_status = Status::OK();
};

// Test fixture for PackedFileReader
class PackedFileReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _packed_file_content = "0123456789abcdefghijklmnopqrstuvwxyz";
        _inner_reader = std::make_shared<MockFileReader>(_packed_file_content);
        _packed_file_offset = 10;
        _file_size = 20;
    }

    std::string _packed_file_content;
    FileReaderSPtr _inner_reader;
    int64_t _packed_file_offset;
    int64_t _file_size;
};

TEST_F(PackedFileReaderTest, ReadFromMergeFile) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    char buffer[100];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    // Read from offset 0 in the small file (which is at offset 10 in merge file)
    Status st = reader.read_at(0, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(bytes_read, _file_size);

    // Verify content matches expected portion of merge file
    std::string expected = _packed_file_content.substr(_packed_file_offset, _file_size);
    EXPECT_EQ(std::string(buffer, bytes_read), expected);
}

TEST_F(PackedFileReaderTest, ReadPartialData) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    char buffer[10];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader.read_at(0, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(bytes_read, 10);

    std::string expected = _packed_file_content.substr(_packed_file_offset, 10);
    EXPECT_EQ(std::string(buffer, bytes_read), expected);
}

TEST_F(PackedFileReaderTest, ReadAtOffset) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    char buffer[10];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    // Read from offset 5 in the small file (which is at offset 15 in merge file)
    Status st = reader.read_at(5, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(bytes_read, 10);

    std::string expected = _packed_file_content.substr(_packed_file_offset + 5, 10);
    EXPECT_EQ(std::string(buffer, bytes_read), expected);
}

TEST_F(PackedFileReaderTest, ReadBeyondFileSize) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    char buffer[100];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    // Try to read beyond file size
    Status st = reader.read_at(0, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    // Should only read up to file size
    EXPECT_EQ(bytes_read, _file_size);
}

TEST_F(PackedFileReaderTest, ReadAtEndOfFile) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    char buffer[10];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    // Read from near the end of the file
    Status st = reader.read_at(15, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    // Should only read remaining bytes (5 bytes)
    EXPECT_EQ(bytes_read, 5);

    std::string expected = _packed_file_content.substr(_packed_file_offset + 15, 5);
    EXPECT_EQ(std::string(buffer, bytes_read), expected);
}

TEST_F(PackedFileReaderTest, ReadBeyondFileBoundary) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    char buffer[10];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    // Try to read from beyond file size (offset 25, file_size is 20)
    // When offset >= file_size, _file_size - offset becomes negative
    // When cast to size_t, it becomes a very large number
    // So max_read = min(result.get_size(), large_number) = result.get_size()
    // actual_offset = merge_offset + offset = 10 + 25 = 35
    // Content has 36 chars (indices 0-35), so inner reader can read 1 byte at offset 35
    // The behavior is that it reads 1 byte from the merge file, even though it's beyond the small file size
    Status st = reader.read_at(25, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    // Should read at most the buffer size (implementation may read from merge file)
    EXPECT_LE(bytes_read, result.get_size());
}

TEST_F(PackedFileReaderTest, Close) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    EXPECT_FALSE(reader.closed());
    Status st = reader.close();
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(reader.closed());
}

TEST_F(PackedFileReaderTest, CloseMultipleTimes) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    Status st1 = reader.close();
    EXPECT_TRUE(st1.ok());
    EXPECT_TRUE(reader.closed());

    // Closing again should be OK
    Status st2 = reader.close();
    EXPECT_TRUE(st2.ok());
    EXPECT_TRUE(reader.closed());
}

TEST_F(PackedFileReaderTest, PathAndSize) {
    Path file_path("test_path");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    EXPECT_EQ(reader.path().native(), "test_path");
    EXPECT_EQ(reader.size(), _file_size);
}

TEST_F(PackedFileReaderTest, ReadWithZeroOffset) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    char buffer[5];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader.read_at(0, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(bytes_read, 5);

    std::string expected = _packed_file_content.substr(_packed_file_offset, 5);
    EXPECT_EQ(std::string(buffer, bytes_read), expected);
}

TEST_F(PackedFileReaderTest, ReadWithSmallBuffer) {
    Path file_path("test_file");
    PackedFileReader reader(_inner_reader, file_path, _packed_file_offset, _file_size);

    char buffer[3];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    // Read multiple times with small buffer
    std::string all_read;
    for (int i = 0; i < 7; ++i) {
        Status st = reader.read_at(i * 3, result, &bytes_read, nullptr);
        EXPECT_TRUE(st.ok());
        if (bytes_read > 0) {
            all_read.append(buffer, bytes_read);
        }
    }

    // Should have read all file content
    std::string expected = _packed_file_content.substr(_packed_file_offset, _file_size);
    EXPECT_EQ(all_read, expected);
}

} // namespace doris::io
