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

#include "vec/exec/format/orc/orc_file_reader.h"

#include <gtest/gtest.h>

#include "io/fs/local_file_system.h"
#include "util/slice.h"

namespace doris {
namespace vectorized {

class MockFileReader : public io::FileReader {
public:
    MockFileReader() = default;
    ~MockFileReader() override = default;

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const io::Path& path() const override { return _path; }

    size_t size() const override { return _data.size(); }

    bool closed() const override { return _closed; }

    void set_data(const std::string& data) { _data = data; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        if (offset >= _data.size()) {
            *bytes_read = 0;
            return Status::OK();
        }
        *bytes_read = std::min(result.size, _data.size() - offset);
        memcpy(result.data, _data.data() + offset, *bytes_read);
        return Status::OK();
    }

private:
    std::string _data;
    bool _closed = false;
    io::Path _path = "/tmp/mock";
};

class OrcMergeRangeFileReaderTest : public testing::Test {
protected:
    void SetUp() override { _mock_reader = std::make_shared<MockFileReader>(); }

    std::shared_ptr<MockFileReader> _mock_reader;
};

TEST_F(OrcMergeRangeFileReaderTest, basic_init) {
    std::string test_data(1024, 'A');
    _mock_reader->set_data(test_data);

    io::PrefetchRange range {0, 1024};
    OrcMergeRangeFileReader reader(nullptr, _mock_reader, range);
    EXPECT_EQ(1024, reader.size());
    EXPECT_FALSE(reader.closed());
}

TEST_F(OrcMergeRangeFileReaderTest, read_with_cache) {
    std::string test_data(1024, 'A');
    _mock_reader->set_data(test_data);

    io::PrefetchRange range {0, 1024};
    const size_t test_size = 128;

    OrcMergeRangeFileReader reader(nullptr, _mock_reader, range);

    // Read from cache
    char buffer[test_size];
    Slice result(buffer, test_size);
    size_t bytes_read = 0;

    // Read from start
    ASSERT_TRUE(reader.read_at(0, result, &bytes_read, nullptr).ok());
    EXPECT_EQ(bytes_read, test_size);
    EXPECT_EQ(std::string(buffer, test_size), std::string(test_size, 'A'));

    // Read from middle
    ASSERT_TRUE(reader.read_at(512, result, &bytes_read, nullptr).ok());
    EXPECT_EQ(bytes_read, test_size);
    EXPECT_EQ(std::string(buffer, test_size), std::string(test_size, 'A'));

    // Verify statistics
    EXPECT_EQ(reader.statistics().merged_io, 1);
    EXPECT_EQ(reader.statistics().merged_bytes, 1024);
}

TEST_F(OrcMergeRangeFileReaderTest, read_empty_data) {
    _mock_reader->set_data("");

    io::PrefetchRange range {0, 1024};
    OrcMergeRangeFileReader reader(nullptr, _mock_reader, range);

    char buffer[128];
    Slice result(buffer, 128);
    size_t bytes_read = 0;

    ASSERT_FALSE(reader.read_at(0, result, &bytes_read, nullptr).ok());
    EXPECT_EQ(bytes_read, 0);
}

TEST_F(OrcMergeRangeFileReaderTest, close) {
    std::string test_data(1024, 'A');
    _mock_reader->set_data(test_data);

    io::PrefetchRange range {0, 1024};
    OrcMergeRangeFileReader reader(nullptr, _mock_reader, range);
    ASSERT_FALSE(reader.closed());

    ASSERT_TRUE(reader.close().ok());
    ASSERT_TRUE(reader.closed());
}

TEST_F(OrcMergeRangeFileReaderTest, multiple_reads_from_cache) {
    std::string test_data;
    for (int i = 0; i < 1024; i++) {
        test_data.push_back(i % 256);
    }
    _mock_reader->set_data(test_data);

    io::PrefetchRange range {0, 1024};
    OrcMergeRangeFileReader reader(nullptr, _mock_reader, range);

    // Perform multiple reads with different sizes and offsets
    const std::vector<std::pair<size_t, size_t>> read_patterns = {
            {0, 128},   // Start, 128 bytes
            {256, 64},  // Middle, 64 bytes
            {1000, 24}, // Near end, 24 bytes
            {512, 256}, // Middle, large read
    };

    for (const auto& pattern : read_patterns) {
        std::vector<char> buffer(pattern.second);
        Slice result(buffer.data(), pattern.second);
        size_t bytes_read = 0;

        ASSERT_TRUE(reader.read_at(pattern.first, result, &bytes_read, nullptr).ok());
        EXPECT_EQ(bytes_read, pattern.second);
        EXPECT_EQ(memcmp(buffer.data(), test_data.data() + pattern.first, pattern.second), 0);
    }

    // Verify that we only did one actual read
    EXPECT_EQ(reader.statistics().merged_io, 1);
    EXPECT_EQ(reader.statistics().merged_bytes, 1024);
}

} // namespace vectorized
} // namespace doris
