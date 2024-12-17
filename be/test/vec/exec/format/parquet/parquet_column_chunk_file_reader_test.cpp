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

#include "vec/exec/format/parquet/parquet_column_chunk_file_reader.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "util/slice.h"
#include "vec/exec/format/parquet/reference_counted_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"

namespace doris::vectorized {

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

class ParquetColumnChunkFileReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        _mock_file_reader = std::make_shared<MockFileReader>();
        _statistics = std::make_unique<ChunkReader::Statistics>();
    }

    // 创建一个简单的 Parquet 列块数据
    std::string create_mock_column_chunk(int32_t value, bool compressed = false) {
        // 这里简化处理，创建一个包含单个 int32 值的列块
        // 实际的 Parquet 文件格式会更复杂
        std::string data;
        data.resize(sizeof(int32_t));
        memcpy(data.data(), &value, sizeof(int32_t));

        if (compressed) {
            // TODO: 如果需要测试压缩的情况，这里可以添加压缩逻辑
        }

        return data;
    }

    std::shared_ptr<MockFileReader> _mock_file_reader;
    std::unique_ptr<ChunkReader::Statistics> _statistics;
};

// 测试基本的读取功能
TEST_F(ParquetColumnChunkFileReaderTest, BasicRead) {
    // 创建测试数据
    int32_t test_value = 42;
    std::string chunk_data = create_mock_column_chunk(test_value);
    _mock_file_reader->set_data(chunk_data);

    // 创建 ReferenceCountedReader
    io::PrefetchRange range {0, chunk_data.size()};
    auto ref_reader =
            std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);

    // 创建列块读取器
    std::vector<std::shared_ptr<ChunkReader>> chunks;
    chunks.push_back(ref_reader);
    ParquetColumnChunkFileReader chunk_reader(std::move(chunks), *_statistics);

    // 读取数据
    char buffer[sizeof(int32_t)];
    Slice result(buffer, sizeof(int32_t));
    size_t bytes_read;
    auto status = chunk_reader.read_at(0, result, &bytes_read, nullptr);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(bytes_read, sizeof(int32_t));

    // 验证读取的值
    int32_t read_value;
    memcpy(&read_value, buffer, sizeof(int32_t));
    EXPECT_EQ(read_value, test_value);

    // 检查统计信息
    EXPECT_EQ(_statistics->merged_io, 1);
    EXPECT_EQ(_statistics->merged_bytes, sizeof(int32_t));
}

// 测试空数据读取
TEST_F(ParquetColumnChunkFileReaderTest, EmptyRead) {
    _mock_file_reader->set_data("");

    io::PrefetchRange range {0, 0};
    auto ref_reader =
            std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);
    std::vector<std::shared_ptr<ChunkReader>> chunks;
    chunks.push_back(ref_reader);
    ParquetColumnChunkFileReader chunk_reader(std::move(chunks), *_statistics);

    char buffer[1];
    Slice result(buffer, 1);
    size_t bytes_read;
    auto status = chunk_reader.read_at(0, result, &bytes_read, nullptr);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(bytes_read, 0);
}

// 测试范围读取
TEST_F(ParquetColumnChunkFileReaderTest, RangeRead) {
    // 创建一个包含多个值的数据块
    std::string chunk_data;
    for (int32_t i = 0; i < 5; i++) {
        chunk_data += create_mock_column_chunk(i);
    }
    _mock_file_reader->set_data(chunk_data);

    // 只读取中间的一部分
    size_t start_offset = sizeof(int32_t);
    size_t length = sizeof(int32_t) * 2;
    io::PrefetchRange full_range {0, chunk_data.size()};
    io::PrefetchRange read_range {start_offset, start_offset + length};

    auto ref_reader =
            std::make_shared<ReferenceCountedReader>(full_range, _mock_file_reader, *_statistics);
    std::vector<std::shared_ptr<ChunkReader>> chunks;
    chunks.push_back(ref_reader);
    ParquetColumnChunkFileReader chunk_reader(std::move(chunks), *_statistics);

    // 使用 unique_ptr 来管理动态分配的内存
    auto buffer = std::make_unique<char[]>(length);
    Slice result(buffer.get(), length);
    size_t bytes_read;
    auto status = chunk_reader.read_at(start_offset, result, &bytes_read, nullptr);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(bytes_read, length);

    // 验证读取的值
    std::vector<int32_t> read_values(2);
    memcpy(read_values.data(), buffer.get(), length);
    EXPECT_EQ(read_values[0], 1);
    EXPECT_EQ(read_values[1], 2);
}

// 测试多个 chunk reader
TEST_F(ParquetColumnChunkFileReaderTest, MultipleChunks) {
    // 创建多个 chunk，每个 chunk 包含不同的值
    std::vector<std::shared_ptr<ChunkReader>> chunks;
    std::vector<int32_t> test_values = {10, 20, 30, 40, 50};
    size_t chunk_size = sizeof(int32_t);
    //    size_t total_size = 0;

    // 创建包含所有数据的 file reader
    std::string all_data;
    for (int32_t value : test_values) {
        all_data += create_mock_column_chunk(value);
    }
    auto mock_reader = std::make_shared<MockFileReader>();
    mock_reader->set_data(all_data);

    // 为每个 chunk 创建 ReferenceCountedReader，共享同一个 file reader
    for (size_t i = 0; i < test_values.size(); ++i) {
        io::PrefetchRange range {i * chunk_size, (i + 1) * chunk_size};
        auto ref_reader =
                std::make_shared<ReferenceCountedReader>(range, mock_reader, *_statistics);
        chunks.push_back(ref_reader);
        //        total_size += chunk_size;
    }

    ParquetColumnChunkFileReader chunk_reader(std::move(chunks), *_statistics);

    // 读取每个 chunk 的数据并验证
    for (size_t i = 0; i < test_values.size(); ++i) {
        char buffer[sizeof(int32_t)];
        Slice result(buffer, sizeof(int32_t));
        size_t bytes_read;
        auto status = chunk_reader.read_at(i * chunk_size, result, &bytes_read, nullptr);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(bytes_read, chunk_size);

        int32_t read_value;
        memcpy(&read_value, buffer, sizeof(int32_t));
        EXPECT_EQ(read_value, test_values[i]);
    }
}

// 测试连续顺序读取
TEST_F(ParquetColumnChunkFileReaderTest, SequentialReads) {
    // 创建两个 chunk
    std::vector<std::shared_ptr<ChunkReader>> chunks;
    std::vector<int32_t> test_values = {100, 200};
    size_t chunk_size = sizeof(int32_t);
    size_t total_size = 0;

    // 创建包含所有数据的 file reader
    std::string all_data;
    for (int32_t value : test_values) {
        all_data += create_mock_column_chunk(value);
    }
    auto mock_reader = std::make_shared<MockFileReader>();
    mock_reader->set_data(all_data);

    // 为每个 chunk 创建 ReferenceCountedReader
    for (size_t i = 0; i < test_values.size(); ++i) {
        io::PrefetchRange range {i * chunk_size, (i + 1) * chunk_size};
        auto ref_reader =
                std::make_shared<ReferenceCountedReader>(range, mock_reader, *_statistics);
        chunks.push_back(ref_reader);
        total_size += chunk_size;
    }

    ParquetColumnChunkFileReader chunk_reader(std::move(chunks), *_statistics);

    // 连续读取小块数据
    for (size_t offset = 0; offset < total_size; offset += 2) {
        char buffer[2];
        Slice result(buffer, 2);
        size_t bytes_read;
        auto status = chunk_reader.read_at(offset, result, &bytes_read, nullptr);
        EXPECT_TRUE(status.ok());
        EXPECT_GT(bytes_read, 0);
    }
}

// 测试跳跃式顺序读取
TEST_F(ParquetColumnChunkFileReaderTest, SkipReads) {
    // 创建多个 chunk
    std::vector<std::shared_ptr<ChunkReader>> chunks;
    std::vector<int32_t> test_values = {1, 2, 3, 4, 5};
    size_t chunk_size = sizeof(int32_t);
    //    size_t total_size = 0;

    // 创建包含所有数据的 file reader
    std::string all_data;
    for (int32_t value : test_values) {
        all_data += create_mock_column_chunk(value);
    }
    auto mock_reader = std::make_shared<MockFileReader>();
    mock_reader->set_data(all_data);

    // 为每个 chunk 创建 ReferenceCountedReader
    for (size_t i = 0; i < test_values.size(); ++i) {
        io::PrefetchRange range {i * chunk_size, (i + 1) * chunk_size};
        auto ref_reader =
                std::make_shared<ReferenceCountedReader>(range, mock_reader, *_statistics);
        chunks.push_back(ref_reader);
        //        total_size += chunk_size;
    }

    ParquetColumnChunkFileReader chunk_reader(std::move(chunks), *_statistics);

    // 跳跃式读取（读取第1、3、5个值）
    std::vector<size_t> read_offsets = {0, 2 * chunk_size, 4 * chunk_size};
    std::vector<int32_t> expected_values = {1, 3, 5};

    for (size_t i = 0; i < read_offsets.size(); ++i) {
        char buffer[sizeof(int32_t)];
        Slice result(buffer, sizeof(int32_t));
        size_t bytes_read;
        auto status = chunk_reader.read_at(read_offsets[i], result, &bytes_read, nullptr);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(bytes_read, chunk_size);

        int32_t read_value;
        memcpy(&read_value, buffer, sizeof(int32_t));
        EXPECT_EQ(read_value, expected_values[i]);
    }
}

// 测试逆序读取（应该失败）
TEST_F(ParquetColumnChunkFileReaderTest, ReverseReads) {
    // 创建两个 chunk
    std::vector<std::shared_ptr<ChunkReader>> chunks;
    std::vector<int32_t> test_values = {1, 2};
    size_t chunk_size = sizeof(int32_t);
    //    size_t total_size = 0;

    // 创建包含所有数据的 file reader
    std::string all_data;
    for (int32_t value : test_values) {
        all_data += create_mock_column_chunk(value);
    }
    auto mock_reader = std::make_shared<MockFileReader>();
    mock_reader->set_data(all_data);

    // 为每个 chunk 创建 ReferenceCountedReader
    for (size_t i = 0; i < test_values.size(); ++i) {
        io::PrefetchRange range {i * chunk_size, (i + 1) * chunk_size};
        auto ref_reader =
                std::make_shared<ReferenceCountedReader>(range, mock_reader, *_statistics);
        chunks.push_back(ref_reader);
        //        total_size += chunk_size;
    }

    ParquetColumnChunkFileReader chunk_reader(std::move(chunks), *_statistics);

    // 先读取后面的 offset
    char buffer[sizeof(int32_t)];
    Slice result(buffer, sizeof(int32_t));
    size_t bytes_read;
    auto status = chunk_reader.read_at(chunk_size, result, &bytes_read, nullptr);
    EXPECT_TRUE(status.ok());

    // 再读取前面的 offset（应该失败）
    status = chunk_reader.read_at(0, result, &bytes_read, nullptr);
    EXPECT_FALSE(status.ok());
}

// 测试引用计数
TEST_F(ParquetColumnChunkFileReaderTest, ReferenceCount) {
    std::string chunk_data = create_mock_column_chunk(42);
    _mock_file_reader->set_data(chunk_data);

    io::PrefetchRange range {0, chunk_data.size()};
    auto ref_reader =
            std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);

    {
        // 创建一个作用域
        std::vector<std::shared_ptr<ChunkReader>> chunks;
        chunks.push_back(ref_reader);
        ParquetColumnChunkFileReader chunk_reader(std::move(chunks), *_statistics);
        EXPECT_EQ(ref_reader->reference_count(), 1);

        // 读取数据
        char buffer[sizeof(int32_t)];
        Slice result(buffer, sizeof(int32_t));
        size_t bytes_read;
        auto status = chunk_reader.read_at(0, result, &bytes_read, nullptr);
        EXPECT_TRUE(status.ok());
    }
    // chunk_reader 离开作用域后，引用计数应该减少
    EXPECT_EQ(ref_reader->reference_count(), 0);
}

} // namespace doris::vectorized
