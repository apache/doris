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

#include "vec/exec/format/parquet/reference_counted_reader.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "util/slice.h"

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

    size_t size() const override { return 0; }

    bool closed() const override { return _closed; }

    //    Status read_at(size_t offset, size_t nbytes, std::shared_ptr<Slice>* result,
    //                  const io::IOContext* io_ctx) override {
    //        size_t bytes_to_read = std::min(nbytes, _data.size() - offset);
    //        auto buffer = new char[bytes_to_read];
    //        memcpy(buffer, _data.data() + offset, bytes_to_read);
    //        *result = std::make_shared<Slice>(buffer, bytes_to_read);
    //        return Status::OK();
    //    }

    void set_data(const std::string& data) { _data = data; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        *bytes_read = std::min(result.size, _data.size() - offset);
        memcpy(result.data, _data.data() + offset, *bytes_read);
        return Status::OK();
    }

private:
    std::string _data;
    bool _closed = false;
    io::Path _path = "/tmp/mock";
};

class ReferenceCountedReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        _mock_file_reader = std::make_shared<MockFileReader>();
        _statistics = std::make_unique<ChunkReader::Statistics>();
    }

    std::shared_ptr<MockFileReader> _mock_file_reader;
    std::unique_ptr<ChunkReader::Statistics> _statistics;
};

// 测试引用计数的基本功能
TEST_F(ReferenceCountedReaderTest, ReferenceCountBasic) {
    io::PrefetchRange range {0, 10};
    auto reader = std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);

    // 初始引用计数应该为1
    EXPECT_EQ(reader->reference_count(), 1);

    // 增加引用计数
    reader->addReference();
    EXPECT_EQ(reader->reference_count(), 2);

    // 减少引用计数
    reader->free();
    EXPECT_EQ(reader->reference_count(), 1);

    // 最后一次释放
    reader->free();
    // 引用计数为0时，reader应该已经被销毁
}

// 测试读取功能
TEST_F(ReferenceCountedReaderTest, ReadData) {
    std::string test_data = "Hello, World!";
    _mock_file_reader->set_data(test_data);

    io::PrefetchRange range {0, test_data.size()};
    auto reader = std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);

    std::shared_ptr<Slice> result;
    auto status = reader->read(nullptr, &result);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result->size, test_data.size());
    EXPECT_EQ(std::string(result->data, result->size), test_data);

    // 检查统计信息
    EXPECT_EQ(_statistics->merged_io, 1);
    EXPECT_EQ(_statistics->merged_bytes, test_data.size());
}

// 测试多次读取相同数据只会产生一次IO
TEST_F(ReferenceCountedReaderTest, CacheRead) {
    std::string test_data = "Hello, World!";
    _mock_file_reader->set_data(test_data);

    io::PrefetchRange range {0, test_data.size()};
    auto reader = std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);

    // 第一次读取
    std::shared_ptr<Slice> result1;
    auto status = reader->read(nullptr, &result1);
    EXPECT_TRUE(status.ok());

    // 第二次读取
    std::shared_ptr<Slice> result2;
    status = reader->read(nullptr, &result2);
    EXPECT_TRUE(status.ok());

    // 两次读取应该返回相同的数据
    EXPECT_EQ(std::string(result1->data, result1->size), std::string(result2->data, result2->size));

    // 但只应该有一次IO操作
    EXPECT_EQ(_statistics->merged_io, 1);
    EXPECT_EQ(_statistics->merged_bytes, test_data.size());
}

// 测试读取部分范围
TEST_F(ReferenceCountedReaderTest, PartialRead) {
    std::string test_data = "Hello, World!";
    _mock_file_reader->set_data(test_data);

    // 只读取部分数据
    io::PrefetchRange range {0, 5}; // 只读取 "Hello"
    auto reader = std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);

    std::shared_ptr<Slice> result;
    auto status = reader->read(nullptr, &result);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result->size, 5);
    EXPECT_EQ(std::string(result->data, result->size), "Hello");

    EXPECT_EQ(_statistics->merged_io, 1);
    EXPECT_EQ(_statistics->merged_bytes, 5);
}

// 测试引用计数和共享数据
TEST_F(ReferenceCountedReaderTest, SharedData) {
    std::string test_data = "Hello, World!";
    _mock_file_reader->set_data(test_data);

    io::PrefetchRange range {0, test_data.size()};
    auto reader1 = std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);

    // 增加引用计数
    reader1->addReference();
    auto reader2 = reader1;

    // 两个reader应该共享相同的数据
    std::shared_ptr<Slice> result1, result2;
    auto status1 = reader1->read(nullptr, &result1);
    auto status2 = reader2->read(nullptr, &result2);

    EXPECT_TRUE(status1.ok());
    EXPECT_TRUE(status2.ok());
    EXPECT_EQ(result1->data, result2->data); // 应该指向相同的内存位置

    // 只应该有一次IO操作
    EXPECT_EQ(_statistics->merged_io, 1);

    // 释放引用
    reader1->free();
    reader2->free();
}

// 测试大数据读取
//TEST_F(ReferenceCountedReaderTest, LargeDataRead) {
//    // 创建一个大于 MAX_ARRAY_SIZE 的数据
//    std::string large_data(ReferenceCountedReader::MAX_ARRAY_SIZE + 1, 'A');
//    _mock_file_reader->set_data(large_data);
//
//    io::PrefetchRange range{0, large_data.size()};
//    auto reader = std::make_shared<ReferenceCountedReader>(range, _mock_file_reader, *_statistics);
//
//    std::shared_ptr<Slice> result;
//    auto status = reader->read(nullptr, &result);
//
//    // 应该返回错误，因为数据太大
//    EXPECT_FALSE(status.ok());
//    EXPECT_TRUE(status.to_string().find("Data size exceeds maximum allowed size") != std::string::npos);
//}

} // namespace doris::vectorized
