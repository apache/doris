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

#include "format/orc/vorc_reader.h"

#include <gtest/gtest.h>

#include "io/fs/buffered_reader.h"
#include "io/fs/tracing_file_reader.h"

namespace doris {
namespace vectorized {

class MockInnerFileReader : public io::FileReader {
public:
    MockInnerFileReader(size_t size) : _size(size) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _size; }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

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
    size_t _size = 0;
    bool _closed = false;
    io::Path _path = "/tmp/mock_inner";
};

// Wraps a MockInnerFileReader and records how many read_at calls reached it,
// so we can verify that reads go through (or bypass) the wrapper chain.
class CountingFileReader : public io::FileReader {
public:
    CountingFileReader(io::FileReaderSPtr inner) : _inner(std::move(inner)) {}

    Status close() override { return _inner->close(); }
    const io::Path& path() const override { return _inner->path(); }
    size_t size() const override { return _inner->size(); }
    bool closed() const override { return _inner->closed(); }
    int64_t mtime() const override { return _inner->mtime(); }

    int read_at_count() const { return _read_at_count; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        _read_at_count++;
        return _inner->read_at(offset, result, bytes_read, io_ctx);
    }

private:
    io::FileReaderSPtr _inner;
    int _read_at_count = 0;
};

class ORCFileInputStreamTest : public testing::Test {
protected:
    void SetUp() override {
        _inner_data = std::string(2048, 'X');
        _mock_inner = std::make_shared<MockInnerFileReader>(_inner_data.size());
        _mock_inner->set_data(_inner_data);
    }

    std::shared_ptr<MockInnerFileReader> _mock_inner;
    std::string _inner_data;
};

// Verify that set_file_reader updates _tracing_file_reader when IOContext is present.
// This is the core fix — before the fix, calling set_file_reader would update
// _file_reader but leave _tracing_file_reader pointing at the old inner reader,
// so ORCFileInputStream::read() would bypass the RangeCacheFileReader wrapper.
TEST_F(ORCFileInputStreamTest, set_file_reader_updates_tracing_with_io_ctx) {
    io::FileReaderStats stats;
    io::IOContext io_ctx;
    io_ctx.file_reader_stats = &stats;

    ORCFileInputStream stream("test_file", _mock_inner, &io_ctx, nullptr,
                              config::orc_natural_read_size_mb << 20, 0);

    // Initially _tracing_file_reader wraps _inner_reader via TracingFileReader
    auto* initial_tracing = dynamic_cast<io::TracingFileReader*>(stream.get_tracing_file_reader().get());
    ASSERT_TRUE(initial_tracing != nullptr);
    EXPECT_EQ(initial_tracing->inner_reader().get(), _mock_inner.get());

    // Count the inner reader to verify reads actually reach it through the wrapper
    auto counting_inner = std::make_shared<CountingFileReader>(_mock_inner);

    // Simulate the all-tiny-stripe path: wrap inner reader with RangeCacheFileReader
    // via set_file_reader
    auto ranges = std::make_shared<io::LinearProbeRangeFinder>(
            std::vector<io::PrefetchRange> {{0, 2048}});
    auto range_cache_reader = std::make_shared<io::RangeCacheFileReader>(
            nullptr, counting_inner, ranges);

    stream.set_file_reader(range_cache_reader);

    // After set_file_reader, _tracing_file_reader should now wrap the NEW
    // _file_reader (RangeCacheFileReader), not the old inner reader
    auto* updated_tracing = dynamic_cast<io::TracingFileReader*>(stream.get_tracing_file_reader().get());
    ASSERT_TRUE(updated_tracing != nullptr);
    EXPECT_EQ(updated_tracing->inner_reader().get(), range_cache_reader.get());

    // Verify that read() goes through the full chain: TracingFileReader →
    // RangeCacheFileReader → CountingFileReader → MockInnerFileReader
    char buf[128];
    stream.read(buf, 128, 0);

    // The read should have gone through the tracing → range cache → counting chain
    EXPECT_EQ(counting_inner->read_at_count(), 1);
    EXPECT_EQ(std::string(buf, 128), std::string(128, 'X'));
}

// Verify that set_file_reader updates _tracing_file_reader when IOContext is null.
// Without IOContext, _tracing_file_reader should simply equal _file_reader (no
// TracingFileReader wrapping).
TEST_F(ORCFileInputStreamTest, set_file_reader_updates_tracing_without_io_ctx) {
    ORCFileInputStream stream("test_file", _mock_inner, nullptr, nullptr,
                              config::orc_natural_read_size_mb << 20, 0);

    // Without IOContext, _tracing_file_reader == _file_reader == _inner_reader
    EXPECT_EQ(stream.get_tracing_file_reader().get(), stream.get_file_reader().get());
    EXPECT_EQ(stream.get_tracing_file_reader().get(), _mock_inner.get());

    auto counting_inner = std::make_shared<CountingFileReader>(_mock_inner);
    auto ranges = std::make_shared<io::LinearProbeRangeFinder>(
            std::vector<io::PrefetchRange> {{0, 2048}});
    auto range_cache_reader = std::make_shared<io::RangeCacheFileReader>(
            nullptr, counting_inner, ranges);

    stream.set_file_reader(range_cache_reader);

    // _tracing_file_reader should now point to the RangeCacheFileReader directly
    EXPECT_EQ(stream.get_tracing_file_reader().get(), range_cache_reader.get());
    auto* tracing = dynamic_cast<io::TracingFileReader*>(stream.get_tracing_file_reader().get());
    EXPECT_EQ(tracing, nullptr); // no TracingFileReader wrapping without IOContext

    char buf[128];
    stream.read(buf, 128, 0);

    EXPECT_EQ(counting_inner->read_at_count(), 1);
    EXPECT_EQ(std::string(buf, 128), std::string(128, 'X'));
}

// Verify that _inner_reader remains unchanged after set_file_reader — it always
// holds the original reader.
TEST_F(ORCFileInputStreamTest, set_file_reader_preserves_inner_reader) {
    io::FileReaderStats stats;
    io::IOContext io_ctx;
    io_ctx.file_reader_stats = &stats;

    ORCFileInputStream stream("test_file", _mock_inner, &io_ctx, nullptr,
                              config::orc_natural_read_size_mb << 20, 0);

    EXPECT_EQ(stream.get_inner_reader().get(), _mock_inner.get());

    auto counting_inner = std::make_shared<CountingFileReader>(_mock_inner);
    auto ranges = std::make_shared<io::LinearProbeRangeFinder>(
            std::vector<io::PrefetchRange> {{0, 2048}});
    auto range_cache_reader = std::make_shared<io::RangeCacheFileReader>(
            nullptr, counting_inner, ranges);

    stream.set_file_reader(range_cache_reader);

    // _inner_reader must stay unchanged
    EXPECT_EQ(stream.get_inner_reader().get(), _mock_inner.get());
}

} // namespace vectorized
} // namespace doris
