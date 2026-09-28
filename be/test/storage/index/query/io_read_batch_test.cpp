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

#include "storage/index/query/spi/io_read_batch.h"

#include <gtest/gtest.h>

#include <array>
#include <numeric>
#include <vector>

namespace doris::index_query {
namespace {

class MemoryIoReader final : public IoReader {
public:
    MemoryIoReader() { std::iota(bytes.begin(), bytes.end(), uint8_t {0}); }

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        ++calls;
        if (calls == fail_call || offset > bytes.size() || len > bytes.size() - offset) {
            return Status::IOError("Injected byte reader failure");
        }
        out->assign(bytes.begin() + offset, bytes.begin() + offset + len);
        return Status::OK();
    }
    uint64_t size() const override { return bytes.size(); }

    std::array<uint8_t, 64> bytes;
    size_t calls = 0;
    size_t fail_call = 0;
};

TEST(IndexQueryIoReadBatch, ReleasesPartialReadBuffersBeforeRetry) {
    MemoryIoReader reader;
    reader.fail_call = 2;
    IoReadBatch batch(&reader);
    batch.add(0, 4);
    batch.add(32, 4);
    EXPECT_FALSE(batch.fetch().ok());
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(batch.get(0)[3], 3U);
    EXPECT_EQ(batch.get(1)[0], 32U);
}

TEST(IndexQueryIoReadBatch, ReaderFallbackPreservesExactReadAndErrorSemantics) {
    MemoryIoReader reader;
    std::array<uint8_t, 4> output {};
    ASSERT_TRUE(reader.read_into(3, output.data(), output.size()).ok());
    EXPECT_EQ(output, (std::array<uint8_t, 4> {3, 4, 5, 6}));
    EXPECT_FALSE(reader.read_into(63, output.data(), output.size()).ok());
    const size_t calls = reader.calls;
    EXPECT_TRUE(reader.read_into(0, nullptr, 0).ok());
    EXPECT_EQ(reader.calls, calls);
}

} // namespace
} // namespace doris::index_query
