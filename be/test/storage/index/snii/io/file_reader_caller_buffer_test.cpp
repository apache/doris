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

// Caller-buffer read primitive (read_into(offset, out, out_len)): the blob read
// shim (SniiBlobIndexInput) must fill CLucene's BufferedIndexInput refill
// buffer -- and faiss's destination arrays -- DIRECTLY, without a per-read
// vector allocation + memcpy round-trip. The base-class default may fall back
// to the vector overload; concrete readers serve it natively.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/local_file.h"

using namespace doris::snii;
using doris::Status;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_reader_buf_test_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".bin";
}

std::vector<uint8_t> Pattern(size_t n) {
    std::vector<uint8_t> out(n);
    for (size_t i = 0; i < n; ++i) {
        out[i] = static_cast<uint8_t>(i * 13 + 5);
    }
    return out;
}

// Overrides only the vector read: proves the caller-buffer entry point has a
// working default for readers that predate it.
class VectorOnlyReader : public io::FileReader {
public:
    explicit VectorOnlyReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset > bytes_.size() || len > bytes_.size() - offset) {
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "test: read past EOF");
        }
        out->assign(bytes_.begin() + static_cast<std::ptrdiff_t>(offset),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(offset + len));
        return Status::OK();
    }
    uint64_t size() const override { return bytes_.size(); }

private:
    std::vector<uint8_t> bytes_;
};

TEST(SniiFileReaderCallerBuffer, DefaultImplementationDelegatesToVectorRead) {
    const auto payload = Pattern(1000);
    VectorOnlyReader reader(payload);

    std::vector<uint8_t> buf(100, 0xEE);
    ASSERT_TRUE(reader.read_into(37, buf.data(), buf.size()).ok());
    EXPECT_EQ(0, std::memcmp(payload.data() + 37, buf.data(), buf.size()));

    // Zero-length read is a no-op, even at EOF.
    ASSERT_TRUE(reader.read_into(payload.size(), buf.data(), 0).ok());
    // Past-EOF fails.
    EXPECT_FALSE(reader.read_into(payload.size() - 1, buf.data(), 2).ok());
}

TEST(SniiFileReaderCallerBuffer, LocalFileReaderFillsCallerBuffer) {
    const auto payload = Pattern(64 * 1024 + 17);
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        ASSERT_TRUE(writer.append(Slice(payload)).ok());
        ASSERT_TRUE(writer.finalize().ok());
    }
    io::LocalFileReader reader;
    ASSERT_TRUE(reader.open(path).ok());

    std::vector<uint8_t> buf(4096, 0);
    ASSERT_TRUE(reader.read_into(12345, buf.data(), buf.size()).ok());
    EXPECT_EQ(0, std::memcmp(payload.data() + 12345, buf.data(), buf.size()));

    // Both overloads agree byte-for-byte.
    std::vector<uint8_t> via_vector;
    ASSERT_TRUE(reader.read_at(12345, buf.size(), &via_vector).ok());
    EXPECT_EQ(via_vector, buf);

    EXPECT_FALSE(reader.read_into(payload.size(), buf.data(), 1).ok());
    std::remove(path.c_str());
}

} // namespace
