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

#include "storage/index/snii/bkd/point_run.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "storage/index/snii/writer/temp_dir.h"

namespace doris::snii::bkd {
namespace {

constexpr uint32_t kRecordSize = 12; // 8 value bytes + 4 doc id bytes

// A run file that removes itself, so a failing assertion cannot leave litter in
// the temp dir for the next run to trip over.
class ScopedRunPath {
public:
    explicit ScopedRunPath(const char* tag)
            : path_(writer::resolve_temp_dir() + "/bkd_point_run_test_" + tag + "_" +
                    std::to_string(::getpid()) + ".run") {}
    ~ScopedRunPath() { ::unlink(path_.c_str()); }

    ScopedRunPath(const ScopedRunPath&) = delete;
    ScopedRunPath& operator=(const ScopedRunPath&) = delete;

    const std::string& path() const { return path_; }

private:
    std::string path_;
};

// Deterministic record bodies: byte i of record n is a pure function of both, so
// a reader that returns the right COUNT but the wrong bytes still fails.
std::vector<uint8_t> make_records(uint32_t count) {
    std::vector<uint8_t> records(static_cast<size_t>(count) * kRecordSize);
    for (uint32_t n = 0; n < count; ++n) {
        for (uint32_t i = 0; i < kRecordSize; ++i) {
            records[static_cast<size_t>(n) * kRecordSize + i] =
                    static_cast<uint8_t>((n * 31 + i * 7 + 1) & 0xFF);
        }
    }
    return records;
}

// Drains a reader into one buffer, so the assertion is on the whole run rather
// than on a record at a time.
Status drain(PointRunReader* reader, std::vector<uint8_t>* out) {
    while (!reader->exhausted()) {
        const Slice record = reader->current();
        out->insert(out->end(), record.data(), record.data() + record.size());
        RETURN_IF_ERROR(reader->advance());
    }
    return Status::OK();
}

} // namespace

// A run is written once and read back once; the bytes that come out must be the
// bytes that went in, in order. Everything else in the merge rests on this.
TEST(BkdPointRunTest, RoundTripsEveryRecordInOrder) {
    for (const uint32_t count : {1U, 2U, 7U, 64U, 1000U}) {
        SCOPED_TRACE("count " + std::to_string(count));
        ScopedRunPath run("roundtrip");
        const std::vector<uint8_t> expected = make_records(count);

        PointRunWriter writer;
        ASSERT_TRUE(writer.open(run.path()).ok());
        ASSERT_TRUE(writer.append(Slice(expected)).ok());
        ASSERT_TRUE(writer.close().ok());

        PointRunReader reader;
        ASSERT_TRUE(reader.open(run.path(), kRecordSize, /*buffer_records=*/8).ok());
        std::vector<uint8_t> actual;
        ASSERT_TRUE(drain(&reader, &actual).ok());
        EXPECT_EQ(actual, expected);
        EXPECT_TRUE(reader.exhausted());
    }
}

// The buffer is a window, not a limit: a run larger than the window must refill
// transparently, and a window larger than the run must not read past its end.
TEST(BkdPointRunTest, BufferSizeDoesNotChangeWhatIsRead) {
    ScopedRunPath run("window");
    const std::vector<uint8_t> expected = make_records(257);

    PointRunWriter writer;
    ASSERT_TRUE(writer.open(run.path()).ok());
    ASSERT_TRUE(writer.append(Slice(expected)).ok());
    ASSERT_TRUE(writer.close().ok());

    for (const uint32_t buffer_records : {1U, 2U, 16U, 256U, 1024U}) {
        SCOPED_TRACE("buffer_records " + std::to_string(buffer_records));
        PointRunReader reader;
        ASSERT_TRUE(reader.open(run.path(), kRecordSize, buffer_records).ok());
        std::vector<uint8_t> actual;
        ASSERT_TRUE(drain(&reader, &actual).ok());
        EXPECT_EQ(actual, expected);
    }
}

// Appends are a stream, not a framing: the split points between them must not be
// observable to the reader.
TEST(BkdPointRunTest, AppendBoundariesAreInvisible) {
    ScopedRunPath run("appends");
    const std::vector<uint8_t> expected = make_records(100);

    PointRunWriter writer;
    ASSERT_TRUE(writer.open(run.path()).ok());
    // 7 + 1 + 42 + 50 records, deliberately uneven and not aligned to the
    // reader's window.
    size_t offset = 0;
    for (const uint32_t chunk : {7U, 1U, 42U, 50U}) {
        const size_t bytes = static_cast<size_t>(chunk) * kRecordSize;
        ASSERT_TRUE(writer.append(Slice(expected.data() + offset, bytes)).ok());
        offset += bytes;
    }
    ASSERT_EQ(offset, expected.size());
    ASSERT_TRUE(writer.close().ok());

    PointRunReader reader;
    ASSERT_TRUE(reader.open(run.path(), kRecordSize, /*buffer_records=*/16).ok());
    std::vector<uint8_t> actual;
    ASSERT_TRUE(drain(&reader, &actual).ok());
    EXPECT_EQ(actual, expected);
}

// An empty run is a legal shape -- the last resident run can be empty when the
// point count is an exact multiple of the buffer -- and must read as "exhausted"
// straight away rather than as an error.
TEST(BkdPointRunTest, EmptyRunIsExhaustedImmediately) {
    ScopedRunPath run("empty");
    PointRunWriter writer;
    ASSERT_TRUE(writer.open(run.path()).ok());
    ASSERT_TRUE(writer.close().ok());

    PointRunReader reader;
    ASSERT_TRUE(reader.open(run.path(), kRecordSize, /*buffer_records=*/8).ok());
    EXPECT_TRUE(reader.exhausted());
    std::vector<uint8_t> actual;
    ASSERT_TRUE(drain(&reader, &actual).ok());
    EXPECT_TRUE(actual.empty());
}

// A missing run is an IO failure, not a silently empty stream: swallowing it
// would drop points from the merge and produce an index that is short by a whole
// run with no error anywhere.
TEST(BkdPointRunTest, OpeningAMissingRunFails) {
    PointRunReader reader;
    const std::string missing = writer::resolve_temp_dir() + "/bkd_point_run_test_does_not_exist_" +
                                std::to_string(::getpid()) + ".run";
    EXPECT_FALSE(reader.open(missing, kRecordSize, /*buffer_records=*/8).ok());
}

} // namespace doris::snii::bkd
