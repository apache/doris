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

#include "storage/index/snii/format/norms_pod.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/format/format_constants.h"

using namespace doris::snii;
using doris::Status; // RETURN_IF_ERROR expands to bare Status
using doris::snii::format::NormsPodReader;
using doris::snii::format::NormsPodWriter;

namespace {

// Use writer to encode a sequence of encoded_norms into a framed payload and return the buffer.
std::vector<uint8_t> BuildPod(const std::vector<uint8_t>& norms) {
    NormsPodWriter writer;
    for (uint8_t n : norms) {
        writer.add(n);
    }
    ByteSink sink;
    writer.finish(&sink);
    return sink.buffer();
}

} // namespace

// After writing N norms, read them back per-doc and verify they match.
TEST(SniiNormsPod, RoundTripValues) {
    std::vector<uint8_t> norms = {0, 1, 7, 42, 128, 200, 255};
    NormsPodWriter writer;
    for (uint8_t n : norms) {
        writer.add(n);
    }
    EXPECT_EQ(writer.count(), norms.size());

    ByteSink sink;
    writer.finish(&sink);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(sink.view(), &reader).ok());
    ASSERT_EQ(reader.doc_count(), norms.size());
    for (uint32_t docid = 0; docid < norms.size(); ++docid) {
        EXPECT_EQ(reader.encoded_norm(docid), norms[docid]) << "docid=" << docid;
    }
}

// Large-scale round-trip covering the multi-byte varint doc_count path.
TEST(SniiNormsPod, RoundTripLarge) {
    std::vector<uint8_t> norms;
    norms.reserve(5000);
    for (uint32_t i = 0; i < 5000; ++i) {
        norms.push_back(static_cast<uint8_t>((i * 31 + 7) & 0xFF));
    }
    auto buf = BuildPod(norms);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(buf), &reader).ok());
    ASSERT_EQ(reader.doc_count(), 5000U);
    for (uint32_t docid = 0; docid < 5000; ++docid) {
        EXPECT_EQ(reader.encoded_norm(docid), norms[docid]) << "docid=" << docid;
    }
}

// Empty POD: count = 0 is valid and open should succeed.
TEST(SniiNormsPod, EmptyPod) {
    NormsPodWriter writer;
    EXPECT_EQ(writer.count(), 0U);

    ByteSink sink;
    writer.finish(&sink);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.doc_count(), 0U);
}

// CRC corruption is detectable: flipping a byte in the payload causes open to fail.
// The integrated reader frames via SectionFramer, which reports a CRC mismatch as
// INVERTED_INDEX_FILE_CORRUPTED (the standalone test's generic Corruption code was
// replaced during integration with this inverted-index-specific code).
TEST(SniiNormsPod, DetectsCorruption) {
    std::vector<uint8_t> norms = {10, 20, 30, 40, 50};
    auto buf = BuildPod(norms);
    // Flip a byte near the end so the framer CRC no longer matches the payload.
    buf[buf.size() - 3] ^= 0xFF;

    NormsPodReader reader;
    Status s = NormsPodReader::open(Slice(buf), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// Truncated input should return an error rather than crash.
TEST(SniiNormsPod, DetectsTruncation) {
    std::vector<uint8_t> norms = {1, 2, 3, 4, 5, 6, 7, 8};
    auto buf = BuildPod(norms);
    buf.resize(buf.size() - 4); // Chop off the trailing CRC region.

    NormsPodReader reader;
    Status s = NormsPodReader::open(Slice(buf), &reader);
    EXPECT_FALSE(s.ok());
}

// A mismatch between the declared doc_count and the actual payload byte count should be detected.
// The integrated reader reports this as INVERTED_INDEX_FILE_CORRUPTED.
TEST(SniiNormsPod, DetectsLengthMismatch) {
    // Manually construct: framer payload = [varint doc_count=4][only 2 norm bytes].
    ByteSink payload;
    payload.put_varint64(4);
    payload.put_u8(11);
    payload.put_u8(22);

    ByteSink sink;
    // Reuse the framer to ensure a self-consistent CRC, specifically to trigger the length-mismatch branch.
    doris::snii::SectionFramer::write(
            sink, static_cast<uint8_t>(doris::snii::format::SectionType::kStatsBlock),
            payload.view());

    NormsPodReader reader;
    Status s = NormsPodReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

#ifndef NDEBUG
// In a debug build, an out-of-range docid triggers an assertion (death test).
TEST(SniiNormsPodDeathTest, OutOfRangeDocidAsserts) {
    std::vector<uint8_t> norms = {3, 6, 9};
    auto buf = BuildPod(norms);
    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(buf), &reader).ok());
    EXPECT_DEATH({ (void)reader.encoded_norm(3); }, "");
}
#endif

// Checked access: a valid docid returns the value, an out-of-range docid returns
// InvalidArgument (also effective in Release builds).
TEST(SniiNormsPod, TryEncodedNormChecksBounds) {
    std::vector<uint8_t> norms = {3, 6, 9};
    auto buf = BuildPod(norms);
    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(buf), &reader).ok());
    uint8_t v = 0;
    ASSERT_TRUE(reader.try_encoded_norm(1, &v).ok());
    EXPECT_EQ(v, 6U);
    Status s = reader.try_encoded_norm(3, &v);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>()) << s.to_string();
}
