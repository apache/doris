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

#include "storage/index/snii/format/sampled_term_index.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/format/format_constants.h"

using namespace doris::snii;         // NOLINT
using namespace doris::snii::format; // NOLINT
using doris::Status;

namespace {

// Build a SampledTermIndex byte buffer from an ordered set of first_terms.
std::vector<uint8_t> BuildIndex(const std::vector<std::string>& first_terms) {
    SampledTermIndexBuilder builder;
    for (const auto& t : first_terms) {
        builder.add_block_first_term(t);
    }
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

// Convenience wrapper to open a reader.
SampledTermIndexReader OpenOrDie(const std::vector<uint8_t>& bytes) {
    SampledTermIndexReader reader;
    Status s = SampledTermIndexReader::open(Slice(bytes), &reader);
    EXPECT_TRUE(s.ok()) << s.to_string();
    return reader;
}

} // namespace

// Multiple blocks: locate returns the correct ordinal for each first_term.
TEST(SniiSampledTermIndex, LocateExactFirstTermHitsOrdinal) {
    const std::vector<std::string> terms = {"alpha", "delta", "kappa", "omega"};
    auto bytes = BuildIndex(terms);
    auto reader = OpenOrDie(bytes);
    ASSERT_EQ(reader.n_blocks(), 4U);

    for (uint32_t i = 0; i < terms.size(); ++i) {
        bool maybe_present = false;
        uint32_t ord = 0xFFFFFFFFU;
        ASSERT_TRUE(reader.locate(terms[i], &maybe_present, &ord).ok());
        EXPECT_TRUE(maybe_present) << "term=" << terms[i];
        EXPECT_EQ(ord, i) << "term=" << terms[i];
    }
}

// target falls between two first_terms → returns the ordinal of the lower one.
TEST(SniiSampledTermIndex, LocateBetweenReturnsLowerOrdinal) {
    const std::vector<std::string> terms = {"alpha", "delta", "kappa", "omega"};
    auto bytes = BuildIndex(terms);
    auto reader = OpenOrDie(bytes);

    bool maybe_present = false;
    uint32_t ord = 0;
    // "echo" is between "delta"(1) and "kappa"(2) → should land in block 1.
    ASSERT_TRUE(reader.locate("echo", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 1U);

    // "beta" is between "alpha"(0) and "delta"(1) → block 0.
    ASSERT_TRUE(reader.locate("beta", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 0U);

    // "zzz" > "omega"(3, the LAST block's first term) routes to the last block: the
    // last block can hold terms greater than its first term, so locate must return a
    // candidate (find_term then confirms). This is a router, not an exact set.
    ASSERT_TRUE(reader.locate("zzz", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 3U);
}

// target < min_term → maybe_present=false (not-present signal).
TEST(SniiSampledTermIndex, LocateBelowMinIsOutOfRange) {
    const std::vector<std::string> terms = {"banana", "cherry", "mango"};
    auto bytes = BuildIndex(terms);
    auto reader = OpenOrDie(bytes);

    bool maybe_present = true;
    uint32_t ord = 12345;
    ASSERT_TRUE(reader.locate("apple", &maybe_present, &ord).ok());
    EXPECT_FALSE(maybe_present);
}

// target > the last block's first term routes to the LAST block (that block may
// contain terms greater than its first term; find_term confirms presence). The
// router must NOT reject such a target, or present tail-of-last-block terms would
// be falsely reported absent.
TEST(SniiSampledTermIndex, LocateAboveLastFirstTermRoutesToLastBlock) {
    const std::vector<std::string> terms = {"banana", "cherry", "mango"};
    auto bytes = BuildIndex(terms);
    auto reader = OpenOrDie(bytes);

    bool maybe_present = false;
    uint32_t ord = 0;
    ASSERT_TRUE(reader.locate("zebra", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 2U);
}

// target == min_term / == max_term boundary cases should both hit and be in-range.
TEST(SniiSampledTermIndex, LocateBoundaryTermsInRange) {
    const std::vector<std::string> terms = {"banana", "cherry", "mango"};
    auto bytes = BuildIndex(terms);
    auto reader = OpenOrDie(bytes);

    bool maybe_present = false;
    uint32_t ord = 0;
    ASSERT_TRUE(reader.locate("banana", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 0U);

    ASSERT_TRUE(reader.locate("mango", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 2U);
}

// Single block: min==max==the only first_term.
TEST(SniiSampledTermIndex, SingleBlock) {
    const std::vector<std::string> terms = {"solo"};
    auto bytes = BuildIndex(terms);
    auto reader = OpenOrDie(bytes);
    ASSERT_EQ(reader.n_blocks(), 1U);

    bool maybe_present = false;
    uint32_t ord = 99;
    ASSERT_TRUE(reader.locate("solo", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 0U);

    // "solz" > "solo" routes to the single (last) block: that block may hold terms
    // greater than its first term, so locate returns block 0 (find_term confirms).
    ASSERT_TRUE(reader.locate("solz", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 0U);

    // "sola" < "solo" → before the first block, so it cannot exist (out of range).
    ASSERT_TRUE(reader.locate("sola", &maybe_present, &ord).ok());
    EXPECT_FALSE(maybe_present);
}

// Terms sharing a long common prefix (verify front coding round-trip correctness).
TEST(SniiSampledTermIndex, SharedPrefixTermsRoundTrip) {
    const std::vector<std::string> terms = {"international", "internationalize",
                                            "internationalized", "internet", "interoperate"};
    auto bytes = BuildIndex(terms);
    auto reader = OpenOrDie(bytes);
    ASSERT_EQ(reader.n_blocks(), 5U);

    for (uint32_t i = 0; i < terms.size(); ++i) {
        bool maybe_present = false;
        uint32_t ord = 0;
        ASSERT_TRUE(reader.locate(terms[i], &maybe_present, &ord).ok());
        EXPECT_TRUE(maybe_present) << "term=" << terms[i];
        EXPECT_EQ(ord, i) << "term=" << terms[i];
    }

    // "internationalizes" is between idx2 and idx3 → lower ordinal 2.
    bool maybe_present = false;
    uint32_t ord = 0;
    ASSERT_TRUE(reader.locate("internationalizes", &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 2U);
}

// Terms containing null bytes / high-bit bytes, compared by unsigned byte order.
TEST(SniiSampledTermIndex, BinarySafeTerms) {
    std::vector<std::string> terms;
    terms.emplace_back("\x01\x00z", 3);
    terms.emplace_back("\x80\x00", 2);
    terms.emplace_back("\xFF", 1);
    auto bytes = BuildIndex(terms);
    auto reader = OpenOrDie(bytes);
    ASSERT_EQ(reader.n_blocks(), 3U);

    bool maybe_present = false;
    uint32_t ord = 0;
    ASSERT_TRUE(reader.locate(std::string("\x80\x00", 2), &maybe_present, &ord).ok());
    EXPECT_TRUE(maybe_present);
    EXPECT_EQ(ord, 1U);
}

// The first byte is the SectionFramer type and must be kSampledTermIndex.
TEST(SniiSampledTermIndex, FramedAsSampledTermIndexType) {
    auto bytes = BuildIndex({"alpha", "beta"});
    ASSERT_GE(bytes.size(), 1U);
    EXPECT_EQ(bytes[0], static_cast<uint8_t>(SectionType::kSampledTermIndex));
}

// CRC checksum: flipping one byte in the payload → open returns Corruption.
TEST(SniiSampledTermIndex, DetectsCorruption) {
    auto bytes = BuildIndex({"alpha", "delta", "kappa"});
    ASSERT_GE(bytes.size(), 4U);
    bytes[3] ^= 0xFF; // Flip a byte in the payload region (skip the type+len prefix)
    SampledTermIndexReader reader;
    Status s = SampledTermIndexReader::open(Slice(bytes), &reader);
    EXPECT_FALSE(s.ok()) << s.to_string();
}

// Truncation: drop the trailing byte → open fails.
TEST(SniiSampledTermIndex, DetectsTruncation) {
    auto bytes = BuildIndex({"alpha", "delta", "kappa"});
    bytes.pop_back();
    SampledTermIndexReader reader;
    Status s = SampledTermIndexReader::open(Slice(bytes), &reader);
    EXPECT_FALSE(s.ok());
}

// A wrong section type should be rejected.
TEST(SniiSampledTermIndex, WrongSectionTypeRejected) {
    ByteSink sink;
    const uint8_t p[] = {0, 0};
    SectionFramer::write(sink, static_cast<uint8_t>(SectionType::kXFilter), Slice(p, 2));
    SampledTermIndexReader reader;
    Status s = SampledTermIndexReader::open(sink.view(), &reader);
    EXPECT_FALSE(s.ok());
}

// Empty builder (n_blocks=0): valid build, locate on any term is out of range.
TEST(SniiSampledTermIndex, EmptyIndexLocateOutOfRange) {
    auto bytes = BuildIndex({});
    auto reader = OpenOrDie(bytes);
    ASSERT_EQ(reader.n_blocks(), 0U);

    bool maybe_present = true;
    uint32_t ord = 7;
    ASSERT_TRUE(reader.locate("anything", &maybe_present, &ord).ok());
    EXPECT_FALSE(maybe_present);
}
