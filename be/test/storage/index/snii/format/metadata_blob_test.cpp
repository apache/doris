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

#include "storage/index/snii/format/metadata_blob.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii_query_test_util.h"

using namespace doris::snii;
using namespace doris::snii::format;
using doris::snii::snii_test::ScopedEnv;

namespace {

std::vector<uint8_t> BuildSampled(uint32_t n) {
    SampledTermIndexBuilder builder;
    for (uint32_t i = 0; i < n; ++i) {
        builder.add_block_first_term("term_" + std::to_string(1000000 + i));
    }
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

std::vector<uint8_t> BuildDirectory() {
    DictBlockDirectoryBuilder builder;
    builder.add(BlockRef {.offset = 100, .length = 20, .n_entries = 1, .checksum = 123});
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

struct MetadataFrameCase {
    std::vector<uint8_t> raw;
    SectionType raw_type;
    SectionType compressed_type;
};

std::vector<MetadataFrameCase> MetadataFrameCases() {
    return {{BuildSampled(3), SectionType::kSampledTermIndex, SectionType::kSampledTermIndexZstd},
            {BuildDirectory(), SectionType::kDictBlockDirectory,
             SectionType::kDictBlockDirectoryZstd}};
}

std::vector<uint8_t> FramePayload(SectionType type, Slice payload) {
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(type), payload);
    return sink.buffer();
}

SectionType StoredType(Slice frame) {
    ByteSource source(frame);
    FramedSection section;
    EXPECT_TRUE(SectionFramer::read(source, &section).ok());
    return static_cast<SectionType>(section.type);
}

std::vector<uint8_t> CarrierPayload(uint64_t uncomp_len, Slice compressed) {
    ByteSink payload;
    payload.put_varint64(uncomp_len);
    payload.put_bytes(compressed);
    return payload.buffer();
}

void ExpectBytesEq(Slice actual, Slice expected) {
    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_EQ(std::memcmp(actual.data(), expected.data(), actual.size()), 0);
}

} // namespace

TEST(SniiMetadataBlob, RejectsRawFrameWithWrongType) {
    const auto raw = BuildSampled(3);
    ByteSink stored;

    EXPECT_TRUE(encode_metadata_blob(Slice(raw), SectionType::kDictBlockDirectory,
                                     SectionType::kDictBlockDirectoryZstd, &stored)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiMetadataBlob, RejectsTrailingBytesOnRawEncodeAndMaterialize) {
    for (const auto& test : MetadataFrameCases()) {
        auto trailing = test.raw;
        trailing.push_back(0xEE);

        ByteSink stored;
        const doris::Status encode_status =
                encode_metadata_blob(Slice(trailing), test.raw_type, test.compressed_type, &stored);
        EXPECT_TRUE(encode_status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << encode_status;

        std::vector<uint8_t> scratch;
        Slice materialized;
        const doris::Status materialize_status = materialize_metadata_blob(
                Slice(trailing), test.raw_type, test.compressed_type, &scratch, &materialized);
        EXPECT_TRUE(materialize_status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
                << materialize_status;
    }
}

TEST(SniiMetadataBlob, RejectsTrailingOuterAndMalformedInnerCompressedFrames) {
    const auto tests = MetadataFrameCases();
    for (size_t i = 0; i < tests.size(); ++i) {
        const auto& test = tests[i];
        std::vector<uint8_t> compressed;
        ASSERT_TRUE(zstd_compress(Slice(test.raw), 3, &compressed).ok());
        const auto payload = CarrierPayload(test.raw.size(), Slice(compressed));
        auto outer_trailing = FramePayload(test.compressed_type, Slice(payload));
        outer_trailing.push_back(0xEE);

        std::vector<uint8_t> scratch;
        Slice materialized;
        doris::Status status =
                materialize_metadata_blob(Slice(outer_trailing), test.raw_type,
                                          test.compressed_type, &scratch, &materialized);
        EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;

        auto inner_trailing = test.raw;
        inner_trailing.push_back(0xEE);
        ASSERT_TRUE(zstd_compress(Slice(inner_trailing), 3, &compressed).ok());
        const auto inner_trailing_payload =
                CarrierPayload(inner_trailing.size(), Slice(compressed));
        const auto inner_trailing_carrier =
                FramePayload(test.compressed_type, Slice(inner_trailing_payload));
        status = materialize_metadata_blob(Slice(inner_trailing_carrier), test.raw_type,
                                           test.compressed_type, &scratch, &materialized);
        EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;

        const auto& wrong_inner = tests[(i + 1) % tests.size()].raw;
        ASSERT_TRUE(zstd_compress(Slice(wrong_inner), 3, &compressed).ok());
        const auto wrong_type_payload = CarrierPayload(wrong_inner.size(), Slice(compressed));
        const auto wrong_type_carrier =
                FramePayload(test.compressed_type, Slice(wrong_type_payload));
        status = materialize_metadata_blob(Slice(wrong_type_carrier), test.raw_type,
                                           test.compressed_type, &scratch, &materialized);
        EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    }
}

TEST(SniiMetadataBlob, KeepsSmallRawFrameByteIdentical) {
    const auto raw = BuildSampled(3);
    ByteSink stored;
    ASSERT_TRUE(encode_metadata_blob(Slice(raw), SectionType::kSampledTermIndex,
                                     SectionType::kSampledTermIndexZstd, &stored)
                        .ok());
    EXPECT_EQ(stored.buffer(), raw);

    std::vector<uint8_t> scratch;
    Slice materialized;
    ASSERT_TRUE(materialize_metadata_blob(stored.view(), SectionType::kSampledTermIndex,
                                          SectionType::kSampledTermIndexZstd, &scratch,
                                          &materialized)
                        .ok());
    EXPECT_TRUE(scratch.empty());
    ExpectBytesEq(materialized, Slice(raw));
}

TEST(SniiMetadataBlob, ThresholdGateControlsCompression) {
    const auto raw = BuildSampled(300);
    ASSERT_LT(raw.size(), kMetaSectionCompressMinBytes);

    ByteSink default_stored;
    ASSERT_TRUE(encode_metadata_blob(Slice(raw), SectionType::kSampledTermIndex,
                                     SectionType::kSampledTermIndexZstd, &default_stored)
                        .ok());
    EXPECT_EQ(default_stored.buffer(), raw);

    ByteSink forced_stored;
    {
        ScopedEnv force("SNII_META_COMPRESS_MIN", "1");
        ASSERT_TRUE(encode_metadata_blob(Slice(raw), SectionType::kSampledTermIndex,
                                         SectionType::kSampledTermIndexZstd, &forced_stored)
                            .ok());
    }
    EXPECT_EQ(StoredType(forced_stored.view()), SectionType::kSampledTermIndexZstd);
    EXPECT_LT(forced_stored.size(), raw.size());

    std::vector<uint8_t> scratch;
    Slice materialized;
    ASSERT_TRUE(materialize_metadata_blob(forced_stored.view(), SectionType::kSampledTermIndex,
                                          SectionType::kSampledTermIndexZstd, &scratch,
                                          &materialized)
                        .ok());
    ExpectBytesEq(materialized, Slice(raw));
}

TEST(SniiMetadataBlob, CompressesLargeFrameAndMaterializesByteExactly) {
    const auto raw = BuildSampled(2000);
    ASSERT_GT(raw.size(), kMetaSectionCompressMinBytes);
    ByteSink stored;
    ASSERT_TRUE(encode_metadata_blob(Slice(raw), SectionType::kSampledTermIndex,
                                     SectionType::kSampledTermIndexZstd, &stored)
                        .ok());
    EXPECT_EQ(StoredType(stored.view()), SectionType::kSampledTermIndexZstd);
    EXPECT_LT(stored.size(), raw.size());

    std::vector<uint8_t> scratch;
    Slice materialized;
    ASSERT_TRUE(materialize_metadata_blob(stored.view(), SectionType::kSampledTermIndex,
                                          SectionType::kSampledTermIndexZstd, &scratch,
                                          &materialized)
                        .ok());
    ExpectBytesEq(materialized, Slice(raw));
}

TEST(SniiMetadataBlob, KeepsIncompressibleFrameRaw) {
    std::vector<uint8_t> payload(kMetaSectionCompressMinBytes * 2);
    uint32_t state = 0x12345678U;
    for (uint8_t& byte : payload) {
        state = state * 1664525U + 1013904223U;
        byte = static_cast<uint8_t>(state >> 24U);
    }
    const auto raw = FramePayload(SectionType::kSampledTermIndex, Slice(payload));
    ByteSink stored;
    ASSERT_TRUE(encode_metadata_blob(Slice(raw), SectionType::kSampledTermIndex,
                                     SectionType::kSampledTermIndexZstd, &stored)
                        .ok());
    EXPECT_EQ(stored.buffer(), raw);
}

TEST(SniiMetadataBlob, RejectsCorruptZstdPayload) {
    std::vector<uint8_t> garbage(64, 0xAB);
    const auto payload = CarrierPayload(1024, Slice(garbage));
    const auto stored = FramePayload(SectionType::kSampledTermIndexZstd, Slice(payload));

    std::vector<uint8_t> scratch;
    Slice materialized;
    EXPECT_TRUE(materialize_metadata_blob(Slice(stored), SectionType::kSampledTermIndex,
                                          SectionType::kSampledTermIndexZstd, &scratch,
                                          &materialized)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiMetadataBlob, RejectsTruncatedOrMisdeclaredZstdPayload) {
    const auto raw = BuildSampled(500);
    std::vector<uint8_t> compressed;
    ASSERT_TRUE(zstd_compress(Slice(raw), 3, &compressed).ok());

    for (const auto& [uncomp_len, comp] :
         {std::pair<uint64_t, Slice> {raw.size(), Slice(compressed.data(), compressed.size() / 2)},
          std::pair<uint64_t, Slice> {raw.size() - 1, Slice(compressed)}}) {
        const auto payload = CarrierPayload(uncomp_len, comp);
        const auto stored = FramePayload(SectionType::kSampledTermIndexZstd, Slice(payload));
        std::vector<uint8_t> scratch;
        Slice materialized;
        EXPECT_TRUE(materialize_metadata_blob(Slice(stored), SectionType::kSampledTermIndex,
                                              SectionType::kSampledTermIndexZstd, &scratch,
                                              &materialized)
                            .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    }
}

TEST(SniiMetadataBlob, RejectsOutOfRangeZstdUncompressedLength) {
    std::vector<uint8_t> compressed(16, 0x01);
    for (const uint64_t uncomp_len : {uint64_t {0}, uint64_t {256ULL * 1024 * 1024 + 1}}) {
        const auto payload = CarrierPayload(uncomp_len, Slice(compressed));
        const auto stored = FramePayload(SectionType::kSampledTermIndexZstd, Slice(payload));
        std::vector<uint8_t> scratch;
        Slice materialized;
        EXPECT_TRUE(materialize_metadata_blob(Slice(stored), SectionType::kSampledTermIndex,
                                              SectionType::kSampledTermIndexZstd, &scratch,
                                              &materialized)
                            .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    }
}
