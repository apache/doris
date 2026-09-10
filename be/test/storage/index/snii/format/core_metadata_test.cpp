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

#include "storage/index/snii/format/core_metadata.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "gen_cpp/snii.pb.h"
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"

namespace doris::snii::format {
namespace {

using segment_v2::gram::GramMode;
using segment_v2::gram::GramScheme;

CoreMetadata sample_core(IndexConfig index_config = IndexConfig::kDocsOnly) {
    CoreMetadata metadata;
    metadata.index_config = index_config;
    metadata.stats = {.doc_count = 20,
                      .indexed_doc_count = 19,
                      .term_count = 18,
                      .sum_total_term_freq = 123,
                      .null_count = 1};
    metadata.section_refs = {.dict_region = {.offset = 10, .length = 11},
                             .posting_region = {.offset = 21, .length = 22},
                             .norms = {},
                             .null_bitmap = {.offset = 41, .length = 42},
                             .bsbf = {.offset = 51, .length = 52}};
    // Norms require positions for BM25 frequencies; docs-only samples omit them.
    if (has_positions(index_config)) {
        metadata.section_refs.norms = {.offset = 31, .length = 32};
    }
    return metadata;
}

std::vector<uint8_t> encode(const CoreMetadata& metadata) {
    ByteSink sink;
    EXPECT_TRUE(encode_core_metadata(metadata, &sink).ok());
    return sink.buffer();
}

std::vector<uint8_t> payload_of(const std::vector<uint8_t>& framed) {
    ByteSource source {Slice(framed)};
    FramedSection section;
    EXPECT_TRUE(SectionFramer::read(source, &section).ok());
    EXPECT_TRUE(source.eof());
    return std::vector<uint8_t>(section.payload.data(),
                                section.payload.data() + section.payload.size());
}

std::vector<uint8_t> frame_payload(
        const std::vector<uint8_t>& payload,
        uint8_t type = static_cast<uint8_t>(SectionType::kCoreMetadataPB)) {
    ByteSink sink;
    SectionFramer::write(sink, type, Slice(payload));
    return sink.buffer();
}

std::vector<uint8_t> mutate_core_payload(
        const CoreMetadata& metadata,
        const std::function<void(doris::snii::SniiCoreMetadataPB*)>& mutation) {
    const auto payload = payload_of(encode(metadata));
    doris::snii::SniiCoreMetadataPB core;
    EXPECT_TRUE(core.ParseFromArray(payload.data(), static_cast<int>(payload.size())));
    mutation(&core);
    std::string mutated;
    EXPECT_TRUE(core.SerializeToString(&mutated));
    return std::vector<uint8_t>(mutated.begin(), mutated.end());
}

void expect_core_eq(const CoreMetadata& expected, const CoreMetadata& actual) {
    EXPECT_EQ(expected.index_config, actual.index_config);
    EXPECT_EQ(expected.stats.doc_count, actual.stats.doc_count);
    EXPECT_EQ(expected.stats.indexed_doc_count, actual.stats.indexed_doc_count);
    EXPECT_EQ(expected.stats.term_count, actual.stats.term_count);
    EXPECT_EQ(expected.stats.sum_total_term_freq, actual.stats.sum_total_term_freq);
    EXPECT_EQ(expected.stats.null_count, actual.stats.null_count);
    EXPECT_EQ(expected.section_refs.dict_region.offset, actual.section_refs.dict_region.offset);
    EXPECT_EQ(expected.section_refs.dict_region.length, actual.section_refs.dict_region.length);
    EXPECT_EQ(expected.section_refs.posting_region.offset,
              actual.section_refs.posting_region.offset);
    EXPECT_EQ(expected.section_refs.posting_region.length,
              actual.section_refs.posting_region.length);
    EXPECT_EQ(expected.section_refs.norms.offset, actual.section_refs.norms.offset);
    EXPECT_EQ(expected.section_refs.norms.length, actual.section_refs.norms.length);
    EXPECT_EQ(expected.section_refs.null_bitmap.offset, actual.section_refs.null_bitmap.offset);
    EXPECT_EQ(expected.section_refs.null_bitmap.length, actual.section_refs.null_bitmap.length);
    EXPECT_EQ(expected.section_refs.bsbf.offset, actual.section_refs.bsbf.offset);
    EXPECT_EQ(expected.section_refs.bsbf.length, actual.section_refs.bsbf.length);
    EXPECT_EQ(expected.gram_scheme, actual.gram_scheme);
}

// Since A2, core metadata forbids norms on docs-only segments: norms require positions.
TEST(SniiCoreMetadata, RejectsNormsOnDocsOnlyIndex) {
    auto metadata = sample_core();
    metadata.section_refs.norms = {.offset = 31, .length = 32};
    ByteSink sink;
    const auto status = encode_core_metadata(metadata, &sink);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiCoreMetadata, RoundTripsDocsOnlyWithAllStatsAndRefs) {
    const auto expected = sample_core();
    CoreMetadata actual;
    ASSERT_TRUE(decode_core_metadata(Slice(encode(expected)), &actual).ok());
    expect_core_eq(expected, actual);
}

TEST(SniiCoreMetadata, RoundTripsPositions) {
    const auto expected = sample_core(IndexConfig::kDocsPositions);
    CoreMetadata actual;
    ASSERT_TRUE(decode_core_metadata(Slice(encode(expected)), &actual).ok());
    expect_core_eq(expected, actual);
}

// CommonGrams was removed: fields 4 (CommonGrams metadata) and 5 (posting policy) mark unsupported
// segments that require an index rebuild. Production writers never emitted these fields.
TEST(SniiCoreMetadata, RejectsLegacyCommonGramsMetadataFieldAsUnsupported) {
    auto payload = payload_of(encode(sample_core(IndexConfig::kDocsPositions)));
    // Field 4, length-delimited, with arbitrary contents.
    payload.push_back(static_cast<uint8_t>((4u << 3) | 2u));
    payload.push_back(3);
    payload.insert(payload.end(), {'c', 'g', '1'});

    CoreMetadata actual;
    const auto status = decode_core_metadata(Slice(frame_payload(payload)), &actual);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
    EXPECT_NE(status.to_string().find("CommonGrams"), std::string::npos) << status;
}

TEST(SniiCoreMetadata, RejectsLegacyCommonGramsPostingPolicyFieldAsUnsupported) {
    auto payload = payload_of(encode(sample_core(IndexConfig::kDocsPositions)));
    ByteSink field;
    field.put_varint32((5u << 3) | 0u);
    field.put_varint32(1);
    payload.insert(payload.end(), field.buffer().begin(), field.buffer().end());

    CoreMetadata actual;
    const auto status = decode_core_metadata(Slice(frame_payload(payload)), &actual);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
}

TEST(SniiCoreMetadata, AcceptsUnknownOptionalPbField) {
    auto payload = payload_of(encode(sample_core()));
    ByteSink unknown_field;
    unknown_field.put_varint32((100u << 3) | 0u);
    unknown_field.put_varint32(7);
    payload.insert(payload.end(), unknown_field.buffer().begin(), unknown_field.buffer().end());

    CoreMetadata actual;
    ASSERT_TRUE(decode_core_metadata(Slice(frame_payload(payload)), &actual).ok());
    expect_core_eq(sample_core(), actual);
}

TEST(SniiCoreMetadata, RejectsMissingRequiredLogicalField) {
    ByteSink payload;
    payload.put_varint32((1u << 3) | 0u);
    payload.put_varint32(static_cast<uint32_t>(IndexConfig::kDocsOnly));

    CoreMetadata actual;
    const auto status = decode_core_metadata(Slice(frame_payload(payload.buffer())), &actual);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiCoreMetadata, RejectsUnsupportedIndexConfig) {
    auto payload = payload_of(encode(sample_core()));
    ByteSink field;
    field.put_varint32((1u << 3) | 0u);
    field.put_varint32(9);
    payload.insert(payload.end(), field.buffer().begin(), field.buffer().end());

    CoreMetadata actual;
    const auto status = decode_core_metadata(Slice(frame_payload(payload)), &actual);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
}

// During development on master, index_config=2 denoted a scoring tier. The norms region now
// determines scoring capability, so reject this obsolete value. It was never deployed to
// production and has no compatibility requirements.
TEST(SniiCoreMetadata, RejectsLegacyScoringIndexConfigAsUnsupported) {
    auto payload = payload_of(encode(sample_core()));
    ByteSink field;
    field.put_varint32((1u << 3) | 0u);
    field.put_varint32(2);
    payload.insert(payload.end(), field.buffer().begin(), field.buffer().end());

    CoreMetadata actual;
    const auto status = decode_core_metadata(Slice(frame_payload(payload)), &actual);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
}

// The deployed 3.1-series writer omits stats.sum_total_term_freq and section_refs.norms. Such
// segments must remain readable, with missing statistics set to 0 and an empty norms region.
TEST(SniiCoreMetadata, AcceptsProductionShapeWithoutNormsAndTotalTermFreq) {
    const auto metadata = sample_core(IndexConfig::kDocsPositions);
    const auto payload = mutate_core_payload(metadata, [](auto* core) {
        core->mutable_stats()->clear_sum_total_term_freq();
        core->mutable_section_refs()->clear_norms();
    });
    CoreMetadata actual;
    const auto status = decode_core_metadata(Slice(frame_payload(payload)), &actual);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(actual.stats.doc_count, metadata.stats.doc_count);
    EXPECT_EQ(actual.stats.sum_total_term_freq, 0U);
    EXPECT_EQ(actual.section_refs.norms.offset, 0U);
    EXPECT_EQ(actual.section_refs.norms.length, 0U);
    EXPECT_EQ(actual.section_refs.dict_region.offset, metadata.section_refs.dict_region.offset);
    EXPECT_EQ(actual.section_refs.bsbf.length, metadata.section_refs.bsbf.length);
}

// Segments without norms omit section_refs.norms, matching the production writer's bytes.
TEST(SniiCoreMetadata, OmitsEmptyNormsRefOnEncode) {
    auto metadata = sample_core(IndexConfig::kDocsPositions);
    metadata.section_refs.norms = {};
    const auto framed = encode(metadata);
    const auto payload = payload_of(framed);
    doris::snii::SniiCoreMetadataPB core;
    ASSERT_TRUE(core.ParseFromArray(payload.data(), static_cast<int>(payload.size())));
    EXPECT_FALSE(core.section_refs().has_norms());
    EXPECT_TRUE(core.stats().has_sum_total_term_freq());
    CoreMetadata decoded;
    ASSERT_TRUE(decode_core_metadata(Slice(framed), &decoded).ok());
    EXPECT_EQ(decoded.section_refs.norms.length, 0U);
    expect_core_eq(metadata, decoded);
}

TEST(SniiCoreMetadata, RejectsMissingEachStatsField) {
    const auto metadata = sample_core();
    // sum_total_term_freq is optional and omitted by the production writer.
    for (const auto clear : std::array<void (doris::snii::SniiStatsPB::*)(), 4> {
                 &doris::snii::SniiStatsPB::clear_doc_count,
                 &doris::snii::SniiStatsPB::clear_indexed_doc_count,
                 &doris::snii::SniiStatsPB::clear_term_count,
                 &doris::snii::SniiStatsPB::clear_null_count}) {
        const auto payload = mutate_core_payload(
                metadata, [clear](auto* core) { (core->mutable_stats()->*clear)(); });
        CoreMetadata actual;
        const auto status = decode_core_metadata(Slice(frame_payload(payload)), &actual);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    }
}

TEST(SniiCoreMetadata, RejectsMissingEachRegionRefOffsetOrLength) {
    using RegionGetter = doris::snii::SniiRegionRefPB* (doris::snii::SniiSectionRefsPB::*)();
    const auto metadata = sample_core(IndexConfig::kDocsPositions);
    for (const auto region :
         std::array<RegionGetter, 5> {&doris::snii::SniiSectionRefsPB::mutable_dict_region,
                                      &doris::snii::SniiSectionRefsPB::mutable_posting_region,
                                      &doris::snii::SniiSectionRefsPB::mutable_norms,
                                      &doris::snii::SniiSectionRefsPB::mutable_null_bitmap,
                                      &doris::snii::SniiSectionRefsPB::mutable_bsbf}) {
        for (const auto clear : std::array<void (doris::snii::SniiRegionRefPB::*)(), 2> {
                     &doris::snii::SniiRegionRefPB::clear_offset,
                     &doris::snii::SniiRegionRefPB::clear_length}) {
            const auto payload = mutate_core_payload(metadata, [region, clear](auto* core) {
                auto* refs = core->mutable_section_refs();
                ((refs->*region)()->*clear)();
            });
            CoreMetadata actual;
            const auto status = decode_core_metadata(Slice(frame_payload(payload)), &actual);
            EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
        }
    }
}

TEST(SniiCoreMetadata, RejectsBadFrameTypeCrcAndTruncation) {
    const auto framed = encode(sample_core());
    CoreMetadata actual;

    const auto bad_type = frame_payload(payload_of(framed), /*obsolete non-Core frame type=*/1);
    EXPECT_TRUE(decode_core_metadata(Slice(bad_type), &actual)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());

    auto bad_crc = framed;
    bad_crc.back() ^= 1;
    EXPECT_TRUE(decode_core_metadata(Slice(bad_crc), &actual)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());

    std::vector<uint8_t> truncated(framed.begin(), framed.end() - 1);
    EXPECT_TRUE(decode_core_metadata(Slice(truncated), &actual)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiCoreMetadata, GramSchemeRoundTrip) {
    auto expected = sample_core();
    GramScheme scheme;
    scheme.mode = GramMode::DENSE;
    scheme.min_len = 3;
    scheme.max_len = 3;
    scheme.density_permille = 1000;
    scheme.lower_case = true;
    scheme.hash_version = 1;
    expected.gram_scheme = scheme;

    CoreMetadata actual;
    ASSERT_TRUE(decode_core_metadata(Slice(encode(expected)), &actual).ok());
    ASSERT_TRUE(actual.gram_scheme.has_value());
    EXPECT_TRUE(*actual.gram_scheme == scheme);
    expect_core_eq(expected, actual);

    // A non-gram index has no tokenizer scheme to persist.
    const auto none_framed = encode(sample_core());
    CoreMetadata none_actual;
    ASSERT_TRUE(decode_core_metadata(Slice(none_framed), &none_actual).ok());
    EXPECT_FALSE(none_actual.gram_scheme.has_value());

    const auto none_payload = payload_of(none_framed);
    doris::snii::SniiCoreMetadataPB none_pb;
    ASSERT_TRUE(none_pb.ParseFromArray(none_payload.data(), static_cast<int>(none_payload.size())));
    EXPECT_FALSE(none_pb.has_gram_scheme());
}

TEST(SniiCoreMetadata, GramSchemeSparseRoundTrip) {
    auto expected = sample_core();
    expected.gram_scheme = GramScheme {}; // member defaults: SPARSE / 3 / 16 / 250 / lc0 / v1

    CoreMetadata actual;
    ASSERT_TRUE(decode_core_metadata(Slice(encode(expected)), &actual).ok());
    ASSERT_TRUE(actual.gram_scheme.has_value());
    EXPECT_EQ(actual.gram_scheme->mode, GramMode::SPARSE);
    EXPECT_EQ(actual.gram_scheme->min_len, 3U);
    EXPECT_EQ(actual.gram_scheme->max_len, 16U);
    EXPECT_EQ(actual.gram_scheme->density_permille, 250U);
    expect_core_eq(expected, actual);
}

// Decoding has to validate the whole scheme, not just glance at mode: an out-of-range scheme that
// slips through would feed values such as min_len=0 straight into GramExtractor.
TEST(SniiCoreMetadata, RejectsCorruptGramScheme) {
    auto metadata = sample_core();
    metadata.gram_scheme = GramScheme {};

    // mode only allows 1 (DENSE) / 2 (SPARSE).
    const auto bad_mode = mutate_core_payload(
            metadata, [](auto* core) { core->mutable_gram_scheme()->set_mode(7); });
    CoreMetadata actual;
    auto status = decode_core_metadata(Slice(frame_payload(bad_mode)), &actual);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;

    // A partial message: only mode is set, every other field defaults to 0 by PB semantics, and 0
    // is not part of any valid scheme.
    const auto partial = mutate_core_payload(sample_core(), [](auto* core) {
        core->mutable_gram_scheme()->set_mode(static_cast<uint32_t>(GramMode::SPARSE));
    });
    status = decode_core_metadata(Slice(frame_payload(partial)), &actual);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiCoreMetadata, ResetsOutputBeforeDecodeFailureAndNullOutputIsInvalid) {
    auto populated = sample_core(IndexConfig::kDocsPositions);
    CoreMetadata reused;
    ASSERT_TRUE(decode_core_metadata(Slice(encode(populated)), &reused).ok());

    const auto status = decode_core_metadata(Slice(std::vector<uint8_t> {1}), &reused);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    expect_core_eq(CoreMetadata {}, reused);
    EXPECT_TRUE(decode_core_metadata(Slice(encode(sample_core())), nullptr)
                        .is<ErrorCode::INVALID_ARGUMENT>());
}

} // namespace
} // namespace doris::snii::format
