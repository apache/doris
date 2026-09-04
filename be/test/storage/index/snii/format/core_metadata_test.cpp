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
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"

namespace doris::snii::format {
namespace {


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
    // norms 只对带位置的段合法（BM25 的词频来自位置）；docs-only 样本不带 norms。
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
}

// docs-only 段不能带 norms（norms 需要位置）：这是 A2 之后 core 元数据的一条硬约束。
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

// CommonGrams 已删除：写过字段 4（CommonGrams 元数据）或字段 5（posting 策略）的段是墓碑，
// 必须重建索引。生产 writer 从未写过这两个字段。
TEST(SniiCoreMetadata, RejectsLegacyCommonGramsMetadataFieldAsUnsupported) {
    auto payload = payload_of(encode(sample_core(IndexConfig::kDocsPositions)));
    // 字段 4，length-delimited，任意内容。
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

// master 开发期曾用 index_config=2 表示"带打分 tier"；打分能力现在由 norms region 表达，
// 该取值不再有意义，按不支持拒绝（从未进入生产，不存在兼容负担）。
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

// 已上线的生产 writer（selectdb-core 4.1.7 系）不写 stats.sum_total_term_freq 与
// section_refs.norms：这种形态必须能打开，缺失的统计按 0、norms 按空 region 处理。
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

// 没有 norms 的段编码时不写 section_refs.norms（与生产 writer 的字节形态一致）。
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
    // sum_total_term_freq 是可选字段（生产 writer 不写），不在必填之列。
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
