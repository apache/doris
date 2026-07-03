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

#include "storage/index/snii/format/per_index_meta.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/format/stats_block.h"
#include "storage/index/snii_query_test_util.h"

using namespace doris::snii;
using namespace doris::snii::format;
using doris::snii::snii_test::ScopedEnv;

// Env value that disables G13 meta-section compression (threshold == SIZE_MAX).
static const char* const kCompressOff = "18446744073709551615";

namespace {

// Produces the raw framed bytes of a SampledTermIndex from a list of first_terms.
std::vector<uint8_t> BuildSampled(const std::vector<std::string>& terms) {
    SampledTermIndexBuilder b;
    for (const auto& t : terms) {
        b.add_block_first_term(t);
    }
    ByteSink sink;
    b.finish(&sink);
    return sink.buffer();
}

// Produces the raw framed bytes of a DICT block directory from a list of refs.
std::vector<uint8_t> BuildDict(const std::vector<BlockRef>& refs) {
    DictBlockDirectoryBuilder b;
    for (const auto& r : refs) {
        b.add(r);
    }
    ByteSink sink;
    b.finish(&sink);
    return sink.buffer();
}

StatsBlock SampleStats() {
    StatsBlock sb;
    sb.doc_count = 1000;
    sb.indexed_doc_count = 950;
    sb.term_count = 4242;
    sb.sum_total_term_freq = 1234567;
    sb.null_count = 50;
    return sb;
}

SectionRefs SampleRefs() {
    SectionRefs r;
    r.dict_region = {.offset = 4096, .length = 65536};
    r.posting_region = {.offset = 70000, .length = 24576}; // single interleaved [prx][frq] region
    r.norms = {.offset = 100000, .length = 4096};
    r.null_bitmap = {.offset = 0, .length = 0}; // absent
    r.bsbf = {.offset = 120000, .length = 32768};
    return r;
}

void ExpectRegionEq(const RegionRef& a, const RegionRef& b) {
    EXPECT_EQ(a.offset, b.offset);
    EXPECT_EQ(a.length, b.length);
}

void ExpectRefsEq(const SectionRefs& a, const SectionRefs& b) {
    ExpectRegionEq(a.dict_region, b.dict_region);
    ExpectRegionEq(a.posting_region, b.posting_region);
    ExpectRegionEq(a.norms, b.norms);
    ExpectRegionEq(a.null_bitmap, b.null_bitmap);
    ExpectRegionEq(a.bsbf, b.bsbf);
}

void ExpectStatsEq(const StatsBlock& a, const StatsBlock& b) {
    EXPECT_EQ(a.doc_count, b.doc_count);
    EXPECT_EQ(a.indexed_doc_count, b.indexed_doc_count);
    EXPECT_EQ(a.term_count, b.term_count);
    EXPECT_EQ(a.sum_total_term_freq, b.sum_total_term_freq);
    EXPECT_EQ(a.null_count, b.null_count);
}

// Builds a full per-index meta block (SectionRefs carry the bsbf section ref).
std::vector<uint8_t> BuildMeta(uint64_t index_id, std::string suffix,
                               const std::vector<std::string>& sample_terms,
                               const std::vector<BlockRef>& dict_refs) {
    PerIndexMetaBuilder builder(index_id, std::move(suffix), PerIndexMetaBuilder::kHasBsbf);
    builder.set_stats(SampleStats());
    builder.set_sampled_term_index(Slice(BuildSampled(sample_terms)));
    builder.set_dict_block_directory(Slice(BuildDict(dict_refs)));
    builder.set_section_refs(SampleRefs());
    ByteSink sink;
    static_cast<void>(builder.finish(&sink));
    return sink.buffer();
}

} // namespace

TEST(SniiPerIndexMeta, RoundTrip) {
    std::vector<std::string> sample_terms = {"alpha", "kappa", "zeta"};
    std::vector<BlockRef> dict_refs = {
            {.offset = 4096, .length = 1024, .n_entries = 10, .flags = 0, .checksum = 0x11111111U},
            {.offset = 5120, .length = 2048, .n_entries = 20, .flags = 1, .checksum = 0x22222222U},
            {.offset = 7168, .length = 512, .n_entries = 5, .flags = 0, .checksum = 0x33333333U},
    };
    auto bytes = BuildMeta(7, "title", sample_terms, dict_refs);

    PerIndexMetaReader reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader).ok());

    EXPECT_EQ(reader.index_id(), 7U);
    EXPECT_EQ(reader.index_suffix(), "title");
    // The bsbf XFilter is a physical section ref, not embedded in the meta.
    EXPECT_TRUE(reader.has_bsbf());
    EXPECT_NE(reader.flags() & PerIndexMetaBuilder::kHasBsbf, 0U);

    ExpectStatsEq(reader.stats(), SampleStats());
    ExpectRefsEq(reader.section_refs(), SampleRefs());

    // The materialized sub-section frames must be openable by their own readers.
    // Small sections stay raw, so the frames alias the block and leave the
    // scratch untouched.
    std::vector<uint8_t> sti_scratch;
    Slice sti_frame;
    ASSERT_TRUE(reader.sampled_term_index_frame(&sti_scratch, &sti_frame).ok());
    EXPECT_TRUE(sti_scratch.empty());
    SampledTermIndexReader sti;
    ASSERT_TRUE(SampledTermIndexReader::open(sti_frame, &sti).ok());
    EXPECT_EQ(sti.n_blocks(), 3U);
    bool maybe = false;
    uint32_t ord = 0;
    ASSERT_TRUE(sti.locate("kappa", &maybe, &ord).ok());
    EXPECT_TRUE(maybe);
    EXPECT_EQ(ord, 1U);

    std::vector<uint8_t> dbd_scratch;
    Slice dbd_frame;
    ASSERT_TRUE(reader.dict_block_directory_frame(&dbd_scratch, &dbd_frame).ok());
    EXPECT_TRUE(dbd_scratch.empty());
    DictBlockDirectoryReader dbd;
    ASSERT_TRUE(DictBlockDirectoryReader::open(dbd_frame, &dbd).ok());
    ASSERT_EQ(dbd.n_blocks(), 3U);
    BlockRef got {};
    ASSERT_TRUE(dbd.get(1, &got).ok());
    EXPECT_EQ(got.offset, 5120U);
    EXPECT_EQ(got.checksum, 0x22222222U);
}

TEST(SniiPerIndexMeta, HasPositionsFlagRoundTrips) {
    // The kHasPositions header bit must survive a build/open round-trip and be read
    // back via the convenience accessor (capability lives in the flag, not a length).
    PerIndexMetaBuilder builder(9, "body",
                                PerIndexMetaBuilder::kHasPositions | PerIndexMetaBuilder::kHasBsbf);
    builder.set_stats(SampleStats());
    builder.set_sampled_term_index(Slice(BuildSampled({"a"})));
    builder.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    builder.set_section_refs(SampleRefs());
    ByteSink sink;
    ASSERT_TRUE(builder.finish(&sink).ok());

    PerIndexMetaReader reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(sink.buffer()), &reader).ok());
    EXPECT_TRUE(reader.has_positions());
    EXPECT_TRUE(reader.has_bsbf());
    EXPECT_NE(reader.flags() & PerIndexMetaBuilder::kHasPositions, 0U);

    // A docs-only meta (flag clear) reports has_positions() == false even though its
    // posting_region length is non-zero -- capability never comes from a region length.
    PerIndexMetaBuilder docs_only(10, "tag", 0U);
    docs_only.set_stats(SampleStats());
    docs_only.set_sampled_term_index(Slice(BuildSampled({"a"})));
    docs_only.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    docs_only.set_section_refs(SampleRefs()); // posting_region.length > 0
    ByteSink docs_sink;
    ASSERT_TRUE(docs_only.finish(&docs_sink).ok());
    PerIndexMetaReader docs_reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(docs_sink.buffer()), &docs_reader).ok());
    EXPECT_FALSE(docs_reader.has_positions());
    EXPECT_GT(docs_reader.section_refs().posting_region.length, 0U);
}

TEST(SniiPerIndexMeta, EmptySuffix) {
    auto bytes = BuildMeta(1, "", {"a"},
                           {{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}});
    PerIndexMetaReader reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader).ok());
    EXPECT_EQ(reader.index_id(), 1U);
    EXPECT_TRUE(reader.index_suffix().empty());
}

TEST(SniiPerIndexMeta, BigramPruneMinDfRoundTripsAndDefaultsToZero) {
    // G01: absent kBigramPruneInfo section (the builder default AND every legacy
    // segment) reads back as 0 == "not pruned, legacy semantics".
    auto legacy_bytes =
            BuildMeta(1, "body", {"a"},
                      {{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}});
    PerIndexMetaReader legacy;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(legacy_bytes), &legacy).ok());
    EXPECT_EQ(legacy.bigram_prune_min_df(), 0U);
    EXPECT_EQ(legacy.bigram_prune_max_df(), 0U); // absent section: BOTH gates inactive

    // A non-zero threshold emits the optional framed section and round-trips
    // exactly, without disturbing the required sub-sections.
    PerIndexMetaBuilder builder(2, "body", PerIndexMetaBuilder::kHasPositions);
    builder.set_stats(SampleStats());
    builder.set_sampled_term_index(Slice(BuildSampled({"a"})));
    builder.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    builder.set_section_refs(SampleRefs());
    builder.set_bigram_prune_min_df(640);
    ByteSink sink;
    ASSERT_TRUE(builder.finish(&sink).ok());

    PerIndexMetaReader pruned;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(sink.buffer()), &pruned).ok());
    EXPECT_EQ(pruned.bigram_prune_min_df(), 640U);
    // (min>0, max=0): a post-G15 writer with the upper gate disabled encodes an
    // explicit 0 max varint; it must decode back as "max gate inactive".
    EXPECT_EQ(pruned.bigram_prune_max_df(), 0U);
    EXPECT_TRUE(pruned.has_positions());
    ExpectStatsEq(pruned.stats(), SampleStats());
    ExpectRefsEq(pruned.section_refs(), SampleRefs());
}

TEST(SniiPerIndexMeta, BigramPruneMaxDfRoundTripsAndPreG15PayloadDefaultsToZero) {
    // G15: both thresholds round-trip through the (single) kBigramPruneInfo
    // section; a max-only declaration must still emit the section (the reader's
    // dict-miss fallback keys on EITHER gate being non-zero).
    PerIndexMetaBuilder builder(2, "body", PerIndexMetaBuilder::kHasPositions);
    builder.set_stats(SampleStats());
    builder.set_sampled_term_index(Slice(BuildSampled({"a"})));
    builder.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    builder.set_section_refs(SampleRefs());
    builder.set_bigram_prune_min_df(640);
    builder.set_bigram_prune_max_df(1800);
    ByteSink sink;
    ASSERT_TRUE(builder.finish(&sink).ok());
    PerIndexMetaReader both;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(sink.buffer()), &both).ok());
    EXPECT_EQ(both.bigram_prune_min_df(), 640U);
    EXPECT_EQ(both.bigram_prune_max_df(), 1800U);

    PerIndexMetaBuilder max_only_builder(3, "body", PerIndexMetaBuilder::kHasPositions);
    max_only_builder.set_stats(SampleStats());
    max_only_builder.set_sampled_term_index(Slice(BuildSampled({"a"})));
    max_only_builder.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    max_only_builder.set_section_refs(SampleRefs());
    max_only_builder.set_bigram_prune_max_df(1800);
    ByteSink max_only_sink;
    ASSERT_TRUE(max_only_builder.finish(&max_only_sink).ok());
    PerIndexMetaReader max_only;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(max_only_sink.buffer()), &max_only).ok());
    EXPECT_EQ(max_only.bigram_prune_min_df(), 0U);
    EXPECT_EQ(max_only.bigram_prune_max_df(), 1800U);

    // Back-compat: a pre-G15 (G01..G13) writer framed ONLY the min varint.
    // Splice such a section in verbatim via add_raw_section; decode must read
    // the min and default the absent max to 0 instead of erroring.
    PerIndexMetaBuilder legacy_builder(4, "body", PerIndexMetaBuilder::kHasPositions);
    legacy_builder.set_stats(SampleStats());
    legacy_builder.set_sampled_term_index(Slice(BuildSampled({"a"})));
    legacy_builder.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    legacy_builder.set_section_refs(SampleRefs());
    ByteSink old_payload;
    old_payload.put_varint64(640);
    ByteSink old_frame;
    SectionFramer::write(old_frame, static_cast<uint8_t>(SectionType::kBigramPruneInfo),
                         Slice(old_payload.buffer()));
    legacy_builder.add_raw_section(Slice(old_frame.buffer()));
    ByteSink legacy_sink;
    ASSERT_TRUE(legacy_builder.finish(&legacy_sink).ok());
    PerIndexMetaReader legacy;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(legacy_sink.buffer()), &legacy).ok());
    EXPECT_EQ(legacy.bigram_prune_min_df(), 640U);
    EXPECT_EQ(legacy.bigram_prune_max_df(), 0U);
}

TEST(SniiPerIndexMeta, HeaderStartsWithMetaFormatVersion) {
    auto bytes = BuildMeta(7, "x", {"a"},
                           {{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}});
    ASSERT_GE(bytes.size(), 2U);
    // u16 meta_format_version, little-endian, is the first field.
    ByteSource src {Slice(bytes)};
    uint16_t ver = 0;
    ASSERT_TRUE(src.get_fixed16(&ver).ok());
    EXPECT_EQ(ver, kMetaFormatVersion);
}

TEST(SniiPerIndexMeta, HeaderCrcCorruptionDetected) {
    auto bytes = BuildMeta(7, "title", {"a", "b"},
                           {{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}});
    ASSERT_GE(bytes.size(), 3U);
    // Flip a byte inside the header (index_id varint region after the u16 version).
    bytes[3] ^= 0xFF;
    PerIndexMetaReader reader;
    EXPECT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiPerIndexMeta, SubSectionCrcCorruptionDetected) {
    std::vector<std::string> sample_terms = {"alpha", "kappa"};
    std::vector<BlockRef> dict_refs = {
            {.offset = 4096, .length = 1024, .n_entries = 10, .flags = 0, .checksum = 0x11111111U}};
    auto bytes = BuildMeta(7, "title", sample_terms, dict_refs);
    // Corrupt a byte deep in the block (well past the header, inside a framed
    // sub-section payload). The framer CRC of that sub-section must catch it.
    ASSERT_GT(bytes.size(), 40U);
    bytes[bytes.size() - 10] ^= 0xFF;
    PerIndexMetaReader reader;
    EXPECT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiPerIndexMeta, UnknownOptionalSectionSkipped) {
    // Build a normal meta block, then splice an unknown-type framed section in
    // front of the SectionRefs by rebuilding manually: header + stats + sampled +
    // dict + unknown + section_refs. The reader must skip the unknown section and
    // still expose all the known ones.
    PerIndexMetaBuilder builder(7, "title", 0);
    builder.set_stats(SampleStats());
    builder.set_sampled_term_index(Slice(BuildSampled({"a", "b"})));
    builder.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    builder.set_section_refs(SampleRefs());

    // Inject an extra framed section with an unrecognized type id (200).
    const uint8_t junk[] = {0xDE, 0xAD, 0xBE, 0xEF};
    ByteSink extra;
    SectionFramer::write(extra, 200, Slice(junk, sizeof(junk)));
    builder.add_raw_section(extra.view());

    ByteSink sink;
    ASSERT_TRUE(builder.finish(&sink).ok());
    auto bytes = sink.buffer();

    PerIndexMetaReader reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader).ok());
    ExpectStatsEq(reader.stats(), SampleStats());
    ExpectRefsEq(reader.section_refs(), SampleRefs());
    std::vector<uint8_t> sti_scratch;
    Slice sti_frame;
    ASSERT_TRUE(reader.sampled_term_index_frame(&sti_scratch, &sti_frame).ok());
    SampledTermIndexReader sti;
    ASSERT_TRUE(SampledTermIndexReader::open(sti_frame, &sti).ok());
    EXPECT_EQ(sti.n_blocks(), 2U);
    std::vector<uint8_t> dbd_scratch;
    Slice dbd_frame;
    ASSERT_TRUE(reader.dict_block_directory_frame(&dbd_scratch, &dbd_frame).ok());
    DictBlockDirectoryReader dbd;
    ASSERT_TRUE(DictBlockDirectoryReader::open(dbd_frame, &dbd).ok());
    EXPECT_EQ(dbd.n_blocks(), 1U);
}

TEST(SniiPerIndexMeta, TruncatedHeaderRejected) {
    auto bytes = BuildMeta(7, "title", {"a"},
                           {{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}});
    // Keep only one byte: cannot even read the u16 version.
    std::vector<uint8_t> truncated(bytes.begin(), bytes.begin() + 1);
    PerIndexMetaReader reader;
    EXPECT_FALSE(PerIndexMetaReader::open(Slice(truncated), &reader).ok());
}

TEST(SniiPerIndexMeta, NullSinkRejectedByFinish) {
    PerIndexMetaBuilder builder(1, "x", 0);
    builder.set_stats(SampleStats());
    builder.set_sampled_term_index(Slice(BuildSampled({"a"})));
    builder.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    builder.set_section_refs(SampleRefs());
    EXPECT_TRUE(builder.finish(nullptr).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiPerIndexMeta, OpenNullReaderRejected) {
    auto bytes = BuildMeta(1, "x", {"a"},
                           {{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}});
    EXPECT_TRUE(PerIndexMetaReader::open(Slice(bytes), nullptr)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// ---- G13: zstd-compressed embedded sti/dbd sections ----

namespace {

// A large ascending vocabulary whose sampled-term frame exceeds the 4KB
// compression threshold. Fixed-width numeric tails keep lexicographic order.
std::vector<std::string> ManyTerms(uint32_t n) {
    std::vector<std::string> terms;
    terms.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        terms.push_back("term_" + std::to_string(1000000 + i));
    }
    return terms;
}

// A large block-ref table whose directory frame exceeds the threshold.
std::vector<BlockRef> ManyRefs(uint32_t n) {
    std::vector<BlockRef> refs;
    refs.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        refs.push_back({.offset = 4096 + static_cast<uint64_t>(i) * 128,
                        .length = 128,
                        .n_entries = 3,
                        .flags = 0,
                        .checksum = i});
    }
    return refs;
}

// Walks a per-index meta block and returns the framed section type ids in
// emission order (header fields are consumed structurally, not verified here).
std::vector<uint8_t> SectionTypesOf(Slice block) {
    ByteSource src(block);
    uint16_t ver = 0;
    EXPECT_TRUE(src.get_fixed16(&ver).ok());
    uint64_t index_id = 0;
    EXPECT_TRUE(src.get_varint64(&index_id).ok());
    uint32_t suffix_len = 0;
    EXPECT_TRUE(src.get_varint32(&suffix_len).ok());
    Slice suffix;
    EXPECT_TRUE(src.get_bytes(suffix_len, &suffix).ok());
    uint32_t flags = 0;
    EXPECT_TRUE(src.get_fixed32(&flags).ok());
    uint32_t crc = 0;
    EXPECT_TRUE(src.get_fixed32(&crc).ok());
    std::vector<uint8_t> types;
    while (!src.eof()) {
        FramedSection sec;
        EXPECT_TRUE(SectionFramer::read(src, &sec).ok());
        types.push_back(sec.type);
    }
    return types;
}

bool ContainsType(const std::vector<uint8_t>& types, SectionType t) {
    return std::find(types.begin(), types.end(), static_cast<uint8_t>(t)) != types.end();
}

// Builds a meta block whose sti/dbd sections are corrupt zstd carriers: the
// carrier frame CRC is VALID (SectionFramer computes it over the given payload),
// so only the zstd layer can catch the damage. `payload` = the carrier frame
// payload (varint64 uncomp_len + compressed bytes).
std::vector<uint8_t> BuildMetaWithRawStiSection(Slice sti_carrier_payload) {
    PerIndexMetaBuilder builder(1, "x", 0);
    builder.set_stats(SampleStats());
    builder.set_dict_block_directory(Slice(
            BuildDict({{.offset = 0, .length = 1, .n_entries = 1, .flags = 0, .checksum = 0}})));
    builder.set_section_refs(SampleRefs());
    ByteSink carrier;
    SectionFramer::write(carrier, static_cast<uint8_t>(SectionType::kSampledTermIndexZstd),
                         sti_carrier_payload);
    builder.add_raw_section(carrier.view());
    ByteSink sink;
    EXPECT_TRUE(builder.finish(&sink).ok());
    return sink.buffer();
}

} // namespace

// Large sti/dbd frames are emitted as kSampledTermIndexZstd/kDictBlockDirectoryZstd
// carriers, the block shrinks vs the uncompressed control, and the materialized
// frames are BYTE-EXACT copies of the original framed sub-sections.
TEST(SniiPerIndexMetaZstd, LargeSectionsCompressedAndByteRoundTrip) {
    const auto terms = ManyTerms(2000);
    const auto refs = ManyRefs(2000);
    const auto sti_raw = BuildSampled(terms);
    const auto dbd_raw = BuildDict(refs);
    ASSERT_GT(sti_raw.size(), kMetaSectionCompressMinBytes);
    ASSERT_GT(dbd_raw.size(), kMetaSectionCompressMinBytes);

    const auto compressed = BuildMeta(7, "title", terms, refs);
    std::vector<uint8_t> control;
    {
        ScopedEnv off("SNII_META_COMPRESS_MIN", kCompressOff);
        control = BuildMeta(7, "title", terms, refs);
    }
    // The compressed layout must be a real shrink over the raw layout.
    EXPECT_LT(compressed.size(), control.size());
    std::cout << "[G13] per-index meta block: raw=" << control.size()
              << "B zstd=" << compressed.size() << "B (sti_frame=" << sti_raw.size()
              << "B dbd_frame=" << dbd_raw.size() << "B)\n";

    const auto compressed_types = SectionTypesOf(Slice(compressed));
    EXPECT_TRUE(ContainsType(compressed_types, SectionType::kSampledTermIndexZstd));
    EXPECT_TRUE(ContainsType(compressed_types, SectionType::kDictBlockDirectoryZstd));
    EXPECT_FALSE(ContainsType(compressed_types, SectionType::kSampledTermIndex));
    EXPECT_FALSE(ContainsType(compressed_types, SectionType::kDictBlockDirectory));
    const auto control_types = SectionTypesOf(Slice(control));
    EXPECT_TRUE(ContainsType(control_types, SectionType::kSampledTermIndex));
    EXPECT_FALSE(ContainsType(control_types, SectionType::kSampledTermIndexZstd));

    PerIndexMetaReader reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(compressed), &reader).ok());
    std::vector<uint8_t> sti_scratch;
    Slice sti_frame;
    ASSERT_TRUE(reader.sampled_term_index_frame(&sti_scratch, &sti_frame).ok());
    ASSERT_EQ(sti_frame.size(), sti_raw.size());
    EXPECT_EQ(std::memcmp(sti_frame.data(), sti_raw.data(), sti_raw.size()), 0);
    std::vector<uint8_t> dbd_scratch;
    Slice dbd_frame;
    ASSERT_TRUE(reader.dict_block_directory_frame(&dbd_scratch, &dbd_frame).ok());
    ASSERT_EQ(dbd_frame.size(), dbd_raw.size());
    EXPECT_EQ(std::memcmp(dbd_frame.data(), dbd_raw.data(), dbd_raw.size()), 0);

    // The materialized frames parse with the unchanged sub-module readers.
    SampledTermIndexReader sti;
    ASSERT_TRUE(SampledTermIndexReader::open(sti_frame, &sti).ok());
    EXPECT_EQ(sti.n_blocks(), 2000U);
    bool maybe = false;
    uint32_t ord = 0;
    ASSERT_TRUE(sti.locate("term_1001500", &maybe, &ord).ok());
    EXPECT_TRUE(maybe);
    EXPECT_EQ(ord, 1500U);
    DictBlockDirectoryReader dbd;
    ASSERT_TRUE(DictBlockDirectoryReader::open(dbd_frame, &dbd).ok());
    ASSERT_EQ(dbd.n_blocks(), 2000U);
    BlockRef got {};
    ASSERT_TRUE(dbd.get(1500, &got).ok());
    EXPECT_EQ(got.offset, 4096U + 1500U * 128U);
    EXPECT_EQ(got.checksum, 1500U);
}

// Old-segment compatibility: a large meta built with compression disabled (the
// pre-G13 layout) still opens and materializes as in-place raw frames.
TEST(SniiPerIndexMetaZstd, UncompressedLargeSectionsStillRead) {
    const auto terms = ManyTerms(2000);
    const auto refs = ManyRefs(2000);
    std::vector<uint8_t> legacy;
    {
        ScopedEnv off("SNII_META_COMPRESS_MIN", kCompressOff);
        legacy = BuildMeta(7, "title", terms, refs);
    }
    PerIndexMetaReader reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(legacy), &reader).ok());
    std::vector<uint8_t> sti_scratch;
    Slice sti_frame;
    ASSERT_TRUE(reader.sampled_term_index_frame(&sti_scratch, &sti_frame).ok());
    // Raw frame: a view into the block, no decompression scratch used.
    EXPECT_TRUE(sti_scratch.empty());
    EXPECT_GE(sti_frame.data(), legacy.data());
    EXPECT_LE(sti_frame.data() + sti_frame.size(), legacy.data() + legacy.size());
    SampledTermIndexReader sti;
    ASSERT_TRUE(SampledTermIndexReader::open(sti_frame, &sti).ok());
    EXPECT_EQ(sti.n_blocks(), 2000U);
    std::vector<uint8_t> dbd_scratch;
    Slice dbd_frame;
    ASSERT_TRUE(reader.dict_block_directory_frame(&dbd_scratch, &dbd_frame).ok());
    EXPECT_TRUE(dbd_scratch.empty());
    DictBlockDirectoryReader dbd;
    ASSERT_TRUE(DictBlockDirectoryReader::open(dbd_frame, &dbd).ok());
    EXPECT_EQ(dbd.n_blocks(), 2000U);
}

// Below the threshold nothing changes: the default build is byte-identical to a
// compression-disabled build (small/legacy segments keep the exact old layout).
TEST(SniiPerIndexMetaZstd, SmallSectionsStayRawByteIdentical) {
    const std::vector<std::string> terms = {"alpha", "kappa", "zeta"};
    const std::vector<BlockRef> refs = {
            {.offset = 4096, .length = 1024, .n_entries = 10, .flags = 0, .checksum = 0x11U}};
    const auto def = BuildMeta(7, "title", terms, refs);
    std::vector<uint8_t> off_bytes;
    {
        ScopedEnv off("SNII_META_COMPRESS_MIN", kCompressOff);
        off_bytes = BuildMeta(7, "title", terms, refs);
    }
    EXPECT_EQ(def, off_bytes);
    const auto types = SectionTypesOf(Slice(def));
    EXPECT_TRUE(ContainsType(types, SectionType::kSampledTermIndex));
    EXPECT_TRUE(ContainsType(types, SectionType::kDictBlockDirectory));
    EXPECT_FALSE(ContainsType(types, SectionType::kSampledTermIndexZstd));
    EXPECT_FALSE(ContainsType(types, SectionType::kDictBlockDirectoryZstd));
}

// The threshold gate itself: sections between the zstd win size and the 4KB
// default stay raw by default but compress when the env lowers the threshold,
// and the forced layout still round-trips byte-exactly.
TEST(SniiPerIndexMetaZstd, ThresholdGateControlsCompression) {
    const auto terms = ManyTerms(300); // ~2KB sti: below the 4KB default, above zstd win size
    const auto refs = ManyRefs(300);
    const auto sti_raw = BuildSampled(terms);
    ASSERT_LT(sti_raw.size(), kMetaSectionCompressMinBytes);

    const auto def = BuildMeta(7, "title", terms, refs);
    EXPECT_TRUE(ContainsType(SectionTypesOf(Slice(def)), SectionType::kSampledTermIndex));

    std::vector<uint8_t> forced;
    {
        ScopedEnv force("SNII_META_COMPRESS_MIN", "1");
        forced = BuildMeta(7, "title", terms, refs);
    }
    const auto forced_types = SectionTypesOf(Slice(forced));
    EXPECT_TRUE(ContainsType(forced_types, SectionType::kSampledTermIndexZstd));
    EXPECT_TRUE(ContainsType(forced_types, SectionType::kDictBlockDirectoryZstd));
    EXPECT_LT(forced.size(), def.size());

    PerIndexMetaReader reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(forced), &reader).ok());
    std::vector<uint8_t> scratch;
    Slice frame;
    ASSERT_TRUE(reader.sampled_term_index_frame(&scratch, &frame).ok());
    ASSERT_EQ(frame.size(), sti_raw.size());
    EXPECT_EQ(std::memcmp(frame.data(), sti_raw.data(), sti_raw.size()), 0);
}

// Garbage compressed bytes under a VALID carrier-frame crc must fail with
// kCorruption at materialization (the zstd layer catches what the crc cannot).
TEST(SniiPerIndexMetaZstd, CorruptZstdPayloadRejected) {
    ByteSink payload;
    payload.put_varint64(1024); // plausible uncomp_len
    std::vector<uint8_t> garbage(64, 0xAB);
    payload.put_bytes(Slice(garbage));
    const auto bytes = BuildMetaWithRawStiSection(payload.view());

    PerIndexMetaReader reader;
    ASSERT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader).ok());
    std::vector<uint8_t> scratch;
    Slice frame;
    EXPECT_TRUE(reader.sampled_term_index_frame(&scratch, &frame)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// Truncated compressed bytes and a wrong declared uncomp_len are both detected.
TEST(SniiPerIndexMetaZstd, TruncatedOrMisdeclaredZstdRejected) {
    const auto sti_raw = BuildSampled(ManyTerms(500));
    std::vector<uint8_t> comp;
    ASSERT_TRUE(zstd_compress(Slice(sti_raw), 3, &comp).ok());

    {
        // Half the compressed stream, full declared length.
        ByteSink payload;
        payload.put_varint64(sti_raw.size());
        payload.put_bytes(Slice(comp.data(), comp.size() / 2));
        const auto bytes = BuildMetaWithRawStiSection(payload.view());
        PerIndexMetaReader reader;
        ASSERT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader).ok());
        std::vector<uint8_t> scratch;
        Slice frame;
        EXPECT_TRUE(reader.sampled_term_index_frame(&scratch, &frame)
                            .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    }
    {
        // Full compressed stream, under-declared length (decompress overflows dst).
        ByteSink payload;
        payload.put_varint64(sti_raw.size() - 1);
        payload.put_bytes(Slice(comp));
        const auto bytes = BuildMetaWithRawStiSection(payload.view());
        PerIndexMetaReader reader;
        ASSERT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader).ok());
        std::vector<uint8_t> scratch;
        Slice frame;
        EXPECT_TRUE(reader.sampled_term_index_frame(&scratch, &frame)
                            .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    }
}

// A declared uncomp_len outside (0, 256MB] is rejected before any allocation.
TEST(SniiPerIndexMetaZstd, ZstdUncompLenOutOfRangeRejected) {
    for (const uint64_t bad_len : {uint64_t {0}, uint64_t {256ULL * 1024 * 1024 + 1}}) {
        ByteSink payload;
        payload.put_varint64(bad_len);
        std::vector<uint8_t> some(16, 0x01);
        payload.put_bytes(Slice(some));
        const auto bytes = BuildMetaWithRawStiSection(payload.view());
        PerIndexMetaReader reader;
        ASSERT_TRUE(PerIndexMetaReader::open(Slice(bytes), &reader).ok());
        std::vector<uint8_t> scratch;
        Slice frame;
        EXPECT_TRUE(reader.sampled_term_index_frame(&scratch, &frame)
                            .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
                << "uncomp_len=" << bad_len;
    }
}
