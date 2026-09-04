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

#include "storage/index/snii/query/query_profile.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <functional>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include "common/status.h"
#include "runtime/runtime_profile.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/internal/phrase_query_split.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/phrase_prx_validation.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/phrase_verify_timer.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/regexp_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/query/wildcard_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_prx_profile.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "util/defer_op.h"

using namespace doris::snii;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
using doris::Status;

namespace {

template <typename T>
concept HasSniiPrxEncodedBytes = requires(T value) { value.snii_prx_encoded_bytes; };
template <typename T>
concept HasSniiPrxPayloadBytes = requires(T value) { value.snii_prx_payload_bytes; };
template <typename T>
concept HasSniiPrxCompressedBytes = requires(T value) { value.snii_prx_compressed_bytes; };
template <typename T>
concept HasSniiPrxChild128Touches = requires(T value) { value.snii_prx_child_128_touches; };
template <typename T>
concept HasSniiPrxChild256Touches = requires(T value) { value.snii_prx_child_256_touches; };
template <typename T>
concept HasSniiPrxCrcValidationNs = requires(T value) { value.snii_prx_crc_validation_ns; };
template <typename T>
concept HasSniiPrxDecompressNs = requires(T value) { value.snii_prx_decompress_ns; };
template <typename T>
concept HasSniiPrxScratchAllocationEvents =
        requires(T value) { value.snii_prx_scratch_allocation_events; };
template <typename T>
concept HasSniiPrxContainerAllocationEvents =
        requires(T value) { value.snii_prx_container_allocation_events; };

static_assert(!HasSniiPrxEncodedBytes<doris::OlapReaderStatistics>);
static_assert(!HasSniiPrxPayloadBytes<doris::OlapReaderStatistics>);
static_assert(!HasSniiPrxCompressedBytes<doris::OlapReaderStatistics>);
static_assert(!HasSniiPrxChild128Touches<doris::OlapReaderStatistics>);
static_assert(!HasSniiPrxChild256Touches<doris::OlapReaderStatistics>);
static_assert(!HasSniiPrxCrcValidationNs<doris::OlapReaderStatistics>);
static_assert(!HasSniiPrxDecompressNs<doris::OlapReaderStatistics>);
static_assert(!HasSniiPrxScratchAllocationEvents<doris::OlapReaderStatistics>);
static_assert(!HasSniiPrxContainerAllocationEvents<doris::OlapReaderStatistics>);

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_query_profile_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

class MutableMemoryReader final : public io::FileReader {
public:
    explicit MutableMemoryReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset > bytes_.size() || len > bytes_.size() - offset) {
            return Status::Corruption<false>("mutable memory reader read past eof");
        }
        out->assign(bytes_.begin() + static_cast<std::ptrdiff_t>(offset),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(offset + len));
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }
    std::vector<uint8_t>& bytes() { return bytes_; }

private:
    std::vector<uint8_t> bytes_;
};

struct Corpus {
    std::vector<std::vector<std::string>> docs;
};

Corpus BuildCorpus() {
    Corpus c;
    c.docs.resize(128);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        std::vector<std::string>& doc = c.docs[d];
        doc.emplace_back("lead");
        doc.emplace_back("quick");
        doc.emplace_back(d % 2 == 0 ? "brown" : "bronze");
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d);
        doc.emplace_back(term);
    }
    return c;
}

Corpus BuildHighTfCorpus() {
    constexpr uint32_t kTailCount = 33;
    constexpr uint32_t kRepetitions = 48;
    Corpus corpus;
    corpus.docs.resize(600);
    for (uint32_t docid = 0; docid < corpus.docs.size(); ++docid) {
        std::vector<std::string>& doc = corpus.docs[docid];
        doc.reserve(kRepetitions * 5);
        char tail[32];
        std::snprintf(tail, sizeof(tail), "epsilon_%02u", docid % kTailCount);
        for (uint32_t repetition = 0; repetition < kRepetitions; ++repetition) {
            doc.insert(doc.end(), {"alpha", "beta", "gamma", "delta", tail});
        }
    }
    return corpus;
}

Corpus BuildEarlyHitCorpus(uint32_t doc_count) {
    constexpr uint32_t kPositionsPerTerm = 48;
    Corpus corpus;
    corpus.docs.resize(doc_count);
    for (auto& doc : corpus.docs) {
        doc.reserve(kPositionsPerTerm * 2);
        for (uint32_t position = 0; position < kPositionsPerTerm; ++position) {
            doc.insert(doc.end(), {"a", "b"});
        }
    }
    return corpus;
}

Corpus BuildSingleTailGroupCorpus() {
    Corpus corpus;
    corpus.docs.resize(96);
    for (uint32_t docid = 0; docid < corpus.docs.size(); ++docid) {
        char tail[32];
        std::snprintf(tail, sizeof(tail), "epsilon_%02u", docid % 3);
        corpus.docs[docid] = {"alpha", "beta", tail};
    }
    return corpus;
}

Corpus BuildDisjointTailGroupCorpus() {
    Corpus corpus;
    corpus.docs.resize(99);
    for (uint32_t docid = 0; docid < 96; ++docid) {
        corpus.docs[docid] = {"alpha", "beta", "other"};
    }
    for (uint32_t tail = 0; tail < 3; ++tail) {
        char term[32];
        std::snprintf(term, sizeof(term), "epsilon_%02u", tail);
        corpus.docs[96 + tail] = {term};
    }
    return corpus;
}

void WriteCorpus(const Corpus& c, const std::string& path, int prx_zstd_level = 3,
                 bool write_freq = true) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        const std::vector<std::string>& terms = c.docs[d];
        for (uint32_t pos = 0; pos < terms.size(); ++pos) {
            buf.add_token(terms[pos], d, pos);
        }
    }

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = write_freq ? doris::snii::format::IndexConfig::kDocsPositionsScoring
                           : doris::snii::format::IndexConfig::kDocsPositions;
    in.doc_count = static_cast<uint32_t>(c.docs.size());
    if (write_freq) {
        in.encoded_norms.assign(c.docs.size(), 1);
        doris::segment_v2::inverted_index::CommonGramsSegmentMetadata metadata;
        metadata.plain_term_key_version =
                doris::segment_v2::inverted_index::PlainTermKeyVersion::kRawNoInternal;
        metadata.scoring_coverage = doris::segment_v2::inverted_index::ScoringCoverage::kComplete;
        metadata.scoring_stats_version =
                doris::segment_v2::inverted_index::COMMON_GRAMS_SCORING_STATS_VERSION_V1;
        metadata.norm_semantics_version =
                doris::segment_v2::inverted_index::COMMON_GRAMS_NORM_SEMANTICS_VERSION_V1;
        metadata.base_analyzer_fingerprint = "query-profile-test";
        metadata.scoring_doc_count = c.docs.size();
        for (const auto& terms : c.docs) {
            metadata.scoring_token_count += terms.size();
        }
        in.common_grams_metadata = std::move(metadata);
    }
    in.terms = buf.finalize_sorted();
    in.target_dict_block_bytes = 512;
    in.prx_zstd_level = prx_zstd_level;
    in.write_freq = write_freq;

    io::LocalFileWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    ASSERT_TRUE(compound.add_logical_index(in).ok());
    ASSERT_TRUE(compound.finish().ok());
}

std::vector<uint8_t> ReadFile(const std::string& path) {
    io::LocalFileReader file;
    EXPECT_TRUE(file.open(path).ok());
    std::vector<uint8_t> bytes;
    EXPECT_TRUE(file.read_at(0, file.size(), &bytes).ok());
    return bytes;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): gtest assertions inflate the score.
void CorruptLastRawDocumentWithTrailingPosition(MutableMemoryReader* file,
                                                const LogicalIndexReader& index,
                                                std::string_view term) {
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    ASSERT_TRUE(index.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    ASSERT_TRUE(found);
    ASSERT_EQ(entry.kind, format::DictEntryKind::kPodRef);

    uint64_t frame_offset = 0;
    uint64_t frame_length = 0;
    ASSERT_TRUE(index.resolve_prx_window(entry, prx_base, &frame_offset, &frame_length).ok());
    ASSERT_LE(frame_offset, file->bytes().size());
    ASSERT_LE(frame_length, file->bytes().size() - frame_offset);
    Slice frames(file->bytes().data() + frame_offset, static_cast<size_t>(frame_length));
    ByteSource frame_source(frames);
    size_t last_frame_offset = 0;
    size_t last_payload_offset = 0;
    uint32_t last_payload_length = 0;
    while (!frame_source.eof()) {
        last_frame_offset = frame_source.position();
        uint8_t codec = 0;
        ASSERT_TRUE(frame_source.get_u8(&codec).ok());
        ASSERT_EQ(codec, static_cast<uint8_t>(format::PrxCodec::kRaw));
        ASSERT_TRUE(frame_source.get_varint32(&last_payload_length).ok());
        last_payload_offset = frame_source.position();
        Slice payload_bytes;
        ASSERT_TRUE(frame_source.get_bytes(last_payload_length, &payload_bytes).ok());
        uint32_t checksum = 0;
        ASSERT_TRUE(frame_source.get_fixed32(&checksum).ok());
    }
    const size_t last_frame_length = frame_source.position() - last_frame_offset;

    ByteSource payload(frames.subslice(last_payload_offset, last_payload_length));
    uint32_t doc_count = 0;
    ASSERT_TRUE(payload.get_varint32(&doc_count).ok());
    ASSERT_GT(doc_count, 1U);
    size_t last_count_offset = 0;
    uint32_t last_count = 0;
    for (uint32_t doc = 0; doc < doc_count; ++doc) {
        const size_t count_offset = payload.position();
        uint32_t count = 0;
        ASSERT_TRUE(payload.get_varint32(&count).ok());
        for (uint32_t position = 0; position < count; ++position) {
            uint32_t delta = 0;
            ASSERT_TRUE(payload.get_varint32(&delta).ok());
        }
        if (doc + 1 == doc_count) {
            last_count_offset = count_offset;
            last_count = count;
        }
    }
    ASSERT_TRUE(payload.eof());
    ASSERT_EQ(last_count, 48U);
    uint8_t& encoded_count = file->bytes()[frame_offset + last_payload_offset + last_count_offset];
    ASSERT_EQ(encoded_count, 48U);
    encoded_count = 47U;

    const uint32_t checksum =
            doris::snii::crc32c(Slice(file->bytes().data() + frame_offset + last_frame_offset,
                                      last_frame_length - sizeof(uint32_t)));
    for (size_t byte = 0; byte < sizeof(checksum); ++byte) {
        file->bytes()[frame_offset + last_frame_offset + last_frame_length - sizeof(checksum) +
                      byte] = static_cast<uint8_t>(checksum >> (8 * byte));
    }
}

void LookupPodRefEntry(const LogicalIndexReader& index, std::string_view term,
                       format::DictEntry* entry, uint64_t* prx_base) {
    bool found = false;
    uint64_t frq_base = 0;
    ASSERT_TRUE(index.lookup(term, &found, entry, &frq_base, prx_base).ok());
    ASSERT_TRUE(found);
    ASSERT_EQ(entry->kind, format::DictEntryKind::kPodRef);
}

void ResolvePrxFrames(MutableMemoryReader* file, const LogicalIndexReader& index,
                      const format::DictEntry& entry, uint64_t prx_base, uint64_t* frame_offset,
                      uint64_t* frame_length) {
    ASSERT_TRUE(index.resolve_prx_window(entry, prx_base, frame_offset, frame_length).ok());
    ASSERT_LE(*frame_offset, file->bytes().size());
    ASSERT_LE(*frame_length, file->bytes().size() - *frame_offset);
}

struct RawFrameLocation {
    size_t payload_offset = 0;
    Slice payload;
    size_t frame_length = 0;
};

void ParseFirstRawFrame(Slice frames, RawFrameLocation* location) {
    ByteSource frame_source(frames);
    uint8_t codec = 0;
    ASSERT_TRUE(frame_source.get_u8(&codec).ok());
    ASSERT_EQ(codec, static_cast<uint8_t>(format::PrxCodec::kRaw));
    uint32_t payload_length = 0;
    ASSERT_TRUE(frame_source.get_varint32(&payload_length).ok());
    location->payload_offset = frame_source.position();
    ASSERT_TRUE(frame_source.get_bytes(payload_length, &location->payload).ok());
    uint32_t stored_checksum = 0;
    ASSERT_TRUE(frame_source.get_fixed32(&stored_checksum).ok());
    location->frame_length = frame_source.position();
}

void RewriteRawFrameDocCount(MutableMemoryReader* file, uint64_t frame_offset,
                             const RawFrameLocation& location, int32_t delta) {
    ByteSource payload(location.payload);
    uint32_t doc_count = 0;
    ASSERT_TRUE(payload.get_varint32(&doc_count).ok());
    const size_t encoded_doc_count_length = payload.position();
    const int64_t changed_doc_count = static_cast<int64_t>(doc_count) + delta;
    ASSERT_GT(changed_doc_count, 0);
    ASSERT_LE(changed_doc_count, std::numeric_limits<uint32_t>::max());
    ByteSink encoded_doc_count;
    encoded_doc_count.put_varint32(static_cast<uint32_t>(changed_doc_count));
    ASSERT_EQ(encoded_doc_count.size(), encoded_doc_count_length);
    for (size_t byte = 0; byte < encoded_doc_count.size(); ++byte) {
        file->bytes()[frame_offset + location.payload_offset + byte] =
                encoded_doc_count.buffer()[byte];
    }

    const uint32_t checksum = doris::snii::crc32c(
            Slice(file->bytes().data() + frame_offset, location.frame_length - sizeof(uint32_t)));
    for (size_t byte = 0; byte < sizeof(checksum); ++byte) {
        file->bytes()[frame_offset + location.frame_length - sizeof(checksum) + byte] =
                static_cast<uint8_t>(checksum >> (8 * byte));
    }
}

void CorruptFirstRawFrameDocCount(MutableMemoryReader* file, const LogicalIndexReader& index,
                                  std::string_view term, int32_t delta) {
    format::DictEntry entry;
    uint64_t prx_base = 0;
    LookupPodRefEntry(index, term, &entry, &prx_base);
    uint64_t frame_offset = 0;
    uint64_t frame_length = 0;
    ResolvePrxFrames(file, index, entry, prx_base, &frame_offset, &frame_length);
    RawFrameLocation location;
    ParseFirstRawFrame(
            Slice(file->bytes().data() + frame_offset, static_cast<size_t>(frame_length)),
            &location);
    RewriteRawFrameDocCount(file, frame_offset, location, delta);
}

void ExpectStreamingDocCountMismatchIsAtomic(int32_t delta) {
    const Corpus corpus = BuildEarlyHitCorpus(/*doc_count=*/600);
    const std::string path = TempPath();
    WriteCorpus(corpus, path, /*prx_zstd_level=*/0);
    MutableMemoryReader file(ReadFile(path));
    std::remove(path.c_str());

    SniiSegmentReader segment;
    LogicalIndexReader index;
    ASSERT_TRUE(SniiSegmentReader::open(&file, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &index).ok());
    CorruptFirstRawFrameDocCount(&file, index, "b", delta);

    std::vector<uint32_t> docs = {99};
    query::QueryProfile profile;
    profile.prx_decode_stats.raw_frames = 99;
    const Status status = query::phrase_query(index, {"a", "b"}, &docs, &profile);

    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_TRUE(docs.empty());
    EXPECT_EQ(profile.prx_decode_stats.raw_frames, 0U);
    EXPECT_EQ(profile.phrase_query_stats.prx_streaming_frames, 0U);
}

LogicalIndexReader OpenMeteredIndex(io::MeteredFileReader* file, SniiSegmentReader* segment) {
    EXPECT_TRUE(SniiSegmentReader::open(file, segment).ok());
    LogicalIndexReader idx;
    EXPECT_TRUE(segment->open_index(1, "body", &idx).ok());
    return idx;
}

std::vector<query::internal::TermPlan> BuildStreamingRoutePlans(
        const std::array<uint64_t, 2>& average_tfs) {
    constexpr uint32_t kDf = 100;
    std::vector<query::internal::TermPlan> plans(2);
    for (size_t i = 0; i < plans.size(); ++i) {
        plans[i].entry.term = std::string(1, static_cast<char>('a' + i));
        plans[i].entry.df = kDf;
        plans[i].entry.ttf_delta = average_tfs[i] * kDf;
        plans[i].entry.term_stats_present = true;
        plans[i].df = kDf;
    }
    return plans;
}

bool ShouldUseStreamingExactPhrase(const std::vector<query::internal::TermPlan>& plans,
                                   std::span<const size_t> phrase_plan_index,
                                   size_t candidate_count, bool needs_frequency,
                                   const query::PhraseQueryOptions& options,
                                   query::internal::ExactPhrasePositionAccess position_access) {
    std::vector<query::phrase_impl::PosSource> sources(plans.size());
    for (size_t plan_index = 0; plan_index < plans.size(); ++plan_index) {
        const format::DictEntry& entry = plans[plan_index].entry;
        sources[plan_index].logical_position_work = entry.kind == format::DictEntryKind::kInline
                                                            ? entry.prx_bytes.size()
                                                            : entry.prx_len;
        sources[plan_index].logical_position_docs = plans[plan_index].df;
    }
    return query::phrase_impl::should_use_streaming_exact_phrase(plans, sources, phrase_plan_index,
                                                                 candidate_count, needs_frequency,
                                                                 options, position_access);
}

void ExpectProfileMatchesMeteredDelta(io::MeteredFileReader* metered,
                                      const std::function<Status(query::QueryProfile*)>& run) {
    metered->reset_metrics();
    query::QueryProfile profile;
    const Status st = run(&profile);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_GT(profile.elapsed_ns, 0U);
    ASSERT_TRUE(profile.has_io_metrics);
    EXPECT_EQ(profile.io_delta.read_at_calls, metered->metrics().read_at_calls);
    EXPECT_EQ(profile.io_delta.serial_rounds, metered->metrics().serial_rounds);
    EXPECT_EQ(profile.io_delta.range_gets, metered->metrics().range_gets);
    EXPECT_EQ(profile.io_delta.remote_bytes, metered->metrics().remote_bytes);
    EXPECT_EQ(profile.io_delta.total_request_bytes, metered->metrics().total_request_bytes);
}

template <typename... Args>
concept CanCallPhraseQuery = requires(Args... args) { query::phrase_query(args...); };

template <typename... Args>
concept CanCallPhrasePrefixQuery = requires(Args... args) { query::phrase_prefix_query(args...); };

} // namespace

TEST(SniiPhraseStreamingRouteTest, RejectsSumAverageTfAtBoundaryWhenEveryTermIsBelowMaximum) {
    const auto plans = BuildStreamingRoutePlans({4, 4});
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/64,
                                               /*needs_frequency=*/false, {},
                                               query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, SelectsMaximumAverageTfAndEstimateBoundaries) {
    const auto plans = BuildStreamingRoutePlans({8, 8});
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_TRUE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/32,
                                              /*needs_frequency=*/false, {},
                                              query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, RejectsMaximumAverageTfBelowBoundary) {
    const auto plans = BuildStreamingRoutePlans({7, 7});
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/64,
                                               /*needs_frequency=*/false, {},
                                               query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, RejectsEstimatedPositionsBelowBoundary) {
    const auto plans = BuildStreamingRoutePlans({8, 65});
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/7,
                                               /*needs_frequency=*/false, {},
                                               query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, RejectsSloppyPhrase) {
    const auto plans = BuildStreamingRoutePlans({8, 8});
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/64,
                                               /*needs_frequency=*/false, {.slop = 1},
                                               query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, RejectsFrequencyCollection) {
    const auto plans = BuildStreamingRoutePlans({8, 8});
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/64,
                                               /*needs_frequency=*/true, {},
                                               query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, RejectsRepeatedPhysicalPlanIndex) {
    const auto plans = BuildStreamingRoutePlans({8, 8});
    const std::vector<size_t> phrase_plan_index = {0, 0};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/64,
                                               /*needs_frequency=*/false, {},
                                               query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, SelectsPodRefWithoutStatsAtWorkBoundaries) {
    auto plans = BuildStreamingRoutePlans({8, 8});
    plans[0].entry.term_stats_present = false;
    plans[0].entry.prx_len = 800;
    plans[1].entry.term_stats_present = false;
    plans[1].entry.prx_len = 800;
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_TRUE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/32,
                                              /*needs_frequency=*/false, {},
                                              query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, UsesRetainedPhysicalDocsForNoStatsWindowWork) {
    auto plans = BuildStreamingRoutePlans({8, 8});
    std::vector<query::phrase_impl::PosSource> sources(plans.size());
    for (size_t plan_index = 0; plan_index < plans.size(); ++plan_index) {
        plans[plan_index].entry.term_stats_present = false;
        plans[plan_index].df = 1000;
        sources[plan_index].logical_position_work = 800;
        sources[plan_index].logical_position_docs = 100;
    }
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_TRUE(query::phrase_impl::should_use_streaming_exact_phrase(
            plans, sources, phrase_plan_index, /*candidate_count=*/32,
            /*needs_frequency=*/false, {}, query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, RejectsPodRefWithoutStatsBelowMaximumWorkBoundary) {
    auto plans = BuildStreamingRoutePlans({8, 8});
    plans[0].entry.term_stats_present = false;
    plans[0].entry.prx_len = 700;
    plans[1].entry.term_stats_present = false;
    plans[1].entry.prx_len = 700;
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/64,
                                               /*needs_frequency=*/false, {},
                                               query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, RejectsPodRefWithoutStatsBelowEstimatedWorkBoundary) {
    auto plans = BuildStreamingRoutePlans({8, 8});
    plans[0].entry.term_stats_present = false;
    plans[0].entry.prx_len = 800;
    plans[1].entry.term_stats_present = false;
    plans[1].entry.prx_len = 6500;
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/7,
                                               /*needs_frequency=*/false, {},
                                               query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, SelectsInlineWithoutStatsAtWorkBoundaries) {
    auto plans = BuildStreamingRoutePlans({8, 8});
    for (auto& plan : plans) {
        plan.entry.kind = format::DictEntryKind::kInline;
        plan.entry.term_stats_present = false;
        plan.entry.prx_bytes.resize(800);
    }
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_TRUE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/32,
                                              /*needs_frequency=*/false, {},
                                              query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, SelectsMixedStatsAndNoStatsAtWorkBoundaries) {
    auto plans = BuildStreamingRoutePlans({8, 8});
    plans[1].entry.term_stats_present = false;
    plans[1].entry.prx_len = 800;
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_TRUE(ShouldUseStreamingExactPhrase(plans, phrase_plan_index, /*candidate_count=*/32,
                                              /*needs_frequency=*/false, {},
                                              query::internal::ExactPhrasePositionAccess::kAuto));
}

TEST(SniiPhraseStreamingRouteTest, RejectsMaterializedOnlyAccess) {
    const auto plans = BuildStreamingRoutePlans({8, 8});
    const std::vector<size_t> phrase_plan_index = {0, 1};

    EXPECT_FALSE(ShouldUseStreamingExactPhrase(
            plans, phrase_plan_index, /*candidate_count=*/64,
            /*needs_frequency=*/false, {},
            query::internal::ExactPhrasePositionAccess::kMaterializedOnly));
}

TEST(SniiQueryProfileTest, PhraseProfileInterfacesDoNotExposeTraceSinks) {
    using PhraseProfileFunction =
            Status (*)(const LogicalIndexReader&, const std::vector<std::string>&,
                       std::vector<uint32_t>*, query::QueryProfile*);
    using PhrasePrefixProfileFunction =
            Status (*)(const LogicalIndexReader&, const std::vector<std::string>&,
                       std::vector<uint32_t>*, query::QueryProfile*, int32_t);

    EXPECT_NE(static_cast<PhraseProfileFunction>(&query::phrase_query), nullptr);
    EXPECT_NE(static_cast<PhrasePrefixProfileFunction>(&query::phrase_prefix_query), nullptr);
    static_assert(
            !CanCallPhraseQuery<const LogicalIndexReader&, const std::vector<std::string>&,
                                std::vector<uint32_t>*, query::QueryProfile*, std::nullptr_t>);
    static_assert(!CanCallPhrasePrefixQuery<const LogicalIndexReader&,
                                            const std::vector<std::string>&, std::vector<uint32_t>*,
                                            query::QueryProfile*, int32_t, std::nullptr_t>);
}

TEST(SniiQueryProfileTest, SuccessfulDecodeKeepsStatsWhenCallerShapeValidationFails) {
    const std::vector<std::vector<uint32_t>> positions = {{1}, {2, 4}, {3}};
    ByteSink sink;
    ASSERT_TRUE(format::build_prx_window(positions, /*zstd_level=*/0, &sink).ok());

    const std::vector<uint32_t> selected_ordinals = {0, 2};
    std::vector<uint32_t> flat;
    std::vector<uint32_t> offsets;
    format::PrxDecodeStats stats;
    format::PrxDecodeContext context {.stats = &stats};
    ByteSource source(sink.view());
    EXPECT_TRUE(query::internal::decode_and_validate_prx_frame(
                        &source, selected_ordinals,
                        /*decode_full=*/false,
                        /*all_docs_selected=*/false,
                        /*expected_total_docs=*/static_cast<uint32_t>(positions.size() + 1),
                        /*expected_selected_docs=*/selected_ordinals.size(), &flat, &offsets,
                        &context)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_EQ(stats.raw_frames, 1U);
    EXPECT_EQ(stats.total_docs, positions.size());
    EXPECT_EQ(stats.selected_docs, selected_ordinals.size());
    EXPECT_EQ(stats.total_positions, 4U);
    EXPECT_EQ(stats.selected_positions, 2U);
}

TEST(SniiQueryProfileTest, PhrasePrxValidationRejectsOnlyTotalDocMismatch) {
    const std::vector<uint32_t> flat = {1, 2, 3, 4};
    const std::vector<uint32_t> selected_ordinals = {0, 2};
    EXPECT_TRUE(query::internal::validate_prx_frame(
                        flat, std::vector<uint32_t> {0, 1, 4},
                        /*actual_total_docs=*/3, /*expected_total_docs=*/4,
                        /*expected_selected_docs=*/selected_ordinals.size(), selected_ordinals,
                        /*offsets_by_prx_ordinal=*/false,
                        /*all_docs_selected=*/false)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiQueryProfileTest, PhrasePrxValidationRejectsOnlyOffsetCountMismatch) {
    const std::vector<uint32_t> flat = {1, 2, 3, 4};
    const std::vector<uint32_t> selected_ordinals = {0, 2};
    EXPECT_TRUE(query::internal::validate_prx_frame(
                        flat, std::vector<uint32_t> {0, 4},
                        /*actual_total_docs=*/4, /*expected_total_docs=*/4,
                        /*expected_selected_docs=*/selected_ordinals.size(), selected_ordinals,
                        /*offsets_by_prx_ordinal=*/false,
                        /*all_docs_selected=*/false)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiQueryProfileTest, PhrasePrxValidationRejectsOnlyFinalOffsetMismatch) {
    const std::vector<uint32_t> flat = {1, 2, 3, 4};
    const std::vector<uint32_t> selected_ordinals = {0, 2};
    EXPECT_TRUE(query::internal::validate_prx_frame(
                        flat, std::vector<uint32_t> {0, 1, 3},
                        /*actual_total_docs=*/4, /*expected_total_docs=*/4,
                        /*expected_selected_docs=*/selected_ordinals.size(), selected_ordinals,
                        /*offsets_by_prx_ordinal=*/false,
                        /*all_docs_selected=*/false)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiQueryProfileTest, PhrasePrxValidationRejectsOnlySelectionCountMismatch) {
    const std::vector<uint32_t> flat = {1, 2, 3, 4};
    const std::vector<uint32_t> selected_ordinals = {0, 2};
    EXPECT_TRUE(query::internal::validate_prx_frame(flat, std::vector<uint32_t> {0, 4},
                                                    /*actual_total_docs=*/4,
                                                    /*expected_total_docs=*/4,
                                                    /*expected_selected_docs=*/1, selected_ordinals,
                                                    /*offsets_by_prx_ordinal=*/false,
                                                    /*all_docs_selected=*/false)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiQueryProfileTest, PhrasePrxValidationRejectsOnlyOrdinalRangeMismatch) {
    const std::vector<uint32_t> flat = {1, 2, 3, 4};
    const std::vector<uint32_t> selected_ordinals = {0, 4};
    EXPECT_TRUE(query::internal::validate_prx_frame(
                        flat, std::vector<uint32_t> {0, 1, 2, 3, 4},
                        /*actual_total_docs=*/4, /*expected_total_docs=*/4,
                        /*expected_selected_docs=*/selected_ordinals.size(), selected_ordinals,
                        /*offsets_by_prx_ordinal=*/true,
                        /*all_docs_selected=*/false)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiQueryProfileTest, PhraseVerifyTimeExcludesDecodeDelta) {
    EXPECT_EQ(query::internal::exclusive_phrase_verify_ns(
                      /*elapsed_ns=*/100, /*decode_ns_before=*/20, /*decode_ns_after=*/65),
              55U);
    EXPECT_EQ(query::internal::exclusive_phrase_verify_ns(
                      /*elapsed_ns=*/100, /*decode_ns_before=*/65, /*decode_ns_after=*/65),
              100U);
    EXPECT_EQ(query::internal::exclusive_phrase_verify_ns(
                      /*elapsed_ns=*/30, /*decode_ns_before=*/20, /*decode_ns_after=*/65),
              0U);
}

TEST(SniiQueryProfileTest, UnprofiledSelectivePhraseKeepsInstrumentationDisabled) {
    Corpus corpus;
    corpus.docs.resize(600);
    for (uint32_t docid = 0; docid < corpus.docs.size(); ++docid) {
        corpus.docs[docid] = {"common", docid % 4 == 0 ? "rare" : "other"};
    }
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader idx;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &idx).ok());

    std::vector<uint32_t> docs;
    format::testing::reset_prx_clock_read_count();
    query::internal::testing::reset_phrase_verify_clock_read_count();
    DEFER(format::testing::reset_prx_clock_read_count());
    DEFER(query::internal::testing::reset_phrase_verify_clock_read_count());
    ASSERT_TRUE(query::phrase_query(idx, {"common", "rare"}, &docs).ok());

    EXPECT_EQ(docs.size(), 150U);
    EXPECT_EQ(format::testing::prx_clock_read_count(), 0U);
    EXPECT_EQ(query::internal::testing::phrase_verify_clock_read_count(), 0U);

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, PhraseVerifyClockIsDisabledWithoutProfile) {
    const Corpus corpus = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader idx;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &idx).ok());

    std::vector<uint32_t> docs;
    query::internal::testing::reset_phrase_verify_clock_read_count();
    ASSERT_TRUE(query::phrase_query(idx, {"quick", "brown"}, &docs).ok());
    EXPECT_EQ(query::internal::testing::phrase_verify_clock_read_count(), 0U);

    query::QueryProfile profile;
    ASSERT_TRUE(query::phrase_query(idx, {"quick", "brown"}, &docs, &profile).ok());
    EXPECT_GT(query::internal::testing::phrase_verify_clock_read_count(), 0U);
    EXPECT_GT(profile.prx_decode_stats.decode_ns, 0U);
    EXPECT_GT(profile.prx_decode_stats.phrase_verify_ns, 0U);

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, MultiWindowFiveTermQueriesAggregateEveryPrxFrame) {
    Corpus corpus;
    corpus.docs.resize(600);
    for (auto& doc : corpus.docs) {
        doc = {"alpha", "beta", "gamma", "delta", "epsilon"};
    }
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader idx;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &idx).ok());
    const auto expect_totals = [](const query::QueryProfile& profile) {
        EXPECT_EQ(profile.prx_decode_stats.frame_count(), 15U);
        EXPECT_EQ(profile.prx_decode_stats.total_docs, 3000U);
        EXPECT_EQ(profile.prx_decode_stats.selected_docs, 3000U);
        EXPECT_EQ(profile.prx_decode_stats.total_positions, 3000U);
        EXPECT_EQ(profile.prx_decode_stats.selected_positions, 3000U);
    };

    std::vector<uint32_t> docs;
    ASSERT_TRUE(query::phrase_query(idx, {"alpha", "beta"}, &docs).ok());
    EXPECT_EQ(docs.size(), corpus.docs.size());

    query::QueryProfile phrase_profile;
    ASSERT_TRUE(query::phrase_query(idx, {"alpha", "beta", "gamma", "delta", "epsilon"}, &docs,
                                    &phrase_profile)
                        .ok());
    EXPECT_EQ(docs.size(), corpus.docs.size());
    expect_totals(phrase_profile);
    EXPECT_EQ(phrase_profile.prx_decode_stats.zstd_frames, 10U);

    query::QueryProfile prefix_profile;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"alpha", "beta", "gamma", "delta", "eps"}, &docs,
                                           &prefix_profile)
                        .ok());
    EXPECT_EQ(docs.size(), corpus.docs.size());
    expect_totals(prefix_profile);
    EXPECT_EQ(prefix_profile.prx_decode_stats.zstd_frames, 10U);

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, HighTfPhraseAndPrefixAggregateRetainedPrxStats) {
    const Corpus corpus = BuildHighTfCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader idx;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &idx).ok());

    std::vector<uint32_t> docs;
    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    DEFER(query::internal::testing::reset_streaming_exact_phrase_execution_count());
    ASSERT_TRUE(query::phrase_query(idx, {"alpha", "beta"}, &docs).ok());
    EXPECT_EQ(docs.size(), corpus.docs.size());
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 1U);
    const std::vector<uint32_t> matched_docs = docs;

    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    ASSERT_TRUE(query::phrase_query(idx, {"alpha", "gamma"}, &docs).ok());
    EXPECT_TRUE(docs.empty());
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 1U);

    std::vector<query::PhraseMatch> frequency_matches;
    query::QueryProfile frequency_profile;
    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    ASSERT_TRUE(query::phrase_query_with_frequencies(idx, {"alpha", "beta"}, &frequency_matches,
                                                     &frequency_profile)
                        .ok());
    std::vector<uint32_t> frequency_docids;
    frequency_docids.reserve(frequency_matches.size());
    for (const auto& match : frequency_matches) {
        frequency_docids.push_back(match.docid);
    }
    EXPECT_EQ(matched_docs, frequency_docids);
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 0U);
    EXPECT_EQ(frequency_profile.phrase_query_stats.prx_streaming_frames, 0U);

    query::QueryProfile repeated_profile;
    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    ASSERT_TRUE(query::phrase_query(idx, {"alpha", "alpha"}, &docs, &repeated_profile).ok());
    EXPECT_TRUE(docs.empty());
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 0U);
    EXPECT_EQ(repeated_profile.phrase_query_stats.prx_streaming_frames, 0U);

    query::QueryProfile sloppy_profile;
    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    ASSERT_TRUE(query::phrase_query(idx, {"alpha", "gamma"}, &docs, &sloppy_profile,
                                    {.slop = 1, .ordered = true})
                        .ok());
    EXPECT_EQ(docs.size(), corpus.docs.size());
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 0U);
    EXPECT_EQ(sloppy_profile.phrase_query_stats.prx_streaming_frames, 0U);

    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    ASSERT_TRUE(query::phrase_query(idx, {"delta", "epsilon_00"}, &docs).ok());
    EXPECT_EQ(docs.size(), 19U);
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 1U);

    format::testing::reset_prx_clock_read_count();
    query::internal::testing::reset_phrase_verify_clock_read_count();
    DEFER(format::testing::reset_prx_clock_read_count());
    DEFER(query::internal::testing::reset_phrase_verify_clock_read_count());
    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    ASSERT_TRUE(query::phrase_query(idx, {"alpha", "beta", "gamma", "delta"}, &docs).ok());
    EXPECT_EQ(docs.size(), corpus.docs.size());
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 1U);
    EXPECT_EQ(format::testing::prx_clock_read_count(), 0U);
    EXPECT_EQ(query::internal::testing::phrase_verify_clock_read_count(), 0U);

    query::QueryProfile phrase_profile;
    ASSERT_TRUE(
            query::phrase_query(idx, {"alpha", "beta", "gamma", "delta"}, &docs, &phrase_profile)
                    .ok());
    EXPECT_EQ(docs.size(), corpus.docs.size());
    EXPECT_EQ(phrase_profile.prx_decode_stats.zstd_frames, 12U);
    EXPECT_EQ(phrase_profile.prx_decode_stats.frame_count(), 12U);
    EXPECT_EQ(phrase_profile.prx_decode_stats.total_docs, 2400U);
    EXPECT_EQ(phrase_profile.prx_decode_stats.selected_docs, 2400U);
    EXPECT_EQ(phrase_profile.prx_decode_stats.total_positions, 115200U);
    EXPECT_EQ(phrase_profile.prx_decode_stats.selected_positions, 115200U);
    EXPECT_EQ(phrase_profile.phrase_query_stats.prx_streaming_frames, 12U);

    query::internal::query_test_counters() = query::internal::QueryTestCounters {};
    DEFER(query::internal::query_test_counters() = query::internal::QueryTestCounters {});
    query::QueryProfile prefix_profile;
    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"alpha", "beta", "gamma", "delta", "eps"}, &docs,
                                           &prefix_profile, /*max_expansions=*/0)
                        .ok());
    EXPECT_EQ(docs.size(), corpus.docs.size());
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 0U);
    EXPECT_EQ(prefix_profile.prx_decode_stats.zstd_frames, 45U);
    EXPECT_EQ(prefix_profile.prx_decode_stats.frame_count(), 45U);
    EXPECT_EQ(prefix_profile.prx_decode_stats.total_docs, 3000U);
    EXPECT_EQ(prefix_profile.prx_decode_stats.selected_docs, 3000U);
    EXPECT_EQ(prefix_profile.prx_decode_stats.total_positions, 144000U);
    EXPECT_EQ(prefix_profile.prx_decode_stats.selected_positions, 144000U);
    EXPECT_EQ(prefix_profile.phrase_query_stats.prx_streaming_frames, 0U);
    EXPECT_GT(query::internal::query_test_counters().monotonic_position_scans, 0U);

    format::testing::reset_prx_clock_read_count();
    query::internal::testing::reset_phrase_verify_clock_read_count();
    query::QueryProfile single_phrase_profile;
    ASSERT_TRUE(query::phrase_query(idx, {"alpha"}, &docs, &single_phrase_profile).ok());
    EXPECT_EQ(single_phrase_profile.prx_decode_stats, format::PrxDecodeStats {});
    query::QueryProfile single_prefix_profile;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"eps"}, &docs, &single_prefix_profile).ok());
    EXPECT_EQ(single_prefix_profile.prx_decode_stats, format::PrxDecodeStats {});
    EXPECT_EQ(format::testing::prx_clock_read_count(), 0U);
    EXPECT_EQ(query::internal::testing::phrase_verify_clock_read_count(), 0U);

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, FrequencyDroppedHighTfIndexUsesStreamingExactPhrase) {
    const Corpus corpus = BuildHighTfCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path, /*prx_zstd_level=*/3, /*write_freq=*/false);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader index;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &index).ok());

    for (const std::string_view term : {std::string_view("alpha"), std::string_view("beta")}) {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        ASSERT_TRUE(index.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
        ASSERT_TRUE(found);
        EXPECT_FALSE(entry.term_stats_present);
    }

    query::internal::testing::reset_streaming_exact_phrase_execution_count();
    DEFER(query::internal::testing::reset_streaming_exact_phrase_execution_count());
    query::QueryProfile profile;
    std::vector<uint32_t> docs;
    ASSERT_TRUE(query::phrase_query(index, {"alpha", "beta"}, &docs, &profile).ok());
    std::vector<uint32_t> expected(corpus.docs.size());
    std::iota(expected.begin(), expected.end(), 0U);
    EXPECT_EQ(docs, expected);
    EXPECT_EQ(query::internal::testing::streaming_exact_phrase_execution_count(), 1U);
    EXPECT_GT(profile.phrase_query_stats.prx_streaming_frames, 0U);
    EXPECT_EQ(profile.prx_decode_stats.raw_frames, 0U);
    EXPECT_GT(profile.prx_decode_stats.zstd_frames + profile.prx_decode_stats.pfor_frames, 0U);

    std::vector<query::PhraseMatch> materialized_matches;
    ASSERT_TRUE(
            query::phrase_query_with_frequencies(index, {"alpha", "beta"}, &materialized_matches)
                    .ok());
    std::vector<uint32_t> materialized_docs;
    materialized_docs.reserve(materialized_matches.size());
    for (const auto& match : materialized_matches) {
        materialized_docs.push_back(match.docid);
    }
    EXPECT_EQ(docs, materialized_docs);

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, EarlyExactPhraseReturnsExpectedDocsForEveryCodec) {
    struct CodecCase {
        const char* name;
        int writer_prx_level;
        uint32_t doc_count;
        format::PrxCodec expected_codec;
    };
    const std::array<CodecCase, 3> cases = {
            CodecCase {.name = "RAW",
                       .writer_prx_level = 0,
                       .doc_count = 6,
                       .expected_codec = format::PrxCodec::kRaw},
            CodecCase {.name = "ZSTD",
                       .writer_prx_level = -3,
                       .doc_count = 12,
                       .expected_codec = format::PrxCodec::kZstd},
            CodecCase {.name = "PFOR auto",
                       .writer_prx_level = 3,
                       .doc_count = 6,
                       .expected_codec = format::PrxCodec::kPfor},
    };

    for (const CodecCase& codec_case : cases) {
        const Corpus corpus = BuildEarlyHitCorpus(codec_case.doc_count);
        const std::string path = TempPath();
        WriteCorpus(corpus, path, codec_case.writer_prx_level);

        io::LocalFileReader local;
        ASSERT_TRUE(local.open(path).ok()) << codec_case.name;
        SniiSegmentReader segment;
        LogicalIndexReader index;
        ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok()) << codec_case.name;
        ASSERT_TRUE(segment.open_index(1, "body", &index).ok()) << codec_case.name;

        query::QueryProfile profile;
        std::vector<uint32_t> docs;
        ASSERT_TRUE(query::phrase_query(index, {"a", "b"}, &docs, &profile).ok())
                << codec_case.name;
        std::vector<uint32_t> expected(codec_case.doc_count);
        std::iota(expected.begin(), expected.end(), 0U);
        EXPECT_EQ(docs, expected) << codec_case.name;
        EXPECT_GT(profile.phrase_query_stats.prx_streaming_frames, 0U) << codec_case.name;
        EXPECT_EQ(profile.phrase_query_stats.prx_streaming_frames,
                  profile.prx_decode_stats.frame_count())
                << codec_case.name;

        const uint64_t actual_codec_frames = [&]() {
            switch (codec_case.expected_codec) {
            case format::PrxCodec::kRaw:
                return profile.prx_decode_stats.raw_frames;
            case format::PrxCodec::kZstd:
                return profile.prx_decode_stats.zstd_frames;
            case format::PrxCodec::kPfor:
                return profile.prx_decode_stats.pfor_frames;
            }
            __builtin_unreachable();
        }();
        EXPECT_EQ(actual_codec_frames, profile.prx_decode_stats.frame_count()) << codec_case.name;

        std::remove(path.c_str());
    }
}

TEST(SniiQueryProfileTest, LateStreamingCorruptionPublishesNoPartialCallerOutput) {
    // Keep the posting out of the dictionary's inline threshold so the mutable
    // file reader observes the same PRX bytes that the query path fetches.
    const Corpus corpus = BuildEarlyHitCorpus(/*doc_count=*/600);
    const std::string path = TempPath();
    WriteCorpus(corpus, path, /*prx_zstd_level=*/0);
    MutableMemoryReader file(ReadFile(path));
    std::remove(path.c_str());

    SniiSegmentReader segment;
    LogicalIndexReader index;
    ASSERT_TRUE(SniiSegmentReader::open(&file, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &index).ok());
    CorruptLastRawDocumentWithTrailingPosition(&file, index, "b");

    std::vector<uint32_t> docs = {99};
    query::QueryProfile profile;
    profile.prx_decode_stats.raw_frames = 99;
    const Status status = query::phrase_query(index, {"a", "b"}, &docs, &profile);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_TRUE(docs.empty());
    EXPECT_GT(profile.prx_decode_stats.raw_frames, 0U);
    EXPECT_EQ(profile.phrase_query_stats.prx_streaming_frames, 0U);
}

TEST(SniiQueryProfileTest, StreamingDocCountMismatchPublishesNoPartialCallerOutput) {
    ExpectStreamingDocCountMismatchIsAtomic(/*delta=*/-1);
    ExpectStreamingDocCountMismatchIsAtomic(/*delta=*/1);
}

TEST(SniiQueryProfileTest, SingleTailGroupVisitsExpectedDocsOnce) {
    const Corpus corpus = BuildSingleTailGroupCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader idx;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &idx).ok());

    query::internal::query_test_counters() = query::internal::QueryTestCounters {};
    DEFER(query::internal::query_test_counters() = query::internal::QueryTestCounters {});
    std::vector<uint32_t> docs;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"alpha", "beta", "eps"}, &docs,
                                           /*max_expansions=*/0)
                        .ok());

    EXPECT_EQ(docs.size(), corpus.docs.size());
    EXPECT_EQ(query::internal::query_test_counters().prefix_expected_doc_visits,
              corpus.docs.size());

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, DisjointTailGroupDoesNotVisitExpectedDocs) {
    const Corpus corpus = BuildDisjointTailGroupCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader idx;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &idx).ok());

    query::internal::query_test_counters() = query::internal::QueryTestCounters {};
    DEFER(query::internal::query_test_counters() = query::internal::QueryTestCounters {});
    std::vector<uint32_t> docs;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"alpha", "beta", "eps"}, &docs,
                                           /*max_expansions=*/0)
                        .ok());

    EXPECT_TRUE(docs.empty());
    EXPECT_EQ(query::internal::query_test_counters().prefix_expected_doc_visits, 0U);

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, MultiTermPhraseResumesPairStartsAndEmitsEachDocOnce) {
    Corpus corpus;
    corpus.docs.resize(64, {"c"});
    corpus.docs[0] = {"a", "b", "x", "a", "b", "c"};
    corpus.docs[1] = {"a", "a", "a", "c"};
    corpus.docs[2] = {"a", "b", "c", "a", "b", "c"};
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader idx;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &idx).ok());

    std::vector<uint32_t> docs;
    ASSERT_TRUE(query::phrase_query(idx, {"a", "b", "c"}, &docs).ok());
    EXPECT_EQ(docs, (std::vector<uint32_t> {0, 2}));

    ASSERT_TRUE(query::phrase_query(idx, {"a", "a", "a"}, &docs).ok());
    EXPECT_EQ(docs, (std::vector<uint32_t> {1}));

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, HalfDensePhraseUsesFullDecodeAndReportsOriginalSelection) {
    Corpus corpus;
    corpus.docs.resize(600);
    for (uint32_t docid = 0; docid < corpus.docs.size(); ++docid) {
        corpus.docs[docid] = {"common", docid % 2 == 0 ? "rare" : "other"};
    }
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    SniiSegmentReader segment;
    LogicalIndexReader idx;
    ASSERT_TRUE(SniiSegmentReader::open(&local, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "body", &idx).ok());

    query::QueryProfile profile;
    std::vector<uint32_t> docs;
    ASSERT_TRUE(query::phrase_query(idx, {"common", "rare"}, &docs, &profile).ok());

    ASSERT_EQ(docs.size(), 300U);
    for (size_t i = 0; i < docs.size(); ++i) {
        EXPECT_EQ(docs[i], i * 2);
    }
    EXPECT_EQ(profile.prx_decode_stats.frame_count(), 4U);
    EXPECT_EQ(profile.prx_decode_stats.total_docs, 900U);
    EXPECT_EQ(profile.prx_decode_stats.selected_docs, 600U);
    EXPECT_EQ(profile.prx_decode_stats.total_positions, 900U);
    EXPECT_EQ(profile.prx_decode_stats.selected_positions, 600U);

    std::remove(path.c_str());
}

TEST(SniiQueryProfile, ReportsElapsedTimeAndMeteredIoForNativeOperators) {
    const Corpus corpus = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/512);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    std::vector<uint32_t> docs;
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::term_query(idx, "lead", &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::boolean_or(idx, {"lead", "missing"}, &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::boolean_and(idx, {"quick", "brown"}, &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::prefix_query(idx, "aa_", &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::wildcard_query(idx, "aa_0??", &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::regexp_query(idx, "aa_00[0-9]", &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::phrase_query(idx, {"quick", "brown"}, &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::phrase_prefix_query(idx, {"quick", "bro"}, &docs, profile);
    });

    std::remove(path.c_str());
}

TEST(SniiQueryProfile, ReportsElapsedTimeForInvalidRegexpPath) {
    const Corpus corpus = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/512);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    metered.reset_metrics();
    std::vector<uint32_t> docs;
    query::QueryProfile profile;
    const Status st = query::regexp_query(idx, "(", &docs, &profile);

    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(docs.empty());
    EXPECT_GT(profile.elapsed_ns, 0U);
    ASSERT_TRUE(profile.has_io_metrics);
    EXPECT_EQ(profile.io_delta.read_at_calls, 0U);
    EXPECT_EQ(profile.io_delta.serial_rounds, 0U);
    EXPECT_EQ(profile.io_delta.range_gets, 0U);
    EXPECT_EQ(profile.io_delta.remote_bytes, 0U);
    EXPECT_EQ(profile.io_delta.total_request_bytes, 0U);

    std::remove(path.c_str());
}

TEST(SniiQueryProfile, ScopeFinalizesProfileOnEarlyReturn) {
    query::QueryProfile profile;
    { query::QueryProfileScope scope(/*reader=*/nullptr, &profile); }

    EXPECT_GT(profile.elapsed_ns, 0U);
    EXPECT_FALSE(profile.has_io_metrics);
}

TEST(SniiQueryProfileTest, AggregatesMultiTermPrxAndKeepsSingleTermControlsAtZero) {
    const Corpus corpus = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/512);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    std::vector<uint32_t> docs;
    query::QueryProfile phrase_profile;
    ASSERT_TRUE(query::phrase_query(idx, {"quick", "brown"}, &docs, &phrase_profile).ok());
    EXPECT_GT(phrase_profile.prx_decode_stats.frame_count(), 0U);
    EXPECT_GT(phrase_profile.prx_decode_stats.total_docs, 0U);
    EXPECT_LE(phrase_profile.prx_decode_stats.selected_docs,
              phrase_profile.prx_decode_stats.total_docs);
    EXPECT_GT(phrase_profile.prx_decode_stats.phrase_verify_ns, 0U);

    query::QueryProfile prefix_profile;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"quick", "bro"}, &docs, &prefix_profile).ok());
    EXPECT_GT(prefix_profile.prx_decode_stats.frame_count(), 0U);
    EXPECT_TRUE(prefix_profile.prx_decode_stats.is_valid());

    query::QueryProfile one_phrase_profile;
    ASSERT_TRUE(query::phrase_query(idx, {"quick"}, &docs, &one_phrase_profile).ok());
    EXPECT_EQ(one_phrase_profile.prx_decode_stats, doris::snii::format::PrxDecodeStats {});

    query::QueryProfile one_prefix_profile;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"bro"}, &docs, &one_prefix_profile).ok());
    EXPECT_EQ(one_prefix_profile.prx_decode_stats, doris::snii::format::PrxDecodeStats {});

    std::remove(path.c_str());
}

TEST(SniiQueryProfileTest, ReaderAndRuntimePrxTotalsAreAdditiveWithStableNames) {
    doris::snii::format::PrxDecodeStats delta;
    delta.raw_frames = 1;
    delta.zstd_frames = 2;
    delta.pfor_frames = 3;
    delta.plaintext_bytes = 101;
    delta.total_docs = 8;
    delta.selected_docs = 7;
    delta.total_positions = 9;
    delta.selected_positions = 6;
    delta.fetch_ns = 10;
    delta.decode_ns = 11;
    delta.phrase_verify_ns = 12;

    doris::OlapReaderStatistics reader_stats;
    doris::snii::add_prx_decode_stats(&reader_stats, delta);
    doris::snii::add_prx_decode_stats(&reader_stats, delta);
    EXPECT_EQ(reader_stats.snii_stats.prx_raw_frames, 2);
    EXPECT_EQ(reader_stats.snii_stats.prx_zstd_frames, 4);
    EXPECT_EQ(reader_stats.snii_stats.prx_pfor_frames, 6);
    EXPECT_EQ(reader_stats.snii_stats.prx_plaintext_bytes, 202);
    EXPECT_EQ(reader_stats.snii_stats.prx_total_docs, 16);
    EXPECT_EQ(reader_stats.snii_stats.prx_selected_docs, 14);
    EXPECT_EQ(reader_stats.snii_stats.prx_total_positions, 18);
    EXPECT_EQ(reader_stats.snii_stats.prx_selected_positions, 12);
    EXPECT_EQ(reader_stats.snii_stats.prx_fetch_ns, 20);
    EXPECT_EQ(reader_stats.snii_stats.prx_decode_ns, 22);
    EXPECT_EQ(reader_stats.snii_stats.prx_phrase_verify_ns, 24);

    doris::RuntimeProfile runtime_profile("IndexFilter");
    doris::snii::SniiPrxRuntimeProfileCounters counters;
    counters.initialize(&runtime_profile);

    std::vector<doris::TRuntimeProfileNode> zero_nodes;
    runtime_profile.to_thrift(&zero_nodes);
    ASSERT_EQ(zero_nodes.size(), 1U);
    for (const char* name : doris::snii::SniiPrxRuntimeProfileCounters::counter_names()) {
        EXPECT_TRUE(std::ranges::none_of(
                zero_nodes.front().counters,
                [name](const doris::TCounter& counter) { return counter.name == name; }))
                << name;
    }

    counters.update(reader_stats);
    counters.update(reader_stats);

    EXPECT_EQ(doris::snii::SniiPrxRuntimeProfileCounters::counter_names().size(), 11U);
    for (const char* name : doris::snii::SniiPrxRuntimeProfileCounters::counter_names()) {
        auto* counter = runtime_profile.get_counter(name);
        ASSERT_NE(counter, nullptr) << name;
        EXPECT_NE(dynamic_cast<doris::RuntimeProfile::NonZeroCounter*>(counter), nullptr) << name;
    }
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxEncodedBytes"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxPayloadBytes"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxCompressedBytes"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxChild128Touches"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxChild256Touches"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxNestedCrcValidationTime"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxNestedDecompressTime"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxScratchAllocationEvents"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxContainerAllocationEvents"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxCountScanTime"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxSkipTime"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxTraceAllocationEvents"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxTraceRecords"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxDecodeTime"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxCrcValidationTime"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxDecompressTime"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxPhraseVerifyTime"), nullptr);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxRawFrames")->type(), doris::TUnit::UNIT);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxPlaintextBytes")->type(), doris::TUnit::BYTES);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxInclusiveDecodeTime")->type(),
              doris::TUnit::TIME_NS);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxExclusivePhraseVerifyTime")->type(),
              doris::TUnit::TIME_NS);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxRawFrames")->value(), 4);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxZstdFrames")->value(), 8);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxPforFrames")->value(), 12);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxPlaintextBytes")->value(), 404);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxTotalDocs")->value(), 32);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxSelectedDocs")->value(), 28);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxTotalPositions")->value(), 36);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxSelectedPositions")->value(), 24);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxFetchTime")->value(), 40);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxInclusiveDecodeTime")->value(), 44);
    EXPECT_EQ(runtime_profile.get_counter("SniiPrxExclusivePhraseVerifyTime")->value(), 48);

    std::vector<doris::TRuntimeProfileNode> nonzero_nodes;
    runtime_profile.to_thrift(&nonzero_nodes);
    ASSERT_EQ(nonzero_nodes.size(), 1U);
    for (const char* name : doris::snii::SniiPrxRuntimeProfileCounters::counter_names()) {
        EXPECT_TRUE(std::ranges::any_of(
                nonzero_nodes.front().counters,
                [name](const doris::TCounter& counter) { return counter.name == name; }))
                << name;
    }
}

TEST(SniiQueryProfileTest, PhraseReaderAndRuntimeTotalsAreAdditiveAndNonZero) {
    doris::snii::format::PhraseQueryExecutionStats delta;
    delta.exact_candidate_docs = 1;
    delta.exact_candidate_visits = 2;
    delta.prefix_leading_candidate_docs = 3;
    delta.prefix_tail_candidate_visits = 4;
    delta.common_grams_candidate_queries = 5;
    delta.common_grams_plain_plans = 6;
    delta.common_grams_gram_plans = 7;
    delta.common_grams_fallback_no_gram = 8;
    delta.common_grams_fallback_incompatible = 9;
    delta.common_grams_fallback_kill_switch = 10;
    delta.common_grams_fallback_cost = 11;
    delta.common_grams_authoritative_empty = 12;
    delta.common_grams_plain_posting_bytes = 13;
    delta.common_grams_gram_posting_bytes = 14;
    delta.common_grams_plain_estimated_candidate_df = 15;
    delta.common_grams_gram_estimated_candidate_df = 16;
    delta.common_grams_plain_estimated_cost = 17;
    delta.common_grams_gram_estimated_cost = 18;
    delta.common_grams_fallback_base_analyzer_mismatch = 19;
    delta.common_grams_fallback_prefix_tail_empty = 20;
    delta.common_grams_planning_ns = 23;
    delta.prx_streaming_frames = 3;

    doris::OlapReaderStatistics reader_stats;
    doris::snii::add_phrase_query_stats(&reader_stats, delta);
    doris::snii::add_phrase_query_stats(&reader_stats, delta);
    EXPECT_EQ(reader_stats.snii_stats.phrase_candidate_docs, 2);
    EXPECT_EQ(reader_stats.snii_stats.phrase_candidate_visits, 4);
    EXPECT_EQ(reader_stats.snii_stats.phrase_prefix_leading_candidate_docs, 6);
    EXPECT_EQ(reader_stats.snii_stats.phrase_prefix_tail_candidate_visits, 8);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_candidate_queries, 10);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_plain_plans, 12);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_gram_plans, 14);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_fallback_no_gram, 16);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_fallback_incompatible, 18);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_fallback_kill_switch, 20);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_fallback_cost, 22);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_authoritative_empty, 24);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_plain_posting_bytes, 26);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_gram_posting_bytes, 28);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_plain_estimated_candidate_df, 30);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_gram_estimated_candidate_df, 32);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_plain_estimated_cost, 34);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_gram_estimated_cost, 36);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_fallback_base_analyzer_mismatch, 38);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_fallback_prefix_tail_empty, 40);
    EXPECT_EQ(reader_stats.snii_stats.common_grams_planning_ns, 46);
    EXPECT_EQ(reader_stats.snii_stats.prx_streaming_frames, 6);

    doris::RuntimeProfile runtime_profile("IndexFilter");
    doris::snii::SniiPhraseRuntimeProfileCounters counters;
    counters.initialize(&runtime_profile);

    struct ExpectedCounter {
        const char* name;
        int64_t value_after_two_updates;
        doris::TUnit::type unit;
    };
    const ExpectedCounter expected_counters[] = {
            {"SniiPhraseCandidateDocs", 4, doris::TUnit::UNIT},
            {"SniiPhraseCandidateVisits", 8, doris::TUnit::UNIT},
            {"SniiPhrasePrefixLeadingCandidateDocs", 12, doris::TUnit::UNIT},
            {"SniiPhrasePrefixTailCandidateVisits", 16, doris::TUnit::UNIT},
            {"SniiCommonGramsCandidateQueries", 20, doris::TUnit::UNIT},
            {"SniiCommonGramsPlainPlans", 24, doris::TUnit::UNIT},
            {"SniiCommonGramsGramPlans", 28, doris::TUnit::UNIT},
            {"SniiCommonGramsFallbackNoGram", 32, doris::TUnit::UNIT},
            {"SniiCommonGramsFallbackIncompatible", 36, doris::TUnit::UNIT},
            {"SniiCommonGramsFallbackKillSwitch", 40, doris::TUnit::UNIT},
            {"SniiCommonGramsFallbackCost", 44, doris::TUnit::UNIT},
            {"SniiCommonGramsAuthoritativeEmpty", 48, doris::TUnit::UNIT},
            {"SniiCommonGramsPlainPostingBytes", 52, doris::TUnit::BYTES},
            {"SniiCommonGramsGramPostingBytes", 56, doris::TUnit::BYTES},
            {"SniiCommonGramsPlainEstimatedCandidateDf", 60, doris::TUnit::UNIT},
            {"SniiCommonGramsGramEstimatedCandidateDf", 64, doris::TUnit::UNIT},
            {"SniiCommonGramsPlainEstimatedCost", 68, doris::TUnit::UNIT},
            {"SniiCommonGramsGramEstimatedCost", 72, doris::TUnit::UNIT},
            {"SniiCommonGramsFallbackBaseAnalyzerMismatch", 76, doris::TUnit::UNIT},
            {"SniiCommonGramsFallbackPrefixTailEmpty", 80, doris::TUnit::UNIT},
            {"SniiCommonGramsPlanningTime", 92, doris::TUnit::TIME_NS},
            {"SniiPhraseStreamingPrxFrames", 12, doris::TUnit::UNIT},
    };

    std::vector<doris::TRuntimeProfileNode> zero_nodes;
    runtime_profile.to_thrift(&zero_nodes);
    ASSERT_EQ(zero_nodes.size(), 1U);
    for (const auto& expected : expected_counters) {
        EXPECT_TRUE(std::ranges::none_of(
                zero_nodes.front().counters,
                [&](const doris::TCounter& counter) { return counter.name == expected.name; }))
                << expected.name;
    }

    counters.update(reader_stats);
    EXPECT_EQ(runtime_profile.get_counter("SniiPhraseStreamingPrxFrames")->value(), 6);
    counters.update(reader_stats);

    EXPECT_EQ(doris::snii::SniiPrxRuntimeProfileCounters::counter_names().size(), 11U);
    for (const auto& expected : expected_counters) {
        auto* counter = runtime_profile.get_counter(expected.name);
        ASSERT_NE(counter, nullptr) << expected.name;
        EXPECT_NE(dynamic_cast<doris::RuntimeProfile::NonZeroCounter*>(counter), nullptr)
                << expected.name;
        EXPECT_EQ(counter->type(), expected.unit) << expected.name;
        EXPECT_EQ(counter->value(), expected.value_after_two_updates) << expected.name;
    }

    std::vector<doris::TRuntimeProfileNode> nonzero_nodes;
    runtime_profile.to_thrift(&nonzero_nodes);
    ASSERT_EQ(nonzero_nodes.size(), 1U);
    for (const auto& expected : expected_counters) {
        EXPECT_TRUE(std::ranges::any_of(
                nonzero_nodes.front().counters,
                [&](const doris::TCounter& counter) { return counter.name == expected.name; }))
                << expected.name;
    }
}

TEST(SniiQueryProfileTest, ExecutionProfileScopeFlushesNormalAndErrorReturnsAdditively) {
    static_assert(!std::is_copy_constructible_v<doris::snii::SniiPrxExecutionProfileScope>);
    static_assert(!std::is_move_constructible_v<doris::snii::SniiPrxExecutionProfileScope>);

    doris::OlapReaderStatistics reader_stats;
    const auto execute = [&](bool fail) -> Status {
        doris::snii::SniiPrxExecutionProfileScope scope(reader_stats);
        scope.profile()->prx_decode_stats.raw_frames = 1;
        scope.profile()->prx_decode_stats.plaintext_bytes = 17;
        if (fail) {
            return Status::InternalError("injected execution failure");
        }
        return Status::OK();
    };

    ASSERT_TRUE(execute(false).ok());
    EXPECT_EQ(reader_stats.snii_stats.prx_raw_frames, 1);
    EXPECT_EQ(reader_stats.snii_stats.prx_plaintext_bytes, 17);
    ASSERT_FALSE(execute(true).ok());
    EXPECT_EQ(reader_stats.snii_stats.prx_raw_frames, 2);
    EXPECT_EQ(reader_stats.snii_stats.prx_plaintext_bytes, 34);
}
