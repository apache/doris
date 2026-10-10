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

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <numeric>
#include <roaring/roaring.hh>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_query_oracle.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// An exact phrase whose terms hold many positions per document streams them through the shared
// engine, each document's positions decoded only as far as the match reads them; any other phrase
// decodes its blocks' positions whole. A frame the stream skipped through is still checked.
namespace doris::snii::query {
namespace {

using reader::LogicalIndexReader;
using reader::SniiSegmentReader;

std::string temp_path() {
    static int counter = 0;
    return "/tmp/snii_phrase_streaming_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

// The index file in memory, so a test can corrupt the bytes the query reads.
class MemoryFileReader final : public io::FileReader {
public:
    explicit MemoryFileReader(std::vector<uint8_t> bytes) : _bytes(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset > _bytes.size() || len > _bytes.size() - offset) {
            return Status::Corruption<false>("memory file read past eof");
        }
        out->assign(_bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                    _bytes.begin() + static_cast<std::ptrdiff_t>(offset + len));
        return Status::OK();
    }

    uint64_t size() const override { return _bytes.size(); }
    std::vector<uint8_t>& bytes() { return _bytes; }

private:
    std::vector<uint8_t> _bytes;
};

using Corpus = std::vector<std::vector<std::string>>;

// Short documents: every term once in each.
Corpus light_corpus() {
    Corpus docs(128);
    for (uint32_t doc = 0; doc < docs.size(); ++doc) {
        docs[doc] = {"lead", "quick", doc % 2 == 0 ? "brown" : "bronze"};
    }
    return docs;
}

// Long documents repeating "alpha beta gamma delta" and a tail 48 times.
Corpus heavy_corpus() {
    constexpr uint32_t kTailCount = 33;
    constexpr uint32_t kRepetitions = 48;
    Corpus docs(600);
    for (uint32_t doc = 0; doc < docs.size(); ++doc) {
        const std::string tail = "epsilon_" + std::to_string(doc % kTailCount);
        for (uint32_t repetition = 0; repetition < kRepetitions; ++repetition) {
            docs[doc].insert(docs[doc].end(), {"alpha", "beta", "gamma", "delta", tail});
        }
    }
    return docs;
}

// "a b" repeated 48 times: the phrase matches at the first two positions of every document.
Corpus early_hit_corpus(uint32_t doc_count) {
    constexpr uint32_t kPairs = 48;
    Corpus docs(doc_count);
    for (auto& doc : docs) {
        for (uint32_t pair = 0; pair < kPairs; ++pair) {
            doc.insert(doc.end(), {"a", "b"});
        }
    }
    return docs;
}

void write_corpus(const Corpus& docs, const std::string& path, int prx_zstd_level) {
    writer::SpimiTermBuffer buffer(/*has_positions=*/true);
    for (uint32_t doc = 0; doc < docs.size(); ++doc) {
        for (uint32_t position = 0; position < docs[doc].size(); ++position) {
            buffer.add_token(docs[doc][position], doc, position);
        }
    }
    writer::SniiIndexInput input;
    input.index_id = 1;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = static_cast<uint32_t>(docs.size());
    input.encoded_norms.assign(docs.size(), 1);
    input.terms = buffer.finalize_sorted();
    input.target_dict_block_bytes = 512;
    input.prx_zstd_level = prx_zstd_level;
    io::LocalFileWriter file;
    ASSERT_TRUE(file.open(path).ok());
    writer::SniiCompoundWriter compound(&file);
    ASSERT_TRUE(compound.add_logical_index(input).ok());
    ASSERT_TRUE(compound.finish().ok());
}

std::vector<uint8_t> written_bytes(const Corpus& docs, int prx_zstd_level) {
    const std::string path = temp_path();
    write_corpus(docs, path, prx_zstd_level);
    io::LocalFileReader file;
    EXPECT_TRUE(file.open(path).ok());
    std::vector<uint8_t> bytes;
    EXPECT_TRUE(file.read_at(0, file.size(), &bytes).ok());
    std::remove(path.c_str());
    return bytes;
}

// The corpus written with `prx_zstd_level` and opened from memory.
class Index {
public:
    Index(const Corpus& docs, int prx_zstd_level) : file(written_bytes(docs, prx_zstd_level)) {
        EXPECT_TRUE(SniiSegmentReader::open(&file, &segment).ok());
        EXPECT_TRUE(segment.open_index(1, "body", &index).ok());
    }

    MemoryFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader index;
};

// The frames `stats` decoded with `codec`.
uint64_t codec_frames(const format::PrxDecodeStats& stats, format::PrxCodec codec) {
    switch (codec) {
    case format::PrxCodec::kRaw:
        return stats.raw_frames;
    case format::PrxCodec::kZstd:
        return stats.zstd_frames;
    case format::PrxCodec::kPfor:
        return stats.pfor_frames;
    }
    __builtin_unreachable();
}

std::vector<uint32_t> every_doc(size_t count) {
    std::vector<uint32_t> docs(count);
    std::iota(docs.begin(), docs.end(), 0U);
    return docs;
}

// The byte range of `term`'s PRX frames in the file.
void prx_range(Index& fixture, std::string_view term, uint64_t* offset, uint64_t* length) {
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    ASSERT_TRUE(fixture.index.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    ASSERT_TRUE(found);
    ASSERT_EQ(entry.kind, format::DictEntryKind::kPodRef);
    ASSERT_TRUE(fixture.index.resolve_prx_window(entry, prx_base, offset, length).ok());
    ASSERT_LE(*offset + *length, fixture.file.bytes().size());
}

// Rewrites the frame's checksum after its bytes changed.
void reseal_frame(Index& fixture, uint64_t frame_offset, size_t frame_length) {
    std::vector<uint8_t>& bytes = fixture.file.bytes();
    const uint32_t checksum =
            crc32c(Slice(bytes.data() + frame_offset, frame_length - sizeof(uint32_t)));
    for (size_t byte = 0; byte < sizeof(checksum); ++byte) {
        bytes[frame_offset + frame_length - sizeof(checksum) + byte] =
                static_cast<uint8_t>(checksum >> (8 * byte));
    }
}

// One raw frame of a PRX region: where its payload starts and how long the frame is.
struct RawFrame {
    size_t offset = 0;
    size_t payload_offset = 0;
    uint32_t payload_length = 0;
    size_t length = 0;
};

std::vector<RawFrame> raw_frames(Slice frames) {
    std::vector<RawFrame> out;
    ByteSource source(frames);
    while (!source.eof()) {
        RawFrame frame {.offset = source.position()};
        uint8_t codec = 0;
        EXPECT_TRUE(source.get_u8(&codec).ok());
        EXPECT_EQ(codec, static_cast<uint8_t>(format::PrxCodec::kRaw));
        EXPECT_TRUE(source.get_varint32(&frame.payload_length).ok());
        frame.payload_offset = source.position();
        Slice payload;
        EXPECT_TRUE(source.get_bytes(frame.payload_length, &payload).ok());
        uint32_t checksum = 0;
        EXPECT_TRUE(source.get_fixed32(&checksum).ok());
        frame.length = source.position() - frame.offset;
        out.push_back(frame);
    }
    return out;
}

// Lowers the position count of the last document of `term`'s last raw frame by one, so the
// frame ends with a position no document owns: a stream that matched early must still see it.
void corrupt_last_document(Index& fixture, std::string_view term) {
    uint64_t offset = 0;
    uint64_t length = 0;
    prx_range(fixture, term, &offset, &length);
    std::vector<uint8_t>& bytes = fixture.file.bytes();
    const auto frames = raw_frames(Slice(bytes.data() + offset, static_cast<size_t>(length)));
    ASSERT_FALSE(frames.empty());
    const RawFrame& last = frames.back();
    ByteSource payload(Slice(bytes.data() + offset + last.payload_offset, last.payload_length));
    uint32_t doc_count = 0;
    ASSERT_TRUE(payload.get_varint32(&doc_count).ok());
    size_t count_offset = 0;
    for (uint32_t doc = 0; doc < doc_count; ++doc) {
        count_offset = payload.position();
        uint32_t count = 0;
        ASSERT_TRUE(payload.get_varint32(&count).ok());
        ASSERT_TRUE(payload.skip_varints(count).ok());
    }
    uint8_t& count = bytes[offset + last.payload_offset + count_offset];
    ASSERT_EQ(count, 48U);
    count = 47U;
    reseal_frame(fixture, offset + last.offset, last.length);
}

// Moves the document count of `term`'s first raw frame by `delta`.
void corrupt_first_doc_count(Index& fixture, std::string_view term, int32_t delta) {
    uint64_t offset = 0;
    uint64_t length = 0;
    prx_range(fixture, term, &offset, &length);
    std::vector<uint8_t>& bytes = fixture.file.bytes();
    const auto frames = raw_frames(Slice(bytes.data() + offset, static_cast<size_t>(length)));
    ASSERT_FALSE(frames.empty());
    const RawFrame& first = frames.front();
    ByteSource payload(Slice(bytes.data() + offset + first.payload_offset, first.payload_length));
    uint32_t doc_count = 0;
    ASSERT_TRUE(payload.get_varint32(&doc_count).ok());
    ByteSink encoded;
    encoded.put_varint32(static_cast<uint32_t>(static_cast<int64_t>(doc_count) + delta));
    ASSERT_EQ(encoded.size(), payload.position());
    for (size_t byte = 0; byte < encoded.size(); ++byte) {
        bytes[offset + first.payload_offset + byte] = encoded.buffer()[byte];
    }
    reseal_frame(fixture, offset + first.offset, first.length);
}

TEST(SniiPhraseStreaming, HeavyExactPhraseStreamsEveryFrameItDecodes) {
    const Corpus docs = heavy_corpus();
    Index fixture(docs, /*prx_zstd_level=*/3);
    QueryProfile profile;
    std::vector<uint32_t> found;
    ASSERT_TRUE(phrase_query(fixture.index, {"alpha", "beta"}, &found, &profile).ok());
    EXPECT_EQ(found, every_doc(docs.size()));
    EXPECT_GT(profile.prx_decode_stats.streaming_frames, 0U);
    EXPECT_EQ(profile.prx_decode_stats.streaming_frames, profile.prx_decode_stats.frame_count());

    // Counting the phrase's occurrences reads every position, so its blocks decode whole.
    std::vector<PhraseMatch> matches;
    ASSERT_TRUE(phrase_query_with_frequencies(fixture.index, {"alpha", "beta"}, &matches, &profile)
                        .ok());
    ASSERT_EQ(matches.size(), docs.size());
    EXPECT_EQ(matches.front().frequency, 48.0F);
    EXPECT_EQ(profile.prx_decode_stats.streaming_frames, 0U);
    EXPECT_GT(profile.prx_decode_stats.frame_count(), 0U);
}

TEST(SniiPhraseStreaming, EarlyExactPhraseStreamsOnEveryCodec) {
    struct CodecCase {
        const char* name;
        int prx_zstd_level;
        uint32_t doc_count;
        format::PrxCodec codec;
    };
    const std::array<CodecCase, 3> cases = {
            CodecCase {.name = "RAW",
                       .prx_zstd_level = 0,
                       .doc_count = 6,
                       .codec = format::PrxCodec::kRaw},
            CodecCase {.name = "ZSTD",
                       .prx_zstd_level = -3,
                       .doc_count = 12,
                       .codec = format::PrxCodec::kZstd},
            CodecCase {.name = "PFOR",
                       .prx_zstd_level = 3,
                       .doc_count = 6,
                       .codec = format::PrxCodec::kPfor},
    };
    for (const CodecCase& codec_case : cases) {
        Index fixture(early_hit_corpus(codec_case.doc_count), codec_case.prx_zstd_level);
        QueryProfile profile;
        std::vector<uint32_t> found;
        ASSERT_TRUE(phrase_query(fixture.index, {"a", "b"}, &found, &profile).ok())
                << codec_case.name;
        EXPECT_EQ(found, every_doc(codec_case.doc_count)) << codec_case.name;
        const format::PrxDecodeStats& stats = profile.prx_decode_stats;
        EXPECT_GT(stats.streaming_frames, 0U) << codec_case.name;
        EXPECT_EQ(stats.streaming_frames, stats.frame_count()) << codec_case.name;
        EXPECT_EQ(codec_frames(stats, codec_case.codec), stats.frame_count()) << codec_case.name;
    }
}

TEST(SniiPhraseStreaming, LightOrRepeatedTermPhrasesDecodeWholeBlocks) {
    Index light(light_corpus(), /*prx_zstd_level=*/0);
    QueryProfile profile;
    std::vector<uint32_t> found;
    ASSERT_TRUE(phrase_query(light.index, {"lead", "quick"}, &found, &profile).ok());
    EXPECT_EQ(found, every_doc(128));
    EXPECT_EQ(profile.prx_decode_stats.streaming_frames, 0U);
    EXPECT_GT(profile.prx_decode_stats.frame_count(), 0U);

    const Corpus docs = early_hit_corpus(600);
    Index repeated(docs, /*prx_zstd_level=*/0);
    ASSERT_TRUE(phrase_query(repeated.index, {"a", "b", "a"}, &found, &profile).ok());
    EXPECT_EQ(found, every_doc(docs.size()));
    EXPECT_EQ(profile.prx_decode_stats.streaming_frames, 0U);
}

// Each listed row's document is the only one a streamed term decodes.
TEST(SniiPhraseStreaming, CandidatesStreamOnlyTheirDocuments) {
    const Corpus docs = early_hit_corpus(600);
    Index fixture(docs, /*prx_zstd_level=*/0);
    roaring::Roaring candidates;
    std::vector<uint32_t> expected;
    for (uint32_t doc = 0; doc < docs.size(); doc += 7) {
        candidates.add(doc);
        expected.push_back(doc);
    }
    QueryProfile profile;
    std::vector<uint32_t> found;
    ASSERT_TRUE(
            phrase_query(fixture.index, {"a", "b"}, &found, &profile, {.candidates = &candidates})
                    .ok());
    EXPECT_EQ(found, expected);
    EXPECT_GT(profile.prx_decode_stats.streaming_frames, 0U);
    EXPECT_EQ(profile.prx_decode_stats.selected_docs, 2 * expected.size());
}

// A light phrase decodes only the listed rows' documents of a block too, when they are fewer than
// half of it.
TEST(SniiPhraseStreaming, LightCandidatesDecodeOnlyTheirDocuments) {
    Index fixture(light_corpus(), /*prx_zstd_level=*/0);
    roaring::Roaring candidates;
    std::vector<uint32_t> expected;
    for (uint32_t doc = 0; doc < 128; doc += 7) {
        candidates.add(doc);
        expected.push_back(doc);
    }
    QueryProfile profile;
    std::vector<uint32_t> found;
    ASSERT_TRUE(phrase_query(fixture.index, {"lead", "quick"}, &found, &profile,
                             {.candidates = &candidates})
                        .ok());
    EXPECT_EQ(found, expected);
    EXPECT_EQ(profile.prx_decode_stats.streaming_frames, 0U);
    EXPECT_EQ(profile.prx_decode_stats.selected_docs, 2 * expected.size());
}

// A corrupt document after the early match still fails the phrase: the stream checks the rest of
// each frame once its last listed document is read.
TEST(SniiPhraseStreaming, CorruptionAfterTheMatchFailsThePhrase) {
    Index fixture(early_hit_corpus(600), /*prx_zstd_level=*/0);
    corrupt_last_document(fixture, "b");
    std::vector<uint32_t> found;
    const Status status = phrase_query(fixture.index, {"a", "b"}, &found);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_TRUE(found.empty());
}

TEST(SniiPhraseStreaming, FrameDocumentCountMismatchFailsThePhrase) {
    for (const int32_t delta : {-1, 1}) {
        Index fixture(early_hit_corpus(600), /*prx_zstd_level=*/0);
        corrupt_first_doc_count(fixture, "b", delta);
        std::vector<uint32_t> found;
        const Status status = phrase_query(fixture.index, {"a", "b"}, &found);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << delta << status;
        EXPECT_TRUE(found.empty()) << delta;
    }
}

} // namespace
} // namespace doris::snii::query
