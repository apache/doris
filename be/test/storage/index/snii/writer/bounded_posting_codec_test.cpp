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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <numeric>
#include <span>
#include <vector>

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/prx_frame.h"
#include "storage/index/snii/writer/encoded_spill_run.h"
#include "storage/index/snii/writer/posting_byte_buffer.h"
#include "storage/index/snii/writer/posting_prx_encoder.h"
#include "storage/index/snii/writer/spill_run_codec.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::writer {
namespace {

constexpr uint64_t kMiB = 1ULL << 20;

struct EncodedTemporaryFile {
    EncodedTemporaryFile() {
        static uint32_t sequence = 0;
        path = "/tmp/snii_bounded_codec_" + std::to_string(getpid()) + "_" +
               std::to_string(sequence++) + ".run";
    }
    ~EncodedTemporaryFile() { std::remove(path.c_str()); }
    std::string path;
};

Status consume_encoded_file(const std::string& path) {
    StreamingRunReader reader;
    RETURN_IF_ERROR(reader.open(path, true));
    while (!reader.exhausted()) {
        bool end = false;
        do {
            uint32_t doc = 0;
            uint32_t position = 0;
            RETURN_IF_ERROR(reader.next_token(&doc, &position, &end));
        } while (!end);
        RETURN_IF_ERROR(reader.advance());
    }
    return Status::OK();
}

TEST(SniiBoundedPostingCodec, FailedSpoolAppendKeepsEarlierSealedRuns) {
    EncodedTemporaryFile spool;
    uint64_t metadata_bytes = 0;
    MemoryReporter reporter(nullptr, 0, MemoryReporter::CapPolicy::kSpillThreshold, kMiB);
    {
        EncodedRunWriter writer(&reporter);
        ASSERT_TRUE(writer.open(spool.path).ok());
        ASSERT_TRUE(writer.close().ok());
        // close releases payload caches while the writer object remains alive.
        metadata_bytes = reporter.postings_current_bytes();
    }
    auto blocker = reporter.make_postings_reservation();
    ASSERT_TRUE(blocker.set_bytes(kMiB - metadata_bytes).ok());
    {
        EncodedRunWriter writer(&reporter);
        // Object admission succeeds; the first payload allocation must fail.
        EXPECT_FALSE(writer.open(spool.path, /*append=*/true).ok());
    }
    blocker.reset();
    EXPECT_EQ(reporter.current_bytes(), 0);
    EXPECT_TRUE(consume_encoded_file(spool.path).ok());
}

void write_small_terms(EncodedRunWriter* writer, uint32_t term_count) {
    TermPostings term;
    term.retain_positions = true;
    term.freqs = {2};
    term.positions_flat = {0, 1};
    for (uint32_t id = 0; id < term_count; ++id) {
        term.docids = {id};
        ASSERT_TRUE(writer->write_term(id, term).ok());
    }
}

void verify_small_term(StreamingRunReader* reader, uint32_t id) {
    ASSERT_FALSE(reader->exhausted());
    EXPECT_EQ(reader->current().term_id, id);
    for (uint32_t expected_position = 0; expected_position < 2; ++expected_position) {
        uint32_t doc = 0;
        uint32_t position = 0;
        bool end = false;
        ASSERT_TRUE(reader->next_token(&doc, &position, &end).ok());
        EXPECT_FALSE(end);
        EXPECT_EQ(doc, id);
        EXPECT_EQ(position, expected_position);
    }
    // advance also consumes the term terminator and checks its CRCs.
    ASSERT_TRUE(reader->advance().ok());
}

TEST(SniiBoundedPostingCodec, SmallTermsReuseSequentialRunReadAhead) {
    EncodedTemporaryFile spool;
    constexpr uint32_t term_count = 10'000;
    EncodedRunWriter writer;
    ASSERT_TRUE(writer.open(spool.path).ok());
    ASSERT_NO_FATAL_FAILURE(write_small_terms(&writer, term_count));
    ASSERT_TRUE(writer.close().ok());
    const uint64_t file_bytes = writer.file_offset();
    MemoryReporter reporter(nullptr, 0, MemoryReporter::CapPolicy::kSpillThreshold, kMiB);
    {
        StreamingRunReader reader(&reporter);
        ASSERT_TRUE(reader.open(spool.path, true).ok());
        for (uint32_t id = 0; id < term_count; ++id) {
            ASSERT_NO_FATAL_FAILURE(verify_small_term(&reader, id));
        }
        EXPECT_TRUE(reader.exhausted());
    }
    // A vocabulary of tiny postings must not reread a full cache per term.
    EXPECT_LE(reporter.postings_read_bytes(), 2 * file_bytes);
    EXPECT_LE(reporter.postings_peak_bytes(), kMiB);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

void verify_second_spool_range(const std::string& path, const std::array<uint64_t, 2>& offsets) {
    StreamingRunReader second;
    ASSERT_TRUE(second.open(path, true, false, offsets[0], offsets[1]).ok());
    uint32_t doc = 0;
    uint32_t position = 0;
    bool end = false;
    ASSERT_TRUE(second.next_token(&doc, &position, &end).ok());
    EXPECT_FALSE(end);
    EXPECT_EQ(doc, 7U);
    EXPECT_EQ(position, 1U);
    ASSERT_TRUE(second.next_token(&doc, &position, &end).ok());
    EXPECT_TRUE(end);
    ASSERT_TRUE(second.advance().ok());
    EXPECT_TRUE(second.exhausted());
    StreamingRunReader truncated;
    EXPECT_FALSE(truncated.open(path, true, false, offsets[0], offsets[1] - 1).ok());
}

void verify_boundary_spool_batch(const TermPostingBuffer& batch, bool exhausted) {
    EXPECT_TRUE(exhausted);
    EXPECT_EQ(batch.document_count(), 1U);
    EXPECT_EQ(batch.docids()[0], 7U);
    EXPECT_EQ(batch.freqs()[0], 2U);
    EXPECT_EQ(std::vector<uint32_t>(batch.positions_flat().begin(), batch.positions_flat().end()),
              (std::vector<uint32_t> {0, 1}));
}

void write_boundary_spool(const std::string& path, std::array<uint64_t, 2>* offsets) {
    TermPostings term;
    term.retain_positions = true;
    term.docids = {7};
    term.freqs = {1};
    for (size_t i = 0; i < offsets->size(); ++i) {
        term.positions_flat = {static_cast<uint32_t>(i)};
        EncodedRunWriter writer;
        ASSERT_TRUE(writer.open(path, /*append=*/true).ok());
        ASSERT_TRUE(writer.write_term(0, term).ok());
        ASSERT_TRUE(writer.close().ok());
        (*offsets)[i] = writer.file_offset();
    }
}

TEST(SniiBoundedPostingCodec, SealedSpoolRangesPreserveBoundaryDocuments) {
    EncodedTemporaryFile spool;
    std::array<uint64_t, 2> offsets {};
    ASSERT_NO_FATAL_FAILURE(write_boundary_spool(spool.path, &offsets));
    ASSERT_NO_FATAL_FAILURE(verify_second_spool_range(spool.path, offsets));

    PostingByteBuffer directory;
    for (uint64_t offset : offsets) {
        std::array<uint8_t, 12> record {};
        std::memcpy(record.data(), &offset, sizeof(offset));
        const uint32_t crc = crc32c(Slice(record.data(), sizeof(offset)));
        std::memcpy(record.data() + sizeof(offset), &crc, sizeof(crc));
        ASSERT_TRUE(directory.append(record).ok());
    }
    uint32_t seen = 0;
    const Status merged = merge_spooled_run_sources(
            spool.path, &directory, 2, {"hot"}, {0}, true,
            [&](StreamedTermPostings&& posting) {
                TermPostingBuffer batch(nullptr);
                bool exhausted = false;
                RETURN_IF_ERROR(posting.source->fill(2, &batch, &exhausted));
                verify_boundary_spool_batch(batch, exhausted);
                ++seen;
                return Status::OK();
            },
            nullptr, 2);
    ASSERT_TRUE(merged.ok()) << merged;
    EXPECT_EQ(seen, 1U);

    PostingByteBuffer damaged;
    const std::array<uint8_t, 12> invalid_crc {};
    ASSERT_TRUE(damaged.append(invalid_crc).ok());
    EXPECT_FALSE(merge_spooled_run_sources(
                         spool.path, &damaged, 1, {"hot"}, {0}, true,
                         [](StreamedTermPostings&&) {
                             ADD_FAILURE();
                             return Status::OK();
                         },
                         nullptr, 2)
                         .ok());
}

TEST(SniiBoundedPostingCodec, RejectsTruncatedBlocksAndBadChecksums) {
    EncodedTemporaryFile original;
    EncodedTemporaryFile damaged;
    TermPostings term;
    term.retain_positions = true;
    term.docids = {0, 9};
    term.freqs = {2, 1};
    term.positions_flat = {0, 7, 3};
    EncodedRunWriter writer;
    ASSERT_TRUE(writer.open(original.path).ok());
    ASSERT_TRUE(writer.write_term(0, term).ok());
    ASSERT_TRUE(writer.close().ok());
    ASSERT_TRUE(consume_encoded_file(original.path).ok());
    std::ifstream input(original.path, std::ios::binary);
    const std::vector<char> bytes((std::istreambuf_iterator<char>(input)), {});
    for (size_t length = 0; length < bytes.size(); ++length) {
        std::ofstream truncated(damaged.path, std::ios::binary | std::ios::trunc);
        truncated.write(bytes.data(), length);
        truncated.close();
        EXPECT_FALSE(consume_encoded_file(damaged.path).ok()) << "truncated at " << length;
    }
    auto corrupt = bytes;
    ASSERT_GT(corrupt.size(), 6);
    corrupt[corrupt.size() - 11] ^= 1; // CRC before two terminators and the eight-byte seal.
    std::ofstream output(damaged.path, std::ios::binary | std::ios::trunc);
    output.write(corrupt.data(), corrupt.size());
    output.close();
    EXPECT_FALSE(consume_encoded_file(damaged.path).ok());
    EncodedTemporaryFile copied;
    EXPECT_FALSE(compact_runs({damaged.path}, {0}, true, copied.path).ok());
    for (size_t field = 8; field < 12; ++field) {
        corrupt = bytes;
        corrupt[field] ^= 1;
        std::ofstream bad_header(damaged.path, std::ios::binary | std::ios::trunc);
        bad_header.write(corrupt.data(), corrupt.size());
        bad_header.close();
        EXPECT_FALSE(consume_encoded_file(damaged.path).ok()) << "header field " << field;
    }
}

TEST(SniiBoundedPostingCodec, ReadsLegacyStatlessRunsWithoutMaterializingDocids) {
    EncodedTemporaryFile file;
    // Historical shape 0 has no frequency section; today's fixture writer emits shape 1.
    constexpr std::array<uint8_t, 15> kStatlessRun = {0, 0, 3, 0, 0, 0, 0, 3, 0, 0, 0, 9, 0, 0, 0};
    std::ofstream output(file.path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(kStatlessRun.data()), kStatlessRun.size());
    output.close();
    ASSERT_TRUE(output.good());
    StreamingRunReader reader;
    ASSERT_TRUE(reader.open(file.path, false, /*allow_legacy=*/true).ok());
    bool end = false;
    for (uint32_t expected : {0, 3, 9}) {
        uint32_t doc = 0;
        uint32_t position = 0;
        ASSERT_TRUE(reader.next_token(&doc, &position, &end).ok());
        EXPECT_FALSE(end);
        EXPECT_EQ(doc, expected);
    }
    uint32_t doc = 0;
    uint32_t position = 0;
    ASSERT_TRUE(reader.next_token(&doc, &position, &end).ok());
    EXPECT_TRUE(end);
    ASSERT_TRUE(reader.advance().ok());
    EXPECT_TRUE(reader.exhausted());
}

TEST(SniiBoundedPostingCodec, EmptyTermHasNoEmptyFragment) {
    EncodedTemporaryFile original;
    EncodedTemporaryFile compacted;
    TermPostings empty;
    empty.retain_positions = true;
    EncodedRunWriter writer;
    ASSERT_TRUE(writer.open(original.path).ok());
    ASSERT_TRUE(writer.write_term(0, empty).ok());
    ASSERT_TRUE(writer.close().ok());
    ASSERT_TRUE(consume_encoded_file(original.path).ok());
    ASSERT_TRUE(compact_runs({original.path}, {0}, true, compacted.path).ok());
    EXPECT_TRUE(consume_encoded_file(compacted.path).ok());
}

void verify_merged_boundary_document(const TermPostingBuffer& buffer, uint32_t next_doc,
                                     uint32_t run_count) {
    EXPECT_EQ(buffer.docids()[0], next_doc);
    const bool boundary = next_doc == 0 || next_doc == run_count;
    EXPECT_EQ(buffer.freqs()[0], boundary ? 1 : 2);
    EXPECT_EQ(buffer.positions_flat()[0], next_doc == 0 ? 2 : 1);
    if (!boundary) {
        EXPECT_EQ(buffer.positions_flat()[1], 2);
    }
}

void write_boundary_runs(std::span<const EncodedTemporaryFile> files,
                         std::vector<std::string>* paths) {
    for (uint32_t run = 0; run < files.size(); ++run) {
        TermPostings term;
        term.retain_positions = true;
        term.docids = {run, run + 1};
        term.freqs = {1, 1};
        term.positions_flat = {2, 1};
        EncodedRunWriter writer;
        ASSERT_TRUE(writer.open(files[run].path).ok());
        ASSERT_TRUE(writer.write_term(0, term).ok());
        ASSERT_TRUE(writer.close().ok());
        paths->push_back(files[run].path);
    }
}

TEST(SniiBoundedPostingCodec, MultiPassMergeCoalescesBoundaryDocumentsInRunOrder) {
    constexpr uint32_t kRuns = 55;
    std::array<EncodedTemporaryFile, kRuns> files;
    std::vector<std::string> paths;
    ASSERT_NO_FATAL_FAILURE(write_boundary_runs(files, &paths));
    MemoryReporter reporter(nullptr, kMiB, MemoryReporter::CapPolicy::kHardLimit, kMiB);
    uint32_t next_doc = 0;
    Status merged = merge_run_sources(
            paths, {"term"}, {0}, true,
            [&](StreamedTermPostings&& term) {
                TermPostingBuffer buffer(&reporter);
                bool end = false;
                while (!end) {
                    buffer.clear_reuse();
                    RETURN_IF_ERROR(term.source->fill(1, &buffer, &end));
                    if (buffer.empty()) {
                        break;
                    }
                    verify_merged_boundary_document(buffer, next_doc, kRuns);
                    ++next_doc;
                }
                return Status::OK();
            },
            &reporter);
    ASSERT_TRUE(merged.ok()) << merged.to_string();
    EXPECT_EQ(next_doc, kRuns + 1);
    EXPECT_EQ(reporter.current_bytes(), 0);
    EXPECT_LE(reporter.postings_peak_bytes(), kMiB);
    for (const auto& path : paths) {
        EXPECT_EQ(::access(path.c_str(), F_OK), 0);
    }
}

TEST(SniiBoundedPostingCodec, OrdinaryPositionWindowsKeepResidentEncodingWithoutTemporaryIO) {
    const std::vector<uint32_t> docs(8192, 0);
    const std::vector<uint32_t> freqs(8192, 10);
    std::vector<uint32_t> positions(81920);
    for (size_t i = 0; i < positions.size(); ++i) {
        positions[i] = i % 10;
    }
    MemoryReporter reporter(nullptr, 32 * kMiB);
    TermPostingBuffer source(&reporter);
    ASSERT_TRUE(source.append(docs, freqs, positions).ok());
    ASSERT_FALSE(source.positions_spooled());
    for (int level : {0, -1, 3}) {
        PostingPrxEncoder encoder(&reporter);
        format::PrxWindowBuildOutcome outcome;
        ASSERT_TRUE(encoder.build({source.positions_flat(), nullptr, 0, positions.size()}, freqs,
                                  level, format::kReaderPrxWindowLimits, &outcome)
                            .ok());
        ASSERT_TRUE(encoder.resident());
        ByteSink expected;
        ASSERT_TRUE(format::build_prx_window_flat(positions, freqs, level, &expected).ok());
        const Slice actual = encoder.resident_bytes();
        EXPECT_EQ(std::vector<uint8_t>(actual.data(), actual.data() + actual.size()),
                  expected.buffer());
    }
    EXPECT_EQ(reporter.postings_read_bytes(), 0);
    EXPECT_EQ(reporter.postings_written_bytes(), 0);
}

TEST(SniiBoundedPostingCodec, SplitRequestStillRejectsAnUnrepresentableOrUnsortedDocument) {
    MemoryReporter reporter(nullptr, kMiB);
    PostingPrxEncoder encoder(&reporter);
    format::PrxWindowBuildOutcome outcome;
    const std::array<uint32_t, 2> freqs {3, 1};
    std::array<uint32_t, 4> positions {0, 1, 2, 0};
    const format::PrxWindowLimits limits {
            .max_docs = 1, .max_positions = 4, .max_uncomp_bytes = 100};
    ASSERT_TRUE(encoder.build({positions, nullptr, 0, 4}, freqs, 0, limits, &outcome).ok());
    EXPECT_EQ(outcome, format::PrxWindowBuildOutcome::kNeedsSplit);
    EXPECT_FALSE(encoder.build({positions, nullptr, 0, 4}, freqs, 0, {1, 2, 100}, &outcome).ok());
    EXPECT_FALSE(encoder.build({positions, nullptr, 0, 4}, freqs, 0, {1, 4, 4}, &outcome).ok());
    positions[1] = 3;
    EXPECT_FALSE(encoder.build({positions, nullptr, 0, 4}, freqs, 0, limits, &outcome).ok());
}

void verify_byte_cursor_replay(PostingByteCursor* cursor, std::span<const uint8_t> pattern) {
    uint64_t offset = 65530;
    while (cursor->remaining() != 0) {
        std::span<const uint8_t> part;
        ASSERT_TRUE(cursor->next_span(&part).ok());
        for (uint8_t value : part) {
            EXPECT_EQ(value, pattern[offset++ % pattern.size()]);
        }
    }
}

void build_spilled_byte_fixture(PostingByteBuffer* bytes, std::array<uint8_t, 4096>* pattern) {
    for (size_t i = 0; i < pattern->size(); ++i) {
        (*pattern)[i] = static_cast<uint8_t>(i * 37);
    }
    for (size_t i = 0; i < 1280; ++i) {
        ASSERT_TRUE(bytes->append(*pattern).ok());
    }
    ASSERT_TRUE(bytes->spilled());
    ASSERT_EQ(bytes->size(), 5 * kMiB);
    ASSERT_TRUE(bytes->spill_and_release_buffer().ok());
}

void verify_byte_buffer_lifetime(MemoryReporter* reporter) {
    PostingByteBuffer bytes(reporter);
    std::array<uint8_t, 4096> pattern {};
    ASSERT_NO_FATAL_FAILURE(build_spilled_byte_fixture(&bytes, &pattern));
    // Only the temp-path descriptor remains; the payload cache is released.
    EXPECT_GT(reporter->postings_current_bytes(), 0);
    EXPECT_LT(reporter->postings_current_bytes(), 4096);
    PostingByteCursor cursor(&bytes);
    ASSERT_TRUE(cursor.reset(65530, 8193).ok());
    ASSERT_NO_FATAL_FAILURE(verify_byte_cursor_replay(&cursor, pattern));
    std::array<uint8_t, 2> invalid;
    EXPECT_FALSE(bytes.read_at(bytes.size() - 1, invalid).ok());
}

TEST(SniiBoundedPostingCodec, ByteBufferReplaysAcrossBlocksAndReleasesItsCache) {
    MemoryReporter reporter(nullptr, 256 * 1024, MemoryReporter::CapPolicy::kHardLimit, 256 * 1024);
    ASSERT_NO_FATAL_FAILURE(verify_byte_buffer_lifetime(&reporter));
    EXPECT_LE(reporter.postings_peak_bytes(), 256 * 1024);
    EXPECT_EQ(reporter.postings_written_bytes(), 5 * kMiB);
    EXPECT_GT(reporter.postings_read_bytes(), 0);
    EXPECT_EQ(reporter.current_bytes(), 0);
    EXPECT_EQ(reporter.postings_current_bytes(), 0);
}

TEST(SniiBoundedPostingCodec, SmallWindowsKeepTheExistingBytes) {
    const std::array<uint32_t, 3> frequencies {1, 3, 2};
    const std::array<uint32_t, 6> positions {7, 0, 1, 19, 4, 8};
    for (int level : {0, -1, -3, 3}) {
        MemoryReporter reporter(nullptr, kMiB);
        PostingPrxEncoder encoder(&reporter);
        format::PrxWindowBuildOutcome outcome;
        const Status built = encoder.build(
                {.flat = positions, .buffer = nullptr, .offset = 0, .count = positions.size()},
                frequencies, level, format::kReaderPrxWindowLimits, &outcome);
        ASSERT_TRUE(built.ok()) << built.to_string();
        ASSERT_EQ(outcome, format::PrxWindowBuildOutcome::kBuilt);
        ByteSink reference;
        ASSERT_TRUE(format::build_prx_window_flat(positions, frequencies, level, &reference).ok());
        ASSERT_TRUE(encoder.resident());
        const Slice actual = encoder.resident_bytes();
        EXPECT_EQ(std::vector<uint8_t>(actual.data(), actual.data() + actual.size()),
                  reference.buffer());
    }
}

void verify_large_position_frame(MemoryReporter* reporter, const std::vector<uint32_t>& docs,
                                 const std::vector<uint32_t>& frequencies,
                                 const std::vector<uint32_t>& positions, int level) {
    TermPostingBuffer source(reporter);
    ASSERT_TRUE(source.append(docs, frequencies, positions).ok());
    ASSERT_TRUE(source.positions_spooled());
    PostingPrxEncoder encoder(reporter);
    format::PrxWindowBuildOutcome outcome;
    const Status built =
            encoder.build({.flat = {}, .buffer = &source, .offset = 0, .count = positions.size()},
                          frequencies, level, format::kReaderPrxWindowLimits, &outcome);
    ASSERT_TRUE(built.ok()) << built.to_string();
    ASSERT_EQ(outcome, format::PrxWindowBuildOutcome::kBuilt);
    std::vector<uint8_t> encoded;
    ASSERT_TRUE(encoder.visit_bytes([&](Slice bytes) {
                           encoded.insert(encoded.end(), bytes.data(), bytes.data() + bytes.size());
                           return Status::OK();
                       })
                        .ok());
    ByteSource input {Slice(encoded)};
    std::vector<uint32_t> decoded;
    std::vector<uint32_t> offsets;
    ASSERT_TRUE(format::read_prx_window_csr(&input, &decoded, &offsets).ok());
    EXPECT_TRUE(input.eof());
    EXPECT_EQ(decoded, positions);
    ASSERT_EQ(offsets.size(), docs.size() + 1);
    for (size_t doc = 0; doc <= docs.size(); ++doc) {
        EXPECT_EQ(offsets[doc], doc * frequencies.front());
    }
    EXPECT_LE(reporter->postings_peak_bytes(), 8 * kMiB);
}

TEST(SniiBoundedPostingCodec, LargePositionFramesDecodeWithTheExistingReader) {
    constexpr size_t kDocs = 8;
    constexpr uint32_t kFrequency = 100'000;
    std::vector<uint32_t> docs(kDocs);
    std::iota(docs.begin(), docs.end(), 0);
    const std::vector<uint32_t> frequencies(kDocs, kFrequency);
    std::vector<uint32_t> positions(kDocs * kFrequency);
    for (size_t doc = 0; doc < kDocs; ++doc) {
        for (uint32_t position = 0; position < kFrequency; ++position) {
            positions[doc * kFrequency + position] = position;
        }
    }
    for (int level : {0, -1, -3, 3}) {
        MemoryReporter reporter(nullptr, 8 * kMiB, MemoryReporter::CapPolicy::kHardLimit, 8 * kMiB);
        ASSERT_NO_FATAL_FAILURE(
                verify_large_position_frame(&reporter, docs, frequencies, positions, level));
        EXPECT_EQ(reporter.current_bytes(), 0);
        EXPECT_EQ(reporter.postings_current_bytes(), 0);
    }
}

Status encode_high_level_positions(MemoryReporter* reporter, const std::vector<uint32_t>& positions,
                                   std::vector<uint32_t>* decoded) {
    const std::array<uint32_t, 1> frequency {static_cast<uint32_t>(positions.size())};
    PostingPrxEncoder encoder(reporter);
    format::PrxWindowBuildOutcome outcome;
    RETURN_IF_ERROR(encoder.build(
            {.flat = positions, .buffer = nullptr, .offset = 0, .count = positions.size()},
            frequency, 19, format::kReaderPrxWindowLimits, &outcome));
    if (outcome != format::PrxWindowBuildOutcome::kBuilt) {
        return Status::InternalError("high-level frame unexpectedly needs splitting");
    }
    std::vector<uint8_t> encoded;
    RETURN_IF_ERROR(encoder.visit_bytes([&](Slice bytes) {
        encoded.insert(encoded.end(), bytes.data(), bytes.data() + bytes.size());
        return Status::OK();
    }));
    ByteSource input {Slice(encoded)};
    std::vector<uint32_t> offsets;
    return format::read_prx_window_csr(&input, decoded, &offsets);
}

TEST(SniiBoundedPostingCodec, HighCompressionLevelUsesAnExplicitlyLargerWorkspace) {
    std::vector<uint32_t> positions(4'000'000);
    std::iota(positions.begin(), positions.end(), 0);
    for (uint64_t budget : {32 * kMiB, 128 * kMiB}) {
        MemoryReporter reporter(nullptr, 0, MemoryReporter::CapPolicy::kHardLimit, budget);
        std::vector<uint32_t> decoded;
        const Status status = encode_high_level_positions(&reporter, positions, &decoded);
        if (budget == 32 * kMiB) {
            EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
        } else {
            ASSERT_TRUE(status.ok()) << status;
            EXPECT_EQ(decoded, positions);
        }
        EXPECT_LE(reporter.postings_peak_bytes(), budget);
        EXPECT_EQ(reporter.current_bytes(), 0);
    }
}

TEST(SniiBoundedPostingCodec, StreamedInlineDictionaryKeepsBytesAndAnchorOffsets) {
    using namespace format;
    DictBlockBuilder resident(IndexTier::kT2, true, 0, 0, 2);
    DictBlockBuilder streamed(IndexTier::kT2, true, 0, 0, 2);
    std::vector<std::vector<uint8_t>> payloads(5);
    std::vector<uint64_t> lengths(5, 0);
    for (size_t i = 0; i < payloads.size(); ++i) {
        DictEntry entry;
        entry.term = std::string(1, static_cast<char>('a' + i));
        entry.kind = DictEntryKind::kInline;
        entry.enc = DictEntryEnc::kSlim;
        entry.df = 1;
        entry.frq_bytes = {1, 2, 3};
        entry.dd_meta.uncomp_len = 3;
        entry.prx_bytes.resize(i % 2 == 0 ? 100'000 : 7);
        for (size_t offset = 0; offset < entry.prx_bytes.size(); ++offset) {
            entry.prx_bytes[offset] = static_cast<uint8_t>(offset * 17 + i);
        }
        resident.add_entry(entry);
        if (i % 2 == 0) {
            payloads[i] = std::move(entry.prx_bytes);
            lengths[i] = payloads[i].size();
        }
        streamed.add_entry(std::move(entry), lengths[i]);
    }
    EXPECT_EQ(streamed.estimated_bytes(), resident.estimated_bytes());
    std::vector<uint8_t> actual;
    const Status encoded = streamed.finish_streamed(
            lengths,
            [&](uint32_t entry, const std::function<Status(Slice)>& append) {
                const auto& payload = payloads[entry];
                for (size_t offset = 0; offset < payload.size(); offset += 4096) {
                    RETURN_IF_ERROR(append(Slice(payload.data() + offset,
                                                 std::min<size_t>(4096, payload.size() - offset))));
                }
                return Status::OK();
            },
            [&](Slice bytes) {
                actual.insert(actual.end(), bytes.data(), bytes.data() + bytes.size());
                return Status::OK();
            });
    ASSERT_TRUE(encoded.ok()) << encoded.to_string();
    EXPECT_EQ(actual, resident.finish_owned());
}

void verify_sorted_posting_batch(const TermPostingBuffer& postings, uint32_t* next_doc) {
    for (size_t doc = 0; doc < postings.document_count(); ++doc) {
        EXPECT_EQ(postings.docids()[doc], (*next_doc)++);
        EXPECT_EQ(postings.freqs()[doc], 2);
        EXPECT_EQ(postings.positions_flat()[2 * doc], 3);
        EXPECT_EQ(postings.positions_flat()[2 * doc + 1], 4);
    }
}

TEST(SniiBoundedPostingCodec, OutOfOrderCompatibilityInputUsesBoundedStableSort) {
    constexpr uint32_t kDocs = 100'000;
    MemoryReporter reporter(nullptr, 4 * kMiB, MemoryReporter::CapPolicy::kHardLimit, kMiB);
    {
        SpimiTermBuffer buffer(true, 0, &reporter);
        for (uint32_t doc = kDocs; doc != 0; --doc) {
            buffer.add_token("term", doc - 1, 3);
        }
        for (uint32_t doc = kDocs; doc != 0; --doc) {
            buffer.add_token("term", doc - 1, 4);
        }
        ASSERT_TRUE(buffer.status().ok());
        uint32_t next_doc = 0;
        const Status drained = buffer.for_each_term_sorted([&](StreamedTermPostings&& term) {
            TermPostingBuffer postings(&reporter);
            bool end = false;
            while (!end) {
                postings.clear_reuse();
                RETURN_IF_ERROR(term.source->fill(256, &postings, &end));
                verify_sorted_posting_batch(postings, &next_doc);
            }
            return Status::OK();
        });
        ASSERT_TRUE(drained.ok()) << drained.to_string();
        EXPECT_EQ(next_doc, kDocs);
        EXPECT_LE(reporter.postings_peak_bytes(), kMiB);
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiBoundedPostingCodec, WorkspaceReservationsRollbackAndShareTheRootCounter) {
    int64_t observed = 0;
    MemoryReporter reporter([&](int64_t delta) { observed += delta; }, 1024,
                            MemoryReporter::CapPolicy::kHardLimit, 512);
    auto arena = reporter.make_reservation();
    auto first = reporter.make_postings_reservation();
    auto second = reporter.make_postings_reservation();
    ASSERT_TRUE(arena.set_bytes(600).ok());
    ASSERT_TRUE(first.set_bytes(400).ok());
    EXPECT_FALSE(second.set_bytes(100).ok());
    EXPECT_EQ(reporter.postings_current_bytes(), 400);
    EXPECT_EQ(observed, 1000);
    arena.reset();
    ASSERT_TRUE(second.set_bytes(100).ok());
    EXPECT_FALSE(first.set_bytes(450).ok());
    EXPECT_EQ(first.bytes(), 400);
    EXPECT_EQ(reporter.postings_current_bytes(), 500);
    first.reset();
    second.reset();
    EXPECT_EQ(observed, 0);
    EXPECT_EQ(reporter.postings_peak_bytes(), 500);
}

} // namespace
} // namespace doris::snii::writer
