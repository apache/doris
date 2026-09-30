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
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <numeric>
#include <string>
#include <vector>

#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spill_run_codec.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::writer {
namespace {

constexpr uint64_t kMiB = 1ULL << 20;

class TemporaryPostingFile {
public:
    TemporaryPostingFile() {
        static std::atomic<uint64_t> sequence {0};
        path = "/tmp/snii_spill_memory_" + std::to_string(getpid()) + "_" +
               std::to_string(sequence.fetch_add(1));
    }
    ~TemporaryPostingFile() { std::remove(path.c_str()); }

    std::string path;
};

Status consume_dense_postings(StreamedTermPostings&& term, uint32_t expected_docs) {
    TermPostingBuffer batch(nullptr);
    bool exhausted = false;
    uint32_t seen = 0;
    while (!exhausted) {
        batch.clear_reuse();
        RETURN_IF_ERROR(term.source->fill(256, &batch, &exhausted));
        for (size_t i = 0; i < batch.document_count(); ++i) {
            EXPECT_EQ(batch.docids()[i], seen++);
            EXPECT_EQ(batch.freqs()[i], 1);
            EXPECT_EQ(batch.positions_flat()[i], 3);
        }
    }
    EXPECT_EQ(seen, expected_docs);
    return Status::OK();
}

TEST(SniiSpillMemory, SpillDoesNotExpandTheCompleteHighFrequencyTerm) {
    // The compact arena fits comfortably, but three uint32 arrays do not.
    constexpr uint32_t kDocs = 2'000'000;
    const std::vector<std::string> vocabulary {"hot"};
    MemoryReporter reporter(nullptr, 16 * kMiB);
    {
        SpimiTermBuffer buffer(&vocabulary, true, 0, &reporter);
        for (uint32_t doc = 0; doc < kDocs; ++doc) {
            buffer.add_token(0, doc, 3);
        }
        ASSERT_TRUE(buffer.status().ok()) << buffer.status().to_string();
        ASSERT_EQ(buffer.run_count_for_test(), 0);
        const Status spilled = buffer.spill_to_run();
        ASSERT_TRUE(spilled.ok()) << spilled.to_string();
        ASSERT_EQ(buffer.arena_bytes_for_test(), 0);
        ASSERT_EQ(buffer.run_count_for_test(), 1);
        const Status drained = buffer.for_each_term_sorted([](StreamedTermPostings&& term) {
            return consume_dense_postings(std::move(term), kDocs);
        });
        ASSERT_TRUE(drained.ok()) << drained.to_string();
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

void write_dense_run(const std::string& path, uint32_t first, uint32_t count) {
    // Fixture construction is outside the measured merge workspace.
    TermPostings postings;
    postings.docids.resize(count);
    std::iota(postings.docids.begin(), postings.docids.end(), first);
    postings.freqs.assign(count, 1);
    postings.positions_flat.assign(count, 3);
    postings.retain_positions = true;
    RunWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    ASSERT_TRUE(writer.write_term(0, postings).ok());
    ASSERT_TRUE(writer.close().ok());
}

TEST(SniiSpillMemory, FinalMergeDoesNotLoadWholeTermsFromEachRun) {
    constexpr uint32_t kRunDocs = 250'000;
    TemporaryPostingFile first;
    TemporaryPostingFile second;
    write_dense_run(first.path, 0, kRunDocs);
    write_dense_run(second.path, kRunDocs, kRunDocs);
    const std::vector<std::string> paths {first.path, second.path};
    const std::vector<std::string> vocabulary {"hot"};
    const std::vector<uint32_t> ranks {0};
    MemoryReporter reporter(nullptr, 2 * kMiB);
    const Status merged = merge_run_sources(
            paths, vocabulary, ranks, true,
            [](StreamedTermPostings&& term) {
                return consume_dense_postings(std::move(term), 2 * kRunDocs);
            },
            &reporter, /*allow_legacy=*/true);
    ASSERT_TRUE(merged.ok()) << merged.to_string();
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiSpillMemory, IntermediateMergeDoesNotMaterializeMergedPostings) {
    constexpr uint32_t kRunDocs = 250'000;
    TemporaryPostingFile first;
    TemporaryPostingFile second;
    TemporaryPostingFile output;
    write_dense_run(first.path, 0, kRunDocs);
    write_dense_run(second.path, kRunDocs, kRunDocs);
    const std::vector<std::string> paths {first.path, second.path};
    const std::vector<uint32_t> ranks {0};
    MemoryReporter reporter(nullptr, 2 * kMiB);
    const Status compacted =
            compact_runs(paths, ranks, true, output.path, &reporter, /*allow_legacy=*/true);
    ASSERT_TRUE(compacted.ok()) << compacted.to_string();
    EXPECT_EQ(reporter.current_bytes(), 0);
    const Status merged = merge_run_sources(
            {output.path}, {"hot"}, ranks, true,
            [](StreamedTermPostings&& term) {
                return consume_dense_postings(std::move(term), 2 * kRunDocs);
            },
            &reporter, /*allow_legacy=*/true);
    ASSERT_TRUE(merged.ok()) << merged.to_string();
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiSpillMemory, OneLongDocumentDoesNotRequireResidentPositions) {
    // The caller-owned source is deliberately larger than the entire writer
    // workspace. It must remain legal without copying all positions at once.
    constexpr uint32_t kPositions = 4'000'000;
    std::vector<uint32_t> positions(kPositions);
    std::iota(positions.begin(), positions.end(), 0);
    const std::vector<uint32_t> docids {0};
    const std::vector<uint32_t> frequencies {kPositions};
    TemporaryPostingFile file;
    MemoryReporter reporter(nullptr, 8 * kMiB);
    {
        io::LocalFileWriter output;
        ASSERT_TRUE(output.open(file.path).ok());
        SniiCompoundWriter compound(&output);
        SniiIndexInput input;
        input.index_id = 1;
        input.config = format::IndexConfig::kDocsPositions;
        input.doc_count = 1;
        input.mem_reporter = &reporter;
        SniiStreamedIndexSession* session = nullptr;
        ASSERT_TRUE(compound.begin_streamed_index(std::move(input), &session).ok());
        SpanTermPostingSource source(docids, frequencies, positions);
        const Status pushed = session->push_term(
                StreamedTermPostings {.term = "long", .retain_positions = true, .source = &source});
        ASSERT_TRUE(pushed.ok()) << pushed.to_string();
        ASSERT_TRUE(session->finish().ok());
        ASSERT_TRUE(compound.finish().ok());
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

void verify_inline_positions(reader::LogicalIndexReader* index, const char* term,
                             const std::vector<uint32_t>& expected) {
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    ASSERT_TRUE(index->lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    ASSERT_TRUE(found);
    ASSERT_EQ(entry.df, 1);
    ByteSource bytes {Slice(entry.prx_bytes)};
    std::vector<uint32_t> decoded;
    std::vector<uint32_t> offsets;
    ASSERT_TRUE(format::read_prx_window_csr(&bytes, &decoded, &offsets).ok());
    EXPECT_EQ(decoded, expected);
    EXPECT_TRUE(bytes.eof());
}

Status write_large_inline_terms(const std::string& path, MemoryReporter* reporter,
                                const std::vector<uint32_t>& positions) {
    const std::vector<uint32_t> docids {0};
    const std::vector<uint32_t> frequencies {static_cast<uint32_t>(positions.size())};
    io::LocalFileWriter output;
    RETURN_IF_ERROR(output.open(path));
    SniiCompoundWriter compound(&output);
    SniiIndexInput input;
    input.index_id = 1;
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 1;
    input.mem_reporter = reporter;
    input.prx_zstd_level = 0;
    input.target_dict_block_bytes = 64 * kMiB;
    SniiStreamedIndexSession* session = nullptr;
    RETURN_IF_ERROR(compound.begin_streamed_index(std::move(input), &session));
    for (const char* term : {"a", "b", "c"}) {
        SpanTermPostingSource source(docids, frequencies, positions);
        RETURN_IF_ERROR(session->push_term(
                StreamedTermPostings {.term = term, .retain_positions = true, .source = &source}));
    }
    RETURN_IF_ERROR(session->finish());
    return compound.finish();
}

TEST(SniiSpillMemory, SeveralLargeInlinePayloadsShareOneBoundedDictionaryBlock) {
    constexpr uint32_t kPositions = 4'000'000;
    std::vector<uint32_t> positions(kPositions);
    std::iota(positions.begin(), positions.end(), 0);
    TemporaryPostingFile file;
    MemoryReporter reporter(nullptr, 8 * kMiB, MemoryReporter::CapPolicy::kHardLimit, 8 * kMiB);
    const Status written = write_large_inline_terms(file.path, &reporter, positions);
    ASSERT_TRUE(written.ok()) << written.to_string();
    EXPECT_EQ(reporter.current_bytes(), 0);
    EXPECT_LE(reporter.postings_peak_bytes(), 8 * kMiB);
    io::LocalFileReader input;
    ASSERT_TRUE(input.open(file.path).ok());
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&input, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "", &index).ok());
    for (const char* term : {"a", "b", "c"}) {
        ASSERT_NO_FATAL_FAILURE(verify_inline_positions(&index, term, positions));
    }
}

Status consume_pair_postings(StreamedTermPostings&& term, MemoryReporter* reporter,
                             uint32_t* seen) {
    TermPostingBuffer batch(reporter);
    bool exhausted = false;
    while (!exhausted) {
        batch.clear_reuse();
        RETURN_IF_ERROR(term.source->fill(256, &batch, &exhausted));
        for (size_t i = 0; i < batch.document_count(); ++i) {
            std::array<uint32_t, 2> positions {};
            RETURN_IF_ERROR(batch.read_positions(i * 2, positions));
            if (batch.docids()[i] != (*seen)++ || batch.freqs()[i] != 2 ||
                positions != std::array<uint32_t, 2> {0, 1}) {
                return Status::InternalError("spooled boundary document differs at row {}",
                                             *seen - 1);
            }
        }
    }
    return Status::OK();
}

Status write_many_inline_terms(const std::string& path, MemoryReporter* reporter) {
    std::vector<uint32_t> positions(1024);
    std::iota(positions.begin(), positions.end(), 0);
    const std::array<uint32_t, 1> docs {0};
    const std::array<uint32_t, 1> frequencies {1024};
    io::LocalFileWriter output;
    RETURN_IF_ERROR(output.open(path));
    SniiCompoundWriter compound(&output);
    SniiIndexInput input;
    input.index_id = 1;
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 1;
    input.mem_reporter = reporter;
    input.prx_zstd_level = 0;
    input.target_dict_block_bytes = 64 * kMiB;
    SniiStreamedIndexSession* session = nullptr;
    RETURN_IF_ERROR(compound.begin_streamed_index(std::move(input), &session));
    for (uint32_t term = 100'000; term < 110'000; ++term) {
        const std::string text = std::to_string(term);
        SpanTermPostingSource source(docs, frequencies, positions);
        RETURN_IF_ERROR(session->push_term(
                StreamedTermPostings {.term = text, .retain_positions = true, .source = &source}));
    }
    RETURN_IF_ERROR(session->finish());
    return compound.finish();
}

TEST(SniiSpillMemory, SmallInlinePayloadsCannotAccumulateOutsideThePostingBudget) {
    TemporaryPostingFile file;
    MemoryReporter reporter(nullptr, 0, MemoryReporter::CapPolicy::kSpillThreshold, 8 * kMiB);
    ASSERT_TRUE(write_many_inline_terms(file.path, &reporter).ok());
    // RAW inline PRX bytes alone exceed B, even though each term fits easily.
    // The dictionary must stage them within the posting budget before closing.
    EXPECT_GT(reporter.postings_written_bytes(), 10'000U * 1024U);
    EXPECT_LE(reporter.postings_peak_bytes(), 8 * kMiB);
    EXPECT_EQ(reporter.current_bytes(), 0);
    io::LocalFileReader input;
    ASSERT_TRUE(input.open(file.path).ok());
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&input, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "", &index).ok());
    EXPECT_EQ(index.stats().term_count, 10'000U);
    std::vector<uint32_t> positions(1024);
    std::iota(positions.begin(), positions.end(), 0);
    for (const char* term : {"100000", "105000", "109999"}) {
        ASSERT_NO_FATAL_FAILURE(verify_inline_positions(&index, term, positions));
    }
}

Status write_many_inline_document_lists(const std::string& path, MemoryReporter* reporter) {
    std::vector<uint32_t> docs(200);
    for (size_t i = 0; i < docs.size(); ++i) {
        docs[i] = i * 123 + (i % 2) * 64;
    }
    io::LocalFileWriter output;
    RETURN_IF_ERROR(output.open(path));
    SniiCompoundWriter compound(&output);
    SniiIndexInput input;
    input.index_id = 1;
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = docs.back() + 1;
    input.mem_reporter = reporter;
    input.target_dict_block_bytes = 64 * kMiB;
    SniiStreamedIndexSession* session = nullptr;
    RETURN_IF_ERROR(compound.begin_streamed_index(std::move(input), &session));
    for (uint32_t term = 100'000; term < 160'000; ++term) {
        const std::string text = std::to_string(term);
        SpanTermPostingSource source(docs, {}, {});
        RETURN_IF_ERROR(session->push_term(
                StreamedTermPostings {.term = text, .retain_positions = false, .source = &source}));
    }
    RETURN_IF_ERROR(session->finish());
    return compound.finish();
}

void verify_inline_document_list(reader::LogicalIndexReader* index, const char* term,
                                 const std::vector<uint32_t>& expected) {
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    ASSERT_TRUE(index->lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    ASSERT_TRUE(found);
    EXPECT_EQ(entry.kind, format::DictEntryKind::kInline);
    EXPECT_EQ(entry.df, 200U);
    EXPECT_FALSE(entry.frq_bytes.empty());
    EXPECT_GT(entry.frq_bytes.size() * 60'000U, 8 * kMiB);
    std::vector<uint32_t> actual;
    ASSERT_TRUE(query::term_query(*index, term, &actual).ok());
    EXPECT_EQ(actual, expected);
}

TEST(SniiSpillMemory, InlineDocumentListsShareThePostingBudgetWithoutPositions) {
    TemporaryPostingFile file;
    MemoryReporter reporter(nullptr, 0, MemoryReporter::CapPolicy::kSpillThreshold, 8 * kMiB);
    ASSERT_TRUE(write_many_inline_document_lists(file.path, &reporter).ok());
    // Each slim DD field fits the inline threshold, but their sum exceeds B.
    EXPECT_GT(reporter.postings_written_bytes(), 8 * kMiB);
    EXPECT_LE(reporter.postings_peak_bytes(), 8 * kMiB);
    EXPECT_EQ(reporter.current_bytes(), 0);
    io::LocalFileReader input;
    ASSERT_TRUE(input.open(file.path).ok());
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&input, &segment).ok());
    ASSERT_TRUE(segment.open_index(1, "", &index).ok());
    EXPECT_EQ(index.stats().term_count, 60'000U);
    std::vector<uint32_t> expected(200);
    for (size_t i = 0; i < expected.size(); ++i) {
        expected[i] = i * 123 + (i % 2) * 64;
    }
    for (const char* term : {"100000", "130000", "159999"}) {
        ASSERT_NO_FATAL_FAILURE(verify_inline_document_list(&index, term, expected));
    }
}

TEST(SniiSpillMemory, TenThousandRunsKeepOneSpoolAndBoundedDirectory) {
    constexpr uint32_t kTokens = 10'000;
    MemoryReporter reporter(nullptr, 1024, MemoryReporter::CapPolicy::kSpillThreshold, kMiB);
    {
        SpimiTermBuffer buffer(true, 1024, &reporter);
        buffer.set_max_run_files(2);
        buffer.set_forced_spill_min_arena_bytes(0);
        for (uint32_t i = 0; i < kTokens; ++i) {
            buffer.add_token("hot", i / 2, i % 2);
        }
        ASSERT_TRUE(buffer.status().ok()) << buffer.status();
        ASSERT_EQ(buffer.run_count_for_test(), kTokens);
        EXPECT_EQ(buffer.spill_file_count_for_test(), 1U);
        // An append never re-reads any older payload or directory prefix.
        EXPECT_EQ(reporter.postings_read_bytes(), 0);
        EXPECT_LE(reporter.postings_current_bytes(), 128 * 1024);
        uint32_t seen = 0;
        const Status status = buffer.for_each_term_sorted([&](StreamedTermPostings&& term) {
            return consume_pair_postings(std::move(term), &reporter, &seen);
        });
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(seen, kTokens / 2);
        EXPECT_EQ(buffer.spill_file_count_for_test(), 0U);
        EXPECT_EQ(reporter.current_bytes(), 0);
        EXPECT_LE(reporter.postings_peak_bytes(), kMiB);
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

} // namespace
} // namespace doris::snii::writer
