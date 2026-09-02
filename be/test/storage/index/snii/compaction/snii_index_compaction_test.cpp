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

#include "storage/index/snii/compaction/snii_index_compaction.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/snii/compaction/posting_run_merger.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace {

using namespace doris::snii;            // NOLINT
using namespace doris::snii::snii_test; // NOLINT
using compaction::RowIdConversionMap;
using compaction::SniiPlainT2MergePlan;
using compaction::ValidatedRowIdConversion;
using query::phrase_query;
using query::term_query;
using writer::SniiCompoundWriter;
using writer::SniiIndexInput;
using writer::SniiStreamedIndexSession;
using writer::TermPostings;
using writer::MemoryReporter;
namespace inverted_index = doris::segment_v2::inverted_index;

constexpr uint64_t kIndexId = 41;
constexpr std::string_view kIndexSuffix = "body";
constexpr std::pair<uint32_t, uint32_t> kDeleted = {std::numeric_limits<uint32_t>::max(),
                                                    std::numeric_limits<uint32_t>::max()};

struct OpenedIndex {
    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
};

SniiIndexInput make_input(uint32_t doc_count, std::vector<uint32_t> null_docids,
                          std::vector<TermPostings> terms) {
    SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = kIndexSuffix;
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = doc_count;
    input.null_docids = std::move(null_docids);
    input.terms = std::move(terms);
    input.write_freq = false;
    return input;
}

inverted_index::CommonGramsSegmentMetadata common_grams_metadata(uint64_t doc_count,
                                                                 uint64_t token_count) {
    inverted_index::CommonGramsQueryIdentity identity {.common_grams_dictionary_identity = "dict-a",
                                                       .base_analyzer_fingerprint = "base-a",
                                                       .common_grams_fingerprint = "grams-a"};
    auto metadata = inverted_index::make_common_grams_segment_metadata(identity);
    metadata.scoring_doc_count = doc_count;
    metadata.scoring_token_count = token_count;
    return metadata;
}

std::string common_gram(std::string_view left, std::string_view right) {
    auto encoded = inverted_index::encode_common_gram(left, right);
    EXPECT_TRUE(encoded.has_value());
    return encoded.has_value() ? std::move(encoded.value()) : std::string();
}

SniiIndexInput make_common_grams_input(uint32_t doc_count, std::vector<uint32_t> null_docids,
                                       std::vector<uint8_t> norms, std::vector<TermPostings> terms,
                                       uint64_t token_count) {
    SniiIndexInput input = make_input(doc_count, std::move(null_docids), std::move(terms));
    input.config = format::IndexConfig::kDocsPositionsScoring;
    input.write_freq = true;
    input.encoded_norms = std::move(norms);
    input.common_grams_metadata = common_grams_metadata(doc_count, token_count);
    return input;
}

TermPostings make_docs_only_gram(std::string term, std::vector<uint32_t> docids) {
    TermPostings postings;
    postings.term = std::move(term);
    postings.docids = std::move(docids);
    postings.retain_positions = false;
    return postings;
}

SniiIndexInput make_hybrid_common_grams_input(uint32_t doc_count, std::vector<uint32_t> null_docids,
                                              std::vector<uint8_t> norms,
                                              std::vector<TermPostings> terms,
                                              uint64_t token_count) {
    std::ranges::sort(terms, [](const auto& lhs, const auto& rhs) { return lhs.term < rhs.term; });
    SniiIndexInput input = make_common_grams_input(doc_count, std::move(null_docids),
                                                   std::move(norms), std::move(terms), token_count);
    input.common_grams_metadata->common_grams_coverage =
            inverted_index::CommonGramsCoverage::kMixed;
    input.common_grams_posting_policy = format::CommonGramsPostingPolicy::kHybridV1;
    return input;
}

void build_index(SniiIndexInput input, OpenedIndex* out,
                 reader::LogicalIndexOpenMode open_mode = reader::LogicalIndexOpenMode::kQuery) {
    SniiCompoundWriter compound(&out->file);
    assert_ok(compound.add_logical_index(input));
    assert_ok(compound.finish());
    assert_ok(reader::SniiSegmentReader::open(&out->file, &out->segment));
    assert_ok(out->segment.open_index(kIndexId, kIndexSuffix, &out->index, open_mode));
}

std::vector<uint8_t> copy_region(const MemoryFile& file, const format::RegionRef& region) {
    EXPECT_LE(region.offset + region.length, file.data().size());
    if (region.offset + region.length > file.data().size()) {
        return {};
    }
    return {file.data().begin() + static_cast<ptrdiff_t>(region.offset),
            file.data().begin() + static_cast<ptrdiff_t>(region.offset + region.length)};
}

void expect_identical_index_image(MemoryFile* actual, MemoryFile* expected) {
    reader::SniiSegmentReader actual_segment;
    reader::SniiSegmentReader expected_segment;
    reader::LogicalIndexReader actual_index;
    reader::LogicalIndexReader expected_index;
    assert_ok(reader::SniiSegmentReader::open(actual, &actual_segment));
    assert_ok(reader::SniiSegmentReader::open(expected, &expected_segment));
    assert_ok(actual_segment.open_index(kIndexId, kIndexSuffix, &actual_index));
    assert_ok(expected_segment.open_index(kIndexId, kIndexSuffix, &expected_index));

    const format::SectionRefs& actual_refs = actual_index.section_refs();
    const format::SectionRefs& expected_refs = expected_index.section_refs();
    auto expect_region = [&](std::string_view name, const format::RegionRef& actual_region,
                             const format::RegionRef& expected_region) {
        SCOPED_TRACE(name);
        EXPECT_EQ(actual_region.offset, expected_region.offset);
        EXPECT_EQ(actual_region.length, expected_region.length);
        EXPECT_EQ(copy_region(*actual, actual_region), copy_region(*expected, expected_region));
    };
    expect_region("posting", actual_refs.posting_region, expected_refs.posting_region);
    expect_region("DICT", actual_refs.dict_region, expected_refs.dict_region);
    expect_region("norms", actual_refs.norms, expected_refs.norms);
    expect_region("null bitmap", actual_refs.null_bitmap, expected_refs.null_bitmap);
    expect_region("BSBF", actual_refs.bsbf, expected_refs.bsbf);

    EXPECT_EQ(actual->data(), expected->data()) << "complete SNII image differs";
}

std::unique_ptr<ValidatedRowIdConversion> make_validated_conversion(
        const RowIdConversionMap* conversion, const std::vector<uint32_t>& source_rows,
        const std::vector<uint32_t>& destination_rows) {
    std::unique_ptr<ValidatedRowIdConversion> validated;
    const Status status =
            ValidatedRowIdConversion::create(conversion, source_rows, destination_rows, &validated);
    EXPECT_TRUE(status.ok()) << status;
    return validated;
}

struct MergedPostingCopy {
    uint32_t destination = 0;
    uint32_t docid = 0;
    uint32_t frequency = 0;
    std::vector<uint32_t> positions;

    bool operator==(const MergedPostingCopy&) const = default;
};

struct MergedRunHarness {
    std::vector<std::unique_ptr<compaction::SniiPostingReadContext>> read_contexts;
    std::vector<std::unique_ptr<compaction::SniiPostingCursor>> cursors;
};

Status make_merged_run_harness(const std::vector<OpenedIndex*>& sources, std::string_view term,
                               const ValidatedRowIdConversion* rowid_conversion,
                               MergedRunHarness* harness) {
    for (size_t source_ordinal = 0; source_ordinal < sources.size(); ++source_ordinal) {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        RETURN_IF_ERROR(
                sources[source_ordinal]->index.lookup(term, &found, &entry, &frq_base, &prx_base));
        if (!found) {
            continue;
        }
        auto read_context = std::make_unique<compaction::SniiPostingReadContext>(
                &sources[source_ordinal]->index, /*total_read_ahead_budget_bytes=*/1U << 20);
        RETURN_IF_ERROR(read_context->init());
        auto cursor = std::make_unique<compaction::SniiPostingCursor>(
                read_context.get(), std::move(entry), frq_base, prx_base,
                static_cast<uint32_t>(source_ordinal), rowid_conversion);
        RETURN_IF_ERROR(cursor->init());
        harness->read_contexts.push_back(std::move(read_context));
        harness->cursors.push_back(std::move(cursor));
    }
    return Status::OK();
}

Status drain_merged_runs(compaction::MergedPostingRuns* runs, uint32_t max_run_docs,
                         std::vector<MergedPostingCopy>* output) {
    while (!runs->empty()) {
        const uint32_t destination = runs->next_destination();
        RETURN_IF_ERROR(runs->begin_destination(destination));
        while (true) {
            writer::PostingRunView run;
            bool has_run = false;
            RETURN_IF_ERROR(runs->next_run(max_run_docs, &run, &has_run));
            if (!has_run) {
                break;
            }
            EXPECT_FALSE(run.docids.empty());
            EXPECT_TRUE(run.freqs.empty() || run.freqs.size() == run.docids.size());
            EXPECT_TRUE(run.position_offsets.empty() ||
                        run.position_offsets.size() == run.docids.size() + 1);
            if (!run.position_offsets.empty()) {
                EXPECT_EQ(run.position_offsets.back() - run.position_offsets.front(),
                          run.positions_flat.size());
            }
            for (size_t ordinal = 0; ordinal < run.docids.size(); ++ordinal) {
                std::vector<uint32_t> positions;
                if (!run.position_offsets.empty()) {
                    const uint32_t position_base = run.position_offsets.front();
                    const uint32_t begin = run.position_offsets[ordinal] - position_base;
                    const uint32_t end = run.position_offsets[ordinal + 1] - position_base;
                    positions.assign(run.positions_flat.begin() + begin,
                                     run.positions_flat.begin() + end);
                }
                output->push_back({.destination = destination,
                                   .docid = run.docids[ordinal],
                                   .frequency = run.freqs.empty() ? 0 : run.freqs[ordinal],
                                   .positions = std::move(positions)});
            }
        }
    }
    return Status::OK();
}

std::vector<MergedPostingCopy> merge_posting_oracle(
        const std::vector<std::vector<PostingDoc>>& source_postings,
        const RowIdConversionMap& conversion) {
    std::vector<MergedPostingCopy> output;
    for (size_t source = 0; source < source_postings.size(); ++source) {
        for (const PostingDoc& posting : source_postings[source]) {
            const auto [destination, docid] = conversion[source][posting.docid];
            if (std::pair(destination, docid) == kDeleted) {
                continue;
            }
            output.push_back({.destination = destination,
                              .docid = docid,
                              .frequency = static_cast<uint32_t>(posting.positions.size()),
                              .positions = posting.positions});
        }
    }
    std::ranges::sort(output, [](const MergedPostingCopy& lhs, const MergedPostingCopy& rhs) {
        return std::pair(lhs.destination, lhs.docid) < std::pair(rhs.destination, rhs.docid);
    });
    return output;
}

std::vector<TermPostings> source_zero_terms() {
    return {
            make_term("alpha", {{.docid = 0, .positions = {0}}, {.docid = 3, .positions = {0, 2}}}),
            make_term("beta", {{.docid = 0, .positions = {1}},
                               {.docid = 2, .positions = {0}},
                               {.docid = 3, .positions = {3}}}),
            make_term("gamma", {{.docid = 0, .positions = {2}}, {.docid = 2, .positions = {1}}}),
    };
}

std::vector<TermPostings> source_one_terms() {
    return {
            make_term("alpha", {{.docid = 0, .positions = {0}}}),
            make_term("beta", {{.docid = 1, .positions = {0}}}),
            make_term("gamma", {{.docid = 0, .positions = {1}}}),
    };
}

std::vector<TermPostings> destination_zero_terms() {
    return {
            make_term("alpha", {{.docid = 0, .positions = {0}}, {.docid = 1, .positions = {0}}}),
            make_term("beta", {{.docid = 0, .positions = {1}}, {.docid = 3, .positions = {0}}}),
            make_term("gamma", {{.docid = 0, .positions = {2}}, {.docid = 1, .positions = {1}}}),
    };
}

std::vector<TermPostings> destination_one_terms() {
    return {
            make_term("beta", {{.docid = 0, .positions = {0}}}),
            make_term("gamma", {{.docid = 0, .positions = {1}}}),
    };
}

void expect_query_semantics(const reader::LogicalIndexReader& index,
                            const std::vector<uint32_t>& alpha,
                            const std::vector<uint32_t>& alpha_beta,
                            const std::vector<uint32_t>& alpha_beta_gamma) {
    std::vector<uint32_t> docs;
    assert_ok(term_query(index, "alpha", &docs));
    EXPECT_EQ(docs, alpha);
    assert_ok(phrase_query(index, {"alpha", "beta"}, &docs));
    EXPECT_EQ(docs, alpha_beta);
    assert_ok(phrase_query(index, {"alpha", "beta", "gamma"}, &docs));
    EXPECT_EQ(docs, alpha_beta_gamma);
}

TEST(SniiIndexCompactionTest, DirectPostingRunsMatchPositionedOracleAcrossSources) {
    const std::vector<PostingDoc> source_zero_postings = {
            {.docid = 0, .positions = {1, 4}}, {.docid = 2, .positions = {8}},
            {.docid = 3, .positions = {2}},    {.docid = 4, .positions = {3, 8, 9}},
            {.docid = 6, .positions = {6}},
    };
    const std::vector<PostingDoc> source_one_postings = {
            {.docid = 0, .positions = {0}},     {.docid = 1, .positions = {1, 5}},
            {.docid = 2, .positions = {4}},     {.docid = 4, .positions = {11}},
            {.docid = 5, .positions = {7, 10}},
    };
    OpenedIndex source_zero;
    OpenedIndex source_one;
    build_index(make_input(/*doc_count=*/7, /*null_docids=*/ {},
                           {make_term("shared", source_zero_postings)}),
                &source_zero, reader::LogicalIndexOpenMode::kCompaction);
    build_index(make_input(/*doc_count=*/6, /*null_docids=*/ {},
                           {make_term("shared", source_one_postings)}),
                &source_one, reader::LogicalIndexOpenMode::kCompaction);

    const RowIdConversionMap conversion = {
            {{0, 0}, {0, 2}, kDeleted, {0, 4}, {1, 0}, {1, 2}, {1, 4}},
            {{0, 1}, {0, 3}, {0, 5}, {1, 1}, kDeleted, {1, 3}},
    };
    const std::vector<uint32_t> destination_rows = {6, 5};
    auto validated = make_validated_conversion(&conversion, {7, 6}, destination_rows);
    ASSERT_NE(validated, nullptr);

    MergedRunHarness harness;
    assert_ok(make_merged_run_harness({&source_zero, &source_one}, "shared", validated.get(),
                                      &harness));
    std::vector<uint64_t> semantic_token_counts(destination_rows.size(), 0);
    compaction::testing::reset_posting_run_merge_counters();
    compaction::MergedPostingRuns runs(std::move(harness.cursors), /*retain_positions=*/true,
                                       /*counts_as_semantic_token=*/false, destination_rows,
                                       semantic_token_counts);
    assert_ok(runs.init());
    std::vector<MergedPostingCopy> got;
    assert_ok(drain_merged_runs(&runs, /*max_run_docs=*/2, &got));

    const std::vector<MergedPostingCopy> expected =
            merge_posting_oracle({source_zero_postings, source_one_postings}, conversion);
    EXPECT_EQ(got, expected);
    EXPECT_EQ(compaction::testing::posting_run_documents(), expected.size());
    EXPECT_EQ(compaction::testing::posting_run_shape_scan_documents(), expected.size());
    EXPECT_GT(compaction::testing::posting_run_emitted_runs(), 1U);
    EXPECT_EQ(compaction::testing::posting_run_legacy_fill_calls(), 0U);
    EXPECT_EQ(compaction::testing::posting_run_copied_documents(), 0U);
}

TEST(SniiIndexCompactionTest, DirectPostingRunsMatchSingleSourceDocsOnlyOracle) {
    const std::string gram = common_gram("a", "term");
    OpenedIndex source;
    build_index(make_hybrid_common_grams_input(
                        /*doc_count=*/5, /*null_docids=*/ {}, /*norms=*/ {1, 1, 1, 1, 1},
                        {make_docs_only_gram(gram, {0, 2, 4}),
                         make_term("plain", {{.docid = 1, .positions = {0}}})},
                        /*token_count=*/1),
                &source, reader::LogicalIndexOpenMode::kCompaction);

    const RowIdConversionMap conversion = {{{0, 0}, {0, 1}, {0, 2}, {1, 0}, {1, 1}}};
    const std::vector<uint32_t> destination_rows = {3, 2};
    auto validated = make_validated_conversion(&conversion, {5}, destination_rows);
    ASSERT_NE(validated, nullptr);

    MergedRunHarness harness;
    assert_ok(make_merged_run_harness({&source}, gram, validated.get(), &harness));
    std::vector<uint64_t> semantic_token_counts(destination_rows.size(), 0);
    compaction::testing::reset_posting_run_merge_counters();
    compaction::MergedPostingRuns runs(std::move(harness.cursors), /*retain_positions=*/false,
                                       /*counts_as_semantic_token=*/false, destination_rows,
                                       semantic_token_counts);
    assert_ok(runs.init());
    std::vector<MergedPostingCopy> got;
    assert_ok(drain_merged_runs(&runs, /*max_run_docs=*/1, &got));

    EXPECT_EQ(got, (std::vector<MergedPostingCopy> {
                           {0, 0, 0, {}},
                           {0, 2, 0, {}},
                           {1, 1, 0, {}},
                   }));
    EXPECT_EQ(compaction::testing::posting_run_shape_scan_documents(), got.size());
    EXPECT_EQ(compaction::testing::posting_run_legacy_fill_calls(), 0U);
    EXPECT_EQ(compaction::testing::posting_run_copied_documents(), 0U);
}

TEST(SniiIndexCompactionTest, MergesDeletesNullsAndInterleavedSourcesByteIdenticallyToRebuild) {
    OpenedIndex source_zero;
    OpenedIndex source_one;
    build_index(make_input(/*doc_count=*/4, /*null_docids=*/ {1}, source_zero_terms()),
                &source_zero, reader::LogicalIndexOpenMode::kCompaction);
    build_index(make_input(/*doc_count=*/3, /*null_docids=*/ {2}, source_one_terms()), &source_one);

    const RowIdConversionMap conversion = {
            {{0, 0}, {0, 2}, {1, 0}, kDeleted},
            {{0, 1}, {0, 3}, {1, 1}},
    };
    const std::vector<uint32_t> destination_rows = {4, 2};
    auto validated = make_validated_conversion(&conversion, {4, 3}, destination_rows);
    ASSERT_NE(validated, nullptr);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source_zero.index, &source_one.index}, *validated,
                                            /*total_read_ahead_budget_bytes=*/1U << 20, &plan));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->destination_null_docids(0), (std::vector<uint32_t> {2}));
    EXPECT_EQ(plan->destination_null_docids(1), (std::vector<uint32_t> {1}));

    std::array<MemoryFile, 2> merged_files;
    std::array<std::unique_ptr<SniiCompoundWriter>, 2> compounds;
    std::array<SniiStreamedIndexSession*, 2> sessions = {nullptr, nullptr};
    for (size_t i = 0; i < compounds.size(); ++i) {
        compounds[i] = std::make_unique<SniiCompoundWriter>(&merged_files[i]);
        SniiIndexInput input =
                make_input(destination_rows[i], plan->destination_null_docids(i), {});
        assert_ok(compounds[i]->begin_streamed_index(std::move(input), &sessions[i]));
    }
    assert_ok(plan->execute(sessions));
    for (auto& compound : compounds) {
        assert_ok(compound->finish());
    }

    std::array<OpenedIndex, 2> expected;
    build_index(make_input(/*doc_count=*/4, /*null_docids=*/ {2}, destination_zero_terms()),
                &expected[0]);
    build_index(make_input(/*doc_count=*/2, /*null_docids=*/ {1}, destination_one_terms()),
                &expected[1]);
    expect_identical_index_image(&merged_files[0], &expected[0].file);
    expect_identical_index_image(&merged_files[1], &expected[1].file);

    std::array<reader::SniiSegmentReader, 2> merged_segments;
    std::array<reader::LogicalIndexReader, 2> merged_indexes;
    for (size_t i = 0; i < merged_files.size(); ++i) {
        assert_ok(reader::SniiSegmentReader::open(&merged_files[i], &merged_segments[i]));
        assert_ok(merged_segments[i].open_index(kIndexId, kIndexSuffix, &merged_indexes[i]));
    }
    expect_query_semantics(merged_indexes[0], /*alpha=*/ {0, 1}, /*alpha_beta=*/ {0},
                           /*alpha_beta_gamma=*/ {0});
    expect_query_semantics(merged_indexes[1], /*alpha=*/ {}, /*alpha_beta=*/ {},
                           /*alpha_beta_gamma=*/ {});
    EXPECT_EQ(merged_indexes[0].stats().doc_count, 4U);
    EXPECT_EQ(merged_indexes[0].stats().indexed_doc_count, 3U);
    EXPECT_EQ(merged_indexes[0].stats().null_count, 1U);
    EXPECT_EQ(merged_indexes[1].stats().doc_count, 2U);
    EXPECT_EQ(merged_indexes[1].stats().indexed_doc_count, 1U);
    EXPECT_EQ(merged_indexes[1].stats().null_count, 1U);
}

TEST(SniiIndexCompactionTest, MergesTwentyFourRunSourcesByteIdenticallyToReferenceRebuild) {
    constexpr size_t kSourceCount = 24;
    constexpr uint32_t kDocsPerSource = 400;
    constexpr uint32_t kSourceRunDocs = 17;

    std::array<OpenedIndex, kSourceCount> sources;
    std::array<std::vector<PostingDoc>, kSourceCount> source_docs;
    for (size_t source = 0; source < kSourceCount; ++source) {
        source_docs[source].reserve(kDocsPerSource);
        for (uint32_t docid = 0; docid < kDocsPerSource; ++docid) {
            const uint32_t first_position = static_cast<uint32_t>((source + docid) % 19);
            source_docs[source].push_back(
                    {.docid = docid, .positions = {first_position, first_position + 3}});
        }
        std::vector<TermPostings> terms;
        if (source == 0) {
            terms.push_back(make_term("first-destination-only", {{.docid = 0, .positions = {1}}}));
        }
        terms.push_back(make_term("shared", source_docs[source]));
        build_index(make_input(kDocsPerSource, {}, std::move(terms)), &sources[source],
                    reader::LogicalIndexOpenMode::kCompaction);
    }

    RowIdConversionMap conversion(kSourceCount);
    for (auto& source_mapping : conversion) {
        source_mapping.assign(kDocsPerSource, kDeleted);
    }
    std::vector<std::pair<size_t, uint32_t>> reference_order;
    reference_order.reserve(kSourceCount * kDocsPerSource);
    for (uint32_t run_begin = 0; run_begin < kDocsPerSource; run_begin += kSourceRunDocs) {
        const uint32_t run_end = std::min(kDocsPerSource, run_begin + kSourceRunDocs);
        for (size_t source = 0; source < kSourceCount; ++source) {
            for (uint32_t docid = run_begin; docid < run_end; ++docid) {
                const bool deleted = source == kSourceCount - 1 ||
                                     (source * 31 + static_cast<size_t>(docid) * 17) % 113 == 7;
                if (!deleted) {
                    reference_order.emplace_back(source, docid);
                }
            }
        }
    }
    ASSERT_GT(reference_order.size(), 8500U);
    const uint32_t first_destination_rows = 8500;
    const std::vector<uint32_t> destination_rows = {
            first_destination_rows,
            static_cast<uint32_t>(reference_order.size() - first_destination_rows)};
    std::array<std::vector<PostingDoc>, 2> reference_docs;
    for (size_t ordinal = 0; ordinal < reference_order.size(); ++ordinal) {
        const auto [source, source_docid] = reference_order[ordinal];
        const uint32_t destination = ordinal < first_destination_rows ? 0 : 1;
        const uint32_t destination_docid =
                destination == 0 ? static_cast<uint32_t>(ordinal)
                                 : static_cast<uint32_t>(ordinal - first_destination_rows);
        conversion[source][source_docid] = {destination, destination_docid};
        reference_docs[destination].push_back(
                {.docid = destination_docid,
                 .positions = source_docs[source][source_docid].positions});
    }
    ASSERT_EQ(conversion[0][0].first, 0U);

    std::vector<const reader::LogicalIndexReader*> source_indexes;
    source_indexes.reserve(kSourceCount);
    for (const OpenedIndex& source : sources) {
        source_indexes.push_back(&source.index);
    }
    auto validated = make_validated_conversion(
            &conversion, std::vector<uint32_t>(kSourceCount, kDocsPerSource), destination_rows);
    ASSERT_NE(validated, nullptr);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare(
            std::move(source_indexes), *validated,
            kSourceCount * SniiPlainT2MergePlan::kMinReadAheadBudgetPerSource, &plan));

    std::array<MemoryFile, 2> merged_files;
    std::array<std::unique_ptr<SniiCompoundWriter>, 2> compounds;
    std::array<SniiStreamedIndexSession*, 2> sessions = {nullptr, nullptr};
    for (size_t destination = 0; destination < compounds.size(); ++destination) {
        compounds[destination] = std::make_unique<SniiCompoundWriter>(&merged_files[destination]);
        assert_ok(compounds[destination]->begin_streamed_index(
                make_input(destination_rows[destination], {}, {}), &sessions[destination]));
    }

    compaction::testing::reset_posting_run_merge_counters();
    assert_ok(plan->execute(sessions));
    EXPECT_GT(compaction::testing::posting_run_frontier_updates(), 0U);
    EXPECT_LE(compaction::testing::posting_run_frontier_updates(),
              compaction::testing::posting_run_emitted_runs());
    EXPECT_GT(compaction::testing::posting_run_frontier_comparisons(), 0U);
    EXPECT_EQ(compaction::testing::posting_run_documents(), reference_order.size() + 1);
    EXPECT_LE(compaction::testing::posting_run_boundary_searches(),
              compaction::testing::posting_run_emitted_runs());
    for (auto& compound : compounds) {
        assert_ok(compound->finish());
    }

    for (size_t destination = 0; destination < merged_files.size(); ++destination) {
        std::vector<TermPostings> expected_terms;
        if (destination == 0) {
            expected_terms.push_back(
                    make_term("first-destination-only",
                              {{.docid = conversion[0][0].second, .positions = {1}}}));
        }
        expected_terms.push_back(make_term("shared", reference_docs[destination]));
        OpenedIndex expected;
        build_index(make_input(destination_rows[destination], {}, std::move(expected_terms)),
                    &expected);
        expect_identical_index_image(&merged_files[destination], &expected.file);
    }
}

TEST(SniiIndexCompactionTest, CommonGramsMergeMatchesRebuildAfterDeletesAndRemap) {
    const std::string gram = common_gram("the", "cat");
    OpenedIndex source_zero;
    OpenedIndex source_one;
    build_index(make_common_grams_input(
                        /*doc_count=*/3, /*null_docids=*/ {2}, /*norms=*/ {10, 20, 30},
                        {make_term(gram, {{.docid = 0, .positions = {1}},
                                          {.docid = 1, .positions = {1}}}),
                         make_term("alpha", {{.docid = 0, .positions = {0, 2}},
                                             {.docid = 1, .positions = {0}}}),
                         make_term("beta", {{.docid = 2, .positions = {0}}})},
                        /*token_count=*/4),
                &source_zero, reader::LogicalIndexOpenMode::kCompaction);
    build_index(make_common_grams_input(
                        /*doc_count=*/2, /*null_docids=*/ {}, /*norms=*/ {40, 50},
                        {make_term(gram, {{.docid = 1, .positions = {1}}}),
                         make_term("alpha", {{.docid = 0, .positions = {0}}}),
                         make_term("gamma", {{.docid = 1, .positions = {0, 2}}})},
                        /*token_count=*/3),
                &source_one, reader::LogicalIndexOpenMode::kCompaction);

    const RowIdConversionMap conversion = {
            {{0, 0}, kDeleted, {1, 0}},
            {{0, 1}, {0, 2}},
    };
    const std::vector<uint32_t> destination_rows = {3, 1};
    auto validated = make_validated_conversion(&conversion, {3, 2}, destination_rows);
    ASSERT_NE(validated, nullptr);
    compaction::SniiCompactionEligibility eligibility {
            .kind = compaction::SniiStreamedMergeKind::kCommonGramsT3,
            .common_grams_metadata_seed = common_grams_metadata(0, 0)};
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source_zero.index, &source_one.index}, *validated,
                                            eligibility,
                                            /*total_read_ahead_budget_bytes=*/1U << 20, &plan));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->destination_null_docids(0), (std::vector<uint32_t> {}));
    EXPECT_EQ(plan->destination_null_docids(1), (std::vector<uint32_t> {0}));
    EXPECT_EQ(plan->destination_encoded_norms(0), (std::vector<uint8_t> {10, 40, 50}));
    EXPECT_EQ(plan->destination_encoded_norms(1), (std::vector<uint8_t> {30}));

    std::array<MemoryFile, 2> merged_files;
    std::array<std::unique_ptr<SniiCompoundWriter>, 2> compounds;
    std::array<SniiStreamedIndexSession*, 2> sessions = {nullptr, nullptr};
    for (size_t i = 0; i < compounds.size(); ++i) {
        compounds[i] = std::make_unique<SniiCompoundWriter>(&merged_files[i]);
        SniiIndexInput input = make_input(destination_rows[i], {}, {});
        input.config = plan->destination_index_config();
        input.write_freq = true;
        input.common_grams_metadata = plan->destination_common_grams_metadata(i);
        assert_ok(compounds[i]->begin_streamed_index(
                std::move(input), plan->take_destination_null_docids(i),
                plan->take_destination_encoded_norms(i), &sessions[i]));
    }
    assert_ok(plan->execute(sessions));
    for (auto& compound : compounds) {
        assert_ok(compound->finish());
    }

    std::array<OpenedIndex, 2> rebuilt;
    build_index(make_common_grams_input(
                        /*doc_count=*/3, /*null_docids=*/ {}, /*norms=*/ {10, 40, 50},
                        {make_term(gram, {{.docid = 0, .positions = {1}},
                                          {.docid = 2, .positions = {1}}}),
                         make_term("alpha", {{.docid = 0, .positions = {0, 2}},
                                             {.docid = 1, .positions = {0}}}),
                         make_term("gamma", {{.docid = 2, .positions = {0, 2}}})},
                        /*token_count=*/5),
                &rebuilt[0]);
    build_index(make_common_grams_input(
                        /*doc_count=*/1, /*null_docids=*/ {0}, /*norms=*/ {30},
                        {make_term("beta", {{.docid = 0, .positions = {0}}})},
                        /*token_count=*/1),
                &rebuilt[1]);
    expect_identical_index_image(&merged_files[0], &rebuilt[0].file);
    expect_identical_index_image(&merged_files[1], &rebuilt[1].file);

    std::array<reader::SniiSegmentReader, 2> merged_segments;
    std::array<reader::LogicalIndexReader, 2> merged_indexes;
    for (size_t i = 0; i < merged_files.size(); ++i) {
        assert_ok(reader::SniiSegmentReader::open(&merged_files[i], &merged_segments[i]));
        assert_ok(merged_segments[i].open_index(kIndexId, kIndexSuffix, &merged_indexes[i]));
        ASSERT_NE(merged_indexes[i].common_grams_metadata(), nullptr);
        EXPECT_EQ(*merged_indexes[i].common_grams_metadata(),
                  *rebuilt[i].index.common_grams_metadata());
    }
    EXPECT_EQ(merged_indexes[0].common_grams_metadata()->scoring_token_count, 5U);
    EXPECT_EQ(merged_indexes[1].common_grams_metadata()->scoring_token_count, 1U);
    format::NormsPodReader first_norms;
    format::NormsPodReader second_norms;
    assert_ok(merged_indexes[0].open_norms(&first_norms));
    assert_ok(merged_indexes[1].open_norms(&second_norms));
    EXPECT_EQ(first_norms.encoded_norm(0), 10U);
    EXPECT_EQ(first_norms.encoded_norm(1), 40U);
    EXPECT_EQ(first_norms.encoded_norm(2), 50U);
    EXPECT_EQ(second_norms.encoded_norm(0), 30U);

    std::vector<uint32_t> merged_docs;
    std::vector<uint32_t> rebuilt_docs;
    assert_ok(term_query(merged_indexes[0], "alpha", &merged_docs));
    assert_ok(term_query(rebuilt[0].index, "alpha", &rebuilt_docs));
    EXPECT_EQ(merged_docs, rebuilt_docs);
    assert_ok(phrase_query(merged_indexes[0], {"alpha", "gamma"}, &merged_docs));
    assert_ok(phrase_query(rebuilt[0].index, {"alpha", "gamma"}, &rebuilt_docs));
    EXPECT_EQ(merged_docs, rebuilt_docs);
}

TEST(SniiIndexCompactionTest, HybridCommonGramsMergePreservesPostingShapesAfterDeleteAndSplit) {
    const std::string positioned_gram = common_gram("the", "of");
    const std::string docs_only_gram = common_gram("of", "wolf");
    const auto source_terms = [&] {
        return std::vector<TermPostings> {
                make_term(positioned_gram, {{.docid = 0, .positions = {0}},
                                            {.docid = 1, .positions = {0}},
                                            {.docid = 2, .positions = {0}}}),
                make_docs_only_gram(docs_only_gram, {0, 1, 2}),
                make_term("of", {{.docid = 0, .positions = {1}},
                                 {.docid = 1, .positions = {1}},
                                 {.docid = 2, .positions = {1}}}),
                make_term("the", {{.docid = 0, .positions = {0}},
                                  {.docid = 1, .positions = {0}},
                                  {.docid = 2, .positions = {0}}}),
                make_term("wolf", {{.docid = 0, .positions = {2}},
                                   {.docid = 1, .positions = {2}},
                                   {.docid = 2, .positions = {2}}})};
    };
    OpenedIndex source;
    build_index(make_hybrid_common_grams_input(
                        /*doc_count=*/4, /*null_docids=*/ {3}, /*norms=*/ {10, 20, 30, 40},
                        source_terms(), /*token_count=*/9),
                &source, reader::LogicalIndexOpenMode::kCompaction);

    const RowIdConversionMap conversion = {{{0, 0}, kDeleted, {1, 0}, {1, 1}}};
    const std::vector<uint32_t> destination_rows = {1, 2};
    auto validated = make_validated_conversion(&conversion, {4}, destination_rows);
    ASSERT_NE(validated, nullptr);
    compaction::SniiCompactionEligibility eligibility {
            .kind = compaction::SniiStreamedMergeKind::kCommonGramsT3,
            .common_grams_metadata_seed =
                    [] {
                        auto metadata = common_grams_metadata(0, 0);
                        metadata.common_grams_coverage =
                                inverted_index::CommonGramsCoverage::kMixed;
                        return metadata;
                    }(),
            .common_grams_posting_policy = format::CommonGramsPostingPolicy::kHybridV1};
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source.index}, *validated, eligibility,
                                            /*total_read_ahead_budget_bytes=*/1U << 20, &plan));

    std::array<MemoryFile, 2> merged_files;
    std::array<std::unique_ptr<SniiCompoundWriter>, 2> compounds;
    std::array<SniiStreamedIndexSession*, 2> sessions = {nullptr, nullptr};
    for (size_t i = 0; i < compounds.size(); ++i) {
        compounds[i] = std::make_unique<SniiCompoundWriter>(&merged_files[i]);
        SniiIndexInput input = make_input(destination_rows[i], {}, {});
        input.config = plan->destination_index_config();
        input.write_freq = true;
        input.common_grams_metadata = plan->destination_common_grams_metadata(i);
        input.common_grams_posting_policy = plan->destination_common_grams_posting_policy();
        assert_ok(compounds[i]->begin_streamed_index(
                std::move(input), plan->take_destination_null_docids(i),
                plan->take_destination_encoded_norms(i), &sessions[i]));
    }
    assert_ok(plan->execute(sessions));
    for (auto& compound : compounds) {
        assert_ok(compound->finish());
    }

    std::array<reader::SniiSegmentReader, 2> merged_segments;
    std::array<reader::LogicalIndexReader, 2> merged_indexes;
    for (size_t i = 0; i < merged_indexes.size(); ++i) {
        assert_ok(reader::SniiSegmentReader::open(&merged_files[i], &merged_segments[i]));
        assert_ok(merged_segments[i].open_index(kIndexId, kIndexSuffix, &merged_indexes[i]));
        ASSERT_NE(merged_indexes[i].common_grams_metadata(), nullptr);
        EXPECT_EQ(merged_indexes[i].common_grams_metadata()->common_grams_coverage,
                  inverted_index::CommonGramsCoverage::kMixed);
        EXPECT_EQ(merged_indexes[i].common_grams_posting_policy(),
                  format::CommonGramsPostingPolicy::kHybridV1);

        bool found = false;
        format::DictEntry positioned_entry;
        format::DictEntry docs_only_entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(merged_indexes[i].lookup(positioned_gram, &found, &positioned_entry, &frq_base,
                                           &prx_base));
        ASSERT_TRUE(found);
        assert_ok(merged_indexes[i].lookup(docs_only_gram, &found, &docs_only_entry, &frq_base,
                                           &prx_base));
        ASSERT_TRUE(found);
        EXPECT_TRUE(compaction::posting_entry_has_positions(positioned_entry));
        EXPECT_FALSE(compaction::posting_entry_has_positions(docs_only_entry));

        std::vector<uint32_t> docs;
        assert_ok(term_query(merged_indexes[i], docs_only_gram, &docs));
        EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
        assert_ok(phrase_query(merged_indexes[i], {"the", "of", "wolf"}, &docs));
        EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
    }
    EXPECT_EQ(merged_indexes[0].common_grams_metadata()->scoring_token_count, 3U);
    EXPECT_EQ(merged_indexes[1].common_grams_metadata()->scoring_token_count, 3U);
}

TEST(SniiIndexCompactionTest, HybridCommonGramsRejectsSameTermPositionShapeMismatch) {
    const std::string gram = common_gram("the", "of");
    OpenedIndex positioned;
    OpenedIndex docs_only;
    build_index(make_hybrid_common_grams_input(1, {}, {1},
                                               {make_term(gram, {{.docid = 0, .positions = {0}}}),
                                                make_term("the", {{.docid = 0, .positions = {0}}})},
                                               1),
                &positioned, reader::LogicalIndexOpenMode::kCompaction);
    build_index(make_hybrid_common_grams_input(1, {}, {1},
                                               {make_docs_only_gram(gram, {0}),
                                                make_term("the", {{.docid = 0, .positions = {0}}})},
                                               1),
                &docs_only, reader::LogicalIndexOpenMode::kCompaction);

    const RowIdConversionMap conversion = {{{0, 0}}, {{0, 1}}};
    auto validated = make_validated_conversion(&conversion, {1, 1}, {2});
    ASSERT_NE(validated, nullptr);
    compaction::SniiCompactionEligibility eligibility {
            .kind = compaction::SniiStreamedMergeKind::kCommonGramsT3,
            .common_grams_metadata_seed =
                    [] {
                        auto metadata = common_grams_metadata(0, 0);
                        metadata.common_grams_coverage =
                                inverted_index::CommonGramsCoverage::kMixed;
                        return metadata;
                    }(),
            .common_grams_posting_policy = format::CommonGramsPostingPolicy::kHybridV1};
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&positioned.index, &docs_only.index}, *validated,
                                            eligibility, /*total_read_ahead_budget_bytes=*/1U << 20,
                                            &plan));

    MemoryFile merged_file;
    SniiCompoundWriter compound(&merged_file);
    SniiIndexInput input = make_input(2, {}, {});
    input.config = plan->destination_index_config();
    input.write_freq = true;
    input.common_grams_metadata = plan->destination_common_grams_metadata(0);
    input.common_grams_posting_policy = plan->destination_common_grams_posting_policy();
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), plan->take_destination_null_docids(0),
                                            plan->take_destination_encoded_norms(0), &session));
    const Status status = plan->execute(std::span(&session, 1));
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiIndexCompactionTest, CommonGramsWindowedMergeKeepsGramDocsOnlyAndPlainScoring) {
    constexpr uint32_t kDocCount = 600;
    const std::string gram = common_gram("the", "cat");
    std::vector<PostingDoc> gram_docs;
    std::vector<PostingDoc> plain_docs;
    gram_docs.reserve(kDocCount);
    plain_docs.reserve(kDocCount);
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        gram_docs.push_back({.docid = docid, .positions = {1}});
        plain_docs.push_back({.docid = docid, .positions = {0, 2}});
    }

    OpenedIndex source;
    build_index(make_common_grams_input(kDocCount, /*null_docids=*/ {},
                                        /*norms=*/std::vector<uint8_t>(kDocCount, 1),
                                        {make_term(gram, std::move(gram_docs)),
                                         make_term("alpha", std::move(plain_docs))},
                                        /*token_count=*/2 * kDocCount),
                &source, reader::LogicalIndexOpenMode::kCompaction);

    RowIdConversionMap conversion(1);
    conversion[0].reserve(kDocCount);
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        conversion[0].emplace_back(0, docid);
    }
    auto validated = make_validated_conversion(&conversion, {kDocCount}, {kDocCount});
    ASSERT_NE(validated, nullptr);
    compaction::SniiCompactionEligibility eligibility {
            .kind = compaction::SniiStreamedMergeKind::kCommonGramsT3,
            .common_grams_metadata_seed = common_grams_metadata(0, 0)};
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source.index}, *validated, eligibility,
                                            /*total_read_ahead_budget_bytes=*/1U << 20, &plan));
    ASSERT_NE(plan, nullptr);

    MemoryFile merged_file;
    SniiCompoundWriter compound(&merged_file);
    SniiStreamedIndexSession* session = nullptr;
    SniiIndexInput input = make_input(kDocCount, {}, {});
    input.config = plan->destination_index_config();
    input.write_freq = true;
    input.common_grams_metadata = plan->destination_common_grams_metadata(0);
    assert_ok(compound.begin_streamed_index(std::move(input), plan->take_destination_null_docids(0),
                                            plan->take_destination_encoded_norms(0), &session));
    std::array<SniiStreamedIndexSession*, 1> sessions = {session};
    assert_ok(plan->execute(sessions));
    assert_ok(compound.finish());

    reader::SniiSegmentReader merged_segment;
    reader::LogicalIndexReader merged_index;
    assert_ok(reader::SniiSegmentReader::open(&merged_file, &merged_segment));
    assert_ok(merged_segment.open_index(kIndexId, kIndexSuffix, &merged_index));

    for (const reader::LogicalIndexReader* index : {&source.index, &merged_index}) {
        bool found = false;
        format::DictEntry gram_entry;
        format::DictEntry plain_entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(index->lookup(gram, &found, &gram_entry, &frq_base, &prx_base));
        ASSERT_TRUE(found);
        assert_ok(index->lookup("alpha", &found, &plain_entry, &frq_base, &prx_base));
        ASSERT_TRUE(found);

        EXPECT_EQ(gram_entry.kind, format::DictEntryKind::kPodRef);
        EXPECT_EQ(gram_entry.enc, format::DictEntryEnc::kWindowed);
        EXPECT_FALSE(gram_entry.term_stats_present);
        EXPECT_EQ(gram_entry.frq_len, gram_entry.frq_docs_len);
        EXPECT_EQ(plain_entry.kind, format::DictEntryKind::kPodRef);
        EXPECT_EQ(plain_entry.enc, format::DictEntryEnc::kWindowed);
        EXPECT_TRUE(plain_entry.term_stats_present);
        EXPECT_EQ(plain_entry.ttf_delta, 2 * kDocCount);
        EXPECT_EQ(plain_entry.max_freq, 2U);
        EXPECT_GT(plain_entry.frq_len, plain_entry.frq_docs_len);
    }

    ASSERT_NE(merged_index.common_grams_metadata(), nullptr);
    EXPECT_EQ(merged_index.common_grams_metadata()->common_grams_coverage,
              inverted_index::CommonGramsCoverage::kComplete);
    EXPECT_EQ(merged_index.common_grams_metadata()->scoring_token_count, 2 * kDocCount);
}

TEST(SniiIndexCompactionTest, CommonGramsMergeReclaimsResidentDictBeforeLargePlainTerm) {
    constexpr size_t kGramPositions = 4096;
    constexpr size_t kPlainPositions = 16384;
    auto make_positions = [](size_t count, uint32_t salt) {
        std::vector<uint32_t> positions;
        positions.reserve(count);
        uint32_t position = salt;
        for (size_t i = 0; i < count; ++i) {
            position += 17 + static_cast<uint32_t>((i * 257 + salt) & 1023);
            positions.push_back(position);
        }
        return positions;
    };

    std::vector<TermPostings> source_terms;
    for (uint32_t i = 0; i < 32; ++i) {
        source_terms.push_back(
                make_term(common_gram("the", std::string("gram-") + std::to_string(100 + i)),
                          {{.docid = 0, .positions = make_positions(kGramPositions, i + 1)}}));
    }
    source_terms.push_back(make_term(
            "zeta", {{.docid = 0, .positions = make_positions(kPlainPositions, /*salt=*/99)}}));

    SniiIndexInput source_input =
            make_common_grams_input(/*doc_count=*/1, /*null_docids=*/ {}, /*norms=*/ {1},
                                    std::move(source_terms), /*token_count=*/kPlainPositions);
    source_input.target_dict_block_bytes = 1;
    OpenedIndex source;
    build_index(std::move(source_input), &source, reader::LogicalIndexOpenMode::kCompaction);
    const RowIdConversionMap conversion = {{{0, 0}}};
    auto validated = make_validated_conversion(&conversion, {1}, {1});
    ASSERT_NE(validated, nullptr);

    constexpr size_t kReadAhead = SniiPlainT2MergePlan::kMinReadAheadBudgetPerSource;
    constexpr size_t kHardCap = 256U << 10;
    auto reporter = std::make_shared<MemoryReporter>(nullptr, kHardCap);
    compaction::SniiCompactionEligibility eligibility {
            .kind = compaction::SniiStreamedMergeKind::kCommonGramsT3,
            .common_grams_metadata_seed = common_grams_metadata(0, 0)};
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source.index}, *validated, eligibility, kReadAhead,
                                            reporter, &plan));
    ASSERT_NE(plan, nullptr);

    MemoryFile merged_file;
    SniiCompoundWriter compound(&merged_file);
    SniiIndexInput input = make_input(/*doc_count=*/1, {}, {});
    input.config = plan->destination_index_config();
    input.write_freq = true;
    input.target_dict_block_bytes = 1;
    input.dict_resident_cap_bytes = kHardCap / 8;
    input.mem_reporter = reporter.get();
    input.common_grams_metadata = plan->destination_common_grams_metadata(0);
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(input), plan->take_destination_null_docids(0),
                                            plan->take_destination_encoded_norms(0), &session));
    std::array<SniiStreamedIndexSession*, 1> sessions = {session};
    assert_ok(plan->execute(sessions));
    assert_ok(compound.finish());

    reader::SniiSegmentReader merged_segment;
    reader::LogicalIndexReader merged_index;
    assert_ok(reader::SniiSegmentReader::open(&merged_file, &merged_segment));
    assert_ok(merged_segment.open_index(kIndexId, kIndexSuffix, &merged_index));
    std::vector<uint32_t> docs;
    assert_ok(term_query(merged_index, "zeta", &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
}

TEST(SniiIndexCompactionTest, DecodesWindowedSourceAndReencodesEachDestination) {
    std::vector<PostingDoc> source_docs;
    source_docs.reserve(600);
    for (uint32_t docid = 0; docid < 600; ++docid) {
        source_docs.push_back({.docid = docid, .positions = {1, 4, 9}});
    }
    OpenedIndex source;
    build_index(make_input(/*doc_count=*/600, /*null_docids=*/ {},
                           {make_term("wide", std::move(source_docs))}),
                &source);
    bool found = false;
    format::DictEntry source_entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(source.index.lookup("wide", &found, &source_entry, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    ASSERT_EQ(source_entry.enc, format::DictEntryEnc::kWindowed);

    RowIdConversionMap conversion(1);
    conversion[0].reserve(600);
    for (uint32_t docid = 0; docid < 600; ++docid) {
        conversion[0].emplace_back(docid / 300, docid % 300);
    }
    const std::vector<uint32_t> destination_rows = {300, 300};
    auto validated = make_validated_conversion(&conversion, {600}, destination_rows);
    ASSERT_NE(validated, nullptr);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source.index}, *validated,
                                            /*total_read_ahead_budget_bytes=*/1U << 20, &plan));

    std::array<MemoryFile, 2> merged_files;
    std::array<std::unique_ptr<SniiCompoundWriter>, 2> compounds;
    std::array<SniiStreamedIndexSession*, 2> sessions = {nullptr, nullptr};
    for (size_t i = 0; i < compounds.size(); ++i) {
        compounds[i] = std::make_unique<SniiCompoundWriter>(&merged_files[i]);
        SniiIndexInput input = make_input(destination_rows[i], {}, {});
        assert_ok(compounds[i]->begin_streamed_index(std::move(input), &sessions[i]));
    }
    compaction::testing::reset_posting_run_merge_counters();
    assert_ok(plan->execute(sessions));
    EXPECT_EQ(compaction::testing::posting_run_frontier_comparisons(), 0U);
    EXPECT_GT(compaction::testing::posting_run_documents(), 0U);
    for (auto& compound : compounds) {
        assert_ok(compound->finish());
    }

    std::vector<PostingDoc> expected_docs;
    expected_docs.reserve(300);
    for (uint32_t docid = 0; docid < 300; ++docid) {
        expected_docs.push_back({.docid = docid, .positions = {1, 4, 9}});
    }
    for (size_t i = 0; i < merged_files.size(); ++i) {
        OpenedIndex expected;
        build_index(make_input(/*doc_count=*/300, /*null_docids=*/ {},
                               {make_term("wide", expected_docs)}),
                    &expected);
        expect_identical_index_image(&merged_files[i], &expected.file);
    }
}

TEST(SniiIndexCompactionTest, RejectsInsufficientBudgetBeforeCreatingPlan) {
    OpenedIndex source;
    build_index(make_input(/*doc_count=*/1, /*null_docids=*/ {},
                           {make_term("alpha", {{.docid = 0, .positions = {0}}})}),
                &source);
    const RowIdConversionMap conversion = {{{0, 0}}};
    auto validated = make_validated_conversion(&conversion, {1}, {1});
    ASSERT_NE(validated, nullptr);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    const Status status = SniiPlainT2MergePlan::prepare({&source.index}, *validated,
                                                        /*total_read_ahead_budget_bytes=*/1, &plan);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status;
    EXPECT_EQ(plan, nullptr);
}

TEST(SniiIndexCompactionTest, ReadAheadHardReservationFailureUnwindsAllCharges) {
    OpenedIndex source;
    build_index(make_input(/*doc_count=*/1, /*null_docids=*/ {},
                           {make_term("alpha", {{.docid = 0, .positions = {0}}})}),
                &source);
    const RowIdConversionMap conversion = {{{0, 0}}};
    auto validated = make_validated_conversion(&conversion, {1}, {1});
    ASSERT_NE(validated, nullptr);

    constexpr size_t kReadAhead = SniiPlainT2MergePlan::kMinReadAheadBudgetPerSource;
    auto reporter = std::make_shared<MemoryReporter>(nullptr, kReadAhead - 1);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    const Status status =
            SniiPlainT2MergePlan::prepare({&source.index}, *validated, kReadAhead, reporter, &plan);
    EXPECT_TRUE(status.is<doris::ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
    EXPECT_EQ(plan, nullptr);
    EXPECT_EQ(reporter->current_bytes(), 0);
}

TEST(SniiIndexCompactionTest, NullRemapMemoryIsReservedBeforeReadAhead) {
    constexpr uint32_t kDocCount = 128;
    std::vector<uint32_t> null_docids;
    RowIdConversionMap conversion(1);
    null_docids.reserve(kDocCount);
    conversion[0].reserve(kDocCount);
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        null_docids.push_back(docid);
        conversion[0].emplace_back(0, docid);
    }

    OpenedIndex source;
    build_index(make_input(kDocCount, std::move(null_docids), {}), &source);
    auto validated = make_validated_conversion(&conversion, {kDocCount}, {kDocCount});
    ASSERT_NE(validated, nullptr);

    constexpr size_t kReadAhead = SniiPlainT2MergePlan::kMinReadAheadBudgetPerSource;
    auto reporter = std::make_shared<MemoryReporter>(nullptr, kReadAhead);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    const Status status =
            SniiPlainT2MergePlan::prepare({&source.index}, *validated, kReadAhead, reporter, &plan);
    EXPECT_TRUE(status.is<doris::ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
    EXPECT_EQ(plan, nullptr);
    EXPECT_EQ(reporter->current_bytes(), 0);
}

TEST(SniiIndexCompactionTest, NullRemapExactCapIsRetainedUntilPlanDestruction) {
    constexpr uint32_t kDocCount = 128;
    std::vector<uint32_t> null_docids;
    RowIdConversionMap conversion(1);
    null_docids.reserve(kDocCount);
    conversion[0].reserve(kDocCount);
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        null_docids.push_back(docid);
        conversion[0].emplace_back(0, docid);
    }

    OpenedIndex source;
    build_index(make_input(kDocCount, null_docids, {}), &source);
    auto validated = make_validated_conversion(&conversion, {kDocCount}, {kDocCount});
    ASSERT_NE(validated, nullptr);

    constexpr size_t kReadAhead = SniiPlainT2MergePlan::kMinReadAheadBudgetPerSource;
    constexpr size_t kRetainedNullBytes = kDocCount * sizeof(uint32_t);
    constexpr size_t kExactCap = kReadAhead + kRetainedNullBytes;
    auto exact_reporter = std::make_shared<MemoryReporter>(nullptr, kExactCap);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source.index}, *validated, kReadAhead, exact_reporter,
                                            &plan));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->destination_null_docids(0), null_docids);
    EXPECT_EQ(exact_reporter->current_bytes(), static_cast<int64_t>(kExactCap));
    writer::TrackedNullDocids transferred = plan->take_destination_null_docids(0);
    EXPECT_EQ(transferred.size(), null_docids.size());
    plan.reset();
    EXPECT_EQ(exact_reporter->current_bytes(), static_cast<int64_t>(kRetainedNullBytes));
    transferred.release();
    EXPECT_EQ(exact_reporter->current_bytes(), 0);

    auto insufficient_reporter = std::make_shared<MemoryReporter>(nullptr, kExactCap - 1);
    const Status status = SniiPlainT2MergePlan::prepare({&source.index}, *validated, kReadAhead,
                                                        insufficient_reporter, &plan);
    EXPECT_TRUE(status.is<doris::ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
    EXPECT_EQ(plan, nullptr);
    EXPECT_EQ(insufficient_reporter->current_bytes(), 0);
}

TEST(SniiIndexCompactionTest, ExecuteBudgetFailureAbortsSessionAndKeepsChargeBalanced) {
    OpenedIndex source;
    build_index(make_input(/*doc_count=*/1, /*null_docids=*/ {},
                           {make_term("alpha", {{.docid = 0, .positions = {0}}})}),
                &source);
    const RowIdConversionMap conversion = {{{0, 0}}};
    auto validated = make_validated_conversion(&conversion, {1}, {1});
    ASSERT_NE(validated, nullptr);

    constexpr size_t kReadAhead = SniiPlainT2MergePlan::kMinReadAheadBudgetPerSource;
    auto reporter = std::make_shared<MemoryReporter>(nullptr, kReadAhead);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source.index}, *validated, kReadAhead, reporter,
                                            &plan));
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(reporter->current_bytes(), static_cast<int64_t>(kReadAhead));

    MemoryFile destination_file;
    SniiCompoundWriter compound(&destination_file);
    SniiIndexInput destination_input = make_input(/*doc_count=*/1, /*null_docids=*/ {}, {});
    destination_input.mem_reporter = reporter.get();
    SniiStreamedIndexSession* session = nullptr;
    assert_ok(compound.begin_streamed_index(std::move(destination_input), &session));
    ASSERT_NE(session, nullptr);

    const Status status = plan->execute(std::span(&session, 1));
    EXPECT_TRUE(status.is<doris::ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
    EXPECT_TRUE(session->finish().is<doris::ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_TRUE(compound.finish().is<doris::ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_TRUE(plan->execute(std::span(&session, 1)).is<doris::ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_EQ(reporter->current_bytes(), static_cast<int64_t>(kReadAhead));

    plan.reset();
    EXPECT_EQ(reporter->current_bytes(), 0);
}

TEST(SniiIndexCompactionTest, ReusesValidatedConversionAndRejectsLogicalIndexDocCountMismatch) {
    OpenedIndex first;
    OpenedIndex second;
    OpenedIndex mismatched;
    build_index(make_input(/*doc_count=*/1, /*null_docids=*/ {},
                           {make_term("first", {{.docid = 0, .positions = {0}}})}),
                &first);
    build_index(make_input(/*doc_count=*/1, /*null_docids=*/ {},
                           {make_term("second", {{.docid = 0, .positions = {0}}})}),
                &second);
    build_index(make_input(/*doc_count=*/2, /*null_docids=*/ {},
                           {make_term("mismatch", {{.docid = 1, .positions = {0}}})}),
                &mismatched);

    const RowIdConversionMap conversion = {{{0, 0}}};
    auto validated = make_validated_conversion(&conversion, {1}, {1});
    ASSERT_NE(validated, nullptr);
    std::unique_ptr<SniiPlainT2MergePlan> first_plan;
    std::unique_ptr<SniiPlainT2MergePlan> second_plan;
    std::unique_ptr<SniiPlainT2MergePlan> mismatched_plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&first.index}, *validated,
                                            /*total_read_ahead_budget_bytes=*/1U << 20,
                                            &first_plan));
    assert_ok(SniiPlainT2MergePlan::prepare({&second.index}, *validated,
                                            /*total_read_ahead_budget_bytes=*/1U << 20,
                                            &second_plan));
    const Status mismatch_status = SniiPlainT2MergePlan::prepare(
            {&mismatched.index}, *validated, /*total_read_ahead_budget_bytes=*/1U << 20,
            &mismatched_plan);
    EXPECT_TRUE(mismatch_status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << mismatch_status;
    EXPECT_EQ(mismatched_plan, nullptr);
}

TEST(SniiIndexCompactionTest, ExecuteFailureAbortsEveryDestinationSession) {
    OpenedIndex source;
    build_index(make_input(/*doc_count=*/1, /*null_docids=*/ {},
                           {make_term("alpha", {{.docid = 0, .positions = {0}}})}),
                &source);
    const RowIdConversionMap conversion = {{{0, 0}}};
    auto validated = make_validated_conversion(&conversion, {1}, {1});
    ASSERT_NE(validated, nullptr);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source.index}, *validated,
                                            /*total_read_ahead_budget_bytes=*/1U << 20, &plan));

    std::array<MemoryFile, 2> destination_files;
    std::array<std::unique_ptr<SniiCompoundWriter>, 2> compounds;
    std::array<SniiStreamedIndexSession*, 2> sessions = {nullptr, nullptr};
    for (size_t i = 0; i < compounds.size(); ++i) {
        compounds[i] = std::make_unique<SniiCompoundWriter>(&destination_files[i]);
        assert_ok(compounds[i]->begin_streamed_index(make_input(1, {}, {}), &sessions[i]));
    }

    const Status status = plan->execute(sessions);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status;
    for (size_t i = 0; i < sessions.size(); ++i) {
        EXPECT_TRUE(sessions[i]->finish().is<doris::ErrorCode::INVALID_ARGUMENT>());
        EXPECT_TRUE(compounds[i]->finish().is<doris::ErrorCode::INVALID_ARGUMENT>());
        EXPECT_FALSE(destination_files[i].finalized());
    }
}

TEST(SniiIndexCompactionTest, StickyExecuteFailureAbortsNewDestinationSession) {
    OpenedIndex source;
    build_index(make_input(/*doc_count=*/1, /*null_docids=*/ {},
                           {make_term("alpha", {{.docid = 0, .positions = {0}}})}),
                &source);
    const RowIdConversionMap conversion = {{{0, 0}}};
    auto validated = make_validated_conversion(&conversion, {1}, {1});
    ASSERT_NE(validated, nullptr);
    std::unique_ptr<SniiPlainT2MergePlan> plan;
    assert_ok(SniiPlainT2MergePlan::prepare({&source.index}, *validated,
                                            /*total_read_ahead_budget_bytes=*/1U << 20, &plan));

    MemoryFile first_file;
    SniiCompoundWriter first_compound(&first_file);
    SniiStreamedIndexSession* first_session = nullptr;
    assert_ok(first_compound.begin_streamed_index(make_input(1, {}, {}), &first_session));
    std::array<SniiStreamedIndexSession*, 2> mismatched_sessions = {first_session, first_session};
    const Status first_status = plan->execute(mismatched_sessions);
    EXPECT_TRUE(first_status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << first_status;

    MemoryFile retry_file;
    SniiCompoundWriter retry_compound(&retry_file);
    SniiStreamedIndexSession* retry_session = nullptr;
    assert_ok(retry_compound.begin_streamed_index(make_input(1, {}, {}), &retry_session));
    const Status retry_status = plan->execute(std::span(&retry_session, 1));
    EXPECT_EQ(retry_status.to_string(), first_status.to_string());
    EXPECT_TRUE(retry_session->finish().is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(retry_compound.finish().is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_FALSE(retry_file.finalized());
}

} // namespace
