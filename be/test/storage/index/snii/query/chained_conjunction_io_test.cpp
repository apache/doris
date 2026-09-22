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
#include <cstdint>
#include <cstdio>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/io_metrics.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// Pins the documents, docid sources and physical reads of the SNII chained
// conjunction. Every term below drives one of its paths: full windows that need
// no read, near-full windows that are scanned whole, covering windows, flat
// postings, a chain that empties early and a missing term.
namespace doris::snii::query {
namespace {

using index_query::IoMetrics;

constexpr uint32_t kDocCount = 20000;

struct TermSpec {
    const char* term;
    bool (*holds)(uint32_t doc);
};

const TermSpec kTerms[] = {
        {.term = "dense", .holds = [](uint32_t) { return true; }},
        {.term = "near_full", .holds = [](uint32_t doc) { return doc % 100 != 0; }},
        {.term = "wide", .holds = [](uint32_t doc) { return doc % 3 != 0; }},
        {.term = "sevenths", .holds = [](uint32_t doc) { return doc % 7 == 0; }},
        {.term = "fifths", .holds = [](uint32_t doc) { return doc % 5 == 0; }},
        {.term = "rare", .holds = [](uint32_t doc) { return doc % 997 == 0; }},
        {.term = "cluster", .holds = [](uint32_t doc) { return doc >= 5000 && doc < 5100; }},
        {.term = "evens", .holds = [](uint32_t doc) { return doc < 800 && doc % 80 == 0; }},
        {.term = "odds", .holds = [](uint32_t doc) { return doc < 800 && doc % 80 == 41; }},
};

std::vector<uint32_t> docs_of(const std::string& term) {
    std::vector<uint32_t> docs;
    for (const TermSpec& spec : kTerms) {
        if (term != spec.term) {
            continue;
        }
        for (uint32_t doc = 0; doc < kDocCount; ++doc) {
            if (spec.holds(doc)) {
                docs.push_back(doc);
            }
        }
    }
    return docs;
}

std::vector<uint32_t> intersect(const std::vector<uint32_t>& left,
                                const std::vector<uint32_t>& right) {
    std::vector<uint32_t> out;
    std::ranges::set_intersection(left, right, std::back_inserter(out));
    return out;
}

std::string corpus_path() {
    return "/tmp/snii_chained_conjunction_io_" + std::to_string(getpid()) + ".idx";
}

void write_corpus(const std::string& path) {
    writer::SpimiTermBuffer buffer(/*has_positions=*/true);
    for (uint32_t doc = 0; doc < kDocCount; ++doc) {
        uint32_t position = 0;
        for (const TermSpec& spec : kTerms) {
            if (spec.holds(doc)) {
                buffer.add_token(spec.term, doc, position++);
            }
        }
    }
    writer::SniiIndexInput input;
    input.index_id = 1;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = kDocCount;
    input.terms = buffer.finalize_sorted();
    input.target_dict_block_bytes = 256;
    io::LocalFileWriter file;
    ASSERT_TRUE(file.open(path).ok());
    writer::SniiCompoundWriter compound(&file);
    ASSERT_TRUE(compound.add_logical_index(input).ok());
    ASSERT_TRUE(compound.finish().ok());
}

std::string describe(const IoMetrics& io) {
    return "{.read_at_calls = " + std::to_string(io.read_at_calls) +
           ", .serial_rounds = " + std::to_string(io.serial_rounds) +
           ", .range_gets = " + std::to_string(io.range_gets) +
           ", .remote_bytes = " + std::to_string(io.remote_bytes) +
           ", .total_request_bytes = " + std::to_string(io.total_request_bytes) + "}";
}

void expect_io(const IoMetrics& actual, const IoMetrics& expected) {
    EXPECT_EQ(actual.read_at_calls, expected.read_at_calls) << describe(actual);
    EXPECT_EQ(actual.serial_rounds, expected.serial_rounds) << describe(actual);
    EXPECT_EQ(actual.range_gets, expected.range_gets) << describe(actual);
    EXPECT_EQ(actual.remote_bytes, expected.remote_bytes) << describe(actual);
    EXPECT_EQ(actual.total_request_bytes, expected.total_request_bytes) << describe(actual);
}

class SniiChainedConjunctionIoTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        path_ = new std::string(corpus_path());
        write_corpus(*path_);
    }

    static void TearDownTestSuite() {
        std::remove(path_->c_str());
        delete path_;
        path_ = nullptr;
    }

    void SetUp() override {
        ASSERT_TRUE(_local.open(*path_).ok());
        _metered = std::make_unique<io::MeteredFileReader>(&_local, /*block_size=*/1024);
        ASSERT_TRUE(reader::SniiSegmentReader::open(_metered.get(), &_segment).ok());
        ASSERT_TRUE(_segment.open_index(1, "body", &_index).ok());
    }

    struct AndRun {
        std::vector<uint32_t> docs;
        IoMetrics io;
    };

    AndRun run_and(const std::vector<std::string>& terms) {
        _metered->reset_metrics();
        AndRun run;
        const Status status = boolean_and(_index, terms, &run.docs);
        EXPECT_TRUE(status.ok()) << status;
        run.io = _metered->metrics();
        return run;
    }

    static std::vector<uint32_t> expected_and(const std::vector<std::string>& terms) {
        std::vector<uint32_t> docs = docs_of(terms.front());
        for (size_t i = 1; i < terms.size(); ++i) {
            docs = intersect(docs, docs_of(terms[i]));
        }
        return docs;
    }

    struct FilterRun {
        std::vector<internal::TermPlan> plans;
        std::vector<uint32_t> candidates;
        std::vector<internal::DocidSource> sources;
        IoMetrics io;
    };

    // The candidate-restricted conjunction the phrase executor runs, with sources.
    void run_filter(const std::vector<std::string>& terms, const std::vector<uint32_t>& initial,
                    FilterRun* run) {
        _metered->reset_metrics();
        io::BatchRangeFetcher round1(_index.reader());
        bool all_present = false;
        ASSERT_TRUE(internal::plan_terms(_index, terms, &round1, &run->plans, &all_present,
                                         /*need_positions=*/true)
                            .ok());
        ASSERT_TRUE(all_present);
        ASSERT_TRUE(round1.fetch().ok());
        ASSERT_TRUE(internal::open_preludes(round1, &run->plans, /*need_positions=*/true).ok());
        ASSERT_TRUE(internal::filter_docids_by_conjunction(_index, round1, run->plans, initial,
                                                           &run->candidates, &run->sources)
                            .ok());
        run->io = _metered->metrics();
    }

    // The term's documents inside the chunk's window, or all of them for a flat term.
    static std::vector<uint32_t> chunk_scope(const internal::DocidChunk& chunk,
                                             const internal::TermPlan& plan,
                                             std::vector<uint32_t> docs) {
        if (!chunk.windowed) {
            return docs;
        }
        format::WindowMeta meta;
        EXPECT_TRUE(plan.prelude.window(chunk.window, &meta).ok());
        const uint32_t first = chunk.window == 0 ? 0 : static_cast<uint32_t>(meta.win_base + 1);
        std::erase_if(docs, [&](uint32_t doc) { return doc < first || doc > meta.last_docid; });
        return docs;
    }

    // Checks one chunk's documents and their positions within its window.
    static void expect_chunk(const internal::DocidChunk& chunk, const std::vector<uint32_t>& scope,
                             const std::string& term) {
        EXPECT_EQ(chunk.prx_doc_count, scope.size()) << term;
        if (chunk.prx_doc_ordinals.empty()) {
            // No ordinals means the chunk holds every document of its window.
            EXPECT_EQ(chunk.docids, scope) << term;
            return;
        }
        ASSERT_EQ(chunk.prx_doc_ordinals.size(), chunk.docids.size()) << term;
        for (size_t i = 0; i < chunk.docids.size(); ++i) {
            ASSERT_LT(chunk.prx_doc_ordinals[i], scope.size()) << term;
            EXPECT_EQ(scope[chunk.prx_doc_ordinals[i]], chunk.docids[i]) << term;
        }
    }

    static std::vector<size_t> listing_order(const std::vector<internal::TermPlan>& plans) {
        std::vector<size_t> order(plans.size());
        for (size_t i = 0; i < order.size(); ++i) {
            order[i] = i;
        }
        std::ranges::sort(
                order, [&](size_t left, size_t right) { return plans[left].df < plans[right].df; });
        return order;
    }

    // Checks that each source holds the term's documents among the candidates the
    // term was listed against, window by window.
    static void expect_sources(const FilterRun& run, const std::vector<std::string>& terms,
                               const std::vector<uint32_t>& initial) {
        ASSERT_EQ(run.sources.size(), terms.size());
        const std::vector<size_t> order = listing_order(run.plans);
        std::vector<uint32_t> candidates = initial;
        for (size_t k = 0; k < order.size(); ++k) {
            const size_t term = order[k];
            const std::vector<uint32_t> all_docs = docs_of(terms[term]);
            const internal::DocidSource& source = run.sources[term];
            std::vector<uint32_t> listed;
            for (const internal::DocidChunk& chunk : source.chunks) {
                expect_chunk(chunk, chunk_scope(chunk, run.plans[term], all_docs), terms[term]);
                listed.insert(listed.end(), chunk.docids.begin(), chunk.docids.end());
            }
            candidates = intersect(all_docs, candidates);
            EXPECT_EQ(listed, candidates) << terms[term];
            EXPECT_EQ(source.docids_are_final_candidates, k + 1 == order.size()) << terms[term];
        }
        EXPECT_EQ(run.candidates, candidates);
    }

    static std::string* path_;
    io::LocalFileReader _local;
    std::unique_ptr<io::MeteredFileReader> _metered;
    reader::SniiSegmentReader _segment;
    reader::LogicalIndexReader _index;
};

std::string* SniiChainedConjunctionIoTest::path_ = nullptr;

// A rare flat driver keeps its candidates in full windows of the dense term,
// which are answered from the window metadata alone.
TEST_F(SniiChainedConjunctionIoTest, FullWindowsNeedNoRead) {
    const std::vector<std::string> terms = {"dense", "rare"};
    const AndRun run = run_and(terms);
    EXPECT_EQ(run.docs, expected_and(terms));
    expect_io(run.io, {.read_at_calls = 1,
                       .serial_rounds = 1,
                       .range_gets = 1,
                       .remote_bytes = 1024,
                       .total_request_bytes = 254});
}

// Candidates in one cluster read only the windows of the wide term covering it.
TEST_F(SniiChainedConjunctionIoTest, CoveringWindowsOfASparseCluster) {
    const std::vector<std::string> terms = {"wide", "cluster"};
    const AndRun run = run_and(terms);
    EXPECT_EQ(run.docs, expected_and(terms));
    expect_io(run.io, {.read_at_calls = 2,
                       .serial_rounds = 2,
                       .range_gets = 2,
                       .remote_bytes = 2048,
                       .total_request_bytes = 446});
}

// Many candidates on a near-full term scan every window, coalesced.
TEST_F(SniiChainedConjunctionIoTest, NearFullTermScansAllWindows) {
    const std::vector<std::string> terms = {"near_full", "fifths"};
    const AndRun run = run_and(terms);
    EXPECT_EQ(run.docs, expected_and(terms));
    expect_io(run.io, {.read_at_calls = 4,
                       .serial_rounds = 3,
                       .range_gets = 4,
                       .remote_bytes = 6144,
                       .total_request_bytes = 5078});
}

// Few candidates on the same near-full term still read covering windows only.
TEST_F(SniiChainedConjunctionIoTest, NearFullTermWithFewCandidatesReadsCoveringWindows) {
    const std::vector<std::string> terms = {"near_full", "cluster"};
    const AndRun run = run_and(terms);
    EXPECT_EQ(run.docs, expected_and(terms));
    expect_io(run.io, {.read_at_calls = 2,
                       .serial_rounds = 2,
                       .range_gets = 2,
                       .remote_bytes = 2048,
                       .total_request_bytes = 412});
}

// Three windowed terms narrow the candidates term by term.
TEST_F(SniiChainedConjunctionIoTest, ChainsThreeWindowedTerms) {
    const std::vector<std::string> terms = {"wide", "sevenths", "fifths"};
    const AndRun run = run_and(terms);
    EXPECT_EQ(run.docs, expected_and(terms));
    expect_io(run.io, {.read_at_calls = 6,
                       .serial_rounds = 4,
                       .range_gets = 6,
                       .remote_bytes = 8192,
                       .total_request_bytes = 6667});
}

// Two disjoint flat terms end the chain before the wide term reads a window.
TEST_F(SniiChainedConjunctionIoTest, EmptyIntermediateResultReadsNoLaterWindow) {
    const std::vector<std::string> terms = {"wide", "evens", "odds"};
    const AndRun run = run_and(terms);
    EXPECT_TRUE(run.docs.empty());
    expect_io(run.io, {.read_at_calls = 1,
                       .serial_rounds = 1,
                       .range_gets = 1,
                       .remote_bytes = 1024,
                       .total_request_bytes = 180});
}

// A missing term ends the query before any posting is read.
TEST_F(SniiChainedConjunctionIoTest, MissingTermReadsNoPosting) {
    const std::vector<std::string> terms = {"rare", "absent", "wide"};
    const AndRun run = run_and(terms);
    EXPECT_TRUE(run.docs.empty());
    expect_io(run.io, {});
}

// The phrase executor's candidate-restricted conjunction keeps each term's
// documents and their positions in its windows for the position reads.
TEST_F(SniiChainedConjunctionIoTest, CandidateFilterKeepsDocidSources) {
    const std::vector<std::string> terms = {"wide", "sevenths", "cluster"};
    std::vector<uint32_t> initial;
    for (uint32_t doc = 4000; doc < 12000; doc += 3) {
        initial.push_back(doc);
    }
    FilterRun run;
    run_filter(terms, initial, &run);
    expect_sources(run, terms, initial);
    expect_io(run.io, {.read_at_calls = 4,
                       .serial_rounds = 2,
                       .range_gets = 3,
                       .remote_bytes = 3072,
                       .total_request_bytes = 692});
}

// Without a restriction the first term's source lists all of its documents.
TEST_F(SniiChainedConjunctionIoTest, UnrestrictedConjunctionKeepsDocidSources) {
    const std::vector<std::string> terms = {"near_full", "fifths", "rare"};
    std::vector<uint32_t> all(kDocCount);
    for (uint32_t doc = 0; doc < kDocCount; ++doc) {
        all[doc] = doc;
    }
    FilterRun run;
    _metered->reset_metrics();
    io::BatchRangeFetcher round1(_index.reader());
    bool all_present = false;
    ASSERT_TRUE(internal::plan_terms(_index, terms, &round1, &run.plans, &all_present,
                                     /*need_positions=*/true)
                        .ok());
    ASSERT_TRUE(round1.fetch().ok());
    ASSERT_TRUE(internal::open_preludes(round1, &run.plans, /*need_positions=*/true).ok());
    ASSERT_TRUE(internal::build_docid_only_conjunction(_index, round1, run.plans, &run.candidates,
                                                       &run.sources)
                        .ok());
    run.io = _metered->metrics();
    expect_sources(run, terms, all);
    expect_io(run.io, {.read_at_calls = 4,
                       .serial_rounds = 3,
                       .range_gets = 4,
                       .remote_bytes = 6144,
                       .total_request_bytes = 5078});
}

} // namespace
} // namespace doris::snii::query
