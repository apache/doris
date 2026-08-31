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

#include "storage/index/snii/query/scoring_query.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <roaring/roaring.hh>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "runtime/thread_context.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::writer;
using doris::snii::query::Bm25Params;
using doris::snii::query::ScoredDoc;
using doris::snii::stats::SniiStatsProvider;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_score_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

class ControllableFileReader final : public io::FileReader {
public:
    explicit ControllableFileReader(io::FileReader* inner) : inner_(inner) {}

    doris::Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        read_at_calls_.fetch_add(1, std::memory_order_relaxed);
        if (fail_next_read_.exchange(false, std::memory_order_acq_rel)) {
            return doris::Status::IOError<false>("injected norms read failure");
        }

        {
            std::unique_lock lock(mutex_);
            if (block_next_read_) {
                block_next_read_ = false;
                blocked_read_started_ = true;
                condition_.notify_all();
                condition_.wait(lock, [this] { return blocked_read_released_; });
            }
        }
        return inner_->read_at(offset, len, out);
    }

    uint64_t size() const override { return inner_->size(); }

    void reset_read_at_calls() { read_at_calls_.store(0, std::memory_order_relaxed); }
    uint64_t read_at_calls() const { return read_at_calls_.load(std::memory_order_relaxed); }
    void fail_next_read() { fail_next_read_.store(true, std::memory_order_release); }

    void block_next_read() {
        std::lock_guard lock(mutex_);
        block_next_read_ = true;
        blocked_read_started_ = false;
        blocked_read_released_ = false;
    }

    void wait_for_blocked_read() {
        std::unique_lock lock(mutex_);
        condition_.wait(lock, [this] { return blocked_read_started_; });
    }

    void release_blocked_read() {
        std::lock_guard lock(mutex_);
        blocked_read_released_ = true;
        condition_.notify_all();
    }

private:
    io::FileReader* inner_ = nullptr;
    std::atomic<uint64_t> read_at_calls_ = 0;
    std::atomic<bool> fail_next_read_ = false;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool block_next_read_ = false;
    bool blocked_read_started_ = false;
    bool blocked_read_released_ = false;
};

// A small in-memory corpus: each doc is a bag of (term -> freq). Doc lengths vary
// so length normalization matters. "common" is a high-df term (~half the docs),
// "rare" is a low-df term.
struct Corpus {
    uint32_t doc_count = 0;
    // term -> (docid -> freq), docids ascending.
    std::map<std::string, std::map<uint32_t, uint32_t>> postings;
    std::vector<uint64_t> doc_len; // per-doc total token count
};

// Builds ~60 docs with varied lengths and a high-df + low-df term.
Corpus MakeCorpus() {
    Corpus c;
    c.doc_count = 60;
    c.doc_len.assign(c.doc_count, 0);

    auto add = [&](const std::string& term, uint32_t doc, uint32_t freq) {
        c.postings[term][doc] += freq;
        c.doc_len[doc] += freq;
    };

    for (uint32_t d = 0; d < c.doc_count; ++d) {
        // "common": appears in even docs, freq varies 1..4.
        if (d % 2 == 0) {
            add("common", d, 1 + (d % 4));
        }
        // "rare": appears in only a few docs.
        if (d == 3 || d == 17 || d == 42) {
            add("rare", d, 2);
        }
        // "filler": gives docs varied lengths so dl differs widely.
        add("filler", d, 1 + (d % 7) * 3);
        // a unique padding token per doc to spread lengths further.
        add("pad" + std::to_string(d % 11), d, (d % 5) + 1);
    }
    return c;
}

// Converts the corpus into a sorted SniiIndexInput with encoded norms.
SniiIndexInput ToInput(const Corpus& c) {
    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositionsScoring;
    in.doc_count = c.doc_count;
    in.target_dict_block_bytes = 1; // one block per term

    in.encoded_norms.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        in.encoded_norms[d] = doris::snii::query::encode_norm(c.doc_len[d]);
    }

    for (const auto& [term, plist] : c.postings) {
        TermPostings tp;
        tp.term = term;
        for (const auto& [docid, freq] : plist) {
            tp.docids.push_back(docid);
            tp.freqs.push_back(freq);
            for (uint32_t k = 0; k < freq; ++k) {
                tp.positions_flat.push_back(k); // flat
            }
        }
        in.terms.push_back(std::move(tp));
    }
    uint64_t token_count = 0;
    for (const auto& term : in.terms) {
        token_count += term.positions_flat.size();
    }
    in.common_grams_metadata = snii_test::make_plain_scoring_metadata(in.doc_count, token_count);
    return in;
}

// Reference BM25 ranking computed directly from the corpus (same encode/decode).
std::vector<ScoredDoc> ReferenceRanking(const Corpus& c, const std::vector<uint8_t>& norms,
                                        const std::vector<std::string>& terms, uint32_t k,
                                        const Bm25Params& params) {
    uint64_t sum_ttf = 0;
    for (const auto& dl : c.doc_len) {
        sum_ttf += dl;
    }
    const double avgdl = static_cast<double>(sum_ttf) / std::max<uint64_t>(1, c.doc_count);

    std::unordered_map<uint32_t, double> scores;
    for (const auto& term : terms) {
        auto it = c.postings.find(term);
        if (it == c.postings.end()) {
            continue;
        }
        const uint64_t df = it->second.size();
        const double idf =
                std::log(1.0 + (static_cast<double>(c.doc_count) - df + 0.5) / (df + 0.5));
        for (const auto& [docid, freq] : it->second) {
            const double dl = doris::snii::query::decode_norm(norms[docid]);
            const double denom = freq + params.k1 * (1.0 - params.b + params.b * dl / avgdl);
            scores[docid] += idf * (freq * (params.k1 + 1.0)) / denom;
        }
    }

    std::vector<ScoredDoc> all;
    all.reserve(scores.size());
    for (const auto& [docid, s] : scores) {
        all.push_back({docid, s});
    }
    std::ranges::sort(all, [](const ScoredDoc& a, const ScoredDoc& b) {
        if (a.score != b.score) {
            return a.score > b.score;
        }
        return a.docid < b.docid;
    });
    if (all.size() > k) {
        all.resize(k);
    }
    return all;
}

} // namespace

// Helper that mirrors ToInput's encoding so the reference path can decode norms.
namespace {
std::vector<uint8_t> EncodeNorms(const Corpus& c) {
    std::vector<uint8_t> v(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        v[d] = doris::snii::query::encode_norm(c.doc_len[d]);
    }
    return v;
}
} // namespace

// Fixture-free test: build, open, and compare.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiScoringQuery, ReferenceOracleAndWandEqualsExhaustive) {
    const Corpus corpus = MakeCorpus();
    const std::vector<uint8_t> norms = EncodeNorms(corpus);
    const std::string path = TempPath();

    // --- build the scoring index ---
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(cw.finish().ok());
    }

    // --- open via SniiSegmentReader over a MeteredFileReader ---
    io::LocalFileReader inner;
    ASSERT_TRUE(inner.open(path).ok());
    io::MeteredFileReader metered(&inner);
    reader::SniiSegmentReader seg;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered, &seg).ok());
    reader::LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    SniiStatsProvider stats;
    ASSERT_TRUE(SniiStatsProvider::open(&idx, &stats).ok());

    // (c) SniiStatsProvider df / ttf / avgdl / encoded_norm match brute force.
    uint64_t sum_ttf = 0;
    for (const auto& dl : corpus.doc_len) {
        sum_ttf += dl;
    }
    EXPECT_EQ(stats.indexed_doc_count(), corpus.doc_count);
    EXPECT_EQ(stats.sum_total_term_freq(), sum_ttf);
    EXPECT_NEAR(stats.avgdl(), static_cast<double>(sum_ttf) / corpus.doc_count, 1e-9);

    for (const auto& [term, plist] : corpus.postings) {
        uint64_t df = 0, ttf = 0;
        ASSERT_TRUE(stats.doc_freq(term, &df).ok());
        ASSERT_TRUE(stats.total_term_freq(term, &ttf).ok());
        uint64_t exp_ttf = 0;
        for (const auto& [d, f] : plist) {
            exp_ttf += f;
        }
        EXPECT_EQ(df, plist.size()) << term;
        EXPECT_EQ(ttf, exp_ttf) << term;
    }
    for (uint32_t d = 0; d < corpus.doc_count; ++d) {
        uint8_t got = 0;
        ASSERT_TRUE(stats.encoded_norm(d, &got).ok());
        EXPECT_EQ(got, norms[d]) << "docid " << d;
    }

    // (a) single-term scoring_query top-K matches the reference.
    const Bm25Params params; // defaults k1=1.2, b=0.75
    const uint32_t k = 10;

    auto run_and_check = [&](const std::vector<std::string>& terms) {
        std::vector<ScoredDoc> reference = ReferenceRanking(corpus, norms, terms, k, params);
        std::vector<ScoredDoc> exhaustive;
        ASSERT_TRUE(doris::snii::query::scoring_query_exhaustive(idx, stats, terms, k, params,
                                                                 &exhaustive)
                            .ok());
        std::vector<ScoredDoc> wand;
        ASSERT_TRUE(
                doris::snii::query::scoring_query_wand(idx, stats, terms, k, params, &wand).ok());

        ASSERT_EQ(exhaustive.size(), reference.size());
        for (size_t i = 0; i < reference.size(); ++i) {
            EXPECT_EQ(exhaustive[i].docid, reference[i].docid) << "rank " << i;
            EXPECT_NEAR(exhaustive[i].score, reference[i].score, 1e-6) << "rank " << i;
        }
        // (b) WAND-pruned top-K equals the exhaustive top-K.
        ASSERT_EQ(wand.size(), exhaustive.size());
        for (size_t i = 0; i < wand.size(); ++i) {
            EXPECT_EQ(wand[i].docid, exhaustive[i].docid) << "wand rank " << i;
            EXPECT_NEAR(wand[i].score, exhaustive[i].score, 1e-6) << "wand rank " << i;
        }
    };

    run_and_check({"common"});
    run_and_check({"rare"});
    run_and_check({"common", "rare"});
    run_and_check({"common", "rare", "filler"});

    std::remove(path.c_str());
}

TEST(SniiScoringQuery, StatsProviderSharesValidatedNormsAcrossQueries) {
    const Corpus corpus = MakeCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound_writer(&writer);
        ASSERT_TRUE(compound_writer.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(compound_writer.finish().ok());
    }

    io::LocalFileReader file_reader;
    ASSERT_TRUE(file_reader.open(path).ok());
    io::MeteredFileReader metered_reader(&file_reader, /*block_size=*/64);
    reader::SniiSegmentReader segment_reader;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered_reader, &segment_reader).ok());
    reader::LogicalIndexReader logical_reader;
    ASSERT_TRUE(segment_reader.open_index(1, "body", &logical_reader).ok());

    metered_reader.reset_metrics();
    const size_t memory_usage_before_load = logical_reader.memory_usage();
    SniiStatsProvider first;
    ASSERT_TRUE(SniiStatsProvider::open(&logical_reader, &first).ok());
    const io::IoMetrics after_first = metered_reader.metrics();
    EXPECT_GT(after_first.total_request_bytes, 0U);

    SniiStatsProvider second;
    ASSERT_TRUE(SniiStatsProvider::open(&logical_reader, &second).ok());
    EXPECT_EQ(metered_reader.metrics().read_at_calls, after_first.read_at_calls);
    EXPECT_EQ(metered_reader.metrics().total_request_bytes, after_first.total_request_bytes);
    EXPECT_EQ(logical_reader.memory_usage(), memory_usage_before_load);

    uint8_t first_norm = 0;
    uint8_t second_norm = 0;
    ASSERT_TRUE(first.encoded_norm(17, &first_norm).ok());
    ASSERT_TRUE(second.encoded_norm(17, &second_norm).ok());
    EXPECT_EQ(first_norm, second_norm);

    std::remove(path.c_str());
}

TEST(SniiScoringQuery, StatsProviderSharesOneConcurrentNormsLoad) {
    const Corpus corpus = MakeCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound_writer(&writer);
        ASSERT_TRUE(compound_writer.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(compound_writer.finish().ok());
    }

    io::LocalFileReader local_reader;
    ASSERT_TRUE(local_reader.open(path).ok());
    ControllableFileReader controlled_reader(&local_reader);
    reader::SniiSegmentReader segment_reader;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&controlled_reader, &segment_reader).ok());
    reader::LogicalIndexReader logical_reader;
    ASSERT_TRUE(segment_reader.open_index(1, "body", &logical_reader).ok());

    constexpr size_t kThreadCount = 16;
    std::barrier start(static_cast<std::ptrdiff_t>(kThreadCount + 1));
    std::vector<doris::Status> statuses(kThreadCount);
    std::vector<uint8_t> norms(kThreadCount);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    controlled_reader.reset_read_at_calls();
    controlled_reader.block_next_read();
    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i] {
            SCOPED_INIT_THREAD_CONTEXT();
            start.arrive_and_wait();
            SniiStatsProvider provider;
            statuses[i] = SniiStatsProvider::open(&logical_reader, &provider);
            if (statuses[i].ok()) {
                statuses[i] = provider.encoded_norm(17, &norms[i]);
            }
        });
    }
    start.arrive_and_wait();
    controlled_reader.wait_for_blocked_read();
    controlled_reader.release_blocked_read();
    for (auto& thread : threads) {
        thread.join();
    }

    for (size_t i = 0; i < kThreadCount; ++i) {
        EXPECT_TRUE(statuses[i].ok()) << statuses[i].to_string();
        EXPECT_EQ(norms[i], norms[0]);
    }
    EXPECT_EQ(controlled_reader.read_at_calls(), 1U);

    std::remove(path.c_str());
}

TEST(SniiScoringQuery, StatsProviderRetriesTransientNormsReadFailure) {
    const Corpus corpus = MakeCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound_writer(&writer);
        ASSERT_TRUE(compound_writer.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(compound_writer.finish().ok());
    }

    io::LocalFileReader local_reader;
    ASSERT_TRUE(local_reader.open(path).ok());
    ControllableFileReader controlled_reader(&local_reader);
    reader::SniiSegmentReader segment_reader;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&controlled_reader, &segment_reader).ok());
    reader::LogicalIndexReader logical_reader;
    ASSERT_TRUE(segment_reader.open_index(1, "body", &logical_reader).ok());

    controlled_reader.reset_read_at_calls();
    controlled_reader.fail_next_read();
    SniiStatsProvider failed;
    const doris::Status first_status = SniiStatsProvider::open(&logical_reader, &failed);
    EXPECT_TRUE(first_status.is<doris::ErrorCode::IO_ERROR>()) << first_status.to_string();

    SniiStatsProvider retry;
    ASSERT_TRUE(SniiStatsProvider::open(&logical_reader, &retry).ok());
    uint8_t norm = 0;
    ASSERT_TRUE(retry.encoded_norm(17, &norm).ok());
    EXPECT_EQ(controlled_reader.read_at_calls(), 2U);

    std::remove(path.c_str());
}

TEST(SniiScoringQuery, CandidatesUseCollectionStatisticsAndPreserveDuplicateClauses) {
    const Corpus corpus = MakeCorpus();
    const std::vector<uint8_t> norms = EncodeNorms(corpus);
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound_writer(&writer);
        ASSERT_TRUE(compound_writer.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(compound_writer.finish().ok());
    }

    io::LocalFileReader file_reader;
    ASSERT_TRUE(file_reader.open(path).ok());
    io::MeteredFileReader metered_reader(&file_reader);
    reader::SniiSegmentReader segment_reader;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered_reader, &segment_reader).ok());
    reader::LogicalIndexReader logical_reader;
    ASSERT_TRUE(segment_reader.open_index(1, "body", &logical_reader).ok());
    SniiStatsProvider segment_stats;
    ASSERT_TRUE(SniiStatsProvider::open(&logical_reader, &segment_stats).ok());

    constexpr double kCollectionAvgdl = 100.0;
    const Bm25Params params;
    const std::vector<doris::snii::query::CollectionScoringTerm> clauses {
            {.physical_term = "common", .idf = 0.25},
            {.physical_term = "rare", .idf = 2.5},
            {.physical_term = "rare", .idf = 2.5}};
    roaring::Roaring final_candidates;
    final_candidates.add(3);
    final_candidates.add(42);

    std::vector<ScoredDoc> scored;
    ASSERT_TRUE(doris::snii::query::scoring_query_candidates(logical_reader, segment_stats, clauses,
                                                             final_candidates, kCollectionAvgdl,
                                                             params, &scored)
                        .ok());

    ASSERT_EQ(scored.size(), 2u);
    EXPECT_EQ(scored[0].docid, 3u);
    EXPECT_EQ(scored[1].docid, 42u);
    auto expected_score = [&](uint32_t docid) {
        double score = 0.0;
        for (const auto& clause : clauses) {
            const auto& term_postings = corpus.postings.at(clause.physical_term);
            const auto posting = term_postings.find(docid);
            if (posting == term_postings.end()) {
                continue;
            }
            const double tf = posting->second;
            const double dl = doris::snii::query::decode_norm(norms[docid]);
            const double denominator =
                    tf + params.k1 * (1.0 - params.b + params.b * dl / kCollectionAvgdl);
            score += clause.idf * (tf * (params.k1 + 1.0)) / denominator;
        }
        return score;
    };
    EXPECT_NEAR(scored[0].score, expected_score(3), 1e-9);
    EXPECT_NEAR(scored[1].score, expected_score(42), 1e-9);

    std::remove(path.c_str());
}

TEST(SniiScoringQuery, CandidatesReadOnlyCoveredFrequencyWindows) {
    constexpr uint32_t kDocCount = 32 * format::kAdaptiveWindowDocs;
    constexpr double kCollectionAvgdl = 100.0;
    const std::string path = TempPath();
    SniiIndexInput input;
    input.index_id = 1;
    input.index_suffix = "body";
    input.config = IndexConfig::kDocsPositionsScoring;
    input.doc_count = kDocCount;
    input.target_dict_block_bytes = 1;
    input.encoded_norms.reserve(kDocCount);
    TermPostings posting;
    posting.term = "dense";
    posting.docids.reserve(kDocCount);
    posting.freqs.reserve(kDocCount);
    TermPostings leading_posting;
    leading_posting.term = "leading";
    leading_posting.docids.reserve(kDocCount / 2);
    leading_posting.freqs.reserve(kDocCount / 2);
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        const uint32_t frequency = 1 + docid % 4;
        input.encoded_norms.push_back(doris::snii::query::encode_norm(8 + docid % 17));
        posting.docids.push_back(docid);
        posting.freqs.push_back(frequency);
        for (uint32_t position = 0; position < frequency; ++position) {
            posting.positions_flat.push_back(position);
        }
        if (docid < kDocCount / 2) {
            leading_posting.docids.push_back(docid);
            leading_posting.freqs.push_back(1);
            leading_posting.positions_flat.push_back(0);
        }
    }
    input.terms.push_back(std::move(posting));
    input.terms.push_back(std::move(leading_posting));
    uint64_t token_count = 0;
    for (const auto& term : input.terms) {
        token_count += term.positions_flat.size();
    }
    input.common_grams_metadata =
            snii_test::make_plain_scoring_metadata(input.doc_count, token_count);
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound_writer(&writer);
        ASSERT_TRUE(compound_writer.add_logical_index(input).ok());
        ASSERT_TRUE(compound_writer.finish().ok());
    }

    io::LocalFileReader file_reader;
    ASSERT_TRUE(file_reader.open(path).ok());
    io::MeteredFileReader metered_reader(&file_reader, /*block_size=*/256);
    reader::SniiSegmentReader segment_reader;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered_reader, &segment_reader).ok());
    reader::LogicalIndexReader logical_reader;
    ASSERT_TRUE(segment_reader.open_index(1, "body", &logical_reader).ok());
    SniiStatsProvider segment_stats;
    ASSERT_TRUE(SniiStatsProvider::open(&logical_reader, &segment_stats).ok());

    const std::vector<doris::snii::query::CollectionScoringTerm> clauses {
            {.physical_term = "dense", .idf = 0.75}, {.physical_term = "dense", .idf = 0.75}};
    roaring::Roaring sparse_candidates;
    sparse_candidates.add(17);
    sparse_candidates.add(kDocCount - 19);
    metered_reader.reset_metrics();
    format::testing::reset_window_probe_count();
    std::vector<ScoredDoc> sparse_scores;
    ASSERT_TRUE(scoring_query_candidates(logical_reader, segment_stats, clauses, sparse_candidates,
                                         kCollectionAvgdl, Bm25Params {}, &sparse_scores)
                        .ok());
    const io::IoMetrics sparse_metrics = metered_reader.metrics();
    const uint64_t sparse_window_probes = format::testing::window_probe_count();

    roaring::Roaring clustered_candidates;
    clustered_candidates.addRange(0, 129);
    metered_reader.reset_metrics();
    format::testing::reset_window_probe_count();
    std::vector<ScoredDoc> clustered_scores;
    ASSERT_TRUE(scoring_query_candidates(logical_reader, segment_stats, clauses,
                                         clustered_candidates, kCollectionAvgdl, Bm25Params {},
                                         &clustered_scores)
                        .ok());
    const io::IoMetrics clustered_metrics = metered_reader.metrics();
    const uint64_t clustered_window_probes = format::testing::window_probe_count();

    roaring::Roaring dense_candidates;
    dense_candidates.addRange(0, kDocCount);
    metered_reader.reset_metrics();
    format::testing::reset_window_probe_count();
    std::vector<ScoredDoc> dense_scores;
    ASSERT_TRUE(scoring_query_candidates(logical_reader, segment_stats, clauses, dense_candidates,
                                         kCollectionAvgdl, Bm25Params {}, &dense_scores)
                        .ok());
    const io::IoMetrics dense_metrics = metered_reader.metrics();
    const uint64_t dense_window_probes = format::testing::window_probe_count();

    const std::vector<doris::snii::query::CollectionScoringTerm> leading_clause {
            {.physical_term = "leading", .idf = 0.5}};
    roaring::Roaring leading_hit_candidates;
    leading_hit_candidates.addRange(0, 129);
    metered_reader.reset_metrics();
    std::vector<ScoredDoc> leading_hit_scores;
    ASSERT_TRUE(scoring_query_candidates(logical_reader, segment_stats, leading_clause,
                                         leading_hit_candidates, kCollectionAvgdl, Bm25Params {},
                                         &leading_hit_scores)
                        .ok());
    const io::IoMetrics leading_hit_metrics = metered_reader.metrics();

    roaring::Roaring disjoint_candidates;
    disjoint_candidates.addRange(kDocCount / 2, kDocCount);
    metered_reader.reset_metrics();
    std::vector<ScoredDoc> disjoint_scores;
    ASSERT_TRUE(scoring_query_candidates(logical_reader, segment_stats, leading_clause,
                                         disjoint_candidates, kCollectionAvgdl, Bm25Params {},
                                         &disjoint_scores)
                        .ok());
    const io::IoMetrics disjoint_metrics = metered_reader.metrics();

    ASSERT_EQ(sparse_scores.size(), 2U);
    ASSERT_EQ(clustered_scores.size(), 129U);
    ASSERT_EQ(dense_scores.size(), kDocCount);
    const auto scorer = doris::snii::query::ScorerContext::from_idf(0.75);
    for (const auto& scored_doc : sparse_scores) {
        const uint32_t frequency = 1 + scored_doc.docid % 4;
        const uint8_t norm = input.encoded_norms[scored_doc.docid];
        const double expected = 2 * scorer.score(frequency, norm, kCollectionAvgdl, Bm25Params {});
        EXPECT_NEAR(scored_doc.score, expected, 1e-9);
        EXPECT_NEAR(dense_scores[scored_doc.docid].score, expected, 1e-9);
    }
    EXPECT_LT(sparse_metrics.total_request_bytes, dense_metrics.total_request_bytes);
    EXPECT_LT(sparse_metrics.remote_bytes, dense_metrics.remote_bytes);
    EXPECT_LT(clustered_metrics.total_request_bytes, dense_metrics.total_request_bytes);
    EXPECT_LT(clustered_metrics.remote_bytes, dense_metrics.remote_bytes);
    EXPECT_LE(sparse_metrics.serial_rounds, dense_metrics.serial_rounds);
    EXPECT_GT(sparse_window_probes, 0U);
    EXPECT_GT(clustered_window_probes, 0U);
    EXPECT_EQ(dense_window_probes, 0U);
    ASSERT_EQ(disjoint_scores.size(), kDocCount / 2);
    for (const auto& scored_doc : disjoint_scores) {
        EXPECT_DOUBLE_EQ(scored_doc.score, 0.0);
    }
    EXPECT_LT(disjoint_metrics.total_request_bytes, leading_hit_metrics.total_request_bytes);
    EXPECT_LT(disjoint_metrics.remote_bytes, leading_hit_metrics.remote_bytes);

    std::remove(path.c_str());
}

namespace {

// A corpus engineered to produce SCORE TIES at the top-k boundary and to drive
// the WINDOWED posting path: uniform doc length (so length-norm is constant) and
// high-df terms (df >= kSlimDfThreshold = 512 -> windowed pod_ref + frq_prelude).
// Every doc has the same length L=8, so docs sharing a term/freq score identically.
Corpus MakeWindowedTieCorpus() {
    Corpus c;
    c.doc_count = 700; // >= 512 so "anchor" becomes a windowed term
    c.doc_len.assign(c.doc_count, 0);
    auto add = [&](const std::string& term, uint32_t doc, uint32_t freq) {
        c.postings[term][doc] += freq;
        c.doc_len[doc] += freq;
    };
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        add("anchor", d, 1); // df=700 (windowed), freq=1 everywhere -> ties
        if (d % 2 == 0) {
            add("evenz", d, 1); // df=350 (windowed), another high-df term
        }
        add("u" + std::to_string(d), d, 6); // unique pad: keeps every dl == 8 exactly
    }
    return c;
}

} // namespace

// Differential: WAND top-k MUST equal exhaustive top-k EVEN with boundary ties and
// windowed (block-max) terms, across many k. Strict-'>' pruning would drop ties.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiScoringQuery, WandEqualsExhaustiveWithTiesAndWindowedTerms) {
    const Corpus corpus = MakeWindowedTieCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(cw.finish().ok());
    }
    io::LocalFileReader inner;
    ASSERT_TRUE(inner.open(path).ok());
    io::MeteredFileReader metered(&inner);
    reader::SniiSegmentReader seg;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered, &seg).ok());
    reader::LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    SniiStatsProvider stats;
    ASSERT_TRUE(SniiStatsProvider::open(&idx, &stats).ok());

    const Bm25Params params;
    const std::vector<uint8_t> norms = EncodeNorms(corpus);
    auto check = [&](const std::vector<std::string>& terms, uint32_t k) {
        std::vector<ScoredDoc> ex, wa;
        ASSERT_TRUE(scoring_query_exhaustive(idx, stats, terms, k, params, &ex).ok());
        ASSERT_TRUE(scoring_query_wand(idx, stats, terms, k, params, &wa).ok());
        const std::vector<ScoredDoc> ref = ReferenceRanking(corpus, norms, terms, k, params);
        ASSERT_EQ(wa.size(), ex.size());
        ASSERT_EQ(ex.size(), ref.size());
        for (size_t i = 0; i < ex.size(); ++i) {
            EXPECT_EQ(wa[i].docid, ex[i].docid)
                    << "terms[0]=" << terms[0] << " k=" << k << " i=" << i;
            EXPECT_EQ(ex[i].docid, ref[i].docid) << "ref k=" << k << " i=" << i;
            EXPECT_NEAR(wa[i].score, ex[i].score, 1e-9);
        }
    };
    // Single high-df term: all 700 docs tie -> top-k must be the k smallest docids.
    for (uint32_t k : {1U, 3U, 5U, 50U, 200U}) {
        check({"anchor"}, k);
    }
    // Two windowed terms: even docs score higher (two terms) -> ties within each tier.
    for (uint32_t k : {1U, 4U, 10U, 100U}) {
        check({"anchor", "evenz"}, k);
    }

    std::remove(path.c_str());
}
