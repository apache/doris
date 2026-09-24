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

#include <algorithm>
#include <array>
#include <bit>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/query_v2/collect/top_k_collector.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_weight.h"
#include "storage/index/inverted/query_v2/term_query/term_weight.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "testutil/benchmark_control.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

constexpr uint32_t kRows = 4096;
constexpr const char* kDirectory = "./ut_dir/phrase_postings_bench";
constexpr std::array kFields {L"early", L"late", L"missing", L"repeated"};
constexpr std::array kNames {"early", "late", "missing", "repeated"};

uint32_t repetitions(uint32_t row) {
    return row % 63 + 1;
}

uint32_t expected_frequency(size_t field, uint32_t row) {
    if (row % 11 == 0 || field == 2) {
        return 0;
    }
    return field == 3 ? 2 * repetitions(row) - 1 : 1;
}

std::string phrase_text(size_t field, uint32_t row) {
    if (row % 11 == 0) {
        return "omega";
    }
    std::string text = field == 0 ? "alpha beta" : "";
    for (uint32_t occurrence = 0; occurrence < repetitions(row); ++occurrence) {
        text += field == 3 ? " alpha alpha" : " alpha";
    }
    if (field == 1) {
        text += " beta";
    } else if (field == 2) {
        text += " omega beta";
    }
    return text;
}

WeightPtr phrase_weight(size_t field, bool scoring) {
    std::vector<TermInfo> terms(2);
    terms[0].term = "alpha";
    terms[0].position = 0;
    terms[1].term = field == 3 ? "alpha" : "beta";
    terms[1].position = 1;
    SimilarityPtr similarity = scoring ? std::make_shared<BM25Similarity>(2.0F, 32.0F) : nullptr;
    return std::make_shared<PhraseWeight>(std::make_shared<IndexQueryContext>(), kFields[field],
                                          std::move(terms), index_query::PhraseQueryOptions {},
                                          std::move(similarity), scoring, false);
}

uint64_t phrase_hash_row(uint64_t hash, uint32_t doc, float score) {
    hash = (hash ^ doc) * 1099511628211ULL;
    return (hash ^ std::bit_cast<uint32_t>(score)) * 1099511628211ULL;
}

uint64_t execute_phrase(const WeightPtr& weight, const QueryExecutionContext& context, bool scoring,
                        uint32_t first) {
    auto scorer = weight->scorer(context);
    uint64_t hash = 1469598103934665603ULL;
    uint32_t doc = scorer->doc();
    if (doc < first) {
        doc = scorer->seek(first);
    }
    for (; doc != TERMINATED; doc = scorer->advance()) {
        hash = phrase_hash_row(hash, doc, scoring ? scorer->score() : 0.0F);
    }
    return hash;
}

uint64_t cpu_ns() {
    timespec value {};
    DORIS_CHECK_EQ(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value), 0);
    return uint64_t(value.tv_sec) * 1000000000ULL + value.tv_nsec;
}

uint32_t parameter(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

class PhrasePostingsBench : public testing::Test {
public:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kDirectory).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kDirectory).ok());
        for (size_t field = 0; field < kFields.size(); ++field) {
            const auto directory = std::string(kDirectory) + "/" + std::to_string(field);
            ASSERT_TRUE(io::global_local_filesystem()->create_directory(directory).ok());
            write_index(directory, field);
            _readers[field] = {lucene::index::IndexReader::open(directory.c_str()),
                               [](auto* reader) {
                                   reader->close();
                                   _CLDELETE(reader);
                               }};
            _context.field_reader_bindings.emplace(kFields[field], _readers[field]);
        }
        _context.segment_num_rows = kRows;
    }

    void TearDown() override {
        _context.field_reader_bindings.clear();
        for (auto& reader : _readers) {
            reader.reset();
        }
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kDirectory).ok());
    }

protected:
    static void write_index(const std::string& directory, size_t field,
                            const std::vector<std::string>* documents = nullptr) {
        CustomAnalyzerConfig::Builder config;
        config.with_tokenizer_config("standard", {});
        auto analyzer = CustomAnalyzer::build_custom_analyzer(config.build());
        auto input = std::make_shared<lucene::util::SStringReader<char>>();
        auto writer = std::make_unique<lucene::index::IndexWriter>(directory.c_str(),
                                                                   analyzer.get(), true);
        writer->setMaxBufferedDocs(kRows + 1);
        writer->setRAMBufferSizeMB(-1);
        writer->setUseCompoundFile(false);
        constexpr int flags = static_cast<int>(lucene::document::Field::STORE_NO) |
                              lucene::document::Field::INDEX_TOKENIZED;
        auto document = std::make_unique<lucene::document::Document>();
        auto* value = _CLNEW lucene::document::Field(kFields[field], flags);
        value->setOmitTermFreqAndPositions(false);
        document->add(*value);
        ErrorContext error_context;
        try {
            const auto count = documents == nullptr ? kRows : documents->size();
            for (uint32_t row = 0; row < count; ++row) {
                const auto text =
                        documents == nullptr ? phrase_text(field, row) : (*documents)[row];
                input->init(text.data(), text.size(), false);
                value->setValue(analyzer->reusableTokenStream(value->name(), input));
                writer->addDocument(document.get());
            }
        } catch (...) {
            error_context.eptr = std::current_exception();
        }
        FINALLY_EXCEPTION({ FINALLY_CLOSE(writer); });
    }

    uint64_t expected_hash(size_t field, bool scoring, uint32_t first) const {
        BM25Similarity similarity(2.0F, 32.0F);
        const auto* norms = _readers[field]->norms(kFields[field]);
        uint64_t hash = 1469598103934665603ULL;
        for (uint32_t row = first; row < kRows; ++row) {
            const auto frequency = expected_frequency(field, row);
            if (frequency != 0) {
                const float score = scoring ? similarity.score(frequency, norms[row]) : 0.0F;
                hash = phrase_hash_row(hash, row, score);
            }
        }
        return hash;
    }

    std::vector<TermScorerPtr> wand_terms(size_t field, size_t count, uint32_t first) const {
        std::vector<TermScorerPtr> scorers;
        for (size_t clause = 0; clause < count; ++clause) {
            const auto* term = field == 3 ? L"omega" : L"beta";
            if (clause == 0) {
                term = L"alpha";
            }
            TermWeight weight(std::make_shared<IndexQueryContext>(), kFields[field], term,
                              std::make_shared<BM25Similarity>(2.0F, 32.0F), true);
            auto scorer = std::dynamic_pointer_cast<TermScorer>(weight.scorer(_context, {}));
            DORIS_CHECK(scorer != nullptr);
            if (scorer->doc() < first) {
                scorer->seek(first);
            }
            scorers.push_back(std::move(scorer));
        }
        return scorers;
    }

    std::vector<ScoredDoc> full_scan_terms(size_t field, size_t count, uint32_t first) const {
        std::array<float, kRows> scores {};
        std::array<bool, kRows> matched {};
        for (auto& scorer : wand_terms(field, count, first)) {
            for (auto doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
                scores[doc] += scorer->score();
                matched[doc] = true;
            }
        }
        std::vector<ScoredDoc> result;
        for (uint32_t doc = first; doc < kRows; ++doc) {
            if (matched[doc]) {
                result.emplace_back(doc, scores[doc]);
            }
        }
        return result;
    }

    static uint32_t term_frequency(size_t field, size_t clause, uint32_t row) {
        if (row % 11 == 0) {
            return field == 3 && clause == 1 ? 1 : 0;
        }
        if (clause == 1) {
            return field == 3 ? 0 : 1;
        }
        return field == 3 ? 2 * repetitions(row) : repetitions(row) + (field == 0 ? 1 : 0);
    }

    void verify_full_scan(size_t field, size_t count, uint32_t first,
                          const std::vector<ScoredDoc>& rows) const {
        BM25Similarity similarity(2.0F, 32.0F);
        const auto* norms = _readers[field]->norms(kFields[field]);
        size_t index = 0;
        for (uint32_t doc = first; doc < kRows; ++doc) {
            bool matched = false;
            float expected = 0.0F;
            for (size_t clause = 0; clause < count; ++clause) {
                const auto frequency = term_frequency(field, clause, doc);
                if (frequency != 0) {
                    matched = true;
                    expected += similarity.score(frequency, norms[doc]);
                }
            }
            if (matched) {
                ASSERT_LT(index, rows.size());
                EXPECT_EQ(rows[index].doc_id, doc);
                EXPECT_FLOAT_EQ(rows[index].score, expected);
                ++index;
            }
        }
        EXPECT_EQ(index, rows.size());
    }

    std::vector<ScoredDoc> wand_top_k(size_t field, size_t count, uint32_t first, size_t k) const {
        TopKCollector collector(k);
        BlockWand::execute(
                wand_terms(field, count, first), collector.threshold(),
                [&](uint32_t doc, float score) { return collector.collect(doc, score); });
        return collector.into_sorted_vec();
    }

    void verify_wand(size_t field, size_t count, uint32_t first) const {
        auto expected = full_scan_terms(field, count, first);
        verify_full_scan(field, count, first, expected);
        std::ranges::sort(expected, ScoredDocByScoreDesc {});
        for (size_t k : {1U, 7U, 128U}) {
            const auto actual = wand_top_k(field, count, first, k);
            ASSERT_EQ(actual.size(), std::min(k, expected.size()));
            for (size_t row = 0; row < actual.size(); ++row) {
                EXPECT_EQ(actual[row].doc_id, expected[row].doc_id);
                EXPECT_FLOAT_EQ(actual[row].score, expected[row].score);
            }
        }
    }

    static uint64_t top_k_hash(const std::vector<ScoredDoc>& rows) {
        uint64_t hash = 1469598103934665603ULL;
        for (const auto& row : rows) {
            hash = phrase_hash_row(hash, row.doc_id, row.score);
        }
        return hash;
    }

    void benchmark_wand(size_t field, size_t count, uint32_t first, size_t k) const {
        auto expected_rows = full_scan_terms(field, count, first);
        std::ranges::sort(expected_rows, ScoredDocByScoreDesc {});
        expected_rows.resize(std::min(k, expected_rows.size()));
        const auto expected = top_k_hash(expected_rows);
        ASSERT_EQ(top_k_hash(wand_top_k(field, count, first, k)), expected);
        const auto samples = parameter("QUERY_ENGINE_BENCH_SAMPLES", 32);
        const auto iterations = parameter("WAND_POSTINGS_BENCH_ITERATIONS", 8);
        const auto label = std::string("wand_postings/") + kNames[field] + "/" +
                           std::to_string(count) + "/" + std::to_string(first) + "/" +
                           std::to_string(k);
        for (uint32_t sample = 0; sample < samples; ++sample) {
            benchmark::wait_for_turn(label, sample);
            uint64_t checksum = 0;
            const auto start = cpu_ns();
            for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
                checksum += top_k_hash(wand_top_k(field, count, first, k));
            }
            const auto elapsed = cpu_ns() - start;
            ASSERT_EQ(checksum, expected * iterations);
            benchmark::report_sample(label, sample, iterations, elapsed, checksum);
        }
    }

    void verify_rows(size_t field, bool scoring, uint32_t first) const {
        const auto weight = phrase_weight(field, scoring);
        auto scorer = weight->scorer(_context);
        auto actual = scorer->doc();
        if (actual < first) {
            actual = scorer->seek(first);
        }
        const auto* norms = _readers[field]->norms(kFields[field]);
        BM25Similarity similarity(2.0F, 32.0F);
        for (uint32_t row = first; row < kRows; ++row) {
            const auto frequency = expected_frequency(field, row);
            if (frequency == 0) {
                continue;
            }
            ASSERT_EQ(actual, row);
            if (scoring) {
                EXPECT_FLOAT_EQ(scorer->score(), similarity.score(frequency, norms[row]));
            }
            actual = scorer->advance();
        }
        EXPECT_EQ(actual, TERMINATED);
    }

    void benchmark_case(size_t field, bool scoring, uint32_t first) const {
        const auto weight = phrase_weight(field, scoring);
        const auto expected = expected_hash(field, scoring, first);
        ASSERT_EQ(execute_phrase(weight, _context, scoring, first), expected);
        const auto samples = parameter("QUERY_ENGINE_BENCH_SAMPLES", 32);
        const auto iterations = parameter("PHRASE_POSTINGS_BENCH_ITERATIONS", 8);
        const auto label = std::string("phrase_postings/") + kNames[field] +
                           (scoring ? "/scored/" : "/unscored/") + std::to_string(first);
        for (uint32_t sample = 0; sample < samples; ++sample) {
            benchmark::wait_for_turn(label, sample);
            uint64_t checksum = 0;
            const auto start = cpu_ns();
            for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
                checksum += execute_phrase(weight, _context, scoring, first);
            }
            const auto elapsed = cpu_ns() - start;
            ASSERT_EQ(checksum, expected * iterations);
            benchmark::report_sample(label, sample, iterations, elapsed, checksum);
        }
    }

    std::array<std::shared_ptr<lucene::index::IndexReader>, kFields.size()> _readers;
    QueryExecutionContext _context;
};

TEST_F(PhrasePostingsBench, PhysicalPositionsPreserveFrequenciesNormsAndSeek) {
    for (size_t field = 0; field < kFields.size(); ++field) {
        SCOPED_TRACE(kNames[field]);
        const auto* norms = _readers[field]->norms(kFields[field]);
        ASSERT_NE(norms, nullptr);
        const std::set<uint8_t> unique_norms(norms, norms + kRows);
        EXPECT_GT(unique_norms.size(), 1);
        for (bool scoring : {false, true}) {
            verify_rows(field, scoring, 0);
            verify_rows(field, scoring, kRows / 2);
        }
    }
}

TEST_F(PhrasePostingsBench, WandMatchesFullScanWithPhysicalFrequenciesAndNorms) {
    for (size_t field = 0; field < kFields.size(); ++field) {
        SCOPED_TRACE(kNames[field]);
        for (size_t count : {1U, 2U}) {
            SCOPED_TRACE(count);
            verify_wand(field, count, 0);
            verify_wand(field, count, kRows / 2);
        }
    }
}

TEST_F(PhrasePostingsBench, WandKeepsHighFrequencyWinnersAfterOtherTermsAreExhausted) {
    const auto directory = std::string(kDirectory) + "/high_frequency";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(directory).ok());
    std::vector<std::string> documents(2);
    for (uint32_t occurrence = 0; occurrence < 4095; ++occurrence) {
        documents[0] += "alpha ";
    }
    for (uint32_t occurrence = 0; occurrence < 65535; ++occurrence) {
        documents[1] += "beta ";
    }
    write_index(directory, 0, &documents);
    std::shared_ptr<lucene::index::IndexReader> reader {
            lucene::index::IndexReader::open(directory.c_str()), [](auto* index) {
                index->close();
                _CLDELETE(index);
            }};
    _context.field_reader_bindings[kFields[0]] = reader;
    _context.segment_num_rows = documents.size();

    const auto full_scan = full_scan_terms(0, 2, 0);
    ASSERT_EQ(full_scan.size(), 2);
    ASSERT_LT(full_scan[0].score, full_scan[1].score);
    const auto actual = wand_top_k(0, 2, 0, 1);
    ASSERT_EQ(actual.size(), 1);
    EXPECT_EQ(actual[0].doc_id, full_scan[1].doc_id);
    EXPECT_FLOAT_EQ(actual[0].score, full_scan[1].score);
}

TEST_F(PhrasePostingsBench, DISABLED_PhysicalWandQueries) {
    for (size_t field = 0; field < kFields.size(); ++field) {
        for (size_t count : {1U, 2U}) {
            for (uint32_t first : {0U, kRows / 2}) {
                for (size_t k : {1U, 128U}) {
                    benchmark_wand(field, count, first, k);
                }
            }
        }
    }
}

TEST_F(PhrasePostingsBench, DISABLED_PhysicalPhraseQueries) {
    for (size_t field = 0; field < kFields.size(); ++field) {
        for (bool scoring : {false, true}) {
            benchmark_case(field, scoring, 0);
            benchmark_case(field, scoring, kRows / 2);
        }
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index::query_v2
