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
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/null_bitmap_fetcher.h"
#include "storage/index/inverted/query_v2/term_query/term_weight.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "testutil/benchmark_control.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

constexpr uint32_t kPostingRows = 32768;
constexpr std::array kPostingFields {L"whole", L"shared", L"left", L"right"};
constexpr std::array kNullDivisors {0U, 11U, 11U, 13U};
constexpr std::array kDenseTerms {L"densea", L"denseb", L"densec", L"densed"};
constexpr std::array kSparseTerms {L"sparsea", L"sparseb", L"sparsec", L"sparsed"};
constexpr const char* kPostingDirectory = "./ut_dir/boolean_postings_bench";

bool posting_is_null(uint32_t field, uint32_t doc) {
    return kNullDivisors[field] != 0 && doc % kNullDivisors[field] == 0;
}

bool posting_matches(uint32_t clause, uint32_t doc, bool sparse) {
    uint32_t value = (doc + 1) * (clause * 4 + 17) * 2654435761U;
    value ^= value >> 16;
    return value % 97 < (sparse ? 4U : 64U);
}

uint32_t posting_field(uint32_t profile, uint32_t clause) {
    return profile < 2 ? profile : 2 + clause % 2;
}

class PostingNullIterator final : public segment_v2::IndexIterator {
public:
    explicit PostingNullIterator(uint32_t field) : _cache(1024 * 1024, 1) {
        for (uint32_t doc = 0; doc < kPostingRows; ++doc) {
            if (posting_is_null(field, doc)) {
                _nulls.add(doc);
            }
        }
    }
    segment_v2::IndexReaderPtr get_reader(segment_v2::IndexReaderType /*type*/) const override {
        return nullptr;
    }
    Status read_from_index(const segment_v2::IndexParam& /*param*/) override {
        return Status::OK();
    }
    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* handle) override {
        _cache.insert(_key, std::make_shared<roaring::Roaring>(_nulls), handle);
        return Status::OK();
    }
    Result<bool> has_null() override { return !_nulls.isEmpty(); }

private:
    roaring::Roaring _nulls;
    segment_v2::InvertedIndexQueryCache _cache;
    segment_v2::InvertedIndexQueryCache::CacheKey _key {
            .index_path = kPostingDirectory,
            .column_name = "nulls",
            .query_type = segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY,
            .value = ""};
};

class PostingNullResolver final : public NullBitmapResolver {
public:
    PostingNullResolver() {
        for (uint32_t field = 0; field < _fields.size(); ++field) {
            _fields[field] = std::make_unique<PostingNullIterator>(field);
        }
    }
    segment_v2::IndexIterator* iterator_for(const Scorer& /*scorer*/,
                                            const std::string& logical_field) const override {
        for (size_t field = 0; field < kPostingFields.size(); ++field) {
            const std::wstring name(kPostingFields[field]);
            if (logical_field == std::string(name.begin(), name.end())) {
                return _fields[field].get();
            }
        }
        return nullptr;
    }

private:
    std::array<std::unique_ptr<PostingNullIterator>, kPostingFields.size()> _fields;
};

class PostingBenchTerm final : public Query {
public:
    PostingBenchTerm(std::wstring field, std::wstring term)
            : _field(std::move(field)), _term(std::move(term)) {}
    WeightPtr weight(bool scoring) override {
        return std::make_shared<TermWeight>(std::make_shared<IndexQueryContext>(), _field, _term,
                                            std::make_shared<BM25Similarity>(2.0F, 8.0F), scoring);
    }

private:
    std::wstring _field;
    std::wstring _term;
};

struct PostingSpec {
    const char* name;
    bool logical;
    OperatorType op;
    std::array<Occur, 4> roles;
    uint32_t minimum;
};

constexpr std::array musts {Occur::MUST, Occur::MUST, Occur::MUST, Occur::MUST};
constexpr std::array shoulds {Occur::SHOULD, Occur::SHOULD, Occur::SHOULD, Occur::SHOULD};
constexpr std::array kPostingSpecs {
        PostingSpec {.name = "logical_and",
                     .logical = true,
                     .op = OperatorType::OP_AND,
                     .roles = musts,
                     .minimum = 0},
        PostingSpec {.name = "logical_or",
                     .logical = true,
                     .op = OperatorType::OP_OR,
                     .roles = shoulds,
                     .minimum = 1},
        PostingSpec {.name = "occur_and",
                     .logical = false,
                     .op = OperatorType::OP_AND,
                     .roles = musts,
                     .minimum = 0},
        PostingSpec {.name = "occur_or",
                     .logical = false,
                     .op = OperatorType::OP_OR,
                     .roles = shoulds,
                     .minimum = 1},
        PostingSpec {.name = "threshold",
                     .logical = false,
                     .op = OperatorType::OP_OR,
                     .roles = shoulds,
                     .minimum = 2},
        PostingSpec {.name = "optional",
                     .logical = false,
                     .op = OperatorType::OP_OR,
                     .roles = {Occur::MUST, Occur::SHOULD, Occur::SHOULD, Occur::SHOULD},
                     .minimum = 0}};

WeightPtr posting_weight(const PostingSpec& spec, uint32_t profile, bool sparse, bool scoring) {
    std::array<QueryPtr, 4> leaves;
    for (uint32_t clause = 0; clause < leaves.size(); ++clause) {
        leaves[clause] = std::make_shared<PostingBenchTerm>(
                kPostingFields[posting_field(profile, clause)],
                sparse ? kSparseTerms[clause] : kDenseTerms[clause]);
    }
    if (spec.logical) {
        OperatorBooleanQueryBuilder builder(spec.op);
        for (const auto& leaf : leaves) {
            builder.add(leaf);
        }
        return builder.build()->weight(scoring);
    }
    OccurBooleanQueryBuilder builder;
    builder.set_minimum_number_should_match(spec.minimum);
    for (uint32_t clause = 0; clause < leaves.size(); ++clause) {
        builder.add(leaves[clause], spec.roles[clause]);
    }
    return builder.build()->weight(scoring);
}

uint64_t posting_row_checksum(uint32_t doc, float score, bool scoring) {
    return static_cast<uint64_t>(doc + 1) * 1099511628211ULL +
           (scoring ? std::bit_cast<uint32_t>(score) : 0);
}

uint64_t expected_posting_checksum(const PostingSpec& spec, uint32_t profile, bool sparse,
                                   bool scoring) {
    BM25Similarity similarity(2.0F, 8.0F);
    const float term_score = similarity.score(1.0F, 0);
    uint64_t result = 0;
    for (uint32_t doc = 0; doc < kPostingRows; ++doc) {
        bool required = true;
        uint32_t optional = 0;
        float score = 0.0F;
        for (uint32_t clause = 0; clause < spec.roles.size(); ++clause) {
            const bool match = !posting_is_null(posting_field(profile, clause), doc) &&
                               posting_matches(clause, doc, sparse);
            if (spec.roles[clause] == Occur::MUST) {
                required &= match;
            } else {
                optional += match;
            }
            if (match) {
                score += term_score;
            }
        }
        if (required && optional >= spec.minimum) {
            result += posting_row_checksum(doc, score, scoring);
        }
    }
    return result;
}

uint64_t execute_posting_query(const WeightPtr& weight, const QueryExecutionContext& context,
                               bool scoring, bool complete_truth = false) {
    auto scorer = weight->scorer(context);
    uint64_t checksum = 0;
    uint32_t doc = scorer->doc();
    while (doc != TERMINATED) {
        checksum += posting_row_checksum(doc, scoring ? scorer->score() : 0.0F, scoring);
        doc = scorer->advance();
    }
    const auto* nulls = scorer->get_null_bitmap(complete_truth ? context.null_resolver : nullptr);
    if (complete_truth && nulls != nullptr) {
        for (const uint32_t doc : *nulls) {
            checksum += static_cast<uint64_t>(doc + 1) * 11400714819323198485ULL;
        }
    }
    return checksum;
}

struct PhysicalTruthRows {
    roaring::Roaring truths;
    roaring::Roaring nulls;
    std::vector<float> scores;
};

PhysicalTruthRows expected_physical_truth(const PostingSpec& spec, uint32_t profile, bool sparse) {
    PhysicalTruthRows result;
    result.scores.resize(kPostingRows);
    BM25Similarity similarity(2.0F, 8.0F);
    const float term_score = similarity.score(1.0F, 0);
    for (uint32_t doc = 0; doc < kPostingRows; ++doc) {
        bool required_true = true;
        bool required_possible = true;
        uint32_t optional_true = 0;
        uint32_t optional_possible = 0;
        for (uint32_t clause = 0; clause < spec.roles.size(); ++clause) {
            const bool unknown = posting_is_null(posting_field(profile, clause), doc);
            const bool match = !unknown && posting_matches(clause, doc, sparse);
            if (spec.roles[clause] == Occur::MUST) {
                required_true &= match;
                required_possible &= match || unknown;
            } else {
                optional_true += match;
                optional_possible += match || unknown;
            }
            if (match) {
                result.scores[doc] += term_score;
            }
        }
        // Only the operator Booleans are three-valued; an occur Boolean is two-valued.
        if (required_true && optional_true >= spec.minimum) {
            result.truths.add(doc);
        } else if (spec.logical && required_possible && optional_possible >= spec.minimum) {
            result.nulls.add(doc);
        }
    }
    return result;
}

void verify_physical_truth(const WeightPtr& weight, const QueryExecutionContext& context,
                           const PhysicalTruthRows& expected, bool scoring, uint32_t target) {
    auto scorer = weight->scorer(context);
    const auto observed_nulls = [&]() {
        const auto* nulls = scorer->get_null_bitmap(context.null_resolver);
        return nulls == nullptr ? roaring::Roaring() : *nulls;
    };
    EXPECT_EQ(observed_nulls(), expected.nulls);
    roaring::Roaring actual;
    for (uint32_t doc = scorer->seek(target); doc != TERMINATED; doc = scorer->advance()) {
        ASSERT_LT(doc, kPostingRows);
        actual.add(doc);
        if (scoring) {
            EXPECT_FLOAT_EQ(scorer->score(), expected.scores[doc]);
        }
    }
    auto truths = expected.truths;
    truths.removeRange(0, target);
    EXPECT_EQ(actual, truths);
    EXPECT_EQ(observed_nulls(), expected.nulls);
}

uint64_t posting_cpu_ns() {
    timespec value {};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value);
    return static_cast<uint64_t>(value.tv_sec) * 1000000000 + value.tv_nsec;
}

uint32_t posting_parameter(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

void benchmark_postings(const PostingSpec& spec, const QueryExecutionContext& context,
                        uint32_t profile, bool sparse, bool scoring, bool complete_truth = false) {
    const auto weight = posting_weight(spec, profile, sparse, scoring);
    uint64_t expected = expected_posting_checksum(spec, profile, sparse, scoring);
    if (complete_truth) {
        for (const uint32_t doc : expected_physical_truth(spec, profile, sparse).nulls) {
            expected += static_cast<uint64_t>(doc + 1) * 11400714819323198485ULL;
        }
    }
    constexpr std::array profiles {"nonnull", "shared_nulls", "different_nulls"};
    std::string label = complete_truth ? "postings_truth/" : "postings/";
    label += std::string(spec.name) + "/" + profiles[profile];
    label += sparse ? "/sparse" : "/dense";
    label += scoring ? "/scored" : "/unscored";
    ASSERT_EQ(execute_posting_query(weight, context, scoring, complete_truth), expected) << label;
    const uint32_t samples = posting_parameter("QUERY_ENGINE_BENCH_SAMPLES", 32);
    const uint32_t iterations = posting_parameter("BOOLEAN_POSTINGS_BENCH_ITERATIONS", 16);
    for (uint32_t sample = 0; sample < samples; ++sample) {
        doris::benchmark::wait_for_turn(label, sample);
        uint64_t checksum = 0;
        const auto start = posting_cpu_ns();
        for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
            checksum += execute_posting_query(weight, context, scoring, complete_truth);
        }
        const auto elapsed = posting_cpu_ns() - start;
        ASSERT_EQ(checksum, expected * iterations) << label;
        doris::benchmark::report_sample(label, sample, iterations, elapsed, checksum);
    }
}

class BooleanPostingsBench : public testing::Test {
public:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kPostingDirectory).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kPostingDirectory).ok());
        for (uint32_t field = 0; field < kPostingFields.size(); ++field) {
            const std::string directory =
                    std::string(kPostingDirectory) + "/" + std::to_string(field);
            ASSERT_TRUE(io::global_local_filesystem()->create_directory(directory).ok());
            write_field_index(directory, field);
            _readers[field] = {lucene::index::IndexReader::open(directory.c_str()),
                               [](auto* reader) {
                                   reader->close();
                                   _CLDELETE(reader);
                               }};
        }
    }

    void TearDown() override {
        for (auto& reader : _readers) {
            reader.reset();
        }
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kPostingDirectory).ok());
    }

protected:
    static void write_field_index(const std::string& directory, uint32_t field) {
        CustomAnalyzerConfig::Builder config;
        config.with_tokenizer_config("standard", {});
        auto analyzer = CustomAnalyzer::build_custom_analyzer(config.build());
        auto input = std::make_shared<lucene::util::SStringReader<char>>();
        auto writer = std::make_unique<lucene::index::IndexWriter>(directory.c_str(),
                                                                   analyzer.get(), true);
        writer->setMaxBufferedDocs(kPostingRows + 1);
        writer->setRAMBufferSizeMB(-1);
        writer->setUseCompoundFile(false);
        constexpr int kFlags = static_cast<int>(lucene::document::Field::STORE_NO) |
                               lucene::document::Field::INDEX_NONORMS |
                               lucene::document::Field::INDEX_TOKENIZED;
        auto document = std::make_unique<lucene::document::Document>();
        auto* value = _CLNEW lucene::document::Field(kPostingFields[field], kFlags);
        value->setOmitTermFreqAndPositions(false);
        document->add(*value);
        ErrorContext error_context;
        try {
            for (uint32_t row = 0; row < kPostingRows; ++row) {
                const auto wide = field_text(field, row);
                const std::string text(wide.begin(), wide.end());
                input->init(text.data(), text.size(), false);
                value->setValue(analyzer->reusableTokenStream(value->name(), input));
                writer->addDocument(document.get());
            }
        } catch (...) {
            error_context.eptr = std::current_exception();
        }
        FINALLY_EXCEPTION({ FINALLY_CLOSE(writer); });
    }

    static std::wstring field_text(uint32_t field, uint32_t row) {
        std::wstring result = L"filler";
        if (posting_is_null(field, row)) {
            return result;
        }
        for (uint32_t clause = 0; clause < kDenseTerms.size(); ++clause) {
            if (posting_matches(clause, row, false)) {
                result += std::wstring(L" ") + kDenseTerms[clause];
            }
            if (posting_matches(clause, row, true)) {
                result += std::wstring(L" ") + kSparseTerms[clause];
            }
        }
        return result;
    }

    void run_benchmark(bool complete_truth) {
        PostingNullResolver resolver;
        QueryExecutionContext context;
        context.segment_num_rows = kPostingRows;
        for (uint32_t field = 0; field < kPostingFields.size(); ++field) {
            context.field_reader_bindings.emplace(kPostingFields[field], _readers[field]);
        }
        context.null_resolver = &resolver;
        for (const auto& spec : kPostingSpecs) {
            for (uint32_t profile = 0; profile < 3; ++profile) {
                for (bool sparse : {false, true}) {
                    for (bool scoring : {false, true}) {
                        benchmark_postings(spec, context, profile, sparse, scoring, complete_truth);
                    }
                }
            }
        }
    }

    std::array<std::shared_ptr<lucene::index::IndexReader>, kPostingFields.size()> _readers;
};

TEST_F(BooleanPostingsBench, PhysicalFieldsPreserveCompleteTruthAndScores) {
    PostingNullResolver resolver;
    QueryExecutionContext context;
    context.segment_num_rows = kPostingRows;
    for (uint32_t field = 0; field < kPostingFields.size(); ++field) {
        context.field_reader_bindings.emplace(kPostingFields[field], _readers[field]);
    }
    context.null_resolver = &resolver;
    for (const auto& spec : kPostingSpecs) {
        for (uint32_t profile = 0; profile < 3; ++profile) {
            for (bool sparse : {false, true}) {
                const auto expected = expected_physical_truth(spec, profile, sparse);
                for (bool scoring : {false, true}) {
                    SCOPED_TRACE(testing::Message()
                                 << spec.name << " profile=" << profile << " sparse=" << sparse
                                 << " scoring=" << scoring);
                    const auto weight = posting_weight(spec, profile, sparse, scoring);
                    verify_physical_truth(weight, context, expected, scoring, 0);
                    verify_physical_truth(weight, context, expected, scoring, kPostingRows / 2);
                }
            }
        }
    }
}

TEST_F(BooleanPostingsBench, DISABLED_LogicalAndOccurrenceQueries) {
    run_benchmark(false);
}

TEST_F(BooleanPostingsBench, DISABLED_CompleteTruthQueries) {
    run_benchmark(true);
}

} // namespace
} // namespace doris::segment_v2::inverted_index::query_v2
