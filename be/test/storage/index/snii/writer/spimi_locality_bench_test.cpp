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
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii/writer/term_posting_test_utils.h"

namespace doris::snii::writer {
namespace {

uint64_t env_u64(const char* name, uint64_t default_value) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return default_value;
    }
    return static_cast<uint64_t>(std::strtoull(value, nullptr, 10));
}

constexpr uint64_t kFnvOffset = 1469598103934665603ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;

uint64_t fnv_bytes(uint64_t hash, const void* data, size_t size) {
    const auto* bytes = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < size; ++i) {
        hash ^= bytes[i];
        hash *= kFnvPrime;
    }
    return hash;
}

uint64_t fnv_u64(uint64_t hash, uint64_t value) {
    return fnv_bytes(hash, &value, sizeof(value));
}

struct BenchCorpus {
    std::vector<std::string> vocab;
    std::vector<uint32_t> tokens;
    uint32_t tokens_per_doc = 200;
};

const BenchCorpus& corpus() {
    static const BenchCorpus corpus = [] {
        BenchCorpus result;
        const uint64_t token_count = env_u64("SNII_BENCH_TOKENS", 300000);
        constexpr uint32_t kVocabSize = 262144;
        std::mt19937_64 rng(0x5a11d5eedULL);

        result.vocab.reserve(kVocabSize);
        std::uniform_int_distribution<int> extra_len(0, 6);
        std::uniform_int_distribution<int> letter(0, 24);
        for (uint32_t id = 0; id < kVocabSize; ++id) {
            std::string term;
            uint32_t value = id;
            do {
                term.push_back(static_cast<char>('b' + (value % 25)));
                value /= 25;
            } while (value != 0);
            term.push_back('a');
            for (int i = 0, n = extra_len(rng); i < n; ++i) {
                term.push_back(static_cast<char>('b' + letter(rng)));
            }
            result.vocab.push_back(std::move(term));
        }

        std::vector<double> cdf(kVocabSize);
        double sum = 0.0;
        for (uint32_t rank = 1; rank <= kVocabSize; ++rank) {
            sum += 1.0 / std::pow(static_cast<double>(rank), 1.07);
            cdf[rank - 1] = sum;
        }
        std::uniform_real_distribution<double> sample(0.0, sum);
        result.tokens.reserve(token_count);
        for (uint64_t i = 0; i < token_count; ++i) {
            const auto it = std::lower_bound(cdf.begin(), cdf.end(), sample(rng));
            result.tokens.push_back(static_cast<uint32_t>(it - cdf.begin()));
        }
        return result;
    }();
    return corpus;
}

struct FeedResult {
    uint64_t total_ns = 0;
    uint64_t unique_terms = 0;
    uint64_t total_tokens = 0;
    uint64_t digest = 0;
};

FeedResult run_feed(bool drain_digest) {
    const BenchCorpus& input = corpus();
    FeedResult result;
    MemoryReporter reporter(nullptr, /*cap_bytes=*/0);
    SpimiTermBuffer buffer(/*has_positions=*/true, /*spill_threshold_bytes=*/0, &reporter);

    const auto begin = std::chrono::steady_clock::now();
    uint32_t docid = 0;
    for (size_t token = 0; token < input.tokens.size();) {
        const size_t doc_end =
                std::min(input.tokens.size(), token + static_cast<size_t>(input.tokens_per_doc));
        uint32_t position = 0;
        for (; token < doc_end; ++token, ++position) {
            buffer.add_token(input.vocab[input.tokens[token]], docid, position);
        }
        ++docid;
    }
    result.total_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                    std::chrono::steady_clock::now() - begin)
                                                    .count());
    result.unique_terms = buffer.unique_terms();
    result.total_tokens = buffer.total_tokens();
    EXPECT_TRUE(buffer.status().ok()) << buffer.status().to_string();

    if (drain_digest) {
        uint64_t hash = kFnvOffset;
        const auto status = buffer.for_each_term_sorted([&](StreamedTermPostings&& source) {
            TermPostings postings;
            RETURN_IF_ERROR(materialize_streamed_term(std::move(source), &postings));
            hash = fnv_bytes(hash, postings.term.data(), postings.term.size());
            hash = fnv_u64(hash, postings.docids.size());
            hash = fnv_bytes(hash, postings.docids.data(),
                             postings.docids.size() * sizeof(uint32_t));
            hash = fnv_bytes(hash, postings.freqs.data(), postings.freqs.size() * sizeof(uint32_t));
            hash = fnv_bytes(hash, postings.positions_flat.data(),
                             postings.positions_flat.size() * sizeof(uint32_t));
            return Status::OK();
        });
        EXPECT_TRUE(status.ok()) << status.to_string();
        result.digest = hash;
    }
    return result;
}

uint64_t median_ns(std::vector<uint64_t> values) {
    std::ranges::sort(values);
    return values[values.size() / 2];
}

void print_result(const char* label, size_t repetition, const FeedResult& result) {
    const double tokens = static_cast<double>(result.total_tokens);
    printf("[bench] %-16s rep=%zu total=%9.3f ms (%.1f ns/add)\n", label, repetition,
           result.total_ns / 1e6, result.total_ns / tokens);
}

} // namespace

TEST(SniiSpimiLocalityBenchTest, FeedBaseline) {
    const size_t repetitions = static_cast<size_t>(env_u64("SNII_BENCH_REPS", 3));
    std::vector<uint64_t> total;
    uint64_t digest = 0;
    uint64_t unique_terms = 0;
    for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        const FeedResult result = run_feed(/*drain_digest=*/repetition == 0);
        if (repetition == 0) {
            digest = result.digest;
            unique_terms = result.unique_terms;
        } else {
            ASSERT_EQ(unique_terms, result.unique_terms);
        }
        total.push_back(result.total_ns);
        print_result("baseline", repetition, result);
    }
    printf("[bench] baseline median: total=%9.3f ms unique_terms=%llu digest=%016llx\n",
           median_ns(total) / 1e6, static_cast<unsigned long long>(unique_terms),
           static_cast<unsigned long long>(digest));
    ASSERT_NE(digest, 0U);
}

TEST(SniiSpimiLocalityBenchTest, PrefetchToggleAB) {
#ifndef SNII_G11_PREFETCH
    GTEST_SKIP() << "prefetch candidate not compiled in";
#else
    const size_t repetitions = static_cast<size_t>(env_u64("SNII_BENCH_REPS", 3));
    std::vector<uint64_t> disabled_total;
    std::vector<uint64_t> enabled_total;
    uint64_t disabled_digest = 0;
    uint64_t enabled_digest = 0;
    for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        testing::set_bench_disable_g11_prefetch(true);
        const FeedResult disabled = run_feed(/*drain_digest=*/repetition == 0);
        testing::set_bench_disable_g11_prefetch(false);
        const FeedResult enabled = run_feed(/*drain_digest=*/repetition == 0);
        if (repetition == 0) {
            disabled_digest = disabled.digest;
            enabled_digest = enabled.digest;
        }
        disabled_total.push_back(disabled.total_ns);
        enabled_total.push_back(enabled.total_ns);
        print_result("prefetch-OFF", repetition, disabled);
        print_result("prefetch-ON", repetition, enabled);
    }
    testing::set_bench_disable_g11_prefetch(false);
    printf("[bench] prefetch A/B: OFF=%9.3f ms ON=%9.3f ms\n", median_ns(disabled_total) / 1e6,
           median_ns(enabled_total) / 1e6);
    ASSERT_EQ(disabled_digest, enabled_digest);
#endif
}

TEST(SniiSpimiLocalityBenchTest, InternSetReserveUpperBound) {
    const size_t repetitions = static_cast<size_t>(env_u64("SNII_BENCH_REPS", 3));
    const BenchCorpus& input = corpus();

    struct Hash {
        using is_transparent = void;
        const std::vector<std::string>* vocab;
        size_t operator()(std::string_view value) const noexcept {
            return fnv_bytes(kFnvOffset, value.data(), value.size());
        }
        size_t operator()(uint32_t id) const noexcept {
            return operator()(std::string_view((*vocab)[id]));
        }
    };
    struct Equal {
        using is_transparent = void;
        const std::vector<std::string>* vocab;
        bool operator()(uint32_t lhs, uint32_t rhs) const noexcept { return lhs == rhs; }
        bool operator()(std::string_view lhs, uint32_t rhs) const noexcept {
            return lhs == std::string_view((*vocab)[rhs]);
        }
    };

    std::unordered_set<uint32_t> distinct_ids(input.tokens.begin(), input.tokens.end());
    const size_t distinct = distinct_ids.size();
    const auto run = [&](bool reserve) {
        const auto begin = std::chrono::steady_clock::now();
        std::unordered_set<uint32_t, Hash, Equal> intern(0, Hash {&input.vocab},
                                                         Equal {&input.vocab});
        if (reserve) {
            intern.reserve(distinct);
        }
        for (uint32_t id : input.tokens) {
            const std::string& term = input.vocab[id];
            if (intern.find(std::string_view(term)) == intern.end()) {
                intern.insert(id);
            }
        }
        EXPECT_EQ(intern.size(), distinct);
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                             std::chrono::steady_clock::now() - begin)
                                             .count());
    };

    std::vector<uint64_t> no_reserve;
    std::vector<uint64_t> with_reserve;
    for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        no_reserve.push_back(run(false));
        with_reserve.push_back(run(true));
    }
    printf("[bench] intern-set: distinct=%zu no-reserve=%9.3f ms reserve=%9.3f ms\n", distinct,
           median_ns(no_reserve) / 1e6, median_ns(with_reserve) / 1e6);
}

} // namespace doris::snii::writer
