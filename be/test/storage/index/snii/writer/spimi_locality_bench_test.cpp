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
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// G11 in-process locality microbenchmark for the SPIMI per-token hot path.
//
// WHY IN-PROCESS: the wikipedia import's wall time swings 2080s..7100s with the
// host's memory-bandwidth contention, so whole-import A/B numbers are
// meaningless for CPU-locality candidates. This bench feeds a synthetic
// wikipedia-shaped token stream (Zipf unigrams + adjacent-pair bigrams,
// production add order: all unigrams of a doc, then its pairs) straight through
// add_token_returning_id / add_bigram_token(left_id, right_id) in ONE process
// and reports steady_clock wall time per rep. Relative (before/after) deltas
// from interleaved reps + medians are contention-independent enough to gate a
// keep/drop decision; absolute numbers are not the point.
//
// EQUALITY EVIDENCE: every timed rep is followed (rep 0 of each arm) by a full
// for_each_term_sorted drain folded into an FNV-1a digest over every emitted
// term's bytes, docids, freqs and positions. A locality candidate must keep the
// digest bit-identical to the baseline build's.
//
// Scale knobs (env, so the default CI run stays cheap):
//   SNII_BENCH_TOKENS  total tokens to feed  (default 300000; perf runs use 5M)
//   SNII_BENCH_REPS    timed reps per arm    (default 3)

namespace doris::snii::writer {

namespace {

uint64_t env_u64(const char* name, uint64_t def) {
    const char* v = std::getenv(name);
    if (v == nullptr || *v == '\0') {
        return def;
    }
    return static_cast<uint64_t>(std::strtoull(v, nullptr, 10));
}

constexpr uint64_t kFnvOffset = 1469598103934665603ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;

uint64_t fnv_bytes(uint64_t h, const void* data, size_t n) {
    const auto* p = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < n; ++i) {
        h ^= p[i];
        h *= kFnvPrime;
    }
    return h;
}

uint64_t fnv_u64(uint64_t h, uint64_t v) {
    return fnv_bytes(h, &v, sizeof(v));
}

// Wikipedia-shaped synthetic corpus: Zipf(s=1.07) over a fixed vocabulary of
// lowercase-ascii words (2..12 chars, all phrase-bigram-indexable so every
// adjacent pair reaches the pair-keyed bigram path, like body text), cut into
// fixed-size docs. Deterministic (fixed seed). Built once, untimed.
struct BenchCorpus {
    std::vector<std::string> vocab; // distinct words
    std::vector<uint32_t> tokens;   // per-token vocab index
    uint32_t tokens_per_doc = 200;
    uint64_t total_tokens() const { return tokens.size(); }
};

const BenchCorpus& corpus() {
    static const BenchCorpus c = [] {
        BenchCorpus b;
        const uint64_t n_tokens = env_u64("SNII_BENCH_TOKENS", 300000);
        const uint32_t vocab_size = 262144;
        std::mt19937_64 rng(0x5a11d5eedULL);

        // Distinct words, GUARANTEED unique by a prefix-free construction: the
        // vocab index in base 25 over 'b'..'z', terminated by 'a', then 0..6
        // random 'b'..'z' chars (the terminator is the FIRST 'a', so equal
        // strings imply equal indexes). Lengths land in 2..12 (SSO,
        // bigram-indexable), Zipf-weighted below like body text.
        b.vocab.reserve(vocab_size);
        std::uniform_int_distribution<int> extra_len(0, 6);
        std::uniform_int_distribution<int> letter(0, 24);
        for (uint32_t i = 0; i < vocab_size; ++i) {
            std::string w;
            uint32_t x = i;
            do {
                w.push_back(static_cast<char>('b' + (x % 25)));
                x /= 25;
            } while (x != 0);
            w.push_back('a');
            const int extra = extra_len(rng);
            for (int k = 0; k < extra; ++k) {
                w.push_back(static_cast<char>('b' + letter(rng)));
            }
            b.vocab.push_back(std::move(w));
        }

        // Zipf CDF over ranks 1..V, s = 1.07; token index sampled by binary
        // search. Rank r maps to vocab id (r - 1) (vocab order is arbitrary).
        std::vector<double> cdf(vocab_size);
        double acc = 0.0;
        for (uint32_t r = 1; r <= vocab_size; ++r) {
            acc += 1.0 / std::pow(static_cast<double>(r), 1.07);
            cdf[r - 1] = acc;
        }
        std::uniform_real_distribution<double> uni(0.0, acc);
        b.tokens.reserve(n_tokens);
        for (uint64_t t = 0; t < n_tokens; ++t) {
            const double u = uni(rng);
            const auto it = std::lower_bound(cdf.begin(), cdf.end(), u);
            b.tokens.push_back(static_cast<uint32_t>(it - cdf.begin()));
        }
        return b;
    }();
    return c;
}

struct FeedResult {
    uint64_t total_ns = 0;   // whole feed loop (unigrams + bigrams)
    uint64_t unigram_ns = 0; // add_token_returning_id portion
    uint64_t bigram_ns = 0;  // add_bigram_token(left_id, right_id) portion
    uint64_t unique_terms = 0;
    uint64_t total_tokens = 0;
    uint64_t digest = 0; // 0 unless drained
};

// Feeds the corpus through a fresh buffer exactly the way the production SNII
// column writer does (owned vocab, positions on, bigram diet configured with
// the default 512 MiB cap and the 0-doc-floor drain df gate; per doc all
// unigrams first, then all adjacent pairs keyed by the captured unigram ids;
// pair position = left token's position). `drain_digest`: run the terminal
// for_each_term_sorted drain and fold everything emitted into an FNV digest
// (equality evidence); reps used purely for timing skip it (dtor cleans up).
FeedResult run_feed(bool drain_digest) {
    const BenchCorpus& c = corpus();
    FeedResult r;

    MemoryReporter reporter(nullptr, /*cap_bytes=*/0); // production per-token
                                                       // report path, no spill
    SpimiTermBuffer buf(/*has_positions=*/true, /*spill_threshold_bytes=*/0, &reporter);
    buf.configure_bigram_diet(/*vocab_cap_bytes=*/512ULL << 20,
                              format::default_phrase_bigram_prune_min_df(0));

    std::vector<uint32_t> ids(c.tokens_per_doc);
    const uint64_t n = c.total_tokens();
    const auto t_begin = std::chrono::steady_clock::now();
    uint32_t docid = 0;
    for (uint64_t t = 0; t < n;) {
        const uint64_t doc_end = std::min<uint64_t>(n, t + c.tokens_per_doc);
        const auto t_uni = std::chrono::steady_clock::now();
        uint32_t m = 0;
        for (; t < doc_end; ++t, ++m) {
            const std::string& w = c.vocab[c.tokens[t]];
            ids[m] = buf.add_token_returning_id(std::string_view(w), docid, m);
        }
        const auto t_big = std::chrono::steady_clock::now();
        for (uint32_t i = 0; i + 1 < m; ++i) {
            buf.add_bigram_token(ids[i], ids[i + 1], docid, i);
        }
        const auto t_end = std::chrono::steady_clock::now();
        r.unigram_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(t_big - t_uni).count();
        r.bigram_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_big).count();
        ++docid;
    }
    const auto t_total = std::chrono::steady_clock::now();
    r.total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t_total - t_begin).count();
    r.unique_terms = buf.unique_terms();
    r.total_tokens = buf.total_tokens();
    EXPECT_TRUE(buf.status().ok()) << buf.status().to_string();

    if (drain_digest) {
        uint64_t h = kFnvOffset;
        std::vector<uint32_t> pos_scratch(4096);
        const Status st = buf.for_each_term_sorted([&](TermPostings&& tp) {
            h = fnv_bytes(h, tp.term.data(), tp.term.size());
            h = fnv_u64(h, tp.docids.size());
            h = fnv_bytes(h, tp.docids.data(), tp.docids.size() * sizeof(uint32_t));
            h = fnv_bytes(h, tp.freqs.data(), tp.freqs.size() * sizeof(uint32_t));
            if (tp.pos_pump) {
                uint64_t remaining = tp.pos_total;
                while (remaining > 0) {
                    const size_t k =
                            static_cast<size_t>(std::min<uint64_t>(remaining, pos_scratch.size()));
                    tp.pos_pump(pos_scratch.data(), k);
                    h = fnv_bytes(h, pos_scratch.data(), k * sizeof(uint32_t));
                    remaining -= k;
                }
            } else {
                h = fnv_bytes(h, tp.positions_flat.data(),
                              tp.positions_flat.size() * sizeof(uint32_t));
            }
        });
        EXPECT_TRUE(st.ok()) << st.to_string();
        r.digest = h;
    }
    return r;
}

uint64_t median_ns(std::vector<uint64_t> v) {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

void print_result(const char* tag, size_t rep, const FeedResult& r) {
    const double tokens = static_cast<double>(2 * r.total_tokens); // uni + bigram adds
    printf("[bench] %-16s rep=%zu total=%9.3f ms  unigram=%9.3f ms  bigram=%9.3f ms  "
           "(%.1f ns/add)\n",
           tag, rep, r.total_ns / 1e6, r.unigram_ns / 1e6, r.bigram_ns / 1e6, r.total_ns / tokens);
}

} // namespace

// Baseline feed timing + drain-digest stability. The digest printed here is the
// cross-build equality reference every candidate build must reproduce.
TEST(SniiSpimiLocalityBenchTest, FeedBaseline) {
    const size_t reps = static_cast<size_t>(env_u64("SNII_BENCH_REPS", 3));
    const BenchCorpus& c = corpus();
    printf("[bench] corpus: %llu tokens, vocab %zu, %u tokens/doc\n",
           static_cast<unsigned long long>(c.total_tokens()), c.vocab.size(), c.tokens_per_doc);

    std::vector<uint64_t> total, uni, big;
    uint64_t digest = 0;
    uint64_t unique_terms = 0;
    for (size_t rep = 0; rep < reps; ++rep) {
        const FeedResult r = run_feed(/*drain_digest=*/rep == 0);
        if (rep == 0) {
            digest = r.digest;
            unique_terms = r.unique_terms;
        } else {
            ASSERT_EQ(unique_terms, r.unique_terms); // determinism across reps
        }
        total.push_back(r.total_ns);
        uni.push_back(r.unigram_ns);
        big.push_back(r.bigram_ns);
        print_result("baseline", rep, r);
    }
    printf("[bench] baseline median: total=%9.3f ms  unigram=%9.3f ms  bigram=%9.3f ms\n",
           median_ns(total) / 1e6, median_ns(uni) / 1e6, median_ns(big) / 1e6);
    printf("[bench] unique_terms=%llu drain_digest=%016llx\n",
           static_cast<unsigned long long>(unique_terms), static_cast<unsigned long long>(digest));
    ASSERT_NE(digest, 0U);
}

// In-process A/B for the G11 prefetch candidate: alternates prefetch-disabled /
// prefetch-enabled reps in ONE process and compares medians, so machine drift
// cancels. Compiled to a skip until the candidate (and its BE_TEST toggle seam)
// is present.
TEST(SniiSpimiLocalityBenchTest, PrefetchToggleAB) {
#ifndef SNII_G11_PREFETCH
    GTEST_SKIP() << "G11 prefetch candidate not compiled in";
#else
    const size_t reps = static_cast<size_t>(env_u64("SNII_BENCH_REPS", 3));
    std::vector<uint64_t> off_total, on_total, off_big, on_big;
    uint64_t digest_off = 0;
    uint64_t digest_on = 0;
    for (size_t rep = 0; rep < reps; ++rep) {
        testing::set_bench_disable_g11_prefetch(true);
        const FeedResult off = run_feed(/*drain_digest=*/rep == 0);
        testing::set_bench_disable_g11_prefetch(false);
        const FeedResult on = run_feed(/*drain_digest=*/rep == 0);
        if (rep == 0) {
            digest_off = off.digest;
            digest_on = on.digest;
        }
        off_total.push_back(off.total_ns);
        on_total.push_back(on.total_ns);
        off_big.push_back(off.bigram_ns);
        on_big.push_back(on.bigram_ns);
        print_result("prefetch-OFF", rep, off);
        print_result("prefetch-ON", rep, on);
    }
    testing::set_bench_disable_g11_prefetch(false);
    const uint64_t mo = median_ns(off_total);
    const uint64_t mn = median_ns(on_total);
    const uint64_t bo = median_ns(off_big);
    const uint64_t bn = median_ns(on_big);
    printf("[bench] prefetch A/B: total OFF=%9.3f ms ON=%9.3f ms (%+.2f%%)  "
           "bigram OFF=%9.3f ms ON=%9.3f ms (%+.2f%%)\n",
           mo / 1e6, mn / 1e6, 100.0 * (static_cast<double>(mn) - mo) / mo, bo / 1e6, bn / 1e6,
           100.0 * (static_cast<double>(bn) - bo) / bo);
    // Byte-identical: prefetch has no architectural side effects.
    ASSERT_EQ(digest_off, digest_on);
#endif
}

// Candidate (d) upper bound, measured WITHOUT touching production code: replays
// the exact pair-key stream (find-or-emplace, feed order, dups included) into a
// raw phmap::flat_hash_map<uint64_t, uint32_t> -- the bigram_pair_map_ type --
// with and without a perfect up-front reserve. The delta bounds what any
// doc-count-heuristic reserve of the pair map could save the whole feed.
TEST(SniiSpimiLocalityBenchTest, PairMapReserveUpperBound) {
    const size_t reps = static_cast<size_t>(env_u64("SNII_BENCH_REPS", 3));
    const BenchCorpus& c = corpus();

    // Pair-key stream in feed order (unigram ids == first-seen order, exactly
    // what add_token_returning_id assigns; kNoPairKey never collides).
    std::vector<uint32_t> first_seen(c.vocab.size(), 0xFFFFFFFFU);
    uint32_t next_id = 0;
    std::vector<uint64_t> pair_stream;
    pair_stream.reserve(c.tokens.size());
    const uint64_t n = c.total_tokens();
    for (uint64_t t = 0; t < n;) {
        const uint64_t doc_end = std::min<uint64_t>(n, t + c.tokens_per_doc);
        uint32_t prev_id = 0;
        bool have_prev = false;
        for (; t < doc_end; ++t) {
            uint32_t& slot = first_seen[c.tokens[t]];
            if (slot == 0xFFFFFFFFU) {
                slot = next_id++;
            }
            if (have_prev) {
                pair_stream.push_back((static_cast<uint64_t>(prev_id) << 32) | slot);
            }
            prev_id = slot;
            have_prev = true;
        }
    }

    size_t distinct = 0;
    {
        phmap::flat_hash_map<uint64_t, uint32_t> probe;
        for (uint64_t k : pair_stream) {
            probe.emplace(k, 0);
        }
        distinct = probe.size();
    }

    auto run_arm = [&](bool reserve) {
        const auto t0 = std::chrono::steady_clock::now();
        phmap::flat_hash_map<uint64_t, uint32_t> m;
        if (reserve) {
            m.reserve(distinct);
        }
        uint32_t id = 0;
        uint64_t hits = 0;
        for (uint64_t k : pair_stream) {
            auto it = m.find(k);
            if (it == m.end()) {
                m.emplace(k, id++);
            } else {
                ++hits;
            }
        }
        const auto t1 = std::chrono::steady_clock::now();
        EXPECT_EQ(m.size(), distinct);
        (void)hits;
        return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    };

    std::vector<uint64_t> no_res, with_res;
    for (size_t rep = 0; rep < reps; ++rep) {
        const uint64_t t_no = run_arm(false);
        const uint64_t t_yes = run_arm(true);
        no_res.push_back(t_no);
        with_res.push_back(t_yes);
        printf("[bench] pair-map rep=%zu  no-reserve=%9.3f ms  reserve=%9.3f ms\n", rep, t_no / 1e6,
               t_yes / 1e6);
    }
    const uint64_t mn = median_ns(no_res);
    const uint64_t my = median_ns(with_res);
    printf("[bench] pair-map reserve upper bound: distinct=%zu stream=%zu  "
           "no-reserve=%9.3f ms reserve=%9.3f ms delta=%+.2f%%\n",
           distinct, pair_stream.size(), mn / 1e6, my / 1e6,
           100.0 * (static_cast<double>(my) - mn) / mn);
}

// Same experiment for the unigram intern set: a replica std::unordered_set
// keyed by u32 ids with vocab-backed heterogeneous string hashing (the
// production intern_ shape), fed the unigram token stream, with and without a
// perfect up-front reserve.
TEST(SniiSpimiLocalityBenchTest, InternSetReserveUpperBound) {
    const size_t reps = static_cast<size_t>(env_u64("SNII_BENCH_REPS", 3));
    const BenchCorpus& c = corpus();

    struct H {
        using is_transparent = void;
        const std::vector<std::string>* vocab;
        size_t operator()(std::string_view s) const noexcept {
            return fnv_bytes(kFnvOffset, s.data(), s.size());
        }
        size_t operator()(uint32_t id) const noexcept {
            return operator()(std::string_view((*vocab)[id]));
        }
    };
    struct E {
        using is_transparent = void;
        const std::vector<std::string>* vocab;
        bool operator()(uint32_t a, uint32_t b) const noexcept { return a == b; }
        [[maybe_unused]] bool operator()(uint32_t a, std::string_view s) const noexcept {
            return std::string_view((*vocab)[a]) == s;
        }
        [[maybe_unused]] bool operator()(std::string_view s, uint32_t a) const noexcept {
            return std::string_view((*vocab)[a]) == s;
        }
    };

    size_t distinct = 0;
    {
        std::unordered_set<uint32_t> probe;
        for (uint32_t t : c.tokens) {
            probe.insert(t);
        }
        distinct = probe.size();
    }

    auto run_arm = [&](bool reserve) {
        const auto t0 = std::chrono::steady_clock::now();
        std::unordered_set<uint32_t, H, E> s(0, H {&c.vocab}, E {&c.vocab});
        if (reserve) {
            s.reserve(distinct);
        }
        for (uint32_t t : c.tokens) {
            const std::string& w = c.vocab[t];
            auto it = s.find(std::string_view(w));
            if (it == s.end()) {
                s.insert(t); // id == vocab index in this replica
            }
        }
        const auto t1 = std::chrono::steady_clock::now();
        EXPECT_EQ(s.size(), distinct);
        return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    };

    std::vector<uint64_t> no_res, with_res;
    for (size_t rep = 0; rep < reps; ++rep) {
        const uint64_t t_no = run_arm(false);
        const uint64_t t_yes = run_arm(true);
        no_res.push_back(t_no);
        with_res.push_back(t_yes);
        printf("[bench] intern-set rep=%zu  no-reserve=%9.3f ms  reserve=%9.3f ms\n", rep,
               t_no / 1e6, t_yes / 1e6);
    }
    const uint64_t mn = median_ns(no_res);
    const uint64_t my = median_ns(with_res);
    printf("[bench] intern-set reserve upper bound: distinct=%zu  no-reserve=%9.3f ms "
           "reserve=%9.3f ms delta=%+.2f%%\n",
           distinct, mn / 1e6, my / 1e6, 100.0 * (static_cast<double>(my) - mn) / mn);
}

} // namespace doris::snii::writer
