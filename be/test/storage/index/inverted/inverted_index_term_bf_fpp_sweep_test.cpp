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

// fpp sweep for the token-exists Bloom Filter ("tbf"). This is an analysis tool, not a pass/fail
// unit test: it sizes the *real* BlockSplitBloomFilter (the exact code the writer uses) over a
// real distinct-token vocabulary, and measures the *actual* false-positive rate at each fpp. The
// two axes it reports -- on-disk size and measured FP -- are everything fpp controls; the cold
// S3 latency follows linearly from FP (FP * per-segment-GET-cost * TTFB), so this is enough to
// pick the default fpp for `TERM_BF_FPP`.
//
// It does NOT run unless a vocabulary file is supplied, so it never disturbs CI:
//   INV_BF_VOCAB=/tmp/vocab_text.txt ./run-be-ut.sh --run --filter=*TermBfFppSweep*
// Each non-empty line of the file is one distinct analyzed token (the file must already be
// deduplicated -- the build set and the absent probe set are two disjoint line ranges, so
// uniqueness across lines is what guarantees the probe tokens are genuinely absent).

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "storage/index/bloom_filter/bloom_filter.h"

namespace doris::segment_v2 {

namespace {

std::vector<std::string> load_vocab(const std::string& path) {
    std::vector<std::string> vocab;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        vocab.push_back(line);
    }
    return vocab;
}

} // namespace

// Sizing + measured false-positive sweep over a real vocabulary.
TEST(TermBfFppSweepTest, SweepRealVocab) {
    const char* vocab_path = std::getenv("INV_BF_VOCAB");
    if (vocab_path == nullptr) {
        GTEST_SKIP() << "set INV_BF_VOCAB=<distinct-token file> to run the fpp sweep";
    }

    std::vector<std::string> vocab = load_vocab(vocab_path);
    ASSERT_GE(vocab.size(), 1000U) << "vocab too small to be representative";

    // Hold out the tail as the absent probe set (disjoint from the build set because every line
    // in the dedup'd file is unique). Cap the probe set so the sweep stays fast; keep at least
    // 100k probes for a stable FP estimate.
    const size_t probe_count = std::min<size_t>(vocab.size() / 5, 1'000'000);
    ASSERT_GE(probe_count, 100'000U) << "need a larger vocab for a solid FP estimate";
    const size_t build_count = vocab.size() - probe_count;

    const std::string label =
            std::getenv("INV_BF_LABEL") ? std::getenv("INV_BF_LABEL") : vocab_path;

    std::cout << "\n### tbf fpp sweep -- " << label << "\n";
    std::cout << "vocab=" << vocab.size() << " distinct tokens; build_n=" << build_count
              << ", absent_probes=" << probe_count << " (held-out real tokens)\n\n";
    std::cout << "| configured fpp | BF bytes | bytes/term | measured FP% (mean) | FP% stddev |\n";
    std::cout << "|---|---|---|---|---|\n";

    const std::vector<double> fpps = {0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.10};
    constexpr int kChunks = 10; // partition probes into chunks for a stddev across chunks.

    for (double fpp : fpps) {
        std::unique_ptr<BloomFilter> bf;
        ASSERT_TRUE(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf).ok());
        ASSERT_TRUE(bf->init(build_count, fpp, HASH_MURMUR3_X64_64).ok());
        for (size_t i = 0; i < build_count; ++i) {
            bf->add_bytes(vocab[i].data(), vocab[i].size());
        }

        const size_t chunk = probe_count / kChunks;
        std::vector<double> chunk_fp;
        chunk_fp.reserve(kChunks);
        for (int c = 0; c < kChunks; ++c) {
            size_t hits = 0;
            const size_t base = build_count + static_cast<size_t>(c) * chunk;
            for (size_t i = 0; i < chunk; ++i) {
                const std::string& t = vocab[base + i];
                if (bf->test_bytes(t.data(), t.size())) {
                    ++hits; // absent token reported MAYBE == false positive.
                }
            }
            chunk_fp.push_back(100.0 * static_cast<double>(hits) / static_cast<double>(chunk));
        }

        double mean = 0;
        for (double v : chunk_fp) {
            mean += v;
        }
        mean /= kChunks;
        double var = 0;
        for (double v : chunk_fp) {
            var += (v - mean) * (v - mean);
        }
        const double stddev = std::sqrt(var / kChunks);

        const double bytes_per_term =
                static_cast<double>(bf->num_bytes()) / static_cast<double>(build_count);
        printf("| %.4f | %zu | %.2f | %.3f | %.3f |\n", fpp, bf->num_bytes(), bytes_per_term, mean,
               stddev);
    }
    std::cout << std::endl;
}

} // namespace doris::segment_v2
