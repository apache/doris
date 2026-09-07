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

#pragma once

#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/gram/gram_extractor.h"

namespace doris::segment_v2::gram::benchmark_detail {

// Fixed corpora make repeated builds directly comparable. Corpus 0 has many distinct
// ASCII grams, 1 represents repetitive log messages, and 2 includes UTF-8 boundaries.
inline std::string make_row(size_t size, int64_t corpus) {
    std::string row;
    row.reserve(size);
    if (corpus == 0) {
        uint64_t seed = 0x67538;
        constexpr std::string_view alphabet =
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 :_=/.-";
        while (row.size() < size) {
            seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
            row.push_back(alphabet[(seed >> 32) % alphabet.size()]);
        }
        return row;
    }
    const std::string_view chunk =
            corpus == 1 ? "2026-09-06 INFO rpc request completed service=Frontend latency_ms=17 "
                        : "2026-09-06 INFO 查询完成 service=Frontend 手机日志 latency_ms=17 ";
    while (row.size() + chunk.size() <= size) {
        row.append(chunk);
    }
    row.append(size - row.size(), 'x');
    return row;
}

inline uint64_t gram_digest(const std::vector<std::string_view>& grams) {
    uint64_t hash = 14695981039346656037ULL;
    for (auto gram : grams) {
        for (unsigned char byte : gram) {
            hash = (hash ^ byte) * 1099511628211ULL;
        }
        hash = (hash ^ 0xFF) * 1099511628211ULL;
    }
    return hash;
}

// fresh=1 follows the writer's per-value tokenizer lifetime; fresh=0 isolates
// extraction with reusable buffers. This measures tokenization, not SQL throughput.
inline void GramExtraction(benchmark::State& state) {
    GramScheme scheme;
    scheme.mode = state.range(1) == 0 ? GramMode::DENSE : GramMode::SPARSE;
    scheme.lower_case = state.range(2) != 0;
    const std::string row = make_row(state.range(0), state.range(3));
    GramExtractor reused(scheme);
    std::vector<std::string_view> grams;
    reused.extract(row, &grams);
    state.SetLabel("digest=" + std::to_string(gram_digest(grams)));
    state.counters["grams_per_row"] = static_cast<double>(grams.size());

    for ([[maybe_unused]] auto _ : state) {
        if (state.range(4) != 0) {
            GramExtractor fresh(scheme);
            std::vector<std::string_view> fresh_grams;
            fresh.extract(row, &fresh_grams);
            benchmark::DoNotOptimize(fresh_grams);
            benchmark::ClobberMemory();
        } else {
            reused.extract(row, &grams);
            benchmark::DoNotOptimize(grams);
            benchmark::ClobberMemory();
        }
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(row.size()));
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(GramExtraction)
        ->ArgsProduct({{128, 4096, 65536, 1048576}, {0, 1}, {0, 1}, {0, 1, 2}, {0, 1}})
        ->ArgNames({"bytes", "sparse", "lower_case", "corpus", "fresh"});

} // namespace doris::segment_v2::gram::benchmark_detail
