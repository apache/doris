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

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "common/check.h"
#include "storage/compressed_rowid_map.h"

namespace doris::rowid_conversion_benchmark {
using Map = CompressedRowIdMap;
using Clock = std::chrono::steady_clock;
constexpr uint32_t PATTERNS = 64;
constexpr uint32_t BLOCKS_PER_MAP = 1024;

struct Workload {
    uint32_t sources;
    uint32_t max_step;
};
constexpr Workload LINEAR_HOLES {1, 1};
constexpr Workload INTERLEAVE31_HOLES {31, 1};
constexpr Workload WIDE_GAPS_HOLES {1, 1U << 20};

struct Input {
    std::array<uint64_t, Map::BLOCK_ROWS> values;
    std::vector<std::pair<uint32_t, uint64_t>> live;
};
struct Corpus {
    std::vector<Input> inputs;
    uint64_t stride = 0;
    std::unique_ptr<Map> reference;
};

inline void check_status(const Status& status) {
    DORIS_CHECK(status.ok()) << status;
}

inline Corpus make_corpus(Workload workload) {
    Corpus corpus;
    corpus.inputs.resize(PATTERNS);
    std::mt19937 random(89);
    for (auto& input : corpus.inputs) {
        input.values.fill(Map::MISSING);
        uint64_t target = 0;
        for (uint32_t row = 0; row < Map::BLOCK_ROWS; ++row) {
            for (uint32_t source = 0; source < workload.sources; ++source) {
                if (random() % 10 == 0) continue;
                target += workload.max_step == 1 ? 1 : random() % workload.max_step + 1;
                if (source == 0) {
                    input.values[row] = target;
                    input.live.emplace_back(row, target);
                }
            }
        }
        DORIS_CHECK(!input.live.empty());
        DORIS_CHECK_LT(input.live.size(), Map::BLOCK_ROWS);
        corpus.stride = std::max(corpus.stride, target + 1);
    }
    corpus.reference = std::make_unique<Map>();
    check_status(corpus.reference->init_source(PATTERNS * Map::BLOCK_ROWS));
    for (uint32_t block = 0; block < PATTERNS; ++block) {
        for (auto [row, target] : corpus.inputs[block].live) {
            check_status(corpus.reference->append(0, block * Map::BLOCK_ROWS + row,
                                                  block * corpus.stride + target));
        }
    }
    check_status(corpus.reference->finish());
    // Validate every physical position before timing, including deletion holes.
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    for (uint32_t block = 0; block < PATTERNS; ++block) {
        for (uint32_t row = 0; row < Map::BLOCK_ROWS; ++row) {
            uint64_t value = 0;
            check_status(corpus.reference->get(0, block * Map::BLOCK_ROWS + row, cursor, workspace,
                                               &value));
            const uint64_t expected = corpus.inputs[block].values[row];
            DORIS_CHECK_EQ(value, expected == Map::MISSING ? Map::MISSING
                                                           : block * corpus.stride + expected);
        }
    }
    DORIS_CHECK_EQ(cursor.decoded_blocks(), PATTERNS);
    return corpus;
}

inline void report_counters(benchmark::State& state, const Corpus& corpus) {
    uint64_t payload_bytes = 0;
    uint64_t plain_bytes = 0;
    uint32_t zstd_blocks = 0;
    for (uint32_t block = 0; block < PATTERNS; ++block) {
        const auto& meta = corpus.reference->block_meta(0, block);
        payload_bytes += meta.payload_bytes;
        plain_bytes += meta.plain_bytes;
        zstd_blocks += (meta.flags & Map::flag_bits(Map::Flag::ZSTD)) != 0;
    }
    state.counters["rows_per_block"] = Map::BLOCK_ROWS;
    state.counters["patterns"] = PATTERNS;
    state.counters["payload_bytes"] = static_cast<double>(payload_bytes) / PATTERNS;
    state.counters["plain_bytes"] = static_cast<double>(plain_bytes) / PATTERNS;
    state.counters["zstd_fraction"] = static_cast<double>(zstd_blocks) / PATTERNS;
    state.SetItemsProcessed(state.iterations() * Map::BLOCK_ROWS);
    state.SetBytesProcessed(state.iterations() * Map::BLOCK_ROWS * sizeof(uint64_t));
}

inline void BM_RowIdConversionEncode(benchmark::State& state, Workload workload) {
    const auto corpus = make_corpus(workload);
    std::unique_ptr<Map> map;
    uint32_t block = BLOCKS_PER_MAP + 1;
    for (auto _ : state) {
        state.PauseTiming();
        if (block > BLOCKS_PER_MAP) {
            // Keep allocation bounded regardless of Google Benchmark's iteration
            // count. Recreate outside timing and warm the shared encoder on block 0.
            map = std::make_unique<Map>();
            check_status(map->init_source((BLOCKS_PER_MAP + 2) * Map::BLOCK_ROWS));
            for (auto [row, target] : corpus.inputs[0].live)
                check_status(map->append(0, row, target));
            const auto first = corpus.inputs[1].live.front();
            check_status(
                    map->append(0, Map::BLOCK_ROWS + first.first, corpus.stride + first.second));
            block = 1;
        }
        const auto& input = corpus.inputs[block % PATTERNS];
        // The boundary append in the previous iteration already wrote the first
        // live value. Fill the rest outside timing; the holes remain MISSING.
        for (size_t i = 1; i < input.live.size(); ++i) {
            const auto [row, target] = input.live[i];
            check_status(
                    map->append(0, block * Map::BLOCK_ROWS + row, block * corpus.stride + target));
        }
        const auto first = corpus.inputs[(block + 1) % PATTERNS].live.front();
        const uint32_t next_row = (block + 1) * Map::BLOCK_ROWS + first.first;
        const uint64_t next_target = (block + 1) * corpus.stride + first.second;
        state.ResumeTiming();
        const auto start = Clock::now();
        auto status = map->append(0, next_row, next_target);
        benchmark::ClobberMemory();
        const double seconds = std::chrono::duration<double>(Clock::now() - start).count();
        state.SetIterationTime(seconds);
        state.PauseTiming();
        check_status(status);
        // Seal includes bitmap/varint, optional Zstd, payload allocation/copy,
        // directory update, builder reset and the next first-row write.
        DORIS_CHECK_EQ(map->block_meta(0, block).payload_bytes,
                       corpus.reference->block_meta(0, block % PATTERNS).payload_bytes);
        ++block;
        state.ResumeTiming();
    }
    report_counters(state, corpus);
}

inline void BM_RowIdConversionDecode(benchmark::State& state, Workload workload) {
    const auto corpus = make_corpus(workload);
    Map::Cursor cursor;
    Map::DecodeWorkspace workspace;
    uint64_t value = 0;
    // Warm allocation/context separately from the measured cache-miss lookups.
    check_status(
            corpus.reference->get(0, (PATTERNS - 1) * Map::BLOCK_ROWS, cursor, workspace, &value));
    uint32_t block = 0;
    uint64_t lookups = 0;
    for (auto _ : state) {
        state.PauseTiming();
        const auto [row, expected] = corpus.inputs[block].live.front();
        const uint32_t source_row = block * Map::BLOCK_ROWS + row;
        state.ResumeTiming();
        const auto start = Clock::now();
        auto status = corpus.reference->get(0, source_row, cursor, workspace, &value);
        benchmark::DoNotOptimize(value);
        const double seconds = std::chrono::duration<double>(Clock::now() - start).count();
        state.SetIterationTime(seconds);
        state.PauseTiming();
        check_status(status);
        DORIS_CHECK_EQ(value, block * corpus.stride + expected);
        block = (block + 1) % PATTERNS;
        ++lookups;
        state.ResumeTiming();
    }
    // Each query moves to another block. A cache-hit regression must not silently
    // turn this into a benchmark of returning one already-decoded row.
    DORIS_CHECK_EQ(cursor.decoded_blocks(), lookups + 1);
    report_counters(state, corpus);
}

BENCHMARK_CAPTURE(BM_RowIdConversionEncode, linear_holes, LINEAR_HOLES)
        ->UseManualTime()
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_RowIdConversionEncode, interleave31_holes, INTERLEAVE31_HOLES)
        ->UseManualTime()
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_RowIdConversionEncode, wide_gaps_holes, WIDE_GAPS_HOLES)
        ->UseManualTime()
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_RowIdConversionDecode, linear_holes, LINEAR_HOLES)
        ->UseManualTime()
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_RowIdConversionDecode, interleave31_holes, INTERLEAVE31_HOLES)
        ->UseManualTime()
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_RowIdConversionDecode, wide_gaps_holes, WIDE_GAPS_HOLES)
        ->UseManualTime()
        ->Unit(benchmark::kMicrosecond);

} // namespace doris::rowid_conversion_benchmark
