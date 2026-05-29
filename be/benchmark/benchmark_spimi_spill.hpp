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
#include <random>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_merger.h"
#include "storage/index/inverted/spimi/spill_manager.h"

namespace doris::segment_v2::inverted_index::spimi {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
namespace {

// Generate a random term of given length using lowercase letters.
std::string RandomTerm(std::mt19937& rng, int len) {
    static constexpr char kAlpha[] = "abcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<int> dist(0, sizeof(kAlpha) - 2);
    std::string s(len, ' ');
    for (int i = 0; i < len; ++i) {
        s[i] = kAlpha[dist(rng)];
    }
    return s;
}

// Pre-generate a vocabulary of `vocab_size` random terms of length [4, 12).
std::vector<std::string> MakeVocab(int64_t vocab_size, int seed = 42) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> len_dist(4, 11);
    std::vector<std::string> vocab;
    vocab.reserve(vocab_size);
    for (int64_t i = 0; i < vocab_size; ++i) {
        vocab.push_back(RandomTerm(rng, len_dist(rng)));
    }
    return vocab;
}

// Fill a buffer with `num_records` records drawn from `vocab`.
// doc_ids are sequential starting from 0 (matching Doris's
// _rid++ monotonic doc_id guarantee); positions are sequential
// within each doc (also matching Doris's token-position
// monotonicity). This produces representative workload data
// and keeps _compact_streams_sorted == true for the fast path.
void FillBuffer(SpimiPostingBuffer& buffer, const std::vector<std::string>& vocab,
                int64_t num_records, int64_t num_docs, int seed = 123) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<size_t> term_dist(0, vocab.size() - 1);
    uint32_t doc_id = 0;
    uint32_t pos = 0;
    for (int64_t i = 0; i < num_records; ++i) {
        const auto& term = vocab[term_dist(rng)];
        buffer.Append(term, doc_id, pos++);
        // Advance doc_id every few positions to simulate multi-doc
        // workloads (num_docs controls the average occurrences per term).
        if (pos >= static_cast<uint32_t>(num_records / num_docs)) {
            ++doc_id;
            pos = 0;
        }
    }
}

// Build a SegmentMerger::Input from a SpillSegment.
SegmentMerger::Input ToInput(const SpillSegment& seg) {
    SegmentMerger::Input in;
    in.tis_bytes = seg.tis_bytes;
    in.tii_bytes = seg.tii_bytes;
    in.frq_bytes = seg.frq_bytes;
    in.prx_bytes = seg.prx_bytes;
    in.doc_count = seg.doc_count;
    return in;
}

// Create a sink with fresh MemoryByteOutput for each stream.
struct OwnedSink {
    MemoryByteOutput tis, tii, frq, prx, fnm, nrm, seg_n, seg_gen;
    SpimiSegmentSink AsSink() {
        return {.tis = &tis, .tii = &tii, .frq = &frq, .prx = &prx,
                .fnm = &fnm, .nrm = &nrm, .segments_n = &seg_n, .segments_gen = &seg_gen};
    }
};

} // namespace

// ===========================================================================
// 1. PostingBuffer Append throughput
// ===========================================================================

// Measure Append() throughput with low cardinality (100 distinct terms).
static void BM_BufferAppend_LowCardinality(benchmark::State& state) {
    const int64_t num_records = state.range(0);
    auto vocab = MakeVocab(100);
    for (auto _ : state) {
        SpimiPostingBuffer buffer;
        FillBuffer(buffer, vocab, num_records, 10000);
        benchmark::DoNotOptimize(buffer);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * num_records);
}
BENCHMARK(BM_BufferAppend_LowCardinality)->Arg(10000)->Arg(100000)->Arg(500000);

// Measure Append() throughput with high cardinality (50000 distinct terms).
static void BM_BufferAppend_HighCardinality(benchmark::State& state) {
    const int64_t num_records = state.range(0);
    auto vocab = MakeVocab(50000);
    for (auto _ : state) {
        SpimiPostingBuffer buffer;
        FillBuffer(buffer, vocab, num_records, 10000);
        benchmark::DoNotOptimize(buffer);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * num_records);
}
BENCHMARK(BM_BufferAppend_HighCardinality)->Arg(10000)->Arg(100000)->Arg(500000);

// ===========================================================================
// 2. PostingBuffer Sort
// ===========================================================================

static void BM_BufferSort(benchmark::State& state) {
    const int64_t num_records = state.range(0);
    const int64_t vocab_size = state.range(1);
    auto vocab = MakeVocab(vocab_size);
    for (auto _ : state) {
        state.PauseTiming();
        SpimiPostingBuffer buffer;
        FillBuffer(buffer, vocab, num_records, 10000);
        state.ResumeTiming();
        buffer.Sort();
        benchmark::DoNotOptimize(buffer);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * num_records);
}
BENCHMARK(BM_BufferSort)->Args({10000, 100})->Args({100000, 100})->Args({500000, 100})->Args({10000, 50000})->Args({100000, 50000})->Args({500000, 50000});

// ===========================================================================
// 3. SpillManager FlushBuffer (sort + emit segment)
// ===========================================================================

static void BM_SpillFlush(benchmark::State& state) {
    const int64_t num_records = state.range(0);
    const int64_t vocab_size = state.range(1);
    auto vocab = MakeVocab(vocab_size);
    for (auto _ : state) {
        state.PauseTiming();
        SpimiPostingBuffer buffer;
        FillBuffer(buffer, vocab, num_records, 10000);
        SpillManager mgr("content");
        state.ResumeTiming();
        mgr.FlushBuffer(buffer, 10000);
        benchmark::DoNotOptimize(mgr);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * num_records);
}
BENCHMARK(BM_SpillFlush)->Args({10000, 100})->Args({50000, 100})->Args({100000, 100})->Args({10000, 5000})->Args({50000, 5000})->Args({100000, 5000});

// ===========================================================================
// 4. SegmentMerger k-way merge
// ===========================================================================

// Merge N segments, each with `records_per_seg` records from a shared
// vocab of `vocab_size` terms. Measures merge throughput.
static void BM_SegmentMerge(benchmark::State& state) {
    const int64_t num_segments = state.range(0);
    const int64_t records_per_seg = state.range(1);
    const int64_t vocab_size = state.range(2);
    auto vocab = MakeVocab(vocab_size);

    for (auto _ : state) {
        state.PauseTiming();
        SpillManager mgr("content");
        std::vector<SegmentMerger::Input> inputs;
        for (int64_t s = 0; s < num_segments; ++s) {
            SpimiPostingBuffer buffer;
            FillBuffer(buffer, vocab, records_per_seg, 1000, /*seed=*/static_cast<int>(s * 1000));
            mgr.FlushBuffer(buffer, 1000);
            inputs.push_back(ToInput(mgr.Spills().back()));
        }
        OwnedSink owned;
        state.ResumeTiming();
        SegmentMerger::Merge(inputs, owned.AsSink(), "_merged", "content",
                             static_cast<int32_t>(1000 * num_segments),
                             FieldInfosWriter::kIndexVersionV1, false, true);
        benchmark::DoNotOptimize(owned);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * num_segments * records_per_seg);
}
BENCHMARK(BM_SegmentMerge)
    ->Args({2, 10000, 100})->Args({4, 10000, 100})->Args({8, 10000, 100})->Args({16, 10000, 100})
    ->Args({2, 50000, 100})->Args({4, 50000, 100})->Args({8, 50000, 100})
    ->Args({2, 10000, 5000})->Args({4, 10000, 5000})->Args({8, 10000, 5000})
    ->Args({2, 50000, 5000})->Args({4, 50000, 5000})->Args({8, 50000, 5000});

// ===========================================================================
// 5. End-to-end: single large buffer vs multiple spills + merge
// ===========================================================================

// Baseline: one big buffer, no spill, direct emit.
static void BM_NoSpill_EmitOnce(benchmark::State& state) {
    const int64_t total_records = state.range(0);
    const int64_t vocab_size = state.range(1);
    auto vocab = MakeVocab(vocab_size);

    for (auto _ : state) {
        state.PauseTiming();
        SpimiPostingBuffer buffer;
        FillBuffer(buffer, vocab, total_records, 50000);
        OwnedSink owned;
        state.ResumeTiming();
        buffer.Sort();
        SpimiFulltextWriter::EmitSegment(buffer, owned.AsSink(), "_0", "content", 50000,
                                         FieldInfosWriter::kIndexVersionV1, false, true);
        benchmark::DoNotOptimize(owned);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * total_records);
}
BENCHMARK(BM_NoSpill_EmitOnce)->Args({100000, 500})->Args({500000, 500})->Args({100000, 10000})->Args({500000, 10000});

// Spill path: split into N spills, then k-way merge.
static void BM_WithSpill_MergeN(benchmark::State& state) {
    const int64_t total_records = state.range(0);
    const int64_t vocab_size = state.range(1);
    const int64_t num_spills = state.range(2);
    auto vocab = MakeVocab(vocab_size);

    for (auto _ : state) {
        state.PauseTiming();
        const int64_t records_per_spill = total_records / num_spills;
        std::vector<SegmentMerger::Input> inputs;
        SpillManager mgr("content");
        for (int64_t s = 0; s < num_spills; ++s) {
            SpimiPostingBuffer buffer;
            FillBuffer(buffer, vocab, records_per_spill, 50000, /*seed=*/static_cast<int>(s * 777));
            mgr.FlushBuffer(buffer, 50000);
            inputs.push_back(ToInput(mgr.Spills().back()));
        }
        OwnedSink owned;
        state.ResumeTiming();
        SegmentMerger::Merge(inputs, owned.AsSink(), "_merged", "content",
                             50000, FieldInfosWriter::kIndexVersionV1, false, true);
        benchmark::DoNotOptimize(owned);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * total_records);
}
BENCHMARK(BM_WithSpill_MergeN)
    ->Args({100000, 500, 2})->Args({100000, 500, 4})->Args({100000, 500, 8})
    ->Args({500000, 500, 2})->Args({500000, 500, 4})->Args({500000, 500, 8})
    ->Args({100000, 10000, 2})->Args({100000, 10000, 4})->Args({100000, 10000, 8})
    ->Args({500000, 10000, 2})->Args({500000, 10000, 4})->Args({500000, 10000, 8});

// ===========================================================================
// 6. Memory overhead: measure bytes per record at different cardinalities
// ===========================================================================

static void BM_BufferMemoryPerRecord(benchmark::State& state) {
    const int64_t num_records = state.range(0);
    const int64_t vocab_size = state.range(1);
    auto vocab = MakeVocab(vocab_size);
    for (auto _ : state) {
        SpimiPostingBuffer buffer;
        FillBuffer(buffer, vocab, num_records, 10000);
        benchmark::DoNotOptimize(buffer);
        // Report bytes/record as custom counter.
        const double bytes = static_cast<double>(buffer.MemoryUsage());
        state.counters["bytes_per_record"] = bytes / num_records;
        state.counters["total_bytes"] = bytes;
    }
}
BENCHMARK(BM_BufferMemoryPerRecord)
    ->Args({10000, 100})->Args({100000, 100})->Args({500000, 100})
    ->Args({10000, 5000})->Args({100000, 5000})->Args({500000, 5000})
    ->Args({10000, 50000})->Args({100000, 50000})->Args({500000, 50000});

} // namespace doris::segment_v2::inverted_index::spimi
