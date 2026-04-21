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

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "core/column/column_string.h"
#include "core/string_buffer.hpp"
#include "util/var_int.h"

namespace doris {

// ─────────────────────── Old implementation ───────────────────────
struct RetentionState_Old {
    static constexpr size_t MAX_EVENTS = 32;
    uint8_t events[MAX_EVENTS] = {0};

    void reset() {
        for (int64_t i = 0; i < MAX_EVENTS; i++) {
            events[i] = 0;
        }
    }

    void set(int event) { events[event] = 1; }

    void merge(const RetentionState_Old& other) {
        for (int64_t i = 0; i < MAX_EVENTS; i++) {
            events[i] |= other.events[i];
        }
    }

    void write(BufferWritable& out) const {
        int64_t serialized_events = 0;
        for (int64_t i = 0; i < MAX_EVENTS; i++) {
            serialized_events |= events[i];
            serialized_events <<= 1;
        }
        write_var_int(serialized_events, out);
    }

    void read(BufferReadable& in) {
        int64_t serialized_events = 0;
        uint64_t u_serialized_events = 0;
        read_var_int(serialized_events, in);
        u_serialized_events = serialized_events;

        u_serialized_events >>= 1;
        for (int64_t i = MAX_EVENTS - 1; i >= 0; i--) {
            events[i] = (uint8_t)(1 & u_serialized_events);
            u_serialized_events >>= 1;
        }
    }
};

// ─────────────────────── New implementation ───────────────────────
struct RetentionState_New {
    static constexpr size_t MAX_EVENTS = 32;
    uint32_t events = 0;

    void reset() { events = 0; }

    void set(int event) { events |= (1u << event); }

    void merge(const RetentionState_New& other) { events |= other.events; }

    void write(BufferWritable& out) const { out.write_binary(events); }

    void read(BufferReadable& in) { in.read_binary(events); }
};

// ─────────────────────── Benchmark helpers ───────────────────────

// Build a vector of N states with pseudo-random bits set
template <typename State>
static std::vector<State> make_states(int n, int events_per_state = 8) {
    std::vector<State> states(n);
    for (int i = 0; i < n; i++) {
        for (int e = 0; e < events_per_state; e++) {
            int bit = (i * 7 + e * 3) % State::MAX_EVENTS;
            states[i].set(bit);
        }
    }
    return states;
}

// ─────────────────────── merge benchmarks ───────────────────────

static void BM_RetentionMerge_Old(benchmark::State& state) {
    constexpr int N = 10000;
    auto states = make_states<RetentionState_Old>(N);
    RetentionState_Old acc;
    for (auto _ : state) {
        acc.reset();
        for (const auto& s : states) {
            acc.merge(s);
        }
        benchmark::DoNotOptimize(acc);
    }
}

static void BM_RetentionMerge_New(benchmark::State& state) {
    constexpr int N = 10000;
    auto states = make_states<RetentionState_New>(N);
    RetentionState_New acc;
    for (auto _ : state) {
        acc.reset();
        for (const auto& s : states) {
            acc.merge(s);
        }
        benchmark::DoNotOptimize(acc);
    }
}

// ─────────────────────── serialize/deserialize benchmarks ───────────────────────

static void BM_RetentionSerialize_Old(benchmark::State& state) {
    constexpr int N = 10000;
    auto states = make_states<RetentionState_Old>(N);

    for (auto _ : state) {
        auto col = ColumnString::create();
        for (const auto& s : states) {
            BufferWritable buf(*col);
            s.write(buf);
            buf.commit();
        }
        benchmark::DoNotOptimize(col);
    }
}

static void BM_RetentionSerialize_New(benchmark::State& state) {
    constexpr int N = 10000;
    auto states = make_states<RetentionState_New>(N);

    for (auto _ : state) {
        auto col = ColumnString::create();
        for (const auto& s : states) {
            BufferWritable buf(*col);
            s.write(buf);
            buf.commit();
        }
        benchmark::DoNotOptimize(col);
    }
}

static void BM_RetentionDeserialize_Old(benchmark::State& state) {
    constexpr int N = 10000;
    auto states = make_states<RetentionState_Old>(N);

    // pre-serialize
    auto col = ColumnString::create();
    for (const auto& s : states) {
        BufferWritable buf(*col);
        s.write(buf);
        buf.commit();
    }

    for (auto _ : state) {
        std::vector<RetentionState_Old> results(N);
        for (int i = 0; i < N; i++) {
            StringRef ref = col->get_data_at(i);
            BufferReadable rbuf(ref);
            results[i].read(rbuf);
        }
        benchmark::DoNotOptimize(results);
    }
}

static void BM_RetentionDeserialize_New(benchmark::State& state) {
    constexpr int N = 10000;
    auto states = make_states<RetentionState_New>(N);

    // pre-serialize
    auto col = ColumnString::create();
    for (const auto& s : states) {
        BufferWritable buf(*col);
        s.write(buf);
        buf.commit();
    }

    for (auto _ : state) {
        std::vector<RetentionState_New> results(N);
        for (int i = 0; i < N; i++) {
            StringRef ref = col->get_data_at(i);
            BufferReadable rbuf(ref);
            results[i].read(rbuf);
        }
        benchmark::DoNotOptimize(results);
    }
}

// ─────────────────────── reset benchmarks ───────────────────────

static void BM_RetentionReset_Old(benchmark::State& state) {
    constexpr int N = 10000;
    auto states = make_states<RetentionState_Old>(N);
    for (auto _ : state) {
        for (auto& s : states) {
            s.reset();
        }
        benchmark::DoNotOptimize(states);
    }
}

static void BM_RetentionReset_New(benchmark::State& state) {
    constexpr int N = 10000;
    auto states = make_states<RetentionState_New>(N);
    for (auto _ : state) {
        for (auto& s : states) {
            s.reset();
        }
        benchmark::DoNotOptimize(states);
    }
}

} // namespace doris

// ─────────────────────── Register ───────────────────────

#define REGISTER_RETENTION_BM(name)                                                 \
    BENCHMARK(doris::name)                                                          \
            ->Unit(benchmark::kNanosecond)                                          \
            ->Repetitions(5)                                                        \
            ->DisplayAggregatesOnly()                                               \
            ->ComputeStatistics("min",                                              \
                                [](const std::vector<double>& v) -> double {        \
                                    return *std::min_element(v.begin(), v.end());   \
                                })                                                  \
            ->ComputeStatistics("max", [](const std::vector<double>& v) -> double { \
                return *std::max_element(v.begin(), v.end());                       \
            });

REGISTER_RETENTION_BM(BM_RetentionMerge_Old)
REGISTER_RETENTION_BM(BM_RetentionMerge_New)
REGISTER_RETENTION_BM(BM_RetentionSerialize_Old)
REGISTER_RETENTION_BM(BM_RetentionSerialize_New)
REGISTER_RETENTION_BM(BM_RetentionDeserialize_Old)
REGISTER_RETENTION_BM(BM_RetentionDeserialize_New)
REGISTER_RETENTION_BM(BM_RetentionReset_Old)
REGISTER_RETENTION_BM(BM_RetentionReset_New)
