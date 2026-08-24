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
//
// Benchmarks for wide::integer<256> division (Decimal256 backing type).
// Each optimized divide() path is measured against a local copy of the
// generic bit-by-bit binary long-division loop that was the only
// implementation before commit d6a5448 ("fast paths for wide-integer
// division"). Both share the same operands in the same TU, so the
// LegacyGeneric vs Default numbers directly quantify the speedup.

#pragma once

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <stdexcept>
#include <vector>

#include "core/types.h"

namespace doris {
namespace {

using wide::operator&;
using wide::operator|;
using wide::operator-;
using wide::operator~;

// ---------------------------------------------------------------------------
// Generic binary long division -- the pre-fast-path implementation. Operates
// on the public operator surface of wide::integer; throws std::domain_error
// instead of doris::Exception to keep the benchmark TU dependency-light.
// There is intentionally only ONE generic loop here (used for both / and %),
// matching the old code shape where operator% reused divide().
// ---------------------------------------------------------------------------
template <size_t Bits>
wide::integer<Bits, unsigned> divide_generic(wide::integer<Bits, unsigned> numerator,
                                             wide::integer<Bits, unsigned> denominator) {
    const wide::integer<Bits, unsigned> zero = 0;
    if (denominator == zero) {
        throw std::domain_error("Division by zero");
    }
    wide::integer<Bits, unsigned> x = 1;
    wide::integer<Bits, unsigned> quotient = 0;
    const wide::integer<Bits, unsigned> one_al = 1;

    while (!(denominator > numerator) && ((denominator >> (Bits - 1)) & one_al) == zero) {
        x = x << 1;
        denominator = denominator << 1;
    }
    while (x != zero) {
        if (!(denominator > numerator)) {
            numerator = numerator - denominator;
            quotient = quotient | x;
        }
        x = x >> 1;
        denominator = denominator >> 1;
    }
    // quotient is returned; numerator now holds the remainder (same contract
    // as _impl::divide, though callers of this helper only see the quotient).
    return quotient;
}

// ---------------------------------------------------------------------------
// Workload set. Each case constructs N (dividend, divisor) pairs that hit
// exactly one code path of the optimized divide():
//   0  BothFit128     -- n,d < 2^128           -> path 1 (native __int128)
//   1  SingleLimb     -- d < 2^64, wide n      -> path 2 (word-by-word, x/10^k)
//   2  TwoLimbKnuth   -- 2^64<=d<2^128, wide n -> path 3 (Knuth Algorithm D)
//   3  TrulyWide      -- d >= 2^128            -> generic loop (unchanged)
// For every case we run (op in {Div,Mod}) x (impl in {Default,LegacyGeneric}).
// ---------------------------------------------------------------------------
struct DivisorCase {
    const char* name;
    std::vector<wide::UInt256> numerators;
    std::vector<wide::UInt256> divisors;
};

wide::UInt256 random_wide_with_low_bits(std::mt19937_64& rng, unsigned low_bits) {
    // Uniform value strictly below 2^low_bits (multiple of 64: 128, 192, 256
    // in these benchmarks). limb(i) is offset from the little-endian limb array.
    wide::UInt256 v = 0;
    for (unsigned i = 0; i < low_bits / 64; ++i) {
        v = v | (wide::UInt256(rng()) << (64 * i));
    }
    return v;
}

wide::UInt256 pow10_int256(unsigned k) {
    wide::UInt256 r = 1;
    for (unsigned i = 0; i < k; ++i) {
        r = r * 10;
    }
    return r;
}

DivisorCase make_case(int case_id, size_t n) {
    std::mt19937_64 rng(0x9e3779b97f4a7c15ULL + case_id);
    DivisorCase c;
    c.numerators.resize(n);
    c.divisors.resize(n);
    switch (case_id) {
    case 0: { // BothFit128: money/count magnitudes
        c.name = "BothFit128";
        for (size_t i = 0; i < n; ++i) {
            c.numerators[i] = random_wide_with_low_bits(rng, 128);
            wide::UInt256 d = random_wide_with_low_bits(rng, 128);
            c.divisors[i] = (d == 0) ? wide::UInt256(1) : d;
        }
        break;
    }
    case 1: { // SingleLimb: x/10^k rounding path, wide dividends, d=10^k
        c.name = "SingleLimb";
        // 10^19 < 2^64; vary k over the Decimal256 scale range used by
        // round/ceil (10^1..10^19) and mix in random 64-bit divisors.
        for (size_t i = 0; i < n; ++i) {
            c.numerators[i] = random_wide_with_low_bits(rng, 256);
            if (i % 2 == 0) {
                const unsigned k = 1 + static_cast<unsigned>(rng() % 19);
                c.divisors[i] = pow10_int256(k);
            } else {
                wide::UInt256 d(rng());
                c.divisors[i] = (d == 0) ? wide::UInt256(1) : d;
            }
        }
        break;
    }
    case 2: { // TwoLimbKnuth: 65..128-bit divisors, wide dividends
        c.name = "TwoLimbKnuth";
        for (size_t i = 0; i < n; ++i) {
            c.numerators[i] = random_wide_with_low_bits(rng, 256);
            wide::UInt256 d = random_wide_with_low_bits(rng, 128);
            // Ensure the divisor genuinely needs two limbs (bit 64 set) so it
            // misses the single-limb path and lands in divide_knuth.
            d = d | (wide::UInt256(1) << 64);
            c.divisors[i] = d;
        }
        break;
    }
    case 3: { // TrulyWide: divisor >= 2^128, unchanged generic loop
        c.name = "TrulyWide";
        for (size_t i = 0; i < n; ++i) {
            // Divisor=2^192 forces generic loop; placing den in the 3rd limb
            // keeps all fast paths (single-limb, two-limb Knuth) off.
            wide::UInt256 num = random_wide_with_low_bits(rng, 256) | (wide::UInt256(1) << 255);
            wide::UInt256 den = random_wide_with_low_bits(rng, 192) | (wide::UInt256(1) << 132);
            c.numerators[i] = num;
            c.divisors[i] = den;
        }
        break;
    }
    }
    return c;
}

template <bool UseLegacyGeneric>
void bench_div(benchmark::State& state, int case_id) {
    const DivisorCase c = make_case(case_id, state.range(0));
    const size_t batch = c.numerators.size();
    size_t idx = 0;
    for (auto _ : state) {
        for (size_t k = 0; k < batch; ++k) {
            wide::UInt256 n = c.numerators[idx];
            const wide::UInt256 d = c.divisors[idx];
            idx = (idx + 1) & (batch - 1); // batch is always 4096 (power of 2)
            if constexpr (UseLegacyGeneric) {
                wide::UInt256 q = divide_generic(n, d);
                benchmark::DoNotOptimize(q);
            } else {
                // divide() writes the remainder into its first argument, so each
                // iteration must restart from a fresh copy of the dividend.
                wide::UInt256 q = n / d;
                benchmark::DoNotOptimize(q);
            }
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batch));
}

template <bool UseLegacyGeneric>
void bench_mod(benchmark::State& state, int case_id) {
    const DivisorCase c = make_case(case_id, state.range(0));
    const size_t batch = c.numerators.size();
    size_t idx = 0;
    for (auto _ : state) {
        for (size_t k = 0; k < batch; ++k) {
            wide::UInt256 n = c.numerators[idx];
            const wide::UInt256 d = c.divisors[idx];
            idx = (idx + 1) & (batch - 1); // batch is always 4096 (power of 2)
            if constexpr (UseLegacyGeneric) {
                // Old behavior: operator% could not go faster than divide() since
                // it consumed divide()'s remainder slot without extra lanes.
                wide::UInt256 q = n;
                wide::UInt256 sink = divide_generic(q, d);
                benchmark::DoNotOptimize(sink);
                benchmark::DoNotOptimize(q); // remainder lands in the first arg
            } else {
                wide::UInt256 r = n % d;
                benchmark::DoNotOptimize(r);
            }
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batch));
}

// Batch size: large enough to hide div-by-zero retry noise, small enough to
// stay in L2. Parameter sweeps over 4 cases.
void ArgDivisorCases(benchmark::internal::Benchmark* b) {
    b->ArgNames({"batch"});
    b->Args({65536});
    // Drop Iterations() so google benchmark's --benchmark_min_time auto-scales
    // sub-repeats. Batch=65536 keeps a single iter in the 0.9..59ms range.
}

// Thin concrete wrappers: template-argument commas would split the preprocessor
// capture list inside BENCHMARK, so each (op, impl) must be its own function.
#define WIDE_DIV_BENCH(name, case_id)                               \
    static void name##_Div_Default(benchmark::State& state) {       \
        bench_div<false>(state, case_id);                           \
    }                                                               \
    static void name##_Div_LegacyGeneric(benchmark::State& state) { \
        bench_div<true>(state, case_id);                            \
    }                                                               \
    static void name##_Mod_Default(benchmark::State& state) {       \
        bench_mod<false>(state, case_id);                           \
    }                                                               \
    static void name##_Mod_LegacyGeneric(benchmark::State& state) { \
        bench_mod<true>(state, case_id);                            \
    }                                                               \
    BENCHMARK(name##_Div_Default)->Apply(ArgDivisorCases);          \
    BENCHMARK(name##_Div_LegacyGeneric)->Apply(ArgDivisorCases);    \
    BENCHMARK(name##_Mod_Default)->Apply(ArgDivisorCases);          \
    BENCHMARK(name##_Mod_LegacyGeneric)->Apply(ArgDivisorCases);

WIDE_DIV_BENCH(BM_WideInt256BothFit128, 0)
WIDE_DIV_BENCH(BM_WideInt256SingleLimb, 1)
WIDE_DIV_BENCH(BM_WideInt256TwoLimbKnuth, 2)
WIDE_DIV_BENCH(BM_WideInt256TrulyWide, 3)

} // namespace
} // namespace doris
