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

// Standalone benchmark; no Doris libraries or Google Benchmark are required.
// Build from the repository root:
// c++ -O3 -DNDEBUG -std=c++20 -march=x86-64-v3 -Ibe/src be/benchmark/selection_compaction_bench.cpp -o /tmp/selection_compaction_bench
// Run: taskset -c <cpu> /tmp/selection_compaction_bench --help

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "util/simd/bits.h"
#include "util/simd/selection.h"

namespace selection_compaction_bench {

using RowId = uint16_t;
using Clock = std::chrono::steady_clock;
using Select = size_t (*)(const int32_t*, size_t, RowId*);
using Compact = uint16_t (*)(const uint8_t*, uint16_t, RowId*);
constexpr int32_t THRESHOLD = 100;
constexpr size_t COMPARISON_BATCH = 64;
constexpr RowId CANARY = 0xabcd;

struct SelectionTable {
    std::array<std::array<RowId, 8>, 256> positions {};
    std::array<uint8_t, 256> counts {};

    constexpr SelectionTable() {
        for (size_t mask = 0; mask < 256; ++mask) {
            for (size_t bit = 0; bit < 8; ++bit) {
                if ((mask & (1U << bit)) != 0) {
                    positions[mask][counts[mask]++] = static_cast<RowId>(bit);
                }
            }
        }
    }
};

constexpr SelectionTable SELECTION_TABLE;

static_assert(doris::simd::bits_mask_length() == 32,
              "The lookup candidate in this benchmark implements the x86 32-row mask layout");

inline uint16_t append_indices32(uint32_t mask, uint16_t base, RowId* output) {
    uint16_t written = 0;
    for (unsigned offset = 0; offset < 32; offset += 8) {
        const auto part = static_cast<uint8_t>(mask >> offset);
        RowId indices[8];
        for (unsigned lane = 0; lane < 8; ++lane) {
            indices[lane] =
                    static_cast<RowId>(base + offset + SELECTION_TABLE.positions[part][lane]);
        }
        // At most base + offset rows have been emitted, so eight stores fit in the input size.
        std::memcpy(output + written, indices, sizeof(indices));
        written += SELECTION_TABLE.counts[part];
    }
    return written;
}

// SPARSE_LIMIT=-1 is the original SegmentIterator compaction loop; -2 uses the production
// append helper. Zero selects lookup for every partial mask. Positive limits retain ctz for
// sparse partial masks. Zero/full-mask handling and the scalar tail stay identical.
template <int SPARSE_LIMIT>
__attribute__((noinline)) uint16_t doris_compact(const uint8_t* flags, uint16_t selected_size,
                                                 RowId* output) {
    uint16_t new_size = 0;
    uint16_t sel_pos = 0;
    const uint16_t sel_end = sel_pos + selected_size;
    static constexpr size_t SIMD_BYTES = doris::simd::bits_mask_length();
    const uint16_t sel_end_simd = sel_pos + selected_size / SIMD_BYTES * SIMD_BYTES;
    while (sel_pos < sel_end_simd) {
        auto mask = doris::simd::bytes_mask_to_bits_mask(flags + sel_pos);
        if (mask == 0) {
            // No selected rows.
        } else if (mask == doris::simd::bits_mask_all()) {
            for (uint16_t i = 0; i < SIMD_BYTES; ++i) {
                output[new_size++] = sel_pos + i;
            }
        } else {
            if constexpr (SPARSE_LIMIT == -2) {
                if (__builtin_popcount(mask) > 2) {
                    new_size +=
                            doris::simd::append_selected_indices(mask, sel_pos, output + new_size);
                } else {
                    doris::simd::iterate_through_bits_mask(
                            [&](const int bit_pos) {
                                output[new_size++] = sel_pos + static_cast<uint16_t>(bit_pos);
                            },
                            mask);
                }
            } else if constexpr (SPARSE_LIMIT == -1) {
                doris::simd::iterate_through_bits_mask(
                        [&](const int bit_pos) {
                            output[new_size++] = sel_pos + static_cast<uint16_t>(bit_pos);
                        },
                        mask);
            } else {
                if (SPARSE_LIMIT > 0 && __builtin_popcount(mask) <= SPARSE_LIMIT) {
                    doris::simd::iterate_through_bits_mask(
                            [&](const int bit_pos) {
                                output[new_size++] = sel_pos + static_cast<uint16_t>(bit_pos);
                            },
                            mask);
                } else {
                    new_size += append_indices32(mask, sel_pos, output + new_size);
                }
            }
        }
        sel_pos += SIMD_BYTES;
    }
    for (; sel_pos < sel_end; ++sel_pos) {
        if (flags[sel_pos]) {
            output[new_size++] = sel_pos;
        }
    }
    return new_size;
}

__attribute__((noinline)) void evaluate(const int32_t* __restrict values, size_t rows,
                                        uint8_t* __restrict flags) {
    for (size_t i = 0; i < rows; ++i) {
        flags[i] = values[i] < THRESHOLD;
    }
}

__attribute__((noinline)) size_t branched(const int32_t* __restrict values, size_t rows,
                                          RowId* __restrict output) {
    size_t count = 0;
    for (size_t i = 0; i < rows; ++i) {
        if (values[i] < THRESHOLD) {
            output[count++] = static_cast<RowId>(i);
        }
    }
    return count;
}

__attribute__((noinline)) size_t branchless(const int32_t* __restrict values, size_t rows,
                                            RowId* __restrict output) {
    size_t count = 0;
    for (size_t i = 0; i < rows; ++i) {
        output[count] = static_cast<RowId>(i);
        count += values[i] < THRESHOLD;
    }
    return count;
}

inline uint8_t pack_eight(const uint8_t* matches) {
    uint64_t word;
    std::memcpy(&word, matches, sizeof(word));
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    word = __builtin_bswap64(word);
#endif
    return static_cast<uint8_t>((word * UINT64_C(0x0102040810204080)) >> 56);
}

template <bool LOOKUP>
__attribute__((noinline)) size_t batched(const int32_t* __restrict values, size_t rows,
                                         RowId* __restrict output) {
    size_t count = 0;
    size_t base = 0;
    for (; base + COMPARISON_BATCH <= rows; base += COMPARISON_BATCH) {
        uint8_t matches[COMPARISON_BATCH];
        for (size_t i = 0; i < COMPARISON_BATCH; ++i) {
            matches[i] = values[base + i] < THRESHOLD;
        }
        if constexpr (LOOKUP) {
            for (size_t group = 0; group < COMPARISON_BATCH; group += 8) {
                const auto mask = pack_eight(matches + group);
                RowId positions[8];
                for (size_t lane = 0; lane < 8; ++lane) {
                    positions[lane] = static_cast<RowId>(base + group +
                                                         SELECTION_TABLE.positions[mask][lane]);
                }
                // Always write eight entries; only counts[mask] entries become valid.
                // count <= base + group, so a full group fits in rows output entries.
                std::memcpy(output + count, positions, sizeof(positions));
                count += SELECTION_TABLE.counts[mask];
            }
        } else {
            uint64_t mask = 0;
            for (size_t group = 0; group < COMPARISON_BATCH; group += 8) {
                mask |= static_cast<uint64_t>(pack_eight(matches + group)) << group;
            }
            while (mask != 0) {
                output[count++] = static_cast<RowId>(base + __builtin_ctzll(mask));
                mask &= mask - 1;
            }
        }
    }
    for (; base < rows; ++base) {
        if (values[base] < THRESHOLD) {
            output[count++] = static_cast<RowId>(base);
        }
    }
    return count;
}

enum class Algorithm {
    BRANCHED,
    BRANCHLESS,
    LOOKUP,
    CTZ,
    FUSED_COPY,
    DORIS_CTZ,
    DORIS_LOOKUP,
    DORIS_HYBRID1,
    DORIS_HYBRID2,
    DORIS_HYBRID4,
    DORIS_HYBRID8,
    DORIS_HYBRID16,
    DORIS_PRODUCTION
};
constexpr std::array<Select, 4> SELECTORS {branched, branchless, batched<true>, batched<false>};
constexpr std::array<Compact, 8> COMPACTORS {doris_compact<-1>, doris_compact<0>, doris_compact<1>,
                                             doris_compact<2>,  doris_compact<4>, doris_compact<8>,
                                             doris_compact<16>, doris_compact<-2>};
constexpr std::array<const char*, 13> NAMES {
        "branched",      "branchless",     "batch_lookup",    "batch_ctz",     "fused_copy",
        "doris_ctz",     "doris_lookup",   "doris_hybrid1",   "doris_hybrid2", "doris_hybrid4",
        "doris_hybrid8", "doris_hybrid16", "doris_production"};

template <size_t WORDS>
using Payload = std::array<uint64_t, WORDS>;

template <size_t WORDS>
__attribute__((noinline)) size_t fused_copy(const int32_t* __restrict values,
                                            const Payload<WORDS>* __restrict payload, size_t rows,
                                            Payload<WORDS>* __restrict output) {
    size_t count = 0;
    for (size_t i = 0; i < rows; ++i) {
        if (values[i] < THRESHOLD) {
            output[count++] = payload[i];
        }
    }
    return count;
}

struct Options {
    size_t rows = 1U << 20;
    size_t batch = 4096;
    size_t repeats = 7;
    double min_ms = 20;
    int rate = -1;
    std::string pattern = "all";
    std::string mode = "all";
    std::string suite = "doris";
    std::string input = "flags";
    bool verify_only = false;
};

template <size_t WORDS>
struct Data {
    std::vector<int32_t> values;
    std::vector<uint8_t> flags;
    std::vector<Payload<WORDS>> payload;
    size_t batch;
    bool evaluate_values;

    Data(size_t rows, size_t batch_size, int rate, const std::string& pattern,
         bool with_evaluation = false)
            : values(rows), flags(rows), batch(batch_size), evaluate_values(with_evaluation) {
        std::mt19937 random(0x51ec710);
        for (size_t base = 0; base < rows; base += batch) {
            const auto count = std::min(batch, rows - base);
            const auto passing = (count * rate + 50) / 100;
            for (size_t i = 0; i < count; ++i) {
                values[base + i] = i < passing ? static_cast<int32_t>(random() % 100)
                                               : static_cast<int32_t>(100 + random() % 10000);
            }
            if (pattern == "random") {
                std::shuffle(values.begin() + base, values.begin() + base + count, random);
            }
        }
        for (size_t i = 0; i < rows; ++i) {
            flags[i] = values[i] < THRESHOLD;
        }
        if constexpr (WORDS != 0) {
            payload.resize(rows);
            for (size_t i = 0; i < rows; ++i) {
                for (size_t word = 0; word < WORDS; ++word) {
                    payload[i][word] = random() ^ (static_cast<uint64_t>(i) << 32) ^ word;
                }
            }
        }
    }
};

template <size_t WORDS>
struct Scratch {
    std::vector<RowId> selection;
    std::vector<uint8_t> flags;
    std::vector<Payload<WORDS>> copied;

    explicit Scratch(size_t batch) : selection(batch + 8, CANARY), flags(batch) {
        if constexpr (WORDS != 0) {
            copied.resize(batch + 1);
        }
    }
};

template <size_t WORDS, Algorithm ALGORITHM>
uint64_t run(const Data<WORDS>& data, Scratch<WORDS>& scratch, size_t passes) {
    uint64_t total = 0;
    for (size_t pass = 0; pass < passes; ++pass) {
        for (size_t base = 0; base < data.values.size(); base += data.batch) {
            const auto rows = std::min(data.batch, data.values.size() - base);
            size_t count;
            if constexpr (ALGORITHM == Algorithm::FUSED_COPY) {
                count = fused_copy<WORDS>(data.values.data() + base, data.payload.data() + base,
                                          rows, scratch.copied.data());
            } else {
                if constexpr (ALGORITHM >= Algorithm::DORIS_CTZ) {
                    const uint8_t* flags = data.flags.data() + base;
                    if (data.evaluate_values) {
                        evaluate(data.values.data() + base, rows, scratch.flags.data());
                        flags = scratch.flags.data();
                    }
                    count = COMPACTORS[static_cast<size_t>(ALGORITHM) -
                                       static_cast<size_t>(Algorithm::DORIS_CTZ)](
                            flags, static_cast<uint16_t>(rows), scratch.selection.data());
                } else {
                    count = SELECTORS[static_cast<size_t>(ALGORITHM)](
                            data.values.data() + base, rows, scratch.selection.data());
                }
                if constexpr (WORDS != 0) {
                    for (size_t i = 0; i < count; ++i) {
                        scratch.copied[i] = data.payload[base + scratch.selection[i]];
                    }
                }
            }
            total += count;
            // Make every batch's stores observable without timing a checksum over the output.
            asm volatile(""
                         :
                         : "g"(scratch.selection.data()), "g"(scratch.copied.data())
                         : "memory");
        }
    }
    asm volatile("" : : "g"(total) : "memory");
    return total;
}

void verify_compaction(const uint8_t* flags, size_t rows) {
    std::vector<RowId> expected;
    for (size_t i = 0; i < rows; ++i) {
        if (flags[i]) {
            expected.push_back(static_cast<RowId>(i));
        }
    }
    for (auto compact : COMPACTORS) {
        std::vector<RowId> output(rows + 8, CANARY);
        const auto count = compact(flags, static_cast<uint16_t>(rows), output.data());
        if (count != expected.size() ||
            !std::equal(expected.begin(), expected.end(), output.begin()) ||
            !std::all_of(output.begin() + rows, output.end(),
                         [](RowId x) { return x == CANARY; })) {
            throw std::runtime_error("Doris compaction result, row order, or bounds mismatch");
        }
    }
}

void verify_selection(const int32_t* values, size_t rows) {
    std::vector<uint8_t> flags(rows);
    evaluate(values, rows, flags.data());
    verify_compaction(flags.data(), rows);
    std::vector<RowId> expected;
    for (size_t i = 0; i < rows; ++i) {
        if (values[i] < THRESHOLD) {
            expected.push_back(static_cast<RowId>(i));
        }
    }
    for (auto select : SELECTORS) {
        std::vector<RowId> output(rows + 8, CANARY);
        const auto count = select(values, rows, output.data());
        if (count != expected.size() ||
            !std::equal(expected.begin(), expected.end(), output.begin()) ||
            !std::all_of(output.begin() + rows, output.end(),
                         [](RowId x) { return x == CANARY; })) {
            throw std::runtime_error("Selector result, row order, or output bounds mismatch");
        }
    }
}

template <size_t WORDS>
void verify(const Data<WORDS>& data) {
    Scratch<WORDS> scratch(data.batch);
    for (size_t base = 0; base < data.values.size(); base += data.batch) {
        const auto rows = std::min(data.batch, data.values.size() - base);
        verify_selection(data.values.data() + base, rows);
        if constexpr (WORDS != 0) {
            std::vector<Payload<WORDS>> expected;
            for (size_t i = 0; i < rows; ++i) {
                if (data.values[base + i] < THRESHOLD) {
                    expected.push_back(data.payload[base + i]);
                }
            }
            for (auto select : SELECTORS) {
                const auto count =
                        select(data.values.data() + base, rows, scratch.selection.data());
                for (size_t i = 0; i < count; ++i) {
                    scratch.copied[i] = data.payload[base + scratch.selection[i]];
                }
                if (!std::equal(expected.begin(), expected.end(), scratch.copied.begin())) {
                    throw std::runtime_error("Selected payload mismatch");
                }
            }
            evaluate(data.values.data() + base, rows, scratch.flags.data());
            if (!std::equal(scratch.flags.begin(), scratch.flags.begin() + rows,
                            data.flags.begin() + base)) {
                throw std::runtime_error("Predicate flags mismatch");
            }
            for (auto compact : COMPACTORS) {
                const auto count = compact(scratch.flags.data(), static_cast<uint16_t>(rows),
                                           scratch.selection.data());
                for (size_t i = 0; i < count; ++i) {
                    scratch.copied[i] = data.payload[base + scratch.selection[i]];
                }
                if (count != expected.size() ||
                    !std::equal(expected.begin(), expected.end(), scratch.copied.begin())) {
                    throw std::runtime_error("Doris selected payload mismatch");
                }
            }
            const auto count =
                    fused_copy<WORDS>(data.values.data() + base, data.payload.data() + base, rows,
                                      scratch.copied.data());
            if (count != expected.size() ||
                !std::equal(expected.begin(), expected.end(), scratch.copied.begin())) {
                throw std::runtime_error("Fused payload mismatch");
            }
        }
    }
}

void self_test() {
    verify_selection(nullptr, 0);
    for (size_t mask = 0; mask < 256; ++mask) {
        std::array<int32_t, 64> values {};
        for (size_t i = 0; i < values.size(); ++i) {
            values[i] = (mask & (1U << (i % 8))) != 0 ? 0 : THRESHOLD;
        }
        verify_selection(values.data(), values.size());
    }
    for (uint32_t mask = 0; mask < 65536; ++mask) {
        std::array<uint8_t, 32> flags {};
        for (unsigned bit = 0; bit < 16; ++bit) {
            flags[bit] = (mask >> bit) & 1;
            flags[16 + bit] = ((~mask) >> (15 - bit)) & 1;
        }
        verify_compaction(flags.data(), flags.size());
    }
    for (const auto rows : {1, 7, 8, 9, 31, 63, 64, 65, 127, 128, 129, 4095, 4096, 4097, 65535}) {
        Data<0> data(rows, rows, 50, "random");
        verify(data);
    }
    for (const auto rate : {0, 1, 10, 90, 99, 100}) {
        verify(Data<0>(65535, 65535, rate, "random"));
    }
    for (const auto batch : {7, 64, 65, 256}) {
        for (const auto rate : {0, 1, 10, 50, 90, 99, 100}) {
            for (const auto* pattern : {"random", "clustered"}) {
                verify(Data<1>(1103, batch, rate, pattern));
                verify(Data<8>(1103, batch, rate, pattern));
            }
        }
    }
    std::cerr << "Correctness: all 256 table masks, 65536 mixed masks, tails, row order, "
                 "bounds, and payloads passed\n";
}

template <size_t WORDS>
void benchmark(const Options& options, int rate, const std::string& pattern) {
    Data<WORDS> data(options.rows, options.batch, rate, pattern, options.input == "values");
    verify(data);
    Scratch<WORDS> scratch(options.batch);
    using Runner = uint64_t (*)(const Data<WORDS>&, Scratch<WORDS>&, size_t);
    std::vector<Runner> runners;
    size_t first_algorithm;
    if (options.suite == "doris") {
        first_algorithm = static_cast<size_t>(Algorithm::DORIS_CTZ);
        runners = {run<WORDS, Algorithm::DORIS_CTZ>,      run<WORDS, Algorithm::DORIS_LOOKUP>,
                   run<WORDS, Algorithm::DORIS_HYBRID1>,  run<WORDS, Algorithm::DORIS_HYBRID2>,
                   run<WORDS, Algorithm::DORIS_HYBRID4>,  run<WORDS, Algorithm::DORIS_HYBRID8>,
                   run<WORDS, Algorithm::DORIS_HYBRID16>, run<WORDS, Algorithm::DORIS_PRODUCTION>};
    } else {
        first_algorithm = 0;
        runners = {run<WORDS, Algorithm::BRANCHED>, run<WORDS, Algorithm::BRANCHLESS>,
                   run<WORDS, Algorithm::LOOKUP>, run<WORDS, Algorithm::CTZ>};
        if constexpr (WORDS != 0) {
            runners.push_back(run<WORDS, Algorithm::FUSED_COPY>);
        }
    }
    std::vector<size_t> passes(runners.size(), 1);
    std::vector<std::vector<double>> samples(runners.size());
    const auto target_seconds = options.min_ms / 1000;
    for (size_t algorithm = 0; algorithm < runners.size(); ++algorithm) {
        double seconds;
        do {
            const auto start = Clock::now();
            runners[algorithm](data, scratch, passes[algorithm]);
            seconds = std::chrono::duration<double>(Clock::now() - start).count();
            if (seconds < target_seconds / 4) {
                passes[algorithm] *= 2;
            }
        } while (seconds < target_seconds / 4);
        passes[algorithm] = std::max<size_t>(
                1, static_cast<size_t>(std::ceil(passes[algorithm] * target_seconds / seconds)));
    }
    // Interleave algorithms, changing their order on every repetition.
    std::vector<size_t> order(runners.size());
    std::iota(order.begin(), order.end(), 0);
    std::mt19937 random(0xbec123);
    for (size_t repetition = 0; repetition < options.repeats; ++repetition) {
        std::shuffle(order.begin(), order.end(), random);
        for (auto algorithm : order) {
            const auto start = Clock::now();
            runners[algorithm](data, scratch, passes[algorithm]);
            const auto ns = std::chrono::duration<double, std::nano>(Clock::now() - start).count();
            samples[algorithm].push_back(ns / (passes[algorithm] * options.rows));
        }
    }
    for (auto& sample : samples) {
        std::sort(sample.begin(), sample.end());
    }
    const double baseline = samples[0][options.repeats / 2];
    for (size_t algorithm = 0; algorithm < runners.size(); ++algorithm) {
        const auto& sample = samples[algorithm];
        const double median = sample[options.repeats / 2];
        std::cout << pattern << ',' << options.batch << ',' << options.rows << ',' << rate << ','
                  << (WORDS == 0   ? "select"
                      : WORDS == 1 ? "copy8"
                                   : "copy64")
                  << ',' << NAMES[first_algorithm + algorithm] << ',' << median << ','
                  << baseline / median << ',' << sample.front() << ',' << sample.back() << ','
                  << passes[algorithm] << ','
                  << (options.suite == "scalar" ? "values" : options.input) << '\n';
    }
    std::cout.flush();
}

Options parse_options(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string key = argv[i];
        if (key == "--help") {
            std::cout << "Options: --rows N --batch N --repeats N --min-ms M --rate 0..100\n"
                         "         --pattern random|clustered|all --mode select|copy8|copy64|all\n"
                         "         --suite doris|scalar --input flags|values\n"
                         "         --verify-only\n"
                         "Defaults: 1048576 rows, 4096 batch, 7 repeats, 20 ms/sample, all cases.\n"
                         "Row IDs are uint16; values are int32; copy modes include selection.\n"
                         "Default suite=doris: current SIMD/ctz, lookup, and hybrid candidates.\n"
                         "Default input=flags: predicate evaluation is outside timing.\n"
                         "input=values includes one int32 comparison pass before compaction.\n"
                         "suite=scalar retains the earlier scalar-versus-batch experiment.\n"
                         "clustered places passing values first in every batch.\n";
            std::exit(0);
        }
        if (key == "--verify-only") {
            options.verify_only = true;
            continue;
        }
        if (i + 1 == argc) {
            throw std::invalid_argument("Missing option value: " + key);
        }
        const std::string value = argv[++i];
        if (key == "--rows") {
            options.rows = std::stoull(value);
        } else if (key == "--batch") {
            options.batch = std::stoull(value);
        } else if (key == "--repeats") {
            options.repeats = std::stoull(value);
        } else if (key == "--min-ms") {
            options.min_ms = std::stod(value);
        } else if (key == "--rate") {
            options.rate = std::stoi(value);
        } else if (key == "--pattern") {
            options.pattern = value;
        } else if (key == "--mode") {
            options.mode = value;
        } else if (key == "--suite") {
            options.suite = value;
        } else if (key == "--input") {
            options.input = value;
        } else {
            throw std::invalid_argument("Unknown option: " + key);
        }
    }
    if (options.rows == 0 || options.batch == 0 || options.batch > 65535 || options.repeats < 3 ||
        options.repeats % 2 == 0 || !std::isfinite(options.min_ms) || options.min_ms <= 0 ||
        options.rate < -1 || options.rate > 100 ||
        (options.pattern != "random" && options.pattern != "clustered" &&
         options.pattern != "all") ||
        (options.mode != "select" && options.mode != "copy8" && options.mode != "copy64" &&
         options.mode != "all") ||
        (options.suite != "doris" && options.suite != "scalar") ||
        (options.input != "flags" && options.input != "values")) {
        throw std::invalid_argument("Invalid options; use --help for supported values");
    }
    return options;
}

} // namespace selection_compaction_bench

int main(int argc, char** argv) {
    using namespace selection_compaction_bench;
    try {
        const auto options = parse_options(argc, argv);
        self_test();
        if (options.verify_only) {
            return 0;
        }
        std::cerr << "Compiler: " << __VERSION__ << "; rows=" << options.rows
                  << "; batch=" << options.batch << "; repeats=" << options.repeats
                  << "; min_ms=" << options.min_ms << "; suite=" << options.suite
                  << "; input=" << options.input << '\n';
        std::cout << std::fixed << std::setprecision(6)
                  << "pattern,batch,rows,pass_percent,mode,algorithm,ns_per_input_row,"
                     "speedup_vs_baseline,min_ns_per_row,max_ns_per_row,passes,input\n";
        for (const auto* pattern : {"random", "clustered"}) {
            if (options.pattern != "all" && options.pattern != pattern) {
                continue;
            }
            const auto rates = options.rate == -1 ? std::vector<int> {0, 1, 10, 50, 90, 99, 100}
                                                  : std::vector<int> {options.rate};
            for (auto rate : rates) {
                if (options.mode == "all" || options.mode == "select") {
                    benchmark<0>(options, rate, pattern);
                }
                if (options.mode == "all" || options.mode == "copy8") {
                    benchmark<1>(options, rate, pattern);
                }
                if (options.mode == "all" || options.mode == "copy64") {
                    benchmark<8>(options, rate, pattern);
                }
            }
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
    return 0;
}
