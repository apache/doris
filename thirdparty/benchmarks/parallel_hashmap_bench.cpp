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

#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace {

using Map = phmap::flat_hash_map<uint64_t, uint64_t>;
using Group = phmap::priv::Group;

uint64_t mix(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

double cpu_seconds() {
    timespec value {};
    if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) != 0) {
        throw std::runtime_error("clock_gettime failed");
    }
    return value.tv_sec + value.tv_nsec * 1e-9;
}

template <typename Work>
void measure(const std::string& name, uint64_t operations, size_t size, size_t capacity,
             double seconds, Work work) {
    uint64_t expected = work();
    uint64_t checksum = 0;
    uint64_t iterations = 1;
    double cpu_elapsed = 0;
    double wall_elapsed = 0;
    for (;;) {
        checksum = 0;
        auto wall_start = std::chrono::steady_clock::now();
        double cpu_start = cpu_seconds();
        for (uint64_t iteration = 0; iteration < iterations; ++iteration) {
            // Force each iteration to read the table again, including under LTO.
            asm volatile("" ::: "memory");
            checksum += work();
            asm volatile("" : "+r"(checksum) : : "memory");
        }
        cpu_elapsed = cpu_seconds() - cpu_start;
        wall_elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - wall_start)
                               .count();
        if (cpu_elapsed >= seconds) {
            break;
        }
        iterations *= 2;
    }
    if (checksum != expected * iterations) {
        throw std::runtime_error("benchmark result changed between iterations");
    }
    auto count = static_cast<double>(operations) * iterations;
    std::cout << std::setprecision(12) << R"({"name":")" << name << R"(","size":)" << size
              << R"(,"capacity":)" << capacity << R"(,"group_width":)" << Group::kWidth
              << R"(,"portable":)"
              << std::is_same_v<Group, phmap::priv::GroupPortableImpl> << R"(,"iterations":)"
              << iterations << R"(,"operations":)" << operations << R"(,"cpu_ns_per_op":)"
              << cpu_elapsed * 1e9 / count << R"(,"wall_ns_per_op":)" << wall_elapsed * 1e9 / count
              << R"(,"checksum_per_iteration":)" << expected << R"(,"checksum":)" << checksum << "}"
              << std::endl;
}

std::vector<uint64_t> make_keys(size_t size, size_t start = 0) {
    std::vector<uint64_t> keys;
    keys.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        keys.push_back(mix(i + start));
    }
    std::mt19937_64 random(81384);
    std::shuffle(keys.begin(), keys.end(), random);
    return keys;
}

void lookup_benchmarks(size_t size, double seconds) {
    auto keys = make_keys(size);
    auto misses = make_keys(size, size);
    Map map;
    map.reserve(size);
    for (auto key : keys) {
        map.emplace(key, key + 1);
    }
    measure("find_hit/" + std::to_string(size), size, size, map.capacity(), seconds, [&] {
        uint64_t checksum = 0;
        for (auto key : keys) {
            auto found = map.find(key);
            checksum += found->second;
        }
        return checksum;
    });
    measure("find_miss/" + std::to_string(size), size, size, map.capacity(), seconds, [&] {
        uint64_t checksum = 0;
        for (auto key : misses) {
            checksum += map.find(key) != map.end();
        }
        return checksum;
    });
}

void mutation_benchmarks(double seconds) {
    constexpr size_t size = 100000;
    auto keys = make_keys(size);
    measure("insert_reserved/100000", size, size, 0, seconds, [&] {
        Map map;
        map.reserve(size);
        for (auto key : keys) {
            map.emplace(key, key + 1);
        }
        asm volatile("" : : "g"(&map) : "memory");
        return map.size();
    });
    Map map;
    map.reserve(size);
    for (auto key : keys) {
        map.emplace(key, key + 1);
    }
    constexpr size_t changes = 10000;
    measure("erase_insert/10000", changes * 2, size, map.capacity(), seconds, [&] {
        for (size_t i = 0; i < changes; ++i) {
            map.erase(keys[i]);
        }
        for (size_t i = 0; i < changes; ++i) {
            map.emplace(keys[i], keys[i] + 1);
        }
        return map.size();
    });
}

void adjacent_h2_benchmark(double seconds) {
    struct Probe {
        std::array<phmap::priv::ctrl_t, Group::kWidth> ctrl;
        std::array<uint64_t, Group::kWidth> keys;
        uint64_t needle;
    };
    std::vector<Probe> probes(32768);
    for (size_t i = 0; i < probes.size(); ++i) {
        auto& probe = probes[i];
        probe.ctrl.fill(phmap::priv::kEmpty);
        probe.ctrl[2] = 0x12;
        probe.ctrl[3] = 0x13;
        for (size_t j = 0; j < probe.keys.size(); ++j) {
            probe.keys[j] = mix(i * Group::kWidth + j);
        }
        probe.needle = probe.keys[2];
    }
    measure("group_adjacent_h2/32768", probes.size(), probes.size(), 0, seconds, [&] {
        uint64_t checksum = 0;
        for (const auto& probe : probes) {
            for (auto position : Group(probe.ctrl.data()).Match(0x12)) {
                checksum += probe.keys[position] == probe.needle;
            }
        }
        return checksum;
    });
}

} // namespace

int main(int argc, char** argv) {
    double seconds = argc == 2 ? std::stod(argv[1]) : 0.2;
    if (seconds <= 0 || seconds > 10) {
        std::cerr << "minimum CPU seconds must be in (0, 10]\n";
        return 1;
    }
    for (size_t size : {size_t {4096}, size_t {100000}, size_t {114000}, size_t {1000000}}) {
        lookup_benchmarks(size, seconds);
    }
    mutation_benchmarks(seconds);
    adjacent_h2_benchmark(seconds);
    return 0;
}
