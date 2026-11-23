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

#include <cstdint>
#include <string_view>

namespace doris {

constexpr uint64_t sampling_domain(std::string_view name) {
    uint64_t hash = 14695981039346656037ULL;
    for (char c : name) {
        hash = (hash ^ static_cast<uint8_t>(c)) * 1099511628211ULL;
    }
    return hash;
}

inline uint64_t sampling_mix(uint64_t state, uint64_t word) {
    uint64_t z = (state ^ word) + 0x9e3779b97f4a7c15ULL;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

// Differences of two cumulative counts give the allocation to one segment.
class DependentRounding {
public:
    DependentRounding(double probability, int64_t seed, int64_t tablet_id);
    uint64_t cumulative(uint64_t population) const;

private:
    uint64_t _u;
    uint64_t _mantissa;
    unsigned _shift;
};

} // namespace doris
