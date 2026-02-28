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

#include <bit>
#include <cstdint>

namespace doris::segment_v2::inverted_index {

class TinySet {
public:
    TinySet() = default;
    explicit TinySet(uint64_t value) : _bits(value) {}
    ~TinySet() = default;

    TinySet(const TinySet&) = default;
    TinySet& operator=(const TinySet&) = default;
    TinySet(TinySet&&) = default;
    TinySet& operator=(TinySet&&) = default;

    static TinySet empty() { return TinySet(0); }
    static TinySet singleton(uint32_t el) { return TinySet(uint64_t(1) << el); }

    bool is_empty() const { return _bits == 0; }
    uint32_t len() const { return static_cast<uint32_t>(std::popcount(_bits)); }
    void clear() { _bits = 0; }

    bool insert_mut(uint32_t el) {
        uint64_t old = _bits;
        _bits |= (uint64_t(1) << el);
        return old != _bits;
    }

    uint32_t pop_lowest_unchecked() {
        auto lowest = static_cast<uint32_t>(std::countr_zero(_bits));
        _bits ^= (uint64_t(1) << lowest);
        return lowest;
    }

private:
    uint64_t _bits = 0;
};

} // namespace doris::segment_v2::inverted_index