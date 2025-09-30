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
#include <optional>

namespace doris::segment_v2::inverted_index {

class TinySet {
public:
    explicit TinySet(uint64_t value) : _bits(value) {}
    ~TinySet() = default;

    bool is_empty() const { return _bits == 0; }

    void clear() { _bits = 0; }

    bool insert_mut(uint32_t el) {
        if (el >= 64) {
            return false;
        }
        uint64_t old_bits = _bits;
        _bits |= (1ULL << el);
        return old_bits != _bits;
    }

    std::optional<uint32_t> pop_lowest() {
        if (is_empty()) {
            return std::nullopt;
        }
        uint32_t lowest = std::countr_zero(_bits);
        _bits ^= (1ULL << lowest);
        return lowest;
    }

    uint32_t len() const { return std::popcount(_bits); }

private:
    uint64_t _bits = 0;
};
using TinySetPtr = std::shared_ptr<TinySet>;

} // namespace doris::segment_v2::inverted_index