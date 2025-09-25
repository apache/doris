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

#include <stddef.h>
#include <stdint.h>

#include <vector>

#include "common/status.h"
#include "olap/rowset/segment_v2/bloom_filter.h"

namespace doris {
static constexpr uint64_t SEED_GEN = 217728422;

namespace segment_v2 {
#include "common/compile_check_begin.h"
enum HashStrategyPB : int;

class NGramBloomFilter : public BloomFilter {
public:
    // Fixed hash function number
    static const size_t HASH_FUNCTIONS = 2;
    using UnderType = uint64_t;
    NGramBloomFilter(size_t size);
    void add_bytes(const char* data, size_t len) override;
    bool contains(const BloomFilter& bf_) const override;
    Status init(const char* buf, size_t size, HashStrategyPB strategy) override;
    char* data() override { return reinterpret_cast<char*>(filter.data()); }
    size_t size() const override { return _size; }
    void add_hash(uint64_t) override {}
    bool test_hash(uint64_t hash) const override { return true; }
    bool has_null() const override { return true; }
    bool is_ngram_bf() const override { return true; }

private:
// FIXME: non-static data member '_size' of 'NGramBloomFilter' shadows member inherited from type 'BloomFilter'
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
    size_t _size = 0;
#ifdef __clang__
#pragma clang diagnostic pop
#endif
    size_t words = 0;
    std::vector<uint64_t> filter;
};

} // namespace segment_v2
} // namespace doris
#include "common/compile_check_end.h"