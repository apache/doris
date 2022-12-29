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

#include "olap/rowset/segment_v2/ngram_bloom_filter.h"

#include "util/cityhash102/city.h"
#include "util/debug_util.h"

namespace doris {
namespace segment_v2 {

static constexpr uint64_t SEED_GEN = 217728422;

NGramBloomFilter::NGramBloomFilter(size_t size)
        : _size(size),
          words((size + sizeof(UnderType) - 1) / sizeof(UnderType)),
          filter(words, 0) {}

// for read
Status NGramBloomFilter::init(const char* buf, uint32_t size, HashStrategyPB strategy) {
    if (size == 0) {
        return Status::InvalidArgument(strings::Substitute("invalid size:$0", size));
    }
    DCHECK(_size == size);

    if (strategy != CITY_HASH_64) {
        return Status::InvalidArgument(strings::Substitute("invalid strategy:$0", strategy));
    }
    words = (_size + sizeof(UnderType) - 1) / sizeof(UnderType);
    filter.reserve(words);
    const UnderType* from = reinterpret_cast<const UnderType*>(buf);
    for (size_t i = 0; i < words; ++i) {
        filter[i] = from[i];
    }

    return Status::OK();
}

void NGramBloomFilter::add_bytes(const char* data, uint32_t len) {
    size_t hash1 = CityHash_v1_0_2::CityHash64WithSeed(data, len, 0);
    size_t hash2 = CityHash_v1_0_2::CityHash64WithSeed(data, len, SEED_GEN);

    for (size_t i = 0; i < HASH_FUNCTIONS; ++i) {
        size_t pos = (hash1 + i * hash2 + i * i) % (8 * _size);
        filter[pos / (8 * sizeof(UnderType))] |= (1ULL << (pos % (8 * sizeof(UnderType))));
    }
}

bool NGramBloomFilter::contains(const BloomFilter& bf_) const {
    const NGramBloomFilter& bf = static_cast<const NGramBloomFilter&>(bf_);
    for (size_t i = 0; i < words; ++i) {
        if ((filter[i] & bf.filter[i]) != bf.filter[i]) {
            return false;
        }
    }
    return true;
}

} // namespace segment_v2
} // namespace doris
