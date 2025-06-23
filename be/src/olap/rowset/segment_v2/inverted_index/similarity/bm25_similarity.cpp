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

#include "bm25_similarity.h"

#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

using namespace inverted_index;

const int32_t BM25Similarity::MAX_INT32 = std::numeric_limits<int32_t>::max();
const uint32_t BM25Similarity::MAX_INT4 = long_to_int4(static_cast<uint64_t>(MAX_INT32));
const int32_t BM25Similarity::NUM_FREE_VALUES = 255 - static_cast<int>(MAX_INT4);

std::vector<float> BM25Similarity::LENGTH_TABLE = []() {
    std::vector<float> table(256);
    for (int32_t i = 0; i < 256; i++) {
        table[i] = int_to_byte4(i);
    }
    return table;
}();

BM25Similarity::BM25Similarity() : _cache(256) {}

void BM25Similarity::for_one_term(const IndexQueryContextPtr& context,
                                  const std::wstring& field_name, const std::wstring& term) {
    _avgdl = context->collection_statistics->get_or_calculate_avg_dl(field_name);
    _idf = context->collection_statistics->get_or_calculate_idf(field_name, term);
    _weight = _boost * _idf * (_k1 + 1.0F);

    for (int i = 0; i < _cache.size(); i++) {
        _cache[i] = 1.0F / (_k1 * ((1 - _b) + _b * LENGTH_TABLE[i] / _avgdl));
    }
}

float BM25Similarity::score(float freq, int64_t encoded_norm) {
    float norm_inverse = _cache[((uint8_t)encoded_norm) & 0xFF];
    return _weight - _weight / (1.0F + freq * norm_inverse);
}

int32_t BM25Similarity::number_of_leading_zeros(uint64_t value) {
    if (value == 0) {
        return 64;
    }
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_clzll(value);
#else
    int32_t count = 0;
    for (uint64_t mask = 1ULL << 63; mask != 0; mask >>= 1) {
        if (value & mask) break;
        ++count;
    }
    return count;
#endif
}

uint32_t BM25Similarity::long_to_int4(uint64_t i) {
    if (i > std::numeric_limits<uint64_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Only supports positive values");
    }

    int32_t numBits = 64 - number_of_leading_zeros(i);
    if (numBits < 4) {
        return static_cast<uint32_t>(i);
    } else {
        int32_t shift = numBits - 4;
        uint32_t encoded = static_cast<uint32_t>(i >> shift) & 0x07;
        return encoded | ((shift + 1) << 3);
    }
}

uint64_t BM25Similarity::int4_to_long(uint32_t i) {
    uint64_t bits = i & 0x07;
    int32_t shift = (i >> 3) - 1;

    if (shift < 0) {
        return bits;
    } else {
        return (bits | 0x08) << shift;
    }
}

uint8_t BM25Similarity::int_to_byte4(int32_t i) {
    if (i < 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Only supports positive values");
    }

    if (i < NUM_FREE_VALUES) {
        return static_cast<uint8_t>(i);
    } else {
        uint32_t encoded = long_to_int4(i - NUM_FREE_VALUES);
        return static_cast<uint8_t>(NUM_FREE_VALUES + encoded);
    }
}

int32_t BM25Similarity::byte4_to_int(uint8_t b) {
    if (b < NUM_FREE_VALUES) {
        return b;
    } else {
        uint64_t decoded = NUM_FREE_VALUES + int4_to_long(b - NUM_FREE_VALUES);
        return static_cast<int32_t>(decoded);
    }
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2