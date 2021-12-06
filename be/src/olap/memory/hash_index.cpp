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

#include "olap/memory/hash_index.h"

#include <stdio.h>
#ifdef __SSE2__
#include <emmintrin.h>
#endif

#include <algorithm>
#include <vector>

#include "gutil/stringprintf.h"

namespace doris {
namespace memory {

struct alignas(64) HashChunk {
    static const uint32_t CAPACITY = 12;
    uint8_t tags[12];
    std::atomic<uint32_t> size;
    uint32_t values[12];

    const std::string debug_string() const {
        std::string ret;
        StringPrintf("[");
        for (uint32_t i = 0; i < std::min((uint32_t)size, (uint32_t)12); i++) {
            printf("%6u(%02x)", values[i], (uint32_t)tags[i]);
        }
        printf("]\n");
        return ret;
    }
};

const uint64_t HashIndex::npos;

HashIndex::HashIndex(size_t capacity)
        : _size(0), _max_size(0), _num_chunks(0), _chunk_mask(0), _chunks(nullptr) {
    size_t min_chunk = (capacity * 14 / 12 + HashChunk::CAPACITY - 1) / HashChunk::CAPACITY;
    if (min_chunk == 0) {
        return;
    }
    size_t nc = 1;
    while (nc < min_chunk) {
        nc *= 2;
    }
    _chunks = reinterpret_cast<HashChunk*>(aligned_malloc(nc * 64, 64));
    if (_chunks) {
        _num_chunks = nc;
        _chunk_mask = nc - 1;
        memset(static_cast<void*>(_chunks), 0, _num_chunks * 64);
        _max_size = _num_chunks * HashChunk::CAPACITY * 12 / 14;
    }
}

HashIndex::~HashIndex() {
    if (_chunks) {
        free(_chunks);
        _chunks = 0;
        _size = 0;
        _max_size = 0;
        _num_chunks = 0;
        _chunk_mask = 0;
    }
}

uint64_t HashIndex::find(uint64_t key_hash, std::vector<uint32_t>* entries) const {
    uint64_t tag = std::max((uint64_t)1, key_hash & 0xff);
    uint64_t pos = (key_hash >> 8) & _chunk_mask;
    uint64_t orig_pos = pos;
#ifdef __SSE2__
    auto tests = _mm_set1_epi8(static_cast<uint8_t>(tag));
    while (true) {
        // get corresponding chunk
        HashChunk& chunk = _chunks[pos];
        uint32_t sz = chunk.size;
        // load tags
        auto tags = _mm_load_si128(reinterpret_cast<__m128i*>(chunk.tags));
        auto eqs = _mm_cmpeq_epi8(tags, tests);
        // check tag equality and store equal tag positions into masks
        uint32_t mask = _mm_movemask_epi8(eqs) & 0xfff;
        // iterator over mask and put candidates into entries
        while (mask != 0) {
            uint32_t i = __builtin_ctz(mask);
            mask &= (mask - 1);
            entries->emplace_back(chunk.values[i]);
        }
#else
    // TODO: use NEON on arm platform
    while (true) {
        HashChunk& chunk = _chunks[pos];
        uint32_t sz = chunk.size;
        for (uint32_t i = 0; i < sz; i++) {
            if (chunk.tags[i] == (uint8_t)tag) {
                entries->emplace_back(chunk.values[i]);
            }
        }
#endif
        if (sz == HashChunk::CAPACITY) {
            // this chunk is full, so there may be more candidates in other chunks
            uint64_t step = tag * 2 + 1;
            pos = (pos + step) & _chunk_mask;
            if (pos == orig_pos) {
                return npos;
            }
        } else {
            // return new entry position, so if key is not found, this entry position
            // can be used to insert new key directly
            return (pos << 4) | sz;
        }
    }
}

void HashIndex::set(uint64_t entry_pos, uint64_t key_hash, uint32_t value) {
    uint64_t pos = entry_pos >> 4;
    uint64_t tpos = entry_pos & 0xf;
    HashChunk& chunk = _chunks[pos];
    uint64_t tag = std::max((uint64_t)1, key_hash & 0xff);
    chunk.tags[tpos] = tag;
    chunk.values[tpos] = value;
    if (tpos == chunk.size) {
        chunk.size++;
        _size++;
    }
}

bool HashIndex::add(uint64_t key_hash, uint32_t value) {
    uint64_t tag = std::max((uint64_t)1, key_hash & 0xff);
    uint64_t pos = (key_hash >> 8) & _chunk_mask;
    uint64_t orig_pos = pos;
    while (true) {
        HashChunk& chunk = _chunks[pos];
        if (chunk.size == HashChunk::CAPACITY) {
            uint64_t step = tag * 2 + 1;
            pos = (pos + step) & _chunk_mask;
            if (pos == orig_pos) {
                return false;
            }
        } else {
            chunk.tags[chunk.size] = tag;
            chunk.values[chunk.size] = value;
            chunk.size++;
            _size++;
            return true;
        }
    }
}

const std::string HashIndex::dump() const {
    return StringPrintf("chunk: %zu %.1fM capacity: %zu/%zu slot util: %.3f", _num_chunks,
                        _num_chunks * 64.0f / (1024 * 1024), size(), max_size(),
                        size() / (_num_chunks * 12.0f));
}

} // namespace memory
} // namespace doris
