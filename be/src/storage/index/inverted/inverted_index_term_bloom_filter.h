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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/index/inverted/inverted_index_parser.h"

namespace lucene::store {
class IndexOutput;
class IndexInput;
} // namespace lucene::store

namespace doris::segment_v2 {

// token-exists Bloom Filter ("tbf") sub-file.
//
// Records whether an analyzed token exists in this segment's term dictionary for a given
// inverted index. The query path uses it to short-circuit *literal* term lookups: when a
// term is proven ABSENT, the result is guaranteed empty (no false negatives), so the
// searcher open / .tii / .tis / .frq / .prx IO can be skipped entirely.
//
// On-disk layout (little-endian, self-describing):
//   [magic char[4]="TBF1"][algo u8][hash u8][reserved u16]
//   [analyzer_sig u64][distinct_terms u64][bf_size u32][bf_bytes...]
// header = 4 + 1 + 1 + 2 + 8 + 8 + 4 = 28 bytes.
// bf_bytes is the raw BloomFilter blob (bf->data() of bf->size() bytes, including the
// trailing null-flag byte). On read it is fed verbatim into BloomFilter::init(buf,size,..).
class InvertedIndexTermBloomFilter {
public:
    static constexpr char MAGIC[4] = {'T', 'B', 'F', '1'};
    static constexpr uint8_t ALGO_BLOCK = 0; // BLOCK_BLOOM_FILTER
    static constexpr uint8_t HASH_MURMUR3 = 0;
    static constexpr size_t HEADER_SIZE = 28;

    enum class Probe { ABSENT, MAYBE };

    // build-side: own a BlockSplitBloomFilter sized for `distinct_terms` tokens.
    static Result<std::unique_ptr<InvertedIndexTermBloomFilter>> create_for_write(
            uint64_t distinct_terms, uint64_t analyzer_sig);

    // read-side: parse + validate the header, deep-copy the blob into a BloomFilter.
    static Result<std::unique_ptr<InvertedIndexTermBloomFilter>> load(
            lucene::store::IndexInput* in);

    // build-side: feed one analyzed token (UTF-8 bytes; empty token allowed for keyword).
    void add_token(const char* data, size_t len);

    // build-side: write [header][bf blob] to the sub-file.
    void serialize(lucene::store::IndexOutput* out) const;

    uint64_t analyzer_sig() const { return _analyzer_sig; }
    uint64_t distinct_terms() const { return _distinct_terms; }

    // approximate in-memory footprint (BF blob bytes), used as the cache charge.
    size_t byte_size() const { return HEADER_SIZE + (_bf != nullptr ? _bf->size() : 0); }

    // read-side: probe one analyzed token. ABSENT is authoritative (no false negative);
    // MAYBE means "fall through to the normal lookup".
    Probe probe(const std::string& utf8_token) const;

private:
    InvertedIndexTermBloomFilter() = default;

    std::unique_ptr<BloomFilter> _bf;
    uint64_t _analyzer_sig = 0;
    uint64_t _distinct_terms = 0;
};

// analyzer configuration fingerprint. Two configs that would tokenize differently must
// produce different signatures, so the read side can reject a BF built by a different
// analyzer (A3) and fall back to a normal lookup.
uint64_t compute_analyzer_sig(const InvertedIndexAnalyzerConfig& cfg);

} // namespace doris::segment_v2
