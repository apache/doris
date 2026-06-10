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

#include "storage/index/inverted/inverted_index_term_bloom_filter.h"

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>

#include <cstring>
#include <map>

#include "storage/index/bloom_filter/block_split_bloom_filter.h"
#include "storage/index/inverted/inverted_index_term_bf_query.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/hash_util.hpp"

namespace doris::segment_v2 {

// false positive probability for the token-exists BF. The term dictionary is small
// relative to the data, so a tight fpp keeps the sub-file tiny while making the
// absent-term fast path reliable.
static constexpr double TERM_BF_FPP = 0.01;

Result<std::unique_ptr<InvertedIndexTermBloomFilter>>
InvertedIndexTermBloomFilter::create_for_write(uint64_t distinct_terms, uint64_t analyzer_sig) {
    std::unique_ptr<BloomFilter> bf;
    RETURN_IF_ERROR_RESULT(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
    RETURN_IF_ERROR_RESULT(bf->init(distinct_terms, TERM_BF_FPP, HASH_MURMUR3_X64_64));

    std::unique_ptr<InvertedIndexTermBloomFilter> self(new InvertedIndexTermBloomFilter());
    self->_bf = std::move(bf);
    self->_analyzer_sig = analyzer_sig;
    self->_distinct_terms = distinct_terms;
    return self;
}

void InvertedIndexTermBloomFilter::add_token(const char* data, size_t len) {
    // An empty keyword token is a legal token (keyword indexes treat the empty string as a
    // value). BloomFilter::add_bytes treats a nullptr buffer as the "has_null" flag, so use a
    // non-null pointer for the zero-length case to keep it on the data path.
    static constexpr char EMPTY = '\0';
    _bf->add_bytes(len == 0 ? &EMPTY : data, len);
}

void InvertedIndexTermBloomFilter::serialize(lucene::store::IndexOutput* out) const {
    faststring header;
    header.append(MAGIC, sizeof(MAGIC));
    header.push_back(ALGO_BLOCK);
    header.push_back(HASH_MURMUR3);
    header.push_back(0); // reserved
    header.push_back(0); // reserved
    put_fixed64_le(&header, _analyzer_sig);
    put_fixed64_le(&header, _distinct_terms);
    put_fixed32_le(&header, static_cast<uint32_t>(_bf->size()));
    DCHECK_EQ(header.size(), HEADER_SIZE);

    out->writeBytes(reinterpret_cast<const uint8_t*>(header.data()),
                    static_cast<int32_t>(header.size()));
    out->writeBytes(reinterpret_cast<const uint8_t*>(_bf->data()),
                    static_cast<int32_t>(_bf->size()));
}

Result<std::unique_ptr<InvertedIndexTermBloomFilter>> InvertedIndexTermBloomFilter::load(
        lucene::store::IndexInput* in) {
    int64_t total = in->length();
    if (total < static_cast<int64_t>(HEADER_SIZE) + 2) {
        return ResultError(
                Status::Corruption("term bloom filter sub-file too small: {} bytes", total));
    }
    // Upper bound BEFORE resize() / the int32 readBytes cast: a legitimate blob is at most
    // BloomFilter::MAXIMUM_BYTES (+1 null-flag byte), so a larger length is corrupt. This bounds
    // the buffer allocation against a crafted length and keeps total well within int32 range, so
    // the static_cast<int32_t>(total) below cannot overflow.
    static constexpr int64_t kMaxTotal =
            static_cast<int64_t>(HEADER_SIZE) + BloomFilter::MAXIMUM_BYTES + 1;
    if (total > kMaxTotal) {
        return ResultError(Status::Corruption(
                "term bloom filter sub-file too large: {} bytes (max {})", total, kMaxTotal));
    }

    faststring buf;
    buf.resize(total);
    in->readBytes(buf.data(), static_cast<int32_t>(total));

    const uint8_t* p = buf.data();
    if (std::memcmp(p, MAGIC, sizeof(MAGIC)) != 0) {
        return ResultError(Status::Corruption("term bloom filter bad magic"));
    }
    uint8_t algo = p[4];
    uint8_t hash = p[5];
    if (algo != ALGO_BLOCK || hash != HASH_MURMUR3) {
        return ResultError(
                Status::Corruption("term bloom filter unsupported algo/hash: {}/{}", algo, hash));
    }
    uint64_t analyzer_sig = decode_fixed64_le(p + 8);
    uint64_t distinct_terms = decode_fixed64_le(p + 16);
    uint32_t bf_size = decode_fixed32_le(p + 24);
    if (static_cast<int64_t>(HEADER_SIZE) + bf_size != total) {
        return ResultError(
                Status::Corruption("term bloom filter size mismatch: header={} bf={} total={}",
                                   HEADER_SIZE, bf_size, total));
    }

    std::unique_ptr<BloomFilter> bf;
    RETURN_IF_ERROR_RESULT(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
    RETURN_IF_ERROR_RESULT(
            bf->init(reinterpret_cast<const char*>(p + HEADER_SIZE), bf_size, HASH_MURMUR3_X64_64));

    std::unique_ptr<InvertedIndexTermBloomFilter> self(new InvertedIndexTermBloomFilter());
    self->_bf = std::move(bf);
    self->_analyzer_sig = analyzer_sig;
    self->_distinct_terms = distinct_terms;
    return self;
}

InvertedIndexTermBloomFilter::Probe InvertedIndexTermBloomFilter::probe(
        const std::string& utf8_token) const {
    // Mirror add_token's empty-token handling so the zero-length keyword token probes the
    // same hash slot it was added under.
    static constexpr char EMPTY = '\0';
    const char* data = utf8_token.empty() ? &EMPTY : utf8_token.data();
    bool maybe = _bf->test_bytes(data, utf8_token.size());
    return maybe ? Probe::MAYBE : Probe::ABSENT;
}

uint64_t compute_analyzer_sig(const InvertedIndexAnalyzerConfig& cfg) {
    // Fold every field that can change tokenization into one buffer, then hash it. A '\0'
    // separator between fields keeps distinct field boundaries from aliasing.
    faststring buf;
    auto add = [&buf](const std::string& s) {
        buf.append(s.data(), s.size());
        buf.push_back('\0');
    };
    add(cfg.analyzer_name);
    add(std::to_string(static_cast<int>(cfg.parser_type)));
    add(cfg.parser_mode);
    add(cfg.lower_case);
    add(cfg.stop_words);
    // char_filter_map is an ordered std::map, so iteration order is deterministic.
    for (const auto& [k, v] : cfg.char_filter_map) {
        add(k);
        add(v);
    }
    return HashUtil::murmur_hash64A(buf.data(), static_cast<int>(buf.size()),
                                    /*seed=*/0x9747b28c);
}

bool bf_query_proven_empty(InvertedIndexQueryType query_type,
                           const InvertedIndexQueryInfo& query_info,
                           const InvertedIndexTermBloomFilter& bf) {
    using Probe = InvertedIndexTermBloomFilter::Probe;
    // A2: a multi-term slot (CJK overlap / synonyms at one position) is an OR of its sub-terms --
    // ABSENT only when *every* sub-term is ABSENT; never call get_single_term() on a multi slot.
    // `auto` keeps CLucene's lucene::index::TermInfo out of name lookup here.
    auto probe = [&bf](const auto& ti) -> Probe {
        if (ti.is_multi_terms()) {
            for (const auto& sub : ti.get_multi_terms()) {
                if (bf.probe(sub) == Probe::MAYBE) {
                    return Probe::MAYBE;
                }
            }
            return Probe::ABSENT;
        }
        return bf.probe(ti.get_single_term());
    };

    switch (query_type) {
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::EQUAL_QUERY: {
        // AND: any single ABSENT token proves the result empty.
        for (const auto& ti : query_info.term_infos) {
            if (probe(ti) == Probe::ABSENT) {
                return true;
            }
        }
        return false;
    }
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
        // A1: alternatives sharing one position are an OR slot. A slot is dead only when *every*
        // alternative at that position is ABSENT; the phrase is empty when *any* slot is dead.
        std::map<int32_t, bool> slot_alive; // position -> at least one alternative MAYBE present
        for (const auto& ti : query_info.term_infos) {
            bool maybe = probe(ti) == Probe::MAYBE;
            auto it = slot_alive.find(ti.position);
            if (it == slot_alive.end()) {
                slot_alive.emplace(ti.position, maybe);
            } else {
                it->second = it->second || maybe;
            }
        }
        for (const auto& [pos, alive] : slot_alive) {
            if (!alive) {
                return true;
            }
        }
        return false;
    }
    case InvertedIndexQueryType::MATCH_ANY_QUERY: {
        // OR: empty only when *every* token is ABSENT. (No pruned-subset rewrite in v1.)
        for (const auto& ti : query_info.term_infos) {
            if (probe(ti) == Probe::MAYBE) {
                return false;
            }
        }
        return true;
    }
    default:
        // Not eligible for the absent-term fast path.
        return false;
    }
}

} // namespace doris::segment_v2
