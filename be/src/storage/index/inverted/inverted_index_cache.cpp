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

#include "storage/index/inverted/inverted_index_cache.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <sys/resource.h>

#include <cstring>
#include <span>
// IWYU pragma: no_include <bits/chrono.h>
#include <iostream>
#include <memory>

#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "storage/index/query/logical/node.h"
#include "util/coding.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {

namespace {

void append_length_prefixed(std::string_view value, std::string* output) {
    put_fixed64_le(output, value.size());
    output->append(value);
}

} // namespace

std::string InvertedIndexRawQuerySemantic::encode() const {
    std::string output;
    output.reserve(sizeof(cache_semantics_version) + sizeof(uint64_t) + raw_query_bytes.size() +
                   sizeof(query_type) + sizeof(max_expansions));
    put_fixed32_le(&output, cache_semantics_version);
    append_length_prefixed(raw_query_bytes, &output);
    put_fixed32_le(&output, static_cast<uint32_t>(query_type));
    put_fixed32_le(&output, static_cast<uint32_t>(max_expansions));
    return output;
}

namespace {

void append_terms(std::span<const std::string> terms, std::string* output) {
    put_fixed32_le(output, static_cast<uint32_t>(terms.size()));
    for (const auto& term : terms) {
        append_length_prefixed(term, output);
    }
}

// The fields of a leaf that decide its result, after its kind.
void append_leaf(const index_query::logical::Node& leaf, std::string* output) {
    namespace logical = index_query::logical;
    if (const auto* term = leaf.as<logical::Term>()) {
        append_length_prefixed(term->term, output);
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        output->push_back(static_cast<char>(set->require_all));
        put_fixed32_le(output, set->min_should_match);
        append_terms(set->terms, output);
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        put_fixed32_le(output, static_cast<uint32_t>(phrase->slop));
        output->push_back(static_cast<char>(phrase->ordered));
        output->push_back(static_cast<char>(phrase->prefix));
        output->push_back(static_cast<char>(phrase->suffix));
        put_fixed32_le(output, static_cast<uint32_t>(phrase->slots.size()));
        for (const auto& slot : phrase->slots) {
            put_fixed32_le(output, static_cast<uint32_t>(slot.position));
            if (slot.is_single_term()) {
                append_terms(std::span(&slot.get_single_term(), 1), output);
            } else {
                append_terms(slot.get_multi_terms(), output);
            }
        }
    } else if (const auto* expand = leaf.as<logical::Expand>()) {
        output->push_back(static_cast<char>(expand->kind));
        append_length_prefixed(expand->pattern, output);
    } else if (const auto* compare = leaf.as<logical::Compare>()) {
        output->push_back(static_cast<char>(compare->op));
        append_length_prefixed(compare->value, output);
    } else {
        DCHECK(leaf.as<logical::Bool>() == nullptr);
    }
}

} // namespace

std::string InvertedIndexLeafSemantic::encode() const {
    DCHECK(leaf != nullptr);
    std::string output;
    put_fixed32_le(&output, cache_semantics_version);
    put_fixed32_le(&output, static_cast<uint32_t>(leaf->value.index()));
    append_leaf(*leaf, &output);
    put_fixed32_le(&output, static_cast<uint32_t>(max_expansions));
    return output;
}

std::string InvertedIndexQueryCache::CacheKey::encode() const {
    if (query_type_to_string(query_type).empty()) {
        return {};
    }
    std::string output;
    output.reserve(3 * sizeof(uint64_t) + index_path.size() + column_name.size() +
                   sizeof(query_type) + value.size());
    append_length_prefixed(index_path, &output);
    append_length_prefixed(column_name, &output);
    put_fixed32_le(&output, static_cast<uint32_t>(query_type));
    append_length_prefixed(value, &output);
    return output;
}

InvertedIndexSearcherCache* InvertedIndexSearcherCache::create_global_instance(
        size_t capacity, uint32_t num_shards) {
    return new InvertedIndexSearcherCache(capacity, num_shards);
}

InvertedIndexSearcherCache::InvertedIndexSearcherCache(size_t capacity, uint32_t num_shards) {
    uint64_t fd_number = config::min_file_descriptor_number;
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE, &l);
    if (ret != 0) {
        LOG(WARNING) << "call getrlimit() failed. errno=" << strerror(errno)
                     << ", use default configuration instead.";
    } else {
        fd_number = static_cast<uint64_t>(l.rlim_cur);
    }

    static constexpr size_t fd_bound = 100000;
    size_t search_limit_percent = config::inverted_index_fd_number_limit_percent;
    if (fd_number <= fd_bound) {
        search_limit_percent = size_t(search_limit_percent * 0.25); // default 10%
    } else if (fd_number > fd_bound && fd_number < fd_bound * 5) {
        search_limit_percent = size_t(search_limit_percent * 0.5); // default 20%
    }

    uint64_t open_searcher_limit = fd_number * search_limit_percent / 100;
    LOG(INFO) << "fd_number: " << fd_number
              << ", inverted index open searcher limit: " << open_searcher_limit;
#ifdef BE_TEST
    open_searcher_limit = 2;
#endif

    if (config::enable_inverted_index_cache_check_timestamp) {
        auto get_last_visit_time = [](const void* value) -> int64_t {
            auto* cache_value = (InvertedIndexSearcherCache::CacheValue*)value;
            return cache_value->last_visit_time;
        };
        _policy = std::make_unique<InvertedIndexSearcherCachePolicy>(
                capacity, num_shards, open_searcher_limit, get_last_visit_time, true);
    } else {
        _policy = std::make_unique<InvertedIndexSearcherCachePolicy>(capacity, num_shards,
                                                                     open_searcher_limit);
    }
}

Status InvertedIndexSearcherCache::erase(const std::string& index_file_path) {
    InvertedIndexSearcherCache::CacheKey cache_key(index_file_path);
    _policy->erase(cache_key.index_file_path);
    return Status::OK();
}

int64_t InvertedIndexSearcherCache::mem_consumption() {
    return _policy->mem_consumption();
}

bool InvertedIndexSearcherCache::lookup(const InvertedIndexSearcherCache::CacheKey& key,
                                        InvertedIndexCacheHandle* handle) {
    auto* lru_handle = _policy->lookup(key.index_file_path);
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = InvertedIndexCacheHandle(_policy.get(), lru_handle);
    return true;
}

void InvertedIndexSearcherCache::insert(const InvertedIndexSearcherCache::CacheKey& cache_key,
                                        CacheValue* cache_value) {
    auto* lru_handle = _insert(cache_key, cache_value);
    release(lru_handle);
}

void InvertedIndexSearcherCache::insert(const InvertedIndexSearcherCache::CacheKey& cache_key,
                                        CacheValue* cache_value, InvertedIndexCacheHandle* handle) {
    auto* lru_handle = _insert(cache_key, cache_value);
    *handle = InvertedIndexCacheHandle(_policy.get(), lru_handle);
}

Cache::Handle* InvertedIndexSearcherCache::_insert(const InvertedIndexSearcherCache::CacheKey& key,
                                                   CacheValue* value) {
    Cache::Handle* lru_handle = _policy->insert(key.index_file_path, value, value->size,
                                                value->size, CachePriority::NORMAL);
    return lru_handle;
}

bool InvertedIndexQueryCache::lookup(const CacheKey& key, InvertedIndexQueryCacheHandle* handle) {
    const auto encoded = key.encode();
    if (encoded.empty()) {
        return false;
    }
    auto* lru_handle = LRUCachePolicy::lookup(encoded);
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = InvertedIndexQueryCacheHandle(this, lru_handle);
    return true;
}

void InvertedIndexQueryCache::insert(const CacheKey& key, std::shared_ptr<roaring::Roaring> bitmap,
                                     InvertedIndexQueryCacheHandle* handle) {
    const auto encoded = key.encode();
    if (encoded.empty()) {
        return;
    }
    std::unique_ptr<InvertedIndexQueryCache::CacheValue> cache_value_ptr =
            std::make_unique<InvertedIndexQueryCache::CacheValue>();
    cache_value_ptr->bitmap = bitmap;
    auto* lru_handle = LRUCachePolicy::insert(encoded, (void*)cache_value_ptr.release(),
                                              bitmap->getSizeInBytes(), bitmap->getSizeInBytes(),
                                              CachePriority::NORMAL);
    *handle = InvertedIndexQueryCacheHandle(this, lru_handle);
}

} // namespace doris::segment_v2
