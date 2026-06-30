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
#include <functional>
#include <list>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "snii/format/dict_block.h"

// DictBlockCache -- a REQUEST-SCOPED (per-query) MRU cache of decoded DICT
// blocks, keyed by block ordinal.
//
// Why request-scoped (and not a reader-level shared cache): the same DICT block
// is decoded once per LogicalIndexReader::lookup() today, so a multi-term query
// (phrase / boolean / conjunction) whose terms fall in the same block re-runs
// the zstd decompress + CRC verify + anchor parse for every term. Threading one
// of these caches through a single query's lookups collapses that to a single
// decode per unique block.
//
// CONCURRENCY: this object carries NO shared mutable state and is intentionally
// NOT thread-safe. It is meant to live on one query's stack/context and be used
// by a single thread; concurrent queries each own a separate cache. The shared
// LogicalIndexReader therefore stays const and lock-free -- no lock is ever held
// across a decode/IO. (The cross-query, lock-striped variant that would let
// queries share decoded blocks is deferred to the T26 concurrency work.)
namespace snii::reader {

// A decoded DICT block with stable backing storage. Heap-allocated and owned by
// a shared_ptr so the embedded DictBlockReader's Slice into `bytes` stays valid
// for the whole lifetime of any pin handed to a caller -- even after the block
// has been evicted from the cache.
struct DecodedDictBlock {
    std::vector<uint8_t> bytes;           // decompressed (or raw) block bytes
    snii::format::DictBlockReader reader; // its Slice points into `bytes`
};

class DictBlockCache {
public:
    // Loads (decodes) the block for an ordinal into a freshly heap-allocated
    // DecodedDictBlock. Always invoked OUTSIDE any cache bookkeeping; it performs
    // the file read + optional zstd decompress + CRC/anchor parse.
    using Loader = std::function<doris::Status(std::shared_ptr<const DecodedDictBlock>*)>;

    // A small fixed bound is enough for a single query: it only needs to keep the
    // handful of distinct blocks touched while resolving one query's terms.
    static constexpr size_t kDefaultMaxEntries = 8;

    DictBlockCache() = default;
    explicit DictBlockCache(size_t max_entries)
            : max_entries_(max_entries == 0 ? 1 : max_entries) {}

    // Returns the decoded block for `ordinal`, invoking `loader` only on a miss.
    // The returned pin keeps the block alive for the caller's use regardless of
    // any later eviction. On a hit, `loader` is not called (no re-decode).
    doris::Status get_or_load(uint32_t ordinal, const Loader& loader,
                              std::shared_ptr<const DecodedDictBlock>* out) {
        if (auto it = index_.find(ordinal); it != index_.end()) {
            order_.splice(order_.begin(), order_, it->second); // promote to MRU
            *out = it->second->block;
            return doris::Status::OK();
        }

        std::shared_ptr<const DecodedDictBlock> loaded;
        // decode happens here, never under a lock (explicit Status, header-safe:
        // RETURN_IF_ERROR would need a bare `Status` in scope).
        if (doris::Status st = loader(&loaded); !st.ok()) {
            return st;
        }
        order_.push_front(Entry {.ordinal = ordinal, .block = loaded});
        index_[ordinal] = order_.begin();
        evict_overflow();
        *out = std::move(loaded);
        return doris::Status::OK();
    }

    // Number of resident (non-evicted) entries -- bounded by max_entries().
    size_t size() const { return index_.size(); }
    size_t max_entries() const { return max_entries_; }

private:
    struct Entry {
        uint32_t ordinal = 0;
        std::shared_ptr<const DecodedDictBlock> block;
    };

    // Drops least-recently-used entries until the bound holds. Evicting only
    // releases the cache's reference; any pin a caller still holds keeps the
    // block (and its reader's Slice) alive.
    void evict_overflow() {
        while (index_.size() > max_entries_) {
            const Entry& victim = order_.back();
            index_.erase(victim.ordinal);
            order_.pop_back();
        }
    }

    size_t max_entries_ = kDefaultMaxEntries;
    std::list<Entry> order_; // front = most recently used
    std::unordered_map<uint32_t, std::list<Entry>::iterator> index_;
};

} // namespace snii::reader
