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

#include <bvar/bvar.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "io/cache/async_cache_write_service.h"
#include "io/cache/file_cache_common.h"

namespace doris::io {

/// Metadata for one block whose remote payload is waiting for asynchronous persistence.
/// `buffer_offset` and `buffer_size` describe the file interval represented by `buffer`.
struct InflightWriteBufferEntry {
    AsyncCacheWriteBufferPtr buffer;
    size_t buffer_offset {0};
    size_t buffer_size {0};
    int64_t submit_ts_us {0};
    uint64_t write_epoch {0};

    InflightWriteBufferEntry(AsyncCacheWriteBufferPtr buffer_, size_t offset_, size_t size_,
                             int64_t submit_ts_us_, uint64_t write_epoch_)
            : buffer(std::move(buffer_)),
              buffer_offset(offset_),
              buffer_size(size_),
              submit_ts_us(submit_ts_us_),
              write_epoch(write_epoch_) {}
};

/// Sharded index from (cache key, aligned block offset) to an accepted async-write payload.
/// Readers use it before probing disk cache so concurrent misses can reuse remote bytes in memory.
class InflightWriteBufferIndex {
public:
    /// A batch-lookup result retains the requested offset even when `entry` is null.
    struct LookupResult {
        size_t block_offset {0};
        std::shared_ptr<InflightWriteBufferEntry> entry;
    };

    /// @param shard_count Positive number of independently locked hash shards.
    /// @param metric_prefix Prefix that makes per-cache-disk bvar names unique.
    explicit InflightWriteBufferIndex(size_t shard_count, std::string metric_prefix = {});

    /// Claim `(cache_hash, block_offset)` for `entry` if no entry from the same/newer epoch exists.
    /// An older entry is atomically replaced so invalidation cannot block the current epoch.
    /// @return null when insertion/replacement succeeds; otherwise the existing owner.
    std::shared_ptr<InflightWriteBufferEntry> insert_if_absent(
            const UInt128Wrapper& cache_hash, size_t block_offset,
            std::shared_ptr<InflightWriteBufferEntry> entry);

    /// Find the payload only when its epoch equals `expected_epoch`. An older stale entry is erased;
    /// a newer entry is preserved for readers already operating in that epoch.
    std::shared_ptr<InflightWriteBufferEntry> lookup(const UInt128Wrapper& cache_hash,
                                                     size_t block_offset, uint64_t expected_epoch);

    /// Look up aligned `block_offsets` in input order using the same epoch rule as `lookup`.
    std::vector<LookupResult> lookup_all(const UInt128Wrapper& cache_hash,
                                         const std::vector<size_t>& block_offsets,
                                         uint64_t expected_epoch);

    /// Remove the key only if it still points to `expected`, preventing an old task callback from
    /// deleting a replacement entry.
    /// @return true if this exact owner was removed.
    bool remove_if(const UInt128Wrapper& cache_hash, size_t block_offset,
                   const std::shared_ptr<InflightWriteBufferEntry>& expected);

    /// Visit a retained snapshot of entries without holding shard locks during callbacks.
    void for_each_entry(const std::function<void(const InflightWriteBufferEntry&)>& visitor) const;

    /// Return the number of indexed block payloads.
    size_t size() const { return _size.load(std::memory_order_relaxed); }

    /// Estimate index metadata usage; shared payload bytes are tracked by the write service.
    size_t memory_usage() const;

    /// Record removal of an inserted entry because queue submission hit backpressure.
    void record_backpressure_rollback() { *_rollback_on_backpressure_metric << 1; }

private:
    struct Key {
        UInt128Wrapper cache_hash;
        size_t block_offset {0};

        bool operator==(const Key& other) const {
            return cache_hash == other.cache_hash && block_offset == other.block_offset;
        }
    };

    struct KeyHash {
        size_t operator()(const Key& key) const {
            return doris::io::KeyHash()(key.cache_hash) ^ std::hash<size_t>()(key.block_offset);
        }
    };

    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<Key, std::shared_ptr<InflightWriteBufferEntry>, KeyHash> entries;
    };

    size_t _shard_index(const Key& key) const { return KeyHash()(key) % _shards.size(); }

    std::vector<std::unique_ptr<Shard>> _shards;
    std::atomic<size_t> _size {0};

    std::shared_ptr<bvar::PassiveStatus<size_t>> _size_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _memory_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _lookup_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _hit_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _miss_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _insert_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _insert_existing_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _remove_success_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _remove_failed_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _stale_epoch_miss_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _stale_epoch_replace_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _rollback_on_backpressure_metric;
};

} // namespace doris::io
