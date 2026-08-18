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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

struct ConditionCacheSplitContext;

// Opaque immutable metadata shared by all physical splits of one file. Concrete file formats own
// the derived type so the scanner and split-source layers do not depend on format internals.
class FileContext {
public:
    virtual ~FileContext() = default;
};

class FileContextRegistry {
public:
    using Loader = std::function<Status(std::shared_ptr<const FileContext>*)>;

    struct LookupResult {
        bool loaded = false;
        bool waited = false;
        bool hit = false;
    };

    Status get_or_create(const std::string& key, const Loader& loader,
                         std::shared_ptr<const FileContext>* context,
                         LookupResult* lookup_result = nullptr);

private:
    struct Entry {
        std::mutex lock;
        std::condition_variable ready;
        bool loading = true;
        Status status;
        std::weak_ptr<const FileContext> context;
    };

    std::mutex _lock;
    std::unordered_map<std::string, std::shared_ptr<Entry>> _entries;
};

// Tracks completion of one FE source range after BE refinement. Generated children share this
// object, so the FE range is reported finished only when its final physical child completes.
class SourceSplitProgress {
public:
    void reset_for_children(size_t children) {
        DORIS_CHECK(children > 0);
        _remaining.store(children, std::memory_order_release);
    }

    bool complete_one() {
        const size_t previous = _remaining.fetch_sub(1, std::memory_order_acq_rel);
        DORIS_CHECK(previous > 0);
        return previous == 1;
    }

    std::optional<double> adaptive_batch_bytes_per_row() const {
        std::lock_guard lock(_adaptive_batch_lock);
        return _adaptive_batch_bytes_per_row;
    }

    bool update_adaptive_batch_bytes_per_row(double bytes_per_row) {
        DORIS_CHECK(bytes_per_row > 0);
        std::lock_guard lock(_adaptive_batch_lock);
        // Sibling scanners can finish probes concurrently. Merge their samples under the source
        // lock so later row-group children never regress to a fresh small probe.
        const bool first_source_sample = !_adaptive_batch_bytes_per_row.has_value();
        _adaptive_batch_bytes_per_row =
                _adaptive_batch_bytes_per_row.has_value()
                        ? 0.9 * *_adaptive_batch_bytes_per_row + 0.1 * bytes_per_row
                        : bytes_per_row;
        return first_source_sample;
    }

private:
    std::atomic<size_t> _remaining {1};
    mutable std::mutex _adaptive_batch_lock;
    std::optional<double> _adaptive_batch_bytes_per_row;
};

// BE-local scheduling envelope. It deliberately carries no Thrift fields: FE ranges remain the
// source of table-format semantics, while generated physical children add only opaque metadata and
// format-local subrange ids.
struct FileScanSplit {
    TFileRangeDesc range;
    std::shared_ptr<const TFileRangeDesc> source_range;
    int64_t start_offset = 0;
    int64_t size = -1;
    bool clear_table_level_row_count = false;
    std::shared_ptr<const FileContext> file_context;
    std::shared_ptr<ConditionCacheSplitContext> condition_cache_split_context;
    int64_t format_split_id = -1;
    int64_t format_split_id_end = -1;
    bool is_source_split = false;
    uint64_t source_split_id = 0;
    std::shared_ptr<SourceSplitProgress> source_progress;

    // GLOBAL_ROWID second-phase reads batch by the FE source mapping, while start_offset/size above
    // identify only this first-phase physical child.
    const TFileRangeDesc& source_identity_range() const {
        return source_range == nullptr ? range : *source_range;
    }

    TFileRangeDesc materialize_range() const {
        auto materialized = source_range == nullptr ? range : *source_range;
        if (source_range != nullptr) {
            materialized.__set_start_offset(start_offset);
            materialized.__set_size(size);
        }
        if (clear_table_level_row_count && materialized.__isset.table_format_params) {
            materialized.table_format_params.__isset.table_level_row_count = false;
        }
        return materialized;
    }
};

} // namespace doris
