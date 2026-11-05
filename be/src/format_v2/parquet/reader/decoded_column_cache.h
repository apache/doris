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
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "format_v2/parquet/reader/column_reader.h"
#include "runtime/memory/lru_cache_policy.h"

namespace doris {
class MemoryTrackedPageWithPageEntity;
namespace io {
struct FileDescription;
struct FileSystemProperties;
} // namespace io
} // namespace doris

namespace doris::format::parquet {

// Entries contain native fixed-width values and their null map, before table casts or deletes.
// A shared byte budget covers filling, resident, and pinned entries, including replaced entries.
class DecodedColumnCache final : public LRUCachePolicy {
public:
    struct Budget {
        std::atomic<size_t> bytes {0};
    };

    struct Entry final : public LRUCacheValueBase {
        Entry(size_t rows, size_t width, bool nullable, size_t charge,
              std::shared_ptr<Budget> budget, std::shared_ptr<MemTrackerLimiter> tracker);
        ~Entry() override;

        void append_from(const IColumn& column, size_t start, size_t count);
        void append_to(IColumn& column, size_t start, size_t count) const;

        size_t filled_rows = 0;
        size_t rows;
        size_t width;
        bool nullable;
        size_t charge;
        std::shared_ptr<Budget> budget;
        std::unique_ptr<MemoryTrackedPageWithPageEntity> page;
    };

    using Handle = std::shared_ptr<const Entry>;
    using ReaderFactory = std::function<Status(std::unique_ptr<ParquetColumnReader>*)>;

    DecodedColumnCache(size_t capacity, size_t max_entry_bytes, size_t max_file_admission_bytes);
    ~DecodedColumnCache() override = default;

    static DecodedColumnCache* instance();
    static Status create_global_cache();
    static void destroy_global_cache();

    // Only immutable file contracts are admitted until strong versions are available for mutable
    // files. The connection properties are hashed, so keys never retain credentials in clear text.
    static std::string file_key(const io::FileSystemProperties& source,
                                const io::FileDescription& file, size_t file_size);
    static std::string column_key(std::string_view file_key, const ParquetColumnSchema& schema,
                                  int row_group, bool strict_mode);

    Handle lookup_column(const std::string& key);
    std::unique_ptr<Entry> start_fill(const std::string& key, const DataTypePtr& type, size_t rows,
                                      size_t* file_admission_bytes);
    void publish(const std::string& key, std::unique_ptr<Entry> entry);
    size_t live_bytes() const { return _budget->bytes.load(std::memory_order_relaxed); }

    // Hits do not construct a native reader. Misses fill only through dense read(); skip/select
    // abandon the fill rather than decoding extra rows merely to populate the cache.
    Status create_reader(const std::string& key, const ParquetColumnSchema& schema, size_t rows,
                         bool allow_fill, ParquetColumnReaderProfile profile,
                         size_t* file_admission_bytes, const ReaderFactory& factory,
                         std::unique_ptr<ParquetColumnReader>* reader, bool* cache_hit);

private:
    bool reserve(size_t charge);

    size_t _capacity_bytes;
    size_t _max_entry_bytes;
    size_t _max_file_admission_bytes;
    std::shared_ptr<Budget> _budget = std::make_shared<Budget>();
    std::mutex _admission_mutex;
    static DecodedColumnCache* _instance;
};

} // namespace doris::format::parquet
