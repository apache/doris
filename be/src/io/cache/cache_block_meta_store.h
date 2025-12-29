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

#include <concurrentqueue.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "gen_cpp/file_cache.pb.h"
#include "io/cache/file_cache_common.h"
#include "util/threadpool.h"

namespace doris::io {

struct BlockMeta {
    FileCacheType type;
    size_t size;
    uint64_t ttl;

    BlockMeta() : type(DISPOSABLE), size(0), ttl(0) {}
    BlockMeta(FileCacheType type_, size_t size_) : type(type_), size(size_), ttl(0) {}
    BlockMeta(FileCacheType type_, size_t size_, uint64_t ttl_)
            : type(type_), size(size_), ttl(ttl_) {}

    bool operator==(const BlockMeta& other) const {
        return type == other.type && size == other.size && ttl == other.ttl;
    }
};

struct BlockMetaKey {
    int64_t tablet_id;
    UInt128Wrapper hash;
    size_t offset;

    BlockMetaKey() : tablet_id(0), hash(UInt128Wrapper(0)), offset(0) {}
    BlockMetaKey(int64_t tablet_id_, UInt128Wrapper hash_, size_t offset_)
            : tablet_id(tablet_id_), hash(hash_), offset(offset_) {}

    bool operator==(const BlockMetaKey& other) const {
        return tablet_id == other.tablet_id && hash == other.hash && offset == other.offset;
    }

    std::string to_string() const {
        return std::to_string(tablet_id) + "_" + hash.to_string() + "_" + std::to_string(offset);
    }
};

class BlockMetaIterator {
public:
    virtual ~BlockMetaIterator() = default;
    virtual bool valid() const = 0;
    virtual void next() = 0;
    virtual BlockMetaKey key() const = 0;
    virtual BlockMeta value() const = 0;

    // Error status query methods
    virtual Status get_last_key_error() const { return Status::OK(); }
    virtual Status get_last_value_error() const { return Status::OK(); }
};

class CacheBlockMetaStore {
public:
    CacheBlockMetaStore(const std::string& db_path, size_t queue_size = 1000);
    ~CacheBlockMetaStore();

    Status init();

    // Asynchronously write BlockMeta to rocksdb
    void put(const BlockMetaKey& key, const BlockMeta& meta);

    // Synchronously get BlockMeta
    std::optional<BlockMeta> get(const BlockMetaKey& key);

    // Range query all BlockMeta for specified tablet_id
    std::unique_ptr<BlockMetaIterator> range_get(int64_t tablet_id);

    // Get iterator for all BlockMeta records
    std::unique_ptr<BlockMetaIterator> get_all();

    // Asynchronously delete specified BlockMeta
    void delete_key(const BlockMetaKey& key);

    // Clear all records from rocksdb and the async queue
    void clear();

    // Get the approximate size of the write queue
    size_t get_write_queue_size() const;

    // Count entries stored in rocksdb (ignoring pending writes)
    size_t approximate_entry_count() const;

private:
    void async_write_worker();

    std::string _db_path;
    std::unique_ptr<rocksdb::DB> _db;
    rocksdb::Options _options;
    std::unique_ptr<rocksdb::ColumnFamilyHandle> _file_cache_meta_cf_handle;
    std::atomic<bool> _initialized {false};

    enum class OperationType { PUT, DELETE };
    struct WriteOperation {
        OperationType type;
        std::string key;
        std::string value; // Only used for PUT operations
    };
    moodycamel::ConcurrentQueue<WriteOperation> _write_queue;
    std::atomic<bool> _stop_worker {false};
    std::thread _write_thread;
    std::mutex _queue_mutex;

    std::unique_ptr<ThreadPool> _thread_pool;
};

std::string serialize_key(const BlockMetaKey& key);
std::string serialize_value(const BlockMeta& meta);
std::optional<BlockMetaKey> deserialize_key(const std::string& key_str, Status* status = nullptr);
std::optional<BlockMeta> deserialize_value(const std::string& value_str, Status* status = nullptr);
std::optional<BlockMeta> deserialize_value(std::string_view value_view, Status* status = nullptr);

} // namespace doris::io