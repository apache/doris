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

#include "io/cache/file_cache_common.h"
#include "util/threadpool.h"

namespace doris::io {

struct BlockMeta {
    int type;
    size_t size;

    BlockMeta() : type(0), size(0) {}
    BlockMeta(int type_, size_t size_) : type(type_), size(size_) {}

    bool operator==(const BlockMeta& other) const {
        return type == other.type && size == other.size;
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
};

class CacheBlockMetaStore {
public:
    CacheBlockMetaStore(const std::string& db_path, size_t queue_size = 1000);
    ~CacheBlockMetaStore();

    Status init();

    // Asynchronously write BlockMeta to rocksdb
    void put(const BlockMetaKey& key, const BlockMeta& meta);

    // Synchronously get BlockMeta
    BlockMeta get(const BlockMetaKey& key);

    // Range query all BlockMeta for specified tablet_id
    std::unique_ptr<BlockMetaIterator> range_get(int64_t tablet_id);

    // Asynchronously delete specified BlockMeta
    void delete_key(const BlockMetaKey& key);

    // Clear all records from rocksdb and the async queue
    void clear();

private:
    void async_write_worker();
    std::string serialize_key(const BlockMetaKey& key) const;
    std::string serialize_value(const BlockMeta& meta) const;
    BlockMetaKey deserialize_key(const std::string& key_str) const;
    BlockMeta deserialize_value(const std::string& value_str) const;

    std::string _db_path;
    std::unique_ptr<rocksdb::DB> _db;
    rocksdb::Options _options;

    enum class OperationType { PUT, DELETE };
    struct WriteOperation {
        OperationType type;
        std::string key;
        std::string value; // Only used for PUT operations
    };
    moodycamel::ConcurrentQueue<WriteOperation> _write_queue;
    std::atomic<bool> _stop_worker {false};
    std::thread _write_thread;
    std::mutex _db_mutex;

    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace doris::io