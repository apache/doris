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

#include "io/cache/cache_block_meta_store.h"

#include <butil/logging.h>
#include <fmt/format.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <sstream>

#include "common/status.h"
#include "util/threadpool.h"
#include "vec/common/hex.h"

namespace doris::io {

CacheBlockMetaStore::CacheBlockMetaStore(const std::string& db_path, size_t queue_size)
        : _db_path(db_path), _write_queue(queue_size) {}

CacheBlockMetaStore::~CacheBlockMetaStore() {
    _stop_worker.store(true, std::memory_order_release);
    if (_write_thread.joinable()) {
        _write_thread.join();
    }

    if (_db) {
        _db->Close();
    }
}

Status CacheBlockMetaStore::init() {
    std::filesystem::create_directories(_db_path);

    _options.create_if_missing = true;
    _options.error_if_exists = false;
    _options.compression = rocksdb::kNoCompression;
    _options.max_open_files = 1000;
    _options.write_buffer_size = 64 * 1024 * 1024; // 64MB
    _options.target_file_size_base = 64 * 1024 * 1024;

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.block_size = 16 * 1024;
    _options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    rocksdb::DB* db_ptr = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(_options, _db_path, &db_ptr);
    if (!status.ok()) {
        return Status::InternalError("Failed to open rocksdb: {}", status.ToString());
    }
    _db.reset(db_ptr);

    _write_thread = std::thread(&CacheBlockMetaStore::async_write_worker, this);

    return Status::OK();
}

void CacheBlockMetaStore::put(const BlockMetaKey& key, const BlockMeta& meta) {
    std::string key_str = serialize_key(key);
    std::string value_str = serialize_value(meta);

    // Put write task into queue for asynchronous processing
    WriteOperation op;
    op.type = OperationType::PUT;
    op.key = key_str;
    op.value = value_str;
    _write_queue.enqueue(op);
}

BlockMeta CacheBlockMetaStore::get(const BlockMetaKey& key) {
    std::string key_str = serialize_key(key);
    std::string value_str;

    rocksdb::Status status = _db->Get(rocksdb::ReadOptions(), key_str, &value_str);

    if (status.ok()) {
        return deserialize_value(value_str);
    } else if (status.IsNotFound()) {
        return BlockMeta();
    } else {
        LOG(WARNING) << "Failed to get key from rocksdb: " << status.ToString();
        return BlockMeta();
    }
}

std::unique_ptr<BlockMetaIterator> CacheBlockMetaStore::range_get(int64_t tablet_id) {
    class RocksDBIterator : public BlockMetaIterator {
    public:
        RocksDBIterator(rocksdb::Iterator* iter, const std::string& prefix)
                : _iter(iter), _prefix(prefix) {
            _iter->Seek(_prefix);
        }

        ~RocksDBIterator() override { delete _iter; }

        bool valid() const override { return _iter->Valid() && _iter->key().starts_with(_prefix); }

        void next() override { _iter->Next(); }

        BlockMetaKey key() const override {
            std::string key_str = _iter->key().ToString();
            // Key format: "tabletid_hashstring_offset"
            size_t pos1 = key_str.find('_');
            size_t pos2 = key_str.find('_', pos1 + 1);

            int64_t tablet_id = std::stoll(key_str.substr(0, pos1));
            std::string hash_str = key_str.substr(pos1 + 1, pos2 - pos1 - 1);
            size_t offset = std::stoull(key_str.substr(pos2 + 1));

            // Convert hash string back to UInt128Wrapper
            // Using unhex_uint to parse hex string to uint128_t
            uint128_t hash_value = vectorized::unhex_uint<uint128_t>(hash_str.c_str());

            return BlockMetaKey(tablet_id, UInt128Wrapper(hash_value), offset);
        }

        BlockMeta value() const override {
            std::string value_str = _iter->value().ToString();
            // Assuming value format is "type:size"
            size_t pos = value_str.find(':');
            int type = std::stoi(value_str.substr(0, pos));
            size_t size = std::stoull(value_str.substr(pos + 1));
            return BlockMeta(type, size);
        }

    private:
        rocksdb::Iterator* _iter;
        std::string _prefix;
    };

    std::string prefix = std::to_string(tablet_id) + "_";
    rocksdb::Iterator* iter = _db->NewIterator(rocksdb::ReadOptions());
    return std::make_unique<RocksDBIterator>(iter, prefix);
}

void CacheBlockMetaStore::delete_key(const BlockMetaKey& key) {
    std::string key_str = serialize_key(key);

    // Put delete task into queue for asynchronous processing
    WriteOperation op;
    op.type = OperationType::DELETE;
    op.key = key_str;
    _write_queue.enqueue(op);
}

void CacheBlockMetaStore::async_write_worker() {
    while (!_stop_worker.load(std::memory_order_acquire)) {
        WriteOperation op;

        if (_write_queue.try_dequeue(op)) {
            std::lock_guard<std::mutex> lock(_db_mutex);
            rocksdb::Status status;

            if (op.type == OperationType::PUT) {
                status = _db->Put(rocksdb::WriteOptions(), op.key, op.value);
            } else if (op.type == OperationType::DELETE) {
                status = _db->Delete(rocksdb::WriteOptions(), op.key);
            }

            if (!status.ok()) {
                LOG(WARNING) << "Failed to " << (op.type == OperationType::PUT ? "write" : "delete")
                             << " to rocksdb: " << status.ToString();
            }
        } else {
            // Queue is empty, sleep briefly
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    // Process remaining tasks in the queue
    WriteOperation op;
    while (_write_queue.try_dequeue(op)) {
        std::lock_guard<std::mutex> lock(_db_mutex);
        rocksdb::Status status;

        if (op.type == OperationType::PUT) {
            status = _db->Put(rocksdb::WriteOptions(), op.key, op.value);
        } else if (op.type == OperationType::DELETE) {
            status = _db->Delete(rocksdb::WriteOptions(), op.key);
        }

        if (!status.ok()) {
            LOG(WARNING) << "Failed to " << (op.type == OperationType::PUT ? "write" : "delete")
                         << " to rocksdb: " << status.ToString();
        }
    }
}

std::string CacheBlockMetaStore::serialize_key(const BlockMetaKey& key) const {
    return fmt::format("{}_{}_{}", key.tablet_id, key.hash.to_string(), key.offset);
}

std::string CacheBlockMetaStore::serialize_value(const BlockMeta& meta) const {
    return fmt::format("{}:{}", meta.type, meta.size);
}

BlockMetaKey CacheBlockMetaStore::deserialize_key(const std::string& key_str) const {
    // Key format: "tabletid_hashstring_offset"
    size_t pos1 = key_str.find('_');
    size_t pos2 = key_str.find('_', pos1 + 1);

    int64_t tablet_id = std::stoll(key_str.substr(0, pos1));
    std::string hash_str = key_str.substr(pos1 + 1, pos2 - pos1 - 1);
    size_t offset = std::stoull(key_str.substr(pos2 + 1));

    // Convert hash string back to UInt128Wrapper
    // Using unhex_uint to parse hex string to uint128_t
    uint128_t hash_value = vectorized::unhex_uint<uint128_t>(hash_str.c_str());

    return BlockMetaKey(tablet_id, UInt128Wrapper(hash_value), offset);
}

BlockMeta CacheBlockMetaStore::deserialize_value(const std::string& value_str) const {
    // Value format: "type:size"
    size_t pos = value_str.find(':');
    int type = std::stoi(value_str.substr(0, pos));
    size_t size = std::stoull(value_str.substr(pos + 1));
    return BlockMeta(type, size);
}

} // namespace doris::io