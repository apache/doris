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
#include <bvar/bvar.h>
#include <fmt/format.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <optional>
#include <sstream>

#include "common/status.h"
#include "util/threadpool.h"
#include "vec/common/hex.h"

namespace doris::io {

const std::string FILE_CACHE_META_COLUMN_FAMILY = "file_cache_meta";

// bvar metrics for rocksdb operation failures
bvar::Adder<uint64_t> g_rocksdb_write_failed_num("file_cache_meta_rocksdb_write_failed_num");
bvar::Adder<uint64_t> g_rocksdb_delete_failed_num("file_cache_meta_rocksdb_delete_failed_num");

CacheBlockMetaStore::CacheBlockMetaStore(const std::string& db_path, size_t queue_size)
        : _db_path(db_path), _write_queue(queue_size) {
    auto status = init();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to initialize CacheBlockMetaStore: " << status.to_string();
    }
}

CacheBlockMetaStore::~CacheBlockMetaStore() {
    _stop_worker.store(true, std::memory_order_release);
    if (_write_thread.joinable()) {
        _write_thread.join();
    }

    if (_db) {
        if (_file_cache_meta_cf_handle) {
            _db->DestroyColumnFamilyHandle(_file_cache_meta_cf_handle.release());
        }
        _db->Close();
    }
}

size_t CacheBlockMetaStore::get_write_queue_size() const {
    return _write_queue.size_approx();
}

Status CacheBlockMetaStore::init() {
    std::filesystem::create_directories(_db_path);

    _options.create_if_missing = true;
    _options.create_missing_column_families = true;
    _options.error_if_exists = false;
    _options.compression = rocksdb::kNoCompression;
    _options.max_open_files = 1000;
    _options.write_buffer_size = 64 * 1024 * 1024; // 64MB
    _options.target_file_size_base = 64 * 1024 * 1024;

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.block_size = 16 * 1024;
    _options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // Create column family descriptors
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    // Default column family is required
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
    // File cache meta column family
    column_families.emplace_back(FILE_CACHE_META_COLUMN_FAMILY, rocksdb::ColumnFamilyOptions());

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* db_ptr = nullptr;
    rocksdb::Status status =
            rocksdb::DB::Open(_options, _db_path, column_families, &handles, &db_ptr);

    if (!status.ok()) {
        LOG(WARNING) << "Failed to open rocksdb: " << status.ToString()
                     << "Database path: " << _db_path;
        return Status::InternalError("Failed to open rocksdb: {}", status.ToString());
    }
    _db.reset(db_ptr);

    // Store the file_cache_meta column family handle
    // handles[0] is default column family, handles[1] is file_cache_meta
    if (handles.size() >= 2) {
        _file_cache_meta_cf_handle.reset(handles[1]);
        // Close default column family handle as we won't use it
        _db->DestroyColumnFamilyHandle(handles[0]);
    } else {
        return Status::InternalError("Failed to get file_cache_meta column family handle");
    }

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

std::optional<BlockMeta> CacheBlockMetaStore::get(const BlockMetaKey& key) {
    // we trade accurate for clean code. so we ignore pending operations in the write queue
    // only use data in rocksdb
    std::string key_str = serialize_key(key);
    std::string value_str;
    rocksdb::Status status;

    if (!_db) {
        LOG(WARNING) << "Database not initialized, cannot get key";
        return std::nullopt;
    }
    status =
            _db->Get(rocksdb::ReadOptions(), _file_cache_meta_cf_handle.get(), key_str, &value_str);

    if (status.ok()) {
        return deserialize_value(value_str);
    } else if (status.IsNotFound()) {
        return std::nullopt;
    } else {
        LOG(WARNING) << "Failed to get key from rocksdb: " << status.ToString();
        return std::nullopt;
    }
}

std::unique_ptr<BlockMetaIterator> CacheBlockMetaStore::range_get(int64_t tablet_id) {
    // we trade accurate for clean code. so we ignore pending operations in the write queue
    std::string prefix = std::to_string(tablet_id) + "_";

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
            if (pos1 == std::string::npos) {
                return BlockMetaKey();
            }

            size_t pos2 = key_str.find('_', pos1 + 1);
            if (pos2 == std::string::npos) {
                return BlockMetaKey();
            }

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
            auto meta = deserialize_value(value_str);
            VLOG_DEBUG << "RocksDB value: " << value_str << ", deserialized as: type=" << meta.type
                       << ", size=" << meta.size << ", ttl=" << meta.ttl;
            return meta;
        }

    private:
        rocksdb::Iterator* _iter;
        std::string _prefix;
    };

    if (!_db) {
        LOG(WARNING) << "Database not initialized, cannot create iterator";
        return nullptr;
    }
    rocksdb::Iterator* iter =
            _db->NewIterator(rocksdb::ReadOptions(), _file_cache_meta_cf_handle.get());
    return std::unique_ptr<BlockMetaIterator>(new RocksDBIterator(iter, prefix));
}

std::unique_ptr<BlockMetaIterator> CacheBlockMetaStore::get_all() {
    if (!_db) {
        LOG(WARNING) << "Database not initialized in get_all()";
        return nullptr;
    }

    class RocksDBIterator : public BlockMetaIterator {
    public:
        RocksDBIterator(rocksdb::Iterator* iter) : _iter(iter) { _iter->SeekToFirst(); }

        ~RocksDBIterator() override { delete _iter; }

        bool valid() const override { return _iter->Valid(); }

        void next() override { _iter->Next(); }

        BlockMetaKey key() const override {
            std::string key_str = _iter->key().ToString();
            // Key format: "tabletid_hashstring_offset"
            size_t pos1 = key_str.find('_');
            if (pos1 == std::string::npos) {
                return BlockMetaKey();
            }

            size_t pos2 = key_str.find('_', pos1 + 1);
            if (pos2 == std::string::npos) {
                return BlockMetaKey();
            }

            int64_t tablet_id = std::stoll(key_str.substr(0, pos1));
            std::string hash_str = key_str.substr(pos1 + 1, pos2 - pos1 - 1);
            size_t offset = std::stoull(key_str.substr(pos2 + 1));
            // Convert hash string back to UInt128Wrapper
            uint128_t hash_value = vectorized::unhex_uint<uint128_t>(hash_str.c_str());
            return BlockMetaKey(tablet_id, UInt128Wrapper(hash_value), offset);
        }

        BlockMeta value() const override {
            std::string value_str = _iter->value().ToString();
            auto meta = deserialize_value(value_str);
            VLOG_DEBUG << "RocksDB value: " << value_str << ", deserialized as: type=" << meta.type
                       << ", size=" << meta.size << ", ttl=" << meta.ttl;
            return meta;
        }

    private:
        rocksdb::Iterator* _iter;
    };

    rocksdb::Iterator* iter =
            _db->NewIterator(rocksdb::ReadOptions(), _file_cache_meta_cf_handle.get());
    if (!iter) {
        LOG(WARNING) << "Failed to create rocksdb iterator in get_all()";
        return nullptr;
    }
    return std::unique_ptr<BlockMetaIterator>(new RocksDBIterator(iter));
}

void CacheBlockMetaStore::delete_key(const BlockMetaKey& key) {
    std::string key_str = serialize_key(key);

    // Put delete task into queue for asynchronous processing
    WriteOperation op;
    op.type = OperationType::DELETE;
    op.key = key_str;
    _write_queue.enqueue(op);
}

void CacheBlockMetaStore::clear() {
    // First, stop the async worker thread
    _stop_worker.store(true, std::memory_order_release);
    if (_write_thread.joinable()) {
        _write_thread.join();
    }

    // Clear the write queue to remove any pending operations
    WriteOperation op;
    while (_write_queue.try_dequeue(op)) {
        // Just discard all pending operations
    }

    // Delete all records from rocksdb

    if (_db) {
        // Use DeleteRange to delete all keys
        rocksdb::Slice start = "";
        rocksdb::Slice end = "\xff\xff\xff\xff"; // Maximum byte sequence
        rocksdb::Status status = _db->DeleteRange(rocksdb::WriteOptions(),
                                                  _file_cache_meta_cf_handle.get(), start, end);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to delete range from rocksdb: " << status.ToString();
        }
    }

    // Restart the async worker thread
    _stop_worker.store(false, std::memory_order_release);
    _write_thread = std::thread(&CacheBlockMetaStore::async_write_worker, this);
}

void CacheBlockMetaStore::async_write_worker() {
    Thread::set_self_name("cache_block_meta_store_async_write_worker");
    while (!_stop_worker.load(std::memory_order_acquire)) {
        WriteOperation op;

        if (_write_queue.try_dequeue(op)) {
            rocksdb::Status status;

            if (!_db) {
                LOG(WARNING) << "Database not initialized, skipping operation";
                continue;
            }

            if (op.type == OperationType::PUT) {
                status = _db->Put(rocksdb::WriteOptions(), _file_cache_meta_cf_handle.get(), op.key,
                                  op.value);
            } else if (op.type == OperationType::DELETE) {
                status = _db->Delete(rocksdb::WriteOptions(), _file_cache_meta_cf_handle.get(),
                                     op.key);
            }

            if (!status.ok()) {
                LOG(WARNING) << "Failed to " << (op.type == OperationType::PUT ? "write" : "delete")
                             << " to rocksdb: " << status.ToString();
                if (op.type == OperationType::PUT) {
                    g_rocksdb_write_failed_num << 1;
                } else {
                    g_rocksdb_delete_failed_num << 1;
                }
            }
        } else {
            // Queue is empty, sleep briefly
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    // Process remaining tasks in the queue
    WriteOperation op;
    while (_write_queue.try_dequeue(op)) {
        rocksdb::Status status;

        if (!_db) {
            LOG(WARNING) << "Database not initialized, skipping operation";
            continue;
        }

        if (op.type == OperationType::PUT) {
            status = _db->Put(rocksdb::WriteOptions(), _file_cache_meta_cf_handle.get(), op.key,
                              op.value);
        } else if (op.type == OperationType::DELETE) {
            status = _db->Delete(rocksdb::WriteOptions(), _file_cache_meta_cf_handle.get(), op.key);
        }

        if (!status.ok()) {
            LOG(WARNING) << "Failed to " << (op.type == OperationType::PUT ? "write" : "delete")
                         << " to rocksdb: " << status.ToString();
        }
    }
}

std::string serialize_key(const BlockMetaKey& key) {
    return fmt::format("{}_{}_{}", key.tablet_id, key.hash.to_string(), key.offset);
}

std::string serialize_value(const BlockMeta& meta) {
    doris::io::cache::BlockMetaPb pb;
    pb.set_type(meta.type);
    pb.set_size(meta.size);
    pb.set_ttl(meta.ttl);

    std::string result;
    pb.SerializeToString(&result);
    return result;
}

BlockMetaKey deserialize_key(const std::string& key_str) {
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

BlockMeta deserialize_value(const std::string& value_str) {
    // Parse as protobuf format
    doris::io::cache::BlockMetaPb pb;
    if (pb.ParseFromString(value_str)) {
        return BlockMeta(pb.type(), pb.size(), pb.ttl());
    }

    LOG(WARNING) << "Failed to deserialize value as protobuf: " << value_str;
    return BlockMeta();
}

} // namespace doris::io