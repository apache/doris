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
#include <optional>
#include <sstream>

#include "common/status.h"
#include "util/threadpool.h"
#include "vec/common/hex.h"

namespace doris::io {

CacheBlockMetaStore::CacheBlockMetaStore(const std::string& db_path, size_t queue_size)
        : _db_path(db_path), _write_queue(queue_size) {
    auto status = init();
    DCHECK(status.ok()) << "Failed to initialize CacheBlockMetaStore: " << status.to_string();
}

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

void CacheBlockMetaStore::force_close() {
    _stop_worker.store(true, std::memory_order_release);
    if (_write_thread.joinable()) {
        _write_thread.join();
    }

    if (_db) {
        _db->Close();
        _db.reset();
    }
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
    std::string key_str = serialize_key(key);

    // First check if there are any pending operations for this key in the write queue
    WriteOperation op;
    std::vector<WriteOperation> ops_to_requeue;
    bool found_in_queue = false;
    std::optional<BlockMeta> result;
    WriteOperation target_op;

    // Use a lock to protect queue access during query operations
    // This prevents race conditions where multiple threads might dequeue and requeue operations
    {
        std::lock_guard<std::mutex> lock(_queue_mutex);

        // Iterate through the queue to find operations for this key
        while (_write_queue.try_dequeue(op)) {
            if (op.key == key_str) {
                found_in_queue = true;
                target_op = std::move(op);
                // Continue processing to collect all operations for requeue
            } else {
                // Requeue operations for other keys
                ops_to_requeue.push_back(std::move(op));
            }
        }

        // Process the found operation
        if (found_in_queue) {
            if (target_op.type == OperationType::DELETE) {
                // Key is marked for deletion, return nullopt
                result = std::nullopt;
            } else if (target_op.type == OperationType::PUT) {
                // Key has a pending put operation, return the new value
                result = deserialize_value(target_op.value);
            }
            // Requeue the target operation to maintain queue consistency
            ops_to_requeue.push_back(std::move(target_op));
        }

        // Requeue all operations for other keys
        for (auto& op_to_requeue : ops_to_requeue) {
            _write_queue.enqueue(std::move(op_to_requeue));
        }
    }

    // If we found the key in the queue, return the result
    if (found_in_queue) {
        return result;
    }

    // If not found in queue, query rocksdb with proper locking
    std::string value_str;
    rocksdb::Status status;
    {
        std::lock_guard<std::mutex> lock(_db_mutex);
        if (!_db) {
            LOG(WARNING) << "Database not initialized, cannot get key";
            return std::nullopt;
        }
        status = _db->Get(rocksdb::ReadOptions(), key_str, &value_str);
    }

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
    std::string prefix = std::to_string(tablet_id) + "_";

    // Collect all pending operations from the write queue with proper locking
    std::unordered_map<std::string, WriteOperation> tablet_ops;
    {
        std::vector<WriteOperation> pending_ops;
        WriteOperation op;
        while (_write_queue.try_dequeue(op)) {
            pending_ops.push_back(std::move(op));
        }

        // Filter operations for the target tablet_id and requeue others
        std::vector<WriteOperation> ops_to_requeue;

        for (auto& operation : pending_ops) {
            if (operation.key.starts_with(prefix)) {
                // Operation for target tablet, store in map (latest operation wins)
                // But if we have a DELETE operation, it should override any previous operations
                if (operation.type == OperationType::DELETE) {
                    tablet_ops[operation.key] = std::move(operation);
                } else if (tablet_ops.find(operation.key) == tablet_ops.end() ||
                           tablet_ops[operation.key].type != OperationType::DELETE) {
                    // Only store PUT operation if there's no DELETE operation for this key
                    tablet_ops[operation.key] = std::move(operation);
                }
            } else {
                // Operation for other tablets, requeue
                ops_to_requeue.push_back(std::move(operation));
            }
        }

        // Requeue operations for other tablets
        for (auto& op_to_requeue : ops_to_requeue) {
            _write_queue.enqueue(std::move(op_to_requeue));
        }
    }

    // Create merged iterator that combines rocksdb data with pending operations
    class MergedIterator : public BlockMetaIterator {
    public:
        MergedIterator(rocksdb::Iterator* rocksdb_iter,
                       std::unordered_map<std::string, WriteOperation>&& pending_ops,
                       const std::string& prefix)
                : _rocksdb_iter(rocksdb_iter),
                  _pending_ops(std::move(pending_ops)),
                  _prefix(prefix) {
            _rocksdb_iter->Seek(_prefix);
            prepare_next();
        }

        ~MergedIterator() override { delete _rocksdb_iter; }

        bool valid() const override { return !_current_key.empty(); }

        void next() override {
            if (_current_from_pending) {
                if (_pending_iter != _pending_ops.end()) {
                    _pending_iter++;
                }
            } else {
                _rocksdb_iter->Next();
            }
            prepare_next();
        }

        BlockMetaKey key() const override { return deserialize_current_key(); }

        BlockMeta value() const override {
            if (_current_from_pending) {
                auto it = _pending_ops.find(_current_key);
                if (it != _pending_ops.end() && it->second.type == OperationType::PUT) {
                    return deserialize_value(it->second.value);
                }
                // Should not happen for valid entries
                return BlockMeta();
            } else {
                std::string value_str = _rocksdb_iter->value().ToString();
                return deserialize_value(value_str);
            }
        }

    private:
        void prepare_next() {
            _current_key.clear();
            _current_from_pending = false;

            // Initialize pending iterator if not done
            if (_pending_iter == _pending_ops.end() && !_pending_ops_initialized) {
                _pending_iter = _pending_ops.begin();
                _pending_ops_initialized = true;
            }

            // Keep looking until we find a valid entry
            while (true) {
                // Get next candidate from both sources
                std::string rocksdb_key;
                std::string pending_key;

                if (_rocksdb_iter->Valid() && _rocksdb_iter->key().starts_with(_prefix)) {
                    rocksdb_key = _rocksdb_iter->key().ToString();
                }

                if (_pending_iter != _pending_ops.end()) {
                    pending_key = _pending_iter->first;
                }

                // Choose the smallest key
                if (!rocksdb_key.empty() && !pending_key.empty()) {
                    if (rocksdb_key < pending_key) {
                        // Check if rocksdb_key has a pending DELETE operation
                        auto pending_it = _pending_ops.find(rocksdb_key);
                        if (pending_it != _pending_ops.end() &&
                            pending_it->second.type == OperationType::DELETE) {
                            // Skip rocksdb_key because it's marked for deletion
                            _rocksdb_iter->Next();
                            continue;
                        }
                        _current_key = rocksdb_key;
                        _current_from_pending = false;
                        break;
                    } else {
                        // Skip DELETE operations
                        if (_pending_iter->second.type == OperationType::DELETE) {
                            _pending_iter++;
                            if (_pending_iter == _pending_ops.end()) {
                                break;
                            }
                            continue;
                        }
                        _current_key = pending_key;
                        _current_from_pending = true;
                        break;
                    }
                } else if (!rocksdb_key.empty()) {
                    // Check if rocksdb_key has a pending DELETE operation
                    auto pending_it = _pending_ops.find(rocksdb_key);
                    if (pending_it != _pending_ops.end() &&
                        pending_it->second.type == OperationType::DELETE) {
                        // Skip rocksdb_key because it's marked for deletion
                        _rocksdb_iter->Next();
                        continue;
                    }
                    _current_key = rocksdb_key;
                    _current_from_pending = false;
                    break;
                } else if (!pending_key.empty()) {
                    // Skip DELETE operations
                    if (_pending_iter->second.type == OperationType::DELETE) {
                        _pending_iter++;
                        if (_pending_iter == _pending_ops.end()) {
                            break;
                        }
                        continue;
                    }
                    _current_key = pending_key;
                    _current_from_pending = true;
                    break;
                } else {
                    // No more keys
                    break;
                }
            }
        }

        BlockMetaKey deserialize_current_key() const {
            std::string key_str = _current_key;
            // Key format: "tabletid_hashstring_offset"
            size_t pos1 = key_str.find('_');
            if (pos1 == std::string::npos) {
                return BlockMetaKey();
            }

            size_t pos2 = key_str.find('_', pos1 + 1);
            if (pos2 == std::string::npos) {
                return BlockMetaKey();
            }

            try {
                int64_t tablet_id = std::stoll(key_str.substr(0, pos1));
                std::string hash_str = key_str.substr(pos1 + 1, pos2 - pos1 - 1);
                size_t offset = std::stoull(key_str.substr(pos2 + 1));

                // Convert hash string back to UInt128Wrapper
                uint128_t hash_value = vectorized::unhex_uint<uint128_t>(hash_str.c_str());

                return BlockMetaKey(tablet_id, UInt128Wrapper(hash_value), offset);
            } catch (const std::exception& e) {
                LOG(WARNING) << "Failed to deserialize key: " << key_str << ", error: " << e.what();
                return BlockMetaKey();
            }
        }

        BlockMeta deserialize_value(const std::string& value_str) const {
            // Parse as protobuf format
            doris::io::cache::BlockMetaPb pb;
            if (pb.ParseFromString(value_str)) {
                return BlockMeta(pb.type(), pb.size(), pb.ttl());
            }

            LOG(WARNING) << "Failed to deserialize value as protobuf: " << value_str;
            return BlockMeta();
        }

        rocksdb::Iterator* _rocksdb_iter;
        std::unordered_map<std::string, WriteOperation> _pending_ops;
        std::string _prefix;
        std::string _current_key;
        bool _current_from_pending = false;
        bool _pending_ops_initialized = false;
        typename std::unordered_map<std::string, WriteOperation>::iterator _pending_iter;
    };

    if (!_db) {
        LOG(WARNING) << "Database not initialized, cannot create iterator";
        return nullptr;
    }
    rocksdb::Iterator* iter = _db->NewIterator(rocksdb::ReadOptions());
    return std::make_unique<MergedIterator>(iter, std::move(tablet_ops), prefix);
}

std::unique_ptr<BlockMetaIterator> CacheBlockMetaStore::get_all() {
    if (!_db) {
        LOG(WARNING) << "Database not initialized in get_all()";
        return nullptr;
    }
    // Collect all pending operations from the write queue with proper locking
    std::unordered_map<std::string, WriteOperation> all_ops;
    std::vector<WriteOperation> ops_to_requeue;
    {
        std::vector<WriteOperation> pending_ops;
        WriteOperation op;
        while (_write_queue.try_dequeue(op)) {
            pending_ops.push_back(std::move(op));
        }

        // Store operations in map (latest operation wins)
        for (auto& operation : pending_ops) {
            // If we have a DELETE operation, it should override any previous operations
            if (operation.type == OperationType::DELETE) {
                all_ops[operation.key] = std::move(operation);
            } else if (all_ops.find(operation.key) == all_ops.end() ||
                       all_ops[operation.key].type != OperationType::DELETE) {
                // Only store PUT operation if there's no DELETE operation for this key
                all_ops[operation.key] = std::move(operation);
            }
        }

        // Store operations to requeue (only those not included in the snapshot)
        for (auto& operation : pending_ops) {
            if (all_ops.find(operation.key) == all_ops.end() ||
                all_ops[operation.key].type != operation.type) {
                ops_to_requeue.push_back(std::move(operation));
            }
        }
    }

    // Requeue operations that were not included in the snapshot
    for (auto& op_to_requeue : ops_to_requeue) {
        _write_queue.enqueue(std::move(op_to_requeue));
    }

    // Create merged iterator that combines rocksdb data with pending operations
    class MergedFullIterator : public BlockMetaIterator {
    public:
        MergedFullIterator(rocksdb::Iterator* rocksdb_iter,
                           std::unordered_map<std::string, WriteOperation> pending_ops)
                : _rocksdb_iter(rocksdb_iter), _pending_ops(std::move(pending_ops)) {
            _rocksdb_iter->SeekToFirst();
            prepare_next();
        }

        ~MergedFullIterator() override { delete _rocksdb_iter; }

        bool valid() const override { return !_current_key.empty(); }

        void next() override {
            if (_current_from_pending) {
                _pending_iter++;
            } else {
                _rocksdb_iter->Next();
            }
            prepare_next();
        }

        BlockMetaKey key() const override {
            auto key = deserialize_current_key();
            VLOG_DEBUG << "Iterating key: " << _current_key
                       << ", deserialized as: tablet_id=" << key.tablet_id
                       << ", hash=" << key.hash.low() << "-" << key.hash.high()
                       << ", offset=" << key.offset;
            return key;
        }

        BlockMeta value() const override {
            if (_current_from_pending) {
                auto it = _pending_ops.find(_current_key);
                if (it != _pending_ops.end() && it->second.type == OperationType::PUT) {
                    auto meta = deserialize_value(it->second.value);
                    VLOG_DEBUG << "Pending op value: " << it->second.value
                               << ", deserialized as: type=" << meta.type << ", size=" << meta.size
                               << ", ttl=" << meta.ttl;
                    return meta;
                }
                LOG(WARNING) << "Invalid pending operation for key: " << _current_key;
                return BlockMeta();
            } else {
                std::string value_str = _rocksdb_iter->value().ToString();
                auto meta = deserialize_value(value_str);
                VLOG_DEBUG << "RocksDB value: " << value_str
                           << ", deserialized as: type=" << meta.type << ", size=" << meta.size
                           << ", ttl=" << meta.ttl;
                return meta;
            }
        }

    private:
        void prepare_next() {
            _current_key.clear();
            _current_from_pending = false;

            // Initialize pending iterator if not done
            if (_pending_iter == _pending_ops.end() && !_pending_ops_initialized) {
                _pending_iter = _pending_ops.begin();
                _pending_ops_initialized = true;
            }

            // Keep looking until we find a valid entry
            while (true) {
                // Get next candidate from both sources
                std::string rocksdb_key;
                std::string pending_key;

                if (_rocksdb_iter->Valid()) {
                    rocksdb_key = _rocksdb_iter->key().ToString();
                }

                if (_pending_iter != _pending_ops.end()) {
                    pending_key = _pending_iter->first;
                }

                // Choose the smallest key
                if (!rocksdb_key.empty() && !pending_key.empty()) {
                    if (rocksdb_key < pending_key) {
                        // Check if rocksdb_key has a pending DELETE operation
                        auto pending_it = _pending_ops.find(rocksdb_key);
                        if (pending_it != _pending_ops.end() &&
                            pending_it->second.type == OperationType::DELETE) {
                            // Skip rocksdb_key because it's marked for deletion
                            _rocksdb_iter->Next();
                            continue;
                        }
                        _current_key = rocksdb_key;
                        _current_from_pending = false;
                        break;
                    } else {
                        _current_key = pending_key;
                        _current_from_pending = true;
                        // Skip DELETE operations
                        if (_pending_iter->second.type == OperationType::DELETE) {
                            _pending_iter++;
                            if (_pending_iter == _pending_ops.end()) {
                                break;
                            }
                            continue;
                        }
                        break;
                    }
                } else if (!rocksdb_key.empty()) {
                    // Check if rocksdb_key has a pending DELETE operation
                    auto pending_it = _pending_ops.find(rocksdb_key);
                    if (pending_it != _pending_ops.end() &&
                        pending_it->second.type == OperationType::DELETE) {
                        // Skip rocksdb_key because it's marked for deletion
                        _rocksdb_iter->Next();
                        continue;
                    }
                    _current_key = rocksdb_key;
                    _current_from_pending = false;
                    break;
                } else if (!pending_key.empty()) {
                    _current_key = pending_key;
                    _current_from_pending = true;
                    // Skip DELETE operations
                    if (_pending_iter->second.type == OperationType::DELETE) {
                        _pending_iter++;
                        if (_pending_iter == _pending_ops.end()) {
                            break;
                        }
                        continue;
                    }
                    break;
                } else {
                    // No more keys
                    break;
                }
            }
        }

        BlockMetaKey deserialize_current_key() const {
            std::string key_str = _current_key;
            // Key format: "tabletid_hashstring_offset"
            size_t pos1 = key_str.find('_');
            if (pos1 == std::string::npos) {
                return BlockMetaKey();
            }

            size_t pos2 = key_str.find('_', pos1 + 1);
            if (pos2 == std::string::npos) {
                return BlockMetaKey();
            }

            try {
                int64_t tablet_id = std::stoll(key_str.substr(0, pos1));
                std::string hash_str = key_str.substr(pos1 + 1, pos2 - pos1 - 1);
                size_t offset = std::stoull(key_str.substr(pos2 + 1));

                // Convert hash string back to UInt128Wrapper
                uint128_t hash_value = vectorized::unhex_uint<uint128_t>(hash_str.c_str());

                return BlockMetaKey(tablet_id, UInt128Wrapper(hash_value), offset);
            } catch (const std::exception& e) {
                LOG(WARNING) << "Failed to deserialize key: " << key_str << ", error: " << e.what();
                return BlockMetaKey();
            }
        }

        BlockMeta deserialize_value(const std::string& value_str) const {
            // Parse as protobuf format
            doris::io::cache::BlockMetaPb pb;
            if (pb.ParseFromString(value_str)) {
                return BlockMeta(pb.type(), pb.size(), pb.ttl());
            }

            LOG(WARNING) << "Failed to deserialize value as protobuf: " << value_str;
            return BlockMeta();
        }

        rocksdb::Iterator* _rocksdb_iter;
        std::unordered_map<std::string, WriteOperation> _pending_ops;
        std::string _current_key;
        bool _current_from_pending = false;
        bool _pending_ops_initialized = false;
        typename std::unordered_map<std::string, WriteOperation>::iterator _pending_iter;
    };

    if (!_db) {
        LOG(WARNING) << "Database not initialized, cannot create iterator";
        return nullptr;
    }
    rocksdb::Iterator* iter = _db->NewIterator(rocksdb::ReadOptions());
    if (!iter) {
        LOG(WARNING) << "Failed to create rocksdb iterator in get_all()";
        return nullptr;
    }
    auto result = std::make_unique<MergedFullIterator>(iter, std::move(all_ops));
    return result;
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
    {
        std::lock_guard<std::mutex> lock(_db_mutex);
        if (_db) {
            // Use DeleteRange to delete all keys
            rocksdb::Slice start = "";
            rocksdb::Slice end = "\xff\xff\xff\xff"; // Maximum byte sequence
            rocksdb::Status status = _db->DeleteRange(rocksdb::WriteOptions(),
                                                      _db->DefaultColumnFamily(), start, end);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to delete range from rocksdb: " << status.ToString();
            }
        }
    }

    // Restart the async worker thread
    _stop_worker.store(false, std::memory_order_release);
    _write_thread = std::thread(&CacheBlockMetaStore::async_write_worker, this);
}

void CacheBlockMetaStore::async_write_worker() {
    while (!_stop_worker.load(std::memory_order_acquire)) {
        WriteOperation op;

        if (_write_queue.try_dequeue(op)) {
            std::lock_guard<std::mutex> lock(_db_mutex);
            rocksdb::Status status;

            if (!_db) {
                LOG(WARNING) << "Database not initialized, skipping operation";
                continue;
            }

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

        if (!_db) {
            LOG(WARNING) << "Database not initialized, skipping operation";
            continue;
        }

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

//TODO(zhengyu): use pb
std::string CacheBlockMetaStore::serialize_value(const BlockMeta& meta) const {
    doris::io::cache::BlockMetaPb pb;
    pb.set_type(meta.type);
    pb.set_size(meta.size);
    pb.set_ttl(meta.ttl);

    std::string result;
    pb.SerializeToString(&result);
    return result;
}

//TODO(zhengyu): use pb
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
    // Parse as protobuf format
    doris::io::cache::BlockMetaPb pb;
    if (pb.ParseFromString(value_str)) {
        return BlockMeta(pb.type(), pb.size(), pb.ttl());
    }

    LOG(WARNING) << "Failed to deserialize value as protobuf: " << value_str;
    return BlockMeta();
}

} // namespace doris::io