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
#include <string_view>

#include "common/status.h"
#include "olap/field.h"
#include "olap/field.h" // For OLAP_FIELD_TYPE_BIGINT
#include "olap/key_coder.h"
#include "olap/olap_common.h"
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
    if (_initialized.load(std::memory_order_acquire)) {
        return Status::OK();
    }

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
    _initialized.store(true, std::memory_order_release);

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
        Status deserialize_status;
        auto result = deserialize_value(value_str, &deserialize_status);
        if (result.has_value()) {
            return result;
        } else {
            LOG(WARNING) << "Failed to deserialize value: " << deserialize_status.to_string();
            return std::nullopt;
        }
    } else if (status.IsNotFound()) {
        return std::nullopt;
    } else {
        LOG(WARNING) << "Failed to get key from rocksdb: " << status.ToString();
        return std::nullopt;
    }
}

std::unique_ptr<BlockMetaIterator> CacheBlockMetaStore::range_get(int64_t tablet_id) {
    // Generate prefix using new serialization format
    std::string prefix;
    prefix.push_back(0x1); // version byte
    auto* tablet_id_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_BIGINT);
    tablet_id_coder->full_encode_ascending(&tablet_id, &prefix);

    class RocksDBIterator : public BlockMetaIterator {
    public:
        RocksDBIterator(rocksdb::Iterator* iter, const std::string& prefix)
                : _iter(iter),
                  _prefix(prefix),
                  _last_key_error(Status::OK()),
                  _last_value_error(Status::OK()) {
            _iter->Seek(_prefix);
        }

        ~RocksDBIterator() override { delete _iter; }

        bool valid() const override {
            if (!_iter->Valid()) return false;
            Slice key_slice(_iter->key().data(), _prefix.size());
            return key_slice.compare(Slice(_prefix)) == 0;
        }

        void next() override { _iter->Next(); }

        BlockMetaKey key() const override {
            // Reset error state
            _last_key_error = Status::OK();

            auto key_view = std::string_view(_iter->key().data(), _iter->key().size());
            Status status;
            auto result = deserialize_key(std::string(key_view), &status);

            if (!result.has_value()) {
                _last_key_error = status;
                LOG(WARNING) << "Failed to deserialize key in range_get: " << status.to_string();
                // error indicator, caller should check get_last_key_error
                return BlockMetaKey(-1, UInt128Wrapper(0), 0);
            }

            return result.value();
        }

        BlockMeta value() const override {
            // Reset error state
            _last_value_error = Status::OK();

            auto value_view = std::string_view(_iter->value().data(), _iter->value().size());
            Status status;
            auto result = deserialize_value(value_view, &status);

            if (!result.has_value()) {
                _last_value_error = status;
                LOG(WARNING) << "Failed to deserialize value in range_get: " << status.to_string();
                // error indicator, caller should check get_last_value_error
                return BlockMeta(FileCacheType::DISPOSABLE, 0, 0);
            }

            VLOG_DEBUG << "RocksDB value: " << value_view
                       << ", deserialized as: type=" << result->type << ", size=" << result->size
                       << ", ttl=" << result->ttl;
            return result.value();
        }

        Status get_last_key_error() const override { return _last_key_error; }
        Status get_last_value_error() const override { return _last_value_error; }

    private:
        rocksdb::Iterator* _iter;
        std::string _prefix;
        mutable Status _last_key_error;
        mutable Status _last_value_error;
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
        RocksDBIterator(rocksdb::Iterator* iter)
                : _iter(iter), _last_key_error(Status::OK()), _last_value_error(Status::OK()) {
            _iter->SeekToFirst();
        }

        ~RocksDBIterator() override { delete _iter; }

        bool valid() const override { return _iter->Valid(); }

        void next() override { _iter->Next(); }

        BlockMetaKey key() const override {
            // Reset error state
            _last_key_error = Status::OK();

            auto key_view = std::string_view(_iter->key().data(), _iter->key().size());
            Status status;
            auto result = deserialize_key(std::string(key_view), &status);

            if (!result.has_value()) {
                _last_key_error = status;
                LOG(WARNING) << "Failed to deserialize key in get_all: " << status.to_string();
                // 返回一个无效的键作为错误指示，调用方应该检查错误状态
                return BlockMetaKey(-1, UInt128Wrapper(0), 0); // 使用无效值作为错误指示
            }

            return result.value();
        }

        BlockMeta value() const override {
            // Reset error state
            _last_value_error = Status::OK();

            auto value_view = std::string_view(_iter->value().data(), _iter->value().size());
            Status status;
            auto result = deserialize_value(value_view, &status);

            if (!result.has_value()) {
                _last_value_error = status;
                LOG(WARNING) << "Failed to deserialize value in get_all: " << status.to_string();
                // error indicator, caller should check get_last_value_error
                return BlockMeta(FileCacheType::DISPOSABLE, 0, 0);
            }

            VLOG_DEBUG << "RocksDB value: " << value_view
                       << ", deserialized as: type=" << result->type << ", size=" << result->size
                       << ", ttl=" << result->ttl;
            return result.value();
        }

        Status get_last_key_error() const override { return _last_key_error; }
        Status get_last_value_error() const override { return _last_value_error; }

    private:
        rocksdb::Iterator* _iter;
        mutable Status _last_key_error;
        mutable Status _last_value_error;
    };

    rocksdb::Iterator* iter =
            _db->NewIterator(rocksdb::ReadOptions(), _file_cache_meta_cf_handle.get());
    if (!iter) {
        LOG(WARNING) << "Failed to create rocksdb iterator in get_all()";
        return nullptr;
    }
    return std::unique_ptr<BlockMetaIterator>(new RocksDBIterator(iter));
}

size_t CacheBlockMetaStore::approximate_entry_count() const {
    if (!_db) {
        LOG(WARNING) << "Database not initialized when counting entries";
        return 0;
    }

    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter(
            _db->NewIterator(read_options, _file_cache_meta_cf_handle.get()));
    if (!iter) {
        LOG(WARNING) << "Failed to create iterator when counting entries";
        return 0;
    }

    size_t count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ++count;
    }

    if (!iter->status().ok()) {
        LOG(WARNING) << "Iterator encountered error when counting entries: "
                     << iter->status().ToString();
    }

    return count;
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
    std::string result;
    // Add version byte
    result.push_back(0x1);

    // Encode tablet_id using KeyCoderTraits
    auto* tablet_id_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_BIGINT);
    tablet_id_coder->full_encode_ascending(&key.tablet_id, &result);

    // Encode hash high and low parts
    uint64_t hash_high = key.hash.high();
    uint64_t hash_low = key.hash.low();
    tablet_id_coder->full_encode_ascending(&hash_high, &result);
    tablet_id_coder->full_encode_ascending(&hash_low, &result);

    // Encode offset
    tablet_id_coder->full_encode_ascending(&key.offset, &result);

    return result;
}

std::string serialize_value(const BlockMeta& meta) {
    doris::io::cache::BlockMetaPb pb;
    pb.set_type(static_cast<::doris::io::cache::FileCacheType>(meta.type));
    pb.set_size(meta.size);
    pb.set_ttl(meta.ttl);

    std::string result;
    pb.SerializeToString(&result);
    return result;
}

std::optional<BlockMetaKey> deserialize_key(const std::string& key_str, Status* status) {
    // New key format: [version][encoded tablet_id][encoded hash_high][encoded hash_low][encoded offset]
    Slice slice(key_str);

    // Check version byte
    if (slice.size < 1 || slice.data[0] != 0x1) {
        LOG(WARNING) << "Invalid key, expected prefix 0x1";
        if (status) *status = Status::InternalError("Failed to decode key: invalid version");
        return std::nullopt; // Invalid version
    }
    slice.remove_prefix(1); // skip version byte

    auto* tablet_id_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_BIGINT);
    int64_t tablet_id;
    uint64_t hash_high, hash_low;
    size_t offset;

    Status st = tablet_id_coder->decode_ascending(&slice, sizeof(int64_t),
                                                  reinterpret_cast<uint8_t*>(&tablet_id));
    if (!st.ok()) {
        if (status)
            *status = Status::InternalError("Failed to decode tablet_id: {}", st.to_string());
        return std::nullopt;
    }

    st = tablet_id_coder->decode_ascending(&slice, sizeof(uint64_t),
                                           reinterpret_cast<uint8_t*>(&hash_high));
    if (!st.ok()) {
        if (status)
            *status = Status::InternalError("Failed to decode hash_high: {}", st.to_string());
        return std::nullopt;
    }

    st = tablet_id_coder->decode_ascending(&slice, sizeof(uint64_t),
                                           reinterpret_cast<uint8_t*>(&hash_low));
    if (!st.ok()) {
        if (status)
            *status = Status::InternalError("Failed to decode hash_low: {}", st.to_string());
        return std::nullopt;
    }

    st = tablet_id_coder->decode_ascending(&slice, sizeof(size_t),
                                           reinterpret_cast<uint8_t*>(&offset));
    if (!st.ok()) {
        if (status) *status = Status::InternalError("Failed to decode offset: {}", st.to_string());
        return std::nullopt;
    }

    uint128_t hash = (static_cast<uint128_t>(hash_high) << 64) | hash_low;
    if (status) *status = Status::OK();
    return BlockMetaKey(tablet_id, UInt128Wrapper(hash), offset);
}

std::optional<BlockMeta> deserialize_value(const std::string& value_str, Status* status) {
    if (value_str.empty()) {
        if (status) *status = Status::InternalError("Failed to deserialize value");
        return std::nullopt;
    }

    // Parse as protobuf format
    doris::io::cache::BlockMetaPb pb;
    if (pb.ParseFromString(value_str)) {
        // Validate the parsed protobuf data
        int type = pb.type();
        if (type < 0 || type > 3) { // Valid FileCacheType values: 0-3
            LOG(WARNING) << "Invalid FileCacheType value: " << type;
            if (status)
                *status = Status::InternalError("Failed to deserialize value: invalid type");
            return std::nullopt;
        }
        if (pb.size() <= 0) {
            LOG(WARNING) << "Invalid size value: " << pb.size();
            if (status)
                *status = Status::InternalError("Failed to deserialize value: invalid size");
            return std::nullopt;
        }

        if (status) *status = Status::OK();
        return BlockMeta(static_cast<FileCacheType>(pb.type()), pb.size(), pb.ttl());
    }

    LOG(WARNING) << "Failed to deserialize value as protobuf: " << value_str;
    if (status) *status = Status::InternalError("Failed to deserialize value");
    return std::nullopt;
}

std::optional<BlockMeta> deserialize_value(std::string_view value_view, Status* status) {
    if (value_view.empty()) {
        if (status) *status = Status::InternalError("Failed to deserialize value");
        return std::nullopt;
    }

    // Parse as protobuf format using string_view
    doris::io::cache::BlockMetaPb pb;
    if (pb.ParseFromArray(value_view.data(), static_cast<int>(value_view.size()))) {
        // Validate the parsed protobuf data
        int type = pb.type();
        if (type < 0 || type > 3) { // Valid FileCacheType values: 0-3
            LOG(WARNING) << "Invalid FileCacheType value: " << type;
            if (status)
                *status = Status::InternalError("Failed to deserialize value: invalid type");
            return std::nullopt;
        }
        if (pb.size() <= 0) {
            LOG(WARNING) << "Invalid size value: " << pb.size();
            if (status)
                *status = Status::InternalError("Failed to deserialize value: invalid size");
            return std::nullopt;
        }

        if (status) *status = Status::OK();
        return BlockMeta(static_cast<FileCacheType>(pb.type()), pb.size(), pb.ttl());
    }

    LOG(WARNING) << "Failed to deserialize value as protobuf from string_view";
    if (status) *status = Status::InternalError("Failed to deserialize value");
    return std::nullopt;
}

} // namespace doris::io