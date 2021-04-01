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

#include "olap/olap_meta.h"

#include <sstream>
#include <vector>

#include "common/logging.h"
#include "olap/olap_define.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "util/cpu_info.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"

using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::ReadOptions;
using rocksdb::WriteOptions;
using rocksdb::Slice;
using rocksdb::Iterator;

namespace doris {

rocksdb::CompressionType compression_type(const std::string& compression)
{
    if (compression == "SNAPPY") {
        return rocksdb::kSnappyCompression;
    } else if (compression == "LZ4") {
        return rocksdb::kLZ4Compression;
    }

    LOG(FATAL) << "compression type not recognized: " << compression;
    return rocksdb::kNoCompression;
}

std::shared_ptr<rocksdb::Cache> OlapMeta::_s_block_cache;

OlapMeta::OlapMeta(const std::string& root_path) : _root_path(root_path), _db(nullptr) {}

OlapMeta::~OlapMeta() {
    if (_db != nullptr) {
        for (auto& handle : _handles) {
            _db->DestroyColumnFamilyHandle(handle);
            handle = nullptr;
        }
        delete _db;
        _db = nullptr;
    }
}

OLAPStatus OlapMeta::init() {
    // init db
    DBOptions options;
    int total_threads = config::rocksdb_thread_count;
    if (total_threads == -1) {
        total_threads = CpuInfo::num_cores();
    }
    options.IncreaseParallelism(total_threads);
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    std::string db_path = _root_path + META_POSTFIX;
    std::vector<ColumnFamilyDescriptor> column_families;
    // Not used, but we have to open the default column family in order to open all column families
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, ColumnFamilyOptions());
    // Deprecated, but we have to open it to keep compatible
    column_families.emplace_back(DORIS_COLUMN_FAMILY, ColumnFamilyOptions());

    ColumnFamilyOptions column_family_options;
    column_family_options.compression = compression_type(config::rocksdb_compression_type);
    column_family_options.memtable_prefix_bloom_size_ratio = 0.02;
    {
        rocksdb::BlockBasedTableOptions block_based_options;
        block_based_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
        static std::once_flag flag;
        std::call_once(flag, [&]() {
          _s_block_cache = rocksdb::NewLRUCache(config::rocksdb_block_cache_mb * 1024 * 1024);
        });
        block_based_options.block_cache = _s_block_cache;
        column_family_options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    }
    column_families.emplace_back(META_COLUMN_FAMILY, column_family_options);
    rocksdb::Status s = DB::Open(options, db_path, column_families, &_handles, &_db);
    if (!s.ok() || _db == nullptr) {
        LOG(WARNING) << "rocks db open failed, reason:" << s.ToString();
        return OLAP_ERR_META_OPEN_DB;
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapMeta::get(const int column_family_index, const std::string& key,
                         std::string* value) {
    DorisMetrics::instance()->meta_read_request_total->increment(1);
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    int64_t duration_ns = 0;
    rocksdb::Status s;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        s = _db->Get(ReadOptions(), handle, Slice(key), value);
    }
    DorisMetrics::instance()->meta_read_request_duration_us->increment(duration_ns / 1000);
    if (s.IsNotFound()) {
        return OLAP_ERR_META_KEY_NOT_FOUND;
    } else if (!s.ok()) {
        LOG(WARNING) << "rocks db get key:" << key << " failed, reason:" << s.ToString();
        return OLAP_ERR_META_GET;
    }
    return OLAP_SUCCESS;
}

bool OlapMeta::key_may_exist(const int column_family_index, const std::string& key,
                         std::string* value) {
    DorisMetrics::instance()->meta_read_request_total->increment(1);
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    int64_t duration_ns = 0;
    bool is_exist = false;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        is_exist = _db->KeyMayExist(ReadOptions(), handle, Slice(key), value);
    }
    DorisMetrics::instance()->meta_read_request_duration_us->increment(duration_ns / 1000);
    
    return is_exist;
}

OLAPStatus OlapMeta::put(const int column_family_index, const std::string& key,
                         const std::string& value) {
    DorisMetrics::instance()->meta_write_request_total->increment(1);
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    int64_t duration_ns = 0;
    rocksdb::Status s;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Put(write_options, handle, Slice(key), Slice(value));
    }
    DorisMetrics::instance()->meta_write_request_duration_us->increment(duration_ns / 1000);
    if (!s.ok()) {
        LOG(WARNING) << "rocks db put key:" << key << " failed, reason:" << s.ToString();
        return OLAP_ERR_META_PUT;
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapMeta::remove(const int column_family_index, const std::string& key) {
    DorisMetrics::instance()->meta_write_request_total->increment(1);
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    rocksdb::Status s;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Delete(write_options, handle, Slice(key));
    }
    DorisMetrics::instance()->meta_write_request_duration_us->increment(duration_ns / 1000);
    if (!s.ok()) {
        LOG(WARNING) << "rocks db delete key:" << key << " failed, reason:" << s.ToString();
        return OLAP_ERR_META_DELETE;
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapMeta::iterate(
        const int column_family_index, const std::string& prefix,
        std::function<bool(const std::string&, const std::string&)> const& func) {
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    std::unique_ptr<Iterator> it(_db->NewIterator(ReadOptions(), handle));
    if (prefix == "") {
        it->SeekToFirst();
    } else {
        it->Seek(prefix);
    }
    rocksdb::Status status = it->status();
    if (!status.ok()) {
        LOG(WARNING) << "rocksdb seek failed. reason:" << status.ToString();
        return OLAP_ERR_META_ITERATOR;
    }
    for (; it->Valid(); it->Next()) {
        if (prefix != "") {
            if (!it->key().starts_with(prefix)) {
                return OLAP_SUCCESS;
            }
        }
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        bool ret = func(key, value);
        if (!ret) {
            break;
        }
    }
    if (!it->status().ok()) {
        LOG(WARNING) << "rocksdb iterator failed. reason:" << status.ToString();
        return OLAP_ERR_META_ITERATOR;
    }
    return OLAP_SUCCESS;
}

std::string OlapMeta::get_root_path() {
    return _root_path;
}

} // namespace doris
