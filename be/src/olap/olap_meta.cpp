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

#include <bvar/latency_recorder.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <sstream>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "olap/olap_define.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"

using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyOptions;
using rocksdb::ReadOptions;
using rocksdb::WriteOptions;
using rocksdb::Iterator;
using rocksdb::NewFixedPrefixTransform;

namespace doris {
using namespace ErrorCode;
const std::string META_POSTFIX = "/meta";
const size_t PREFIX_LENGTH = 4;

bvar::LatencyRecorder g_meta_put_latency("meta_put");
bvar::LatencyRecorder g_meta_get_latency("meta_get");
bvar::LatencyRecorder g_meta_remove_latency("meta_remove");

OlapMeta::OlapMeta(const std::string& root_path) : _root_path(root_path) {}

OlapMeta::~OlapMeta() = default;

class RocksdbLogger : public rocksdb::Logger {
public:
    void Logv(const char* format, va_list ap) override {
        char buf[1024];
        vsnprintf(buf, sizeof(buf), format, ap);
        LOG(INFO) << "[Rocksdb] " << buf;
    }
};

Status OlapMeta::init() {
    // init db
    DBOptions options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.info_log = std::make_shared<RocksdbLogger>();
    options.info_log_level = rocksdb::WARN_LEVEL;

    std::string db_path = _root_path + META_POSTFIX;
    std::vector<ColumnFamilyDescriptor> column_families;
    // default column family is required
    column_families.emplace_back(DEFAULT_COLUMN_FAMILY, ColumnFamilyOptions());
    column_families.emplace_back(DORIS_COLUMN_FAMILY, ColumnFamilyOptions());

    // meta column family add prefix extractor to improve performance and ensure correctness
    ColumnFamilyOptions meta_column_family;
    meta_column_family.max_write_buffer_number = config::rocksdb_max_write_buffer_number;
    meta_column_family.prefix_extractor.reset(NewFixedPrefixTransform(PREFIX_LENGTH));
    column_families.emplace_back(META_COLUMN_FAMILY, meta_column_family);

    rocksdb::DB* db;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::Status s = DB::Open(options, db_path, column_families, &handles, &db);
    _db = std::unique_ptr<rocksdb::DB, std::function<void(rocksdb::DB*)>>(db, [](rocksdb::DB* db) {
        rocksdb::Status s = db->SyncWAL();
        if (!s.ok()) {
            LOG(WARNING) << "rocksdb sync wal failed: " << s.ToString();
        }
        rocksdb::CancelAllBackgroundWork(db, true);
        s = db->Close();
        if (!s.ok()) {
            LOG(WARNING) << "rocksdb close failed: " << s.ToString();
        }
        LOG(INFO) << "finish close rocksdb for OlapMeta";

        delete db;
    });
    for (auto handle : handles) {
        _handles.emplace_back(handle);
    }
    if (!s.ok() || _db == nullptr) {
        return Status::Error<META_OPEN_DB_ERROR>("rocks db open failed, reason: {}", s.ToString());
    }
    return Status::OK();
}

Status OlapMeta::get(const int column_family_index, const std::string& key, std::string* value) {
    auto& handle = _handles[column_family_index];
    int64_t duration_ns = 0;
    rocksdb::Status s;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        s = _db->Get(ReadOptions(), handle.get(), rocksdb::Slice(key), value);
    }
    g_meta_get_latency << (duration_ns / 1000);
    if (s.IsNotFound()) {
        return Status::Error<META_KEY_NOT_FOUND>("OlapMeta::get meet not found key");
    } else if (!s.ok()) {
        return Status::Error<META_GET_ERROR>("rocks db get failed, key: {}, reason: {}", key,
                                             s.ToString());
    }
    return Status::OK();
}

bool OlapMeta::key_may_exist(const int column_family_index, const std::string& key,
                             std::string* value) {
    auto& handle = _handles[column_family_index];
    int64_t duration_ns = 0;
    bool is_exist = false;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        is_exist = _db->KeyMayExist(ReadOptions(), handle.get(), rocksdb::Slice(key), value);
    }
    g_meta_get_latency << (duration_ns / 1000);

    return is_exist;
}

Status OlapMeta::put(const int column_family_index, const std::string& key,
                     const std::string& value) {
    // log all params
    VLOG_DEBUG << "column_family_index: " << column_family_index << ", key: " << key
               << ", value: " << value;

    auto& handle = _handles[column_family_index];
    rocksdb::Status s;
    {
        int64_t duration_ns = 0;
        Defer defer([&] { g_meta_put_latency << (duration_ns / 1000); });
        SCOPED_RAW_TIMER(&duration_ns);

        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Put(write_options, handle.get(), rocksdb::Slice(key), rocksdb::Slice(value));
    }

    if (!s.ok()) {
        return Status::Error<META_PUT_ERROR>("rocks db put failed, key: {}, reason: {}", key,
                                             s.ToString());
    }
    return Status::OK();
}

Status OlapMeta::put(const int column_family_index, const std::vector<BatchEntry>& entries) {
    auto* handle = _handles[column_family_index].get();
    rocksdb::Status s;
    {
        int64_t duration_ns = 0;
        Defer defer([&] { g_meta_put_latency << (duration_ns / 1000); });
        SCOPED_RAW_TIMER(&duration_ns);

        // construct write batch
        rocksdb::WriteBatch write_batch;
        for (auto entry : entries) {
            VLOG_DEBUG << "column_family_index: " << column_family_index << ", key: " << entry.key
                       << ", value: " << entry.value;
            write_batch.Put(handle, rocksdb::Slice(entry.key), rocksdb::Slice(entry.value));
        }

        // write to rocksdb
        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Write(write_options, &write_batch);
    }

    if (!s.ok()) {
        return Status::Error<META_PUT_ERROR>("rocks db put failed, reason: {}", s.ToString());
    }
    return Status::OK();
}

Status OlapMeta::put(rocksdb::WriteBatch* batch) {
    rocksdb::Status s;
    {
        int64_t duration_ns = 0;
        Defer defer([&] { g_meta_put_latency << (duration_ns / 1000); });
        SCOPED_RAW_TIMER(&duration_ns);

        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Write(write_options, batch);
    }

    if (!s.ok()) {
        return Status::Error<META_PUT_ERROR>("rocks db put failed, reason: {}", s.ToString());
    }
    return Status::OK();
}

Status OlapMeta::remove(const int column_family_index, const std::string& key) {
    auto& handle = _handles[column_family_index];
    rocksdb::Status s;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Delete(write_options, handle.get(), rocksdb::Slice(key));
    }
    g_meta_remove_latency << (duration_ns / 1000);
    if (!s.ok()) {
        return Status::Error<META_DELETE_ERROR>("rocks db delete key: {}, failed, reason: {}", key,
                                                s.ToString());
    }
    return Status::OK();
}

Status OlapMeta::remove(const int column_family_index, const std::vector<std::string>& keys) {
    auto& handle = _handles[column_family_index];
    rocksdb::Status s;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        rocksdb::WriteBatch batch;
        for (auto& key : keys) {
            batch.Delete(handle.get(), rocksdb::Slice(key));
        }
        s = _db->Write(write_options, &batch);
    }
    g_meta_remove_latency << (duration_ns / 1000);
    if (!s.ok()) {
        return Status::Error<META_DELETE_ERROR>("rocks db delete keys:{} failed, reason:{}", keys,
                                                s.ToString());
    }
    return Status::OK();
}

Status OlapMeta::iterate(const int column_family_index, const std::string& prefix,
                         std::function<bool(const std::string&, const std::string&)> const& func) {
    return iterate(column_family_index, prefix, prefix, func);
}

Status OlapMeta::iterate(const int column_family_index, const std::string& seek_key,
                         const std::string& prefix,
                         std::function<bool(const std::string&, const std::string&)> const& func) {
    auto& handle = _handles[column_family_index];
    std::unique_ptr<Iterator> it(_db->NewIterator(ReadOptions(), handle.get()));
    if (seek_key == "") {
        it->SeekToFirst();
    } else {
        it->Seek(seek_key);
    }
    rocksdb::Status status = it->status();
    if (!status.ok()) {
        return Status::Error<META_ITERATOR_ERROR>("rocksdb seek failed. reason: {}",
                                                  status.ToString());
    }

    for (; it->Valid(); it->Next()) {
        if (prefix != "") {
            if (!it->key().starts_with(prefix)) {
                return Status::OK();
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
        return Status::Error<META_ITERATOR_ERROR>("rocksdb iterator failed. reason: {}",
                                                  status.ToString());
    }

    return Status::OK();
}

} // namespace doris
