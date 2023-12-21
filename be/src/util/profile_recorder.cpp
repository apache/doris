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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/runtime-profile.h
// and modified by Doris

#include "profile_recorder.h"

#include <glog/logging.h>
#include <rocksdb/iterator.h>
#include <rocksdb/status.h>
#include <thrift/Thrift.h>

#include <algorithm>
#include <memory>
#include <ostream>

#include "common/config.h"
#include "common/status.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/time.h"

namespace doris {
const std::string Profile_POSTFIX = "/profile";

ProfileRecorder::ProfileRecorder(const std::string& root_path)
        : _root_path(root_path), _db(nullptr), _last_compaction_time(UnixMillis()) {}

ProfileRecorder::~ProfileRecorder() {
    if (_db) {
        for (auto handle : _handles) {
            _db->DestroyColumnFamilyHandle(handle);
            handle = nullptr;
        }
        rocksdb::Status s = _db->SyncWAL();
        if (!s.ok()) {
            LOG(WARNING) << "rocksdb sync wal failed: " << s.ToString();
        }
        rocksdb::CancelAllBackgroundWork(_db, true);
        delete _db;
        _db = nullptr;
        LOG(INFO) << "finish close rocksdb for ~ProfileRecorder";
    }
}

Status ProfileRecorder::init() {
    // init db
    rocksdb::DBOptions options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    std::string db_path = _root_path + Profile_POSTFIX;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    // default column family is required
    column_families.emplace_back(DEFAULT_COLUMN_FAMILY, rocksdb::ColumnFamilyOptions());
    // TODO(zs) 28800 need to be placed in config.cpp
    std::vector<int32_t> ttls = {28800};
    rocksdb::Status s =
            rocksdb::DBWithTTL::Open(options, db_path, column_families, &_handles, &_db, ttls);
    if (!s.ok() || !_db) {
        LOG(WARNING) << "rocks db open failed, reason:" << s.ToString();
        return Status::InternalError("profile record rocksdb open failed, reason: {}",
                                     s.ToString());
    }
    return Status::OK();
}

Status ProfileRecorder::put_profile(const std::string& key, const TRuntimeProfileTree& value) {
    ThriftSerializer ser(false, 4096);
    uint8_t* buf;
    uint32_t len = 0;
    auto st_ser = ser.serialize(&value, &len, &buf);
    if (!st_ser.ok()) {
        LOG(WARNING) << "profile TRuntimeProfileTree serialize failed, errmsg=" << st_ser;
        return Status::InternalError("profile TRuntimeProfileTree serialize failed, errmsg={}",
                                     st_ser);
    }

    auto st = put(key, std::string((const char*)buf, len));
    if (st.ok()) {
        LOG(INFO) << "put profile_record rocksdb successfully. query_id: " << key;
    } else {
        LOG(WARNING) << "put profile_record rocksdb failed. query_id: " << st_ser;
        return Status::InternalError("put profile_record rocksdb failed. query_id: {}", st);
    }
    return Status::OK();
}

Status ProfileRecorder::put(const std::string& key, const std::string& value) {
    rocksdb::ColumnFamilyHandle* handle = _handles[0];
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    rocksdb::Status s = _db->Put(write_options, handle, rocksdb::Slice(key), rocksdb::Slice(value));
    if (!s.ok()) {
        LOG(WARNING) << "rocksdb put key:" << key << " failed, reason:" << s.ToString();
        return Status::InternalError("profile record rocksdb put failed, reason: {}", s.ToString());
    }

    // TODO(zs) 1800 need to be placed in config.cpp
    if ((UnixMillis() - _last_compaction_time) / 1000 > 1800) {
        rocksdb::CompactRangeOptions options;
        s = _db->CompactRange(options, _handles[0], nullptr, nullptr);
        if (s.ok()) {
            _last_compaction_time = UnixMillis();
        }
    }
    return Status::OK();
}

Status ProfileRecorder::get(const std::string& key,
                            std::map<std::string, std::string>& profile_records) {
    if (!_db) {
        return Status::InvalidArgument("rocksdb not initialized");
    }

    rocksdb::ColumnFamilyHandle* handle = _handles[0];
    rocksdb::ReadOptions options;
    std::string value;
    rocksdb::Status status = _db->Get(options, handle, key, &value);

    if (!status.ok()) {
        LOG(WARNING) << "RocksDB get failed. Key: " << key << ", Reason: " << status.ToString();
        return Status::NotFound("Profile record key not found in RocksDB");
    }
    profile_records[key] = value;
    return Status::OK();
}

Status ProfileRecorder::get_batch(const std::vector<std::string>& keys,
                                  std::map<std::string, std::string>& profile_records) {
    if (!_db) {
        return Status::InvalidArgument("rocksdb not initialized");
    }

    std::vector<rocksdb::Slice> slices;
    for (const std::string& key : keys) {
        slices.push_back(key);
    }

    std::vector<std::string> values(keys.size());
    std::vector<rocksdb::ColumnFamilyHandle*> handles(keys.size(), _handles[0]);
    std::vector<rocksdb::Status> statuses =
            _db->MultiGet(rocksdb::ReadOptions(), handles, slices, &values);

    for (size_t i = 0; i < keys.size(); i++) {
        if (statuses[i].ok()) {
            profile_records[keys[i]] = values[i];
        } else {
            LOG(WARNING) << "RocksDB multiGet failed for key: " << keys[i]
                         << ", Reason: " << statuses[i].ToString();
        }
    }
    return Status::OK();
}

} // namespace doris
