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

#include <rocksdb/iterator.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"

namespace rocksdb {
class ColumnFamilyHandle;
class DB;
class WriteBatch;
} // namespace rocksdb

namespace doris {

class OlapMeta final {
public:
    struct BatchEntry {
        const std::string& key;
        const std::string& value;

        BatchEntry(const std::string& key_arg, const std::string& value_arg)
                : key(key_arg), value(value_arg) {}
    };

    OlapMeta(const std::string& root_path);
    ~OlapMeta();

    Status init();

    Status get(const int column_family_index, const std::string& key, std::string* value);

    bool key_may_exist(const int column_family_index, const std::string& key, std::string* value);

    Status put(const int column_family_index, const std::string& key, const std::string& value);
    Status put(const int column_family_index, const std::vector<BatchEntry>& entries);
    Status put(rocksdb::WriteBatch* batch);

    Status remove(const int column_family_index, const std::string& key);
    Status remove(const int column_family_index, const std::vector<std::string>& keys);

    Status iterate(const int column_family_index, std::string_view prefix,
                   std::function<bool(std::string_view, std::string_view)> const& func);

    Status iterate(const int column_family_index, std::string_view seek_key,
                   std::string_view prefix,
                   std::function<bool(std::string_view, std::string_view)> const& func);

    [[nodiscard]] std::string get_root_path() const { return _root_path; }

    rocksdb::ColumnFamilyHandle* get_handle(const int column_family_index) {
        return _handles[column_family_index].get();
    }

private:
    std::string _root_path;
    // keep order of _db && _handles, we need destroy _handles before _db
    std::unique_ptr<rocksdb::DB, std::function<void(rocksdb::DB*)>> _db;
    std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> _handles;
};

} // namespace doris
