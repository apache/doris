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

#include <map>
#include <string>

#include "rocksdb/utilities/db_ttl.h"

#pragma once

namespace doris {

class Status;

class StreamLoadRecord {
public:
    StreamLoadRecord(const std::string& root_path);

    virtual ~StreamLoadRecord();

    Status init();

    Status put(const std::string& key, const std::string& value);

    Status get_batch(const std::string& start, const int batch_size, std::map<std::string, std::string>* stream_load_records);

private:
    std::string _root_path;
    rocksdb::DBWithTTL* _db;
    std::vector<rocksdb::ColumnFamilyHandle*> _handles;

    int64_t _last_compaction_time;

    enum ColumnFamilyIndex {
        DEFAULT_COLUMN_FAMILY_INDEX = 0,
        STREAM_LOAD_COLUMN_FAMILY_INDEX
    };

    const std::string DEFAULT_COLUMN_FAMILY = "default";
    const std::string STREAM_LOAD_COLUMN_FAMILY = "stream_load";
};

} // namespace doris
