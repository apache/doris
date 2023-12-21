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

#pragma once

#include <stdint.h>

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "../common/factory_creator.h"
#include "runtime_profile.h"
#include "util/thrift_util.h"

namespace rocksdb {
class ColumnFamilyHandle;
class DBWithTTL;
} // namespace rocksdb

namespace doris {
class TRuntimeProfileTree;
class Status;

class ProfileRecorder {
    ENABLE_FACTORY_CREATOR(ProfileRecorder);

public:
    ProfileRecorder(const std::string& root_path);

    virtual ~ProfileRecorder();

    Status init();

    Status put_profile(const std::string& key, const TRuntimeProfileTree& value);

private:
    Status put(const std::string& key, const std::string& value);

    Status get(const std::string& key, std::map<std::string, std::string>& profile_records);

    Status get_batch(const std::vector<std::string>& keys,
                     std::map<std::string, std::string>& profile_records);

    std::string _root_path;
    rocksdb::DBWithTTL* _db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> _handles;

    std::atomic<int64_t> _last_compaction_time;

    enum ColumnFamilyIndex {
        DEFAULT_COLUMN_FAMILY_INDEX = 0,
    };

    const std::string DEFAULT_COLUMN_FAMILY = "default";
};

} // namespace doris