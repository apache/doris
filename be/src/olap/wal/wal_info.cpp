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

#include "olap/wal/wal_info.h"

namespace doris {
WalInfo::WalInfo(int64_t wal_id, std::string wal_path, int64_t retry_num, int64_t start_time_ms)
        : _wal_id(wal_id),
          _wal_path(wal_path),
          _retry_num(retry_num),
          _start_time_ms(start_time_ms) {}

int64_t WalInfo::get_wal_id() {
    return _wal_id;
}

std::string WalInfo::get_wal_path() {
    return _wal_path;
}

int32_t WalInfo::get_retry_num() {
    return _retry_num;
}

int64_t WalInfo::get_start_time_ms() {
    return _start_time_ms;
}

void WalInfo::add_retry_num() {
    _retry_num++;
}
} // namespace doris