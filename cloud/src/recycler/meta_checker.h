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

#include <gen_cpp/cloud.pb.h>
#include <mysql/mysql.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "recycler/white_black_list.h"

namespace doris::cloud {
class TxnKv;

struct StatInfo {
    int64_t check_fe_tablet_num = 0;
    int64_t check_fdb_tablet_idx_num = 0;
    int64_t check_fdb_tablet_meta_num = 0;
    int64_t check_fdb_partition_version_num = 0;
};

class MetaChecker {
public:
    explicit MetaChecker(std::shared_ptr<TxnKv> txn_kv);
    void do_check(const std::string& host, const std::string& port, const std::string& user,
                  const std::string& password, const std::string& instance_id, std::string& msg);
    bool check_fe_meta_by_fdb(MYSQL* conn);
    bool check_fdb_by_fe_meta(MYSQL* conn);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    StatInfo stat_info_;
    std::string instance_id_;
};

} // namespace doris::cloud
