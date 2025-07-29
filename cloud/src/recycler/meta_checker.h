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

#include "common/logging.h"
#include "recycler/white_black_list.h"

namespace doris::cloud {
class TxnKv;

struct StatInfo {
    // fe
    int64_t check_fe_tablet_num = 0;
    int64_t check_fe_partition_num = 0;
    int64_t check_fe_tablet_schema_num = 0;
    // fdb
    int64_t check_fdb_tablet_idx_num = 0;
    int64_t check_fdb_tablet_meta_num = 0;
    int64_t check_fdb_tablet_schema_num = 0;
    int64_t check_fdb_partition_version_num = 0;
};

enum CHECK_TYPE {
    CHECK_TXN,
    CHECK_VERSION,
    CHECK_META,
    CHECK_STATS,
    CHECK_JOB,
};

struct TabletInfo {
    int64_t db_id;
    int64_t table_id;
    int64_t partition_id;
    int64_t index_id;
    int64_t tablet_id;
    int64_t schema_version;

    std::string debug_string() const {
        return "db id: " + std::to_string(db_id) + " table id: " + std::to_string(table_id) +
               " partition id: " + std::to_string(partition_id) +
               " index id: " + std::to_string(index_id) +
               " tablet id: " + std::to_string(tablet_id) +
               " schema version: " + std::to_string(schema_version);
    }
};

struct PartitionInfo {
    int64_t db_id;
    int64_t table_id;
    int64_t partition_id;
    int64_t tablet_id;
    int64_t visible_version;
};

class MetaChecker {
public:
    explicit MetaChecker(std::shared_ptr<TxnKv> txn_kv);
    void do_check(const std::string& host, const std::string& port, const std::string& user,
                  const std::string& password, const std::string& instance_id, std::string& msg);
    bool check_fe_meta_by_fdb(MYSQL* conn);
    bool check_fdb_by_fe_meta(MYSQL* conn);

    template <CHECK_TYPE>
    bool handle_check_fe_meta_by_fdb(MYSQL* conn);

    template <CHECK_TYPE>
    bool handle_check_fdb_by_fe_meta(MYSQL* conn);

private:
    void init_tablet_info_from_fe_meta(MYSQL* conn, std::vector<TabletInfo>& tablets,
                                       std::map<int64_t, PartitionInfo>& partitions);

    bool scan_and_handle_kv(std::string& start_key, const std::string& end_key,
                            std::function<int(std::string_view, std::string_view)>);

    bool do_meta_tablet_key_index_check(MYSQL* conn);

    bool do_meta_tablet_key_check(MYSQL* conn);

    bool do_meta_schema_key_check(MYSQL* conn);

    bool do_meta_tablet_index_key_inverted_check(MYSQL* conn,
                                                 const std::vector<TabletInfo>& tablets);

    bool do_meta_tablet_key_inverted_check(MYSQL* conn, std::vector<TabletInfo>& tablets,
                                           std::map<int64_t, PartitionInfo>& partitions);

    bool do_meta_schema_key_inverted_check(MYSQL* conn, std::vector<TabletInfo>& tablets,
                                           std::map<int64_t, PartitionInfo>& partitions);

    void init_db_meta(MYSQL* conn);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    StatInfo stat_info_;
    std::string instance_id_;
    // db_id -> db_name
    std::unordered_map<int64_t, std::string> db_meta_;
};

// not implemented yet
template <>
bool MetaChecker::handle_check_fe_meta_by_fdb<CHECK_STATS>(MYSQL* conn) = delete;

// not implemented yet
template <>
bool MetaChecker::handle_check_fe_meta_by_fdb<CHECK_TXN>(MYSQL* conn) = delete;

// not implemented yet
template <>
bool MetaChecker::handle_check_fe_meta_by_fdb<CHECK_VERSION>(MYSQL* conn) = delete;

// not implemented yet
template <>
bool MetaChecker::handle_check_fe_meta_by_fdb<CHECK_JOB>(MYSQL* conn) = delete;

template <>
bool MetaChecker::handle_check_fe_meta_by_fdb<CHECK_META>(MYSQL* conn);

// not implemented yet
template <>
bool MetaChecker::handle_check_fdb_by_fe_meta<CHECK_STATS>(MYSQL* conn) = delete;

// not implemented yet
template <>
bool MetaChecker::handle_check_fdb_by_fe_meta<CHECK_TXN>(MYSQL* conn) = delete;

// not implemented yet
template <>
bool MetaChecker::handle_check_fdb_by_fe_meta<CHECK_VERSION>(MYSQL* conn) = delete;

// not implemented yet
template <>
bool MetaChecker::handle_check_fdb_by_fe_meta<CHECK_JOB>(MYSQL* conn) = delete;

template <>
bool MetaChecker::handle_check_fdb_by_fe_meta<CHECK_META>(MYSQL* conn);

} // namespace doris::cloud
