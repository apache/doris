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

#include "recycler/meta_checker.h"

#include <curl/curl.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>
#include <mysql/mysql.h>

#include <chrono>
#include <set>

#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"

namespace doris::cloud {

MetaChecker::MetaChecker(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {}

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
    int64_t visible_version;
};

bool MetaChecker::check_fe_meta_by_fdb(MYSQL* conn) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to init txn";
        return false;
    }

    std::string start_key;
    std::string end_key;
    meta_tablet_idx_key({instance_id_, 0}, &start_key);
    meta_tablet_idx_key({instance_id_, std::numeric_limits<int64_t>::max()}, &end_key);
    std::vector<TabletIndexPB> tablet_indexes;

    std::unique_ptr<RangeGetIterator> it;
    do {
        err = txn->get(start_key, end_key, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get tablet idx, ret=" << err;
            return false;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            TabletIndexPB tablet_idx;
            if (!tablet_idx.ParseFromArray(v.data(), v.size())) [[unlikely]] {
                LOG(WARNING) << "malformed tablet index value";
                return false;
            }

            tablet_indexes.push_back(std::move(tablet_idx));
            if (!it->has_next()) start_key = k;
        }
        start_key.push_back('\x00');
    } while (it->more());

    bool check_res = true;
    for (const TabletIndexPB& tablet_idx : tablet_indexes) {
        std::string sql_stmt = "show tablet " + std::to_string(tablet_idx.tablet_id());
        MYSQL_RES* result;
        mysql_query(conn, sql_stmt.c_str());
        result = mysql_store_result(conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (tablet_idx.table_id() != atoll(row[5])) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_idx.ShortDebugString()
                             << " fe table_id: " << atoll(row[5]);
                check_res = false;
            }
            if (tablet_idx.partition_id() != atoll(row[6])) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_idx.ShortDebugString()
                             << " fe partition_id: " << atoll(row[6]);
                check_res = false;
            }
            if (tablet_idx.index_id() != atoll(row[7])) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_idx.ShortDebugString()
                             << " fe index_id: " << atoll(row[7]);
                check_res = false;
            }
        }
        mysql_free_result(result);
        stat_info_.check_fe_tablet_num++;
    }
    LOG(INFO) << "check_fe_tablet_num: " << stat_info_.check_fe_tablet_num;

    return check_res;
}

bool MetaChecker::check_fdb_by_fe_meta(MYSQL* conn) {
    // get db info from FE
    std::string sql_stmt = "show databases";
    MYSQL_RES* result;
    mysql_query(conn, sql_stmt.c_str());
    result = mysql_store_result(conn);
    std::map<std::string, std::vector<std::string>*> db_to_tables;
    if (result) {
        int num_row = mysql_num_rows(result);
        for (int i = 0; i < num_row; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (strcmp(row[0], "__internal_schema") == 0 ||
                strcmp(row[0], "information_schema") == 0) {
                continue;
            }
            db_to_tables.insert({row[0], new std::vector<std::string>()});
        }
    }
    mysql_free_result(result);

    // get tables info from FE
    for (const auto& elem : db_to_tables) {
        std::string sql_stmt = "show tables from " + elem.first;
        mysql_query(conn, sql_stmt.c_str());
        result = mysql_store_result(conn);
        if (result) {
            int num_row = mysql_num_rows(result);
            for (int i = 0; i < num_row; ++i) {
                MYSQL_ROW row = mysql_fetch_row(result);
                elem.second->push_back(row[0]);
            }
        }
        mysql_free_result(result);
    }

    // get tablet info from FE
    std::vector<TabletInfo> tablets;
    for (const auto& elem : db_to_tables) {
        for (const std::string& table : *elem.second) {
            std::string sql_stmt = "show tablets from " + elem.first + "." + table;
            mysql_query(conn, sql_stmt.c_str());
            result = mysql_store_result(conn);
            if (result) {
                int num_row = mysql_num_rows(result);
                for (int i = 0; i < num_row; ++i) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    TabletInfo tablet_info = {0};
                    tablet_info.tablet_id = atoll(row[0]);
                    tablet_info.schema_version = atoll(row[4]);
                    tablets.push_back(std::move(tablet_info));
                }
            }
            mysql_free_result(result);
        }
    }

    // get tablet info from FE
    // get Partition info from FE
    std::map<int64_t, PartitionInfo> partitions;
    for (auto& tablet_info : tablets) {
        std::string sql_stmt = "show tablet " + std::to_string(tablet_info.tablet_id);
        mysql_query(conn, sql_stmt.c_str());
        result = mysql_store_result(conn);
        if (result) {
            int num_row = mysql_num_rows(result);
            for (int i = 0; i < num_row; ++i) {
                MYSQL_ROW row = mysql_fetch_row(result);
                tablet_info.db_id = atoll(row[4]);
                tablet_info.table_id = atoll(row[5]);
                tablet_info.partition_id = atoll(row[6]);
                tablet_info.index_id = atoll(row[7]);

                PartitionInfo partition_info = {0};
                partition_info.db_id = atoll(row[4]);
                partition_info.table_id = atoll(row[5]);
                partition_info.partition_id = atoll(row[6]);
                partitions.insert({partition_info.partition_id, std::move(partition_info)});
            }
        }
        mysql_free_result(result);
    }

    // get partition version from FE
    for (const auto& elem : db_to_tables) {
        for (const std::string& table : *elem.second) {
            std::string sql_stmt = "show partitions from " + elem.first + "." + table;
            mysql_query(conn, sql_stmt.c_str());
            result = mysql_store_result(conn);
            if (result) {
                int num_row = mysql_num_rows(result);
                for (int i = 0; i < num_row; ++i) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    int partition_id = atoll(row[0]);
                    int visible_version = atoll(row[2]);
                    partitions[partition_id].visible_version = visible_version;
                }
            }
            mysql_free_result(result);
        }
    }

    // check tablet idx
    for (const auto& tablet_info : tablets) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            return false;
        }

        std::string key, val;
        meta_tablet_idx_key({instance_id_, tablet_info.tablet_id}, &key);
        err = txn->get(key, &val);
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                LOG(WARNING) << "tablet not found, tablet id: " << tablet_info.tablet_id;
                return false;
            } else {
                LOG(WARNING) << "failed to get tablet_idx, err: " << err
                             << " tablet id: " << tablet_info.tablet_id;
                return false;
            }
        }

        TabletIndexPB tablet_idx;
        if (!tablet_idx.ParseFromString(val)) [[unlikely]] {
            LOG(WARNING) << "malformed tablet index value";
            return false;
        }

        /*
        if (tablet_info.db_id != tablet_idx.db_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe db id: " << tablet_info.db_id
                         << " tablet idx db id: " << tablet_idx.db_id();
            return false;
        }
        */

        if (tablet_info.table_id != tablet_idx.table_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe table id: " << tablet_info.table_id
                         << " tablet idx table id: " << tablet_idx.table_id();
            return false;
        }

        if (tablet_info.partition_id != tablet_idx.partition_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe part id: " << tablet_info.partition_id
                         << " tablet idx part id: " << tablet_idx.partition_id();
            return false;
        }

        if (tablet_info.index_id != tablet_idx.index_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe index id: " << tablet_info.index_id
                         << " tablet idx index id: " << tablet_idx.index_id();
            return false;
        }

        if (tablet_info.tablet_id != tablet_idx.tablet_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe tablet id: " << tablet_info.tablet_id
                         << " tablet idx tablet id: " << tablet_idx.tablet_id();
            return false;
        }

        stat_info_.check_fdb_tablet_idx_num++;
    }

    // check tablet meta
    for (const auto& tablet_info : tablets) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            return false;
        }

        MetaTabletKeyInfo key_info1 {instance_id_, tablet_info.table_id, tablet_info.index_id,
                                     tablet_info.partition_id, tablet_info.tablet_id};
        std::string key, val;
        meta_tablet_key(key_info1, &key);
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "tablet meta not found: " << tablet_info.tablet_id;
            return false;
        } else if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
            LOG(WARNING) << "failed to get tablet, err: " << err;
            return false;
        }
        stat_info_.check_fdb_tablet_meta_num++;
    }

    // check tablet schema
    /*
    for (const auto& tablet_info : tablets) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            return false;
        }

        std::string schema_key, schema_val;
        meta_schema_key({instance_id_, tablet_info.index_id, tablet_info.schema_version},
                        &schema_key);
        ValueBuf val_buf;
        err = cloud::get(txn.get(), schema_key, &val_buf);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "tablet schema not found: " << tablet_info.debug_string();
            return false;
        } else if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
            LOG(WARNING) <<"failed to get tablet schema, err: " << err;
            return false;
        }
    }
    */

    // check partition
    for (const auto& elem : partitions) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            return false;
        }
        if (elem.second.visible_version == 0 || elem.second.visible_version == 1) {
            continue;
        }

        int64_t db_id = elem.second.db_id;
        int64_t table_id = elem.second.table_id;
        int64_t partition_id = elem.second.partition_id;
        std::string ver_key = partition_version_key({instance_id_, db_id, table_id, partition_id});
        std::string ver_val;
        err = txn->get(ver_key, &ver_val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "version key not found, partition id: " << partition_id;
            return false;
        } else if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get version: " << partition_id;
            return false;
        }

        VersionPB version_pb;
        if (!version_pb.ParseFromString(ver_val)) {
            LOG(WARNING) << "malformed version value";
            return false;
        }

        if (version_pb.version() != elem.second.visible_version) {
            LOG(WARNING) << "partition version check failed, FE partition version"
                         << elem.second.visible_version << " ms version: " << version_pb.version();
            return false;
        }
        stat_info_.check_fdb_partition_version_num++;
    }

    LOG(INFO) << "check_fdb_table_idx_num: " << stat_info_.check_fdb_tablet_idx_num
              << " check_fdb_table_meta_num: " << stat_info_.check_fdb_tablet_meta_num
              << " check_fdb_partition_version_num: " << stat_info_.check_fdb_partition_version_num;

    return true;
}

void MetaChecker::do_check(const std::string& host, const std::string& port,
                           const std::string& user, const std::string& password,
                           const std::string& instance_id, std::string& msg) {
    LOG(INFO) << "meta check begin";
    instance_id_ = instance_id;

    MYSQL conn;
    mysql_init(&conn);
    mysql_ssl_mode ssl_mode = SSL_MODE_DISABLED;
    mysql_options(&conn, MYSQL_OPT_SSL_MODE, (void*)&ssl_mode);
    if (!mysql_real_connect(&conn, host.c_str(), user.c_str(), password.c_str(), "", stol(port),
                            nullptr, 0)) {
        msg = "mysql conn failed ";
        LOG(WARNING) << msg << mysql_error(&conn) << " host " << host << " port " << port
                     << " user " << user << " password " << password << " instance_id "
                     << instance_id;
        return;
    }

    LOG(INFO) << "mysql conn succ ";

    using namespace std::chrono;
    int64_t start = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    int64_t now;

    LOG(INFO) << "check_fe_meta_by_fdb begin";
    bool ret = false;
    do {
        ret = check_fe_meta_by_fdb(&conn);
        if (!ret) {
            std::this_thread::sleep_for(seconds(10));
        }
        now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    } while (now - start <= 180 && !ret);

    if (!ret) {
        LOG(WARNING) << "check_fe_meta_by_fdb failed, there may be data leak";
        msg = "meta leak err";
    }
    now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    LOG(INFO) << "check_fe_meta_by_fdb finish, cost(second): " << now - start;

    ret = check_fdb_by_fe_meta(&conn);
    if (!ret) {
        LOG(WARNING) << "check_fdb_by_fe_meta failed, there may be data loss";
        msg = "meta loss err";
        return;
    }
    now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    LOG(INFO) << "check_fdb_by_fe_meta finish, cost(second): " << now - start;

    mysql_close(&conn);

    LOG(INFO) << "meta check finish";
}

} // namespace doris::cloud
