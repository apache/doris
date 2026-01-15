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
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <mysql/mysql.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <tuple>

#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"

namespace doris::cloud {

MetaChecker::MetaChecker(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {
    snapshot_manager_ = std::make_shared<SnapshotManager>(txn_kv_);
}

bool MetaChecker::scan_and_handle_kv(
        std::string& start_key, const std::string& end_key,
        std::function<int(std::string_view, std::string_view)> handle_kv) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to init txn";
        return false;
    }
    std::unique_ptr<RangeGetIterator> it;
    do {
        err = txn->get(start_key, end_key, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get tablet idx, ret=" << err;
            return false;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();

            handle_kv(k, v);
            if (!it->has_next()) {
                start_key = k;
            }
        }
        start_key.push_back('\x00');
    } while (it->more());
    return true;
}

bool MetaChecker::do_meta_tablet_key_check(std::vector<TabletInfo>& tablets_info) {
    bool check_res = true;

    for (const auto& tablet_info : tablets_info) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            continue;
        }

        // get tablet_index to search tablet belongs which db
        std::string tablet_index_key;
        std::string tablet_index_val;
        meta_tablet_idx_key({instance_id_, tablet_info.tablet_id}, &tablet_index_key);
        err = txn->get(tablet_index_key, &tablet_index_val);
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                LOG(WARNING) << "tablet_idx not found, tablet id: " << tablet_info.tablet_id;
                continue;
            } else {
                LOG(WARNING) << "failed to get tablet_idx, err: " << err
                             << " tablet id: " << tablet_info.tablet_id;
                continue;
            }
        }

        TabletIndexPB tablet_index_meta;
        tablet_index_meta.ParseFromString(tablet_index_val);

        if (!db_meta_.contains(tablet_index_meta.db_id())) {
            LOG(WARNING) << "tablet_idx.db_id not found in fe meta, db_id = "
                         << tablet_index_meta.db_id()
                         << "tablet index meta: " << tablet_index_meta.DebugString();
            check_res = false;
            continue;
        }
        std::string db_name = db_meta_.at(tablet_index_meta.db_id());
        if (db_name == "__internal_schema" || db_name == "information_schema" ||
            db_name == "mysql") {
            continue;
        }

        if (mysql_select_db(&conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(&conn);
            continue;
        }
        MYSQL_RES* result;
        std::string sql_stmt = "show tablet " + std::to_string(tablet_info.tablet_id);
        mysql_query(&conn, sql_stmt.c_str());

        result = mysql_store_result(&conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (!row) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe tablet not found";
                check_res = false;
                continue;
            }
            auto [db_id, table_id, partition_id, index_id] =
                    std::make_tuple(atoll(row[4]), atoll(row[5]), atoll(row[6]), atoll(row[7]));
            if (tablet_info.table_id != table_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe table_id: " << atoll(row[5]);
                check_res = false;
            }
            if (tablet_info.partition_id != partition_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe partition_id: " << atoll(row[6]);
                check_res = false;
            }
            if (tablet_info.index_id != index_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe index_id: " << atoll(row[7]);
                check_res = false;
            }
            mysql_free_result(result);
        } else {
            LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                         << " fe tablet not found";
            check_res = false;
        }
        stat_info_.check_fe_tablet_num++;
    }

    return check_res;
}

void MetaChecker::init_partition_info(std::vector<PartitionInfo>* partitions_info) {
    // scan and collect tablet_idx
    std::string start_key;
    std::string end_key;
    partition_version_key({instance_id_, 0, 0, 0}, &start_key);
    partition_version_key({instance_id_, INT64_MAX, 0, 0}, &end_key);
    scan_and_handle_kv(
            start_key, end_key,
            [&partitions_info](std::string_view key, std::string_view value) -> int {
                VersionPB partition_version;
                if (!partition_version.ParseFromArray(value.data(), value.size())) {
                    LOG(WARNING) << "malformed tablet index value";
                    return -1;
                }
                auto k1 = key;
                k1.remove_prefix(1);
                // 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id}
                std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
                decode_key(&k1, &out);
                DCHECK_EQ(out.size(), 6) << key;
                auto db_id = std::get<int64_t>(std::get<0>(out[3]));
                auto table_id = std::get<int64_t>(std::get<0>(out[4]));
                auto partition_id = std::get<int64_t>(std::get<0>(out[5]));
                partitions_info->emplace_back(PartitionInfo {
                        .db_id = db_id, .table_id = table_id, .partition_id = partition_id});
                return 0;
            });
}

void MetaChecker::init_table_info(std::vector<TableInfo>* tables_info) {
    // table id -> version
    std::unordered_map<int64_t, int64_t> fe_tables_info;
    std::string start_key;
    std::string end_key;
    table_version_key({instance_id_, 0, 0}, &start_key);
    table_version_key({instance_id_, INT64_MAX, 0}, &end_key);

    // collect table version from fdb
    scan_and_handle_kv(
            start_key, end_key,
            [&tables_info, this](std::string_view key, std::string_view value) -> int {
                int64_t version = 0;
                std::unique_ptr<Transaction> txn;
                TxnErrorCode err = txn_kv_->create_txn(&txn);
                if (err != TxnErrorCode::TXN_OK) {
                    LOG(WARNING) << "failed to create txn";
                    return -1;
                }
                if (!txn->decode_atomic_int(value, &version)) {
                    LOG(WARNING) << "malformed table version value";
                    return -1;
                }
                auto k1 = key;
                k1.remove_prefix(1);
                // 0x01 "version" ${instance_id} "table" ${db_id} ${tbl_id} -> ${version}
                std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
                decode_key(&k1, &out);
                DCHECK_EQ(out.size(), 5) << key;
                auto db_id = std::get<int64_t>(std::get<0>(out[3]));
                auto table_id = std::get<int64_t>(std::get<0>(out[4]));
                tables_info->emplace_back(TableInfo {.db_id = db_id, .table_id = table_id});
                return 0;
            });
}

void MetaChecker::init_tablet_index_info(std::vector<TabletInfo>* tablets_info) {
    // scan and collect tablet_idx
    std::string start_key;
    std::string end_key;
    meta_tablet_idx_key({instance_id_, 0}, &start_key);
    meta_tablet_idx_key({instance_id_, INT64_MAX}, &end_key);
    scan_and_handle_kv(start_key, end_key,
                       [&tablets_info](std::string_view key, std::string_view value) -> int {
                           TabletIndexPB tablet_idx;
                           if (!tablet_idx.ParseFromArray(value.data(), value.size())) {
                               LOG(WARNING) << "malformed tablet index value";
                               return -1;
                           }
                           tablets_info->push_back(TabletInfo {
                                   .db_id = tablet_idx.db_id(),
                                   .table_id = tablet_idx.table_id(),
                                   .partition_id = tablet_idx.partition_id(),
                                   .index_id = tablet_idx.index_id(),
                                   .tablet_id = tablet_idx.tablet_id(),
                           });
                           return 0;
                       });
}

void MetaChecker::init_tablet_meta_info(std::vector<TabletInfo>* tablets_info) {
    // scan and collect tablet_meta
    tablets_info->clear();
    std::string start_key;
    std::string end_key;
    meta_tablet_key({instance_id_, 0, 0, 0, 0}, &start_key);
    meta_tablet_key({instance_id_, INT64_MAX, 0, 0, 0}, &end_key);
    scan_and_handle_kv(start_key, end_key,
                       [&tablets_info](std::string_view key, std::string_view value) -> int {
                           doris::TabletMetaCloudPB tablet_meta_pb;
                           if (!tablet_meta_pb.ParseFromArray(value.data(), value.size())) {
                               LOG(WARNING) << "malformed tablet meta value";
                               return -1;
                           }
                           tablets_info->push_back(TabletInfo {
                                   .table_id = tablet_meta_pb.table_id(),
                                   .partition_id = tablet_meta_pb.partition_id(),
                                   .index_id = tablet_meta_pb.index_id(),
                                   .tablet_id = tablet_meta_pb.tablet_id(),
                                   .schema_version = tablet_meta_pb.schema_version(),
                           });
                           return 0;
                       });
}

bool MetaChecker::do_meta_tablet_key_index_check(std::vector<TabletInfo>& tablets_info) {
    bool check_res = true;

    for (const TabletInfo& tablet_info : tablets_info) {
        if (!db_meta_.contains(tablet_info.db_id)) {
            LOG(WARNING) << "tablet_idx.db_id not found in fe meta, db_id = " << tablet_info.db_id;
            check_res = false;
            continue;
        }
        std::string sql_stmt = "show tablet " + std::to_string(tablet_info.tablet_id);
        MYSQL_RES* result;
        std::string db_name = db_meta_.at(tablet_info.db_id);
        if (db_name == "__internal_schema" || db_name == "information_schema" ||
            db_name == "mysql") {
            continue;
        }
        if (mysql_select_db(&conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(&conn);
            continue;
        }
        mysql_query(&conn, sql_stmt.c_str());
        result = mysql_store_result(&conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (!row) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe tablet not found";
                check_res = false;
                continue;
            }
            auto [db_id, table_id, partition_id, index_id] =
                    std::make_tuple(atoll(row[4]), atoll(row[5]), atoll(row[6]), atoll(row[7]));
            if (tablet_info.db_id != db_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe db_id: " << atoll(row[4]);
                check_res = false;
            }
            if (tablet_info.table_id != table_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe table_id: " << atoll(row[5]);
                check_res = false;
            }
            if (tablet_info.partition_id != partition_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe partition_id: " << atoll(row[6]);
                check_res = false;
            }
            if (tablet_info.index_id != index_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                             << " fe index_id: " << atoll(row[7]);
                check_res = false;
            }
            mysql_free_result(result);
        } else {
            LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                         << " fe tablet not found";
            check_res = false;
        }
        stat_info_.check_fe_tablet_num++;
    }
    LOG(INFO) << "check_fe_tablet_num: " << stat_info_.check_fe_tablet_num;

    return check_res;
}

bool MetaChecker::do_meta_schema_key_check(std::vector<TabletInfo>& tablets_info) {
    bool check_res = true;

    for (const auto& tablet_info : tablets_info) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            continue;
        }

        // get tablet_index to search tablet belongs which db
        std::string tablet_index_key;
        std::string tablet_index_val;
        meta_tablet_idx_key({instance_id_, tablet_info.tablet_id}, &tablet_index_key);
        err = txn->get(tablet_index_key, &tablet_index_val);
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                LOG(WARNING) << "tablet_idx not found, tablet id: " << tablet_info.tablet_id;
                continue;
            } else {
                LOG(WARNING) << "failed to get tablet_idx, err: " << err
                             << " tablet id: " << tablet_info.tablet_id;
                continue;
            }
        }

        TabletIndexPB tablet_index_meta;
        tablet_index_meta.ParseFromString(tablet_index_val);

        if (!db_meta_.contains(tablet_index_meta.db_id())) {
            LOG(WARNING) << "tablet_idx.db_id not found in fe meta, db_id = "
                         << tablet_index_meta.db_id()
                         << "tablet index meta: " << tablet_index_meta.DebugString();
            check_res = false;
            continue;
        }
        std::string db_name = db_meta_.at(tablet_index_meta.db_id());
        if (db_name == "__internal_schema" || db_name == "information_schema" ||
            db_name == "mysql") {
            continue;
        }

        if (mysql_select_db(&conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(&conn);
            continue;
        }
        std::string tablet_schema_key;
        std::string tablet_schema_val;
        meta_schema_key({instance_id_, tablet_index_meta.index_id(), tablet_info.schema_version},
                        &tablet_schema_key);
        ValueBuf val_buf;
        err = cloud::blob_get(txn.get(), tablet_schema_key, &val_buf);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << fmt::format(
                    "failed to get schema, err={}",
                    err == TxnErrorCode::TXN_KEY_NOT_FOUND ? "not found" : "internal error");
            continue;
        }
        doris::TabletSchemaCloudPB tablet_schema;
        if (!parse_schema_value(val_buf, &tablet_schema)) {
            LOG(WARNING) << fmt::format("malformed schema value, key={}", tablet_schema_key);
            continue;
        }

        MYSQL_RES* result;
        std::string sql_stmt = fmt::format("SHOW PROC '/dbs/{}/{}/index_schema'",
                                           tablet_index_meta.db_id(), tablet_info.table_id);
        mysql_query(&conn, sql_stmt.c_str());

        result = mysql_store_result(&conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (!row) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_schema.ShortDebugString()
                             << " fe tablet schema not found";
                check_res = false;
                continue;
            }
            int64_t schema_version = atoll(row[2]);
            if (tablet_schema.schema_version() != schema_version) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_schema.ShortDebugString()
                             << " fe schema_version: " << schema_version;
                check_res = false;
            }
            mysql_free_result(result);
        } else {
            LOG(WARNING) << "check failed, fdb meta: " << tablet_info.debug_string()
                         << " fe tablet not found";
            check_res = false;
        }
        stat_info_.check_fe_tablet_num++;
    }

    return check_res;
}

bool MetaChecker::do_version_partition_key_check(std::vector<PartitionInfo>& partitions_info) {
    bool check_res = true;

    for (const auto& partition_info : partitions_info) {
        if (!db_meta_.contains(partition_info.db_id)) {
            LOG(WARNING) << "partition_info.db_id not found in fe meta, db_id = "
                         << partition_info.db_id
                         << "partition_info meta: " << partition_info.debug_string();
            check_res = false;
            continue;
        }
        std::string db_name = db_meta_.at(partition_info.db_id);
        if (db_name == "__internal_schema" || db_name == "information_schema" ||
            db_name == "mysql") {
            continue;
        }

        if (mysql_select_db(&conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(&conn);
            continue;
        }
        MYSQL_RES* result;
        std::string sql_stmt = fmt::format("show partition {}", partition_info.partition_id);
        mysql_query(&conn, sql_stmt.c_str());

        result = mysql_store_result(&conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (!row) {
                LOG(WARNING) << "check failed, fdb meta: " << partition_info.debug_string()
                             << " fe partition not found";
                check_res = false;
                continue;
            }
            if (partition_info.table_id != atoll(row[4])) {
                LOG(WARNING) << "check failed, fdb meta: " << partition_info.debug_string()
                             << " fe partition of table_id: " << atoll(row[4]);
                check_res = false;
            } else if (partition_info.db_id != atoll(row[3])) {
                LOG(WARNING) << "check failed, fdb meta: " << partition_info.debug_string()
                             << " fe partition of db_id: " << atoll(row[3]);
                check_res = false;
            }
            mysql_free_result(result);
        } else {
            LOG(WARNING) << "check failed, fdb meta: " << partition_info.debug_string()
                         << " fe partition not found";
            check_res = false;
        }
        stat_info_.check_fe_partition_version_num++;
    }

    return check_res;
}

bool MetaChecker::do_version_table_key_check(std::vector<TableInfo>& tables_info) {
    bool check_res = true;

    // collect table version from fe meta
    for (const auto& table_info : tables_info) {
        if (!db_meta_.contains(table_info.db_id)) {
            LOG(WARNING) << "table_info.db_id not found in fe meta, db_id = " << table_info.db_id
                         << "table_info meta: " << table_info.debug_string();
            check_res = false;
            continue;
        }
        std::string db_name = db_meta_.at(table_info.db_id);
        if (db_name == "__internal_schema" || db_name == "information_schema" ||
            db_name == "mysql") {
            continue;
        }

        if (mysql_select_db(&conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(&conn);
            continue;
        }

        MYSQL_RES* result;
        std::string sql_stmt = fmt::format("show table {}", table_info.table_id);
        mysql_query(&conn, sql_stmt.c_str());

        result = mysql_store_result(&conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (!row) {
                LOG(WARNING) << "check failed, fdb meta: " << table_info.debug_string()
                             << " fe table not found";
                check_res = false;
                continue;
            }
            int64_t db_id = atoll(row[2]);
            if (table_info.db_id != db_id) {
                LOG(WARNING) << "check failed, fdb meta: " << table_info.debug_string()
                             << " fe table of db_id: " << atoll(row[2]);
                check_res = false;
            }
        } else {
            LOG(WARNING) << "check failed, fdb meta: " << table_info.debug_string()
                         << " fe db not found";
            check_res = false;
        }
        stat_info_.check_fe_table_version_num++;
    }

    return check_res;
}

template <>
bool MetaChecker::handle_check_fe_meta_by_fdb<CHECK_VERSION>() {
    bool check_res = true;
    {
        std::vector<PartitionInfo> partitions_info;
        init_partition_info(&partitions_info);
        // check PartitionVersionKey
        if (!do_version_partition_key_check(partitions_info)) {
            check_res = false;
            LOG(WARNING) << "do_version_partition_key_check failed";
        } else {
            LOG(INFO) << "do_version_partition_key_check success";
        }
    }

    {
        std::vector<TableInfo> tables_info;
        init_table_info(&tables_info);
        // check TableVersionKey
        if (!do_version_table_key_check(tables_info)) {
            check_res = false;
            LOG(WARNING) << "do_version_table_key_check failed";
        } else {
            LOG(INFO) << "do_version_table_key_check success";
        }
    }
    return check_res;
}

template <>
bool MetaChecker::handle_check_fe_meta_by_fdb<CHECK_META>() {
    bool check_res = true;

    std::vector<TabletInfo> tablets_info;
    init_tablet_index_info(&tablets_info);
    // check MetaTabletIdxKey inverted
    if (!do_meta_tablet_key_index_check(tablets_info)) {
        check_res = false;
        LOG(WARNING) << "do_meta_tablet_key_index_check failed";
    } else {
        LOG(INFO) << "do_meta_tablet_key_index_check success";
    }

    init_tablet_meta_info(&tablets_info);
    // check MetaTabletKey
    if (!do_meta_tablet_key_check(tablets_info)) {
        check_res = false;
        LOG(WARNING) << "do_meta_tablet_key_check failed";
    } else {
        LOG(INFO) << "do_meta_tablet_key_check success";
    }

    // check MetaSchemaKey
    if (!do_meta_schema_key_check(tablets_info)) {
        check_res = false;
        LOG(WARNING) << "do_meta_schema_key_check failed";
    } else {
        LOG(INFO) << "do_meta_schema_key_check success";
    }

    return check_res;
}

bool MetaChecker::check_fe_meta_by_fdb() {
    bool success = true;
    if (config::enable_meta_key_check) {
        if (!handle_check_fe_meta_by_fdb<CHECK_META>()) {
            success = false;
            LOG(WARNING) << "handle_check_fe_meta_by_fdb<CHECK_META> failed";
        }
    }

    if (config::enable_version_key_check) {
        if (!handle_check_fe_meta_by_fdb<CHECK_VERSION>()) {
            success = false;
            LOG(WARNING) << "handle_check_fe_meta_by_fdb<CHECK_VERSION> failed";
        }
    }
    return success;
}

bool MetaChecker::do_meta_tablet_index_key_inverted_check() {
    bool check_res = true;
    // check tablet idx
    for (const auto& tablet_info : tablets_info) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            continue;
        }

        std::string key, val;
        meta_tablet_idx_key({instance_id_, tablet_info.tablet_id}, &key);
        err = txn->get(key, &val);
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                LOG(WARNING) << "tablet not found, tablet id: " << tablet_info.tablet_id;
                check_res = false;
                continue;
            } else {
                LOG(WARNING) << "failed to get tablet_idx, err: " << err
                             << " tablet id: " << tablet_info.tablet_id;
                check_res = false;
                continue;
            }
        }

        TabletIndexPB tablet_idx;
        if (!tablet_idx.ParseFromString(val)) [[unlikely]] {
            LOG(WARNING) << "malformed tablet index value";
            continue;
        }

        if (tablet_info.db_id != tablet_idx.db_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe db id: " << tablet_info.db_id
                         << " tablet idx db id: " << tablet_idx.db_id();
            check_res = false;
            continue;
        }

        if (tablet_info.table_id != tablet_idx.table_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe table id: " << tablet_info.table_id
                         << " tablet idx table id: " << tablet_idx.table_id();
            check_res = false;
            continue;
        }

        if (tablet_info.partition_id != tablet_idx.partition_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe part id: " << tablet_info.partition_id
                         << " tablet idx part id: " << tablet_idx.partition_id();
            check_res = false;
            continue;
        }

        if (tablet_info.index_id != tablet_idx.index_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe index id: " << tablet_info.index_id
                         << " tablet idx index id: " << tablet_idx.index_id();
            check_res = false;
            continue;
        }

        if (tablet_info.tablet_id != tablet_idx.tablet_id()) [[unlikely]] {
            LOG(WARNING) << "tablet idx check failed, fe tablet id: " << tablet_info.tablet_id
                         << " tablet idx tablet id: " << tablet_idx.tablet_id();
            check_res = false;
            continue;
        }
        stat_info_.check_fdb_tablet_idx_num++;
    }
    return check_res;
}

bool MetaChecker::do_meta_tablet_key_inverted_check() {
    bool check_res = true;
    // check tablet meta
    for (const auto& tablet_info : tablets_info) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            continue;
        }

        MetaTabletKeyInfo key_info1 {instance_id_, tablet_info.table_id, tablet_info.index_id,
                                     tablet_info.partition_id, tablet_info.tablet_id};
        std::string key, val;
        meta_tablet_key(key_info1, &key);
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "tablet meta not found: " << tablet_info.tablet_id;
            check_res = false;
            continue;
        } else if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
            LOG(WARNING) << "failed to get tablet, err: " << err;
            check_res = false;
            continue;
        }
        stat_info_.check_fdb_tablet_meta_num++;
    }

    return check_res;
}

bool MetaChecker::do_meta_schema_key_inverted_check() {
    bool check_res = true;

    for (const auto& tablet_info : tablets_info) {
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
        err = cloud::blob_get(txn.get(), schema_key, &val_buf);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "tablet schema not found: " << tablet_info.debug_string();
            check_res = false;
            continue;
        } else if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
            LOG(WARNING) << "failed to get tablet schema, err: " << err;
            check_res = false;
            continue;
        }
        stat_info_.check_fdb_tablet_schema_num++;
    }
    return check_res;
}

template <>
bool MetaChecker::handle_check_fdb_by_fe_meta<CHECK_META>() {
    bool check_res = true;
    // check MetaTabletIdxKey
    if (!do_meta_tablet_index_key_inverted_check()) {
        check_res = false;
        LOG(WARNING) << "do_meta_tablet_index_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_meta_tablet_index_key_inverted_check success";
    }

    // check MetaTabletKey
    if (!do_meta_tablet_key_inverted_check()) {
        check_res = false;
        LOG(WARNING) << "do_meta_tablet_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_meta_tablet_key_inverted_check success";
    }

    // check MetaSchemaKey
    if (!do_meta_schema_key_inverted_check()) {
        check_res = false;
        LOG(WARNING) << "do_meta_schema_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_meta_schema_key_inverted_check success";
    }

    return check_res;
}

bool MetaChecker::check_fdb_by_fe_meta() {
    bool success = true;
    if (config::enable_meta_key_check) {
        if (!handle_check_fdb_by_fe_meta<CHECK_META>()) {
            success = false;
            LOG(WARNING) << "handle_check_fdb_by_fe_meta<CHECK_META> failed";
        }
    }

    LOG(INFO) << "check_fdb_table_idx_num: " << stat_info_.check_fdb_tablet_idx_num
              << " check_fdb_table_meta_num: " << stat_info_.check_fdb_tablet_meta_num
              << " check_fdb_tablet_schema_num: " << stat_info_.check_fdb_tablet_schema_num
              << " check_fe_table_version_num: " << stat_info_.check_fe_table_version_num
              << " check_fe_partition_version_num: " << stat_info_.check_fe_partition_version_num;
    return success;
}

void MetaChecker::init_db_meta() {
    // init db_meta_ -> map<db_id, db_name>
    db_meta_.clear();
    std::string sql_stmt = "SHOW PROC '/dbs/'";
    MYSQL_RES* result;
    mysql_query(&conn, sql_stmt.c_str());
    result = mysql_store_result(&conn);
    if (result) {
        int num_row = mysql_num_rows(result);
        for (int i = 0; i < num_row; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (!row) {
                continue;
            }
            auto [db_id, db_name] = std::make_tuple(atoll(row[0]), row[1]);
            db_meta_.insert({db_id, db_name});
        }
        mysql_free_result(result);
    }
}

void MetaChecker::init_mysql_connection(const std::string& host, const std::string& port,
                                        const std::string& user, const std::string& password,
                                        const std::string& instance_id, std::string& msg) {
    instance_id_ = instance_id;
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
}

void MetaChecker::do_check(std::string& msg) {
    LOG(INFO) << "meta check begin";

    DORIS_CLOUD_DEFER {
        mysql_close(&conn);
    };

    using namespace std::chrono;
    int64_t start = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    int64_t now;

    LOG(INFO) << "check_fe_meta_by_fdb begin";
    bool ret = false;
    do {
        init_db_meta();
        init_tablet_and_partition_info_from_fe_meta();
        ret = check_fe_meta_by_fdb();
        if (!ret) {
            std::this_thread::sleep_for(seconds(10));
        }
        now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    } while (now - start <= 180 && !ret);

    if (!ret) {
        LOG(WARNING) << "check_fe_meta_by_fdb failed, there may be data leak";
        msg = "meta leak err";
    }

    LOG(INFO) << "check_fe_meta_by_fdb finish, cost(second): " << now - start;

    start = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    LOG(INFO) << "check_fdb_by_fe_meta begin";
    do {
        init_db_meta();
        init_tablet_and_partition_info_from_fe_meta();
        ret = check_fdb_by_fe_meta();
        if (!ret) {
            LOG(WARNING) << "check_fdb_by_fe_meta failed, there may be data loss";
            msg = "meta loss err";
            return;
        }
        now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    } while (now - start <= 180 && !ret);

    LOG(INFO) << "check_fdb_by_fe_meta finish, cost(second): " << now - start;

    if (config::enable_mvcc_meta_check) {
        start = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        LOG(INFO) << "do_mvcc_meta_check begin";
        do {
            init_db_meta();
            init_tablet_and_partition_info_from_fe_meta();
            ret = do_mvcc_check();
            if (!ret) {
                LOG(WARNING) << "do_mvcc_check failed, there may be data loss";
                msg = "meta loss err";
                return;
            }
            now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        } while (now - start <= 180 && !ret);

        LOG(INFO) << "do_mvcc_check finish, cost(second): " << now - start;
    }

    LOG(INFO) << "meta check finish";
}

void MetaChecker::init_tablet_and_partition_info_from_fe_meta() {
    // init tablet info, partition info
    std::map<std::string, std::vector<std::string>> db_to_tables;
    std::string sql_stmt = "show databases";
    MYSQL_RES* result;

    mysql_query(&conn, sql_stmt.c_str());
    result = mysql_store_result(&conn);
    if (result) {
        int num_row = mysql_num_rows(result);
        for (int i = 0; i < num_row; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (!row) {
                continue;
            }
            if (strcmp(row[0], "__internal_schema") == 0 ||
                strcmp(row[0], "information_schema") == 0 || strcmp(row[0], "mysql") == 0) {
                continue;
            }
            db_to_tables.insert({row[0], std::vector<std::string>()});
        }
        mysql_free_result(result);
    }

    // get tables info from FE
    for (auto& elem : db_to_tables) {
        std::string sql_stmt = "show tables from " + elem.first;
        mysql_query(&conn, sql_stmt.c_str());
        result = mysql_store_result(&conn);
        if (result) {
            int num_row = mysql_num_rows(result);
            for (int i = 0; i < num_row; ++i) {
                MYSQL_ROW row = mysql_fetch_row(result);
                if (row) {
                    elem.second.emplace_back(row[0]);
                }
            }
            mysql_free_result(result);
        }
    }

    // get tablet info from FE
    for (const auto& elem : db_to_tables) {
        for (const std::string& table : elem.second) {
            std::string sql_stmt = "show tablets from " + elem.first + "." + table;
            mysql_query(&conn, sql_stmt.c_str());
            result = mysql_store_result(&conn);
            if (result) {
                int num_row = mysql_num_rows(result);
                for (int i = 0; i < num_row; ++i) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    if (!row) {
                        continue;
                    }
                    TabletInfo tablet_info;
                    tablet_info.tablet_id = atoll(row[0]);
                    VLOG_DEBUG << "get tablet info log"
                               << ", db name" << elem.first << ", table name" << table
                               << ",tablet id" << tablet_info.tablet_id;
                    tablets_info.push_back(tablet_info);
                }
                mysql_free_result(result);
            }
        }
    }

    // get tablet info from FE
    // get Partition info from FE
    for (auto& tablet_info : tablets_info) {
        std::string db_name = db_meta_.begin()->second;
        if (mysql_select_db(&conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(&conn);
            continue;
        }
        std::string sql_stmt = "show tablet " + std::to_string(tablet_info.tablet_id);
        mysql_query(&conn, sql_stmt.c_str());
        result = mysql_store_result(&conn);
        if (result) {
            int num_row = mysql_num_rows(result);
            for (int i = 0; i < num_row; ++i) {
                MYSQL_ROW row = mysql_fetch_row(result);
                if (!row) {
                    continue;
                }
                tablet_info.db_id = atoll(row[4]);
                tablet_info.table_id = atoll(row[5]);
                tablet_info.partition_id = atoll(row[6]);
                tablet_info.index_id = atoll(row[7]);

                int schema_version = -1;
                {
                    MYSQL_RES* result;
                    std::string sql_stmt = fmt::format("SHOW PROC '/dbs/{}/{}/index_schema'",
                                                       tablet_info.db_id, tablet_info.table_id);
                    mysql_query(&conn, sql_stmt.c_str());

                    result = mysql_store_result(&conn);
                    if (!result) {
                        continue;
                    }
                    MYSQL_ROW row = mysql_fetch_row(result);
                    if (!row) {
                        continue;
                    }
                    schema_version = atoll(row[2]);
                    mysql_free_result(result);
                }

                tablet_info.schema_version = schema_version;

                PartitionInfo partition_info;
                partition_info.db_id = atoll(row[4]);
                partition_info.table_id = atoll(row[5]);
                partition_info.partition_id = atoll(row[6]);
                partition_info.tablet_id = tablet_info.tablet_id;
                VLOG_DEBUG << "get partition info log"
                           << ", db id" << partition_info.db_id << ", table id"
                           << partition_info.table_id << ", partition id"
                           << partition_info.partition_id << ", tablet id"
                           << partition_info.tablet_id;

                partitions.insert({partition_info.partition_id, partition_info});
            }
            mysql_free_result(result);
        }
    }
}

bool MetaChecker::do_mvcc_check() {
    int ret = snapshot_manager_->check_meta(this);
    if (ret != 0) {
        LOG(INFO) << "do_mvcc_check failed";
        return false;
    }
    return true;
}

} // namespace doris::cloud
