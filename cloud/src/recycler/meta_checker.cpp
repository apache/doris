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
#include <set>
#include <tuple>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"

namespace doris::cloud {

MetaChecker::MetaChecker(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {}

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

bool MetaChecker::do_meta_tablet_key_check(MYSQL* conn) {
    std::vector<doris::TabletMetaCloudPB> tablets_meta;
    bool check_res = true;

    // scan and collect tablet_meta
    std::string start_key;
    std::string end_key;
    meta_tablet_key({instance_id_, 0, 0, 0, 0}, &start_key);
    meta_tablet_key({instance_id_, INT64_MAX, 0, 0, 0}, &end_key);
    scan_and_handle_kv(start_key, end_key,
                       [&tablets_meta](std::string_view key, std::string_view value) -> int {
                           doris::TabletMetaCloudPB tablet_meta;
                           if (!tablet_meta.ParseFromArray(value.data(), value.size())) {
                               LOG(WARNING) << "malformed tablet meta value";
                               return -1;
                           }
                           tablets_meta.push_back(std::move(tablet_meta));
                           return 0;
                       });

    for (const auto& tablet_meta : tablets_meta) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            continue;
        }

        // get tablet_index to search tablet belongs which db
        std::string tablet_index_key;
        std::string tablet_index_val;
        meta_tablet_idx_key({instance_id_, tablet_meta.tablet_id()}, &tablet_index_key);
        err = txn->get(tablet_index_key, &tablet_index_val);
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                LOG(WARNING) << "tablet_idx not found, tablet id: " << tablet_meta.tablet_id();
                continue;
            } else {
                LOG(WARNING) << "failed to get tablet_idx, err: " << err
                             << " tablet id: " << tablet_meta.tablet_id();
                continue;
            }
        }

        TabletIndexPB tablet_index_meta;
        tablet_index_meta.ParseFromString(tablet_index_val);

        if (!db_meta_.contains(tablet_index_meta.db_id())) {
            LOG(WARNING) << "tablet_idx.db_id not found in fe meta, db_id = "
                         << tablet_index_meta.db_id()
                         << "tablet index meta: " << tablet_index_meta.DebugString();
            continue;
        }
        std::string db_name = db_meta_.at(tablet_index_meta.db_id());
        if (db_name == "__internal_schema" || db_name == "information_schema" ||
            db_name == "mysql") {
            continue;
        }

        if (mysql_select_db(conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(conn);
            continue;
        }
        MYSQL_RES* result;
        std::string sql_stmt = "show tablet " + std::to_string(tablet_meta.tablet_id());
        mysql_query(conn, sql_stmt.c_str());

        result = mysql_store_result(conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            auto [db_id, table_id, partition_id, index_id] =
                    std::make_tuple(atoll(row[4]), atoll(row[5]), atoll(row[6]), atoll(row[7]));
            if (tablet_meta.table_id() != table_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_meta.ShortDebugString()
                             << " fe table_id: " << atoll(row[5]);
                check_res = false;
            }
            if (tablet_meta.partition_id() != partition_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_meta.ShortDebugString()
                             << " fe partition_id: " << atoll(row[6]);
                check_res = false;
            }
            if (tablet_meta.index_id() != index_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_meta.ShortDebugString()
                             << " fe index_id: " << atoll(row[7]);
                check_res = false;
            }
            mysql_free_result(result);
        } else {
            LOG(WARNING) << "check failed, fdb meta: " << tablet_meta.ShortDebugString()
                         << " fe tablet not found";
            check_res = false;
        }
        stat_info_.check_fe_tablet_num++;
    }

    return check_res;
}

bool MetaChecker::do_meta_tablet_key_index_check(MYSQL* conn) {
    std::vector<TabletIndexPB> tablet_indexes;
    bool check_res = true;

    // scan and collect tablet_idx
    std::string start_key;
    std::string end_key;
    meta_tablet_idx_key({instance_id_, 0}, &start_key);
    meta_tablet_idx_key({instance_id_, INT64_MAX}, &end_key);
    scan_and_handle_kv(start_key, end_key,
                       [&tablet_indexes](std::string_view key, std::string_view value) -> int {
                           TabletIndexPB tablet_idx;
                           if (!tablet_idx.ParseFromArray(value.data(), value.size())) {
                               LOG(WARNING) << "malformed tablet index value";
                               return -1;
                           }
                           tablet_indexes.push_back(std::move(tablet_idx));
                           return 0;
                       });

    for (const TabletIndexPB& tablet_idx : tablet_indexes) {
        if (!db_meta_.contains(tablet_idx.db_id())) {
            LOG(WARNING) << "tablet_idx.db_id not found in fe meta, db_id = " << tablet_idx.db_id();
            continue;
        }
        std::string sql_stmt = "show tablet " + std::to_string(tablet_idx.tablet_id());
        MYSQL_RES* result;
        std::string db_name = db_meta_.at(tablet_idx.db_id());
        if (db_name == "__internal_schema" || db_name == "information_schema" ||
            db_name == "mysql") {
            continue;
        }
        if (mysql_select_db(conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(conn);
            continue;
        }
        mysql_query(conn, sql_stmt.c_str());
        result = mysql_store_result(conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            auto [db_id, table_id, partition_id, index_id] =
                    std::make_tuple(atoll(row[4]), atoll(row[5]), atoll(row[6]), atoll(row[7]));
            if (tablet_idx.db_id() != db_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_idx.ShortDebugString()
                             << " fe db_id: " << atoll(row[4]);
                check_res = false;
            }
            if (tablet_idx.table_id() != table_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_idx.ShortDebugString()
                             << " fe table_id: " << atoll(row[5]);
                check_res = false;
            }
            if (tablet_idx.partition_id() != partition_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_idx.ShortDebugString()
                             << " fe partition_id: " << atoll(row[6]);
                check_res = false;
            }
            if (tablet_idx.index_id() != index_id) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_idx.ShortDebugString()
                             << " fe index_id: " << atoll(row[7]);
                check_res = false;
            }
            mysql_free_result(result);
        } else {
            LOG(WARNING) << "check failed, fdb meta: " << tablet_idx.ShortDebugString()
                         << " fe tablet not found";
            check_res = false;
        }
        stat_info_.check_fe_tablet_num++;
    }
    LOG(INFO) << "check_fe_tablet_num: " << stat_info_.check_fe_tablet_num;

    return check_res;
}

bool MetaChecker::do_meta_schema_key_check(MYSQL* conn) {
    std::vector<doris::TabletMetaCloudPB> tablets_meta;
    bool check_res = true;

    // scan and collect tablet_meta
    std::string start_key;
    std::string end_key;
    meta_tablet_key({instance_id_, 0, 0, 0, 0}, &start_key);
    meta_tablet_key({instance_id_, INT64_MAX, 0, 0, 0}, &end_key);
    scan_and_handle_kv(start_key, end_key,
                       [&tablets_meta](std::string_view key, std::string_view value) -> int {
                           doris::TabletMetaCloudPB tablet_meta;
                           if (!tablet_meta.ParseFromArray(value.data(), value.size())) {
                               LOG(WARNING) << "malformed tablet meta value";
                               return -1;
                           }
                           tablets_meta.push_back(std::move(tablet_meta));
                           return 0;
                       });

    for (const auto& tablet_meta : tablets_meta) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn";
            continue;
        }

        // get tablet_index to search tablet belongs which db
        std::string tablet_index_key;
        std::string tablet_index_val;
        meta_tablet_idx_key({instance_id_, tablet_meta.tablet_id()}, &tablet_index_key);
        err = txn->get(tablet_index_key, &tablet_index_val);
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                LOG(WARNING) << "tablet_idx not found, tablet id: " << tablet_meta.tablet_id();
                continue;
            } else {
                LOG(WARNING) << "failed to get tablet_idx, err: " << err
                             << " tablet id: " << tablet_meta.tablet_id();
                continue;
            }
        }

        TabletIndexPB tablet_index_meta;
        tablet_index_meta.ParseFromString(tablet_index_val);

        if (!db_meta_.contains(tablet_index_meta.db_id())) {
            LOG(WARNING) << "tablet_idx.db_id not found in fe meta, db_id = "
                         << tablet_index_meta.db_id()
                         << "tablet index meta: " << tablet_index_meta.DebugString();
            continue;
        }
        std::string db_name = db_meta_.at(tablet_index_meta.db_id());
        if (db_name == "__internal_schema" || db_name == "information_schema" ||
            db_name == "mysql") {
            continue;
        }

        if (mysql_select_db(conn, db_name.c_str())) {
            LOG(WARNING) << "mysql select db error, db_name: " << db_name
                         << " error: " << mysql_error(conn);
            continue;
        }
        std::string tablet_schema_key;
        std::string tablet_schema_val;
        meta_schema_key({instance_id_, tablet_index_meta.index_id(), tablet_meta.schema_version()},
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
        std::string sql_stmt =
                fmt::format("SHOW PROC '/dbs/{}/{}/index_schema/{}'", tablet_index_meta.db_id(),
                            tablet_meta.table_id(), tablet_meta.index_id());
        mysql_query(conn, sql_stmt.c_str());

        result = mysql_store_result(conn);
        if (result) {
            MYSQL_ROW row = mysql_fetch_row(result);
            int64_t schema_version = atoll(row[2]);
            if (tablet_schema.schema_version() != schema_version) {
                LOG(WARNING) << "check failed, fdb meta: " << tablet_schema.ShortDebugString()
                             << " fe schema_version: " << schema_version;
                check_res = false;
            }
            mysql_free_result(result);
        } else {
            LOG(WARNING) << "check failed, fdb meta: " << tablet_meta.ShortDebugString()
                         << " fe tablet not found";
            check_res = false;
        }
        stat_info_.check_fe_tablet_num++;
    }

    return check_res;
}

template <>
bool MetaChecker::handle_check_fe_meta_by_fdb<CHECK_META>(MYSQL* conn) {
    bool check_res = true;
    // check MetaTabletIdxKey
    if (!do_meta_tablet_key_index_check(conn)) {
        check_res = false;
        LOG(WARNING) << "do_meta_tablet_key_index_check failed";
    } else {
        LOG(INFO) << "do_meta_tablet_key_index_check success";
    }

    // check MetaTabletKey
    if (!do_meta_tablet_key_check(conn)) {
        check_res = false;
        LOG(WARNING) << "do_meta_tablet_key_check failed";
    } else {
        LOG(INFO) << "do_meta_tablet_key_check success";
    }

    // check MetaSchemaKey
    if (!do_meta_schema_key_check(conn)) {
        check_res = false;
        LOG(WARNING) << "do_meta_schema_key_check failed";
    } else {
        LOG(INFO) << "do_meta_schema_key_check success";
    }
    return check_res;
}

bool MetaChecker::check_fe_meta_by_fdb(MYSQL* conn) {
    bool success = true;
    if (config::enable_checker_for_meta_key_check) {
        success = handle_check_fe_meta_by_fdb<CHECK_META>(conn);
    }

    // TODO(wyxxxcat) add check for version key
    // if (config::enable_checker_for_version_key_check) {
    //     success = handle_check_fe_meta_by_fdb<CHECK_VERSION>(conn);
    // }
    return success;
}

bool MetaChecker::do_meta_tablet_index_key_inverted_check(MYSQL* conn,
                                                          const std::vector<TabletInfo>& tablets) {
    bool check_res = true;
    // check tablet idx
    for (const auto& tablet_info : tablets) {
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

bool MetaChecker::do_meta_tablet_key_inverted_check(MYSQL* conn, std::vector<TabletInfo>& tablets,
                                                    std::map<int64_t, PartitionInfo>& partitions) {
    bool check_res = true;
    // check tablet meta
    for (const auto& tablet_info : tablets) {
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

    // TODO(wyxxxcat):
    // separate from this function to check partition version function
    // for (const auto& elem : partitions) {
    //     std::unique_ptr<Transaction> txn;
    //     TxnErrorCode err = txn_kv_->create_txn(&txn);
    //     if (err != TxnErrorCode::TXN_OK) {
    //         LOG(WARNING) << "failed to init txn";
    //         continue;
    //     }
    //     if (elem.second.visible_version == 0 || elem.second.visible_version == 1) {
    //         continue;
    //     }

    //     int64_t db_id = elem.second.db_id;
    //     int64_t table_id = elem.second.table_id;
    //     int64_t partition_id = elem.second.partition_id;
    //     int64_t tablet_id = elem.second.tablet_id;
    //     std::string ver_key = partition_version_key({instance_id_, db_id, table_id, partition_id});
    //     std::string ver_val;
    //     err = txn->get(ver_key, &ver_val);
    //     if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
    //         LOG_WARNING("version key not found.")
    //                 .tag("db id", db_id)
    //                 .tag("table id", table_id)
    //                 .tag("partition id", partition_id)
    //                 .tag("tablet id", tablet_id);
    //         check_res = false;
    //         continue;
    //     } else if (err != TxnErrorCode::TXN_OK) {
    //         LOG_WARNING("failed to get version.")
    //                 .tag("db id", db_id)
    //                 .tag("table id", table_id)
    //                 .tag("partition id", partition_id)
    //                 .tag("tablet id", tablet_id);
    //         check_res = false;
    //         continue;
    //     }

    //     VersionPB version_pb;
    //     if (!version_pb.ParseFromString(ver_val)) {
    //         LOG(WARNING) << "malformed version value";
    //         check_res = false;
    //         continue;
    //     }

    //     if (version_pb.version() != elem.second.visible_version) {
    //         LOG(WARNING) << "partition version check failed, FE partition version"
    //                      << elem.second.visible_version << " ms version: " << version_pb.version();
    //         check_res = false;
    //         continue;
    //     }
    //     stat_info_.check_fdb_partition_version_num++;
    // }
    return check_res;
}

bool MetaChecker::do_meta_schema_key_inverted_check(MYSQL* conn, std::vector<TabletInfo>& tablets,
                                                    std::map<int64_t, PartitionInfo>& partitions) {
    bool check_res = true;

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
bool MetaChecker::handle_check_fdb_by_fe_meta<CHECK_META>(MYSQL* conn) {
    std::vector<TabletInfo> tablets;
    std::map<int64_t, PartitionInfo> partitions;

    init_tablet_info_from_fe_meta(conn, tablets, partitions);

    bool check_res = true;
    // check MetaTabletIdxKey
    if (!do_meta_tablet_index_key_inverted_check(conn, tablets)) {
        check_res = false;
        LOG(WARNING) << "do_meta_tablet_index_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_meta_tablet_index_key_inverted_check success";
    }

    // check MetaTabletKey
    if (!do_meta_tablet_key_inverted_check(conn, tablets, partitions)) {
        check_res = false;
        LOG(WARNING) << "do_meta_tablet_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_meta_tablet_key_inverted_check success";
    }

    // check MetaSchemaKey
    if (!do_meta_schema_key_inverted_check(conn, tablets, partitions)) {
        check_res = false;
        LOG(WARNING) << "do_meta_schema_key_inverted_check failed";
    } else {
        LOG(INFO) << "do_meta_schema_key_inverted_check success";
    }

    return check_res;
}

bool MetaChecker::check_fdb_by_fe_meta(MYSQL* conn) {
    bool success = true;
    if (config::enable_checker_for_meta_key_check) {
        success = handle_check_fdb_by_fe_meta<CHECK_META>(conn);
    }

    // TODO(wyxxxcat) add check for version key
    // if (config::enable_checker_for_version_key_check) {
    //     success = handle_check_fdb_by_fe_meta<CHECK_VERSION>(conn);
    // }

    LOG(INFO) << "check_fdb_table_idx_num: " << stat_info_.check_fdb_tablet_idx_num
              << " check_fdb_table_meta_num: " << stat_info_.check_fdb_tablet_meta_num
              << " check_fdb_tablet_schema_num: " << stat_info_.check_fdb_tablet_schema_num
              << " check_fdb_partition_version_num: " << stat_info_.check_fdb_partition_version_num;
    return success;
}

void MetaChecker::init_db_meta(MYSQL* conn) {
    // init db_meta_ -> map<db_id, db_name>
    db_meta_.clear();
    std::string sql_stmt = "SHOW PROC '/dbs/'";
    MYSQL_RES* result;
    mysql_query(conn, sql_stmt.c_str());
    result = mysql_store_result(conn);
    if (result) {
        int num_row = mysql_num_rows(result);
        for (int i = 0; i < num_row; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            auto [db_id, db_name] = std::make_tuple(atoll(row[0]), row[1]);
            db_meta_.insert({db_id, db_name});
        }
        mysql_free_result(result);
    }
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
        init_db_meta(&conn);
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

    LOG(INFO) << "check_fdb_by_fe_meta begin";
    init_db_meta(&conn);
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

void MetaChecker::init_tablet_info_from_fe_meta(MYSQL* conn, std::vector<TabletInfo>& tablets,
                                                std::map<int64_t, PartitionInfo>& partitions) {
    // init tablet info, partition info
    std::map<std::string, std::vector<std::string>> db_to_tables;
    std::string sql_stmt = "show databases";
    MYSQL_RES* result;

    mysql_query(conn, sql_stmt.c_str());
    result = mysql_store_result(conn);
    if (result) {
        int num_row = mysql_num_rows(result);
        for (int i = 0; i < num_row; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            if (strcmp(row[0], "__internal_schema") == 0 ||
                strcmp(row[0], "information_schema") == 0 || strcmp(row[0], "mysql")) {
                continue;
            }
            db_to_tables.insert({row[0], std::vector<std::string>()});
        }
        mysql_free_result(result);
    }

    // get tables info from FE
    for (auto& elem : db_to_tables) {
        std::string sql_stmt = "show tables from " + elem.first;
        mysql_query(conn, sql_stmt.c_str());
        result = mysql_store_result(conn);
        if (result) {
            int num_row = mysql_num_rows(result);
            for (int i = 0; i < num_row; ++i) {
                MYSQL_ROW row = mysql_fetch_row(result);
                elem.second.emplace_back(row[0]);
            }
            mysql_free_result(result);
        }
    }

    // get tablet info from FE
    for (const auto& elem : db_to_tables) {
        for (const std::string& table : elem.second) {
            std::string sql_stmt = "show tablets from " + elem.first + "." + table;
            mysql_query(conn, sql_stmt.c_str());
            result = mysql_store_result(conn);
            if (result) {
                int num_row = mysql_num_rows(result);
                for (int i = 0; i < num_row; ++i) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    TabletInfo tablet_info;
                    tablet_info.tablet_id = atoll(row[0]);
                    VLOG_DEBUG << "get tablet info log"
                               << ", db name" << elem.first << ", table name" << table
                               << ",tablet id" << tablet_info.tablet_id;
                    tablet_info.schema_version = atoll(row[4]);
                    tablets.push_back(tablet_info);
                }
                mysql_free_result(result);
            }
        }
    }

    // get tablet info from FE
    // get Partition info from FE
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

    // get partition version from FE
    for (const auto& elem : db_to_tables) {
        for (const std::string& table : elem.second) {
            std::string sql_stmt = "show partitions from " + elem.first + "." + table;
            mysql_query(conn, sql_stmt.c_str());
            result = mysql_store_result(conn);
            if (result) {
                int num_row = mysql_num_rows(result);
                for (int i = 0; i < num_row; ++i) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    int64_t partition_id = atoll(row[0]);
                    int64_t visible_version = atoll(row[2]);
                    partitions[partition_id].visible_version = visible_version;
                    VLOG_DEBUG << "get partition version log"
                               << ", db name" << elem.first << ", table name" << table
                               << ", raw partition id" << row[0] << ", first partition id"
                               << partition_id << ", db id" << partitions[partition_id].db_id
                               << ", table id" << partitions[partition_id].table_id
                               << ", second partition id" << partitions[partition_id].partition_id
                               << ", tablet id" << partitions[partition_id].tablet_id;
                }
                mysql_free_result(result);
            }
        }
    }
}

} // namespace doris::cloud
