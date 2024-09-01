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

#include <gen_cpp/cloud.pb.h>

#include <chrono>
#include <cstdint>
#include <limits>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

using namespace std::chrono;

namespace doris::cloud {

struct TableStats {
    int64_t updated_row_count = 0;

    TableStats() = default;

    TableStats(int64_t num_rows) : updated_row_count(num_rows) {}

    std::string to_string() const {
        std::stringstream ss;
        ss << "updated_row_count: " << updated_row_count;
        return ss.str();
    }
};

static void get_pb_from_tablestats(TableStats& stats, TableStatsPB* stats_pb) {
    stats_pb->set_updated_row_count(stats.updated_row_count);
}

static void calc_table_stats(std::unordered_map<int64_t, TabletIndexPB>& tablet_ids,
                             std::unordered_map<int64_t, TabletStats>& tablet_stats,
                             std::map<int64_t, TableStats>& table_stats,
                             std::vector<int64_t> base_tablet_ids) {
    int64_t table_id;

    VLOG_DEBUG << "base_tablet_ids size: " << base_tablet_ids.size();
    for (int64_t tablet_id : base_tablet_ids) {
        auto it = tablet_stats.find(tablet_id);
        if (it == tablet_stats.end()) {
            continue;
        }
        const auto& tablet_stat = it->second;
        table_id = tablet_ids[tablet_id].table_id();
        if (table_stats.find(table_id) == table_stats.end()) {
            table_stats[table_id] = TableStats(tablet_stat.num_rows);
        } else {
            table_stats[table_id].updated_row_count += tablet_stat.num_rows;
        }
    }
}

//TODO: we need move begin/commit etc txn to TxnManager
void MetaServiceImpl::begin_txn(::google::protobuf::RpcController* controller,
                                const BeginTxnRequest* request, BeginTxnResponse* response,
                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(begin_txn);
    if (!request->has_txn_info()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid argument, missing txn info";
        return;
    }

    auto& txn_info = const_cast<TxnInfoPB&>(request->txn_info());
    std::string label = txn_info.has_label() ? txn_info.label() : "";
    int64_t db_id = txn_info.has_db_id() ? txn_info.db_id() : -1;

    if (label.empty() || db_id < 0 || txn_info.table_ids().empty() || !txn_info.has_timeout_ms()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid argument, label=" << label << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " label=" << label;
        msg = ss.str();
        return;
    }

    RPC_RATE_LIMIT(begin_txn)
    //1. Generate version stamp for txn id
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "txn_kv_->create_txn() failed, err=" << err << " label=" << label
           << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    const std::string label_key = txn_label_key({instance_id, db_id, label});
    std::string label_val;
    err = txn->get(label_key, &label_val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "txn->get failed(), err=" << err << " label=" << label;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn->get label_key=" << hex(label_key) << " label=" << label << " err=" << err;

    // err == OK means label has previous txn ids.
    if (err == TxnErrorCode::TXN_OK) {
        label_val = label_val.substr(0, label_val.size() - VERSION_STAMP_LEN);
    }

    //ret > 0, means label not exist previously.
    txn->atomic_set_ver_value(label_key, label_val);
    LOG(INFO) << "txn->atomic_set_ver_value label_key=" << hex(label_key);

    TEST_SYNC_POINT_CALLBACK("begin_txn:before:commit_txn:1", &label);
    err = txn->commit();
    TEST_SYNC_POINT_CALLBACK("begin_txn:after:commit_txn:1", &label);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "txn->commit failed(), label=" << label << " err=" << err;
        msg = ss.str();
        return;
    }
    //2. Get txn id from version stamp
    txn.reset();

    err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn when get txn id, label=" << label << " err=" << err;
        msg = ss.str();
        return;
    }

    label_val.clear();
    err = txn->get(label_key, &label_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "txn->get() failed, label=" << label << " err=" << err;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn->get label_key=" << hex(label_key) << " label=" << label << " err=" << err;

    // Generated by TxnKv system
    int64_t txn_id = 0;
    int ret =
            get_txn_id_from_fdb_ts(std::string_view(label_val).substr(
                                           label_val.size() - VERSION_STAMP_LEN, label_val.size()),
                                   &txn_id);
    if (ret != 0) {
        code = MetaServiceCode::TXN_GEN_ID_ERR;
        ss << "get_txn_id_from_fdb_ts() failed, label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "get_txn_id_from_fdb_ts() label=" << label << " txn_id=" << txn_id
              << " label_val.size()=" << label_val.size();

    TxnLabelPB label_pb;
    if (label_val.size() > VERSION_STAMP_LEN) {
        //3. Check label
        //label_val.size() > VERSION_STAMP_LEN means label has previous txn ids.
        if (!label_pb.ParseFromArray(label_val.data(), label_val.size() - VERSION_STAMP_LEN)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "label_pb->ParseFromString() failed, txn_id=" << txn_id << " label=" << label;
            msg = ss.str();
            return;
        }

        // Check if label already used, by following steps
        // 1. get all existing transactions
        // 2. if there is a PREPARE transaction, check if this is a retry request.
        // 3. if there is a non-aborted transaction, throw label already used exception.

        for (auto it = label_pb.txn_ids().rbegin(); it != label_pb.txn_ids().rend(); ++it) {
            int64_t cur_txn_id = *it;
            const std::string cur_info_key = txn_info_key({instance_id, db_id, cur_txn_id});
            std::string cur_info_val;
            err = txn->get(cur_info_key, &cur_info_val);
            if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                code = cast_as<ErrCategory::READ>(err);
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " label=" << label
                   << " err=" << err;
                msg = ss.str();
                return;
            }

            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                //label_to_idx and txn info inconsistency.
                code = MetaServiceCode::TXN_ID_NOT_FOUND;
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " label=" << label
                   << " err=" << err;
                msg = ss.str();
                return;
            }

            TxnInfoPB cur_txn_info;
            if (!cur_txn_info.ParseFromString(cur_info_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "cur_txn_info->ParseFromString() failed, cur_txn_id=" << cur_txn_id
                   << " label=" << label << " err=" << err;
                msg = ss.str();
                return;
            }

            VLOG_DEBUG << "cur_txn_info=" << cur_txn_info.ShortDebugString();
            LOG(INFO) << " size=" << label_pb.txn_ids().size()
                      << " status=" << cur_txn_info.status() << " txn_id=" << txn_id
                      << " label=" << label;
            if (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
                if (label_pb.txn_ids().size() >= config::max_num_aborted_txn) {
                    code = MetaServiceCode::INVALID_ARGUMENT;
                    ss << "too many aborted txn for label=" << label << " txn_id=" << txn_id
                       << ", please check your data quality";
                    msg = ss.str();
                    LOG(WARNING) << msg << " label_pb=" << label_pb.ShortDebugString();
                    return;
                }
                break;
            }

            if (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED ||
                cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED) {
                // clang-format off
                if (cur_txn_info.has_request_id() && txn_info.has_request_id() &&
                    ((cur_txn_info.request_id().hi() == txn_info.request_id().hi()) &&
                     (cur_txn_info.request_id().lo() == txn_info.request_id().lo()))) {

                    response->set_dup_txn_id(cur_txn_info.txn_id());
                    code = MetaServiceCode::TXN_DUPLICATED_REQ;
                    ss << "db_id=" << db_id << " label=" << label << " txn_id=" << cur_txn_info.txn_id() << " dup begin txn request.";
                    msg = ss.str();
                    return;
                }
                // clang-format on
            }
            code = MetaServiceCode::TXN_LABEL_ALREADY_USED;
            ss << "Label [" << label << "] has already been used, relate to txn ["
               << cur_txn_info.txn_id() << "]";
            msg = ss.str();
            return;
        }
    }

    // Update txn_info to be put into TxnKv
    // Update txn_id in PB
    txn_info.set_txn_id(txn_id);
    // TODO:
    // check initial status must be TXN_STATUS_PREPARED or TXN_STATUS_UNKNOWN
    txn_info.set_status(TxnStatusPB::TXN_STATUS_PREPARED);

    auto now_time = system_clock::now();
    uint64_t prepare_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();

    txn_info.set_prepare_time(prepare_time);
    //4. put txn info and db_tbl
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    std::string info_val;
    if (!txn_info.SerializeToString(&info_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info, label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    const std::string index_key = txn_index_key({instance_id, txn_id});
    std::string index_val;
    TxnIndexPB index_pb;
    index_pb.mutable_tablet_index()->set_db_id(db_id);
    if (!index_pb.SerializeToString(&index_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_index_pb "
           << "label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    const std::string running_key = txn_running_key({instance_id, db_id, txn_id});
    std::string running_val;
    TxnRunningPB running_pb;
    running_pb.set_timeout_time(prepare_time + txn_info.timeout_ms());
    running_pb.mutable_table_ids()->CopyFrom(txn_info.table_ids());
    VLOG_DEBUG << "label=" << label << " txn_id=" << txn_id
               << "running_pb=" << running_pb.ShortDebugString();
    if (!running_pb.SerializeToString(&running_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_pb label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    label_pb.add_txn_ids(txn_id);
    VLOG_DEBUG << "label=" << label << " txn_id=" << txn_id
               << "txn_label_pb=" << label_pb.ShortDebugString();
    if (!label_pb.SerializeToString(&label_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_label_pb label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->atomic_set_ver_value(label_key, label_val);
    LOG(INFO) << "txn->atomic_set_ver_value label_key=" << hex(label_key) << " label=" << label
              << " txn_id=" << txn_id;

    txn->put(info_key, info_val);
    txn->put(index_key, index_val);
    txn->put(running_key, running_val);
    LOG(INFO) << "xxx put info_key=" << hex(info_key) << " txn_id=" << txn_id;
    LOG(INFO) << "xxx put running_key=" << hex(running_key) << " txn_id=" << txn_id;
    LOG(INFO) << "xxx put index_key=" << hex(index_key) << " txn_id=" << txn_id;

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit txn kv, label=" << label << " txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }
    TEST_SYNC_POINT_CALLBACK("begin_txn:after:commit_txn:2", &txn_id);
    response->set_txn_id(txn_id);
}

void MetaServiceImpl::precommit_txn(::google::protobuf::RpcController* controller,
                                    const PrecommitTxnRequest* request,
                                    PrecommitTxnResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(precommit_txn);
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if ((txn_id < 0 && db_id < 0)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid argument, "
           << "txn_id=" << txn_id << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    RPC_RATE_LIMIT(precommit_txn);
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "txn_kv_->create_txn() failed, err=" << err << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    //not provide db_id, we need read from disk.
    if (db_id < 0) {
        const std::string index_key = txn_index_key({instance_id, txn_id});
        std::string index_val;
        err = txn->get(index_key, &index_val);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            ss << "failed to get db id with txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            return;
        }
        TxnIndexPB index_pb;
        if (!index_pb.ParseFromString(index_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_info"
               << " txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        DCHECK(index_pb.has_tablet_index() == true);
        DCHECK(index_pb.tablet_index().has_db_id() == true);
        db_id = index_pb.tablet_index().db_id();
        VLOG_DEBUG << " find db_id=" << db_id << " from index";
    } else {
        db_id = request->db_id();
    }

    // Get txn info with db_id and txn_id
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    std::string info_val; // Will be reused when saving updated txn
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        ss << "failed to get db id with db_id=" << db_id << " txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_inf db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    DCHECK(txn_info.txn_id() == txn_id);
    if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
        code = MetaServiceCode::TXN_ALREADY_ABORTED;
        ss << "transaction is already aborted: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
        code = MetaServiceCode::TXN_ALREADY_VISIBLE;
        ss << "transaction is already visible: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED) {
        code = MetaServiceCode::TXN_ALREADY_PRECOMMITED;
        ss << "transaction is already precommited: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
    }

    LOG(INFO) << "before update txn_info=" << txn_info.ShortDebugString();

    // Update txn_info
    txn_info.set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);

    auto now_time = system_clock::now();
    uint64_t precommit_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
    txn_info.set_precommit_time(precommit_time);
    if (request->has_commit_attachment()) {
        txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }
    LOG(INFO) << "after update txn_info=" << txn_info.ShortDebugString();

    info_val.clear();
    if (!txn_info.SerializeToString(&info_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    txn->put(info_key, info_val);
    LOG(INFO) << "xxx put info_key=" << hex(info_key) << " txn_id=" << txn_id;

    const std::string running_key = txn_running_key({instance_id, db_id, txn_id});
    std::string running_val;

    TxnRunningPB running_pb;
    running_pb.set_timeout_time(precommit_time + txn_info.precommit_timeout_ms());
    running_pb.mutable_table_ids()->CopyFrom(txn_info.table_ids());
    if (!running_pb.SerializeToString(&running_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize running_pb, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    txn->put(running_key, running_val);
    LOG(INFO) << "xxx put running_key=" << hex(running_key) << " txn_id=" << txn_id;

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit txn kv, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }
}

void put_routine_load_progress(MetaServiceCode& code, std::string& msg,
                               const std::string& instance_id, const CommitTxnRequest* request,
                               Transaction* txn, int64_t db_id) {
    std::stringstream ss;
    int64_t txn_id = request->txn_id();
    if (!request->has_commit_attachment()) {
        ss << "failed to get commit attachment from req, db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    TxnCommitAttachmentPB txn_commit_attachment = request->commit_attachment();
    RLTaskTxnCommitAttachmentPB commit_attachment =
            txn_commit_attachment.rl_task_txn_commit_attachment();
    int64_t job_id = commit_attachment.job_id();

    std::string rl_progress_key;
    std::string rl_progress_val;
    bool prev_progress_existed = true;
    RLJobProgressKeyInfo rl_progress_key_info {instance_id, db_id, job_id};
    rl_job_progress_key_info(rl_progress_key_info, &rl_progress_key);
    TxnErrorCode err = txn->get(rl_progress_key, &rl_progress_val);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            prev_progress_existed = false;
        } else {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get routine load progress, db_id=" << db_id << " txn_id=" << txn_id
               << " err=" << err;
            msg = ss.str();
            return;
        }
    }

    RoutineLoadProgressPB prev_progress_info;
    if (prev_progress_existed) {
        if (!prev_progress_info.ParseFromString(rl_progress_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse routine load progress, db_id=" << db_id << " txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
    }

    std::string new_progress_val;
    RoutineLoadProgressPB new_progress_info;
    new_progress_info.CopyFrom(commit_attachment.progress());
    for (auto const& elem : prev_progress_info.partition_to_offset()) {
        auto it = new_progress_info.partition_to_offset().find(elem.first);
        if (it == new_progress_info.partition_to_offset().end()) {
            new_progress_info.mutable_partition_to_offset()->insert(elem);
        }
    }

    std::string new_statistic_val;
    RoutineLoadJobStatisticPB* new_statistic_info = new_progress_info.mutable_stat();
    if (prev_progress_info.has_stat()) {
        const RoutineLoadJobStatisticPB& prev_statistic_info = prev_progress_info.stat();

        new_statistic_info->set_filtered_rows(prev_statistic_info.filtered_rows() +
                                              commit_attachment.filtered_rows());
        new_statistic_info->set_loaded_rows(prev_statistic_info.loaded_rows() +
                                            commit_attachment.loaded_rows());
        new_statistic_info->set_unselected_rows(prev_statistic_info.unselected_rows() +
                                                commit_attachment.unselected_rows());
        new_statistic_info->set_received_bytes(prev_statistic_info.received_bytes() +
                                               commit_attachment.received_bytes());
        new_statistic_info->set_task_execution_time_ms(
                prev_statistic_info.task_execution_time_ms() +
                commit_attachment.task_execution_time_ms());
    } else {
        new_statistic_info->set_filtered_rows(commit_attachment.filtered_rows());
        new_statistic_info->set_loaded_rows(commit_attachment.loaded_rows());
        new_statistic_info->set_unselected_rows(commit_attachment.unselected_rows());
        new_statistic_info->set_received_bytes(commit_attachment.received_bytes());
        new_statistic_info->set_task_execution_time_ms(commit_attachment.task_execution_time_ms());
    }

    if (!new_progress_info.SerializeToString(&new_progress_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize new progress val, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    txn->put(rl_progress_key, new_progress_val);
    LOG(INFO) << "put rl_progress_key key=" << hex(rl_progress_key)
              << " routine load new progress: " << new_progress_info.ShortDebugString();
}

void MetaServiceImpl::get_rl_task_commit_attach(::google::protobuf::RpcController* controller,
                                                const GetRLTaskCommitAttachRequest* request,
                                                GetRLTaskCommitAttachResponse* response,
                                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_rl_task_commit_attach);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_rl_task_commit_attach)

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "filed to create txn, err=" << err;
        msg = ss.str();
        return;
    }

    if (!request->has_db_id() || !request->has_job_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty db_id or job_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    int64_t db_id = request->db_id();
    int64_t job_id = request->job_id();
    std::string rl_progress_key;
    std::string rl_progress_val;
    RLJobProgressKeyInfo rl_progress_key_info {instance_id, db_id, job_id};
    rl_job_progress_key_info(rl_progress_key_info, &rl_progress_key);
    err = txn->get(rl_progress_key, &rl_progress_val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = MetaServiceCode::ROUTINE_LOAD_PROGRESS_NOT_FOUND;
        ss << "progress info not found, db_id=" << db_id << " job_id=" << job_id << " err=" << err;
        msg = ss.str();
        return;
    } else if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get progress info, db_id=" << db_id << " job_id=" << job_id
           << " err=" << err;
        msg = ss.str();
        return;
    }

    RLTaskTxnCommitAttachmentPB* commit_attach = response->mutable_commit_attach();
    RoutineLoadProgressPB* progress_info = commit_attach->mutable_progress();
    if (!progress_info->ParseFromString(rl_progress_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse progress info, db_id=" << db_id << " job_id=" << job_id;
        msg = ss.str();
        return;
    }

    if (progress_info->has_stat()) {
        const RoutineLoadJobStatisticPB& statistic_info = progress_info->stat();
        commit_attach->set_filtered_rows(statistic_info.filtered_rows());
        commit_attach->set_loaded_rows(statistic_info.loaded_rows());
        commit_attach->set_unselected_rows(statistic_info.unselected_rows());
        commit_attach->set_received_bytes(statistic_info.received_bytes());
        commit_attach->set_task_execution_time_ms(statistic_info.task_execution_time_ms());
    }
}

void MetaServiceImpl::reset_rl_progress(::google::protobuf::RpcController* controller,
                                        const ResetRLProgressRequest* request,
                                        ResetRLProgressResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(reset_rl_progress);
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(reset_rl_progress)

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "filed to create txn, err=" << err;
        msg = ss.str();
        return;
    }

    if (!request->has_db_id() || !request->has_job_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty db_id or job_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }

    int64_t db_id = request->db_id();
    int64_t job_id = request->job_id();
    std::string rl_progress_key;
    std::string rl_progress_val;
    RLJobProgressKeyInfo rl_progress_key_info {instance_id, db_id, job_id};
    rl_job_progress_key_info(rl_progress_key_info, &rl_progress_key);

    if (request->partition_to_offset().size() == 0) {
        txn->remove(rl_progress_key);
        LOG(INFO) << "remove rl_progress_key key=" << hex(rl_progress_key);
    }

    if (request->partition_to_offset().size() > 0) {
        bool prev_progress_existed = true;
        RoutineLoadProgressPB prev_progress_info;
        TxnErrorCode err = txn->get(rl_progress_key, &rl_progress_val);
        if (err != TxnErrorCode::TXN_OK) {
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                prev_progress_existed = false;
            } else {
                code = cast_as<ErrCategory::READ>(err);
                ss << "failed to get routine load progress, db_id=" << db_id << "job_id=" << job_id
                   << " err=" << err;
                msg = ss.str();
                return;
            }
        }
        if (prev_progress_existed) {
            if (!prev_progress_info.ParseFromString(rl_progress_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "failed to parse routine load progress, db_id=" << db_id
                   << "job_id=" << job_id;
                msg = ss.str();
                return;
            }
        }

        std::string new_progress_val;
        RoutineLoadProgressPB new_progress_info;
        for (auto const& elem : request->partition_to_offset()) {
            new_progress_info.mutable_partition_to_offset()->insert(elem);
        }
        if (request->partition_to_offset().size() > 0) {
            for (auto const& elem : prev_progress_info.partition_to_offset()) {
                auto it = new_progress_info.partition_to_offset().find(elem.first);
                if (it == new_progress_info.partition_to_offset().end()) {
                    new_progress_info.mutable_partition_to_offset()->insert(elem);
                }
            }
        }

        if (!new_progress_info.SerializeToString(&new_progress_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize new progress val"
               << "db_id=" << db_id << "job_id=" << job_id;
            msg = ss.str();
            return;
        }
        txn->put(rl_progress_key, new_progress_val);
        LOG(INFO) << "put rl_progress_key key=" << hex(rl_progress_key);
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to commit progress info, db_id=" << db_id << " job_id=" << job_id
           << " err=" << err;
        msg = ss.str();
        return;
    }
}

void scan_tmp_rowset(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg, int64_t* db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* tmp_rowsets_meta) {
    // Create a readonly txn for scan tmp rowset
    std::stringstream ss;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    // Get db id with txn id
    std::string index_val;
    const std::string index_key = txn_index_key({instance_id, txn_id});
    err = txn->get(index_key, &index_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get db id, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    TxnIndexPB index_pb;
    if (!index_pb.ParseFromString(index_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_index_pb, txn_id=" << txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    DCHECK(index_pb.has_tablet_index() == true);
    DCHECK(index_pb.tablet_index().has_db_id() == true);
    *db_id = index_pb.tablet_index().db_id();

    // Get temporary rowsets involved in the txn
    // This is a range scan
    MetaRowsetTmpKeyInfo rs_tmp_key_info0 {instance_id, txn_id, 0};
    MetaRowsetTmpKeyInfo rs_tmp_key_info1 {instance_id, txn_id + 1, 0};
    std::string rs_tmp_key0;
    std::string rs_tmp_key1;
    meta_rowset_tmp_key(rs_tmp_key_info0, &rs_tmp_key0);
    meta_rowset_tmp_key(rs_tmp_key_info1, &rs_tmp_key1);

    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [rs_tmp_key0, rs_tmp_key1, &num_rowsets, &txn_id](int*) {
                LOG(INFO) << "get tmp rowset meta, txn_id=" << txn_id
                          << " num_rowsets=" << num_rowsets << " range=[" << hex(rs_tmp_key0) << ","
                          << hex(rs_tmp_key1) << ")";
            });

    std::unique_ptr<RangeGetIterator> it;
    do {
        err = txn->get(rs_tmp_key0, rs_tmp_key1, &it, true);
        if (err == TxnErrorCode::TXN_TOO_OLD) {
            err = txn_kv->create_txn(&txn);
            if (err == TxnErrorCode::TXN_OK) {
                err = txn->get(rs_tmp_key0, rs_tmp_key1, &it, true);
            }
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "internal error, failed to get tmp rowset while committing, txn_id=" << txn_id
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            LOG(INFO) << "range_get rowset_tmp_key=" << hex(k) << " txn_id=" << txn_id;
            tmp_rowsets_meta->emplace_back();
            if (!tmp_rowsets_meta->back().second.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed rowset meta, unable to initialize, txn_id=" << txn_id
                   << " key=" << hex(k);
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }
            // Save keys that will be removed later
            tmp_rowsets_meta->back().first = std::string(k.data(), k.size());
            ++num_rowsets;
            if (!it->has_next()) rs_tmp_key0 = k;
        }
        rs_tmp_key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    VLOG_DEBUG << "txn_id=" << txn_id << " tmp_rowsets_meta.size()=" << tmp_rowsets_meta->size();
    return;
}

void update_tablet_stats(const StatsTabletKeyInfo& info, const TabletStats& stats,
                         std::unique_ptr<Transaction>& txn, MetaServiceCode& code,
                         std::string& msg) {
    if (config::split_tablet_stats) {
        if (stats.num_segs > 0) {
            std::string data_size_key;
            stats_tablet_data_size_key(info, &data_size_key);
            txn->atomic_add(data_size_key, stats.data_size);
            std::string num_rows_key;
            stats_tablet_num_rows_key(info, &num_rows_key);
            txn->atomic_add(num_rows_key, stats.num_rows);
            std::string num_segs_key;
            stats_tablet_num_segs_key(info, &num_segs_key);
            txn->atomic_add(num_segs_key, stats.num_segs);
        }
        std::string num_rowsets_key;
        stats_tablet_num_rowsets_key(info, &num_rowsets_key);
        txn->atomic_add(num_rowsets_key, stats.num_rowsets);
    } else {
        std::string key;
        stats_tablet_key(info, &key);
        std::string val;
        TxnErrorCode err = txn->get(key, &val);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TABLET_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to get tablet stats, err={} tablet_id={}", err,
                              std::get<4>(info));
            return;
        }
        TabletStatsPB stats_pb;
        if (!stats_pb.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed tablet stats value, key={}", hex(key));
            return;
        }
        stats_pb.set_data_size(stats_pb.data_size() + stats.data_size);
        stats_pb.set_num_rows(stats_pb.num_rows() + stats.num_rows);
        stats_pb.set_num_rowsets(stats_pb.num_rowsets() + stats.num_rowsets);
        stats_pb.set_num_segments(stats_pb.num_segments() + stats.num_segs);
        stats_pb.SerializeToString(&val);
        txn->put(key, val);
        LOG(INFO) << "put stats_tablet_key key=" << hex(key);
    }
}

/**
 * 0. Extract txn_id from request
 * 1. Get db id from TxnKv with txn_id
 * 2. Get TxnInfo from TxnKv with db_id and txn_id
 * 3. Get tmp rowset meta, there may be several or hundred of tmp rowsets
 * 4. Get versions of each rowset
 * 5. Put rowset meta, which will be visible to user
 * 6. Put TxnInfo back into TxnKv with updated txn status (committed)
 * 7. Update versions of each partition
 * 8. Remove tmp rowset meta
 *
 * Note: getting version and all changes maded are in a single TxnKv transaction:
 *       step 5, 6, 7, 8
 */
void commit_txn_immediately(
        const CommitTxnRequest* request, CommitTxnResponse* response,
        std::shared_ptr<TxnKv>& txn_kv, std::shared_ptr<TxnLazyCommitter>& txn_lazy_committer,
        MetaServiceCode& code, std::string& msg, const std::string& instance_id, int64_t db_id,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta) {
    std::stringstream ss;
    int64_t txn_id = request->txn_id();
    do {
        int64_t last_pending_txn_id = 0;
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        // Get txn info with db_id and txn_id
        std::string info_val; // Will be reused when saving updated txn
        const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
        err = txn->get(info_key, &info_val);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                ss << "transaction [" << txn_id << "] not found, db_id=" << db_id;
            } else {
                ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id
                   << " err=" << err;
            }
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        TxnInfoPB txn_info;
        if (!txn_info.ParseFromString(info_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_info, db_id=" << db_id << " txn_id=" << txn_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        // TODO: do more check like txn state, 2PC etc.
        DCHECK(txn_info.txn_id() == txn_id);
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
            code = MetaServiceCode::TXN_ALREADY_ABORTED;
            ss << "transaction [" << txn_id << "] is already aborted, db_id=" << db_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
            if (request->has_is_2pc() && request->is_2pc()) {
                code = MetaServiceCode::TXN_ALREADY_VISIBLE;
                ss << "transaction [" << txn_id << "] is already visible, not pre-committed.";
                msg = ss.str();
                LOG(INFO) << msg;
                response->mutable_txn_info()->CopyFrom(txn_info);
                return;
            }
            code = MetaServiceCode::OK;
            ss << "transaction is already visible: db_id=" << db_id << " txn_id=" << txn_id;
            msg = ss.str();
            LOG(INFO) << msg;
            response->mutable_txn_info()->CopyFrom(txn_info);
            return;
        }

        if (request->has_is_2pc() && request->is_2pc() &&
            txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED) {
            code = MetaServiceCode::TXN_INVALID_STATUS;
            ss << "transaction is prepare, not pre-committed: db_id=" << db_id << " txn_id"
               << txn_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        LOG(INFO) << "txn_id=" << txn_id << " txn_info=" << txn_info.ShortDebugString();

        // Prepare rowset meta and new_versions
        // Read tablet indexes in batch.
        std::vector<std::string> tablet_idx_keys;
        for (auto& [_, i] : tmp_rowsets_meta) {
            tablet_idx_keys.push_back(meta_tablet_idx_key({instance_id, i.tablet_id()}));
        }
        std::vector<std::optional<std::string>> tablet_idx_values;
        err = txn->batch_get(&tablet_idx_values, tablet_idx_keys,
                             Transaction::BatchGetOptions(false));
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get tablet table index ids, err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg << " txn_id=" << txn_id;
            return;
        }

        size_t total_rowsets = tmp_rowsets_meta.size();
        // tablet_id -> {table/index/partition}_id
        std::unordered_map<int64_t, TabletIndexPB> tablet_ids;
        // table_id -> tablets_ids
        std::unordered_map<int64_t, std::vector<int64_t>> table_id_tablet_ids;
        for (size_t i = 0; i < total_rowsets; i++) {
            uint64_t tablet_id = tmp_rowsets_meta[i].second.tablet_id();
            if (!tablet_idx_values[i].has_value()) [[unlikely]] {
                // The value must existed
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get tablet table index ids, err=not found"
                   << " tablet_id=" << tablet_id << " key=" << hex(tablet_idx_keys[i]);
                msg = ss.str();
                LOG(WARNING) << msg << " err=" << err << " txn_id=" << txn_id;
                return;
            }
            if (!tablet_ids[tablet_id].ParseFromString(tablet_idx_values[i].value())) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed tablet index value tablet_id=" << tablet_id
                   << " txn_id=" << txn_id;
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }
            table_id_tablet_ids[tablet_ids[tablet_id].table_id()].push_back(tablet_id);
            VLOG_DEBUG << "tablet_id:" << tablet_id
                       << " value:" << tablet_ids[tablet_id].ShortDebugString();
        }

        tablet_idx_keys.clear();
        tablet_idx_values.clear();

        // {table/partition} -> version
        std::unordered_map<std::string, uint64_t> new_versions;
        std::vector<std::string> version_keys;
        for (auto& [_, i] : tmp_rowsets_meta) {
            int64_t tablet_id = i.tablet_id();
            int64_t table_id = tablet_ids[tablet_id].table_id();
            int64_t partition_id = i.partition_id();
            std::string ver_key =
                    partition_version_key({instance_id, db_id, table_id, partition_id});
            if (new_versions.count(ver_key) == 0) {
                new_versions.insert({ver_key, 0});
                version_keys.push_back(std::move(ver_key));
            }
        }
        std::vector<std::optional<std::string>> version_values;
        err = txn->batch_get(&version_values, version_keys);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get partition versions, err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg << " txn_id=" << txn_id;
            return;
        }
        size_t total_versions = version_keys.size();
        for (size_t i = 0; i < total_versions; i++) {
            int64_t version;
            if (version_values[i].has_value()) {
                VersionPB version_pb;
                if (!version_pb.ParseFromString(version_values[i].value())) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "failed to parse version pb txn_id=" << txn_id
                       << " key=" << hex(version_keys[i]);
                    msg = ss.str();
                    return;
                }
                if (version_pb.pending_txn_ids_size() > 0) {
                    DCHECK(version_pb.pending_txn_ids_size() == 1);
                    last_pending_txn_id = version_pb.pending_txn_ids(0);
                    DCHECK(last_pending_txn_id > 0);
                    break;
                }
                version = version_pb.version();
            } else {
                version = 1;
            }
            new_versions[version_keys[i]] = version + 1;
            last_pending_txn_id = 0;
        }
        version_keys.clear();
        version_values.clear();

        if (last_pending_txn_id > 0) {
            txn.reset();
            std::shared_ptr<TxnLazyCommitTask> task =
                    txn_lazy_committer->submit(instance_id, last_pending_txn_id);

            std::tie(code, msg) = task->wait();
            if (code != MetaServiceCode::OK) {
                LOG(WARNING) << "advance_last_txn failed last_txn=" << last_pending_txn_id
                             << " code=" << code << "msg=" << msg;
                return;
            }
            last_pending_txn_id = 0;
            continue;
        }

        std::vector<std::pair<std::string, std::string>> rowsets;
        std::unordered_map<int64_t, TabletStats> tablet_stats; // tablet_id -> stats
        rowsets.reserve(tmp_rowsets_meta.size());
        for (auto& [_, i] : tmp_rowsets_meta) {
            int64_t tablet_id = i.tablet_id();
            int64_t table_id = tablet_ids[tablet_id].table_id();
            int64_t partition_id = i.partition_id();
            std::string ver_key =
                    partition_version_key({instance_id, db_id, table_id, partition_id});
            if (new_versions[ver_key] == 0) [[unlikely]] {
                // it is impossible.
                code = MetaServiceCode::UNDEFINED_ERR;
                ss << "failed to get partition version key, the target version not exists in "
                      "new_versions."
                   << " txn_id=" << txn_id;
                msg = ss.str();
                LOG(ERROR) << msg;
                return;
            }

            // Update rowset version
            int64_t new_version = new_versions[ver_key];
            i.set_start_version(new_version);
            i.set_end_version(new_version);

            std::string key = meta_rowset_key({instance_id, tablet_id, i.end_version()});
            std::string val;
            if (!i.SerializeToString(&val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                ss << "failed to serialize rowset_meta, txn_id=" << txn_id;
                msg = ss.str();
                return;
            }
            rowsets.emplace_back(std::move(key), std::move(val));

            // Accumulate affected rows
            auto& stats = tablet_stats[tablet_id];
            stats.data_size += i.data_disk_size();
            stats.num_rows += i.num_rows();
            ++stats.num_rowsets;
            stats.num_segs += i.num_segments();
        } // for tmp_rowsets_meta

        // process mow table, check lock and remove pending key
        std::vector<std::string> lock_keys;
        lock_keys.reserve(request->mow_table_ids().size());
        for (auto table_id : request->mow_table_ids()) {
            lock_keys.push_back(meta_delete_bitmap_update_lock_key({instance_id, table_id, -1}));
        }
        std::vector<std::optional<std::string>> lock_values;
        err = txn->batch_get(&lock_values, lock_keys);
        if (err != TxnErrorCode::TXN_OK) {
            ss << "failed to get delete bitmap update lock key info, instance_id=" << instance_id
               << " err=" << err;
            msg = ss.str();
            code = cast_as<ErrCategory::READ>(err);
            LOG(WARNING) << msg << " txn_id=" << txn_id;
            return;
        }
        size_t total_locks = lock_keys.size();
        for (size_t i = 0; i < total_locks; i++) {
            int64_t table_id = request->mow_table_ids(i);
            // When the key does not exist, it means the lock has been acquired
            // by another transaction and successfully committed.
            if (!lock_values[i].has_value()) {
                ss << "get delete bitmap update lock info, lock is expired"
                   << " table_id=" << table_id << " key=" << hex(lock_keys[i]);
                code = MetaServiceCode::LOCK_EXPIRED;
                msg = ss.str();
                LOG(WARNING) << msg << " txn_id=" << txn_id;
                return;
            }

            DeleteBitmapUpdateLockPB lock_info;
            if (!lock_info.ParseFromString(lock_values[i].value())) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse DeleteBitmapUpdateLockPB";
                LOG(WARNING) << msg << " txn_id=" << txn_id;
                return;
            }
            if (lock_info.lock_id() != request->txn_id()) {
                msg = "lock is expired";
                code = MetaServiceCode::LOCK_EXPIRED;
                return;
            }
            txn->remove(lock_keys[i]);
            LOG(INFO) << "xxx remove delete bitmap lock, lock_key=" << hex(lock_keys[i])
                      << " txn_id=" << txn_id;

            for (auto tablet_id : table_id_tablet_ids[table_id]) {
                std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
                txn->remove(pending_key);
                LOG(INFO) << "xxx remove delete bitmap pending key, pending_key="
                          << hex(pending_key) << " txn_id=" << txn_id;
            }
        }
        lock_keys.clear();
        lock_values.clear();

        // Save rowset meta
        for (auto& i : rowsets) {
            size_t rowset_size = i.first.size() + i.second.size();
            txn->put(i.first, i.second);
            LOG(INFO) << "xxx put rowset_key=" << hex(i.first) << " txn_id=" << txn_id
                      << " rowset_size=" << rowset_size;
        }

        // Save versions
        int64_t version_update_time_ms =
                duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        response->set_version_update_time_ms(version_update_time_ms);
        for (auto& i : new_versions) {
            std::string ver_val;
            VersionPB version_pb;
            version_pb.set_version(i.second);
            version_pb.set_update_time_ms(version_update_time_ms);
            if (!version_pb.SerializeToString(&ver_val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                ss << "failed to serialize version_pb when saving, txn_id=" << txn_id;
                msg = ss.str();
                return;
            }

            txn->put(i.first, ver_val);
            LOG(INFO) << "xxx put partition_version_key=" << hex(i.first) << " version:" << i.second
                      << " txn_id=" << txn_id << " update_time=" << version_update_time_ms;

            std::string_view ver_key = i.first;
            ver_key.remove_prefix(1); // Remove key space
            // PartitionVersionKeyInfo  {instance_id, db_id, table_id, partition_id}
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            int ret = decode_key(&ver_key, &out);
            if (ret != 0) [[unlikely]] {
                // decode version key error means this is something wrong,
                // we can not continue this txn
                LOG(WARNING) << "failed to decode key, ret=" << ret << " key=" << hex(ver_key);
                code = MetaServiceCode::UNDEFINED_ERR;
                msg = "decode version key error";
                return;
            }

            int64_t table_id = std::get<int64_t>(std::get<0>(out[4]));
            int64_t partition_id = std::get<int64_t>(std::get<0>(out[5]));
            VLOG_DEBUG << " table_id=" << table_id << " partition_id=" << partition_id;

            response->add_table_ids(table_id);
            response->add_partition_ids(partition_id);
            response->add_versions(i.second);
        }

        // Save table versions
        for (auto& i : table_id_tablet_ids) {
            std::string ver_key = table_version_key({instance_id, db_id, i.first});
            txn->atomic_add(ver_key, 1);
            LOG(INFO) << "xxx atomic add table_version_key=" << hex(ver_key)
                      << " txn_id=" << txn_id;
        }

        LOG(INFO) << " before update txn_info=" << txn_info.ShortDebugString();

        // Update txn_info
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);

        auto now_time = system_clock::now();
        uint64_t commit_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
        if ((txn_info.prepare_time() + txn_info.timeout_ms()) < commit_time) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = fmt::format("txn is expired, not allow to commit txn_id={}", txn_id);
            LOG(INFO) << msg << " prepare_time=" << txn_info.prepare_time()
                      << " timeout_ms=" << txn_info.timeout_ms() << " commit_time=" << commit_time;
            return;
        }
        txn_info.set_commit_time(commit_time);
        txn_info.set_finish_time(commit_time);
        if (request->has_commit_attachment()) {
            txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
        }
        LOG(INFO) << "after update txn_info=" << txn_info.ShortDebugString();
        info_val.clear();
        if (!txn_info.SerializeToString(&info_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        txn->put(info_key, info_val);
        LOG(INFO) << "xxx put info_key=" << hex(info_key) << " txn_id=" << txn_id;

        // Update stats of affected tablet
        for (auto& [tablet_id, stats] : tablet_stats) {
            DCHECK(tablet_ids.count(tablet_id));
            auto& tablet_idx = tablet_ids[tablet_id];
            StatsTabletKeyInfo info {instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                     tablet_idx.partition_id(), tablet_id};
            update_tablet_stats(info, stats, txn, code, msg);
            if (code != MetaServiceCode::OK) return;
        }
        // Remove tmp rowset meta
        for (auto& [k, _] : tmp_rowsets_meta) {
            txn->remove(k);
            LOG(INFO) << "xxx remove tmp_rowset_key=" << hex(k) << " txn_id=" << txn_id;
        }

        const std::string running_key = txn_running_key({instance_id, db_id, txn_id});
        LOG(INFO) << "xxx remove running_key=" << hex(running_key) << " txn_id=" << txn_id;
        txn->remove(running_key);

        std::string recycle_val;
        std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        RecycleTxnPB recycle_pb;
        recycle_pb.set_creation_time(commit_time);
        recycle_pb.set_label(txn_info.label());

        if (!recycle_pb.SerializeToString(&recycle_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize recycle_pb, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        txn->put(recycle_key, recycle_val);

        if (txn_info.load_job_source_type() ==
            LoadJobSourceTypePB::LOAD_JOB_SRC_TYPE_ROUTINE_LOAD_TASK) {
            put_routine_load_progress(code, msg, instance_id, request, txn.get(), db_id);
        }

        LOG(INFO) << "xxx commit_txn put recycle_key key=" << hex(recycle_key)
                  << " txn_id=" << txn_id;
        LOG(INFO) << "commit_txn put_size=" << txn->put_bytes()
                  << " del_size=" << txn->delete_bytes() << " num_put_keys=" << txn->num_put_keys()
                  << " num_del_keys=" << txn->num_del_keys()
                  << " txn_size=" << txn->approximate_bytes() << " txn_id=" << txn_id;

        // Finally we are done...
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to commit kv txn, txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            return;
        }

        // calculate table stats from tablets stats
        std::map<int64_t /*table_id*/, TableStats> table_stats;
        std::vector<int64_t> base_tablet_ids(request->base_tablet_ids().begin(),
                                             request->base_tablet_ids().end());
        calc_table_stats(tablet_ids, tablet_stats, table_stats, base_tablet_ids);
        for (const auto& pair : table_stats) {
            TableStatsPB* stats_pb = response->add_table_stats();
            auto table_id = pair.first;
            auto stats = pair.second;
            get_pb_from_tablestats(stats, stats_pb);
            stats_pb->set_table_id(table_id);
            VLOG_DEBUG << "Add TableStats to CommitTxnResponse. txn_id=" << txn_id
                       << " table_id=" << table_id
                       << " updated_row_count=" << stats_pb->updated_row_count();
        }
        response->mutable_txn_info()->CopyFrom(txn_info);
        break;
    } while (true);
} // end commit_txn_immediately

void get_tablet_indexes(
        const std::string& instance_id, int64_t txn_id,
        const std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta,
        std::unique_ptr<Transaction>& txn, MetaServiceCode& code, std::string& msg,
        std::unordered_map<int64_t, TabletIndexPB>* tablet_ids,
        std::unordered_map<int64_t, std::vector<int64_t>>* table_id_tablet_ids,
        bool* need_repair_tablet_idx) {
    // Read tablet indexes in batch.
    std::stringstream ss;
    std::vector<std::string> tablet_idx_keys;
    for (auto& [_, i] : tmp_rowsets_meta) {
        tablet_idx_keys.push_back(meta_tablet_idx_key({instance_id, i.tablet_id()}));
    }
    std::vector<std::optional<std::string>> tablet_idx_values;
    TxnErrorCode err =
            txn->batch_get(&tablet_idx_values, tablet_idx_keys, Transaction::BatchGetOptions(true));
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get tablet table index ids, err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg << " txn_id=" << txn_id;
        return;
    }

    for (size_t i = 0; i < tmp_rowsets_meta.size(); i++) {
        uint64_t tablet_id = tmp_rowsets_meta[i].second.tablet_id();
        if (!tablet_idx_values[i].has_value()) [[unlikely]] {
            // The value must existed
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get tablet table index ids, err=not found"
               << " tablet_id=" << tablet_id << " key=" << hex(tablet_idx_keys[i]);
            msg = ss.str();
            LOG(WARNING) << msg << " err=" << err << " txn_id=" << txn_id;
            return;
        }
        if (!(*tablet_ids)[tablet_id].ParseFromString(tablet_idx_values[i].value())) [[unlikely]] {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "malformed tablet index value tablet_id=" << tablet_id << " txn_id=" << txn_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        if (!(*tablet_ids)[tablet_id].has_db_id()) {
            *need_repair_tablet_idx = true;
        }
        (*table_id_tablet_ids)[(*tablet_ids)[tablet_id].table_id()].push_back(tablet_id);
        VLOG_DEBUG << "tablet_id:" << tablet_id
                   << " value:" << (*tablet_ids)[tablet_id].ShortDebugString();
    }

    tablet_idx_keys.clear();
    tablet_idx_values.clear();
}

// rewrite TabletIndexPB for fill db_id, in case of historical reasons
// TabletIndexPB missing db_id
void repair_tablet_index(
        std::shared_ptr<TxnKv>& txn_kv, MetaServiceCode& code, std::string& msg,
        const std::string& instance_id, int64_t db_id, int64_t txn_id,
        const std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta) {
    std::stringstream ss;
    std::vector<std::string> tablet_idx_keys;
    for (auto& [_, i] : tmp_rowsets_meta) {
        tablet_idx_keys.push_back(meta_tablet_idx_key({instance_id, i.tablet_id()}));
    }

    for (size_t i = 0; i < tablet_idx_keys.size(); i += config::max_tablet_index_num_per_batch) {
        size_t end = (i + config::max_tablet_index_num_per_batch) > tablet_idx_keys.size()
                             ? tablet_idx_keys.size()
                             : i + config::max_tablet_index_num_per_batch;
        const std::vector<std::string> sub_tablet_idx_keys(tablet_idx_keys.begin() + i,
                                                           tablet_idx_keys.begin() + end);

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        std::vector<std::optional<std::string>> tablet_idx_values;
        // batch get snapshot is false
        err = txn->batch_get(&tablet_idx_values, sub_tablet_idx_keys,
                             Transaction::BatchGetOptions(false));
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get tablet table index ids, err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg << " txn_id=" << txn_id;
            return;
        }
        DCHECK(tablet_idx_values.size() <= config::max_tablet_index_num_per_batch);

        for (size_t j = 0; j < sub_tablet_idx_keys.size(); j++) {
            if (!tablet_idx_values[j].has_value()) [[unlikely]] {
                // The value must existed
                code = MetaServiceCode::KV_TXN_GET_ERR;
                ss << "failed to get tablet table index ids, err=not found"
                   << " key=" << hex(tablet_idx_keys[j]);
                msg = ss.str();
                LOG(WARNING) << msg << " err=" << err << " txn_id=" << txn_id;
                return;
            }
            TabletIndexPB tablet_idx_pb;
            if (!tablet_idx_pb.ParseFromString(tablet_idx_values[j].value())) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed tablet index value key=" << hex(tablet_idx_keys[j])
                   << " txn_id=" << txn_id;
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }

            if (!tablet_idx_pb.has_db_id()) {
                tablet_idx_pb.set_db_id(db_id);
                std::string idx_val;
                if (!tablet_idx_pb.SerializeToString(&idx_val)) {
                    code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                    ss << "failed to serialize tablet index value key=" << hex(tablet_idx_keys[j])
                       << " txn_id=" << txn_id;
                    msg = ss.str();
                    LOG(WARNING) << msg;
                    return;
                }
                txn->put(sub_tablet_idx_keys[j], idx_val);
                LOG(INFO) << " repaire tablet index txn_id=" << txn_id
                          << " tablet_idx_pb:" << tablet_idx_pb.ShortDebugString()
                          << " key=" << hex(sub_tablet_idx_keys[j]);
            }
        }

        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to commit kv txn, txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
    }
}

void commit_txn_eventually(
        const CommitTxnRequest* request, CommitTxnResponse* response,
        std::shared_ptr<TxnKv>& txn_kv, std::shared_ptr<TxnLazyCommitter>& txn_lazy_committer,
        MetaServiceCode& code, std::string& msg, const std::string& instance_id, int64_t db_id,
        const std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>& tmp_rowsets_meta) {
    std::stringstream ss;
    TxnErrorCode err = TxnErrorCode::TXN_OK;
    int64_t txn_id = request->txn_id();

    do {
        int64_t last_pending_txn_id = 0;
        std::unique_ptr<Transaction> txn;
        err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::CREATE>(err);
            ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        // tablet_id -> {table/index/partition}_id
        std::unordered_map<int64_t, TabletIndexPB> tablet_ids;
        // table_id -> tablets_ids
        std::unordered_map<int64_t, std::vector<int64_t>> table_id_tablet_ids;
        bool need_repair_tablet_idx = false;
        get_tablet_indexes(instance_id, txn_id, tmp_rowsets_meta, txn, code, msg, &tablet_ids,
                           &table_id_tablet_ids, &need_repair_tablet_idx);
        if (code != MetaServiceCode::OK) {
            LOG(WARNING) << "get_tablet_indexes failed, txn_id=" << txn_id << " code=" << code;
            return;
        }

        if (need_repair_tablet_idx) {
            txn.reset();
            repair_tablet_index(txn_kv, code, msg, instance_id, db_id, txn_id, tmp_rowsets_meta);
            if (code != MetaServiceCode::OK) {
                LOG(WARNING) << "repair_tablet_index failed, txn_id=" << txn_id << " code=" << code;
                return;
            }
            continue;
        }

        // <partition_version_key, version>
        std::unordered_map<std::string, uint64_t> new_versions;
        std::vector<std::string> version_keys;
        for (auto& [_, i] : tmp_rowsets_meta) {
            int64_t tablet_id = i.tablet_id();
            int64_t table_id = tablet_ids[tablet_id].table_id();
            int64_t partition_id = i.partition_id();
            std::string ver_key =
                    partition_version_key({instance_id, db_id, table_id, partition_id});
            if (new_versions.count(ver_key) == 0) {
                new_versions.insert({ver_key, 0});
                version_keys.push_back(std::move(ver_key));
            }
        }

        std::vector<std::optional<std::string>> version_values;
        err = txn->batch_get(&version_values, version_keys);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get partition versions, err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg << " txn_id=" << txn_id;
            return;
        }

        for (size_t i = 0; i < version_keys.size(); i++) {
            int64_t version;
            if (version_values[i].has_value()) {
                VersionPB version_pb;
                if (!version_pb.ParseFromString(version_values[i].value())) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "failed to parse version pb txn_id=" << txn_id
                       << " key=" << hex(version_keys[i]);
                    msg = ss.str();
                    return;
                }
                if (version_pb.pending_txn_ids_size() > 0) {
                    DCHECK(version_pb.pending_txn_ids_size() == 1);
                    last_pending_txn_id = version_pb.pending_txn_ids(0);
                    DCHECK(last_pending_txn_id > 0);
                    break;
                }
                version = version_pb.version();
            } else {
                version = 1;
            }
            new_versions[version_keys[i]] = version;
            last_pending_txn_id = 0;
        }

        if (last_pending_txn_id > 0) {
            txn.reset();
            std::shared_ptr<TxnLazyCommitTask> task =
                    txn_lazy_committer->submit(instance_id, last_pending_txn_id);

            std::tie(code, msg) = task->wait();
            if (code != MetaServiceCode::OK) {
                LOG(WARNING) << "advance_last_txn failed last_txn=" << last_pending_txn_id
                             << " code=" << code << "msg=" << msg;
                return;
            }

            last_pending_txn_id = 0;
            // there maybe concurrent commit_txn_eventually, so we need continue to make sure
            // partition versionPB has no txn_id
            continue;
        }

        std::string info_val;
        const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
        err = txn->get(info_key, &info_val);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id
               << " err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        TxnInfoPB txn_info;
        if (!txn_info.ParseFromString(info_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_info, db_id=" << db_id << " txn_id=" << txn_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        LOG(INFO) << "txn_id=" << txn_id << " txn_info=" << txn_info.ShortDebugString();

        DCHECK(txn_info.txn_id() == txn_id);
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
            code = MetaServiceCode::TXN_ALREADY_ABORTED;
            ss << "transaction is already aborted: db_id=" << db_id << " txn_id=" << txn_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
            if (request->has_is_2pc() && request->is_2pc()) {
                code = MetaServiceCode::TXN_ALREADY_VISIBLE;
                ss << "transaction [" << txn_id << "] is already visible, not pre-committed.";
                msg = ss.str();
                LOG(INFO) << msg;
                response->mutable_txn_info()->CopyFrom(txn_info);
                return;
            }
            code = MetaServiceCode::OK;
            ss << "transaction is already visible: db_id=" << db_id << " txn_id=" << txn_id;
            msg = ss.str();
            LOG(INFO) << msg;
            response->mutable_txn_info()->CopyFrom(txn_info);
            return;
        }

        if (request->has_is_2pc() && request->is_2pc() &&
            txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED) {
            code = MetaServiceCode::TXN_INVALID_STATUS;
            ss << "transaction is prepare, not pre-committed: db_id=" << db_id << " txn_id"
               << txn_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        auto now_time = system_clock::now();
        uint64_t commit_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
        if ((txn_info.prepare_time() + txn_info.timeout_ms()) < commit_time) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = fmt::format("txn is expired, not allow to commit txn_id={}", txn_id);
            LOG(INFO) << msg << " prepare_time=" << txn_info.prepare_time()
                      << " timeout_ms=" << txn_info.timeout_ms() << " commit_time=" << commit_time;
            return;
        }
        txn_info.set_commit_time(commit_time);
        txn_info.set_finish_time(commit_time);
        if (request->has_commit_attachment()) {
            txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
        }
        DCHECK(txn_info.status() != TxnStatusPB::TXN_STATUS_COMMITTED);
        // set status TXN_STATUS_COMMITTED not TXN_STATUS_VISIBLE !!!
        // lazy commit task will advance txn to make txn visible
        txn_info.set_status(TxnStatusPB::TXN_STATUS_COMMITTED);

        LOG(INFO) << "after update txn_id= " << txn_id
                  << " txn_info=" << txn_info.ShortDebugString();
        info_val.clear();
        if (!txn_info.SerializeToString(&info_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        txn->put(info_key, info_val);
        LOG(INFO) << "put info_key=" << hex(info_key) << " txn_id=" << txn_id;

        if (txn_info.load_job_source_type() ==
            LoadJobSourceTypePB::LOAD_JOB_SRC_TYPE_ROUTINE_LOAD_TASK) {
            put_routine_load_progress(code, msg, instance_id, request, txn.get(), db_id);
        }

        // save versions for partition
        int64_t version_update_time_ms =
                duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        response->set_version_update_time_ms(version_update_time_ms);
        for (auto& i : new_versions) {
            std::string ver_val;
            VersionPB version_pb;
            version_pb.add_pending_txn_ids(txn_id);
            version_pb.set_update_time_ms(version_update_time_ms);
            if (i.second > 1) {
                version_pb.set_version(i.second);
            }

            if (!version_pb.SerializeToString(&ver_val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                ss << "failed to serialize version_pb when saving, txn_id=" << txn_id
                   << " partiton_key=" << hex(i.first);
                msg = ss.str();
                return;
            }

            txn->put(i.first, ver_val);
            LOG(INFO) << "put partition_version_key=" << hex(i.first) << " version:" << i.second
                      << " txn_id=" << txn_id << " update_time=" << version_update_time_ms;

            std::string_view ver_key = i.first;
            ver_key.remove_prefix(1); // Remove key space
            // PartitionVersionKeyInfo  {instance_id, db_id, table_id, partition_id}
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            int ret = decode_key(&ver_key, &out);
            if (ret != 0) [[unlikely]] {
                // decode version key error means this is something wrong,
                // we can not continue this txn
                LOG(WARNING) << "failed to decode key, ret=" << ret << " key=" << hex(ver_key);
                code = MetaServiceCode::UNDEFINED_ERR;
                msg = "decode version key error";
                return;
            }

            int64_t table_id = std::get<int64_t>(std::get<0>(out[4]));
            int64_t partition_id = std::get<int64_t>(std::get<0>(out[5]));
            VLOG_DEBUG << "txn_id=" << txn_id << " table_id=" << table_id
                       << " partition_id=" << partition_id << " version=" << i.second;

            response->add_table_ids(table_id);
            response->add_partition_ids(partition_id);
            response->add_versions(i.second + 1);
        }

        // process mow table, check lock and remove pending key
        std::vector<std::string> lock_keys;
        lock_keys.reserve(request->mow_table_ids().size());
        for (auto table_id : request->mow_table_ids()) {
            lock_keys.push_back(meta_delete_bitmap_update_lock_key({instance_id, table_id, -1}));
        }
        std::vector<std::optional<std::string>> lock_values;
        err = txn->batch_get(&lock_values, lock_keys);
        if (err != TxnErrorCode::TXN_OK) {
            ss << "failed to get delete bitmap update lock key info, instance_id=" << instance_id
               << " err=" << err;
            msg = ss.str();
            code = cast_as<ErrCategory::READ>(err);
            LOG(WARNING) << msg << " txn_id=" << txn_id;
            return;
        }

        for (size_t i = 0; i < lock_keys.size(); i++) {
            int64_t table_id = request->mow_table_ids(i);
            // When the key does not exist, it means the lock has been acquired
            // by another transaction and successfully committed.
            if (!lock_values[i].has_value()) {
                ss << "get delete bitmap update lock info, lock is expired"
                   << " table_id=" << table_id << " key=" << hex(lock_keys[i]);
                code = MetaServiceCode::LOCK_EXPIRED;
                msg = ss.str();
                LOG(WARNING) << msg << " txn_id=" << txn_id;
                return;
            }

            DeleteBitmapUpdateLockPB lock_info;
            if (!lock_info.ParseFromString(lock_values[i].value())) [[unlikely]] {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse DeleteBitmapUpdateLockPB";
                LOG(WARNING) << msg << " txn_id=" << txn_id;
                return;
            }
            if (lock_info.lock_id() != request->txn_id()) {
                msg = "lock is expired";
                code = MetaServiceCode::LOCK_EXPIRED;
                return;
            }
            txn->remove(lock_keys[i]);
            LOG(INFO) << "xxx remove delete bitmap lock, lock_key=" << hex(lock_keys[i])
                      << " txn_id=" << txn_id;

            for (auto tablet_id : table_id_tablet_ids[table_id]) {
                std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
                txn->remove(pending_key);
                LOG(INFO) << "xxx remove delete bitmap pending key, pending_key="
                          << hex(pending_key) << " txn_id=" << txn_id;
            }
        }
        lock_keys.clear();
        lock_values.clear();

        // Save table versions
        for (auto& i : table_id_tablet_ids) {
            std::string ver_key = table_version_key({instance_id, db_id, i.first});
            txn->atomic_add(ver_key, 1);
            LOG(INFO) << "xxx atomic add table_version_key=" << hex(ver_key)
                      << " txn_id=" << txn_id;
        }

        LOG(INFO) << "commit_txn_eventually put_size=" << txn->put_bytes()
                  << " del_size=" << txn->delete_bytes() << " num_put_keys=" << txn->num_put_keys()
                  << " num_del_keys=" << txn->num_del_keys()
                  << " txn_size=" << txn->approximate_bytes() << " txn_id=" << txn_id;

        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to commit kv txn, txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            return;
        }

        std::shared_ptr<TxnLazyCommitTask> task = txn_lazy_committer->submit(instance_id, txn_id);
        std::pair<MetaServiceCode, std::string> ret = task->wait();
        if (ret.first != MetaServiceCode::OK) {
            LOG(WARNING) << "txn lazy commit failed txn_id=" << txn_id << " code=" << ret.first
                         << "msg=" << ret.second;
        }

        std::unordered_map<int64_t, TabletStats> tablet_stats; // tablet_id -> stats
        for (auto& [_, i] : tmp_rowsets_meta) {
            // Accumulate affected rows
            auto& stats = tablet_stats[i.tablet_id()];
            stats.data_size += i.data_disk_size();
            stats.num_rows += i.num_rows();
            ++stats.num_rowsets;
            stats.num_segs += i.num_segments();
        }

        // calculate table stats from tablets stats
        std::map<int64_t /*table_id*/, TableStats> table_stats;
        std::vector<int64_t> base_tablet_ids(request->base_tablet_ids().begin(),
                                             request->base_tablet_ids().end());
        calc_table_stats(tablet_ids, tablet_stats, table_stats, base_tablet_ids);
        for (const auto& pair : table_stats) {
            TableStatsPB* stats_pb = response->add_table_stats();
            auto table_id = pair.first;
            auto stats = pair.second;
            get_pb_from_tablestats(stats, stats_pb);
            stats_pb->set_table_id(table_id);
            VLOG_DEBUG << "Add TableStats to CommitTxnResponse. txn_id=" << txn_id
                       << " table_id=" << table_id
                       << " updated_row_count=" << stats_pb->updated_row_count();
        }

        // txn set visible for fe callback
        txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);
        response->mutable_txn_info()->CopyFrom(txn_info);
        break;
    } while (true);
}

/**
 * This process is generally the same as commit_txn, the difference is that
 * the partitions version will plus 1 in multi sub txns.
 *
 * One example:
 *  Suppose the table, partition, tablet and version info is:
 *  --------------------------------------------
 *  | table | partition | tablet    | version |
 *  --------------------------------------------
 *  | t1    | t1_p1     | t1_p1.1   | 1       |
 *  | t1    | t1_p1     | t1_p1.2   | 1       |
 *  | t1    | t1_p2     | t1_p2.1   | 2       |
 *  | t2    | t2_p3     | t2_p3.1   | 3       |
 *  | t2    | t2_p4     | t2_p4.1   | 4       |
 *  --------------------------------------------
 *
 *  Now we commit a txn with 3 sub txns and the tablets are:
 *    sub_txn1: t1_p1.1, t1_p1.2, t1_p2.1
 *    sub_txn2: t2_p3.1
 *    sub_txn3: t1_p1.1, t1_p1.2
 *  When commit, the partitions version will be:
 *    sub_txn1: t1_p1(1 -> 2), t1_p2(2 -> 3)
 *    sub_txn2: t2_p3(3 -> 4)
 *    sub_txn3: t1_p1(2 -> 3)
 *  After commit, the partitions version will be:
 *    t1: t1_p1(3), t1_p2(3)
 *    t2: t2_p3(4), t2_p4(4)
 */
void commit_txn_with_sub_txn(const CommitTxnRequest* request, CommitTxnResponse* response,
                             std::shared_ptr<TxnKv>& txn_kv, MetaServiceCode& code,
                             std::string& msg, const std::string& instance_id) {
    std::stringstream ss;
    int64_t txn_id = request->txn_id();
    auto sub_txn_infos = request->sub_txn_infos();
    // Create a readonly txn for scan tmp rowset
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "filed to create txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    // Get db id with txn id
    std::string index_val;
    const std::string index_key = txn_index_key({instance_id, txn_id});
    err = txn->get(index_key, &index_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get db id, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    TxnIndexPB index_pb;
    if (!index_pb.ParseFromString(index_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_index_pb, txn_id=" << txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    DCHECK(index_pb.has_tablet_index() == true);
    DCHECK(index_pb.tablet_index().has_db_id() == true);
    int64_t db_id = index_pb.tablet_index().db_id();

    // Get temporary rowsets involved in the txn
    std::map<int64_t, std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>>
            sub_txn_to_tmp_rowsets_meta;
    for (const auto& sub_txn_info : sub_txn_infos) {
        auto sub_txn_id = sub_txn_info.sub_txn_id();
        // This is a range scan
        MetaRowsetTmpKeyInfo rs_tmp_key_info0 {instance_id, sub_txn_id, 0};
        MetaRowsetTmpKeyInfo rs_tmp_key_info1 {instance_id, sub_txn_id + 1, 0};
        std::string rs_tmp_key0;
        std::string rs_tmp_key1;
        meta_rowset_tmp_key(rs_tmp_key_info0, &rs_tmp_key0);
        meta_rowset_tmp_key(rs_tmp_key_info1, &rs_tmp_key1);
        // Get rowset meta that should be commited
        //                   tmp_rowset_key -> rowset_meta
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> tmp_rowsets_meta;

        int num_rowsets = 0;
        std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
                (int*)0x01, [rs_tmp_key0, rs_tmp_key1, &num_rowsets, &txn_id, &sub_txn_id](int*) {
                    LOG(INFO) << "get tmp rowset meta, txn_id=" << txn_id
                              << ", sub_txn_id=" << sub_txn_id << " num_rowsets=" << num_rowsets
                              << " range=[" << hex(rs_tmp_key0) << "," << hex(rs_tmp_key1) << ")";
                });

        std::unique_ptr<RangeGetIterator> it;
        do {
            err = txn->get(rs_tmp_key0, rs_tmp_key1, &it, true);
            if (err == TxnErrorCode::TXN_TOO_OLD) {
                err = txn_kv->create_txn(&txn);
                if (err == TxnErrorCode::TXN_OK) {
                    err = txn->get(rs_tmp_key0, rs_tmp_key1, &it, true);
                }
            }
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                ss << "internal error, failed to get tmp rowset while committing, txn_id=" << txn_id
                   << " err=" << err;
                msg = ss.str();
                LOG(WARNING) << msg;
                return;
            }

            while (it->has_next()) {
                auto [k, v] = it->next();
                LOG(INFO) << "range_get rowset_tmp_key=" << hex(k) << " txn_id=" << txn_id;
                tmp_rowsets_meta.emplace_back();
                if (!tmp_rowsets_meta.back().second.ParseFromArray(v.data(), v.size())) {
                    code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                    ss << "malformed rowset meta, unable to initialize, txn_id=" << txn_id
                       << " key=" << hex(k);
                    msg = ss.str();
                    LOG(WARNING) << msg;
                    return;
                }
                // Save keys that will be removed later
                tmp_rowsets_meta.back().first = std::string(k.data(), k.size());
                ++num_rowsets;
                if (!it->has_next()) rs_tmp_key0 = k;
            }
            rs_tmp_key0.push_back('\x00'); // Update to next smallest key for iteration
        } while (it->more());

        VLOG_DEBUG << "txn_id=" << txn_id << " sub_txn_id=" << sub_txn_id
                   << " tmp_rowsets_meta.size()=" << tmp_rowsets_meta.size();
        sub_txn_to_tmp_rowsets_meta.emplace(sub_txn_id, std::move(tmp_rowsets_meta));
    }

    // Create a read/write txn for guarantee consistency
    txn.reset();
    err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "filed to create txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    // Get txn info with db_id and txn_id
    std::string info_val; // Will be reused when saving updated txn
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_info, db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    // TODO: do more check like txn state
    DCHECK(txn_info.txn_id() == txn_id);
    if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
        code = MetaServiceCode::TXN_ALREADY_ABORTED;
        ss << "transaction is already aborted: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
        code = MetaServiceCode::OK;
        ss << "transaction is already visible: db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        LOG(INFO) << msg;
        response->mutable_txn_info()->CopyFrom(txn_info);
        return;
    }

    LOG(INFO) << "txn_id=" << txn_id << " txn_info=" << txn_info.ShortDebugString();

    // Prepare rowset meta and new_versions
    // Read tablet indexes in batch.
    std::map<int64_t, int64_t> tablet_id_to_idx;
    std::vector<std::string> tablet_idx_keys;
    std::vector<int64_t> partition_ids;
    auto idx = 0;
    for (auto& [_, tmp_rowsets_meta] : sub_txn_to_tmp_rowsets_meta) {
        for (auto& [_, i] : tmp_rowsets_meta) {
            auto tablet_id = i.tablet_id();
            if (tablet_id_to_idx.count(tablet_id) == 0) {
                tablet_id_to_idx.emplace(tablet_id, idx);
                tablet_idx_keys.push_back(meta_tablet_idx_key({instance_id, i.tablet_id()}));
                partition_ids.push_back(i.partition_id());
                idx++;
            }
        }
    }
    std::vector<std::optional<std::string>> tablet_idx_values;
    err = txn->batch_get(&tablet_idx_values, tablet_idx_keys, Transaction::BatchGetOptions(false));
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get tablet table index ids, err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg << " txn_id=" << txn_id;
        return;
    }

    // tablet_id -> {table/index/partition}_id
    std::unordered_map<int64_t, TabletIndexPB> tablet_ids;
    // table_id -> tablets_ids
    std::unordered_map<int64_t, std::vector<int64_t>> table_id_tablet_ids;
    for (auto [tablet_id, i] : tablet_id_to_idx) {
        if (!tablet_idx_values[i].has_value()) [[unlikely]] {
            // The value must existed
            code = MetaServiceCode::KV_TXN_GET_ERR;
            ss << "failed to get tablet table index ids, err=not found"
               << " tablet_id=" << tablet_id << " key=" << hex(tablet_idx_keys[i]);
            msg = ss.str();
            LOG(WARNING) << msg << " err=" << err << " txn_id=" << txn_id;
            return;
        }
        if (!tablet_ids[tablet_id].ParseFromString(tablet_idx_values[i].value())) [[unlikely]] {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "malformed tablet index value tablet_id=" << tablet_id << " txn_id=" << txn_id;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }
        table_id_tablet_ids[tablet_ids[tablet_id].table_id()].push_back(tablet_id);
        VLOG_DEBUG << "tablet_id:" << tablet_id
                   << " value:" << tablet_ids[tablet_id].ShortDebugString();
    }

    tablet_idx_keys.clear();
    tablet_idx_values.clear();

    // {table/partition} -> version
    std::unordered_map<std::string, uint64_t> new_versions;
    std::vector<std::string> version_keys;
    for (auto& [tablet_id, i] : tablet_id_to_idx) {
        int64_t table_id = tablet_ids[tablet_id].table_id();
        int64_t partition_id = partition_ids[i];
        std::string ver_key = partition_version_key({instance_id, db_id, table_id, partition_id});
        if (new_versions.count(ver_key) == 0) {
            new_versions.insert({ver_key, 0});
            LOG(INFO) << "xxx add a partition_version_key=" << hex(ver_key) << " txn_id=" << txn_id
                      << ", db_id=" << db_id << ", table_id=" << table_id
                      << ", partition_id=" << partition_id;
            version_keys.push_back(std::move(ver_key));
        }
    }
    std::vector<std::optional<std::string>> version_values;
    err = txn->batch_get(&version_values, version_keys);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "failed to get partition versions, err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg << " txn_id=" << txn_id;
        return;
    }
    size_t total_versions = version_keys.size();
    for (size_t i = 0; i < total_versions; i++) {
        int64_t version;
        if (version_values[i].has_value()) {
            VersionPB version_pb;
            if (!version_pb.ParseFromString(version_values[i].value())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "failed to parse version pb txn_id=" << txn_id
                   << " key=" << hex(version_keys[i]);
                msg = ss.str();
                return;
            }
            version = version_pb.version();
        } else {
            version = 1;
        }
        new_versions[version_keys[i]] = version;
        LOG(INFO) << "xxx get partition_version_key=" << hex(version_keys[i])
                  << " version:" << version << " txn_id=" << txn_id;
    }
    version_keys.clear();
    version_values.clear();

    std::vector<std::pair<std::string, std::string>> rowsets;
    std::unordered_map<int64_t, TabletStats> tablet_stats; // tablet_id -> stats
    for (const auto& sub_txn_info : sub_txn_infos) {
        auto sub_txn_id = sub_txn_info.sub_txn_id();
        auto tmp_rowsets_meta = sub_txn_to_tmp_rowsets_meta[sub_txn_id];
        std::unordered_map<int64_t, int64_t> partition_id_to_version;
        for (auto& [_, i] : tmp_rowsets_meta) {
            int64_t tablet_id = i.tablet_id();
            int64_t table_id = tablet_ids[tablet_id].table_id();
            int64_t partition_id = i.partition_id();
            std::string ver_key =
                    partition_version_key({instance_id, db_id, table_id, partition_id});
            if (new_versions.count(ver_key) == 0) [[unlikely]] {
                // it is impossible.
                code = MetaServiceCode::UNDEFINED_ERR;
                ss << "failed to get partition version key, the target version not exists in "
                      "new_versions."
                   << " txn_id=" << txn_id << ", db_id=" << db_id << ", table_id=" << table_id
                   << ", partition_id=" << partition_id;
                msg = ss.str();
                LOG(ERROR) << msg;
                return;
            }

            // Update rowset version
            int64_t new_version = new_versions[ver_key];
            if (partition_id_to_version.count(partition_id) == 0) {
                new_versions[ver_key] = new_version + 1;
                new_version = new_versions[ver_key];
                partition_id_to_version[partition_id] = new_version;
            }
            i.set_start_version(new_version);
            i.set_end_version(new_version);
            LOG(INFO) << "xxx update rowset version, txn_id=" << txn_id
                      << ", sub_txn_id=" << sub_txn_id << ", table_id=" << table_id
                      << ", partition_id=" << partition_id << ", tablet_id=" << tablet_id
                      << ", new_version=" << new_version;

            std::string key = meta_rowset_key({instance_id, tablet_id, i.end_version()});
            std::string val;
            if (!i.SerializeToString(&val)) {
                code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
                ss << "failed to serialize rowset_meta, txn_id=" << txn_id;
                msg = ss.str();
                return;
            }
            rowsets.emplace_back(std::move(key), std::move(val));

            // Accumulate affected rows
            auto& stats = tablet_stats[tablet_id];
            stats.data_size += i.data_disk_size();
            stats.num_rows += i.num_rows();
            ++stats.num_rowsets;
            stats.num_segs += i.num_segments();
        } // for tmp_rowsets_meta
    }

    // Save rowset meta
    for (auto& i : rowsets) {
        size_t rowset_size = i.first.size() + i.second.size();
        txn->put(i.first, i.second);
        LOG(INFO) << "xxx put rowset_key=" << hex(i.first) << " txn_id=" << txn_id
                  << " rowset_size=" << rowset_size;
    }

    // Save versions
    for (auto& i : new_versions) {
        std::string ver_val;
        VersionPB version_pb;
        version_pb.set_version(i.second);
        if (!version_pb.SerializeToString(&ver_val)) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "failed to serialize version_pb when saving, txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        txn->put(i.first, ver_val);
        LOG(INFO) << "xxx put partition_version_key=" << hex(i.first) << " version:" << i.second
                  << " txn_id=" << txn_id;

        std::string_view ver_key = i.first;
        ver_key.remove_prefix(1); // Remove key space
        // PartitionVersionKeyInfo  {instance_id, db_id, table_id, partition_id}
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        int ret = decode_key(&ver_key, &out);
        if (ret != 0) [[unlikely]] {
            // decode version key error means this is something wrong,
            // we can not continue this txn
            LOG(WARNING) << "failed to decode key, ret=" << ret << " key=" << hex(ver_key);
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = "decode version key error";
            return;
        }

        int64_t table_id = std::get<int64_t>(std::get<0>(out[4]));
        int64_t partition_id = std::get<int64_t>(std::get<0>(out[5]));
        VLOG_DEBUG << "txn_id=" << txn_id << " table_id=" << table_id
                   << " partition_id=" << partition_id << " version=" << i.second;

        response->add_table_ids(table_id);
        response->add_partition_ids(partition_id);
        response->add_versions(i.second);
    }

    // Save table versions
    for (auto& i : table_id_tablet_ids) {
        std::string ver_key = table_version_key({instance_id, db_id, i.first});
        txn->atomic_add(ver_key, 1);
        LOG(INFO) << "xxx atomic add table_version_key=" << hex(ver_key) << " txn_id=" << txn_id;
    }

    LOG(INFO) << " before update txn_info=" << txn_info.ShortDebugString();

    // Update txn_info
    txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);

    auto now_time = system_clock::now();
    uint64_t commit_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
    if ((txn_info.prepare_time() + txn_info.timeout_ms()) < commit_time) {
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = fmt::format("txn is expired, not allow to commit txn_id={}", txn_id);
        LOG(INFO) << msg << " prepare_time=" << txn_info.prepare_time()
                  << " timeout_ms=" << txn_info.timeout_ms() << " commit_time=" << commit_time;
        return;
    }
    txn_info.set_commit_time(commit_time);
    txn_info.set_finish_time(commit_time);
    if (request->has_commit_attachment()) {
        txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }
    LOG(INFO) << "after update txn_info=" << txn_info.ShortDebugString();
    info_val.clear();
    if (!txn_info.SerializeToString(&info_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->put(info_key, info_val);
    LOG(INFO) << "xxx put info_key=" << hex(info_key) << " txn_id=" << txn_id;

    // Update stats of affected tablet
    std::deque<std::string> kv_pool;
    std::function<void(const StatsTabletKeyInfo&, const TabletStats&)> update_tablet_stats;
    if (config::split_tablet_stats) {
        update_tablet_stats = [&](const StatsTabletKeyInfo& info, const TabletStats& stats) {
            if (stats.num_segs > 0) {
                auto& data_size_key = kv_pool.emplace_back();
                stats_tablet_data_size_key(info, &data_size_key);
                txn->atomic_add(data_size_key, stats.data_size);
                auto& num_rows_key = kv_pool.emplace_back();
                stats_tablet_num_rows_key(info, &num_rows_key);
                txn->atomic_add(num_rows_key, stats.num_rows);
                auto& num_segs_key = kv_pool.emplace_back();
                stats_tablet_num_segs_key(info, &num_segs_key);
                txn->atomic_add(num_segs_key, stats.num_segs);
            }
            auto& num_rowsets_key = kv_pool.emplace_back();
            stats_tablet_num_rowsets_key(info, &num_rowsets_key);
            txn->atomic_add(num_rowsets_key, stats.num_rowsets);
        };
    } else {
        update_tablet_stats = [&](const StatsTabletKeyInfo& info, const TabletStats& stats) {
            auto& key = kv_pool.emplace_back();
            stats_tablet_key(info, &key);
            auto& val = kv_pool.emplace_back();
            TxnErrorCode err = txn->get(key, &val);
            if (err != TxnErrorCode::TXN_OK) {
                code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TABLET_NOT_FOUND
                                                              : cast_as<ErrCategory::READ>(err);
                msg = fmt::format("failed to get tablet stats, err={} tablet_id={}", err,
                                  std::get<4>(info));
                return;
            }
            TabletStatsPB stats_pb;
            if (!stats_pb.ParseFromString(val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = fmt::format("malformed tablet stats value, key={}", hex(key));
                return;
            }
            stats_pb.set_data_size(stats_pb.data_size() + stats.data_size);
            stats_pb.set_num_rows(stats_pb.num_rows() + stats.num_rows);
            stats_pb.set_num_rowsets(stats_pb.num_rowsets() + stats.num_rowsets);
            stats_pb.set_num_segments(stats_pb.num_segments() + stats.num_segs);
            stats_pb.SerializeToString(&val);
            txn->put(key, val);
            LOG(INFO) << "put stats_tablet_key, key=" << hex(key);
        };
    }
    for (auto& [tablet_id, stats] : tablet_stats) {
        DCHECK(tablet_ids.count(tablet_id));
        auto& tablet_idx = tablet_ids[tablet_id];
        StatsTabletKeyInfo info {instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                 tablet_idx.partition_id(), tablet_id};
        update_tablet_stats(info, stats);
        if (code != MetaServiceCode::OK) return;
    }
    // Remove tmp rowset meta
    for (auto& [_, tmp_rowsets_meta] : sub_txn_to_tmp_rowsets_meta) {
        for (auto& [k, _] : tmp_rowsets_meta) {
            txn->remove(k);
            LOG(INFO) << "xxx remove tmp_rowset_key=" << hex(k) << " txn_id=" << txn_id;
        }
    }

    const std::string running_key = txn_running_key({instance_id, db_id, txn_id});
    LOG(INFO) << "xxx remove running_key=" << hex(running_key) << " txn_id=" << txn_id;
    txn->remove(running_key);

    std::string recycle_val;
    std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
    RecycleTxnPB recycle_pb;
    recycle_pb.set_creation_time(commit_time);
    recycle_pb.set_label(txn_info.label());

    if (!recycle_pb.SerializeToString(&recycle_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize recycle_pb, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }
    txn->put(recycle_key, recycle_val);
    LOG(INFO) << "xxx commit_txn put recycle_txn_key key=" << hex(recycle_key)
              << " txn_id=" << txn_id;

    LOG(INFO) << "commit_txn put_size=" << txn->put_bytes() << " del_size=" << txn->delete_bytes()
              << " num_put_keys=" << txn->num_put_keys() << " num_del_keys=" << txn->num_del_keys()
              << " txn_size=" << txn->approximate_bytes() << " txn_id=" << txn_id;

    // Finally we are done...
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_VALUE_TOO_LARGE || err == TxnErrorCode::TXN_BYTES_TOO_LARGE) {
            size_t max_size = 0, max_num_segments = 0,
                   min_num_segments = std::numeric_limits<size_t>::max(), avg_num_segments = 0;
            std::pair<std::string, RowsetMetaCloudPB>* max_rowset_meta = nullptr;
            for (auto& sub_txn : sub_txn_infos) {
                auto it = sub_txn_to_tmp_rowsets_meta.find(sub_txn.sub_txn_id());
                if (it == sub_txn_to_tmp_rowsets_meta.end()) {
                    continue;
                }
                for (auto& rowset_meta : it->second) {
                    if (rowset_meta.second.ByteSizeLong() > max_size) {
                        max_size = rowset_meta.second.ByteSizeLong();
                        max_rowset_meta = &rowset_meta;
                    }
                    if (rowset_meta.second.num_segments() > max_num_segments) {
                        max_num_segments = rowset_meta.second.num_segments();
                    }
                    if (rowset_meta.second.num_segments() < min_num_segments) {
                        min_num_segments = rowset_meta.second.num_segments();
                    }
                    avg_num_segments += rowset_meta.second.num_segments();
                }
                if (!it->second.empty()) {
                    avg_num_segments /= it->second.size();
                }
            }
            if (max_rowset_meta) {
                LOG(WARNING) << "failed to commit kv txn with sub txn"
                             << ", err=" << err << ", txn_id=" << txn_id
                             << ", total_rowsets=" << rowsets.size()
                             << ", avg_num_segments=" << avg_num_segments
                             << ", min_num_segments=" << min_num_segments
                             << ", max_num_segments=" << max_num_segments
                             << ", largest_rowset_size=" << max_size
                             << ", largest_rowset_key=" << hex(max_rowset_meta->first)
                             << ", largest_rowset_value="
                             << max_rowset_meta->second.ShortDebugString();
            }
        }
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit kv txn with sub txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }

    // calculate table stats from tablets stats
    std::map<int64_t /*table_id*/, TableStats> table_stats;
    std::vector<int64_t> base_tablet_ids(request->base_tablet_ids().begin(),
                                         request->base_tablet_ids().end());
    calc_table_stats(tablet_ids, tablet_stats, table_stats, base_tablet_ids);
    for (const auto& pair : table_stats) {
        TableStatsPB* stats_pb = response->add_table_stats();
        auto table_id = pair.first;
        auto stats = pair.second;
        get_pb_from_tablestats(stats, stats_pb);
        stats_pb->set_table_id(table_id);
        VLOG_DEBUG << "Add TableStats to CommitTxnResponse. txn_id=" << txn_id
                   << " table_id=" << table_id
                   << " updated_row_count=" << stats_pb->updated_row_count();
    }

    response->mutable_txn_info()->CopyFrom(txn_info);
} // end commit_txn_with_sub_txn

void MetaServiceImpl::commit_txn(::google::protobuf::RpcController* controller,
                                 const CommitTxnRequest* request, CommitTxnResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(commit_txn);
    if (!request->has_txn_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid argument, missing txn id";
        return;
    }

    int64_t txn_id = request->txn_id();
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << cloud_unique_id << " txn_id=" << txn_id;
        return;
    }
    RPC_RATE_LIMIT(commit_txn)

    if (request->has_is_txn_load() && request->is_txn_load()) {
        commit_txn_with_sub_txn(request, response, txn_kv_, code, msg, instance_id);
        return;
    }

    int64_t db_id;
    std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> tmp_rowsets_meta;
    scan_tmp_rowset(instance_id, txn_id, txn_kv_, code, msg, &db_id, &tmp_rowsets_meta);
    if (code != MetaServiceCode::OK) {
        LOG(WARNING) << "scan_tmp_rowset failed, txn_id=" << txn_id << " code=" << code;
        return;
    }

    if (request->has_is_2pc() && !request->is_2pc() && request->has_enable_txn_lazy_commit() &&
        request->enable_txn_lazy_commit() && config::enable_cloud_txn_lazy_commit &&
        (tmp_rowsets_meta.size() >= config::txn_lazy_commit_rowsets_thresold)) {
        LOG(INFO) << "txn_id=" << txn_id << " commit_txn_eventually"
                  << " size=" << tmp_rowsets_meta.size();
        commit_txn_eventually(request, response, txn_kv_, txn_lazy_committer_, code, msg,
                              instance_id, db_id, tmp_rowsets_meta);
        return;
    }

    commit_txn_immediately(request, response, txn_kv_, txn_lazy_committer_, code, msg, instance_id,
                           db_id, tmp_rowsets_meta);
}

static void _abort_txn(const std::string& instance_id, const AbortTxnRequest* request,
                       Transaction* txn, TxnInfoPB& return_txn_info, std::stringstream& ss,
                       MetaServiceCode& code, std::string& msg) {
    int64_t txn_id = request->txn_id();
    std::string label = request->label();
    int64_t db_id = request->db_id();

    std::string info_key; // Will be used when saving updated txn
    std::string info_val; // Will be reused when saving updated txn
    TxnErrorCode err;
    //TODO: split with two function.
    //there two ways to abort txn:
    //1. abort txn by txn id
    //2. abort txn by label and db_id
    if (txn_id > 0) {
        VLOG_DEBUG << "abort_txn by txn_id, txn_id=" << txn_id;
        //abort txn by txn id
        // Get db id with txn id

        std::string index_key;
        std::string index_val;
        //not provide db_id, we need read from disk.
        if (db_id == 0) {
            index_key = txn_index_key({instance_id, txn_id});
            err = txn->get(index_key, &index_val);
            if (err != TxnErrorCode::TXN_OK) {
                code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                              : cast_as<ErrCategory::READ>(err);
                if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    ss << "transaction [" << txn_id << "] not found";
                } else {
                    ss << "failed to get txn info, txn_id=" << txn_id << " err=" << err;
                }
                msg = ss.str();
                return;
            }

            TxnIndexPB index_pb;
            if (!index_pb.ParseFromString(index_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "failed to parse txn_index_val"
                   << " txn_id=" << txn_id;
                msg = ss.str();
                return;
            }
            DCHECK(index_pb.has_tablet_index() == true);
            DCHECK(index_pb.tablet_index().has_db_id() == true);
            db_id = index_pb.tablet_index().db_id();
        }

        // Get txn info with db_id and txn_id
        info_key = txn_info_key({instance_id, db_id, txn_id});
        err = txn->get(info_key, &info_val);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            ss << "failed to get txn_info, db_id=" << db_id << "txn_id=" << txn_id << "err=" << err;
            msg = ss.str();
            return;
        }

        if (!return_txn_info.ParseFromString(info_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_info db_id=" << db_id << "txn_id=" << txn_id;
            msg = ss.str();
            return;
        }

        DCHECK(return_txn_info.txn_id() == txn_id);

        //check state is valid.
        if (return_txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
            code = MetaServiceCode::TXN_ALREADY_ABORTED;
            ss << "transaction [" << txn_id << "] is already aborted, db_id=" << db_id;
            msg = ss.str();
            return;
        }
        if (return_txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
            code = MetaServiceCode::TXN_ALREADY_VISIBLE;
            ss << "transaction [" << txn_id << "] is already VISIBLE, db_id=" << db_id;
            msg = ss.str();
            return;
        }
    } else {
        VLOG_DEBUG << "abort_txn db_id and label, db_id=" << db_id << " label=" << label;
        //abort txn by label.
        std::string label_key = txn_label_key({instance_id, db_id, label});
        std::string label_val;
        err = txn->get(label_key, &label_val);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "txn->get() failed, label=" << label << " err=" << err;
            msg = ss.str();
            return;
        }

        //label index not exist
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = MetaServiceCode::TXN_LABEL_NOT_FOUND;
            ss << "label not found, db_id=" << db_id << " label=" << label << " err=" << err;
            msg = ss.str();
            return;
        }

        TxnLabelPB label_pb;
        DCHECK(label_val.size() > VERSION_STAMP_LEN);
        if (!label_pb.ParseFromArray(label_val.data(), label_val.size() - VERSION_STAMP_LEN)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "txn_label_pb->ParseFromString() failed, label=" << label;
            msg = ss.str();
            return;
        }

        int64_t prepare_txn_id = 0;
        //found prepare state txn for abort
        for (auto& cur_txn_id : label_pb.txn_ids()) {
            std::string cur_info_key = txn_info_key({instance_id, db_id, cur_txn_id});
            std::string cur_info_val;
            err = txn->get(cur_info_key, &cur_info_val);
            if (err != TxnErrorCode::TXN_OK) {
                code = cast_as<ErrCategory::READ>(err);
                std::stringstream ss;
                ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " err=" << err;
                msg = ss.str();
                return;
            }
            // ret == 0
            TxnInfoPB cur_txn_info;
            if (!cur_txn_info.ParseFromString(cur_info_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                std::stringstream ss;
                ss << "cur_txn_info->ParseFromString() failed, cur_txn_id=" << cur_txn_id;
                msg = ss.str();
                return;
            }
            VLOG_DEBUG << "cur_txn_info=" << cur_txn_info.ShortDebugString();
            //TODO: 2pc else need to check TxnStatusPB::TXN_STATUS_PRECOMMITTED
            if ((cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PREPARED) ||
                (cur_txn_info.status() == TxnStatusPB::TXN_STATUS_PRECOMMITTED)) {
                prepare_txn_id = cur_txn_id;
                return_txn_info = std::move(cur_txn_info);
                info_key = std::move(cur_info_key);
                DCHECK_EQ(prepare_txn_id, return_txn_info.txn_id())
                        << "prepare_txn_id=" << prepare_txn_id
                        << " txn_id=" << return_txn_info.txn_id();
                break;
            }
        }

        if (prepare_txn_id == 0) {
            code = MetaServiceCode::TXN_INVALID_STATUS;
            std::stringstream ss;
            ss << "running transaction not found, db_id=" << db_id << " label=" << label;
            msg = ss.str();
            return;
        }
    }

    auto now_time = system_clock::now();
    uint64_t finish_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();

    // Update txn_info
    return_txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
    return_txn_info.set_finish_time(finish_time);
    request->has_reason() ? return_txn_info.set_reason(request->reason())
                          : return_txn_info.set_reason("User Abort");

    if (request->has_commit_attachment()) {
        return_txn_info.mutable_commit_attachment()->CopyFrom(request->commit_attachment());
    }

    info_val.clear();
    if (!return_txn_info.SerializeToString(&info_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << return_txn_info.txn_id();
        msg = ss.str();
        return;
    }
    LOG(INFO) << "check watermark conflict, txn_info=" << return_txn_info.ShortDebugString();
    txn->put(info_key, info_val);
    LOG(INFO) << "xxx put info_key=" << hex(info_key) << " txn_id=" << return_txn_info.txn_id();

    std::string running_key = txn_running_key({instance_id, db_id, return_txn_info.txn_id()});
    txn->remove(running_key);
    LOG(INFO) << "xxx remove running_key=" << hex(running_key)
              << " txn_id=" << return_txn_info.txn_id();

    std::string recycle_key = recycle_txn_key({instance_id, db_id, return_txn_info.txn_id()});
    std::string recycle_val;
    RecycleTxnPB recycle_pb;
    recycle_pb.set_creation_time(finish_time);
    recycle_pb.set_label(return_txn_info.label());

    if (!recycle_pb.SerializeToString(&recycle_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize recycle_pb, txn_id=" << return_txn_info.txn_id();
        msg = ss.str();
        return;
    }
    txn->put(recycle_key, recycle_val);
    LOG(INFO) << "put recycle_txn_key=" << hex(recycle_key)
              << " txn_id=" << return_txn_info.txn_id();
}

void MetaServiceImpl::abort_txn(::google::protobuf::RpcController* controller,
                                const AbortTxnRequest* request, AbortTxnResponse* response,
                                ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(abort_txn);
    // Get txn id
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    std::string label = request->has_label() ? request->label() : "";
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    if (txn_id < 0 && (label.empty() || db_id < 0)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid txn id and label, db_id=" << db_id << " txn_id=" << txn_id
           << " label=" << label;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id) << " label=" << label
           << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    RPC_RATE_LIMIT(abort_txn);
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "filed to txn_kv_->create_txn(), txn_id=" << txn_id << " label=" << label
           << " err=" << err;
        msg = ss.str();
        return;
    }
    TxnInfoPB txn_info;

    _abort_txn(instance_id, request, txn.get(), txn_info, ss, code, msg);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit kv txn, txn_id=" << txn_info.txn_id() << " err=" << err;
        msg = ss.str();
        return;
    }
    response->mutable_txn_info()->CopyFrom(txn_info);
}

void MetaServiceImpl::get_txn(::google::protobuf::RpcController* controller,
                              const GetTxnRequest* request, GetTxnResponse* response,
                              ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_txn);
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    std::string label = request->has_label() ? request->label() : "";
    if (txn_id < 0 && label.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid txn_id, it may be not given or set properly, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }

    RPC_RATE_LIMIT(get_txn)
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }

    if (!label.empty()) {
        //step 1: get label
        const std::string label_key = txn_label_key({instance_id, db_id, label});
        std::string label_val;
        err = txn->get(label_key, &label_val);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "txn->get failed(), err=" << err << " label=" << label;
            msg = ss.str();
            return;
        }
        LOG(INFO) << "txn->get label_key=" << hex(label_key) << " label=" << label
                  << " err=" << err;
        // step 2: get txn info from label pb
        TxnLabelPB label_pb;
        if (err == TxnErrorCode::TXN_OK) {
            if (label_val.size() <= VERSION_STAMP_LEN ||
                !label_pb.ParseFromArray(label_val.data(), label_val.size() - VERSION_STAMP_LEN)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "label_pb->ParseFromString() failed, txn_id=" << txn_id << " label=" << label;
                msg = ss.str();
                return;
            }
            for (auto& cur_txn_id : label_pb.txn_ids()) {
                if (cur_txn_id > txn_id) {
                    txn_id = cur_txn_id;
                }
            }
        } else {
            code = MetaServiceCode::TXN_ID_NOT_FOUND;
            ss << "Label [" << label << "] has not found";
            msg = ss.str();
            return;
        }
    }

    //not provide db_id, we need read from disk.
    if (db_id < 0) {
        const std::string index_key = txn_index_key({instance_id, txn_id});
        std::string index_val;
        err = txn->get(index_key, &index_val);
        if (err != TxnErrorCode::TXN_OK) {
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                          : cast_as<ErrCategory::READ>(err);
            ss << "failed to get db id with txn_id=" << txn_id << " err=" << err;
            msg = ss.str();
            return;
        }

        TxnIndexPB index_pb;
        if (!index_pb.ParseFromString(index_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "failed to parse txn_inf"
               << " txn_id=" << txn_id;
            msg = ss.str();
            return;
        }
        DCHECK(index_pb.has_tablet_index() == true);
        DCHECK(index_pb.tablet_index().has_db_id() == true);
        db_id = index_pb.tablet_index().db_id();
        if (db_id <= 0) {
            ss << "internal error: unexpected db_id " << db_id;
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = ss.str();
            return;
        }
    }

    // Get txn info with db_id and txn_id
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    std::string info_val;
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        ss << "failed to get db id with db_id=" << db_id << " txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_info db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    VLOG_DEBUG << "txn_info=" << txn_info.ShortDebugString();
    DCHECK(txn_info.txn_id() == txn_id);
    response->mutable_txn_info()->CopyFrom(txn_info);
}

//To get current max txn id for schema change watermark etc.
void MetaServiceImpl::get_current_max_txn_id(::google::protobuf::RpcController* controller,
                                             const GetCurrentMaxTxnRequest* request,
                                             GetCurrentMaxTxnResponse* response,
                                             ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_current_max_txn_id);
    // TODO: For auth
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        LOG(INFO) << msg << ", cloud_unique_id=" << request->cloud_unique_id();
        return;
    }
    RPC_RATE_LIMIT(get_current_max_txn_id)
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        code = cast_as<ErrCategory::CREATE>(err);
        return;
    }

    const std::string key = "schema change";
    std::string val;
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        std::stringstream ss;
        ss << "txn->get() failed, err=" << err;
        msg = ss.str();
        return;
    }
    int64_t read_version = 0;
    err = txn->get_read_version(&read_version);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        std::stringstream ss;
        ss << "get read version failed, ret=" << err;
        msg = ss.str();
        return;
    }

    int64_t current_max_txn_id = read_version << 10;
    VLOG_DEBUG << "read_version=" << read_version << " current_max_txn_id=" << current_max_txn_id;
    response->set_current_max_txn_id(current_max_txn_id);
}

/**
 * 1. Generate a sub_txn_id
 *
 * The following steps are done in a txn:
 * 2. Put txn_index_key in sub_txn_id
 * 3. Delete txn_label_key in sub_txn_id
 * 4. Modify the txn state of the txn_id:
 *    - Add the sub txn id to sub_txn_ids: recycler use it to recycle the txn_index_key
 *    - Add the table id to table_ids
 */
void MetaServiceImpl::begin_sub_txn(::google::protobuf::RpcController* controller,
                                    const BeginSubTxnRequest* request,
                                    BeginSubTxnResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(begin_sub_txn);
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    int64_t sub_txn_num = request->has_sub_txn_num() ? request->sub_txn_num() : -1;
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    auto& table_ids = request->table_ids();
    std::string label = request->has_label() ? request->label() : "";
    if (txn_id < 0 || sub_txn_num < 0 || db_id < 0 || table_ids.empty() || label.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid argument, txn_id=" << txn_id << ", sub_txn_num=" << sub_txn_num
           << " db_id=" << db_id << ", label=" << label << ", table_ids=[";
        for (auto table_id : table_ids) {
            ss << table_id << ", ";
        }
        ss << "]";
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }

    RPC_RATE_LIMIT(begin_sub_txn)
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "txn_kv_->create_txn() failed, err=" << err << " txn_id=" << txn_id
           << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    const std::string label_key = txn_label_key({instance_id, db_id, label});
    std::string label_val;
    err = txn->get(label_key, &label_val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "txn->get failed(), err=" << err << " label=" << label;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn->get label_key=" << hex(label_key) << " label=" << label << " err=" << err;

    // err == OK means this is a retry rpc?
    if (err == TxnErrorCode::TXN_OK) {
        label_val = label_val.substr(0, label_val.size() - VERSION_STAMP_LEN);
    }

    // ret > 0, means label not exist previously.
    txn->atomic_set_ver_value(label_key, label_val);
    LOG(INFO) << "txn->atomic_set_ver_value label_key=" << hex(label_key);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "txn->commit failed(), label=" << label << " err=" << err;
        msg = ss.str();
        return;
    }

    // 2. Get sub txn id from version stamp
    txn.reset();
    err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "failed to create txn when get txn id, label=" << label << " err=" << err;
        msg = ss.str();
        return;
    }

    label_val.clear();
    err = txn->get(label_key, &label_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "txn->get() failed, label=" << label << " err=" << err;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "txn->get label_key=" << hex(label_key) << " label=" << label << " err=" << err;

    // Generated by TxnKv system
    int64_t sub_txn_id = 0;
    int ret =
            get_txn_id_from_fdb_ts(std::string_view(label_val).substr(
                                           label_val.size() - VERSION_STAMP_LEN, label_val.size()),
                                   &sub_txn_id);
    if (ret != 0) {
        code = MetaServiceCode::TXN_GEN_ID_ERR;
        ss << "get_txn_id_from_fdb_ts() failed, label=" << label << " ret=" << ret;
        msg = ss.str();
        return;
    }

    LOG(INFO) << "get_txn_id_from_fdb_ts() label=" << label << " sub_txn_id=" << sub_txn_id
              << " txn_id=" << txn_id << " label_val.size()=" << label_val.size();

    // write txn_index_key
    const std::string index_key = txn_index_key({instance_id, sub_txn_id});
    std::string index_val;
    TxnIndexPB index_pb;
    if (!index_pb.SerializeToString(&index_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_index_pb "
           << "label=" << label << " txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    // Get and update txn info with db_id and txn_id
    std::string info_val; // Will be reused when saving updated txn
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_info, db_id=" << db_id << " txn_id=" << txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }
    DCHECK(txn_info.txn_id() == txn_id);
    if (txn_info.status() != TxnStatusPB::TXN_STATUS_PREPARED) {
        code = MetaServiceCode::TXN_INVALID_STATUS;
        ss << "transaction status is " << txn_info.status() << " : db_id=" << db_id
           << " txn_id=" << txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    if (txn_info.sub_txn_ids().size() != sub_txn_num) {
        code = MetaServiceCode::UNDEFINED_ERR;
        ss << "sub_txn_num mismatch, txn_id=" << txn_id << ", expected sub_txn_num=" << sub_txn_num
           << ", txn_info.sub_txn_ids=[";
        for (auto id : txn_info.sub_txn_ids()) {
            ss << id << ", ";
        }
        ss << "]";
        msg = ss.str();
        LOG(WARNING) << msg;
    }
    txn_info.mutable_sub_txn_ids()->Add(sub_txn_id);
    txn_info.mutable_table_ids()->Clear();
    for (auto table_id : table_ids) {
        txn_info.mutable_table_ids()->Add(table_id);
    }
    if (!txn_info.SerializeToString(&info_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_id;
        msg = ss.str();
        return;
    }

    txn->remove(label_key);
    txn->put(info_key, info_val);
    txn->put(index_key, index_val);
    LOG(INFO) << "txn_id=" << txn_id << ", sub_txn_id=" << sub_txn_id
              << ", remove label_key=" << hex(label_key) << ", put info_key=" << hex(info_key)
              << ", put index_key=" << hex(index_key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit kv txn, txn_id=" << txn_id << " err=" << err;
        msg = ss.str();
        return;
    }
    response->set_sub_txn_id(sub_txn_id);
    response->mutable_txn_info()->CopyFrom(txn_info);
}

/**
 * 1. Modify the txn state of the txn_id:
 *    - Remove the table id from table_ids
 */
void MetaServiceImpl::abort_sub_txn(::google::protobuf::RpcController* controller,
                                    const AbortSubTxnRequest* request,
                                    AbortSubTxnResponse* response,
                                    ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(abort_sub_txn);
    int64_t txn_id = request->has_txn_id() ? request->txn_id() : -1;
    int64_t sub_txn_id = request->has_sub_txn_id() ? request->sub_txn_id() : -1;
    int64_t sub_txn_num = request->has_sub_txn_num() ? request->sub_txn_num() : -1;
    int64_t db_id = request->has_db_id() ? request->db_id() : -1;
    auto& table_ids = request->table_ids();
    if (txn_id < 0 || sub_txn_id < 0 || sub_txn_num < 0 || db_id < 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "invalid argument, txn_id=" << txn_id << ", sub_txn_id=" << sub_txn_id
           << ", sub_txn_num=" << sub_txn_num << " db_id=" << db_id << ", table_ids=[";
        for (auto table_id : table_ids) {
            ss << table_id << ", ";
        }
        ss << "]";
        msg = ss.str();
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }

    RPC_RATE_LIMIT(abort_sub_txn)
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "txn_kv_->create_txn() failed, err=" << err << " txn_id=" << txn_id
           << " sub_txn_id=" << sub_txn_id << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    // Get and update txn info with db_id and txn_id
    std::string info_val; // Will be reused when saving updated txn
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TXN_ID_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        ss << "failed to get txn_info, db_id=" << db_id << " txn_id=" << txn_id
           << " sub_txn_id=" << sub_txn_id << " err=" << err;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }
    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "failed to parse txn_info, db_id=" << db_id << " txn_id=" << txn_id
           << " sub_txn_id=" << sub_txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }
    DCHECK(txn_info.txn_id() == txn_id);
    if (txn_info.status() != TxnStatusPB::TXN_STATUS_PREPARED) {
        code = MetaServiceCode::TXN_INVALID_STATUS;
        ss << "transaction status is " << txn_info.status() << " : db_id=" << db_id
           << " txn_id=" << txn_id << " sub_txn_id=" << sub_txn_id;
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }

    // remove table_id and does not need to remove sub_txn_id
    if (txn_info.sub_txn_ids().size() != sub_txn_num) {
        code = MetaServiceCode::UNDEFINED_ERR;
        ss << "sub_txn_num mismatch, txn_id=" << txn_id << ", sub_txn_id=" << sub_txn_id
           << ", expected sub_txn_num=" << sub_txn_num << ", txn_info.sub_txn_ids=[";
        for (auto id : txn_info.sub_txn_ids()) {
            ss << id << ", ";
        }
        ss << "]";
        msg = ss.str();
        LOG(WARNING) << msg;
    }
    txn_info.mutable_table_ids()->Clear();
    for (auto table_id : table_ids) {
        txn_info.mutable_table_ids()->Add(table_id);
    }
    // TODO should we try to delete txn_label_key if begin_sub_txn failed to delete?

    if (!txn_info.SerializeToString(&info_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        ss << "failed to serialize txn_info when saving, txn_id=" << txn_id
           << " sub_txn_id=" << sub_txn_id;
        msg = ss.str();
        return;
    }

    txn->put(info_key, info_val);
    LOG(INFO) << "txn_id=" << txn_id << ", sub_txn_id=" << sub_txn_id
              << ", put info_key=" << hex(info_key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        ss << "failed to commit kv txn, txn_id=" << txn_id << ", sub_txn_id=" << sub_txn_id
           << ", err=" << err;
        msg = ss.str();
        return;
    }
    response->mutable_txn_info()->CopyFrom(txn_info);
}

void MetaServiceImpl::abort_txn_with_coordinator(::google::protobuf::RpcController* controller,
                                                 const AbortTxnWithCoordinatorRequest* request,
                                                 AbortTxnWithCoordinatorResponse* response,
                                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(abort_txn_with_coordinator);
    if (!request->has_id() || !request->has_ip() || !request->has_start_time()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid coordinate id, coordinate ip or coordinate start time.";
        return;
    }
    // TODO: For auth
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }
    RPC_RATE_LIMIT(abort_txn_with_coordinator);
    std::string begin_info_key = txn_info_key({instance_id, 0, 0});
    std::string end_info_key = txn_info_key({instance_id, INT64_MAX, INT64_MAX});
    LOG(INFO) << "begin_info_key:" << hex(begin_info_key) << " end_info_key:" << hex(end_info_key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        code = cast_as<ErrCategory::CREATE>(err);
        return;
    }
    std::unique_ptr<RangeGetIterator> it;
    int64_t abort_txn_cnt = 0;
    int64_t total_iteration_cnt = 0;
    bool need_commit = false;
    do {
        err = txn->get(begin_info_key, end_info_key, &it, true);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get txn info. err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        while (it->has_next()) {
            total_iteration_cnt++;
            auto [k, v] = it->next();
            VLOG_DEBUG << "check txn info txn_info_key=" << hex(k);
            TxnInfoPB info_pb;
            if (!info_pb.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed txn running info";
                msg = ss.str();
                ss << " key=" << hex(k);
                LOG(WARNING) << ss.str();
                return;
            }
            const auto& coordinate = info_pb.coordinator();
            if (info_pb.status() == TxnStatusPB::TXN_STATUS_PREPARED &&
                coordinate.sourcetype() == TXN_SOURCE_TYPE_BE && coordinate.id() == request->id() &&
                coordinate.ip() == request->ip() &&
                coordinate.start_time() < request->start_time()) {
                need_commit = true;
                TxnInfoPB return_txn_info;
                AbortTxnRequest request;
                request.set_db_id(info_pb.db_id());
                request.set_txn_id(info_pb.txn_id());
                request.set_label(info_pb.label());
                request.set_reason("Abort because coordinate be restart/stop");
                _abort_txn(instance_id, &request, txn.get(), return_txn_info, ss, code, msg);
            }
            if (!it->has_next()) {
                begin_info_key = k;
            }
        }
        begin_info_key.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());
    LOG(INFO) << "abort txn count: " << abort_txn_cnt
              << " total iteration count: " << total_iteration_cnt;
    if (need_commit) {
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            ss << "failed to abort txn kv, cooridnate_id=" << request->id()
               << " coordinate_ip=" << request->ip()
               << "coordinate_start_time=" << request->start_time() << " err=" << err;
            msg = ss.str();
            return;
        }
    }
}

std::string get_txn_info_key_from_txn_running_key(std::string_view txn_running_key) {
    std::string conflict_txn_info_key;
    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    txn_running_key.remove_prefix(1);
    int ret = decode_key(&txn_running_key, &out);
    if (ret != 0) [[unlikely]] {
        // decode version key error means this is something wrong,
        // we can not continue this txn
        LOG(WARNING) << "failed to decode key, ret=" << ret << " key=" << hex(txn_running_key);
    } else {
        DCHECK(out.size() == 5) << " key=" << hex(txn_running_key) << " " << out.size();
        const std::string& decode_instance_id = std::get<1>(std::get<0>(out[1]));
        int64_t db_id = std::get<0>(std::get<0>(out[3]));
        int64_t txn_id = std::get<0>(std::get<0>(out[4]));
        conflict_txn_info_key = txn_info_key({decode_instance_id, db_id, txn_id});
    }
    return conflict_txn_info_key;
}

void MetaServiceImpl::check_txn_conflict(::google::protobuf::RpcController* controller,
                                         const CheckTxnConflictRequest* request,
                                         CheckTxnConflictResponse* response,
                                         ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(check_txn_conflict);
    if (!request->has_db_id() || !request->has_end_txn_id() || (request->table_ids_size() <= 0)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid db id, end txn id or table_ids.";
        return;
    }
    // TODO: For auth
    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }
    RPC_RATE_LIMIT(check_txn_conflict)
    int64_t db_id = request->db_id();

    std::string begin_running_key = txn_running_key({instance_id, db_id, 0});
    std::string end_running_key = txn_running_key({instance_id, db_id, request->end_txn_id()});
    LOG(INFO) << "begin_running_key:" << hex(begin_running_key)
              << " end_running_key:" << hex(end_running_key);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        code = cast_as<ErrCategory::CREATE>(err);
        return;
    }

    //TODO: use set to replace
    std::vector<int64_t> src_table_ids(request->table_ids().begin(), request->table_ids().end());
    std::sort(src_table_ids.begin(), src_table_ids.end());
    std::unique_ptr<RangeGetIterator> it;
    int64_t skip_timeout_txn_cnt = 0;
    int total_iteration_cnt = 0;
    bool finished = true;
    do {
        err = txn->get(begin_running_key, end_running_key, &it, true);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "failed to get txn running info. err=" << err;
            msg = ss.str();
            LOG(WARNING) << msg;
            return;
        }

        VLOG_DEBUG << "begin_running_key=" << hex(begin_running_key)
                   << " end_running_key=" << hex(end_running_key)
                   << " it->has_next()=" << it->has_next();

        auto now_time = system_clock::now();
        uint64_t check_time = duration_cast<milliseconds>(now_time.time_since_epoch()).count();
        while (it->has_next()) {
            total_iteration_cnt++;
            auto [k, v] = it->next();
            LOG(INFO) << "check watermark conflict range_get txn_run_key=" << hex(k);
            TxnRunningPB running_pb;
            if (!running_pb.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                ss << "malformed txn running info";
                msg = ss.str();
                ss << " key=" << hex(k);
                LOG(WARNING) << ss.str();
                return;
            }

            if (running_pb.timeout_time() < check_time) {
                skip_timeout_txn_cnt++;
            } else {
                LOG(INFO) << "check watermark conflict range_get txn_run_key=" << hex(k) << " " << k
                          << " running_pb=" << running_pb.ShortDebugString();
                std::vector<int64_t> running_table_ids(running_pb.table_ids().begin(),
                                                       running_pb.table_ids().end());
                std::sort(running_table_ids.begin(), running_table_ids.end());
                std::vector<int64_t> result(
                        std::min(running_table_ids.size(), src_table_ids.size()));
                auto iter = std::set_intersection(src_table_ids.begin(), src_table_ids.end(),
                                                  running_table_ids.begin(),
                                                  running_table_ids.end(), result.begin());
                result.resize(iter - result.begin());
                if (!result.empty()) {
                    finished = false;
                    std::string conflict_txn_info_key = get_txn_info_key_from_txn_running_key(k);
                    if (!conflict_txn_info_key.empty()) {
                        std::string conflict_txn_info_val;
                        err = txn->get(conflict_txn_info_key, &conflict_txn_info_val);
                        if (err != TxnErrorCode::TXN_OK) {
                            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND
                                           ? MetaServiceCode::TXN_ID_NOT_FOUND
                                           : cast_as<ErrCategory::READ>(err);
                            ss << "failed to get txn_info, conflict_txn_info_key="
                               << hex(conflict_txn_info_key);
                            msg = ss.str();
                            LOG(WARNING) << msg;
                            return;
                        }
                        TxnInfoPB& conflict_txn_info = *response->add_conflict_txns();
                        if (!conflict_txn_info.ParseFromString(conflict_txn_info_val)) {
                            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                            ss << "failed to parse txn_info, conflict_txn_info_key="
                               << hex(conflict_txn_info_key);
                            msg = ss.str();
                            LOG(WARNING) << msg;
                            return;
                        }
                    }
                }
            }

            if (!it->has_next()) {
                begin_running_key = k;
            }
        }
        begin_running_key.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());
    LOG(INFO) << "skip timeout txn count: " << skip_timeout_txn_cnt
              << " conflict txn count: " << response->conflict_txns_size()
              << " total iteration count: " << total_iteration_cnt;
    response->set_finished(finished);
}

/**
 * @brief
 *
 * @param txn_kv
 * @param instance_id
 * @param db_id
 * @param label_key
 * @return TxnErrorCode
 */
TxnErrorCode internal_clean_label(std::shared_ptr<TxnKv> txn_kv, const std::string_view instance_id,
                                  int64_t db_id, const std::string_view label_key) {
    std::string label_val;
    TxnLabelPB label_pb;

    int64_t key_size = 0;
    int64_t val_size = 0;
    std::vector<int64_t> survival_txn_ids;
    std::vector<int64_t> clean_txn_ids;

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn. err=" << err << " db_id=" << db_id
                     << " label_key=" << hex(label_key);
        return err;
    }

    err = txn->get(label_key, &label_val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG(WARNING) << "failed to txn get err=" << err << " db_id=" << db_id
                     << " label_key=" << hex(label_key);
        return err;
    }
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG(INFO) << "txn get err=" << err << " db_id=" << db_id << " label_key=" << hex(label_key);
        return TxnErrorCode::TXN_OK;
    }

    if (label_val.size() <= VERSION_STAMP_LEN) {
        LOG(INFO) << "label_val.size()=" << label_val.size() << " db_id=" << db_id
                  << " label_key=" << hex(label_key);
        return TxnErrorCode::TXN_OK;
    }

    if (!label_pb.ParseFromArray(label_val.data(), label_val.size() - VERSION_STAMP_LEN)) {
        LOG(WARNING) << "failed to parse txn label"
                     << " db_id=" << db_id << " label_key=" << hex(label_key)
                     << " label_val.size()=" << label_val.size();
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }

    for (auto txn_id : label_pb.txn_ids()) {
        const std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        const std::string index_key = txn_index_key({instance_id, txn_id});
        const std::string info_key = txn_info_key({instance_id, db_id, txn_id});

        std::string info_val;
        err = txn->get(info_key, &info_val);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("info_key get failed")
                    .tag("info_key", hex(info_key))
                    .tag("label_key", hex(label_key))
                    .tag("db_id", db_id)
                    .tag("txn_id", txn_id)
                    .tag("err", err);
            return err;
        }

        TxnInfoPB txn_info;
        if (!txn_info.ParseFromString(info_val)) {
            LOG_WARNING("info_val parse failed")
                    .tag("info_key", hex(info_key))
                    .tag("label_key", hex(label_key))
                    .tag("db_id", db_id)
                    .tag("txn_id", txn_id)
                    .tag("size", info_val.size());
            return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
        }

        std::string recycle_val;
        if ((txn_info.status() != TxnStatusPB::TXN_STATUS_ABORTED) &&
            (txn_info.status() != TxnStatusPB::TXN_STATUS_VISIBLE)) {
            // txn status is not final status
            LOG(INFO) << "txn not final state, label_key=" << hex(label_key)
                      << " txn_id=" << txn_id;
            survival_txn_ids.push_back(txn_id);
            DCHECK_EQ(txn->get(recycle_key, &recycle_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
            continue;
        }

        DCHECK_EQ(txn->get(recycle_key, &recycle_val), TxnErrorCode::TXN_OK);
        DCHECK((txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) ||
               (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE));

        txn->remove(index_key);
        key_size += index_key.size();

        txn->remove(info_key);
        key_size += info_key.size();

        txn->remove(recycle_key);
        key_size += recycle_key.size();
        clean_txn_ids.push_back(txn_id);
        LOG(INFO) << "remove index_key=" << hex(index_key) << " info_key=" << hex(info_key)
                  << " recycle_key=" << hex(recycle_key);
    }
    if (label_pb.txn_ids().size() == clean_txn_ids.size()) {
        txn->remove(label_key);
        key_size += label_key.size();
        LOG(INFO) << "remove label_key=" << hex(label_key);
    } else {
        label_pb.clear_txn_ids();
        for (auto txn_id : survival_txn_ids) {
            label_pb.add_txn_ids(txn_id);
        }
        LOG(INFO) << "rewrite label_pb=" << label_pb.ShortDebugString();
        label_val.clear();
        if (!label_pb.SerializeToString(&label_val)) {
            LOG(INFO) << "failed to serialize label_pb=" << label_pb.ShortDebugString()
                      << " label_key=" << hex(label_key);
            return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
        }
        txn->atomic_set_ver_value(label_key, label_val);
        key_size += label_key.size();
        val_size += label_val.size();
    }

    err = txn->commit();
    TEST_SYNC_POINT_CALLBACK("internal_clean_label:err", &err);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(INFO) << fmt::format(
                "label_key={} key_size={} val_size={} label_pb={} clean_txn_ids={}", hex(label_key),
                key_size, val_size, label_pb.ShortDebugString(), fmt::join(clean_txn_ids, " "));
    }
    return err;
}

void MetaServiceImpl::clean_txn_label(::google::protobuf::RpcController* controller,
                                      const CleanTxnLabelRequest* request,
                                      CleanTxnLabelResponse* response,
                                      ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(clean_txn_label);
    if (!request->has_db_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "missing db id";
        LOG(WARNING) << msg;
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }
    RPC_RATE_LIMIT(clean_txn_label)
    const int64_t db_id = request->db_id();

    // clean label only by db_id
    if (request->labels().empty()) {
        std::string begin_label_key = txn_label_key({instance_id, db_id, ""});
        const std::string end_label_key = txn_label_key({instance_id, db_id + 1, ""});

        std::unique_ptr<RangeGetIterator> it;
        bool snapshot = true;
        int limit = 1000;
        TEST_SYNC_POINT_CALLBACK("clean_txn_label:limit", &limit);
        do {
            std::unique_ptr<Transaction> txn;
            auto err = txn_kv_->create_txn(&txn);
            if (err != TxnErrorCode::TXN_OK) {
                msg = "failed to create txn";
                code = cast_as<ErrCategory::CREATE>(err);
                LOG(INFO) << msg << " err=" << err << " begin=" << hex(begin_label_key)
                          << " end=" << hex(end_label_key);
                return;
            }

            err = txn->get(begin_label_key, end_label_key, &it, snapshot, limit);
            if (err != TxnErrorCode::TXN_OK) {
                msg = fmt::format("failed to txn range get, err={}", err);
                code = cast_as<ErrCategory::READ>(err);
                LOG(WARNING) << msg << " begin=" << hex(begin_label_key)
                             << " end=" << hex(end_label_key);
                return;
            }

            if (!it->has_next()) {
                LOG(INFO) << "no keys in the range. begin=" << hex(begin_label_key)
                          << " end=" << hex(end_label_key);
                break;
            }
            while (it->has_next()) {
                auto [k, v] = it->next();
                if (!it->has_next()) {
                    begin_label_key = k;
                    LOG(INFO) << "iterator has no more kvs. key=" << hex(k);
                }
                err = internal_clean_label(txn_kv_, instance_id, db_id, k);
                if (err != TxnErrorCode::TXN_OK) {
                    code = cast_as<ErrCategory::READ>(err);
                    msg = fmt::format("failed to clean txn label. err={}", err);
                    LOG(WARNING) << msg << " db_id=" << db_id;
                    return;
                }
            }
            begin_label_key.push_back('\x00');
        } while (it->more());
    } else {
        const std::string& label = request->labels(0);
        const std::string label_key = txn_label_key({instance_id, db_id, label});
        TxnErrorCode err = internal_clean_label(txn_kv_, instance_id, db_id, label_key);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to clean txn label. err={}", err);
            LOG(WARNING) << msg << " db_id=" << db_id << " label_key=" << hex(label_key);
            return;
        }
    }

    code = MetaServiceCode::OK;
}

// get txn id by label
// 1. When the requested status is not empty, return the txnid
//    corresponding to the status. There may be multiple
//    requested status, just match one.
// 2. When the requested status is empty, return the latest txnid.
void MetaServiceImpl::get_txn_id(::google::protobuf::RpcController* controller,
                                 const GetTxnIdRequest* request, GetTxnIdResponse* response,
                                 ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_txn_id);
    if (!request->has_db_id()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "missing db id";
        LOG(WARNING) << msg;
        return;
    }

    std::string cloud_unique_id = request->has_cloud_unique_id() ? request->cloud_unique_id() : "";
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        ss << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        LOG(WARNING) << msg;
        return;
    }
    RPC_RATE_LIMIT(get_txn_id)
    const int64_t db_id = request->db_id();
    std::string label = request->label();
    const std::string label_key = txn_label_key({instance_id, db_id, label});
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn. err=" << err << " db_id=" << db_id
                     << " label=" << label;
        code = cast_as<ErrCategory::CREATE>(err);
        ss << "txn_kv_->create_txn() failed, err=" << err << " label=" << label
           << " db_id=" << db_id;
        msg = ss.str();
        return;
    }

    std::string label_val;
    err = txn->get(label_key, &label_val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "txn->get failed(), err=" << err << " label=" << label;
        msg = ss.str();
        return;
    }

    if (label_val.size() <= VERSION_STAMP_LEN) {
        code = MetaServiceCode::TXN_ID_NOT_FOUND;
        ss << "transaction not found, label=" << label;
        return;
    }

    TxnLabelPB label_pb;
    //label_val.size() > VERSION_STAMP_LEN means label has previous txn ids.
    if (!label_pb.ParseFromArray(label_val.data(), label_val.size() - VERSION_STAMP_LEN)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        ss << "label_pb->ParseFromString() failed, label=" << label;
        msg = ss.str();
        return;
    }
    if (label_pb.txn_ids_size() == 0) {
        code = MetaServiceCode::TXN_ID_NOT_FOUND;
        ss << "transaction not found, label=" << label;
        msg = ss.str();
        return;
    }

    // find the latest txn
    if (request->txn_status_size() == 0) {
        response->set_txn_id(*label_pb.txn_ids().rbegin());
        return;
    }

    for (auto& cur_txn_id : label_pb.txn_ids()) {
        const std::string cur_info_key = txn_info_key({instance_id, db_id, cur_txn_id});
        std::string cur_info_val;
        err = txn->get(cur_info_key, &cur_info_val);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            code = cast_as<ErrCategory::READ>(err);
            ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " label=" << label
               << " err=" << err;
            msg = ss.str();
            return;
        }

        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            //label_to_idx and txn info inconsistency.
            code = MetaServiceCode::TXN_ID_NOT_FOUND;
            ss << "txn->get() failed, cur_txn_id=" << cur_txn_id << " label=" << label
               << " err=" << err;
            msg = ss.str();
            return;
        }

        TxnInfoPB cur_txn_info;
        if (!cur_txn_info.ParseFromString(cur_info_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            ss << "cur_txn_info->ParseFromString() failed, cur_txn_id=" << cur_txn_id
               << " label=" << label << " err=" << err;
            msg = ss.str();
            return;
        }

        VLOG_DEBUG << "cur_txn_info=" << cur_txn_info.ShortDebugString();
        for (auto txn_status : request->txn_status()) {
            if (cur_txn_info.status() == txn_status) {
                response->set_txn_id(cur_txn_id);
                return;
            }
        }
    }
    code = MetaServiceCode::TXN_ID_NOT_FOUND;
    ss << "transaction not found, label=" << label;
    msg = ss.str();
    return;
}

} // namespace doris::cloud
