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

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <chrono>
#include <cstddef>

#include "common/bvars.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "meta_service.h"

// Empty string not is not processed
template <typename T, size_t S>
static inline constexpr size_t get_file_name_offset(const T (&s)[S], size_t i = S - 1) {
    return (s[i] == '/' || s[i] == '\\') ? i + 1 : (i > 0 ? get_file_name_offset(s, i - 1) : 0);
}
#define SS (ss << &__FILE__[get_file_name_offset(__FILE__)] << ":" << __LINE__ << " ")
#define INSTANCE_LOG(severity) (LOG(severity) << '(' << instance_id << ')')

namespace doris::cloud {

static constexpr int COMPACTION_DELETE_BITMAP_LOCK_ID = -1;
static constexpr int SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID = -2;

void start_compaction_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                          std::unique_ptr<Transaction>& txn, const StartTabletJobRequest* request,
                          StartTabletJobResponse* response, std::string& instance_id,
                          bool& need_commit) {
    auto& compaction = request->job().compaction(0);
    if (!compaction.has_id() || compaction.id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no job id specified";
        return;
    }

    // check compaction_cnt to avoid compact on expired tablet cache
    if (!compaction.has_base_compaction_cnt() || !compaction.has_cumulative_compaction_cnt()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid compaction_cnt given";
        return;
    }

    if (compaction.expiration() <= 0 &&
        compaction.type() != TabletCompactionJobPB::EMPTY_CUMULATIVE) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid expiration given";
        return;
    }

    if (compaction.lease() <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid lease given";
        return;
    }

    int64_t table_id = request->job().idx().table_id();
    int64_t index_id = request->job().idx().index_id();
    int64_t partition_id = request->job().idx().partition_id();
    int64_t tablet_id = request->job().idx().tablet_id();
    std::string stats_key =
            stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string stats_val;
    TxnErrorCode err = txn->get(stats_key, &stats_val);
    if (err != TxnErrorCode::TXN_OK) {
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TABLET_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        SS << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? "not found" : "get kv error")
           << " when get tablet stats, tablet_id=" << tablet_id << " key=" << hex(stats_key)
           << " err=" << err;
        msg = ss.str();
        return;
    }
    TabletStatsPB stats;
    CHECK(stats.ParseFromString(stats_val));
    if (compaction.base_compaction_cnt() < stats.base_compaction_cnt() ||
        compaction.cumulative_compaction_cnt() < stats.cumulative_compaction_cnt()) {
        code = MetaServiceCode::STALE_TABLET_CACHE;
        SS << "could not perform compaction on expired tablet cache."
           << " req_base_compaction_cnt=" << compaction.base_compaction_cnt()
           << ", base_compaction_cnt=" << stats.base_compaction_cnt()
           << ", req_cumulative_compaction_cnt=" << compaction.cumulative_compaction_cnt()
           << ", cumulative_compaction_cnt=" << stats.cumulative_compaction_cnt();
        msg = ss.str();
        return;
    }

    auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    TabletJobInfoPB job_pb;
    err = txn->get(job_key, &job_val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        SS << "failed to get tablet job, instance_id=" << instance_id << " tablet_id=" << tablet_id
           << " key=" << hex(job_key) << " err=" << err;
        msg = ss.str();
        code = cast_as<ErrCategory::READ>(err);
        return;
    }
    while (err == TxnErrorCode::TXN_OK) {
        job_pb.ParseFromString(job_val);
        if (job_pb.compaction().empty()) {
            break;
        }
        auto& compactions = *job_pb.mutable_compaction();
        // Remove expired compaction jobs
        // clang-format off
        int64_t now = time(nullptr);
        compactions.erase(std::remove_if(compactions.begin(), compactions.end(), [&](auto& c) {
            DCHECK(c.expiration() > 0 || c.type() == TabletCompactionJobPB::EMPTY_CUMULATIVE) << proto_to_json(c);
            DCHECK(c.lease() > 0) << proto_to_json(c);
            if (c.expiration() > 0 && c.expiration() < now) {
                INSTANCE_LOG(INFO)
                        << "got an expired job. job=" << proto_to_json(c) << " now=" << now;
                return true;
            }
            if (c.lease() > 0 && c.lease() < now) {
                INSTANCE_LOG(INFO)
                        << "got a job exceeding lease. job=" << proto_to_json(c) << " now=" << now;
                return true;
            }
            return false;
        }), compactions.end());
        // clang-format on
        // Check conflict job
        if (compaction.type() == TabletCompactionJobPB::FULL) {
            // Full compaction is generally used for data correctness repair
            // for MOW table, so priority should be given to performing full
            // compaction operations and canceling other types of compaction.
            compactions.Clear();
        } else if (compaction.input_versions().empty()) {
            // Unknown input version range, doesn't support parallel compaction of same type
            for (auto& c : compactions) {
                if (c.type() != compaction.type() && c.type() != TabletCompactionJobPB::FULL)
                    continue;
                if (c.id() == compaction.id()) return; // Same job, return OK to keep idempotency
                msg = fmt::format("compaction has already started, tablet_id={} job={}", tablet_id,
                                  proto_to_json(c));
                code = MetaServiceCode::JOB_TABLET_BUSY;
                return;
            }
        } else {
            DCHECK_EQ(compaction.input_versions_size(), 2) << proto_to_json(compaction);
            DCHECK_LE(compaction.input_versions(0), compaction.input_versions(1))
                    << proto_to_json(compaction);
            auto version_not_conflict = [](const TabletCompactionJobPB& a,
                                           const TabletCompactionJobPB& b) {
                return a.input_versions(0) > b.input_versions(1) ||
                       a.input_versions(1) < b.input_versions(0);
            };
            for (auto& c : compactions) {
                if (c.type() != compaction.type() && c.type() != TabletCompactionJobPB::FULL)
                    continue;
                if (c.input_versions_size() > 0 && version_not_conflict(c, compaction)) continue;
                if (c.id() == compaction.id()) return; // Same job, return OK to keep idempotency
                msg = fmt::format("compaction has already started, tablet_id={} job={}", tablet_id,
                                  proto_to_json(c));
                code = MetaServiceCode::JOB_TABLET_BUSY;
                // Unknown version range of started compaction, BE should not retry other version range
                if (c.input_versions_size() == 0) return;
                // Notify version ranges in started compaction to BE, so BE can retry other version range
                for (auto& c : compactions) {
                    if (c.type() == compaction.type() || c.type() == TabletCompactionJobPB::FULL) {
                        // If there are multiple started compaction of same type, they all must has input version range
                        DCHECK_EQ(c.input_versions_size(), 2) << proto_to_json(c);
                        response->add_version_in_compaction(c.input_versions(0));
                        response->add_version_in_compaction(c.input_versions(1));
                    }
                }
                return;
            }
        }
        break;
    }
    if (!job_pb.has_idx()) {
        job_pb.mutable_idx()->CopyFrom(request->job().idx());
    }
    job_pb.add_compaction()->CopyFrom(compaction);
    job_pb.SerializeToString(&job_val);
    if (job_val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "pb serialization error";
        return;
    }
    INSTANCE_LOG(INFO) << "compaction job to save job=" << proto_to_json(compaction);
    txn->put(job_key, job_val);
    need_commit = true;
}

void start_schema_change_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                             std::unique_ptr<Transaction>& txn,
                             const StartTabletJobRequest* request, std::string& instance_id,
                             bool& need_commit) {
    auto& schema_change = request->job().schema_change();
    if (!schema_change.has_id() || schema_change.id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no job id specified";
        return;
    }
    if (!schema_change.has_initiator()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no initiator specified";
        return;
    }

    // check new_tablet state
    int64_t new_tablet_id = schema_change.new_tablet_idx().tablet_id();
    if (new_tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid new_tablet_id given";
        return;
    }
    int64_t table_id = request->job().idx().table_id();
    int64_t index_id = request->job().idx().index_id();
    int64_t partition_id = request->job().idx().partition_id();
    int64_t tablet_id = request->job().idx().tablet_id();
    if (new_tablet_id == tablet_id) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "not allow new_tablet_id same with base_tablet_id";
        return;
    }
    auto& new_tablet_idx = const_cast<TabletIndexPB&>(schema_change.new_tablet_idx());
    if (!new_tablet_idx.has_table_id() || !new_tablet_idx.has_index_id() ||
        !new_tablet_idx.has_partition_id()) {
        get_tablet_idx(code, msg, txn.get(), instance_id, new_tablet_id, new_tablet_idx);
        if (code != MetaServiceCode::OK) return;
    }
    MetaTabletKeyInfo new_tablet_key_info {instance_id, new_tablet_idx.table_id(),
                                           new_tablet_idx.index_id(), new_tablet_idx.partition_id(),
                                           new_tablet_id};
    std::string new_tablet_key;
    std::string new_tablet_val;
    doris::TabletMetaCloudPB new_tablet_meta;
    meta_tablet_key(new_tablet_key_info, &new_tablet_key);
    TxnErrorCode err = txn->get(new_tablet_key, &new_tablet_val);
    if (err != TxnErrorCode::TXN_OK) {
        SS << "failed to get new tablet meta"
           << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? " (not found)" : "")
           << " instance_id=" << instance_id << " tablet_id=" << new_tablet_id
           << " key=" << hex(new_tablet_key) << " err=" << err;
        msg = ss.str();
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TABLET_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        return;
    }
    if (!new_tablet_meta.ParseFromString(new_tablet_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "malformed tablet meta";
        return;
    }

    if (new_tablet_meta.tablet_state() == doris::TabletStatePB::PB_RUNNING) {
        code = MetaServiceCode::JOB_ALREADY_SUCCESS;
        msg = "schema_change job already success";
        return;
    }
    if (!new_tablet_meta.has_tablet_state() ||
        new_tablet_meta.tablet_state() != doris::TabletStatePB::PB_NOTREADY) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid new tablet state";
        return;
    }

    auto job_key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    std::string job_val;
    TabletJobInfoPB job_pb;
    err = txn->get(job_key, &job_val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        SS << "failed to get tablet job, instance_id=" << instance_id << " tablet_id=" << tablet_id
           << " key=" << hex(job_key) << " err=" << err;
        msg = ss.str();
        code = cast_as<ErrCategory::READ>(err);
        return;
    }
    job_pb.mutable_idx()->CopyFrom(request->job().idx());
    // FE can ensure that a tablet does not have more than one schema_change job at the same time,
    // so we can directly preempt previous schema_change job.
    job_pb.mutable_schema_change()->CopyFrom(schema_change);
    job_pb.SerializeToString(&job_val);
    if (job_val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "pb serialization error";
        return;
    }
    INSTANCE_LOG(INFO) << "schema_change job to save job=" << proto_to_json(schema_change);
    txn->put(job_key, job_val);
    need_commit = true;
}

void MetaServiceImpl::start_tablet_job(::google::protobuf::RpcController* controller,
                                       const StartTabletJobRequest* request,
                                       StartTabletJobResponse* response,
                                       ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(start_tablet_job);
    std::string cloud_unique_id = request->cloud_unique_id();
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        SS << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        return;
    }
    RPC_RATE_LIMIT(start_tablet_job)
    if (!request->has_job() ||
        (request->job().compaction().empty() && !request->job().has_schema_change())) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid job specified";
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    int64_t tablet_id = request->job().idx().tablet_id();
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }
    auto& tablet_idx = const_cast<TabletIndexPB&>(request->job().idx());
    if (!tablet_idx.has_table_id() || !tablet_idx.has_index_id() ||
        !tablet_idx.has_partition_id()) {
        get_tablet_idx(code, msg, txn.get(), instance_id, tablet_id, tablet_idx);
        if (code != MetaServiceCode::OK) return;
    }
    // Check if tablet has been dropped
    if (is_dropped_tablet(txn.get(), instance_id, tablet_idx.index_id(),
                          tablet_idx.partition_id())) {
        code = MetaServiceCode::TABLET_NOT_FOUND;
        msg = fmt::format("tablet {} has been dropped", tablet_id);
        return;
    }

    bool need_commit = false;
    std::unique_ptr<int, std::function<void(int*)>> defer_commit(
            (int*)0x01, [&ss, &txn, &code, &msg, &need_commit](int*) {
                if (!need_commit) return;
                TxnErrorCode err = txn->commit();
                if (err != TxnErrorCode::TXN_OK) {
                    code = cast_as<ErrCategory::COMMIT>(err);
                    ss << "failed to commit job kv, err=" << err;
                    msg = ss.str();
                    return;
                }
            });

    if (!request->job().compaction().empty()) {
        start_compaction_job(code, msg, ss, txn, request, response, instance_id, need_commit);
        return;
    }

    if (request->job().has_schema_change()) {
        start_schema_change_job(code, msg, ss, txn, request, instance_id, need_commit);
        return;
    }
}

static bool check_and_remove_delete_bitmap_update_lock(MetaServiceCode& code, std::string& msg,
                                                       std::stringstream& ss,
                                                       std::unique_ptr<Transaction>& txn,
                                                       std::string& instance_id, int64_t table_id,
                                                       int64_t lock_id, int64_t lock_initiator) {
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
    std::string lock_val;
    TxnErrorCode err = txn->get(lock_key, &lock_val);
    LOG(INFO) << "get delete bitmap update lock info, table_id=" << table_id
              << " key=" << hex(lock_key) << " err=" << err;
    if (err != TxnErrorCode::TXN_OK) {
        ss << "failed to get delete bitmap update lock key, instance_id=" << instance_id
           << " table_id=" << table_id << " key=" << hex(lock_key) << " err=" << err;
        msg = ss.str();
        code = cast_as<ErrCategory::READ>(err);
        return false;
    }
    DeleteBitmapUpdateLockPB lock_info;
    if (!lock_info.ParseFromString(lock_val)) [[unlikely]] {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse DeleteBitmapUpdateLockPB";
        return false;
    }
    if (lock_info.lock_id() != lock_id) {
        msg = "lock id not match";
        code = MetaServiceCode::LOCK_EXPIRED;
        return false;
    }
    bool found = false;
    auto initiators = lock_info.mutable_initiators();
    for (auto iter = initiators->begin(); iter != initiators->end(); iter++) {
        if (*iter == lock_initiator) {
            initiators->erase(iter);
            found = true;
            break;
        }
    }
    if (!found) {
        msg = "lock initiator not exist";
        code = MetaServiceCode::LOCK_EXPIRED;
        return false;
    }
    if (initiators->empty()) {
        INSTANCE_LOG(INFO) << "remove delete bitmap lock, table_id=" << table_id
                           << " key=" << hex(lock_key);
        txn->remove(lock_key);
        return true;
    }
    lock_info.SerializeToString(&lock_val);
    if (lock_val.empty()) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "pb serialization error";
        return false;
    }
    INSTANCE_LOG(INFO) << "remove delete bitmap lock initiator, table_id=" << table_id
                       << ", key=" << hex(lock_key) << ", initiator=" << lock_initiator
                       << " initiators_size=" << lock_info.initiators_size();
    txn->put(lock_key, lock_val);
    return true;
}

static void remove_delete_bitmap_update_lock(std::unique_ptr<Transaction>& txn,
                                             const std::string& instance_id, int64_t table_id,
                                             int64_t lock_id, int64_t lock_initiator) {
    std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id, table_id, -1});
    std::string lock_val;
    TxnErrorCode err = txn->get(lock_key, &lock_val);
    LOG(INFO) << "get delete bitmap update lock info, table_id=" << table_id
              << " key=" << hex(lock_key) << " err=" << err;
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to get delete bitmap update lock key, instance_id=" << instance_id
                     << " table_id=" << table_id << " key=" << hex(lock_key) << " err=" << err;
        return;
    }
    DeleteBitmapUpdateLockPB lock_info;
    if (!lock_info.ParseFromString(lock_val)) [[unlikely]] {
        LOG(WARNING) << "failed to parse DeleteBitmapUpdateLockPB, instance_id=" << instance_id
                     << " table_id=" << table_id << " key=" << hex(lock_key);
        return;
    }
    if (lock_info.lock_id() != lock_id) {
        return;
    }
    bool found = false;
    auto initiators = lock_info.mutable_initiators();
    for (auto iter = initiators->begin(); iter != initiators->end(); iter++) {
        if (*iter == lock_initiator) {
            initiators->erase(iter);
            found = true;
            break;
        }
    }
    if (!found) {
        return;
    }
    if (initiators->empty()) {
        INSTANCE_LOG(INFO) << "remove delete bitmap lock, table_id=" << table_id
                           << " key=" << hex(lock_key);
        txn->remove(lock_key);
        return;
    }
    lock_info.SerializeToString(&lock_val);
    if (lock_val.empty()) {
        INSTANCE_LOG(WARNING) << "failed to seiralize lock_info, table_id=" << table_id
                              << " key=" << hex(lock_key);
        return;
    }
    INSTANCE_LOG(INFO) << "remove delete bitmap lock initiator, table_id=" << table_id
                       << ", key=" << hex(lock_key) << ", initiator=" << lock_initiator
                       << " initiators_size=" << lock_info.initiators_size();
    txn->put(lock_key, lock_val);
}

void process_compaction_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                            std::unique_ptr<Transaction>& txn,
                            const FinishTabletJobRequest* request,
                            FinishTabletJobResponse* response, TabletJobInfoPB& recorded_job,
                            std::string& instance_id, std::string& job_key, bool& need_commit) {
    //==========================================================================
    //                                check
    //==========================================================================
    int64_t table_id = request->job().idx().table_id();
    int64_t index_id = request->job().idx().index_id();
    int64_t partition_id = request->job().idx().partition_id();
    int64_t tablet_id = request->job().idx().tablet_id();
    if (recorded_job.compaction().empty()) {
        SS << "there is no running compaction, tablet_id=" << tablet_id;
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    auto& compaction = request->job().compaction(0);

    auto recorded_compaction = recorded_job.mutable_compaction()->begin();
    for (; recorded_compaction != recorded_job.mutable_compaction()->end(); ++recorded_compaction) {
        if (recorded_compaction->id() == compaction.id()) break;
    }
    if (recorded_compaction == recorded_job.mutable_compaction()->end()) {
        SS << "unmatched job id, recorded_job=" << proto_to_json(recorded_job)
           << " given_job=" << proto_to_json(compaction);
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = ss.str();
        return;
    }

    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    if (recorded_compaction->expiration() > 0 && recorded_compaction->expiration() < now) {
        code = MetaServiceCode::JOB_EXPIRED;
        SS << "expired compaction job, tablet_id=" << tablet_id
           << " job=" << proto_to_json(*recorded_compaction);
        msg = ss.str();
        // FIXME: Just remove or notify to abort?
        // LOG(INFO) << "remove expired job, tablet_id=" << tablet_id << " key=" << hex(job_key);
        return;
    }

    if (request->action() != FinishTabletJobRequest::COMMIT &&
        request->action() != FinishTabletJobRequest::ABORT &&
        request->action() != FinishTabletJobRequest::LEASE) {
        SS << "unsupported action, tablet_id=" << tablet_id << " action=" << request->action();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    //==========================================================================
    //                               Lease
    //==========================================================================
    if (request->action() == FinishTabletJobRequest::LEASE) {
        if (compaction.lease() <= 0 || recorded_compaction->lease() > compaction.lease()) {
            ss << "invalid lease. recoreded_lease=" << recorded_compaction->lease()
               << " req_lease=" << compaction.lease();
            msg = ss.str();
            code = MetaServiceCode::INVALID_ARGUMENT;
            return;
        }
        recorded_compaction->set_lease(compaction.lease());
        auto job_val = recorded_job.SerializeAsString();
        txn->put(job_key, job_val);
        INSTANCE_LOG(INFO) << "lease tablet compaction job, tablet_id=" << tablet_id
                           << " key=" << hex(job_key);
        need_commit = true;
        return;
    }

    //==========================================================================
    //                               Abort
    //==========================================================================
    if (request->action() == FinishTabletJobRequest::ABORT) {
        // TODO(gavin): mv tmp rowsets to recycle or remove them directly
        recorded_job.mutable_compaction()->erase(recorded_compaction);
        auto job_val = recorded_job.SerializeAsString();
        txn->put(job_key, job_val);
        INSTANCE_LOG(INFO) << "abort tablet compaction job, tablet_id=" << tablet_id
                           << " key=" << hex(job_key);
        if (compaction.has_delete_bitmap_lock_initiator()) {
            remove_delete_bitmap_update_lock(txn, instance_id, table_id,
                                             COMPACTION_DELETE_BITMAP_LOCK_ID,
                                             compaction.delete_bitmap_lock_initiator());
        }
        need_commit = true;
        return;
    }

    //==========================================================================
    //                               Commit
    //==========================================================================
    //
    // 1. update tablet stats
    // 2. move compaction input rowsets to recycle
    // 3. change tmp rowset to formal rowset
    // 4. remove compaction job
    //
    //==========================================================================
    //                          Update tablet stats
    //==========================================================================
    auto stats = response->mutable_stats();
    TabletStats detached_stats;
    // ATTN: The condition that snapshot read can be used to get tablet stats is: all other transactions that put tablet stats
    //  can make read write conflicts with this transaction on other keys. Currently, if all meta-service nodes are running
    //  with `config::split_tablet_stats = true` can meet the condition.
    internal_get_tablet_stats(code, msg, txn.get(), instance_id, request->job().idx(), *stats,
                              detached_stats, config::snapshot_get_tablet_stats);
    if (compaction.type() == TabletCompactionJobPB::EMPTY_CUMULATIVE) {
        stats->set_cumulative_compaction_cnt(stats->cumulative_compaction_cnt() + 1);
        stats->set_cumulative_point(compaction.output_cumulative_point());
        stats->set_last_cumu_compaction_time_ms(now * 1000);
    } else if (compaction.type() == TabletCompactionJobPB::CUMULATIVE) {
        // clang-format off
        stats->set_cumulative_compaction_cnt(stats->cumulative_compaction_cnt() + 1);
        if (compaction.output_cumulative_point() > stats->cumulative_point()) {
            // After supporting parallel cumu compaction, compaction with older cumu point may be committed after
            // new cumu point has been set, MUST NOT set cumu point back to old value
            stats->set_cumulative_point(compaction.output_cumulative_point());
        }
        stats->set_num_rows(stats->num_rows() + (compaction.num_output_rows() - compaction.num_input_rows()));
        stats->set_data_size(stats->data_size() + (compaction.size_output_rowsets() - compaction.size_input_rowsets()));
        stats->set_num_rowsets(stats->num_rowsets() + (compaction.num_output_rowsets() - compaction.num_input_rowsets()));
        stats->set_num_segments(stats->num_segments() + (compaction.num_output_segments() - compaction.num_input_segments()));
        stats->set_last_cumu_compaction_time_ms(now * 1000);
        // clang-format on
    } else if (compaction.type() == TabletCompactionJobPB::BASE) {
        // clang-format off
        stats->set_base_compaction_cnt(stats->base_compaction_cnt() + 1);
        stats->set_num_rows(stats->num_rows() + (compaction.num_output_rows() - compaction.num_input_rows()));
        stats->set_data_size(stats->data_size() + (compaction.size_output_rowsets() - compaction.size_input_rowsets()));
        stats->set_num_rowsets(stats->num_rowsets() + (compaction.num_output_rowsets() - compaction.num_input_rowsets()));
        stats->set_num_segments(stats->num_segments() + (compaction.num_output_segments() - compaction.num_input_segments()));
        stats->set_last_base_compaction_time_ms(now * 1000);
        // clang-format on
    } else if (compaction.type() == TabletCompactionJobPB::FULL) {
        // clang-format off
        stats->set_base_compaction_cnt(stats->base_compaction_cnt() + 1);
        if (compaction.output_cumulative_point() > stats->cumulative_point()) {
            // After supporting parallel cumu compaction, compaction with older cumu point may be committed after
            // new cumu point has been set, MUST NOT set cumu point back to old value
            stats->set_cumulative_point(compaction.output_cumulative_point());
        }
        stats->set_num_rows(stats->num_rows() + (compaction.num_output_rows() - compaction.num_input_rows()));
        stats->set_data_size(stats->data_size() + (compaction.size_output_rowsets() - compaction.size_input_rowsets()));
        stats->set_num_rowsets(stats->num_rowsets() + (compaction.num_output_rowsets() - compaction.num_input_rowsets()));
        stats->set_num_segments(stats->num_segments() + (compaction.num_output_segments() - compaction.num_input_segments()));
        stats->set_last_full_compaction_time_ms(now * 1000);
        // clang-format on
    } else {
        msg = "invalid compaction type";
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    auto stats_key = stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    auto stats_val = stats->SerializeAsString();
    txn->put(stats_key, stats_val);
    merge_tablet_stats(*stats, detached_stats);
    if (stats->data_size() < 0 || stats->num_rowsets() < 1) [[unlikely]] {
        INSTANCE_LOG(ERROR) << "buggy data size, tablet_id=" << tablet_id
                            << " stats.data_size=" << stats->data_size()
                            << " compaction.size_output_rowsets="
                            << compaction.size_output_rowsets()
                            << " compaction.size_input_rowsets= "
                            << compaction.size_input_rowsets();
        DCHECK(false) << "buggy data size";
    }

    VLOG_DEBUG << "update tablet stats tablet_id=" << tablet_id << " key=" << hex(stats_key)
               << " stats=" << proto_to_json(*stats);
    if (compaction.type() == TabletCompactionJobPB::EMPTY_CUMULATIVE) {
        recorded_job.mutable_compaction()->erase(recorded_compaction);
        auto job_val = recorded_job.SerializeAsString();
        txn->put(job_key, job_val);
        INSTANCE_LOG(INFO) << "remove compaction job, tablet_id=" << tablet_id
                           << " key=" << hex(job_key);
        need_commit = true;
        return;
    }

    // remove delete bitmap update lock for MoW table
    if (compaction.has_delete_bitmap_lock_initiator()) {
        bool success = check_and_remove_delete_bitmap_update_lock(
                code, msg, ss, txn, instance_id, table_id, COMPACTION_DELETE_BITMAP_LOCK_ID,
                compaction.delete_bitmap_lock_initiator());
        if (!success) {
            return;
        }
    }

    //==========================================================================
    //                    Move input rowsets to recycle
    //==========================================================================
    if (compaction.input_versions_size() != 2 || compaction.output_versions_size() != 1 ||
        compaction.output_rowset_ids_size() != 1) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        SS << "invalid input or output versions, input_versions_size="
           << compaction.input_versions_size()
           << " output_versions_size=" << compaction.output_versions_size()
           << " output_rowset_ids_size=" << compaction.output_rowset_ids_size();
        msg = ss.str();
        return;
    }

    auto start = compaction.input_versions(0);
    auto end = compaction.input_versions(1);
    auto rs_start = meta_rowset_key({instance_id, tablet_id, start});
    auto rs_end = meta_rowset_key({instance_id, tablet_id, end + 1});

    std::unique_ptr<RangeGetIterator> it;
    int num_rowsets = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [&rs_start, &rs_end, &num_rowsets, &instance_id](int*) {
                INSTANCE_LOG(INFO) << "get rowset meta, num_rowsets=" << num_rowsets << " range=["
                                   << hex(rs_start) << "," << hex(rs_end) << "]";
            });

    auto rs_start1 = rs_start;
    do {
        TxnErrorCode err = txn->get(rs_start1, rs_end, &it);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            SS << "internal error, failed to get rowset range, err=" << err
               << " tablet_id=" << tablet_id << " range=[" << hex(rs_start1) << ", << "
               << hex(rs_end) << ")";
            msg = ss.str();
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();

            doris::RowsetMetaCloudPB rs;
            if (!rs.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                SS << "malformed rowset meta, unable to deserialize, tablet_id=" << tablet_id
                   << " key=" << hex(k);
                msg = ss.str();
                return;
            }

            // remove delete bitmap of input rowset for MoW table
            if (compaction.has_delete_bitmap_lock_initiator()) {
                auto delete_bitmap_start =
                        meta_delete_bitmap_key({instance_id, tablet_id, rs.rowset_id_v2(), 0, 0});
                auto delete_bitmap_end = meta_delete_bitmap_key(
                        {instance_id, tablet_id, rs.rowset_id_v2(), INT64_MAX, INT64_MAX});
                txn->remove(delete_bitmap_start, delete_bitmap_end);
            }

            auto recycle_key = recycle_rowset_key({instance_id, tablet_id, rs.rowset_id_v2()});
            RecycleRowsetPB recycle_rowset;
            recycle_rowset.set_creation_time(now);
            recycle_rowset.mutable_rowset_meta()->CopyFrom(rs);
            recycle_rowset.set_type(RecycleRowsetPB::COMPACT);
            auto recycle_val = recycle_rowset.SerializeAsString();
            txn->put(recycle_key, recycle_val);
            INSTANCE_LOG(INFO) << "put recycle rowset, tablet_id=" << tablet_id
                               << " key=" << hex(recycle_key);

            ++num_rowsets;
            if (!it->has_next()) rs_start1 = k;
        }
        rs_start1.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    txn->remove(rs_start, rs_end);

    TEST_SYNC_POINT_CALLBACK("process_compaction_job::loop_input_done", &num_rowsets);

    if (num_rowsets < 1) {
        SS << "too few input rowsets, tablet_id=" << tablet_id << " num_rowsets=" << num_rowsets;
        code = MetaServiceCode::UNDEFINED_ERR;
        msg = ss.str();
        recorded_job.mutable_compaction()->erase(recorded_compaction);
        auto job_val = recorded_job.SerializeAsString();
        txn->put(job_key, job_val);
        INSTANCE_LOG(INFO) << "remove compaction job, tablet_id=" << tablet_id
                           << " key=" << hex(job_key);
        need_commit = true;
        TEST_SYNC_POINT_CALLBACK("process_compaction_job::too_few_rowsets", &need_commit);
        return;
    }

    //==========================================================================
    //                Change tmp rowset to formal rowset
    //==========================================================================
    if (compaction.txn_id_size() != 1) {
        SS << "invalid txn_id, txn_id_size=" << compaction.txn_id_size();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    int64_t txn_id = compaction.txn_id(0);
    auto& rowset_id = compaction.output_rowset_ids(0);
    if (txn_id <= 0 || rowset_id.empty()) {
        SS << "invalid txn_id or rowset_id, tablet_id=" << tablet_id << " txn_id=" << txn_id
           << " rowset_id=" << rowset_id;
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    auto tmp_rowset_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
    std::string tmp_rowset_val;
    TxnErrorCode err = txn->get(tmp_rowset_key, &tmp_rowset_val);
    if (err != TxnErrorCode::TXN_OK) {
        SS << "failed to get tmp rowset key"
           << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? " (not found)" : "")
           << ", tablet_id=" << tablet_id << " tmp_rowset_key=" << hex(tmp_rowset_key)
           << ", err=" << err;
        msg = ss.str();
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::UNDEFINED_ERR
                                                      : cast_as<ErrCategory::READ>(err);
        return;
    }

    // We don't actually need to parse the rowset meta
    doris::RowsetMetaCloudPB rs_meta;
    rs_meta.ParseFromString(tmp_rowset_val);
    if (rs_meta.txn_id() <= 0) {
        SS << "invalid txn_id in output tmp rowset meta, tablet_id=" << tablet_id
           << " txn_id=" << rs_meta.txn_id();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    txn->remove(tmp_rowset_key);
    INSTANCE_LOG(INFO) << "remove tmp rowset meta, tablet_id=" << tablet_id
                       << " tmp_rowset_key=" << hex(tmp_rowset_key);

    int64_t version = compaction.output_versions(0);
    auto rowset_key = meta_rowset_key({instance_id, tablet_id, version});
    txn->put(rowset_key, tmp_rowset_val);
    INSTANCE_LOG(INFO) << "put rowset meta, tablet_id=" << tablet_id
                       << " rowset_key=" << hex(rowset_key);

    //==========================================================================
    //                      Remove compaction job
    //==========================================================================
    // TODO(gavin): move deleted job info into recycle or history
    recorded_job.mutable_compaction()->erase(recorded_compaction);
    auto job_val = recorded_job.SerializeAsString();
    txn->put(job_key, job_val);
    INSTANCE_LOG(INFO) << "remove compaction job tabelt_id=" << tablet_id
                       << " key=" << hex(job_key);

    need_commit = true;
}

void process_schema_change_job(MetaServiceCode& code, std::string& msg, std::stringstream& ss,
                               std::unique_ptr<Transaction>& txn,
                               const FinishTabletJobRequest* request,
                               FinishTabletJobResponse* response, TabletJobInfoPB& recorded_job,
                               std::string& instance_id, std::string& job_key, bool& need_commit) {
    //==========================================================================
    //                                check
    //==========================================================================
    int64_t tablet_id = request->job().idx().tablet_id();
    auto& schema_change = request->job().schema_change();
    int64_t new_tablet_id = schema_change.new_tablet_idx().tablet_id();
    if (new_tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid new_tablet_id given";
        return;
    }
    if (new_tablet_id == tablet_id) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "not allow new_tablet_id same with base_tablet_id";
        return;
    }
    auto& new_tablet_idx = const_cast<TabletIndexPB&>(schema_change.new_tablet_idx());
    if (!new_tablet_idx.has_table_id() || !new_tablet_idx.has_index_id() ||
        !new_tablet_idx.has_partition_id()) {
        get_tablet_idx(code, msg, txn.get(), instance_id, new_tablet_id, new_tablet_idx);
        if (code != MetaServiceCode::OK) return;
    }
    int64_t new_table_id = new_tablet_idx.table_id();
    int64_t new_index_id = new_tablet_idx.index_id();
    int64_t new_partition_id = new_tablet_idx.partition_id();

    auto new_tablet_key = meta_tablet_key(
            {instance_id, new_table_id, new_index_id, new_partition_id, new_tablet_id});
    std::string new_tablet_val;
    doris::TabletMetaCloudPB new_tablet_meta;
    TxnErrorCode err = txn->get(new_tablet_key, &new_tablet_val);
    if (err != TxnErrorCode::TXN_OK) {
        SS << "failed to get new tablet meta"
           << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? " (not found)" : "")
           << " instance_id=" << instance_id << " tablet_id=" << new_tablet_id
           << " key=" << hex(new_tablet_key) << " err=" << err;
        msg = ss.str();
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::TABLET_NOT_FOUND
                                                      : cast_as<ErrCategory::READ>(err);
        return;
    }
    if (!new_tablet_meta.ParseFromString(new_tablet_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "malformed tablet meta";
        return;
    }

    if (new_tablet_meta.tablet_state() == doris::TabletStatePB::PB_RUNNING) {
        code = MetaServiceCode::JOB_ALREADY_SUCCESS;
        msg = "schema_change job already success";
        return;
    }
    if (!new_tablet_meta.has_tablet_state() ||
        new_tablet_meta.tablet_state() != doris::TabletStatePB::PB_NOTREADY) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid new tablet state";
        return;
    }

    if (!recorded_job.has_schema_change()) {
        SS << "there is no running schema_change, tablet_id=" << tablet_id;
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }
    auto& recorded_schema_change = recorded_job.schema_change();
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    if (recorded_schema_change.expiration() > 0 && recorded_schema_change.expiration() < now) {
        code = MetaServiceCode::JOB_EXPIRED;
        SS << "expired schema_change job, tablet_id=" << tablet_id
           << " job=" << proto_to_json(recorded_schema_change);
        msg = ss.str();
        // FIXME: Just remove or notify to abort?
        // LOG(INFO) << "remove expired job, tablet_id=" << tablet_id << " key=" << hex(job_key);
        return;
    }

    // MUST check initiator to let the retried BE commit this schema_change job.
    if (schema_change.id() != recorded_schema_change.id() ||
        schema_change.initiator() != recorded_schema_change.initiator()) {
        SS << "unmatched job id or initiator, recorded_id=" << recorded_schema_change.id()
           << " given_id=" << schema_change.id()
           << " recorded_job=" << proto_to_json(recorded_schema_change)
           << " given_job=" << proto_to_json(schema_change);
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = ss.str();
        return;
    }

    if (request->action() != FinishTabletJobRequest::COMMIT &&
        request->action() != FinishTabletJobRequest::ABORT) {
        SS << "unsupported action, tablet_id=" << tablet_id << " action=" << request->action();
        msg = ss.str();
        code = MetaServiceCode::INVALID_ARGUMENT;
        return;
    }

    //==========================================================================
    //                               Abort
    //==========================================================================
    if (request->action() == FinishTabletJobRequest::ABORT) {
        // TODO(cyx)
        return;
    }

    //==========================================================================
    //                               Commit
    //==========================================================================
    //
    // 1. update new_tablet meta
    // 2. move rowsets [2-alter_version] in new_tablet to recycle
    // 3. update new_tablet stats
    // 4. change tmp rowset to formal rowset
    // 5. remove schema_change job (unnecessary)
    //
    //==========================================================================
    //                          update tablet meta
    //==========================================================================
    new_tablet_meta.set_tablet_state(doris::TabletStatePB::PB_RUNNING);
    new_tablet_meta.set_cumulative_layer_point(schema_change.output_cumulative_point());
    new_tablet_meta.SerializeToString(&new_tablet_val);
    txn->put(new_tablet_key, new_tablet_val);

    //==========================================================================
    //                move rowsets [2-alter_version] to recycle
    //==========================================================================
    if (!schema_change.has_alter_version()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid alter_version";
        return;
    }
    if (schema_change.alter_version() < 2) { // no need to update stats
        // TODO(cyx): clear schema_change job?
        need_commit = true;
        return;
    }

    int64_t num_remove_rows = 0;
    int64_t size_remove_rowsets = 0;
    int64_t num_remove_rowsets = 0;
    int64_t num_remove_segments = 0;

    auto rs_start = meta_rowset_key({instance_id, new_tablet_id, 2});
    auto rs_end = meta_rowset_key({instance_id, new_tablet_id, schema_change.alter_version() + 1});
    std::unique_ptr<RangeGetIterator> it;
    auto rs_start1 = rs_start;
    do {
        TxnErrorCode err = txn->get(rs_start1, rs_end, &it);
        if (err != TxnErrorCode::TXN_OK) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            SS << "internal error, failed to get rowset range, err=" << err
               << " tablet_id=" << new_tablet_id << " range=[" << hex(rs_start1) << ", << "
               << hex(rs_end) << ")";
            msg = ss.str();
            return;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();

            doris::RowsetMetaCloudPB rs;
            if (!rs.ParseFromArray(v.data(), v.size())) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                SS << "malformed rowset meta, unable to deserialize, tablet_id=" << new_tablet_id
                   << " key=" << hex(k);
                msg = ss.str();
                return;
            }

            num_remove_rows += rs.num_rows();
            size_remove_rowsets += rs.data_disk_size();
            ++num_remove_rowsets;
            num_remove_segments += rs.num_segments();

            auto recycle_key = recycle_rowset_key({instance_id, new_tablet_id, rs.rowset_id_v2()});
            RecycleRowsetPB recycle_rowset;
            recycle_rowset.set_creation_time(now);
            recycle_rowset.mutable_rowset_meta()->CopyFrom(rs);
            recycle_rowset.set_type(RecycleRowsetPB::DROP);
            auto recycle_val = recycle_rowset.SerializeAsString();
            txn->put(recycle_key, recycle_val);
            INSTANCE_LOG(INFO) << "put recycle rowset, tablet_id=" << new_tablet_id
                               << " key=" << hex(recycle_key);

            if (!it->has_next()) rs_start1 = k;
        }
        rs_start1.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    txn->remove(rs_start, rs_end);

    //==========================================================================
    //                        update new_tablet stats
    //==========================================================================
    auto stats = response->mutable_stats();
    TabletStats detached_stats;
    // ATTN: The condition that snapshot read can be used to get tablet stats is: all other transactions that put tablet stats
    //  can make read write conflicts with this transaction on other keys. Currently, if all meta-service nodes are running
    //  with `config::split_tablet_stats = true` can meet the condition.
    internal_get_tablet_stats(code, msg, txn.get(), instance_id, new_tablet_idx, *stats,
                              detached_stats, config::snapshot_get_tablet_stats);
    // clang-format off
    stats->set_cumulative_point(schema_change.output_cumulative_point());
    stats->set_num_rows(stats->num_rows() + (schema_change.num_output_rows() - num_remove_rows));
    stats->set_data_size(stats->data_size() + (schema_change.size_output_rowsets() - size_remove_rowsets));
    stats->set_num_rowsets(stats->num_rowsets() + (schema_change.num_output_rowsets() - num_remove_rowsets));
    stats->set_num_segments(stats->num_segments() + (schema_change.num_output_segments() - num_remove_segments));
    // clang-format on
    auto stats_key = stats_tablet_key(
            {instance_id, new_table_id, new_index_id, new_partition_id, new_tablet_id});
    auto stats_val = stats->SerializeAsString();
    txn->put(stats_key, stats_val);
    merge_tablet_stats(*stats, detached_stats);
    VLOG_DEBUG << "update tablet stats tablet_id=" << tablet_id << " key=" << hex(stats_key)
               << " stats=" << proto_to_json(*stats);
    //==========================================================================
    //                  change tmp rowset to formal rowset
    //==========================================================================
    if (schema_change.txn_ids().empty() || schema_change.output_versions().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty txn_ids or output_versions";
        return;
    }

    // process mow table, check lock
    if (new_tablet_meta.enable_unique_key_merge_on_write()) {
        bool success = check_and_remove_delete_bitmap_update_lock(
                code, msg, ss, txn, instance_id, new_table_id, SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID,
                schema_change.delete_bitmap_lock_initiator());
        if (!success) {
            return;
        }
    }

    for (size_t i = 0; i < schema_change.txn_ids().size(); ++i) {
        auto tmp_rowset_key =
                meta_rowset_tmp_key({instance_id, schema_change.txn_ids().at(i), new_tablet_id});
        std::string tmp_rowset_val;
        // FIXME: async get
        TxnErrorCode err = txn->get(tmp_rowset_key, &tmp_rowset_val);
        if (err != TxnErrorCode::TXN_OK) {
            SS << "failed to get tmp rowset key"
               << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? " (not found)" : "")
               << ", tablet_id=" << new_tablet_id << " tmp_rowset_key=" << hex(tmp_rowset_key)
               << ", err=" << err;
            msg = ss.str();
            code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::UNDEFINED_ERR
                                                          : cast_as<ErrCategory::READ>(err);
            return;
        }
        auto rowset_key = meta_rowset_key(
                {instance_id, new_tablet_id, schema_change.output_versions().at(i)});
        txn->put(rowset_key, tmp_rowset_val);
        txn->remove(tmp_rowset_key);
    }

    //==========================================================================
    //                      remove schema_change job
    //==========================================================================
    recorded_job.clear_schema_change();
    auto job_val = recorded_job.SerializeAsString();
    txn->put(job_key, job_val);
    INSTANCE_LOG(INFO) << "remove schema_change job tablet_id=" << tablet_id
                       << " key=" << hex(job_key);

    need_commit = true;
}

void MetaServiceImpl::finish_tablet_job(::google::protobuf::RpcController* controller,
                                        const FinishTabletJobRequest* request,
                                        FinishTabletJobResponse* response,
                                        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(finish_tablet_job);
    std::string cloud_unique_id = request->cloud_unique_id();
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        SS << "cannot find instance_id with cloud_unique_id="
           << (cloud_unique_id.empty() ? "(empty)" : cloud_unique_id);
        msg = ss.str();
        LOG(INFO) << msg;
        return;
    }
    RPC_RATE_LIMIT(finish_tablet_job)
    if (!request->has_job() ||
        (request->job().compaction().empty() && !request->job().has_schema_change())) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid job specified";
        return;
    }

    bool need_commit = false;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }

    int64_t tablet_id = request->job().idx().tablet_id();
    if (tablet_id <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no valid tablet_id given";
        return;
    }
    auto& tablet_idx = const_cast<TabletIndexPB&>(request->job().idx());
    if (!tablet_idx.has_table_id() || !tablet_idx.has_index_id() ||
        !tablet_idx.has_partition_id()) {
        get_tablet_idx(code, msg, txn.get(), instance_id, tablet_id, tablet_idx);
        if (code != MetaServiceCode::OK) return;
    }
    // Check if tablet has been dropped
    if (is_dropped_tablet(txn.get(), instance_id, tablet_idx.index_id(),
                          tablet_idx.partition_id())) {
        code = MetaServiceCode::TABLET_NOT_FOUND;
        msg = fmt::format("tablet {} has been dropped", tablet_id);
        return;
    }

    // TODO(gavin): remove duplicated code with start_tablet_job()
    // Begin to process finish tablet job
    std::string job_key = job_tablet_key({instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                                          tablet_idx.partition_id(), tablet_id});
    std::string job_val;
    err = txn->get(job_key, &job_val);
    if (err != TxnErrorCode::TXN_OK) {
        SS << (err == TxnErrorCode::TXN_KEY_NOT_FOUND ? "job not found," : "internal error,")
           << " instance_id=" << instance_id << " tablet_id=" << tablet_id
           << " job=" << proto_to_json(request->job()) << " err=" << err;
        msg = ss.str();
        code = err == TxnErrorCode::TXN_KEY_NOT_FOUND ? MetaServiceCode::INVALID_ARGUMENT
                                                      : cast_as<ErrCategory::READ>(err);
        return;
    }
    TabletJobInfoPB recorded_job;
    recorded_job.ParseFromString(job_val);
    VLOG_DEBUG << "get tablet job, tablet_id=" << tablet_id
               << " job=" << proto_to_json(recorded_job);

    std::unique_ptr<int, std::function<void(int*)>> defer_commit(
            (int*)0x01, [&ss, &txn, &code, &msg, &need_commit](int*) {
                if (!need_commit) return;
                TxnErrorCode err = txn->commit();
                if (err != TxnErrorCode::TXN_OK) {
                    code = cast_as<ErrCategory::COMMIT>(err);
                    ss << "failed to commit job kv, err=" << err;
                    msg = ss.str();
                    return;
                }
            });

    // Process compaction commit
    if (!request->job().compaction().empty()) {
        process_compaction_job(code, msg, ss, txn, request, response, recorded_job, instance_id,
                               job_key, need_commit);
        return;
    }

    // Process schema change commit
    if (request->job().has_schema_change()) {
        process_schema_change_job(code, msg, ss, txn, request, response, recorded_job, instance_id,
                                  job_key, need_commit);
        return;
    }
}

#undef SS
#undef INSTANCE_LOG
} // namespace doris::cloud
