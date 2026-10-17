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

#include "recycler/recycler_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <numeric>
#include <sstream>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/configbase.h"
#include "common/defer.h"
#include "common/http_helper.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/token_bucket_rate_limiter.h"
#include "meta-service/meta_service_http.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv_error.h"
#include "recycler/checker.h"
#include "recycler/meta_checker.h"
#include "recycler/recycler.h"
#include "recycler/s3_accessor.h"
#include "recycler/util.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

RecyclerServiceImpl::RecyclerServiceImpl(std::shared_ptr<TxnKv> txn_kv, Recycler* recycler,
                                         Checker* checker,
                                         std::shared_ptr<TxnLazyCommitter> txn_lazy_committer)
        : txn_kv_(std::move(txn_kv)),
          recycler_(recycler),
          checker_(checker),
          txn_lazy_committer_(std::move(txn_lazy_committer)) {}

RecyclerServiceImpl::~RecyclerServiceImpl() = default;

void RecyclerServiceImpl::recycle_instance(::google::protobuf::RpcController* controller,
                                           const ::doris::cloud::RecycleInstanceRequest* request,
                                           ::doris::cloud::RecycleInstanceResponse* response,
                                           ::google::protobuf::Closure* done) {
    auto ctrl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << ctrl->remote_side() << " request=" << request->ShortDebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg = "OK";
    DORIS_CLOUD_DEFER {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ") << "recycle_instance"
                  << " " << ctrl->remote_side() << " " << msg;
    };

    std::vector<InstanceInfoPB> instances;
    instances.reserve(request->instance_ids_size());

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    for (auto& id : request->instance_ids()) {
        InstanceKeyInfo key_info {id};
        std::string key;
        instance_key(key_info, &key);
        std::string val;
        err = txn->get(key, &val);
        if (err != TxnErrorCode::TXN_OK) {
            code = MetaServiceCode::KV_TXN_GET_ERR;
            msg = fmt::format("failed to get instance, instance_id={}, err={}", id, err);
            LOG_WARNING(msg);
            continue;
        }
        InstanceInfoPB instance;
        if (!instance.ParseFromString(val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("malformed instance info, key={}, val={}", hex(key), hex(val));
            LOG_WARNING(msg);
            continue;
        }
        instances.push_back(std::move(instance));
    }
    {
        std::lock_guard lock(recycler_->mtx_);
        for (auto& i : instances) {
            auto [_, success] = recycler_->pending_instance_set_.insert(i.instance_id());
            // skip instance already in pending queue
            if (success) {
                // TODO(plat1ko): Support high priority
                recycler_->pending_instance_queue_.push_back(std::move(i));
            }
        }
        recycler_->pending_instance_cond_.notify_all();
    }
}

void RecyclerServiceImpl::check_instance(const std::string& instance_id, MetaServiceCode& code,
                                         std::string& msg) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    std::string key;
    instance_key({instance_id}, &key);
    std::string val;
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = fmt::format("failed to get instance, instance_id={}, err={}", instance_id, err);
        return;
    }
    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("malformed instance info, key={}", hex(key));
        return;
    }
    {
        std::lock_guard lock(checker_->mtx_);
        using namespace std::chrono;
        auto enqueue_time_s =
                duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        auto [_, success] = checker_->pending_instance_map_.insert({instance_id, enqueue_time_s});
        // skip instance already in pending queue
        if (success) {
            // TODO(plat1ko): Support high priority
            checker_->pending_instance_queue_.push_back(std::move(instance));
        }
        checker_->pending_instance_cond_.notify_all();
    }
}

std::pair<MetaServiceCode, std::string> RecyclerServiceImpl::skip_instance_data_cleanup(
        const std::string& instance_id) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        std::string msg = fmt::format("failed to create txn, err={}", err);
        LOG(WARNING) << msg << " instance_id=" << instance_id;
        return {MetaServiceCode::KV_TXN_CREATE_ERR, std::move(msg)};
    }

    std::string key = instance_key({instance_id});
    std::string value;
    err = txn->get(key, &value);
    if (err != TxnErrorCode::TXN_OK) {
        std::string msg =
                fmt::format("failed to get instance, instance_id={}, err={}", instance_id, err);
        LOG(WARNING) << msg;
        return {MetaServiceCode::KV_TXN_GET_ERR, std::move(msg)};
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(value)) {
        std::string msg = fmt::format("malformed instance info, key={}", hex(key));
        LOG(WARNING) << msg;
        return {MetaServiceCode::PROTOBUF_PARSE_ERR, std::move(msg)};
    }
    auto current_state = instance.recycle_state();
    if (instance.status() != InstanceInfoPB::DELETED) {
        std::string msg = fmt::format(
                "failed to set instance recycle state, instance is not deleted, instance_id={}",
                instance_id);
        LOG(WARNING) << msg;
        return {MetaServiceCode::INVALID_ARGUMENT, std::move(msg)};
    }
    if (current_state != INSTANCE_RECYCLE_STATE_DATA_CLEANUP_PENDING) {
        std::string msg = fmt::format(
                "failed to set instance recycle state, instance state should be {}"
                ", current_state={}"
                ", instance_id={}",
                INSTANCE_RECYCLE_STATE_DATA_CLEANUP_PENDING, current_state, instance_id);
        LOG(WARNING) << msg;
        return {MetaServiceCode::INVALID_ARGUMENT, std::move(msg)};
    }
    if (instance.has_multi_version_status() &&
        instance.multi_version_status() != MultiVersionStatus::MULTI_VERSION_DISABLED) {
        std::string msg = fmt::format(
                "cannot skip instance data cleanup for a multi-version instance, instance_id={}, "
                "multi_version_status={}",
                instance_id, MultiVersionStatus_Name(instance.multi_version_status()));
        LOG(WARNING) << msg;
        return {MetaServiceCode::INVALID_ARGUMENT, std::move(msg)};
    }

    instance.set_recycle_state(INSTANCE_RECYCLE_STATE_METADATA_CLEANUP_PENDING);
    instance.set_recycle_state_update_time_ms(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
    if (!instance.SerializeToString(&value)) {
        std::string msg = "failed to serialize InstanceInfoPB";
        LOG(WARNING) << msg << " instance_id=" << instance_id;
        return {MetaServiceCode::PROTOBUF_SERIALIZE_ERR, std::move(msg)};
    }

    txn->atomic_add(system_meta_service_instance_update_key(), 1);
    txn->put(key, value);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        std::string msg = fmt::format("failed to commit kv txn, err={}", err);
        LOG(WARNING) << msg << " instance_id=" << instance_id;
        return {MetaServiceCode::KV_TXN_COMMIT_ERR, std::move(msg)};
    }
    LOG(WARNING) << "successfully skipped instance data cleanup, instance_id=" << instance_id
                 << " current_state=" << InstanceRecycleState_Name(current_state)
                 << " target_state=" << InstanceRecycleState_Name(instance.recycle_state());

    return {MetaServiceCode::OK, "OK"};
}

void recycle_copy_jobs(const std::shared_ptr<TxnKv>& txn_kv, const std::string& instance_id,
                       MetaServiceCode& code, std::string& msg,
                       RecyclerThreadPoolGroup thread_pool_group,
                       std::shared_ptr<TxnLazyCommitter> txn_lazy_committer) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    std::string key;
    instance_key({instance_id}, &key);
    std::string val;
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = fmt::format("failed to get instance, instance_id={}, err={}", instance_id, err);
        return;
    }
    InstanceInfoPB instance;
    if (!instance.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("malformed instance info, key={}", hex(key));
        return;
    }
    static std::mutex s_worker_mtx;
    static std::set<std::string> s_worker;
    {
        std::lock_guard lock(s_worker_mtx);
        if (s_worker.size() >= config::recycle_concurrency) { // use another config entry?
            msg = "exceeded the concurrency limit";
            return;
        }
        auto [_, success] = s_worker.insert(instance_id);
        if (!success) {
            msg = "recycle_copy_jobs not yet finished on this instance";
            return;
        }
    }

    auto recycler = std::make_unique<InstanceRecycler>(txn_kv, instance, thread_pool_group,
                                                       txn_lazy_committer);
    if (recycler->init() != 0) {
        LOG(WARNING) << "failed to init InstanceRecycler recycle_copy_jobs on instance "
                     << instance_id;
        return;
    }
    std::thread worker([recycler = std::move(recycler), instance_id] {
        LOG(INFO) << "manually trigger recycle_copy_jobs on instance " << instance_id;
        recycler->recycle_copy_jobs();
        std::lock_guard lock(s_worker_mtx);
        s_worker.erase(instance_id);
    });
    pthread_setname_np(worker.native_handle(), "recycler_worker");
    worker.detach();
}

void recycle_job_info(const std::shared_ptr<TxnKv>& txn_kv, const std::string& instance_id,
                      std::string_view key, MetaServiceCode& code, std::string& msg) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }
    std::string val;
    err = txn->get(key, &val);
    JobRecyclePB job_info;
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // Not found, check instance existence
            std::string key, val;
            instance_key({instance_id}, &key);
            err = txn->get(key, &val);
            if (err == TxnErrorCode::TXN_OK) { // Never performed a recycle on this instance before
                job_info.set_status(JobRecyclePB::IDLE);
                job_info.set_last_ctime_ms(0);
                job_info.set_last_finish_time_ms(0);
                job_info.set_instance_id(instance_id);
                msg = proto_to_json(job_info);
                return;
            }
        }
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = fmt::format("failed to get recycle job info, instance_id={}, err={}", instance_id,
                          err);
        return;
    }
    if (!job_info.ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("malformed job recycle value, key={}", hex(key));
        return;
    }
    msg = proto_to_json(job_info);
}

void check_meta(const std::shared_ptr<TxnKv>& txn_kv, const std::string& instance_id,
                const std::string& host, const std::string& port, const std::string& user,
                const std::string& password, std::string& msg) {
#ifdef BUILD_CHECK_META
    std::unique_ptr<MetaChecker> meta_checker = std::make_unique<MetaChecker>(txn_kv);
    meta_checker->init_mysql_connection(host, port, user, password, instance_id, msg);
    meta_checker->do_check(msg);
#else
    msg = "check meta not build, please export BUILD_CHECK_META=ON before build cloud";
#endif
}

void RecyclerServiceImpl::http(::google::protobuf::RpcController* controller,
                               const ::doris::cloud::MetaServiceHttpRequest*,
                               ::doris::cloud::MetaServiceHttpResponse*,
                               ::google::protobuf::Closure* done) {
    auto* cntl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << cntl->remote_side()
              << " request: " << cntl->http_request().uri().path();
    brpc::ClosureGuard closure_guard(done);
    const auto& unresolved_path = cntl->http_request().unresolved_path();
    auto api_path = split_http_api_path(unresolved_path);
    const auto& handlers = get_http_handlers();
    auto it = handlers.find(api_path.route);
    const auto* handler =
            it == handlers.end() ? nullptr : resolve_http_handler(it->second, api_path.version);
    if (handler == nullptr ||
        (it->second.role != HttpRole::RECYCLER && it->second.role != HttpRole::BOTH)) {
        std::string msg = "http path not found or not allowed";
        cntl->http_response().set_status_code(404);
        cntl->response_attachment().append(msg);
        cntl->response_attachment().append("\n");
        return;
    }

    // Auth
    const auto* token = cntl->http_request().uri().GetQuery("token");
    if (token == nullptr || *token != config::http_token) {
        std::string msg = "incorrect token, token=" +
                          (token == nullptr ? std::string("(not given)") : *token);
        cntl->http_response().set_status_code(403);
        cntl->response_attachment().append(msg);
        cntl->response_attachment().append("\n");
        LOG(WARNING) << "failed to handle http from " << cntl->remote_side() << " msg: " << msg;
        return;
    }

    auto [status_code, msg, body] = (*handler)(this, cntl);
    cntl->http_response().set_status_code(status_code);
    cntl->response_attachment().append(body);
    cntl->response_attachment().append("\n");

    LOG(INFO) << (status_code == 200 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
              << cntl->remote_side() << " ret=" << status_code << " msg=" << msg;
}

} // namespace doris::cloud
