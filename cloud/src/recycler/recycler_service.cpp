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

#include <algorithm>
#include <functional>
#include <numeric>
#include <sstream>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/s3_rate_limiter.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv_error.h"
#include "recycler/checker.h"
#include "recycler/meta_checker.h"
#include "recycler/recycler.h"
#include "recycler/s3_accessor.h"
#include "recycler/util.h"

namespace doris::cloud {

extern int reset_s3_rate_limiter(S3RateLimitType type, size_t max_speed, size_t max_burst,
                                 size_t limit);

extern std::tuple<int, std::string_view> convert_ms_code_to_http_code(MetaServiceCode ret);

RecyclerServiceImpl::RecyclerServiceImpl(std::shared_ptr<TxnKv> txn_kv, Recycler* recycler,
                                         Checker* checker,
                                         std::shared_ptr<TxnLazyCommitter> txn_lazy_committer)
        : txn_kv_(std::move(txn_kv)),
          recycler_(recycler),
          checker_(checker),
          txn_lazy_committer_(std::move(txn_lazy_committer)) {}

RecyclerServiceImpl::~RecyclerServiceImpl() = default;

void RecyclerServiceImpl::statistics_recycle(StatisticsRecycleRequest& req, MetaServiceCode& code,
                                             std::string& msg) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        return;
    }

    static std::map<std::string, std::function<void(InstanceRecycler&)>> resource_handlers = {
            {"recycle_indexes",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_indexes();
             }},
            {"recycle_partitions",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_partitions();
             }},
            {"recycle_tmp_rowsets",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_tmp_rowsets();
             }},
            {"recycle_rowsets",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_rowsets();
             }},
            {"abort_timeout_txn",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_abort_timeout_txn();
             }},
            {"recycle_expired_txn_label",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_expired_txn_label();
             }},
            {"recycle_versions",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_versions();
             }},
            {"recycle_copy_jobs",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_copy_jobs();
             }},
            {"recycle_stage",
             [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_stage();
             }},
            {"recycle_expired_stage_objects", [](InstanceRecycler& instance_recycler) {
                 instance_recycler.scan_and_statistics_expired_stage_objects();
             }}};

    std::set<std::string> resource_types;
    for (const auto& resource_type : req.resource_type()) {
        if (resource_type == "*") {
            std::ranges::for_each(resource_handlers,
                                  [&](const auto& it) { resource_types.emplace(it.first); });
            break;
        } else {
            if (!resource_handlers.contains(resource_type)) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = fmt::format(
                        "invalid resource type: {}, valid resource_type have [{}]", resource_type,
                        std::accumulate(resource_handlers.begin(), resource_handlers.end(),
                                        std::string(), [](const std::string& acc, const auto& it) {
                                            return acc.empty() ? it.first : acc + ", " + it.first;
                                        }));
                LOG_WARNING(msg);
                return;
            } else {
                resource_types.emplace(resource_type);
            }
        }
    }

    std::set<std::string> instance_ids;
    std::vector<InstanceInfoPB> instances;
    get_all_instances(txn_kv_.get(), instances);

    for (const auto& instance_id : req.instance_ids()) {
        if (instance_id == "*") {
            std::ranges::for_each(instances, [&](const InstanceInfoPB& instance) {
                instance_ids.emplace(instance.instance_id());
            });
            break;
        } else {
            if (std::ranges::find_if(instances, [&](const InstanceInfoPB& instance) {
                    return instance.instance_id() == instance_id;
                }) == instances.end()) {
                code = MetaServiceCode::INVALID_ARGUMENT;
                msg = fmt::format("invalid instance id: {}", instance_id);
                LOG_WARNING(msg);
                return;
            } else {
                instance_ids.emplace(instance_id);
            }
        }
    }

    LOG(INFO) << "begin to statistics recycle for "
              << std::accumulate(instance_ids.begin(), instance_ids.end(), std::string(),
                                 [](const std::string& acc, const std::string& id) {
                                     return acc.empty() ? id : acc + ", " + id;
                                 });

    auto worker_pool = std::make_unique<SimpleThreadPool>(
            config::instance_recycler_statistics_recycle_worker_pool_size, "statistics_recycle");
    worker_pool->start();

    for (const auto& id : instance_ids) {
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
        auto instance_recycler = std::make_shared<InstanceRecycler>(
                txn_kv_, instance, recycler_->_thread_pool_group, txn_lazy_committer_);

        if (int r = instance_recycler->init(); r != 0) {
            LOG(WARNING) << "failed to init instance recycler, instance_id=" << id << " ret=" << r;
            continue;
        }
        // if empty, statistics all resources
        if (resource_types.empty()) {
            for (const auto& [_, func] : resource_handlers) {
                worker_pool->submit([&instance_recycler, &func]() { func(*instance_recycler); });
            }
        } else {
            for (const auto& resource_type : resource_types) {
                if (auto it = resource_handlers.find(resource_type);
                    it != resource_handlers.end()) {
                    worker_pool->submit(
                            [&it, &instance_recycler]() { it->second(*instance_recycler); });
                }
            }
        }
    }

    worker_pool->stop();
    std::stringstream ss;
    std::ranges::for_each(instance_ids, [&](const std::string& id) {
        ss << "Instance ID: " << id << "\n";
        ss << "----------------------------------------\n";

        // tablet and segment statistics
        int64_t tablet_num = g_bvar_recycler_instance_last_round_to_recycle_num.get(
                {"global_recycler", "recycle_tablet"});
        int64_t tablet_bytes = g_bvar_recycler_instance_last_round_to_recycle_num.get(
                {"global_recycler", "recycle_tablet"});
        int64_t segment_num = g_bvar_recycler_instance_last_round_to_recycle_num.get(
                {"global_recycler", "recycle_segment"});
        int64_t segment_bytes = g_bvar_recycler_instance_last_round_to_recycle_num.get(
                {"global_recycler", "recycle_segment"});
        // clang-format off
        ss << "Global recycler: " << "tablet and segment" << "\n";
        ss << "  • Need to recycle tablet count: " << tablet_num << " items\n";
        ss << "  • Need to recycle tablet size: " << tablet_bytes << " bytes\n";
        ss << "  • Need to recycle segment count: " << segment_num << " items\n";
        ss << "  • Need to recycle segment size: " << segment_bytes << " bytes\n";
        // clang-format on

        std::ranges::for_each(resource_types, [&](const auto& resource_type) {
            int64_t to_recycle_num =
                    g_bvar_recycler_instance_last_round_to_recycle_num.get({id, resource_type});
            int64_t to_recycle_bytes = to_recycle_bytes =
                    g_bvar_recycler_instance_last_round_to_recycle_bytes.get({id, resource_type});

            ss << "Task Type: " << resource_type << "\n";

            // Add specific counts for different resource types
            if (resource_type == "recycle_partitions") {
                ss << "  • Need to recycle partition count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle partition size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_rowsets") {
                ss << "  • Need to recycle rowset count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle rowset size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_tmp_rowsets") {
                ss << "  • Need to recycle tmp rowset count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle tmp rowset size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_indexes") {
                ss << "  • Need to recycle index count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle index size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_segment") {
                ss << "  • Need to recycle segment count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle segment size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_tablet") {
                ss << "  • Need to recycle tablet count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle tablet size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_versions") {
                ss << "  • Need to recycle version count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle version size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "abort_timeout_txn") {
                ss << "  • Need to abort timeout txn count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle timeout txn size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_expired_txn_label") {
                ss << "  • Need to recycle expired txn label count: " << to_recycle_num
                   << " items\n";
                ss << "  • Need to recycle expired txn label size: " << to_recycle_bytes
                   << " bytes\n";
            } else if (resource_type == "recycle_copy_jobs") {
                ss << "  • Need to recycle copy job count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle copy job size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_stage") {
                ss << "  • Need to recycle stage count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle stage size: " << to_recycle_bytes << " bytes\n";
            } else if (resource_type == "recycle_expired_stage_objects") {
                ss << "  • Need to recycle expired stage object count: " << to_recycle_num
                   << " items\n";
                ss << "  • Need to recycle expired stage object size: " << to_recycle_bytes
                   << " bytes\n";
            } else {
                ss << "  • Need to recycle count: " << to_recycle_num << " items\n";
                ss << "  • Need to recycle size: " << to_recycle_bytes << " bytes\n";
            }

            ss << "----------------------------------------\n";
        });
        ss << "\n";
    });
    msg = ss.str();
}

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
    meta_checker->do_check(host, port, user, password, instance_id, msg);
#else
    msg = "check meta not build, please export BUILD_CHECK_META=ON before build cloud";
#endif
}

void RecyclerServiceImpl::http(::google::protobuf::RpcController* controller,
                               const ::doris::cloud::MetaServiceHttpRequest* request,
                               ::doris::cloud::MetaServiceHttpResponse* response,
                               ::google::protobuf::Closure* done) {
    auto cntl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "rpc from " << cntl->remote_side() << " request: " << request->DebugString();
    brpc::ClosureGuard closure_guard(done);
    MetaServiceCode code = MetaServiceCode::OK;
    int status_code = 200;
    std::string msg = "OK";
    std::string req;
    std::string response_body;
    std::string request_body;
    DORIS_CLOUD_DEFER {
        status_code = std::get<0>(convert_ms_code_to_http_code(code));
        LOG(INFO) << (code == MetaServiceCode::OK ? "succ to " : "failed to ") << "http"
                  << " " << cntl->remote_side() << " request=\n"
                  << req << "\n ret=" << code << " msg=" << msg;
        cntl->http_response().set_status_code(status_code);
        cntl->response_attachment().append(response_body);
        cntl->response_attachment().append("\n");
    };

    // Prepare input request info
    auto unresolved_path = cntl->http_request().unresolved_path();
    auto uri = cntl->http_request().uri();
    std::stringstream ss;
    ss << "\nuri_path=" << uri.path();
    ss << "\nunresolved_path=" << unresolved_path;
    ss << "\nmethod=" << brpc::HttpMethod2Str(cntl->http_request().method());
    ss << "\nquery strings:";
    for (auto it = uri.QueryBegin(); it != uri.QueryEnd(); ++it) {
        ss << "\n" << it->first << "=" << it->second;
    }
    ss << "\nheaders:";
    for (auto it = cntl->http_request().HeaderBegin(); it != cntl->http_request().HeaderEnd();
         ++it) {
        ss << "\n" << it->first << ":" << it->second;
    }
    req = ss.str();
    ss.clear();
    request_body = cntl->request_attachment().to_string(); // Just copy

    // Auth
    auto token = uri.GetQuery("token");
    if (token == nullptr || *token != config::http_token) {
        msg = "incorrect token, token=" + (token == nullptr ? std::string("(not given)") : *token);
        response_body = "incorrect token";
        status_code = 403;
        return;
    }

    if (unresolved_path == "recycle_instance") {
        RecycleInstanceRequest req;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &req);
        if (!st.ok()) {
            msg = "failed to RecycleInstanceRequest, error: " + st.message().ToString();
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        RecycleInstanceResponse res;
        recycle_instance(cntl, &req, &res, nullptr);
        code = res.status().code();
        msg = res.status().msg();
        response_body = msg;
        return;
    }

    if (unresolved_path == "statistics_recycle") {
        StatisticsRecycleRequest req;
        auto st = google::protobuf::util::JsonStringToMessage(request_body, &req);
        if (!st.ok()) {
            msg = "failed to StatisticsRecycleRequest, error: " + st.message().ToString();
            response_body = msg;
            LOG(WARNING) << msg;
            return;
        }
        statistics_recycle(req, code, msg);
        response_body = msg;
        return;
    }

    if (unresolved_path == "recycle_copy_jobs") {
        auto instance_id = uri.GetQuery("instance_id");
        if (instance_id == nullptr || instance_id->empty()) {
            msg = "no instance id";
            response_body = msg;
            status_code = 400;
            return;
        }
        recycle_copy_jobs(txn_kv_, *instance_id, code, msg, recycler_->_thread_pool_group,
                          txn_lazy_committer_);

        response_body = msg;
        return;
    }

    if (unresolved_path == "recycle_job_info") {
        auto instance_id = uri.GetQuery("instance_id");
        if (instance_id == nullptr || instance_id->empty()) {
            msg = "no instance id";
            response_body = msg;
            status_code = 400;
            return;
        }
        std::string key;
        job_recycle_key({*instance_id}, &key);
        recycle_job_info(txn_kv_, *instance_id, key, code, msg);
        response_body = msg;
        return;
    }

    if (unresolved_path == "check_instance") {
        auto instance_id = uri.GetQuery("instance_id");
        if (instance_id == nullptr || instance_id->empty()) {
            msg = "no instance id";
            response_body = msg;
            status_code = 400;
            return;
        }
        if (!checker_) {
            msg = "checker not enabled";
            response_body = msg;
            status_code = 400;
            return;
        }
        check_instance(*instance_id, code, msg);
        response_body = msg;
        return;
    }

    if (unresolved_path == "check_job_info") {
        auto instance_id = uri.GetQuery("instance_id");
        if (instance_id == nullptr || instance_id->empty()) {
            msg = "no instance id";
            response_body = msg;
            status_code = 400;
            return;
        }
        std::string key;
        job_check_key({*instance_id}, &key);
        recycle_job_info(txn_kv_, *instance_id, key, code, msg);
        response_body = msg;
        return;
    }

    if (unresolved_path == "check_meta") {
        auto instance_id = uri.GetQuery("instance_id");
        auto host = uri.GetQuery("host");
        auto port = uri.GetQuery("port");
        auto user = uri.GetQuery("user");
        auto password = uri.GetQuery("password");
        LOG(INFO) << " host " << *host;
        LOG(INFO) << " port " << *port;
        LOG(INFO) << " user " << *user;
        LOG(INFO) << " instance " << *instance_id;
        if (instance_id == nullptr || instance_id->empty() || host == nullptr || host->empty() ||
            port == nullptr || port->empty() || password == nullptr || user == nullptr ||
            user->empty()) {
            msg = "no instance id or mysql conn str info";
            response_body = msg;
            status_code = 400;
            return;
        }
        check_meta(txn_kv_, *instance_id, *host, *port, *user, *password, msg);
        status_code = 200;
        response_body = msg;
        return;
    }

    if (unresolved_path == "adjust_rate_limiter") {
        auto type_string = uri.GetQuery("type");
        auto speed = uri.GetQuery("speed");
        auto burst = uri.GetQuery("burst");
        auto limit = uri.GetQuery("limit");
        if (type_string->empty() || speed->empty() || burst->empty() || limit->empty() ||
            (*type_string != "get" && *type_string != "put")) {
            msg = "argument not suitable";
            response_body = msg;
            status_code = 400;
            return;
        }
        auto max_speed = speed->empty() ? 0 : std::stoul(*speed);
        auto max_burst = burst->empty() ? 0 : std::stoul(*burst);
        auto max_limit = burst->empty() ? 0 : std::stoul(*limit);
        if (0 != reset_s3_rate_limiter(string_to_s3_rate_limit_type(*type_string), max_speed,
                                       max_burst, max_limit)) {
            msg = "adjust failed";
            response_body = msg;
            status_code = 400;
            return;
        }

        status_code = 200;
        response_body = msg;
        return;
    }

    status_code = 404;
    msg = "http path " + uri.path() + " not found, it may be not implemented";
    response_body = msg;
}

} // namespace doris::cloud
