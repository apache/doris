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

#include "runtime/cdc_client_manager.h"

#include <brpc/closure_guard.h>
#include <fmt/core.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <google/protobuf/stubs/callback.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#ifndef __APPLE__
#include <sys/prctl.h>
#endif

#include <sstream>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "http/http_client.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "util/thrift_util.h"
#include "util/threadpool.h"

namespace doris {

namespace {
// Handle SIGCHLD signal to prevent zombie processes
void handle_sigchld(int sig_no) {
    int status = 0;
    pid_t pid = waitpid(0, &status, WNOHANG);
    LOG(INFO) << "handle cdc process exit, result: " << pid << ", status: " << status;
}

// Check CDC client health
Status check_cdc_client_health(int retry_times, int sleep_time, std::string& health_response) {
    const std::string cdc_health_url = "http://" + BackendOptions::get_localhost() + ":" +
                                       std::to_string(doris::config::cdc_client_port) +
                                       "/actuator/health";

    auto health_request = [cdc_health_url, &health_response](HttpClient* client) {
        RETURN_IF_ERROR(client->init(cdc_health_url));
        client->set_timeout_ms(5000);
        RETURN_IF_ERROR(client->execute(&health_response));
        return Status::OK();
    };

    Status status = HttpClient::execute_with_retry(retry_times, sleep_time, health_request);

    if (!status.ok()) {
        return Status::InternalError(
                fmt::format("CDC client health check failed: url={}", cdc_health_url));
    }

    bool is_up = health_response.find("UP") != std::string::npos;

    if (!is_up) {
        return Status::InternalError(fmt::format("CDC client unhealthy: url={}, response={}",
                                                  cdc_health_url, health_response));
    }

    return Status::OK();
}

// Start CDC client process
Status start_cdc_client(const std::string& params, PForwardCdcClientResult* result) {
    Status st = Status::OK();
    
    // Check DORIS_HOME environment variable
    const char* doris_home = getenv("DORIS_HOME");
    if (!doris_home) {
        st = Status::InternalError("DORIS_HOME environment variable is not set");
        if (result) {
            st.to_protobuf(result->mutable_status());
        }
        return st;
    }
    
    // Check LOG_DIR environment variable
    const char* log_dir = getenv("LOG_DIR");
    if (!log_dir) {
        st = Status::InternalError("LOG_DIR environment variable is not set");
        if (result) {
            st.to_protobuf(result->mutable_status());
        }
        return st;
    }
    
    string cdc_jar_path = string(doris_home) + "/lib/cdc-client/cdc-client.jar";
    string cdc_jar_port = "--server.port=" + std::to_string(doris::config::cdc_client_port);
    string backend_host_port =
            BackendOptions::get_localhost() + ":" + std::to_string(config::webserver_port);
    string cdc_jar_params = params;
    string java_opts = "-Dlog.path=" + string(log_dir);
    
    // check cdc jar exists
    struct stat buffer;
    if (stat(cdc_jar_path.c_str(), &buffer) != 0) {
        st = Status::InternalError("Can not find cdc-client.jar.");
        if (result) {
            st.to_protobuf(result->mutable_status());
        }
        return st;
    }

    // check cdc process already started
    string check_response;
    auto check_st = check_cdc_client_health(1, 0, check_response);
    if (check_st.ok()) {
        LOG(INFO) << "cdc client already started.";
        return Status::OK();
    } else {
        LOG(INFO) << "cdc client not started, to start.";
    }

    const auto* java_home = getenv("JAVA_HOME");
    if (!java_home) {
        st = Status::InternalError("Can not find java home.");
        if (result) {
            st.to_protobuf(result->mutable_status());
        }
        return st;
    }
    std::string path(java_home);

    // Capture signal to prevent child process from becoming a zombie process
    struct sigaction act;
    act.sa_flags = 0;
    act.sa_handler = handle_sigchld;
    sigaction(SIGCHLD, &act, NULL);
    LOG(INFO) << "Start to fork cdc client process with " << path;

    // If has a forked process, the child process fails to start and will automatically exit
    pid_t pid = ::fork();
    if (pid < 0) {
        // Fork failed
        st = Status::InternalError("Fork cdc client failed.");
        if (result) {
            st.to_protobuf(result->mutable_status());
        }
        return st;
    } else if (pid == 0) {
        // When the parent process is killed, the child process also needs to exit
#ifndef __APPLE__
        prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif

        LOG(INFO) << "Cdc client child process ready to start, " << pid << ", response="
                  << std::endl;
        std::cout << "Cdc client child process ready to start." << std::endl;
        std::string java_bin = path + "/bin/java";
        execlp(java_bin.c_str(), "java", java_opts.c_str(), "-jar", cdc_jar_path.c_str(),
               cdc_jar_port.c_str(), backend_host_port.c_str(), cdc_jar_params.c_str(),
               (char*)NULL);
        std::cerr << "Cdc client child process error." << std::endl;
        exit(1);
    } else {
        // Waiting for cdc to start, failed after more than 30 seconds
        string health_response;
        Status status = check_cdc_client_health(5, 6, health_response);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to start cdc client process, status=" << status.to_string()
                       << ", response=" << health_response;
            st = Status::InternalError("Start cdc client failed.");
            if (result) {
                st.to_protobuf(result->mutable_status());
            }
        } else {
            LOG(INFO) << "Start cdc client success, status=" << status.to_string()
                      << ", response=" << health_response;
        }
    }
    return st;
}

} // anonymous namespace

CdcClientManager::CdcClientManager(ExecEnv* exec_env) : _exec_env(exec_env) {
    static_cast<void>(ThreadPoolBuilder("CdcClientThreadPool")
                              .set_min_threads(1)
                              .set_max_threads(config::max_cdc_client_thread_pool_size)
                              .build(&_thread_pool));
}

CdcClientManager::~CdcClientManager() {
    stop();
    LOG(INFO) << "CdcClientManager is destroyed";
}

void CdcClientManager::stop() {
    if (_thread_pool) {
        _thread_pool->shutdown();
    }
    LOG(INFO) << "CdcClientManager is stopped";
}

void CdcClientManager::request_cdc_client_impl(const PRequestCdcClientRequest* request,
                                                PRequestCdcClientResult* result,
                                                google::protobuf::Closure* done) {
    VLOG_RPC << "request to cdc client, api " << request->api();
    brpc::ClosureGuard closure_guard(done);

    // Start CDC client if not started
    Status start_st = start_cdc_client(request->params(), nullptr);
    if (!start_st.ok()) {
        LOG(ERROR) << "Failed to start CDC client, status=" << start_st.to_string();
        start_st.to_protobuf(result->mutable_status());
        return;
    }

    // Send HTTP request synchronously (this is called from heavy_work_pool, so it's already async)
    std::string cdc_response;
    Status st = send_request_to_cdc_client(request->api(), request->params(), &cdc_response);
    result->set_response(cdc_response);
    st.to_protobuf(result->mutable_status());
}


Status CdcClientManager::send_request_to_cdc_client(const std::string& api,
                                                          const std::string& params_body,
                                                          std::string* response) {
    std::string remote_url_prefix = fmt::format("http://{}:{}{}", 
                                                  BackendOptions::get_localhost(),
                                                  doris::config::cdc_client_port, 
                                                  api);

    auto cdc_request = [&remote_url_prefix, response, &params_body](HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_url_prefix));
        client->set_timeout_ms(60 * 1000);
        if (!params_body.empty()) {
            client->set_payload(params_body);
        }
        client->set_content_type("application/json");
        client->set_method(POST);
        RETURN_IF_ERROR(client->execute(response));
        return Status::OK();
    };

    return HttpClient::execute_with_retry(3, 1, cdc_request);
}

Status CdcClientManager::extract_meta_from_response(const std::string& cdc_response,
                                                         std::string* meta_json) {
    rapidjson::Document doc;
    if (doc.Parse(cdc_response.c_str()).HasParseError()) {
        return Status::InternalError("Failed to parse CDC response JSON");
    }

    // Check if there is a data field
    if (!doc.HasMember("data") || !doc["data"].IsObject()) {
        return Status::InternalError("CDC response missing 'data' field or not an object");
    }

    // Check if there is a meta field
    const rapidjson::Value& data = doc["data"];
    if (!data.HasMember("meta") || !data["meta"].IsObject()) {
        return Status::InternalError("CDC response missing 'meta' field or not an object");
    }

    // Extract meta object and serialize as JSON string
    const rapidjson::Value& meta = data["meta"];
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    meta.Accept(writer);
    *meta_json = buffer.GetString();
    return Status::OK();
}

Status CdcClientManager::commit_transaction(const std::string& txn_id,
                                                 const std::string& meta_json) {
    TLoadTxnCommitRequest commit_request;
    commit_request.__set_txn_id(txn_id);

    StreamingTaskCommitAttachmentPB attachment;
    attachment.set_offset(meta_json);

    commit_request.__set_txnCommitAttachment(attachment);

    TNetworkAddress master_addr = _exec_env->cluster_info()->master_fe_addr;
    TLoadTxnCommitResult commit_result;

    Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&commit_request, &commit_result](FrontendServiceConnection& client) {
                client->loadTxnCommit(commit_result, commit_request);
            },
            config::txn_commit_rpc_timeout_ms);

    if (!rpc_st.ok()) {
        return Status::InternalError(
                fmt::format("Failed to call FE loadTxnCommit, rpc_status={}, txn_id={}",
                            rpc_st.to_string(), txn_id));
    }

    Status result_status = Status::create(commit_result.status);
    if (!result_status.ok()) {
        return Status::InternalError(
                fmt::format("FE loadTxnCommit returned error, status={}, txn_id={}",
                            result_status.to_string(), txn_id));
    }

    return Status::OK();
}

void CdcClientManager::execute_cdc_scan_commit_impl(const PRequestCdcClientRequest* request,
                                                     PRequestCdcClientResult* result,
                                                     google::protobuf::Closure* done) {
    VLOG_RPC << "forward request to cdc client, api " << request->api();
    brpc::ClosureGuard closure_guard(done);

    // Start CDC client if not started
    Status start_st = start_cdc_client(request->params(), nullptr);
    if (!start_st.ok()) {
        LOG(ERROR) << "Failed to start CDC client, status=" << start_st.to_string();
        start_st.to_protobuf(result->mutable_status());
        return;
    }

    // Extract parameters from request
    std::string api = request->api();
    std::string txn_id = request->txn_id();
    std::string params_body = request->params();

    // Submit async task to handle CDC scan and commit using internal thread_pool
    Status submit_st = _thread_pool->submit_func([this, api, params_body, txn_id]() {
        // Request cdc client to read and load data
        std::string cdc_response;
        Status st = send_http_request_to_cdc_client(api, params_body, &cdc_response);
        if (!st.ok()) {
            LOG(ERROR) << "CDC client HTTP request failed, status=" << st.to_string()
                       << ", api=" << api << ", txn_id=" << txn_id;
            return;
        }

        LOG(INFO) << "CDC client HTTP request success, response=" << cdc_response
                  << ", txn_id=" << txn_id;

        // Parse JSON, extract data.meta part
        std::string meta_json;
        Status parse_st = extract_meta_from_response(cdc_response, &meta_json);
        if (!parse_st.ok()) {
            LOG(ERROR) << "Failed to extract meta from CDC response, txn_id=" << txn_id
                       << ", status=" << parse_st.to_string();
            return;
        }

        // Commit txn
        Status commit_st = commit_transaction(txn_id, meta_json);
        if (!commit_st.ok()) {
            LOG(ERROR) << "Failed to commit CDC transaction, txn_id=" << txn_id
                       << ", status=" << commit_st.to_string();
            return;
        }

        LOG(INFO) << "Successfully committed CDC transaction to FE, txn_id=" << txn_id;
    });

    if (!submit_st.ok()) {
        LOG(ERROR) << "Failed to submit CDC client async task to thread pool, "
                   << "status=" << submit_st.to_string() << ", txn_id=" << txn_id;
        submit_st.to_protobuf(result->mutable_status());
        return;
    }
    
    // Return success to FE immediately after task is successfully submitted
    Status::OK().to_protobuf(result->mutable_status());
}

} // namespace doris

