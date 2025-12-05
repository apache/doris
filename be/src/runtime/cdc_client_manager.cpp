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
#include <gen_cpp/internal_service.pb.h>
#include <google/protobuf/stubs/callback.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#ifndef __APPLE__
#include <sys/prctl.h>
#endif

#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "http/http_client.h"

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
    const std::string cdc_health_url = "http://127.0.0.1:" +
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
Status start_cdc_client(PRequestCdcClientResult* result) {
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
    
    const std::string cdc_jar_path = std::string(doris_home) + "/lib/cdc_client/cdc-client.jar";
    const std::string cdc_jar_port = "--server.port=" + std::to_string(doris::config::cdc_client_port);
    const std::string backend_http_port = std::to_string(config::webserver_port);
    const std::string java_opts = "-Xmx2048m -Dlog.path=" + std::string(log_dir);
    
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
    std::string check_response;
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
        // java -jar -Dlog.path=xx cdc-client.jar --server.port=9096 8040
        execlp(java_bin.c_str(), "java", java_opts.c_str(), "-jar", cdc_jar_path.c_str(),
               cdc_jar_port.c_str(), backend_http_port.c_str(),(char*)NULL);
        std::cerr << "Cdc client child process error." << std::endl;
        exit(1);
    } else {
        // Waiting for cdc to start, failed after more than 30 seconds
        std::string health_response;
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

CdcClientManager::CdcClientManager() = default;

CdcClientManager::~CdcClientManager() {
    stop();
    LOG(INFO) << "CdcClientManager is destroyed";
}

void CdcClientManager::stop() {
    LOG(INFO) << "CdcClientManager is stopped";
}

void CdcClientManager::request_cdc_client_impl(const PRequestCdcClientRequest* request,
                                                PRequestCdcClientResult* result,
                                                google::protobuf::Closure* done) {
    VLOG_RPC << "request to cdc client, api " << request->api();
    brpc::ClosureGuard closure_guard(done);

    // Start CDC client if not started
    Status start_st = start_cdc_client(result);
    if (!start_st.ok()) {
        LOG(ERROR) << "Failed to start CDC client, status=" << start_st.to_string();
        start_st.to_protobuf(result->mutable_status());
        return;
    }

    std::string cdc_response;
    Status st = send_request_to_cdc_client(request->api(), request->params(), &cdc_response);
    result->set_response(cdc_response);
    st.to_protobuf(result->mutable_status());
}


Status CdcClientManager::send_request_to_cdc_client(const std::string& api,
                                                          const std::string& params_body,
                                                          std::string* response) {
    std::string remote_url_prefix = fmt::format("http://127.0.0.1:{}{}", 
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

} // namespace doris

