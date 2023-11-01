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

#include "agent/utils.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <thrift/transport/TTransportException.h>

#include <cstdio>
#include <exception>
#include <fstream>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "runtime/client_cache.h"

namespace doris {
class TConfirmUnusedRemoteFilesRequest;
class TConfirmUnusedRemoteFilesResult;
class TFinishTaskRequest;
class TMasterResult;
class TReportRequest;
} // namespace doris

using std::map;
using std::string;
using apache::thrift::transport::TTransportException;

namespace doris {

static FrontendServiceClientCache s_client_cache;
static std::unique_ptr<MasterServerClient> s_client;

MasterServerClient* MasterServerClient::create(const TMasterInfo& master_info) {
    s_client.reset(new MasterServerClient(master_info));
    return s_client.get();
}

MasterServerClient* MasterServerClient::instance() {
    return s_client.get();
}

MasterServerClient::MasterServerClient(const TMasterInfo& master_info)
        : _master_info(master_info) {}

Status MasterServerClient::finish_task(const TFinishTaskRequest& request, TMasterResult* result) {
    Status client_status;
    FrontendServiceConnection client(&s_client_cache, _master_info.network_address,
                                     config::thrift_rpc_timeout_ms, &client_status);

    if (!client_status.ok()) {
        LOG(WARNING) << "fail to get master client from cache. "
                     << "host=" << _master_info.network_address.hostname
                     << ", port=" << _master_info.network_address.port
                     << ", code=" << client_status.code();
        return Status::InternalError("Failed to get master client");
    }

    try {
        try {
            client->finishTask(*result, request);
        } catch (TTransportException& e) {
            LOG(WARNING) << "master client, retry finishTask: " << e.what();
            client_status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!client_status.ok()) {
                LOG(WARNING) << "fail to get master client from cache. "
                             << "host=" << _master_info.network_address.hostname
                             << ", port=" << _master_info.network_address.port
                             << ", code=" << client_status.code();
                return Status::InternalError("Master client finish task failed");
            }
            client->finishTask(*result, request);
        }
    } catch (std::exception& e) {
        RETURN_IF_ERROR(client.reopen(config::thrift_rpc_timeout_ms));
        LOG(WARNING) << "fail to finish_task. "
                     << "host=" << _master_info.network_address.hostname
                     << ", port=" << _master_info.network_address.port << ", error=" << e.what();
        return Status::InternalError("Fail to finish task");
    }

    return Status::OK();
}

Status MasterServerClient::report(const TReportRequest& request, TMasterResult* result) {
    Status client_status;
    FrontendServiceConnection client(&s_client_cache, _master_info.network_address,
                                     config::thrift_rpc_timeout_ms, &client_status);

    if (!client_status.ok()) {
        LOG(WARNING) << "fail to get master client from cache. "
                     << "host=" << _master_info.network_address.hostname
                     << ", port=" << _master_info.network_address.port
                     << ", code=" << client_status;
        return Status::InternalError("Fail to get master client from cache");
    }

    try {
        try {
            client->report(*result, request);
        } catch (TTransportException& e) {
            TTransportException::TTransportExceptionType type = e.getType();
            if (type != TTransportException::TTransportExceptionType::TIMED_OUT) {
                // if not TIMED_OUT, retry
                LOG(WARNING) << "master client, retry finishTask: " << e.what();

                client_status = client.reopen(config::thrift_rpc_timeout_ms);
                if (!client_status.ok()) {
                    LOG(WARNING) << "fail to get master client from cache. "
                                 << "host=" << _master_info.network_address.hostname
                                 << ", port=" << _master_info.network_address.port
                                 << ", code=" << client_status.code();
                    return Status::InternalError("Fail to get master client from cache");
                }

                client->report(*result, request);
            } else {
                // TIMED_OUT exception. do not retry
                // actually we don't care what FE returns.
                LOG(WARNING) << "fail to report to master: " << e.what();
                return Status::InternalError("Fail to report to master");
            }
        }
    } catch (std::exception& e) {
        RETURN_IF_ERROR(client.reopen(config::thrift_rpc_timeout_ms));
        LOG(WARNING) << "fail to report to master. "
                     << "host=" << _master_info.network_address.hostname
                     << ", port=" << _master_info.network_address.port
                     << ", code=" << client_status.code() << ", reason=" << e.what();
        return Status::InternalError("Fail to report to master");
    }

    return Status::OK();
}

Status MasterServerClient::confirm_unused_remote_files(
        const TConfirmUnusedRemoteFilesRequest& request, TConfirmUnusedRemoteFilesResult* result) {
    Status client_status;
    FrontendServiceConnection client(&s_client_cache, _master_info.network_address,
                                     config::thrift_rpc_timeout_ms, &client_status);

    if (!client_status.ok()) {
        return Status::InternalError(
                "fail to get master client from cache. host={}, port={}, code={}",
                _master_info.network_address.hostname, _master_info.network_address.port,
                client_status.code());
    }
    try {
        try {
            client->confirmUnusedRemoteFiles(*result, request);
        } catch (TTransportException& e) {
            TTransportException::TTransportExceptionType type = e.getType();
            if (type != TTransportException::TTransportExceptionType::TIMED_OUT) {
                // if not TIMED_OUT, retry
                LOG(WARNING) << "master client, retry finishTask: " << e.what();

                client_status = client.reopen(config::thrift_rpc_timeout_ms);
                if (!client_status.ok()) {
                    return Status::InternalError(
                            "fail to get master client from cache. host={}, port={}, code={}",
                            _master_info.network_address.hostname,
                            _master_info.network_address.port, client_status.code());
                }

                client->confirmUnusedRemoteFiles(*result, request);
            } else {
                // TIMED_OUT exception. do not retry
                // actually we don't care what FE returns.
                return Status::InternalError(
                        "fail to confirm unused remote files. host={}, port={}, code={}, reason={}",
                        _master_info.network_address.hostname, _master_info.network_address.port,
                        client_status.code(), e.what());
            }
        }
    } catch (std::exception& e) {
        RETURN_IF_ERROR(client.reopen(config::thrift_rpc_timeout_ms));
        return Status::InternalError(
                "fail to confirm unused remote files. host={}, port={}, code={}, reason={}",
                _master_info.network_address.hostname, _master_info.network_address.port,
                client_status.code(), e.what());
    }

    return Status::OK();
}

bool AgentUtils::exec_cmd(const string& command, string* errmsg, bool redirect_stderr) {
    // The exit status of the command.
    uint32_t rc = 0;

    // Redirect stderr to stdout to get error message.
    string cmd = command;
    if (redirect_stderr) {
        cmd += " 2>&1";
    }

    // Execute command.
    FILE* fp = popen(cmd.c_str(), "r");
    if (fp == nullptr) {
        *errmsg = fmt::format("popen failed. {}, with errno: {}.\n", strerror(errno), errno);
        return false;
    }

    // Get command output.
    char result[1024] = {'\0'};
    while (fgets(result, sizeof(result), fp) != nullptr) {
        *errmsg += result;
    }

    // Waits for the associated process to terminate and returns.
    rc = pclose(fp);
    if (rc == -1) {
        if (errno == ECHILD) {
            *errmsg += "pclose cannot obtain the child status.\n";
        } else {
            *errmsg += fmt::format("Close popen failed. {}, with errno: {}.\n", strerror(errno),
                                   errno);
        }
        return false;
    }

    // Get return code of command.
    int32_t status_child = WEXITSTATUS(rc);
    if (status_child == 0) {
        return true;
    } else {
        return false;
    }
}

bool AgentUtils::write_json_to_file(const map<string, string>& info, const string& path) {
    rapidjson::Document json_info(rapidjson::kObjectType);
    for (auto& it : info) {
        json_info.AddMember(rapidjson::Value(it.first.c_str(), json_info.GetAllocator()).Move(),
                            rapidjson::Value(it.second.c_str(), json_info.GetAllocator()).Move(),
                            json_info.GetAllocator());
    }
    rapidjson::StringBuffer json_info_str;
    rapidjson::Writer<rapidjson::StringBuffer> writer(json_info_str);
    json_info.Accept(writer);
    std::ofstream fp(path);
    if (!fp) {
        return false;
    }
    fp << json_info_str.GetString() << std::endl;
    fp.close();

    return true;
}

} // namespace doris
