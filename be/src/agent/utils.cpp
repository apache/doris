// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#include <arpa/inet.h>
#include <cstdio>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <thrift/Thrift.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/transport/TTransportUtils.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Status_types.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"

using std::map;
using std::pair;
using std::string;
using std::stringstream;
using std::vector;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::TException;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TTransportException;

namespace palo {


AgentServerClient::AgentServerClient(const TBackend backend) :
        _socket(new TSocket(backend.host, backend.be_port)),
        _transport(new TBufferedTransport(_socket)),
        _protocol(new TBinaryProtocol(_transport)),
        _agent_service_client(_protocol) {
}

AgentServerClient::~AgentServerClient() {
    if (_transport != NULL) {
        _transport->close();
    }
}

AgentStatus AgentServerClient::make_snapshot(
        const TSnapshotRequest& snapshot_request,
        TAgentResult* result) {
    AgentStatus status = PALO_SUCCESS;

    TAgentResult thrift_result;
    try {
        _transport->open();
        _agent_service_client.make_snapshot(thrift_result, snapshot_request);
        *result = thrift_result;
        _transport->close();
    } catch (TException& e) {
        OLAP_LOG_WARNING("agent clinet make snapshot, "
                         "get exception, error: %s", e.what());
        _transport->close();
        status = PALO_ERROR;
    }

    return status;
}

AgentStatus AgentServerClient::release_snapshot(
        const string& snapshot_path,
        TAgentResult* result) {
    AgentStatus status = PALO_SUCCESS;

    try {
        _transport->open();
        _agent_service_client.release_snapshot(*result, snapshot_path);
        _transport->close();
    } catch (TException& e) {
        OLAP_LOG_WARNING("agent clinet make snapshot, "
                         "get exception, error: %s", e.what());
        _transport->close();
        status = PALO_ERROR;
    }
    
    return status;
}

MasterServerClient::MasterServerClient(
        const TMasterInfo& master_info,
        FrontendServiceClientCache* client_cache) :
        _master_info(master_info),
        _client_cache(client_cache) {
}

AgentStatus MasterServerClient::finish_task(
        const TFinishTaskRequest request,
        TMasterResult* result) {
    Status client_status;
    FrontendServiceConnection client(
            _client_cache,
            _master_info.network_address,
            MASTER_CLIENT_TIMEOUT,
            &client_status);

    if (!client_status.ok()) {
        OLAP_LOG_WARNING("master client, get client from cache failed."
                         "host: %s, port: %d, code: %d",
                         _master_info.network_address.hostname.c_str(),
                         _master_info.network_address.port,
                         client_status.code());
        return PALO_ERROR;
    }

    try {
        try {
            client->finishTask(*result, request);
        } catch (TTransportException& e) {
            OLAP_LOG_WARNING("master client, retry finishTask: %s", e.what());
            client_status = client.reopen(MASTER_CLIENT_TIMEOUT);

            if (!client_status.ok()) {
                OLAP_LOG_WARNING("master client, get client from cache failed."
                                 "host: %s, port: %d, code: %d",
                                 _master_info.network_address.hostname.c_str(),
                                 _master_info.network_address.port,
                                 client_status.code());
                return PALO_ERROR;
            }

            client->finishTask(*result, request);
        }
    } catch (TException& e) {
        OLAP_LOG_WARNING("master client, finishTask execute failed."
                         "host: %s, port: %d, error: %s",
                         _master_info.network_address.hostname.c_str(),
                         _master_info.network_address.port,
                         e.what());
        return PALO_ERROR;
    }

    return PALO_SUCCESS;
}

AgentStatus MasterServerClient::report(const TReportRequest request, TMasterResult* result) {
    Status client_status;
    FrontendServiceConnection client(
            _client_cache,
            _master_info.network_address,
            MASTER_CLIENT_TIMEOUT,
            &client_status);

    if (!client_status.ok()) {
        OLAP_LOG_WARNING("master client, get client from cache failed."
                         "host: %s, port: %d, code: %d",
                         _master_info.network_address.hostname.c_str(),
                         _master_info.network_address.port,
                         client_status.code());
        return PALO_ERROR;
    }

    try {
        try {
            client->report(*result, request);
        } catch (TTransportException& e) {
            TTransportException::TTransportExceptionType type = e.getType();
            if (type != TTransportException::TTransportExceptionType::TIMED_OUT) {
                // if not TIMED_OUT, retry
                OLAP_LOG_WARNING("master client, retry report: %s", e.what());

                client_status = client.reopen(MASTER_CLIENT_TIMEOUT);
                if (!client_status.ok()) {
                    OLAP_LOG_WARNING("master client, get client from cache failed."
                                     "host: %s, port: %d, code: %d",
                                     _master_info.network_address.hostname.c_str(),
                                     _master_info.network_address.port,
                                     client_status.code());
                    return PALO_ERROR;
                }   

                client->report(*result, request);
            } else {
                // TIMED_OUT exception. do not retry
                // actually we don't care what FE returns.
                OLAP_LOG_WARNING("master client, report failed: %s", e.what());
                return PALO_ERROR;
            }   
        }   
    } catch (TException& e) {
        OLAP_LOG_WARNING("master client, finish report failed."
                         "host: %s, port: %d, code: %d",
                         _master_info.network_address.hostname.c_str(),
                         _master_info.network_address.port,
                         client_status.code());
        return PALO_ERROR;
    }

    return PALO_SUCCESS;
}

AgentStatus AgentUtils::rsync_from_remote(
        const string& remote_host,
        const string& remote_file_path,
        const string& local_file_path,
        const vector<string>& exclude_file_patterns,
        uint32_t transport_speed_limit_kbps,
        uint32_t timeout_second) {
    int ret_code = 0;
    stringstream cmd_stream;
    cmd_stream << "rsync -r -q -e \"ssh -o StrictHostKeyChecking=no\"";
    for (auto exclude_file_pattern : exclude_file_patterns) {
        cmd_stream << " --exclude=" << exclude_file_pattern;
    }
    if (transport_speed_limit_kbps != 0) {
        cmd_stream << " --bwlimit=" << transport_speed_limit_kbps;
    }
    if (timeout_second != 0) {
        cmd_stream << " --timeout=" << timeout_second;
    }
    cmd_stream << " " << remote_host << ":" << remote_file_path << " " << local_file_path;
    OLAP_LOG_INFO("rsync cmd: %s", cmd_stream.str().c_str());

    FILE* fp = NULL;
    fp = popen(cmd_stream.str().c_str(), "r");

    if (fp == NULL) {
        return PALO_ERROR;
    }

    ret_code = pclose(fp);
    if (ret_code != 0) {
        return PALO_ERROR;
    }

    return PALO_SUCCESS;
}

char* AgentUtils::get_local_ip() {
    char hname[128];
    gethostname(hname, sizeof(hname));

    // Let's hope this is not broken in the glibc we're using
    struct hostent hent;
    struct hostent *he = 0;
    char hbuf[2048];
    int err = 0;
    if (gethostbyname_r(hname, &hent, hbuf, sizeof(hbuf), &he, &err) != 0
            || he == 0) {
        LOG(ERROR) << "gethostbyname : " << hname << ", "
           << "error: " << err;
        return NULL;
    }

    return inet_ntoa(*(struct in_addr*)(he->h_addr_list[0]));
}

std::string AgentUtils::print_agent_status(AgentStatus status) {
    switch (status) {
    case PALO_SUCCESS:
        return "PALO_SUCCESS";
    case PALO_ERROR:
        return "PALO_ERROR";
    case PALO_TASK_REQUEST_ERROR:
        return "PALO_TASK_REQUEST_ERROR";
    case PALO_FILE_DOWNLOAD_INVALID_PARAM:
        return "PALO_FILE_DOWNLOAD_INVALID_PARAM";
    case PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED:
        return "PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED";
    case PALO_FILE_DOWNLOAD_CURL_INIT_FAILED:
        return "PALO_FILE_DOWNLOAD_CURL_INIT_FAILED";
    case PALO_FILE_DOWNLOAD_FAILED:
        return "PALO_FILE_DOWNLOAD_FAILED";
    case PALO_FILE_DOWNLOAD_GET_LENGTH_FAILED:
        return "PALO_FILE_DOWNLOAD_GET_LENGTH_FAILED";
    case PALO_FILE_DOWNLOAD_NOT_EXIST:
        return "PALO_FILE_DOWNLOAD_NOT_EXIST";
    case PALO_FILE_DOWNLOAD_LIST_DIR_FAIL:
        return "PALO_FILE_DOWNLOAD_LIST_DIR_FAIL";
    case PALO_CREATE_TABLE_EXIST:
        return "PALO_CREATE_TABLE_EXIST";
    case PALO_CREATE_TABLE_DIFF_SCHEMA_EXIST:
        return "PALO_CREATE_TABLE_DIFF_SCHEMA_EXIST";
    case PALO_CREATE_TABLE_NOT_EXIST:
        return "PALO_CREATE_TABLE_NOT_EXIST";
    case PALO_DROP_TABLE_NOT_EXIST:
        return "PALO_DROP_TABLE_NOT_EXIST";
    case PALO_PUSH_INVALID_TABLE:
        return "PALO_PUSH_INVALID_TABLE";
    case PALO_PUSH_INVALID_VERSION:
        return "PALO_PUSH_INVALID_VERSION";
    case PALO_PUSH_TIME_OUT:
        return "PALO_PUSH_TIME_OUT";
    case PALO_PUSH_HAD_LOADED:
        return "PALO_PUSH_HAD_LOADED";
    case PALO_TIMEOUT:
        return "PALO_TIMEOUT";
    case PALO_INTERNAL_ERROR:
        return "PALO_INTERNAL_ERROR";
    default:
        return "UNKNOWM";
    }
}

bool AgentUtils::exec_cmd(const string& command, string* errmsg) {
    // The exit status of the command.
    uint32_t rc = 0;

    // Redirect stderr to stdout to get error message.
    string cmd = command + " 2>&1";

    // Execute command.
    FILE *fp = popen(cmd.c_str(), "r");
    if (fp == NULL) {
        stringstream err_stream;
        err_stream << "popen failed. " << strerror(errno) << ", with errno: " << errno << ".\n";
        *errmsg = err_stream.str();
        return false;
    }

    // Get command output.
    char result[1024] = {'\0'};
    while (fgets(result, sizeof(result), fp) != NULL) {
        *errmsg += result;
    }

    // Waits for the associated process to terminate and returns.
    rc = pclose(fp);
    if (rc == -1) {
        if (errno==ECHILD) {
            *errmsg += "pclose cannot obtain the child status.\n";
        } else {
            stringstream err_stream;
            err_stream << "Close popen failed. " << strerror(errno) << ", with errno: "
                       << errno << "\n";
            *errmsg += err_stream.str();
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
    for (auto &it : info) {
        json_info.AddMember(
            rapidjson::Value(it.first.c_str(), json_info.GetAllocator()).Move(),
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

}  // namespace palo
