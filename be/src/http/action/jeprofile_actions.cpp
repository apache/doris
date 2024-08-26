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

#include "http/action/jeprofile_actions.h"

#include <jemalloc/jemalloc.h>
#include <stdlib.h>
#include <unistd.h>

#include <ctime>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include "common/config.h"
#include "common/object_pool.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_handler_with_auth.h"
#include "http/http_method.h"
#include "io/fs/local_file_system.h"

namespace doris {
class HttpRequest;

static std::mutex kJeprofileActionMutex;
class JeHeapAction : public HttpHandlerWithAuth {
public:
    JeHeapAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {}
    virtual ~JeHeapAction() = default;

    virtual void handle(HttpRequest* req) override;
};

void JeHeapAction::handle(HttpRequest* req) {
    std::lock_guard<std::mutex> lock(kJeprofileActionMutex);
#ifndef USE_JEMALLOC
    std::string str = "jemalloc heap dump is not available without setting USE_JEMALLOC";
    HttpChannel::send_reply(req, str);
#else
    std::stringstream tmp_jeprof_file_name;
    std::time_t now = std::time(nullptr);
    // Build a temporary file name that is hopefully unique.
    tmp_jeprof_file_name << config::jeprofile_dir << "/jeheap_dump." << now << "." << getpid()
                         << "." << rand() << ".heap";
    const std::string& tmp_file_name_str = tmp_jeprof_file_name.str();
    const char* file_name_ptr = tmp_file_name_str.c_str();
    int result = jemallctl("prof.dump", nullptr, nullptr, &file_name_ptr, sizeof(const char*));
    std::stringstream response;
    if (result == 0) {
        response << "Jemalloc heap dump success, dump file path: " << tmp_jeprof_file_name.str()
                 << "\n";
    } else {
        response << "Jemalloc heap dump failed, je_mallctl return: " << result << "\n";
    }
    HttpChannel::send_reply(req, response.str());
#endif
}

Status JeprofileActions::setup(doris::ExecEnv* exec_env, doris::EvHttpServer* http_server,
                               doris::ObjectPool& pool) {
    if (!config::jeprofile_dir.empty()) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(config::jeprofile_dir));
    }
    http_server->register_handler(HttpMethod::GET, "/jeheap/dump",
                                  pool.add(new JeHeapAction(exec_env)));
    return Status::OK();
}

} // namespace doris
