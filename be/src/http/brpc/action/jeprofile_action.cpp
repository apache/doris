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

#include "jeprofile_action.h"

#include <brpc/http_method.h>
#include <jemalloc/jemalloc.h>

#include "util/file_utils.h"

namespace doris {
JeProfileHandler::JeProfileHandler() : BaseHttpHandler("jeprofile") {}

void JeProfileHandler::handle_sync(brpc::Controller* cntl) {
    std::lock_guard<std::mutex> lock(_mutex);
#ifndef USE_JEMALLOC
    std::string str = "jemalloc heap dump is not available without setting USE_JEMALLOC";
    on_succ(cntl, str);
#else
    std::stringstream tmp_jeprof_file_name;
    std::time_t now = std::time(nullptr);
    // Build a temporary file name that is hopefully unique.
    tmp_jeprof_file_name << config::jeprofile_dir << "/jeheap_dump." << now << "." << getpid()
                         << "." << rand() << ".heap";
    const std::string& tmp_file_name_str = tmp_jeprof_file_name.str();
    const char* file_name_ptr = tmp_file_name_str.c_str();
    int result = je_mallctl("prof.dump", nullptr, nullptr, &file_name_ptr, sizeof(const char*));
    std::stringstream response;
    if (result == 0) {
        response << "Jemalloc heap dump success, dump file path: " << tmp_jeprof_file_name.str()
                 << "\n";
    } else {
        response << "Jemalloc heap dump failed, je_mallctl return: " << result << "\n";
    }
    on_succ(cntl, response.str());
#endif
}

bool JeProfileHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET;
}

Status JeProfileHandler::setup(doris::ExecEnv* exec_env, HandlerDispatcher* dispatcher) {
    if (!config::jeprofile_dir.empty()) {
        FileUtils::create_dir(config::jeprofile_dir);
    }
    dispatcher->add_handler(new JeProfileHandler());
    return Status::OK();
}
} // namespace doris