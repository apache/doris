// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_COMMON_UTIL_MINI_LOAD_H
#define BDG_PALO_BE_SRC_COMMON_UTIL_MINI_LOAD_H

#include <mutex>
#include <map>
#include <string>
#include <set>

#include "common/status.h"
#include "http/http_handler.h"
#include "util/defer_op.h"

namespace palo {

// Used to identify one mini load job
struct LoadHandle {
    std::string db;
    std::string label;
    std::string sub_label;
};

struct LoadHandleCmp {
    bool operator() (const LoadHandle& lhs, const LoadHandle& rhs) const;
};

class TMasterResult;
class ExecEnv;

// This a handler for mini load
// path is /api/{db}/{table}/_load
class MiniLoadAction : public HttpHandler {
public:
    MiniLoadAction(ExecEnv* exec_env);

    virtual ~MiniLoadAction() {
    }

    virtual void handle(HttpRequest *req, HttpChannel *channel);

private:
    Status load(
            HttpRequest* req, 
            const std::string& file_path);

    Status data_saved_dir(const LoadHandle& desc, 
                          const std::string& table,
                          std::string* file_path);

    Status receive_data(const LoadHandle& desc, HttpRequest* req, 
                    HttpChannel *channel, std::string* file_path);

    Status check_auth(HttpRequest* http_req);

    void erase_handle(const LoadHandle& handle);

    ExecEnv* _exec_env;

    std::mutex _lock;
    // Used to check if load is duplicated in this instance.
    std::set<LoadHandle, LoadHandleCmp> _current_load;
    
    std::string _user;
    std::string _cluster;
};

}
#endif

