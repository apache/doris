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

#pragma once

#include <memory>

#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/Types_types.h"

namespace doris {

class ExecEnv;
class StreamLoadContext;
class Status;
class TTxnCommitAttachment;
class StreamLoadPipe;

class StreamLoadExecutor {
public:
    StreamLoadExecutor(ExecEnv* exec_env) : _exec_env(exec_env) {}

    Status begin_txn(StreamLoadContext* ctx);

    Status pre_commit_txn(StreamLoadContext* ctx);

    Status operate_txn_2pc(StreamLoadContext* ctx);

    Status commit_txn(StreamLoadContext* ctx);

    void get_commit_request(StreamLoadContext* ctx, TLoadTxnCommitRequest& request);

    void rollback_txn(StreamLoadContext* ctx);

    Status execute_plan_fragment(StreamLoadContext* ctx);

private:
    // collect the load statistics from context and set them to stat
    // return true if stat is set, otherwise, return false
    bool collect_load_stat(StreamLoadContext* ctx, TTxnCommitAttachment* attachment);

private:
    ExecEnv* _exec_env;
};

} // namespace doris
