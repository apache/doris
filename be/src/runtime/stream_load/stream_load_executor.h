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

#include "common/factory_creator.h"

namespace doris {

class ExecEnv;
class StreamLoadContext;
class Status;
class TTxnCommitAttachment;
class TLoadTxnCommitRequest;

class StreamLoadExecutor {
    ENABLE_FACTORY_CREATOR(StreamLoadExecutor);

public:
    StreamLoadExecutor(ExecEnv* exec_env) : _exec_env(exec_env) {}

    virtual ~StreamLoadExecutor() = default;

    Status begin_txn(StreamLoadContext* ctx);

    virtual Status pre_commit_txn(StreamLoadContext* ctx);

    virtual Status operate_txn_2pc(StreamLoadContext* ctx);

    virtual Status commit_txn(StreamLoadContext* ctx);

    void get_commit_request(StreamLoadContext* ctx, TLoadTxnCommitRequest& request);

    virtual void rollback_txn(StreamLoadContext* ctx);

    Status execute_plan_fragment(std::shared_ptr<StreamLoadContext> ctx);

protected:
    // collect the load statistics from context and set them to stat
    // return true if stat is set, otherwise, return false
    bool collect_load_stat(StreamLoadContext* ctx, TTxnCommitAttachment* attachment);

    ExecEnv* _exec_env = nullptr;
};

} // namespace doris
