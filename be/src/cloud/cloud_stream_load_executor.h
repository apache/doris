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
#include "runtime/stream_load/stream_load_executor.h"

namespace doris {

class CloudStreamLoadExecutor final : public StreamLoadExecutor {
public:
    CloudStreamLoadExecutor(ExecEnv* exec_env);

    ~CloudStreamLoadExecutor() override;

    Status pre_commit_txn(StreamLoadContext* ctx) override;

    Status operate_txn_2pc(StreamLoadContext* ctx) override;

    Status commit_txn(StreamLoadContext* ctx) override;

    void rollback_txn(StreamLoadContext* ctx) override;
};

} // namespace doris
