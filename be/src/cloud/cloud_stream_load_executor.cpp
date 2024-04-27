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

#include "cloud/cloud_stream_load_executor.h"

#include "cloud/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/stream_load/stream_load_context.h"

namespace doris {

CloudStreamLoadExecutor::CloudStreamLoadExecutor(ExecEnv* exec_env)
        : StreamLoadExecutor(exec_env) {}

CloudStreamLoadExecutor::~CloudStreamLoadExecutor() = default;

Status CloudStreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    // forward to fe to execute commit transaction for MoW table
    Status st;
    int retry_times = 0;
    // mow table will retry when DELETE_BITMAP_LOCK_ERROR occurs
    do {
        st = StreamLoadExecutor::commit_txn(ctx);
        if (st.ok() || !st.is<ErrorCode::DELETE_BITMAP_LOCK_ERROR>()) {
            break;
        }
        LOG_WARNING("Failed to commit txn")
                .tag("txn_id", ctx->txn_id)
                .tag("retry_times", retry_times)
                .error(st);
        retry_times++;
    } while (retry_times < config::mow_stream_load_commit_retry_times);
    return st;
}

} // namespace doris