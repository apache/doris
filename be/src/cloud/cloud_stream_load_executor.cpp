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

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/stream_load/stream_load_context.h"

namespace doris {

CloudStreamLoadExecutor::CloudStreamLoadExecutor(ExecEnv* exec_env)
        : StreamLoadExecutor(exec_env) {}

CloudStreamLoadExecutor::~CloudStreamLoadExecutor() = default;

Status CloudStreamLoadExecutor::pre_commit_txn(StreamLoadContext* ctx) {
    auto st = _exec_env->storage_engine().to_cloud().meta_mgr().precommit_txn(*ctx);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to precommit txn: " << st << ", " << ctx->brief();
        return st;
    }
    ctx->need_rollback = false;
    return st;
}

Status CloudStreamLoadExecutor::operate_txn_2pc(StreamLoadContext* ctx) {
    VLOG_DEBUG << "operate_txn_2pc, op: " << ctx->txn_operation;
    if (ctx->txn_operation.compare("commit") == 0) {
        return _exec_env->storage_engine().to_cloud().meta_mgr().commit_txn(*ctx, true);
    } else {
        // 2pc abort
        return _exec_env->storage_engine().to_cloud().meta_mgr().abort_txn(*ctx);
    }
}

Status CloudStreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    if (ctx->load_type == TLoadType::ROUTINE_LOAD) {
        return StreamLoadExecutor::commit_txn(ctx);
    }

    // forward to fe to excute commit transaction for MoW table
    if (ctx->is_mow_table()) {
        Status st;
        int retry_times = 0;
        while (retry_times < config::mow_stream_load_commit_retry_times) {
            st = StreamLoadExecutor::commit_txn(ctx);
            // DELETE_BITMAP_LOCK_ERROR will be retried
            if (st.ok() || !st.is<ErrorCode::DELETE_BITMAP_LOCK_ERROR>()) {
                break;
            }
            LOG_WARNING("Failed to commit txn")
                    .tag("txn_id", ctx->txn_id)
                    .tag("retry_times", retry_times)
                    .error(st);
            retry_times++;
        }
        return st;
    }

    auto st = _exec_env->storage_engine().to_cloud().meta_mgr().commit_txn(*ctx, false);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to commit txn: " << st << ", " << ctx->brief();
        return st;
    }
    ctx->need_rollback = false;
    return st;
}

void CloudStreamLoadExecutor::rollback_txn(StreamLoadContext* ctx) {
    WARN_IF_ERROR(_exec_env->storage_engine().to_cloud().meta_mgr().abort_txn(*ctx),
                  "Failed to rollback txn");
}

} // namespace doris
