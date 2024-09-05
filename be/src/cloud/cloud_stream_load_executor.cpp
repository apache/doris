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

enum class TxnOpParamType : int {
    ILLEGAL,
    WITH_TXN_ID,
    WITH_LABEL,
};

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
    std::stringstream ss;
    ss << "db_id=" << ctx->db_id << " txn_id=" << ctx->txn_id << " label=" << ctx->label
       << " txn_2pc_op=" << ctx->txn_operation;
    std::string op_info = ss.str();
    VLOG_DEBUG << "operate_txn_2pc " << op_info;
    TxnOpParamType topt = ctx->txn_id > 0       ? TxnOpParamType::WITH_TXN_ID
                          : !ctx->label.empty() ? TxnOpParamType::WITH_LABEL
                                                : TxnOpParamType::ILLEGAL;

    Status st = Status::InternalError<false>("impossible branch reached, " + op_info);

    if (ctx->txn_operation.compare("commit") == 0) {
        if (!config::enable_stream_load_commit_txn_on_be) {
            VLOG_DEBUG << "2pc commit stream load txn with FE support: " << op_info;
            st = StreamLoadExecutor::operate_txn_2pc(ctx);
        } else if (topt == TxnOpParamType::WITH_TXN_ID) {
            VLOG_DEBUG << "2pc commit stream load txn directly: " << op_info;
            st = _exec_env->storage_engine().to_cloud().meta_mgr().commit_txn(*ctx, true);
        } else if (topt == TxnOpParamType::WITH_LABEL) {
            VLOG_DEBUG << "2pc commit stream load txn with FE support: " << op_info;
            st = StreamLoadExecutor::operate_txn_2pc(ctx);
        } else {
            st = Status::InternalError<false>(
                    "failed to 2pc commit txn, with TxnOpParamType::illegal input, " + op_info);
        }
    } else if (ctx->txn_operation.compare("abort") == 0) {
        if (topt == TxnOpParamType::WITH_TXN_ID) {
            LOG(INFO) << "2pc abort stream load txn directly: " << op_info;
            st = _exec_env->storage_engine().to_cloud().meta_mgr().abort_txn(*ctx);
            WARN_IF_ERROR(st, "failed to rollback txn " + op_info);
        } else if (topt == TxnOpParamType::WITH_LABEL) { // maybe a label send to FE to abort
            VLOG_DEBUG << "2pc abort stream load txn with FE support: " << op_info;
            StreamLoadExecutor::rollback_txn(ctx);
            st = Status::OK();
        } else {
            st = Status::InternalError<false>("failed abort txn, with illegal input, " + op_info);
        }
    } else {
        std::string msg =
                "failed to operate_txn_2pc, unrecognized operation: " + ctx->txn_operation;
        LOG(WARNING) << msg << " " << op_info;
        st = Status::InternalError<false>(msg + " " + op_info);
    }
    WARN_IF_ERROR(st, "failed to operate_txn_2pc " + op_info)
    return st;
}

Status CloudStreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    // forward to fe to excute commit transaction for MoW table
    if (ctx->is_mow_table() || !config::enable_stream_load_commit_txn_on_be ||
        ctx->load_type == TLoadType::ROUTINE_LOAD) {
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
    std::stringstream ss;
    ss << "db_id=" << ctx->db_id << " txn_id=" << ctx->txn_id << " label=" << ctx->label;
    std::string op_info = ss.str();
    LOG(INFO) << "rollback stream load txn " << op_info;
    TxnOpParamType topt = ctx->txn_id > 0       ? TxnOpParamType::WITH_TXN_ID
                          : !ctx->label.empty() ? TxnOpParamType::WITH_LABEL
                                                : TxnOpParamType::ILLEGAL;

    if (topt == TxnOpParamType::WITH_TXN_ID && ctx->load_type != TLoadType::ROUTINE_LOAD) {
        VLOG_DEBUG << "abort stream load txn directly: " << op_info;
        WARN_IF_ERROR(_exec_env->storage_engine().to_cloud().meta_mgr().abort_txn(*ctx),
                      "failed to rollback txn " + op_info);
    } else { // maybe a label send to FE to abort
        // does not care about the return status
        // ctx->db_id > 0 && !ctx->label.empty()
        VLOG_DEBUG << "abort stream load txn with FE support: " << op_info;
        StreamLoadExecutor::rollback_txn(ctx);
    }
}

} // namespace doris
