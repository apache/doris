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

#include "runtime/stream_load/stream_load_executor.h"

#include "common/status.h"
#include "common/utils.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/doris_metrics.h"
#include "util/thrift_rpc_helper.h"

#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/Types_types.h"

namespace doris {

#ifdef BE_TEST
TLoadTxnBeginResult k_stream_load_begin_result;
TLoadTxnCommitResult k_stream_load_commit_result;
TLoadTxnRollbackResult k_stream_load_rollback_result;
Status k_stream_load_plan_status;
#endif

Status StreamLoadExecutor::execute_plan_fragment(StreamLoadContext* ctx) {
    DorisMetrics::txn_exec_plan_total.increment(1);
// submit this params
#ifndef BE_TEST
    ctx->ref();
    LOG(INFO) << "begin to execute job. label=" << ctx->label << ", txn_id=" << ctx->txn_id
              << ", query_id=" << print_id(ctx->put_result.params.params.query_id);
    auto st = _exec_env->fragment_mgr()->exec_plan_fragment(
            ctx->put_result.params, [ctx](PlanFragmentExecutor* executor) {
                ctx->commit_infos = std::move(executor->runtime_state()->tablet_commit_infos());
                Status status = executor->status();
                if (status.ok()) {
                    ctx->number_total_rows = executor->runtime_state()->num_rows_load_total();
                    ctx->number_loaded_rows = executor->runtime_state()->num_rows_load_success();
                    ctx->number_filtered_rows = executor->runtime_state()->num_rows_load_filtered();
                    ctx->number_unselected_rows =
                            executor->runtime_state()->num_rows_load_unselected();

                    int64_t num_selected_rows =
                            ctx->number_total_rows - ctx->number_unselected_rows;
                    if ((double)ctx->number_filtered_rows / num_selected_rows >
                        ctx->max_filter_ratio) {
                        // NOTE: Do not modify the error message here, for historical
                        // reasons,
                        // some users may rely on this error message.
                        status = Status::InternalError("too many filtered rows");
                    } else if (ctx->number_loaded_rows == 0) {
                        status = Status::InternalError("all partitions have no load data");
                    }
                    if (ctx->number_filtered_rows > 0 &&
                        !executor->runtime_state()->get_error_log_file_path().empty()) {
                        ctx->error_url = to_load_error_http_path(
                                executor->runtime_state()->get_error_log_file_path());
                    }

                    if (status.ok()) {
                        DorisMetrics::stream_receive_bytes_total.increment(ctx->receive_bytes);
                        DorisMetrics::stream_load_rows_total.increment(ctx->number_loaded_rows);
                    }
                } else {
                    LOG(WARNING) << "fragment execute failed"
                                 << ", query_id="
                                 << UniqueId(ctx->put_result.params.params.query_id)
                                 << ", err_msg=" << status.get_error_msg() << ctx->brief();
                    // cancel body_sink, make sender known it
                    if (ctx->body_sink != nullptr) {
                        ctx->body_sink->cancel();
                    }

                    switch (ctx->load_src_type) {
                    // reset the stream load ctx's kafka commit offset
                    case TLoadSourceType::KAFKA:
                        ctx->kafka_info->reset_offset();
                        break;
                    default:
                        break;
                    }
                }
                ctx->promise.set_value(status);
                if (ctx->unref()) {
                    delete ctx;
                }
            });
    if (!st.ok()) {
        // no need to check unref's return value
        ctx->unref();
        return st;
    }
#else
    ctx->promise.set_value(k_stream_load_plan_status);
#endif
    return Status::OK();
}

Status StreamLoadExecutor::begin_txn(StreamLoadContext* ctx) {
    DorisMetrics::txn_begin_request_total.increment(1);

    TLoadTxnBeginRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.label = ctx->label;
    // set timestamp
    request.__set_timestamp(GetCurrentTimeMicros());
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }
    request.__set_request_id(ctx->id.to_thrift());

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    TLoadTxnBeginResult result;
#ifndef BE_TEST
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->loadTxnBegin(result, request);
            }));
#else
    result = k_stream_load_begin_result;
#endif
    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "begin transaction failed, errmsg=" << status.get_error_msg()
                     << ctx->brief();
        if (result.__isset.job_status) {
            ctx->existing_job_status = result.job_status;
        }
        return status;
    }
    ctx->txn_id = result.txnId;
    ctx->need_rollback = true;

    return Status::OK();
}

Status StreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    DorisMetrics::txn_commit_request_total.increment(1);

    TLoadTxnCommitRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.sync = true;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = std::move(attachment);
        request.__isset.txnCommitAttachment = true;
    }

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    TLoadTxnCommitResult result;
#ifndef BE_TEST
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->loadTxnCommit(result, request);
            },
            config::txn_commit_rpc_timeout_ms));
#else
    result = k_stream_load_commit_result;
#endif
    // Return if this transaction is committed successful; otherwise, we need try
    // to
    // rollback this transaction
    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "commit transaction failed, errmsg=" << status.get_error_msg()
                     << ctx->brief();
        if (status.code() == TStatusCode::PUBLISH_TIMEOUT) {
            ctx->need_rollback = false;
        }
        return status;
    }
    // commit success, set need_rollback to false
    ctx->need_rollback = false;
    return Status::OK();
}

void StreamLoadExecutor::rollback_txn(StreamLoadContext* ctx) {
    DorisMetrics::txn_rollback_request_total.increment(1);

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    TLoadTxnRollbackRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.__set_reason(ctx->status.get_error_msg());

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = std::move(attachment);
        request.__isset.txnCommitAttachment = true;
    }

    TLoadTxnRollbackResult result;
#ifndef BE_TEST
    auto rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->loadTxnRollback(result, request);
            });
    if (!rpc_st.ok()) {
        LOG(WARNING) << "transaction rollback failed. errmsg=" << rpc_st.get_error_msg()
                     << ctx->brief();
    }
#else
    result = k_stream_load_rollback_result;
#endif
}

bool StreamLoadExecutor::collect_load_stat(StreamLoadContext* ctx, TTxnCommitAttachment* attach) {
    if (ctx->load_type != TLoadType::ROUTINE_LOAD && ctx->load_type != TLoadType::MINI_LOAD) {
        // currently, only routine load and mini load need to be set attachment
        return false;
    }
    switch (ctx->load_type) {
    case TLoadType::MINI_LOAD: {
        attach->loadType = TLoadType::MINI_LOAD;

        TMiniLoadTxnCommitAttachment ml_attach;
        ml_attach.loadedRows = ctx->number_loaded_rows;
        ml_attach.filteredRows = ctx->number_filtered_rows;
        if (!ctx->error_url.empty()) {
            ml_attach.__set_errorLogUrl(ctx->error_url);
        }

        attach->mlTxnCommitAttachment = std::move(ml_attach);
        attach->__isset.mlTxnCommitAttachment = true;
        break;
    }
    case TLoadType::ROUTINE_LOAD: {
        attach->loadType = TLoadType::ROUTINE_LOAD;

        TRLTaskTxnCommitAttachment rl_attach;
        rl_attach.jobId = ctx->job_id;
        rl_attach.id = ctx->id.to_thrift();
        rl_attach.__set_loadedRows(ctx->number_loaded_rows);
        rl_attach.__set_filteredRows(ctx->number_filtered_rows);
        rl_attach.__set_unselectedRows(ctx->number_unselected_rows);
        rl_attach.__set_receivedBytes(ctx->receive_bytes);
        rl_attach.__set_loadedBytes(ctx->loaded_bytes);
        rl_attach.__set_loadCostMs(ctx->load_cost_nanos / 1000 / 1000);

        attach->rlTaskTxnCommitAttachment = std::move(rl_attach);
        attach->__isset.rlTaskTxnCommitAttachment = true;
        break;
    }
    default:
        // unknown load type, should not happend
        return false;
    }

    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        TRLTaskTxnCommitAttachment& rl_attach = attach->rlTaskTxnCommitAttachment;
        rl_attach.loadSourceType = TLoadSourceType::KAFKA;

        TKafkaRLTaskProgress kafka_progress;
        kafka_progress.partitionCmtOffset = ctx->kafka_info->cmt_offset;

        rl_attach.kafkaRLTaskProgress = std::move(kafka_progress);
        rl_attach.__isset.kafkaRLTaskProgress = true;
        if (!ctx->error_url.empty()) {
            rl_attach.__set_errorLogUrl(ctx->error_url);
        }
        return true;
    }
    default:
        return true;
    }
    return false;
}

} // end namespace
