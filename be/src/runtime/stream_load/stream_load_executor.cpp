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

#include <bvar/bvar.h>
#include <bvar/latency_recorder.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdint.h>

#include <future>
#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "common/utils.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/message_body_sink.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "thrift/protocol/TDebugProtocol.h"
#include "util/doris_metrics.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

#ifdef BE_TEST
TLoadTxnBeginResult k_stream_load_begin_result;
TLoadTxnCommitResult k_stream_load_commit_result;
TLoadTxnRollbackResult k_stream_load_rollback_result;
Status k_stream_load_plan_status;
#endif

bvar::LatencyRecorder g_stream_load_begin_txn_latency("stream_load", "begin_txn");
bvar::LatencyRecorder g_stream_load_precommit_txn_latency("stream_load", "precommit_txn");
bvar::LatencyRecorder g_stream_load_commit_txn_latency("stream_load", "commit_txn");

Status StreamLoadExecutor::execute_plan_fragment(std::shared_ptr<StreamLoadContext> ctx) {
// submit this params
#ifndef BE_TEST
    ctx->start_write_data_nanos = MonotonicNanos();
    LOG(INFO) << "begin to execute job. label=" << ctx->label << ", txn_id=" << ctx->txn_id
              << ", query_id=" << print_id(ctx->put_result.params.params.query_id);
    Status st;
    if (ctx->put_result.__isset.params) {
        st = _exec_env->fragment_mgr()->exec_plan_fragment(
                ctx->put_result.params, [ctx, this](RuntimeState* state, Status* status) {
                    ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
                    ctx->commit_infos = std::move(state->tablet_commit_infos());
                    if (status->ok()) {
                        ctx->number_total_rows = state->num_rows_load_total();
                        ctx->number_loaded_rows = state->num_rows_load_success();
                        ctx->number_filtered_rows = state->num_rows_load_filtered();
                        ctx->number_unselected_rows = state->num_rows_load_unselected();

                        int64_t num_selected_rows =
                                ctx->number_total_rows - ctx->number_unselected_rows;
                        if (num_selected_rows > 0 &&
                            (double)ctx->number_filtered_rows / num_selected_rows >
                                    ctx->max_filter_ratio) {
                            // NOTE: Do not modify the error message here, for historical reasons,
                            // some users may rely on this error message.
                            *status = Status::InternalError("too many filtered rows");
                        }
                        if (ctx->number_filtered_rows > 0 &&
                            !state->get_error_log_file_path().empty()) {
                            ctx->error_url =
                                    to_load_error_http_path(state->get_error_log_file_path());
                        }

                        if (status->ok()) {
                            DorisMetrics::instance()->stream_receive_bytes_total->increment(
                                    ctx->receive_bytes);
                            DorisMetrics::instance()->stream_load_rows_total->increment(
                                    ctx->number_loaded_rows);
                        }
                    } else {
                        LOG(WARNING)
                                << "fragment execute failed"
                                << ", query_id=" << UniqueId(ctx->put_result.params.params.query_id)
                                << ", err_msg=" << status->to_string() << ", " << ctx->brief();
                        // cancel body_sink, make sender known it
                        if (ctx->body_sink != nullptr) {
                            ctx->body_sink->cancel(status->to_string());
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
                    ctx->write_data_cost_nanos = MonotonicNanos() - ctx->start_write_data_nanos;
                    ctx->promise.set_value(*status);

                    if (!status->ok() && ctx->body_sink != nullptr) {
                        // In some cases, the load execution is exited early.
                        // For example, when max_filter_ratio is 0 and illegal data is encountered
                        // during stream loading, the entire load process is terminated early.
                        // However, the http connection may still be sending data to stream_load_pipe
                        // and waiting for it to be consumed.
                        // Therefore, we need to actively cancel to end the pipe.
                        ctx->body_sink->cancel(status->to_string());
                    }

                    if (ctx->need_commit_self && ctx->body_sink != nullptr) {
                        if (ctx->body_sink->cancelled() || !status->ok()) {
                            ctx->status = *status;
                            this->rollback_txn(ctx.get());
                        } else {
                            static_cast<void>(this->commit_txn(ctx.get()));
                        }
                    }
                });
    } else {
        st = _exec_env->fragment_mgr()->exec_plan_fragment(
                ctx->put_result.pipeline_params, [ctx, this](RuntimeState* state, Status* status) {
                    ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
                    ctx->commit_infos = std::move(state->tablet_commit_infos());
                    if (status->ok()) {
                        ctx->number_total_rows = state->num_rows_load_total();
                        ctx->number_loaded_rows = state->num_rows_load_success();
                        ctx->number_filtered_rows = state->num_rows_load_filtered();
                        ctx->number_unselected_rows = state->num_rows_load_unselected();

                        int64_t num_selected_rows =
                                ctx->number_total_rows - ctx->number_unselected_rows;
                        if (num_selected_rows > 0 &&
                            (double)ctx->number_filtered_rows / num_selected_rows >
                                    ctx->max_filter_ratio) {
                            // NOTE: Do not modify the error message here, for historical reasons,
                            // some users may rely on this error message.
                            *status = Status::InternalError("too many filtered rows");
                        }
                        if (ctx->number_filtered_rows > 0 &&
                            !state->get_error_log_file_path().empty()) {
                            ctx->error_url =
                                    to_load_error_http_path(state->get_error_log_file_path());
                        }

                        if (status->ok()) {
                            DorisMetrics::instance()->stream_receive_bytes_total->increment(
                                    ctx->receive_bytes);
                            DorisMetrics::instance()->stream_load_rows_total->increment(
                                    ctx->number_loaded_rows);
                        }
                    } else {
                        LOG(WARNING)
                                << "fragment execute failed"
                                << ", query_id=" << UniqueId(ctx->put_result.params.params.query_id)
                                << ", err_msg=" << status->to_string() << ", " << ctx->brief();
                        // cancel body_sink, make sender known it
                        if (ctx->body_sink != nullptr) {
                            ctx->body_sink->cancel(status->to_string());
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
                    ctx->write_data_cost_nanos = MonotonicNanos() - ctx->start_write_data_nanos;
                    ctx->promise.set_value(*status);

                    if (!status->ok() && ctx->body_sink != nullptr) {
                        // In some cases, the load execution is exited early.
                        // For example, when max_filter_ratio is 0 and illegal data is encountered
                        // during stream loading, the entire load process is terminated early.
                        // However, the http connection may still be sending data to stream_load_pipe
                        // and waiting for it to be consumed.
                        // Therefore, we need to actively cancel to end the pipe.
                        ctx->body_sink->cancel(status->to_string());
                    }

                    if (ctx->need_commit_self && ctx->body_sink != nullptr) {
                        if (ctx->body_sink->cancelled() || !status->ok()) {
                            ctx->status = *status;
                            this->rollback_txn(ctx.get());
                        } else {
                            static_cast<void>(this->commit_txn(ctx.get()));
                        }
                    }
                });
    }
    if (!st.ok()) {
        // no need to check unref's return value
        return st;
    }
#else
    ctx->promise.set_value(k_stream_load_plan_status);
#endif
    return Status::OK();
}

Status StreamLoadExecutor::begin_txn(StreamLoadContext* ctx) {
    DorisMetrics::instance()->stream_load_txn_begin_request_total->increment(1);

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

    TLoadTxnBeginResult result;
    Status status;
    int64_t duration_ns = 0;
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    if (master_addr.hostname.empty() || master_addr.port == 0) {
        status = Status::Error<SERVICE_UNAVAILABLE>("Have not get FE Master heartbeat yet");
    } else {
        SCOPED_RAW_TIMER(&duration_ns);
#ifndef BE_TEST
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxnBegin(result, request);
                }));
#else
        result = k_stream_load_begin_result;
#endif
        status = Status::create(result.status);
    }
    g_stream_load_begin_txn_latency << duration_ns / 1000;
    if (!status.ok()) {
        LOG(WARNING) << "begin transaction failed, errmsg=" << status << ctx->brief();
        if (result.__isset.job_status) {
            ctx->existing_job_status = result.job_status;
        }
        return status;
    }
    ctx->txn_id = result.txnId;
    if (result.__isset.db_id) {
        ctx->db_id = result.db_id;
    }
    ctx->need_rollback = true;

    return Status::OK();
}

Status StreamLoadExecutor::pre_commit_txn(StreamLoadContext* ctx) {
    TLoadTxnCommitRequest request;
    get_commit_request(ctx, request);

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    TLoadTxnCommitResult result;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
#ifndef BE_TEST
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxnPreCommit(result, request);
                },
                config::txn_commit_rpc_timeout_ms));
#else
        result = k_stream_load_commit_result;
#endif
    }
    g_stream_load_precommit_txn_latency << duration_ns / 1000;
    // Return if this transaction is precommitted successful; otherwise, we need try
    // to
    // rollback this transaction
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "precommit transaction failed, errmsg=" << status << ctx->brief();
        if (status.is<PUBLISH_TIMEOUT>()) {
            ctx->need_rollback = false;
        }
        ctx->status = status;
        return status;
    }
    // precommit success, set need_rollback to false
    ctx->need_rollback = false;
    return Status::OK();
}

Status StreamLoadExecutor::operate_txn_2pc(StreamLoadContext* ctx) {
    TLoadTxn2PCRequest request;
    set_request_auth(&request, ctx->auth);
    request.__set_db(ctx->db);
    request.__set_operation(ctx->txn_operation);
    request.__set_thrift_rpc_timeout_ms(config::txn_commit_rpc_timeout_ms);
    request.__set_label(ctx->label);
    if (ctx->txn_id != ctx->default_txn_id) {
        request.__set_txnId(ctx->txn_id);
    }

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    TLoadTxn2PCResult result;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxn2PC(result, request);
                },
                config::txn_commit_rpc_timeout_ms));
    }
    g_stream_load_commit_txn_latency << duration_ns / 1000;
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "2PC commit transaction failed, errmsg=" << status;
        return status;
    }
    return Status::OK();
}

void StreamLoadExecutor::get_commit_request(StreamLoadContext* ctx,
                                            TLoadTxnCommitRequest& request) {
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    if (ctx->db_id > 0) {
        request.db_id = ctx->db_id;
        request.__isset.db_id = true;
    }
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.sync = true;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;
    request.__set_thrift_rpc_timeout_ms(config::txn_commit_rpc_timeout_ms);
    request.tbls = ctx->table_list;
    request.__isset.tbls = true;

    VLOG_DEBUG << "commit txn request:" << apache::thrift::ThriftDebugString(request);

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = attachment;
        request.__isset.txnCommitAttachment = true;
    }
}

Status StreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    DorisMetrics::instance()->stream_load_txn_commit_request_total->increment(1);

    TLoadTxnCommitRequest request;
    get_commit_request(ctx, request);

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
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "commit transaction failed, errmsg=" << status << ", " << ctx->brief();
        if (status.is<PUBLISH_TIMEOUT>()) {
            ctx->need_rollback = false;
        }
        ctx->status = status;
        return status;
    }
    // commit success, set need_rollback to false
    ctx->need_rollback = false;
    return Status::OK();
}

void StreamLoadExecutor::rollback_txn(StreamLoadContext* ctx) {
    DorisMetrics::instance()->stream_load_txn_rollback_request_total->increment(1);

    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    TLoadTxnRollbackRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    if (ctx->db_id > 0) {
        request.db_id = ctx->db_id;
        request.__isset.db_id = true;
    }
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.__set_reason(ctx->status.to_string());
    request.tbls = ctx->table_list;
    request.__isset.tbls = true;

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = attachment;
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
        LOG(WARNING) << "transaction rollback failed. errmsg=" << rpc_st << ctx->brief();
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
        LOG(FATAL) << "mini load is not supported any more";
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
        rl_attach.__set_loadCostMs(ctx->load_cost_millis);

        attach->rlTaskTxnCommitAttachment = rl_attach;
        attach->__isset.rlTaskTxnCommitAttachment = true;
        break;
    }
    default:
        // unknown load type, should not happened
        return false;
    }

    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        TRLTaskTxnCommitAttachment& rl_attach = attach->rlTaskTxnCommitAttachment;
        rl_attach.loadSourceType = TLoadSourceType::KAFKA;

        TKafkaRLTaskProgress kafka_progress;
        kafka_progress.partitionCmtOffset = ctx->kafka_info->cmt_offset;

        rl_attach.kafkaRLTaskProgress = kafka_progress;
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

} // namespace doris
