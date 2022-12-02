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

#include "pipeline_fragment_context.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "exec/agg_context.h"
#include "exec/aggregation_sink_operator.h"
#include "exec/aggregation_source_operator.h"
#include "exec/data_sink.h"
#include "exec/empty_set_operator.h"
#include "exec/exchange_sink_operator.h"
#include "exec/exchange_source_operator.h"
#include "exec/olap_scan_operator.h"
#include "exec/repeat_operator.h"
#include "exec/result_sink_operator.h"
#include "exec/scan_node.h"
#include "exec/sort_sink_operator.h"
#include "exec/sort_source_operator.h"
#include "exec/streaming_aggregation_sink_operator.h"
#include "exec/streaming_aggregation_source_operator.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "pipeline/exec/table_function_operator.h"
#include "pipeline_task.h"
#include "runtime/client_cache.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "task_scheduler.h"
#include "util/container_util.hpp"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vempty_set_node.h"
#include "vec/exec/vexchange_node.h"
#include "vec/exec/vrepeat_node.h"
#include "vec/exec/vsort_node.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/sink/vdata_stream_sender.h"
#include "vec/sink/vresult_sink.h"

using apache::thrift::transport::TTransportException;
using apache::thrift::TException;

namespace doris::pipeline {

PipelineFragmentContext::PipelineFragmentContext(const TUniqueId& query_id,
                                                 const TUniqueId& instance_id, int backend_num,
                                                 std::shared_ptr<QueryFragmentsCtx> query_ctx,
                                                 ExecEnv* exec_env)
        : _query_id(query_id),
          _fragment_instance_id(instance_id),
          _backend_num(backend_num),
          _exec_env(exec_env),
          _cancel_reason(PPlanFragmentCancelReason::INTERNAL_ERROR),
          _closed_pipeline_cnt(0),
          _query_ctx(std::move(query_ctx)) {
    _fragment_watcher.start();
}

PipelineFragmentContext::~PipelineFragmentContext() = default;

void PipelineFragmentContext::cancel(const PPlanFragmentCancelReason& reason,
                                     const std::string& msg) {
    if (!_runtime_state->is_cancelled()) {
        std::lock_guard<std::mutex> l(_status_lock);
        if (_runtime_state->is_cancelled()) {
            return;
        }
        if (reason != PPlanFragmentCancelReason::LIMIT_REACH) {
            _exec_status = Status::Cancelled(msg);
        }
        _runtime_state->set_is_cancelled(true);
        _cancel_reason = reason;
        _cancel_msg = msg;
        // To notify wait_for_start()
        _query_ctx->set_ready_to_execute(true);

        // must close stream_mgr to avoid dead lock in Exchange Node
        _exec_env->vstream_mgr()->cancel(_fragment_instance_id);
        // Cancel the result queue manager used by spark doris connector
        // TODO pipeline incomp
        // _exec_env->result_queue_mgr()->update_queue_status(id, Status::Aborted(msg));
    }
}

PipelinePtr PipelineFragmentContext::add_pipeline() {
    // _prepared、_submitted, _canceled should do not add pipeline
    PipelineId id = _next_pipeline_id++;
    auto pipeline = std::make_shared<Pipeline>(id, shared_from_this());
    _pipelines.emplace_back(pipeline);
    return pipeline;
}

Status PipelineFragmentContext::prepare(const doris::TExecPlanFragmentParams& request) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    //    _runtime_profile.reset(new RuntimeProfile("PipelineContext"));
    //    _start_timer = ADD_TIMER(_runtime_profile, "StartTime");
    //    COUNTER_UPDATE(_start_timer, _fragment_watcher.elapsed_time());
    //    _prepare_timer = ADD_TIMER(_runtime_profile, "PrepareTime");
    //    SCOPED_TIMER(_prepare_timer);

    auto* fragment_context = this;
    OpentelemetryTracer tracer = telemetry::get_noop_tracer();
    if (opentelemetry::trace::Tracer::GetCurrentSpan()->GetContext().IsValid()) {
        tracer = telemetry::get_tracer(print_id(_query_id));
    }
    START_AND_SCOPE_SPAN(tracer, span, "PipelineFragmentExecutor::prepare");

    const TPlanFragmentExecParams& params = request.params;

    LOG_INFO("PipelineFragmentContext::prepare")
            .tag("query_id", _query_id)
            .tag("instance_id", params.fragment_instance_id)
            .tag("backend_num", request.backend_num)
            .tag("pthread_id", (uintptr_t)pthread_self());

    // Must be vec exec engine
    if (!request.query_options.__isset.enable_vectorized_engine ||
        !request.query_options.enable_vectorized_engine) {
        return Status::InternalError("should set enable_vectorized_engine to true");
    }

    // 1. init _runtime_state
    _runtime_state = std::make_unique<RuntimeState>(params, request.query_options,
                                                    _query_ctx->query_globals, _exec_env);
    _runtime_state->set_query_fragments_ctx(_query_ctx.get());
    _runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker);
    _runtime_state->set_tracer(std::move(tracer));

    // TODO should be combine with plan_fragment_executor.prepare funciton
    SCOPED_ATTACH_TASK(get_runtime_state());
    _runtime_state->init_scanner_mem_trackers();
    _runtime_state->runtime_filter_mgr()->init();
    _runtime_state->set_be_number(request.backend_num);

    if (request.__isset.backend_id) {
        _runtime_state->set_backend_id(request.backend_id);
    }
    if (request.__isset.import_label) {
        _runtime_state->set_import_label(request.import_label);
    }
    if (request.__isset.db_name) {
        _runtime_state->set_db_name(request.db_name);
    }
    if (request.__isset.load_job_id) {
        _runtime_state->set_load_job_id(request.load_job_id);
    }
    if (request.__isset.load_error_hub_info) {
        _runtime_state->set_load_error_hub_info(request.load_error_hub_info);
    }

    if (request.query_options.__isset.is_report_success) {
        fragment_context->set_is_report_success(request.query_options.is_report_success);
    }

    auto* desc_tbl = _query_ctx->desc_tbl;
    _runtime_state->set_desc_tbl(desc_tbl);

    // 2. Create ExecNode to build pipeline with PipelineFragmentContext
    RETURN_IF_ERROR(ExecNode::create_tree(_runtime_state.get(), _runtime_state->obj_pool(),
                                          request.fragment.plan, *desc_tbl, &_root_plan));
    _runtime_state->set_fragment_root_id(_root_plan->id());

    // Set senders of exchange nodes before pipeline build
    std::vector<ExecNode*> exch_nodes;
    _root_plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    for (ExecNode* exch_node : exch_nodes) {
        DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
        int num_senders = find_with_default(params.per_exch_num_senders, exch_node->id(), 0);
        DCHECK_GT(num_senders, 0);
        static_cast<vectorized::VExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }

    // All prepare work do in exec node tree
    RETURN_IF_ERROR(_root_plan->prepare(_runtime_state.get()));
    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    std::vector<TScanRangeParams> no_scan_ranges;
    _root_plan->collect_scan_nodes(&scan_nodes);
    VLOG_CRITICAL << "scan_nodes.size()=" << scan_nodes.size();
    VLOG_CRITICAL << "params.per_node_scan_ranges.size()=" << params.per_node_scan_ranges.size();

    _root_plan->try_do_aggregate_serde_improve();
    // set scan range in ScanNode
    for (int i = 0; i < scan_nodes.size(); ++i) {
        // TODO(cmy): this "if...else" should be removed once all ScanNode are derived from VScanNode.
        ExecNode* node = scan_nodes[i];
        if (typeid(*node) == typeid(vectorized::NewOlapScanNode) ||
            typeid(*node) == typeid(vectorized::NewFileScanNode) // ||
//            typeid(*node) == typeid(vectorized::NewOdbcScanNode) ||
//            typeid(*node) == typeid(vectorized::NewEsScanNode)
#ifdef LIBJVM
//            || typeid(*node) == typeid(vectorized::NewJdbcScanNode)
#endif
        ) {
            auto* scan_node = static_cast<vectorized::VScanNode*>(scan_nodes[i]);
            const std::vector<TScanRangeParams>& scan_ranges =
                    find_with_default(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
            scan_node->set_scan_ranges(scan_ranges);
        } else {
            ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[i]);
            const std::vector<TScanRangeParams>& scan_ranges =
                    find_with_default(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
            scan_node->set_scan_ranges(scan_ranges);
            VLOG_CRITICAL << "scan_node_Id=" << scan_node->id() << " size=" << scan_ranges.size();
        }
    }

    _runtime_state->set_per_fragment_instance_idx(params.sender_id);
    _runtime_state->set_num_per_fragment_instances(params.num_senders);

    if (request.fragment.__isset.output_sink) {
        RETURN_IF_ERROR(DataSink::create_data_sink(
                _runtime_state->obj_pool(), request.fragment.output_sink,
                request.fragment.output_exprs, params, _root_plan->row_desc(), _runtime_state.get(),
                &_sink, *desc_tbl));
    }

    _root_pipeline = fragment_context->add_pipeline();
    RETURN_IF_ERROR(_build_pipelines(_root_plan, _root_pipeline));
    if (_sink) {
        RETURN_IF_ERROR(_create_sink(request.fragment.output_sink));
    }
    RETURN_IF_ERROR(_build_pipeline_tasks(request));
    _runtime_state->runtime_profile()->add_child(_sink->profile(), true, nullptr);
    _runtime_state->runtime_profile()->add_child(_root_plan->runtime_profile(), true, nullptr);

    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(
        const doris::TExecPlanFragmentParams& request) {
    for (auto& pipeline : _pipelines) {
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }

    for (PipelinePtr& pipeline : _pipelines) {
        // if sink
        auto sink = pipeline->sink()->build_operator();
        RETURN_IF_ERROR(sink->init(pipeline->sink()->exec_node(), _runtime_state.get()));
        // TODO pipeline 1 need to add new interface for exec node and operator
        sink->init(request.fragment.output_sink);

        Operators operators;
        RETURN_IF_ERROR(pipeline->build_operators(operators));
        auto task = std::make_unique<PipelineTask>(pipeline, 0, _runtime_state.get(), operators,
                                                   sink, this, pipeline->runtime_profile());
        sink->set_child(task->get_root());
        _tasks.emplace_back(std::move(task));
    }

    for (auto& task : _tasks) {
        RETURN_IF_ERROR(task->prepare(_runtime_state.get()));
    }
    return Status::OK();
}

// TODO: use virtual function to do abstruct
Status PipelineFragmentContext::_build_pipelines(ExecNode* node, PipelinePtr cur_pipe) {
    auto* fragment_context = this;
    auto node_type = node->type();
    switch (node_type) {
    // for source
    case TPlanNodeType::OLAP_SCAN_NODE: {
        auto* new_olap_scan_node = assert_cast<vectorized::NewOlapScanNode*>(node);
        OperatorBuilderPtr operator_t = std::make_shared<OlapScanOperatorBuilder>(
                fragment_context->next_operator_builder_id(), "OlapScanOperator",
                new_olap_scan_node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::EXCHANGE_NODE: {
        OperatorBuilderPtr operator_t = std::make_shared<ExchangeSourceOperatorBuilder>(
                next_operator_builder_id(), "ExchangeSourceOperator", node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::EMPTY_SET_NODE: {
        auto* empty_set_node = assert_cast<vectorized::VEmptySetNode*>(node);
        OperatorBuilderPtr operator_t = std::make_shared<EmptySetSourceOperatorBuilder>(
                next_operator_builder_id(), "EmptySetSourceOperator", empty_set_node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::AGGREGATION_NODE: {
        auto* agg_node = assert_cast<vectorized::AggregationNode*>(node);
        auto new_pipe = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipe));
        if (agg_node->is_streaming_preagg()) {
            auto agg_ctx = std::make_shared<AggContext>();
            OperatorBuilderPtr pre_agg_sink = std::make_shared<StreamingAggSinkOperatorBuilder>(
                    next_operator_builder_id(), "StreamingAggSinkOperator", agg_node, agg_ctx);
            RETURN_IF_ERROR(new_pipe->set_sink(pre_agg_sink));

            OperatorBuilderPtr pre_agg_source = std::make_shared<StreamingAggSourceOperatorBuilder>(
                    next_operator_builder_id(), "StreamingAggSourceOperator", agg_node, agg_ctx);
            RETURN_IF_ERROR(cur_pipe->add_operator(pre_agg_source));
        } else {
            OperatorBuilderPtr agg_sink = std::make_shared<AggSinkOperatorBuilder>(
                    next_operator_builder_id(), "AggSinkOperator", agg_node);
            RETURN_IF_ERROR(new_pipe->set_sink(agg_sink));

            OperatorBuilderPtr agg_source = std::make_shared<AggregationSourceOperatorBuilder>(
                    next_operator_builder_id(), "AggSourceOperator", agg_node);
            RETURN_IF_ERROR(cur_pipe->add_operator(agg_source));
        }
        break;
    }
    case TPlanNodeType::SORT_NODE: {
        auto* sort_node = assert_cast<vectorized::VSortNode*>(node);
        auto new_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipeline));

        OperatorBuilderPtr sort_sink = std::make_shared<SortSinkOperatorBuilder>(
                next_operator_builder_id(), "SortSinkOperator", sort_node);
        RETURN_IF_ERROR(new_pipeline->set_sink(sort_sink));

        OperatorBuilderPtr sort_source = std::make_shared<SortSourceOperatorBuilder>(
                next_operator_builder_id(), "SortSourceOperator", sort_node);
        RETURN_IF_ERROR(cur_pipe->add_operator(sort_source));
        break;
    }
    case TPlanNodeType::REPEAT_NODE: {
        auto* repeat_node = assert_cast<vectorized::VRepeatNode*>(node);
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder =
                std::make_shared<RepeatOperatorBuilder>(next_operator_builder_id(), repeat_node);
        RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        break;
    }
    case TPlanNodeType::TABLE_FUNCTION_NODE: {
        auto* repeat_node = assert_cast<vectorized::VTableFunctionNode*>(node);
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder = std::make_shared<TableFunctionOperatorBuilder>(
                next_operator_builder_id(), repeat_node);
        RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        break;
    }
    default:
        return Status::InternalError("Unsupported exec type in pipeline: {}",
                                     print_plan_node_type(node_type));
    }
    return Status::OK();
}

Status PipelineFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submitted");
    }

    for (auto& task : _tasks) {
        RETURN_IF_ERROR(_exec_env->pipeline_task_scheduler()->schedule_task(task.get()));
    }
    _submitted = true;
    return Status::OK();
}

// construct sink operator
Status PipelineFragmentContext::_create_sink(const TDataSink& thrift_sink) {
    OperatorBuilderPtr sink_;
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        auto* exchange_sink = assert_cast<doris::vectorized::VDataStreamSender*>(_sink.get());
        sink_ = std::make_shared<ExchangeSinkOperatorBuilder>(
                next_operator_builder_id(), "ExchangeSinkOperator", nullptr, exchange_sink, this);
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        auto* result_sink = assert_cast<doris::vectorized::VResultSink*>(_sink.get());
        sink_ = std::make_shared<ResultSinkOperatorBuilder>(
                next_operator_builder_id(), "ResultSinkOperator", nullptr, result_sink);
        break;
    }
    default:
        return Status::InternalError("Unsuported sink type in pipeline: {}", thrift_sink.type);
    }
    return _root_pipeline->set_sink(sink_);
}

void PipelineFragmentContext::close_a_pipeline() {
    ++_closed_pipeline_cnt;
    if (_closed_pipeline_cnt == _pipelines.size()) {
        //        _runtime_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
        send_report(true);
        _exec_env->fragment_mgr()->remove_pipeline_context(shared_from_this());
    }
}

// TODO pipeline dump copy from FragmentExecState::to_http_path
std::string PipelineFragmentContext::to_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_download_load?"
        << "token=" << _exec_env->token() << "&file=" << file_name;
    return url.str();
}

// TODO pipeline dump copy from FragmentExecState::coordinator_callback
// TODO pipeline this callback should be placed in a thread pool
void PipelineFragmentContext::send_report(bool done) {
    DCHECK(_closed_pipeline_cnt == _pipelines.size());

    Status exec_status = Status::OK();
    {
        std::lock_guard<std::mutex> l(_status_lock);
        if (!_exec_status.ok()) {
            exec_status = _exec_status;
        }
    }

    // If plan is done successfully, but _is_report_success is false,
    // no need to send report.
    if (!_is_report_success && done && exec_status.ok()) {
        return;
    }

    Status coord_status;
    auto coord_addr = _query_ctx->coord_addr;
    FrontendServiceConnection coord(_exec_env->frontend_client_cache(), coord_addr, &coord_status);
    if (!coord_status.ok()) {
        std::stringstream ss;
        ss << "couldn't get a client for " << coord_addr << ", reason: " << coord_status;
        LOG(WARNING) << "query_id: " << print_id(_query_id) << ", " << ss.str();
        {
            std::lock_guard<std::mutex> l(_status_lock);
            if (_exec_status.ok()) {
                _exec_status = Status::InternalError(ss.str());
            }
        }
        return;
    }
    auto* profile = _is_report_success ? _runtime_state->runtime_profile() : nullptr;

    TReportExecStatusParams params;
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(_query_id);
    params.__set_backend_num(_backend_num);
    params.__set_fragment_instance_id(_fragment_instance_id);
    exec_status.set_t_status(&params);
    params.__set_done(true);

    auto* runtime_state = _runtime_state.get();
    DCHECK(runtime_state != nullptr);
    if (runtime_state->query_type() == TQueryType::LOAD && !done && exec_status.ok()) {
        // this is a load plan, and load is not finished, just make a brief report
        params.__set_loaded_rows(runtime_state->num_rows_load_total());
        params.__set_loaded_bytes(runtime_state->num_bytes_load_total());
    } else {
        if (runtime_state->query_type() == TQueryType::LOAD) {
            params.__set_loaded_rows(runtime_state->num_rows_load_total());
            params.__set_loaded_bytes(runtime_state->num_bytes_load_total());
        }
        if (profile == nullptr) {
            params.__isset.profile = false;
        } else {
            profile->to_thrift(&params.profile);
            params.__isset.profile = true;
        }

        if (!runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(it));
            }
        }
        if (runtime_state->num_rows_load_total() > 0 ||
            runtime_state->num_rows_load_filtered() > 0) {
            params.__isset.load_counters = true;

            static std::string s_dpp_normal_all = "dpp.norm.ALL";
            static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
            static std::string s_unselected_rows = "unselected.rows";

            params.load_counters.emplace(s_dpp_normal_all,
                                         std::to_string(runtime_state->num_rows_load_success()));
            params.load_counters.emplace(s_dpp_abnormal_all,
                                         std::to_string(runtime_state->num_rows_load_filtered()));
            params.load_counters.emplace(s_unselected_rows,
                                         std::to_string(runtime_state->num_rows_load_unselected()));
        }
        if (!runtime_state->get_error_log_file_path().empty()) {
            params.__set_tracking_url(
                    to_load_error_http_path(runtime_state->get_error_log_file_path()));
        }
        if (!runtime_state->export_output_files().empty()) {
            params.__isset.export_files = true;
            params.export_files = runtime_state->export_output_files();
        }
        if (!runtime_state->tablet_commit_infos().empty()) {
            params.__isset.commitInfos = true;
            params.commitInfos.reserve(runtime_state->tablet_commit_infos().size());
            for (auto& info : runtime_state->tablet_commit_infos()) {
                params.commitInfos.push_back(info);
            }
        }
        if (!runtime_state->error_tablet_infos().empty()) {
            params.__isset.errorTabletInfos = true;
            params.errorTabletInfos.reserve(runtime_state->error_tablet_infos().size());
            for (auto& info : runtime_state->error_tablet_infos()) {
                params.errorTabletInfos.push_back(info);
            }
        }

        // Send new errors to coordinator
        runtime_state->get_unreported_errors(&(params.error_log));
        params.__isset.error_log = (params.error_log.size() > 0);
    }

    if (_exec_env->master_info()->__isset.backend_id) {
        params.__set_backend_id(_exec_env->master_info()->backend_id);
    }

    TReportExecStatusResult res;
    Status rpc_status;

    VLOG_DEBUG << "reportExecStatus params is "
               << apache::thrift::ThriftDebugString(params).c_str();
    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            LOG(WARNING) << "Retrying ReportExecStatus. query id: " << print_id(_query_id)
                         << ", instance id: " << print_id(_fragment_instance_id) << " to "
                         << coord_addr << ", err: " << e.what();
            rpc_status = coord.reopen();

            if (!rpc_status.ok()) {
                // we need to cancel the execution of this fragment
                cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "report rpc fail");
                return;
            }
            coord->reportExecStatus(res, params);
        }

        rpc_status = Status(res.status);
    } catch (TException& e) {
        std::stringstream msg;
        msg << "ReportExecStatus() to " << coord_addr << " failed:\n" << e.what();
        LOG(WARNING) << msg.str();
        rpc_status = Status::InternalError(msg.str());
    }

    if (!rpc_status.ok()) {
        // we need to cancel the execution of this fragment
        cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "rpc fail 2");
    }
}

} // namespace doris::pipeline