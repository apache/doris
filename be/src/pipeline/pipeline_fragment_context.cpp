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

#include <gen_cpp/DataSinks_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "exec/data_sink.h"
#include "exec/scan_node.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/analytic_sink_operator.h"
#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/const_value_operator.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/datagen_operator.h"
#include "pipeline/exec/empty_set_operator.h"
#include "pipeline/exec/empty_source_operator.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/mysql_scan_operator.h"
#include "pipeline/exec/nested_loop_join_build_operator.h"
#include "pipeline/exec/nested_loop_join_probe_operator.h"
#include "pipeline/exec/olap_table_sink_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/repeat_operator.h"
#include "pipeline/exec/result_file_sink_operator.h"
#include "pipeline/exec/result_sink_operator.h"
#include "pipeline/exec/scan_operator.h"
#include "pipeline/exec/schema_scan_operator.h"
#include "pipeline/exec/select_operator.h"
#include "pipeline/exec/set_probe_sink_operator.h"
#include "pipeline/exec/set_sink_operator.h"
#include "pipeline/exec/set_source_operator.h"
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "pipeline/exec/streaming_aggregation_sink_operator.h"
#include "pipeline/exec/streaming_aggregation_source_operator.h"
#include "pipeline/exec/table_function_operator.h"
#include "pipeline/exec/table_sink_operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/exec/union_source_operator.h"
#include "pipeline_task.h"
#include "runtime/client_cache.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "task_scheduler.h"
#include "util/container_util.hpp"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exec/join/vnested_loop_join_node.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vexchange_node.h"
#include "vec/exec/vrepeat_node.h"
#include "vec/exec/vschema_scan_node.h"
#include "vec/exec/vset_operation_node.h"
#include "vec/exec/vsort_node.h"
#include "vec/exec/vunion_node.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/sink/vresult_file_sink.h"
#include "vec/sink/vresult_sink.h"

using apache::thrift::transport::TTransportException;
using apache::thrift::TException;

namespace doris::pipeline {

PipelineFragmentContext::PipelineFragmentContext(
        const TUniqueId& query_id, const TUniqueId& instance_id, const int fragment_id,
        int backend_num, std::shared_ptr<QueryFragmentsCtx> query_ctx, ExecEnv* exec_env,
        const std::function<void(RuntimeState*, Status*)>& call_back,
        const report_status_callback& report_status_cb)
        : _query_id(query_id),
          _fragment_instance_id(instance_id),
          _fragment_id(fragment_id),
          _backend_num(backend_num),
          _exec_env(exec_env),
          _cancel_reason(PPlanFragmentCancelReason::INTERNAL_ERROR),
          _query_ctx(std::move(query_ctx)),
          _call_back(call_back),
          _report_thread_active(false),
          _report_status_cb(report_status_cb),
          _is_report_on_cancel(true) {
    _report_thread_future = _report_thread_promise.get_future();
    _fragment_watcher.start();
}

PipelineFragmentContext::~PipelineFragmentContext() {
    _call_back(_runtime_state.get(), &_exec_status);
    DCHECK(!_report_thread_active);
}

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
        // Get pipe from new load stream manager and send cancel to it or the fragment may hang to wait read from pipe
        // For stream load the fragment's query_id == load id, it is set in FE.
        auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(_query_id);
        if (stream_load_ctx != nullptr) {
            stream_load_ctx->pipe->cancel(PPlanFragmentCancelReason_Name(reason));
        }
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
    auto pipeline = std::make_shared<Pipeline>(id, weak_from_this());
    _pipelines.emplace_back(pipeline);
    return pipeline;
}

Status PipelineFragmentContext::prepare(const doris::TExecPlanFragmentParams& request) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    _runtime_profile.reset(new RuntimeProfile("PipelineContext"));
    _start_timer = ADD_TIMER(_runtime_profile, "StartTime");
    COUNTER_UPDATE(_start_timer, _fragment_watcher.elapsed_time());
    _prepare_timer = ADD_TIMER(_runtime_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);

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

    // 1. init _runtime_state
    _runtime_state = std::make_unique<RuntimeState>(params, request.query_options,
                                                    _query_ctx->query_globals, _exec_env);
    _runtime_state->set_query_fragments_ctx(_query_ctx.get());
    _runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker);
    _runtime_state->set_tracer(std::move(tracer));

    // TODO should be combine with plan_fragment_executor.prepare funciton
    SCOPED_ATTACH_TASK(get_runtime_state());
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

    if (request.query_options.__isset.is_report_success) {
        fragment_context->set_is_report_success(request.query_options.is_report_success);
    }

    auto* desc_tbl = _query_ctx->desc_tbl;
    _runtime_state->set_desc_tbl(desc_tbl);

    // 2. Create ExecNode to build pipeline with PipelineFragmentContext
    RETURN_IF_ERROR(ExecNode::create_tree(_runtime_state.get(), _runtime_state->obj_pool(),
                                          request.fragment.plan, *desc_tbl, &_root_plan));

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
    if (_sink) {
        _runtime_state->runtime_profile()->add_child(_sink->profile(), true, nullptr);
    }
    _runtime_state->runtime_profile()->add_child(_root_plan->runtime_profile(), true, nullptr);
    _runtime_state->runtime_profile()->add_child(_runtime_profile.get(), true, nullptr);

    if (_is_report_success && config::status_report_interval > 0) {
        std::unique_lock<std::mutex> l(_report_thread_lock);
        _exec_env->send_report_thread_pool()->submit_func([this] {
            Defer defer {[&]() { this->_report_thread_promise.set_value(true); }};
            this->report_profile();
        });
        // make sure the thread started up, otherwise report_profile() might get into a race
        // with stop_report_thread()
        _report_thread_started_cv.wait(l);
    }
    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::prepare(const doris::TPipelineFragmentParams& request,
                                        const size_t idx) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    const auto& local_params = request.local_params[idx];
    _runtime_profile.reset(new RuntimeProfile("PipelineContext"));
    _start_timer = ADD_TIMER(_runtime_profile, "StartTime");
    COUNTER_UPDATE(_start_timer, _fragment_watcher.elapsed_time());
    _prepare_timer = ADD_TIMER(_runtime_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);

    auto* fragment_context = this;
    OpentelemetryTracer tracer = telemetry::get_noop_tracer();
    if (opentelemetry::trace::Tracer::GetCurrentSpan()->GetContext().IsValid()) {
        tracer = telemetry::get_tracer(print_id(_query_id));
    }
    START_AND_SCOPE_SPAN(tracer, span, "PipelineFragmentExecutor::prepare");

    LOG_INFO("PipelineFragmentContext::prepare")
            .tag("query_id", _query_id)
            .tag("instance_id", local_params.fragment_instance_id)
            .tag("backend_num", local_params.backend_num)
            .tag("pthread_id", (uintptr_t)pthread_self());

    // 1. init _runtime_state
    _runtime_state =
            std::make_unique<RuntimeState>(local_params, request.query_id, request.query_options,
                                           _query_ctx->query_globals, _exec_env);
    _runtime_state->set_query_fragments_ctx(_query_ctx.get());
    _runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker);
    _runtime_state->set_tracer(std::move(tracer));

    // TODO should be combine with plan_fragment_executor.prepare funciton
    SCOPED_ATTACH_TASK(get_runtime_state());
    _runtime_state->runtime_filter_mgr()->init();
    _runtime_state->set_be_number(local_params.backend_num);

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
    if (request.__isset.shared_scan_opt) {
        _runtime_state->set_shared_scan_opt(request.shared_scan_opt);
    }

    if (request.query_options.__isset.is_report_success) {
        fragment_context->set_is_report_success(request.query_options.is_report_success);
    }

    auto* desc_tbl = _query_ctx->desc_tbl;
    _runtime_state->set_desc_tbl(desc_tbl);

    // 2. Create ExecNode to build pipeline with PipelineFragmentContext
    RETURN_IF_ERROR(ExecNode::create_tree(_runtime_state.get(), _runtime_state->obj_pool(),
                                          request.fragment.plan, *desc_tbl, &_root_plan));

    // Set senders of exchange nodes before pipeline build
    std::vector<ExecNode*> exch_nodes;
    _root_plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    for (ExecNode* exch_node : exch_nodes) {
        DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
        int num_senders = find_with_default(request.per_exch_num_senders, exch_node->id(), 0);
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
    VLOG_CRITICAL << "params.per_node_scan_ranges.size()="
                  << local_params.per_node_scan_ranges.size();

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
            const std::vector<TScanRangeParams>& scan_ranges = find_with_default(
                    local_params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
            scan_node->set_scan_ranges(scan_ranges);
        } else {
            ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[i]);
            const std::vector<TScanRangeParams>& scan_ranges = find_with_default(
                    local_params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
            scan_node->set_scan_ranges(scan_ranges);
            VLOG_CRITICAL << "scan_node_Id=" << scan_node->id() << " size=" << scan_ranges.size();
        }
    }

    _runtime_state->set_per_fragment_instance_idx(local_params.sender_id);
    _runtime_state->set_num_per_fragment_instances(request.num_senders);

    if (request.fragment.__isset.output_sink) {
        RETURN_IF_ERROR(DataSink::create_data_sink(
                _runtime_state->obj_pool(), request.fragment.output_sink,
                request.fragment.output_exprs, request, idx, _root_plan->row_desc(),
                _runtime_state.get(), &_sink, *desc_tbl));
    }

    _root_pipeline = fragment_context->add_pipeline();
    RETURN_IF_ERROR(_build_pipelines(_root_plan, _root_pipeline));
    if (_sink) {
        RETURN_IF_ERROR(_create_sink(request.fragment.output_sink));
    }
    RETURN_IF_ERROR(_build_pipeline_tasks(request));
    if (_sink) {
        _runtime_state->runtime_profile()->add_child(_sink->profile(), true, nullptr);
    }
    _runtime_state->runtime_profile()->add_child(_root_plan->runtime_profile(), true, nullptr);
    _runtime_state->runtime_profile()->add_child(_runtime_profile.get(), true, nullptr);

    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(
        const doris::TExecPlanFragmentParams& request) {
    for (PipelinePtr& pipeline : _pipelines) {
        // if sink
        auto sink = pipeline->sink()->build_operator();
        // TODO pipeline 1 need to add new interface for exec node and operator
        sink->init(request.fragment.output_sink);

        Operators operators;
        RETURN_IF_ERROR(pipeline->build_operators(operators));
        auto task = std::make_unique<PipelineTask>(pipeline, 0, _runtime_state.get(), operators,
                                                   sink, this, pipeline->pipeline_profile());
        sink->set_child(task->get_root());
        _tasks.emplace_back(std::move(task));
        _runtime_profile->add_child(pipeline->pipeline_profile(), true, nullptr);
    }

    for (auto& task : _tasks) {
        RETURN_IF_ERROR(task->prepare(_runtime_state.get()));
    }
    _total_tasks = _tasks.size();
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(
        const doris::TPipelineFragmentParams& request) {
    for (PipelinePtr& pipeline : _pipelines) {
        // if sink
        auto sink = pipeline->sink()->build_operator();
        // TODO pipeline 1 need to add new interface for exec node and operator
        sink->init(request.fragment.output_sink);

        Operators operators;
        RETURN_IF_ERROR(pipeline->build_operators(operators));
        auto task = std::make_unique<PipelineTask>(pipeline, 0, _runtime_state.get(), operators,
                                                   sink, this, pipeline->pipeline_profile());
        sink->set_child(task->get_root());
        _tasks.emplace_back(std::move(task));
        _runtime_profile->add_child(pipeline->pipeline_profile(), true, nullptr);
    }

    for (auto& task : _tasks) {
        RETURN_IF_ERROR(task->prepare(_runtime_state.get()));
    }
    _total_tasks = _tasks.size();
    return Status::OK();
}

void PipelineFragmentContext::_stop_report_thread() {
    if (!_report_thread_active) {
        return;
    }

    _report_thread_active = false;

    _stop_report_thread_cv.notify_one();
    // Wait infinitly to ensure that the report task is finished and the this variable
    // is not used in report thread.
    _report_thread_future.wait();
}

void PipelineFragmentContext::report_profile() {
    SCOPED_ATTACH_TASK(_runtime_state.get());
    VLOG_FILE << "report_profile(): instance_id=" << _runtime_state->fragment_instance_id();

    _report_thread_active = true;

    std::unique_lock<std::mutex> l(_report_thread_lock);
    // tell Open() that we started
    _report_thread_started_cv.notify_one();

    // Jitter the reporting time of remote fragments by a random amount between
    // 0 and the report_interval.  This way, the coordinator doesn't get all the
    // updates at once so its better for contention as well as smoother progress
    // reporting.
    int report_fragment_offset = rand() % config::status_report_interval;
    // We don't want to wait longer than it takes to run the entire fragment.
    _stop_report_thread_cv.wait_for(l, std::chrono::seconds(report_fragment_offset));
    while (_report_thread_active) {
        if (config::status_report_interval > 0) {
            // wait_for can return because the timeout occurred or the condition variable
            // was signaled.  We can't rely on its return value to distinguish between the
            // two cases (e.g. there is a race here where the wait timed out but before grabbing
            // the lock, the condition variable was signaled).  Instead, we will use an external
            // flag, _report_thread_active, to coordinate this.
            _stop_report_thread_cv.wait_for(l,
                                            std::chrono::seconds(config::status_report_interval));
        } else {
            LOG(WARNING) << "config::status_report_interval is equal to or less than zero, exiting "
                            "reporting thread.";
            break;
        }

        if (VLOG_FILE_IS_ON) {
            VLOG_FILE << "Reporting " << (!_report_thread_active ? "final " : " ")
                      << "profile for instance " << _runtime_state->fragment_instance_id();
            std::stringstream ss;
            _runtime_state->runtime_profile()->compute_time_in_profile();
            _runtime_state->runtime_profile()->pretty_print(&ss);
            VLOG_FILE << ss.str();
        }

        if (!_report_thread_active) {
            break;
        }

        send_report(false);
    }

    VLOG_FILE << "exiting reporting thread: instance_id=" << _runtime_state->fragment_instance_id();
}

// TODO: use virtual function to do abstruct
Status PipelineFragmentContext::_build_pipelines(ExecNode* node, PipelinePtr cur_pipe) {
    auto node_type = node->type();
    switch (node_type) {
    // for source
    case TPlanNodeType::OLAP_SCAN_NODE:
    case TPlanNodeType::JDBC_SCAN_NODE:
    case TPlanNodeType::ODBC_SCAN_NODE:
    case TPlanNodeType::FILE_SCAN_NODE:
    case TPlanNodeType::ES_SCAN_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<ScanOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::MYSQL_SCAN_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<MysqlScanOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::SCHEMA_SCAN_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<SchemaScanOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::EXCHANGE_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<ExchangeSourceOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::EMPTY_SET_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<EmptySetSourceOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::DATA_GEN_SCAN_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<DataGenOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::UNION_NODE: {
        auto* union_node = assert_cast<vectorized::VUnionNode*>(node);
        if (union_node->children_count() == 0 &&
            union_node->get_first_materialized_child_idx() == 0) { // only have const expr
            OperatorBuilderPtr builder =
                    std::make_shared<ConstValueOperatorBuilder>(next_operator_builder_id(), node);
            RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        } else {
            int child_count = union_node->children_count();
            auto data_queue = std::make_shared<DataQueue>(child_count);
            for (int child_id = 0; child_id < child_count; ++child_id) {
                auto new_child_pipeline = add_pipeline();
                RETURN_IF_ERROR(_build_pipelines(union_node->child(child_id), new_child_pipeline));
                OperatorBuilderPtr child_sink_builder = std::make_shared<UnionSinkOperatorBuilder>(
                        next_operator_builder_id(), child_id, union_node, data_queue);
                RETURN_IF_ERROR(new_child_pipeline->set_sink(child_sink_builder));
            }
            OperatorBuilderPtr source_builder = std::make_shared<UnionSourceOperatorBuilder>(
                    next_operator_builder_id(), union_node, data_queue);
            RETURN_IF_ERROR(cur_pipe->add_operator(source_builder));
        }
        break;
    }
    case TPlanNodeType::AGGREGATION_NODE: {
        auto* agg_node = assert_cast<vectorized::AggregationNode*>(node);
        auto new_pipe = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipe));
        if (agg_node->is_streaming_preagg()) {
            auto data_queue = std::make_shared<DataQueue>(1);
            OperatorBuilderPtr pre_agg_sink = std::make_shared<StreamingAggSinkOperatorBuilder>(
                    next_operator_builder_id(), agg_node, data_queue);
            RETURN_IF_ERROR(new_pipe->set_sink(pre_agg_sink));

            OperatorBuilderPtr pre_agg_source = std::make_shared<StreamingAggSourceOperatorBuilder>(
                    next_operator_builder_id(), agg_node, data_queue);
            RETURN_IF_ERROR(cur_pipe->add_operator(pre_agg_source));
        } else {
            OperatorBuilderPtr agg_sink =
                    std::make_shared<AggSinkOperatorBuilder>(next_operator_builder_id(), agg_node);
            RETURN_IF_ERROR(new_pipe->set_sink(agg_sink));

            OperatorBuilderPtr agg_source = std::make_shared<AggSourceOperatorBuilder>(
                    next_operator_builder_id(), agg_node);
            RETURN_IF_ERROR(cur_pipe->add_operator(agg_source));
        }
        break;
    }
    case TPlanNodeType::SORT_NODE: {
        auto new_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipeline));

        OperatorBuilderPtr sort_sink =
                std::make_shared<SortSinkOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(new_pipeline->set_sink(sort_sink));

        OperatorBuilderPtr sort_source =
                std::make_shared<SortSourceOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(sort_source));
        break;
    }
    case TPlanNodeType::ANALYTIC_EVAL_NODE: {
        auto new_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipeline));

        OperatorBuilderPtr analytic_sink =
                std::make_shared<AnalyticSinkOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(new_pipeline->set_sink(analytic_sink));

        OperatorBuilderPtr analytic_source =
                std::make_shared<AnalyticSourceOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(analytic_source));
        break;
    }
    case TPlanNodeType::REPEAT_NODE: {
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder =
                std::make_shared<RepeatOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        break;
    }
    case TPlanNodeType::ASSERT_NUM_ROWS_NODE: {
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder =
                std::make_shared<AssertNumRowsOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        break;
    }
    case TPlanNodeType::TABLE_FUNCTION_NODE: {
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder =
                std::make_shared<TableFunctionOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        break;
    }
    case TPlanNodeType::HASH_JOIN_NODE: {
        auto* join_node = assert_cast<vectorized::HashJoinNode*>(node);
        auto new_pipe = add_pipeline();
        if (join_node->should_build_hash_table()) {
            RETURN_IF_ERROR(_build_pipelines(node->child(1), new_pipe));
        } else {
            OperatorBuilderPtr builder = std::make_shared<EmptySourceOperatorBuilder>(
                    next_operator_builder_id(), node->child(1)->row_desc());
            new_pipe->add_operator(builder);
        }
        OperatorBuilderPtr join_sink =
                std::make_shared<HashJoinBuildSinkBuilder>(next_operator_builder_id(), join_node);
        RETURN_IF_ERROR(new_pipe->set_sink(join_sink));
        new_pipe->disable_task_steal();

        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr join_source = std::make_shared<HashJoinProbeOperatorBuilder>(
                next_operator_builder_id(), join_node);
        RETURN_IF_ERROR(cur_pipe->add_operator(join_source));

        cur_pipe->add_dependency(new_pipe);
        break;
    }
    case TPlanNodeType::CROSS_JOIN_NODE: {
        auto new_pipe = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(1), new_pipe));
        OperatorBuilderPtr join_sink = std::make_shared<NestLoopJoinBuildOperatorBuilder>(
                next_operator_builder_id(), node);
        RETURN_IF_ERROR(new_pipe->set_sink(join_sink));

        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr join_source = std::make_shared<NestLoopJoinProbeOperatorBuilder>(
                next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(join_source));

        cur_pipe->add_dependency(new_pipe);
        break;
    }
    case TPlanNodeType::INTERSECT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<true>(node, cur_pipe));
        break;
    }
    case TPlanNodeType::EXCEPT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<false>(node, cur_pipe));
        break;
    }
    case TPlanNodeType::SELECT_NODE: {
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder =
                std::make_shared<SelectOperatorBuilder>(next_operator_builder_id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        break;
    }
    default:
        return Status::InternalError("Unsupported exec type in pipeline: {}",
                                     print_plan_node_type(node_type));
    }
    return Status::OK();
}

template <bool is_intersect>
Status PipelineFragmentContext::_build_operators_for_set_operation_node(ExecNode* node,
                                                                        PipelinePtr cur_pipe) {
    auto build_pipeline = add_pipeline();
    RETURN_IF_ERROR(_build_pipelines(node->child(0), build_pipeline));
    OperatorBuilderPtr sink_builder = std::make_shared<SetSinkOperatorBuilder<is_intersect>>(
            next_operator_builder_id(), node);
    RETURN_IF_ERROR(build_pipeline->set_sink(sink_builder));

    for (int child_id = 1; child_id < node->children_count(); ++child_id) {
        auto probe_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(child_id), probe_pipeline));
        OperatorBuilderPtr probe_sink_builder =
                std::make_shared<SetProbeSinkOperatorBuilder<is_intersect>>(
                        next_operator_builder_id(), child_id, node);
        RETURN_IF_ERROR(probe_pipeline->set_sink(probe_sink_builder));
    }

    OperatorBuilderPtr source_builder = std::make_shared<SetSourceOperatorBuilder<is_intersect>>(
            next_operator_builder_id(), node);
    return cur_pipe->add_operator(source_builder);
}

Status PipelineFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submitted");
    }
    _submitted = true;

    int submit_tasks = 0;
    Status st;
    for (auto& task : _tasks) {
        st = _exec_env->pipeline_task_scheduler()->schedule_task(task.get());
        if (!st) {
            cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "submit context fail");
            _total_tasks = submit_tasks;
            break;
        }
        submit_tasks++;
    }
    if (!st.ok()) {
        if (_closed_tasks == _total_tasks) {
            std::call_once(_close_once_flag, [this] { _close_action(); });
        }
        return Status::InternalError("Submit pipeline failed. err = {}, BE: {}", st.to_string(),
                                     BackendOptions::get_localhost());
    } else {
        return st;
    }
}

void PipelineFragmentContext::close_if_prepare_failed() {
    if (_tasks.empty()) {
        _root_plan->close(_runtime_state.get());
    }
    for (auto& task : _tasks) {
        DCHECK(!task->is_pending_finish());
        WARN_IF_ERROR(task->close(), "close_if_prepare_failed failed: ");
        close_a_pipeline();
    }
}

// construct sink operator
Status PipelineFragmentContext::_create_sink(const TDataSink& thrift_sink) {
    OperatorBuilderPtr sink_;
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        sink_ = std::make_shared<ExchangeSinkOperatorBuilder>(next_operator_builder_id(),
                                                              _sink.get(), this);
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        sink_ = std::make_shared<ResultSinkOperatorBuilder>(next_operator_builder_id(),
                                                            _sink.get());
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        sink_ = std::make_shared<OlapTableSinkOperatorBuilder>(next_operator_builder_id(),
                                                               _sink.get());
        break;
    }
    case TDataSinkType::MYSQL_TABLE_SINK:
    case TDataSinkType::JDBC_TABLE_SINK:
    case TDataSinkType::ODBC_TABLE_SINK: {
        sink_ = std::make_shared<TableSinkOperatorBuilder>(next_operator_builder_id(), _sink.get());
        break;
    }
    case TDataSinkType::RESULT_FILE_SINK: {
        sink_ = std::make_shared<ResultFileSinkOperatorBuilder>(next_operator_builder_id(),
                                                                _sink.get());
        break;
    }
    default:
        return Status::InternalError("Unsuported sink type in pipeline: {}", thrift_sink.type);
    }
    return _root_pipeline->set_sink(sink_);
}

void PipelineFragmentContext::_close_action() {
    _runtime_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
    send_report(true);
    _stop_report_thread();
    // all submitted tasks done
    _exec_env->fragment_mgr()->remove_pipeline_context(shared_from_this());
}

void PipelineFragmentContext::close_a_pipeline() {
    ++_closed_tasks;
    if (_closed_tasks == _total_tasks) {
        std::call_once(_close_once_flag, [this] { _close_action(); });
    }
}

void PipelineFragmentContext::send_report(bool done) {
    Status exec_status = Status::OK();
    {
        std::lock_guard<std::mutex> l(_status_lock);
        exec_status = _exec_status;
    }

    // If plan is done successfully, but _is_report_success is false,
    // no need to send report.
    if (!_is_report_success && done && exec_status.ok()) {
        return;
    }

    // If both _is_report_success and _is_report_on_cancel are false,
    // which means no matter query is success or failed, no report is needed.
    // This may happen when the query limit reached and
    // a internal cancellation being processed
    if (!_is_report_success && !_is_report_on_cancel) {
        return;
    }

    _report_status_cb(
            {exec_status, _is_report_success ? _runtime_state->runtime_profile() : nullptr,
             done || !exec_status.ok(), _query_ctx->coord_addr, _query_id, _fragment_id,
             _fragment_instance_id, _backend_num, _runtime_state.get(),
             std::bind(&PipelineFragmentContext::update_status, this, std::placeholders::_1),
             std::bind(&PipelineFragmentContext::cancel, this, std::placeholders::_1,
                       std::placeholders::_2)});
}

} // namespace doris::pipeline
