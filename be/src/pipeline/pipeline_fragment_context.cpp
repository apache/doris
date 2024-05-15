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
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Planner_types.h>
#include <pthread.h>

#include <cstdlib>
// IWYU pragma: no_include <bits/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <ostream>
#include <typeinfo>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/exec_node.h"
#include "exec/scan_node.h"
#include "io/fs/stream_load_pipe.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/analytic_sink_operator.h"
#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/const_value_operator.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/datagen_operator.h"
#include "pipeline/exec/distinct_streaming_aggregation_sink_operator.h"
#include "pipeline/exec/distinct_streaming_aggregation_source_operator.h"
#include "pipeline/exec/empty_set_operator.h"
#include "pipeline/exec/empty_source_operator.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/exec/group_commit_block_sink_operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/hive_table_sink_operator.h"
#include "pipeline/exec/iceberg_table_sink_operator.h"
#include "pipeline/exec/multi_cast_data_stream_sink.h"
#include "pipeline/exec/multi_cast_data_stream_source.h"
#include "pipeline/exec/mysql_scan_operator.h" // IWYU pragma: keep
#include "pipeline/exec/nested_loop_join_build_operator.h"
#include "pipeline/exec/nested_loop_join_probe_operator.h"
#include "pipeline/exec/olap_table_sink_operator.h"
#include "pipeline/exec/olap_table_sink_v2_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/partition_sort_sink_operator.h"
#include "pipeline/exec/partition_sort_source_operator.h"
#include "pipeline/exec/repeat_operator.h"
#include "pipeline/exec/result_file_sink_operator.h"
#include "pipeline/exec/result_sink_operator.h"
#include "pipeline/exec/scan_operator.h"
#include "pipeline/exec/schema_scan_operator.h"
#include "pipeline/exec/select_operator.h"
#include "pipeline/exec/set_probe_sink_operator.h" // IWYU pragma: keep
#include "pipeline/exec/set_sink_operator.h"       // IWYU pragma: keep
#include "pipeline/exec/set_source_operator.h"     // IWYU pragma: keep
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "pipeline/exec/streaming_aggregation_sink_operator.h"
#include "pipeline/exec/streaming_aggregation_source_operator.h"
#include "pipeline/exec/table_function_operator.h"
#include "pipeline/exec/table_sink_operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/exec/union_source_operator.h"
#include "pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "task_scheduler.h"
#include "util/container_util.hpp"
#include "util/debug_util.h"
#include "util/uid_util.h"
#include "vec/common/assert_cast.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exec/scan/new_es_scan_node.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/exec/scan/new_jdbc_scan_node.h"
#include "vec/exec/scan/new_odbc_scan_node.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/exec/scan/vmeta_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vexchange_node.h"
#include "vec/exec/vunion_node.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris::pipeline {
bvar::Adder<int64_t> g_pipeline_tasks_count("doris_pipeline_tasks_count");

PipelineFragmentContext::PipelineFragmentContext(
        const TUniqueId& query_id, const TUniqueId& instance_id, int fragment_id, int backend_num,
        std::shared_ptr<QueryContext> query_ctx, ExecEnv* exec_env,
        const std::function<void(RuntimeState*, Status*)>& call_back,
        report_status_callback report_status_cb)
        : _query_id(query_id),
          _fragment_instance_id(instance_id),
          _fragment_id(fragment_id),
          _backend_num(backend_num),
          _exec_env(exec_env),
          _query_ctx(std::move(query_ctx)),
          _call_back(call_back),
          _is_report_on_cancel(true),
          _report_status_cb(std::move(report_status_cb)),
          _create_time(MonotonicNanos()) {
    _fragment_watcher.start();
    _start_time = VecDateTimeValue::local_time();
    _query_thread_context = {query_id, _query_ctx->query_mem_tracker};
}

PipelineFragmentContext::~PipelineFragmentContext() {
    // The memory released by the query end is recorded in the query mem tracker.
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_thread_context.query_mem_tracker);
    auto st = _query_ctx->exec_status();
    _query_ctx.reset();
    _tasks.clear();
    if (_runtime_state != nullptr) {
        _call_back(_runtime_state.get(), &st);
        _runtime_state.reset();
    } else {
        _call_back(_runtime_state.get(), &st);
    }
    _root_pipeline.reset();
    _pipelines.clear();
    _sink.reset();
    _multi_cast_stream_sink_senders.clear();
}

bool PipelineFragmentContext::is_timeout(const VecDateTimeValue& now) const {
    if (_timeout <= 0) {
        return false;
    }
    if (now.second_diff(_start_time) > _timeout) {
        return true;
    }
    return false;
}

// Must not add lock in this method. Because it will call query ctx cancel. And
// QueryCtx cancel will call fragment ctx cancel. And Also Fragment ctx's running
// Method like exchange sink buffer will call query ctx cancel. If we add lock here
// There maybe dead lock.
void PipelineFragmentContext::cancel(const PPlanFragmentCancelReason& reason,
                                     const std::string& msg) {
    LOG_INFO("PipelineFragmentContext::cancel")
            .tag("query_id", print_id(_query_ctx->query_id()))
            .tag("fragment_id", _fragment_id)
            .tag("instance_id", print_id(_runtime_state->fragment_instance_id()))
            .tag("reason", PPlanFragmentCancelReason_Name(reason))
            .tag("message", msg);
    // TODO(zhiqiang): may be not need to check if query is already cancelled.
    // Dont cancel in this situation may lead to bug. For example, result sink node
    // can not be cancelled if other fragments set the query_ctx cancelled, this will
    // make result receiver on fe be stocked on rpc forever until timeout...
    // We need a more detail discussion.
    _query_ctx->cancel(msg, Status::Cancelled(msg));
    if (reason == PPlanFragmentCancelReason::LIMIT_REACH) {
        _is_report_on_cancel = false;
    } else {
        LOG(WARNING) << "PipelineFragmentContext "
                     << PrintInstanceStandardInfo(_query_id, _fragment_instance_id)
                     << " is canceled, cancel message: " << msg;
    }

    _runtime_state->set_process_status(_query_ctx->exec_status());
    // Get pipe from new load stream manager and send cancel to it or the fragment may hang to wait read from pipe
    // For stream load the fragment's query_id == load id, it is set in FE.
    auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(_query_id);
    if (stream_load_ctx != nullptr) {
        stream_load_ctx->pipe->cancel(msg);
    }

    // must close stream_mgr to avoid dead lock in Exchange Node
    // TODO bug llj  fix this other instance will not cancel
    _exec_env->vstream_mgr()->cancel(_fragment_instance_id, Status::Cancelled(msg));
    // Cancel the result queue manager used by spark doris connector
    // TODO pipeline incomp
    // _exec_env->result_queue_mgr()->update_queue_status(id, Status::Aborted(msg));
}

PipelinePtr PipelineFragmentContext::add_pipeline() {
    // _prepared、_submitted, _canceled should do not add pipeline
    PipelineId id = _next_pipeline_id++;
    auto pipeline = std::make_shared<Pipeline>(
            id, _num_instances,
            std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
    _pipelines.emplace_back(pipeline);
    return pipeline;
}

PipelinePtr PipelineFragmentContext::add_pipeline(PipelinePtr parent, int idx) {
    // _prepared、_submitted, _canceled should do not add pipeline
    PipelineId id = _next_pipeline_id++;
    auto pipeline = std::make_shared<Pipeline>(
            id, _num_instances,
            std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
    if (idx >= 0) {
        _pipelines.insert(_pipelines.begin() + idx, pipeline);
    } else {
        _pipelines.emplace_back(pipeline);
    }
    parent->set_children(pipeline);
    return pipeline;
}

Status PipelineFragmentContext::prepare(const doris::TPipelineFragmentParams& request, size_t idx) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    if (request.__isset.query_options && request.query_options.__isset.execution_timeout) {
        _timeout = request.query_options.execution_timeout;
    }
    const auto& local_params = request.local_params[idx];
    _runtime_profile = std::make_unique<RuntimeProfile>("PipelineContext");
    _start_timer = ADD_TIMER(_runtime_profile, "StartTime");
    COUNTER_UPDATE(_start_timer, _fragment_watcher.elapsed_time());
    _prepare_timer = ADD_TIMER(_runtime_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);

    auto* fragment_context = this;

    LOG_INFO("Preparing instance {}|{}, backend_num {}", print_id(_query_id),
             print_id(local_params.fragment_instance_id), local_params.backend_num);

    // 1. init _runtime_state
    _runtime_state = RuntimeState::create_unique(
            local_params.fragment_instance_id, request.query_id, request.fragment_id,
            request.query_options, _query_ctx->query_globals, _exec_env, _query_ctx.get());

    _runtime_state->set_task_execution_context(shared_from_this());

    // TODO should be combine with plan_fragment_executor.prepare funciton
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_runtime_state->query_mem_tracker());
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

    if (request.query_options.__isset.is_report_success) {
        fragment_context->set_is_report_success(request.query_options.is_report_success);
    }

    if (request.is_simplified_param) {
        _desc_tbl = _query_ctx->desc_tbl;
    } else {
        DCHECK(request.__isset.desc_tbl);
        RETURN_IF_ERROR(
                DescriptorTbl::create(_runtime_state->obj_pool(), request.desc_tbl, &_desc_tbl));
    }
    _runtime_state->set_desc_tbl(_desc_tbl);

    // 2. Create ExecNode to build pipeline with PipelineFragmentContext
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(
            ExecNode::create_tree(_runtime_state.get(), _runtime_state->obj_pool(),
                                  request.fragment.plan, *_desc_tbl, &_root_plan));

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
    VLOG_CRITICAL << "query " << print_id(get_query_id())
                  << " scan_nodes.size()=" << scan_nodes.size();
    VLOG_CRITICAL << "query " << print_id(get_query_id()) << " params.per_node_scan_ranges.size()="
                  << local_params.per_node_scan_ranges.size();

    // set scan range in ScanNode
    for (auto& i : scan_nodes) {
        // TODO(cmy): this "if...else" should be removed once all ScanNode are derived from VScanNode.
        ExecNode* node = i;
        if (typeid(*node) == typeid(vectorized::NewOlapScanNode) ||
            typeid(*node) == typeid(vectorized::NewFileScanNode) ||
            typeid(*node) == typeid(vectorized::NewOdbcScanNode) ||
            typeid(*node) == typeid(vectorized::NewEsScanNode) ||
            typeid(*node) == typeid(vectorized::VMetaScanNode) ||
            typeid(*node) == typeid(vectorized::NewJdbcScanNode)) {
            auto* scan_node = static_cast<vectorized::VScanNode*>(i);
            auto scan_ranges = find_with_default(local_params.per_node_scan_ranges, scan_node->id(),
                                                 no_scan_ranges);
            const bool shared_scan =
                    find_with_default(local_params.per_node_shared_scans, scan_node->id(), false);
            scan_node->set_scan_ranges(_runtime_state.get(), scan_ranges);
            scan_node->set_shared_scan(_runtime_state.get(), shared_scan);
        } else {
            auto* scan_node = static_cast<ScanNode*>(node);
            auto scan_ranges = find_with_default(local_params.per_node_scan_ranges, scan_node->id(),
                                                 no_scan_ranges);
            RETURN_IF_ERROR(scan_node->set_scan_ranges(_runtime_state.get(), scan_ranges));
            VLOG_CRITICAL << "query " << print_id(get_query_id())
                          << " scan_node_id=" << scan_node->id()
                          << " size=" << scan_ranges.get().size();
        }
    }

    _runtime_state->set_per_fragment_instance_idx(local_params.sender_id);
    _runtime_state->set_num_per_fragment_instances(request.num_senders);
    _runtime_state->set_load_stream_per_node(request.load_stream_per_node);
    _runtime_state->set_total_load_streams(request.total_load_streams);
    _runtime_state->set_num_local_sink(request.num_local_sink);

    if (request.fragment.__isset.output_sink) {
        // Here we build a DataSink object, which will be hold by DataSinkOperator
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(DataSink::create_data_sink(
                _runtime_state->obj_pool(), request.fragment.output_sink,
                request.fragment.output_exprs, request, idx, _root_plan->row_desc(),
                _runtime_state.get(), &_sink, *_desc_tbl));
    }

    _root_pipeline = fragment_context->add_pipeline();
    _root_pipeline->set_is_root_pipeline();
    RETURN_IF_ERROR(_build_pipelines(_root_plan, _root_pipeline));
    if (_sink) {
        // DataSinkOperator is builded here
        RETURN_IF_ERROR(_create_sink(request.local_params[idx].sender_id,
                                     request.fragment.output_sink, _runtime_state.get()));
    }
    RETURN_IF_ERROR(_build_pipeline_tasks(request));
    if (_sink) {
        _runtime_state->runtime_profile()->add_child(_sink->profile(), true, nullptr);
        _sink->profile()->add_child(_root_plan->runtime_profile(), true, nullptr);
    } else {
        _runtime_state->runtime_profile()->add_child(_root_plan->runtime_profile(), true, nullptr);
    }
    _runtime_state->runtime_profile()->add_child(_runtime_profile.get(), true, nullptr);

    _init_next_report_time();

    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(
        const doris::TPipelineFragmentParams& request) {
    _total_tasks = 0;
    for (PipelinePtr& pipeline : _pipelines) {
        // if sink
        auto sink_operator = pipeline->get_sink_builder()->build_operator();
        // TODO pipeline 1 need to add new interface for exec node and operator
        RETURN_IF_ERROR(sink_operator->init(request.fragment.output_sink));

        RETURN_IF_ERROR(pipeline->build_operators());
        auto task =
                std::make_unique<PipelineTask>(pipeline, _total_tasks++, _runtime_state.get(),
                                               sink_operator, this, pipeline->pipeline_profile());
        RETURN_IF_ERROR(sink_operator->set_child(task->get_root()));
        _tasks.emplace_back(std::move(task));
        _runtime_profile->add_child(pipeline->pipeline_profile(), true, nullptr);
    }
    g_pipeline_tasks_count << _total_tasks;
    for (auto& task : _tasks) {
        RETURN_IF_ERROR(task->prepare(_runtime_state.get()));
    }

    // register the profile of child data stream sender
    for (auto& sender : _multi_cast_stream_sink_senders) {
        _sink->profile()->add_child(sender->profile(), true, nullptr);
    }

    return Status::OK();
}

void PipelineFragmentContext::_init_next_report_time() {
    auto interval_s = config::pipeline_status_report_interval;
    if (_is_report_success && interval_s > 0 && _query_ctx->timeout_second > interval_s) {
        std::vector<string> ins_ids;
        instance_ids(ins_ids);
        VLOG_FILE << "enable period report: instance_id="
                  << fmt::format("{}", fmt::join(ins_ids, ", "));
        uint64_t report_fragment_offset = (uint64_t)(rand() % interval_s) * NANOS_PER_SEC;
        // We don't want to wait longer than it takes to run the entire fragment.
        _previous_report_time =
                MonotonicNanos() + report_fragment_offset - (uint64_t)(interval_s)*NANOS_PER_SEC;
        _disable_period_report = false;
    }
}

void PipelineFragmentContext::refresh_next_report_time() {
    auto disable = _disable_period_report.load(std::memory_order_acquire);
    DCHECK(disable == true);
    _previous_report_time.store(MonotonicNanos(), std::memory_order_release);
    _disable_period_report.compare_exchange_strong(disable, false);
}

void PipelineFragmentContext::trigger_report_if_necessary() {
    if (!_is_report_success) {
        return;
    }
    auto disable = _disable_period_report.load(std::memory_order_acquire);
    if (disable) {
        return;
    }
    int32_t interval_s = config::pipeline_status_report_interval;
    if (interval_s <= 0) {
        LOG(WARNING)
                << "config::status_report_interval is equal to or less than zero, do not trigger "
                   "report.";
    }
    uint64_t next_report_time = _previous_report_time.load(std::memory_order_acquire) +
                                (uint64_t)(interval_s)*NANOS_PER_SEC;
    if (MonotonicNanos() > next_report_time) {
        if (!_disable_period_report.compare_exchange_strong(disable, true,
                                                            std::memory_order_acq_rel)) {
            return;
        }
        if (VLOG_FILE_IS_ON) {
            std::vector<string> ins_ids;
            instance_ids(ins_ids);
            VLOG_FILE << "Reporting "
                      << "profile for query_id " << print_id(_query_id)
                      << ", instance ids: " << fmt::format("{}", fmt::join(ins_ids, ", "));

            std::stringstream ss;
            _runtime_state->runtime_profile()->compute_time_in_profile();
            _runtime_state->runtime_profile()->pretty_print(&ss);
            if (_runtime_state->load_channel_profile()) {
                // _runtime_state->load_channel_profile()->compute_time_in_profile(); // TODO load channel profile add timer
                _runtime_state->load_channel_profile()->pretty_print(&ss);
            }

            VLOG_FILE << "Query " << print_id(this->get_query_id()) << " fragment "
                      << this->get_fragment_id() << " instance "
                      << print_id(this->get_fragment_instance_id()) << " profile:\n"
                      << ss.str();
        }
        auto st = send_report(false);
        if (!st.ok()) {
            disable = true;
            _disable_period_report.compare_exchange_strong(disable, false,
                                                           std::memory_order_acq_rel);
        }
    }
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
    case TPlanNodeType::META_SCAN_NODE:
    case TPlanNodeType::GROUP_COMMIT_SCAN_NODE:
    case TPlanNodeType::ES_HTTP_SCAN_NODE:
    case TPlanNodeType::ES_SCAN_NODE: {
        OperatorBuilderPtr operator_t = std::make_shared<ScanOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::MYSQL_SCAN_NODE: {
#ifdef DORIS_WITH_MYSQL
        OperatorBuilderPtr operator_t =
                std::make_shared<MysqlScanOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
#else
        return Status::InternalError(
                "Don't support MySQL table, you should rebuild Doris with WITH_MYSQL option ON");
#endif
    }
    case TPlanNodeType::SCHEMA_SCAN_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<SchemaScanOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::EXCHANGE_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<ExchangeSourceOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::EMPTY_SET_NODE: {
        OperatorBuilderPtr operator_t =
                std::make_shared<EmptySetSourceOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::DATA_GEN_SCAN_NODE: {
        OperatorBuilderPtr operator_t = std::make_shared<DataGenOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(operator_t));
        break;
    }
    case TPlanNodeType::UNION_NODE: {
        auto* union_node = assert_cast<vectorized::VUnionNode*>(node);
        if (union_node->children_count() == 0 &&
            union_node->get_first_materialized_child_idx() == 0) { // only have const expr
            OperatorBuilderPtr builder =
                    std::make_shared<ConstValueOperatorBuilder>(node->id(), node);
            RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        } else {
            int child_count = union_node->children_count();
            auto data_queue = std::make_shared<DataQueue>(child_count);
            data_queue->set_max_blocks_in_sub_queue(_runtime_state->data_queue_max_blocks());
            for (int child_id = 0; child_id < child_count; ++child_id) {
                auto new_child_pipeline = add_pipeline();
                RETURN_IF_ERROR(_build_pipelines(union_node->child(child_id), new_child_pipeline));
                OperatorBuilderPtr child_sink_builder = std::make_shared<UnionSinkOperatorBuilder>(
                        union_node->id(), child_id, union_node, data_queue);
                RETURN_IF_ERROR(new_child_pipeline->set_sink_builder(child_sink_builder));
            }
            OperatorBuilderPtr source_builder = std::make_shared<UnionSourceOperatorBuilder>(
                    node->id(), union_node, data_queue);
            RETURN_IF_ERROR(cur_pipe->add_operator(source_builder));
        }
        break;
    }
    case TPlanNodeType::AGGREGATION_NODE: {
        auto* agg_node = static_cast<vectorized::AggregationNode*>(node);
        auto new_pipe = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipe));
        if (agg_node->is_probe_expr_ctxs_empty() && agg_node->agg_output_desc()->slots().empty()) {
            return Status::InternalError("Illegal aggregate node " +
                                         std::to_string(agg_node->id()) +
                                         ": group by and output is empty");
        }

        const int64_t data_queue_max_blocks = _runtime_state->data_queue_max_blocks();
        if (agg_node->is_aggregate_evaluators_empty() && !agg_node->is_probe_expr_ctxs_empty()) {
            auto data_queue = std::make_shared<DataQueue>(1);
            data_queue->set_max_blocks_in_sub_queue(data_queue_max_blocks);
            OperatorBuilderPtr pre_agg_sink =
                    std::make_shared<DistinctStreamingAggSinkOperatorBuilder>(node->id(), agg_node,
                                                                              data_queue);
            RETURN_IF_ERROR(new_pipe->set_sink_builder(pre_agg_sink));

            OperatorBuilderPtr pre_agg_source =
                    std::make_shared<DistinctStreamingAggSourceOperatorBuilder>(
                            node->id(), agg_node, data_queue);
            RETURN_IF_ERROR(cur_pipe->add_operator(pre_agg_source));
        } else if (agg_node->is_streaming_preagg() && !agg_node->is_probe_expr_ctxs_empty()) {
            auto data_queue = std::make_shared<DataQueue>(1);
            data_queue->set_max_blocks_in_sub_queue(data_queue_max_blocks);
            OperatorBuilderPtr pre_agg_sink = std::make_shared<StreamingAggSinkOperatorBuilder>(
                    node->id(), agg_node, data_queue);
            RETURN_IF_ERROR(new_pipe->set_sink_builder(pre_agg_sink));

            OperatorBuilderPtr pre_agg_source = std::make_shared<StreamingAggSourceOperatorBuilder>(
                    node->id(), agg_node, data_queue);
            RETURN_IF_ERROR(cur_pipe->add_operator(pre_agg_source));
        } else {
            OperatorBuilderPtr agg_sink =
                    std::make_shared<AggSinkOperatorBuilder>(node->id(), agg_node);
            RETURN_IF_ERROR(new_pipe->set_sink_builder(agg_sink));

            OperatorBuilderPtr agg_source =
                    std::make_shared<AggSourceOperatorBuilder>(node->id(), agg_node);
            RETURN_IF_ERROR(cur_pipe->add_operator(agg_source));
        }
        break;
    }
    case TPlanNodeType::SORT_NODE: {
        auto new_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipeline));

        OperatorBuilderPtr sort_sink = std::make_shared<SortSinkOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(new_pipeline->set_sink_builder(sort_sink));

        OperatorBuilderPtr sort_source =
                std::make_shared<SortSourceOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(sort_source));
        break;
    }
    case TPlanNodeType::PARTITION_SORT_NODE: {
        auto new_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipeline));

        OperatorBuilderPtr partition_sort_sink =
                std::make_shared<PartitionSortSinkOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(new_pipeline->set_sink_builder(partition_sort_sink));

        OperatorBuilderPtr partition_sort_source =
                std::make_shared<PartitionSortSourceOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(partition_sort_source));
        break;
    }
    case TPlanNodeType::ANALYTIC_EVAL_NODE: {
        auto new_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipeline));

        OperatorBuilderPtr analytic_sink =
                std::make_shared<AnalyticSinkOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(new_pipeline->set_sink_builder(analytic_sink));

        OperatorBuilderPtr analytic_source =
                std::make_shared<AnalyticSourceOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(analytic_source));
        break;
    }
    case TPlanNodeType::REPEAT_NODE: {
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder = std::make_shared<RepeatOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        break;
    }
    case TPlanNodeType::ASSERT_NUM_ROWS_NODE: {
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder =
                std::make_shared<AssertNumRowsOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(cur_pipe->add_operator(builder));
        break;
    }
    case TPlanNodeType::TABLE_FUNCTION_NODE: {
        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr builder =
                std::make_shared<TableFunctionOperatorBuilder>(node->id(), node);
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
                    node->child(1)->id(), node->child(1)->row_desc(), node->child(1));
            RETURN_IF_ERROR(new_pipe->add_operator(builder));
        }
        OperatorBuilderPtr join_sink =
                std::make_shared<HashJoinBuildSinkBuilder>(node->id(), join_node);
        RETURN_IF_ERROR(new_pipe->set_sink_builder(join_sink));

        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr join_source =
                std::make_shared<HashJoinProbeOperatorBuilder>(node->id(), join_node);
        RETURN_IF_ERROR(cur_pipe->add_operator(join_source));

        cur_pipe->add_dependency(new_pipe);
        break;
    }
    case TPlanNodeType::CROSS_JOIN_NODE: {
        auto new_pipe = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(1), new_pipe));
        OperatorBuilderPtr join_sink =
                std::make_shared<NestLoopJoinBuildOperatorBuilder>(node->id(), node);
        RETURN_IF_ERROR(new_pipe->set_sink_builder(join_sink));

        RETURN_IF_ERROR(_build_pipelines(node->child(0), cur_pipe));
        OperatorBuilderPtr join_source =
                std::make_shared<NestLoopJoinProbeOperatorBuilder>(node->id(), node);
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
        OperatorBuilderPtr builder = std::make_shared<SelectOperatorBuilder>(node->id(), node);
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
    OperatorBuilderPtr sink_builder =
            std::make_shared<SetSinkOperatorBuilder<is_intersect>>(node->id(), node);
    RETURN_IF_ERROR(build_pipeline->set_sink_builder(sink_builder));
    std::vector<PipelinePtr> all_pipelines;
    all_pipelines.emplace_back(build_pipeline);
    for (int child_id = 1; child_id < node->children_count(); ++child_id) {
        auto probe_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(child_id), probe_pipeline));
        OperatorBuilderPtr probe_sink_builder =
                std::make_shared<SetProbeSinkOperatorBuilder<is_intersect>>(node->id(), child_id,
                                                                            node);
        RETURN_IF_ERROR(probe_pipeline->set_sink_builder(probe_sink_builder));
        //eg: These sinks must be completed one by one in order, child(1) must wait child(0) build finish
        probe_pipeline->add_dependency(all_pipelines[child_id - 1]);
        all_pipelines.emplace_back(probe_pipeline);
    }

    OperatorBuilderPtr source_builder =
            std::make_shared<SetSourceOperatorBuilder<is_intersect>>(node->id(), node);
    return cur_pipe->add_operator(source_builder);
}

Status PipelineFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submitted");
    }
    _submitted = true;

    int submit_tasks = 0;
    Status st;
    auto* scheduler = _query_ctx->get_pipe_exec_scheduler();
    for (auto& task : _tasks) {
        st = scheduler->schedule_task(task.get());
        if (!st) {
            {
                std::lock_guard<std::mutex> l(_status_lock);
                cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "submit context fail");
            }
            {
                // The fragment instance may contains 10 tasks, maybe 8 of them is in scheduler running
                // and the 9th failed to add to scheduler, then it will update total_tasks.
                // But the previous 8 tasks may finished and try to access total_tasks. So that
                // has to use a lock to protect it.
                std::lock_guard<std::mutex> l(_task_mutex);
                _total_tasks = submit_tasks;
            }
            break;
        }
        submit_tasks++;
    }
    if (!st.ok()) {
        std::lock_guard<std::mutex> l(_task_mutex);
        if (_closed_tasks == _total_tasks) {
            _close_fragment_instance();
        }
        return Status::InternalError("Submit pipeline failed. err = {}, BE: {}", st.to_string(),
                                     BackendOptions::get_localhost());
    } else {
        return st;
    }
}

void PipelineFragmentContext::close_sink() {
    if (_sink) {
        if (_prepared) {
            static_cast<void>(
                    _sink->close(_runtime_state.get(), Status::RuntimeError("prepare failed")));
        } else {
            static_cast<void>(_sink->close(_runtime_state.get(), Status::OK()));
        }
    }
}

void PipelineFragmentContext::close_if_prepare_failed(Status /*st*/) {
    if (_tasks.empty()) {
        if (_root_plan) {
            static_cast<void>(_root_plan->close(_runtime_state.get()));
        }
        if (_sink) {
            static_cast<void>(
                    _sink->close(_runtime_state.get(), Status::RuntimeError("prepare failed")));
        }
    }
    for (auto& task : _tasks) {
        DCHECK(!task->is_pending_finish());
        WARN_IF_ERROR(task->close(Status::OK()),
                      fmt::format("Query {} closed since prepare failed", print_id(_query_id)));
        close_a_pipeline();
    }
}

// construct sink operator
Status PipelineFragmentContext::_create_sink(int sender_id, const TDataSink& thrift_sink,
                                             RuntimeState* state) {
    OperatorBuilderPtr sink_;
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        sink_ = std::make_shared<ExchangeSinkOperatorBuilder>(thrift_sink.stream_sink.dest_node_id,
                                                              _sink.get());
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        sink_ = std::make_shared<ResultSinkOperatorBuilder>(next_operator_builder_id(),
                                                            _sink.get());
        break;
    }
    case TDataSinkType::GROUP_COMMIT_OLAP_TABLE_SINK:
    case TDataSinkType::OLAP_TABLE_SINK: {
        DCHECK(thrift_sink.__isset.olap_table_sink);
        if (state->query_options().enable_memtable_on_sink_node &&
            !_has_inverted_index_or_partial_update(thrift_sink.olap_table_sink)) {
            sink_ = std::make_shared<OlapTableSinkV2OperatorBuilder>(next_operator_builder_id(),
                                                                     _sink.get());
        } else {
            sink_ = std::make_shared<OlapTableSinkOperatorBuilder>(next_operator_builder_id(),
                                                                   _sink.get());
        }
        break;
    }
    case TDataSinkType::GROUP_COMMIT_BLOCK_SINK: {
        sink_ = std::make_shared<GroupCommitBlockSinkOperatorBuilder>(next_operator_builder_id(),
                                                                      _sink.get());
        break;
    }
    case TDataSinkType::HIVE_TABLE_SINK: {
        sink_ = std::make_shared<HiveTableSinkOperatorBuilder>(next_operator_builder_id(),
                                                               _sink.get());
        break;
    }
    case TDataSinkType::ICEBERG_TABLE_SINK: {
        sink_ = std::make_shared<IcebergTableSinkOperatorBuilder>(next_operator_builder_id(),
                                                                  _sink.get());
        break;
    }
    case TDataSinkType::MYSQL_TABLE_SINK:
    case TDataSinkType::JDBC_TABLE_SINK:
    case TDataSinkType::ODBC_TABLE_SINK:
    case TDataSinkType::RESULT_FILE_SINK: {
        sink_ = std::make_shared<ResultFileSinkOperatorBuilder>(
                thrift_sink.result_file_sink.dest_node_id, _sink.get());
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        sink_ = std::make_shared<MultiCastDataStreamSinkOperatorBuilder>(next_operator_builder_id(),
                                                                         _sink.get());
        RETURN_IF_ERROR(_root_pipeline->set_sink_builder(sink_));

        auto& multi_cast_data_streamer =
                assert_cast<vectorized::MultiCastDataStreamSink*>(_sink.get())
                        ->get_multi_cast_data_streamer();
        DCHECK_EQ(thrift_sink.multi_cast_stream_sink.sinks.size(),
                  thrift_sink.multi_cast_stream_sink.destinations.size());
        auto sender_size = thrift_sink.multi_cast_stream_sink.sinks.size();
        _multi_cast_stream_sink_senders.resize(sender_size);
        for (int i = 0; i < sender_size; ++i) {
            auto new_pipeline = add_pipeline();

            auto row_desc =
                    !thrift_sink.multi_cast_stream_sink.sinks[i].output_exprs.empty()
                            ? RowDescriptor(
                                      _runtime_state->desc_tbl(),
                                      {thrift_sink.multi_cast_stream_sink.sinks[i].output_tuple_id},
                                      {false})
                            : sink_->row_desc();
            // 1. create the data stream sender sink
            _multi_cast_stream_sink_senders[i] = std::make_unique<vectorized::VDataStreamSender>(
                    _runtime_state.get(), _runtime_state->obj_pool(), sender_id, row_desc,
                    thrift_sink.multi_cast_stream_sink.sinks[i],
                    thrift_sink.multi_cast_stream_sink.destinations[i]);

            // 2. create and set the source operator of multi_cast_data_stream_source for new pipeline
            OperatorBuilderPtr source_op =
                    std::make_shared<MultiCastDataStreamerSourceOperatorBuilder>(
                            next_operator_builder_id(), i, multi_cast_data_streamer,
                            thrift_sink.multi_cast_stream_sink.sinks[i]);
            static_cast<void>(new_pipeline->add_operator(source_op));

            // 3. create and set sink operator of data stream sender for new pipeline
            OperatorBuilderPtr sink_op_builder = std::make_shared<ExchangeSinkOperatorBuilder>(
                    next_operator_builder_id(), _multi_cast_stream_sink_senders[i].get(), i);
            static_cast<void>(new_pipeline->set_sink_builder(sink_op_builder));

            // 4. init and prepare the data_stream_sender of diff exchange
            TDataSink t;
            t.stream_sink = thrift_sink.multi_cast_stream_sink.sinks[i];
            RETURN_IF_ERROR(_multi_cast_stream_sink_senders[i]->init(t));
            RETURN_IF_ERROR(_multi_cast_stream_sink_senders[i]->prepare(state));
        }

        return Status::OK();
    }
    default:
        return Status::InternalError("Unsuported sink type in pipeline: {}", thrift_sink.type);
    }
    return _root_pipeline->set_sink_builder(sink_);
}

// If all pipeline tasks binded to the fragment instance are finished, then we could
// close the fragment instance.
void PipelineFragmentContext::_close_fragment_instance() {
    if (_is_fragment_instance_closed) {
        return;
    }
    Defer defer_op {[&]() { _is_fragment_instance_closed = true; }};
    _runtime_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
    _runtime_state->runtime_profile()->total_time_counter()->update(
            _fragment_watcher.elapsed_time());
    static_cast<void>(send_report(true));
    if (_runtime_state->enable_profile()) {
        std::stringstream ss;
        // Compute the _local_time_percent before pretty_print the runtime_profile
        // Before add this operation, the print out like that:
        // UNION_NODE (id=0):(Active: 56.720us, non-child: 00.00%)
        // After add the operation, the print out like that:
        // UNION_NODE (id=0):(Active: 56.720us, non-child: 82.53%)
        // We can easily know the exec node execute time without child time consumed.
        _runtime_state->runtime_profile()->compute_time_in_profile();
        _runtime_state->runtime_profile()->pretty_print(&ss);
        if (_runtime_state->load_channel_profile()) {
            _runtime_state->load_channel_profile()->pretty_print(&ss);
        }

        LOG_INFO("Query {} fragment {} instance {} profile:\n {}", print_id(this->_query_id),
                 this->_fragment_id, print_id(this->get_fragment_instance_id()), ss.str());
    }
    // all submitted tasks done
    _exec_env->fragment_mgr()->remove_pipeline_context(
            std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
}

void PipelineFragmentContext::close_a_pipeline() {
    std::lock_guard<std::mutex> l(_task_mutex);
    g_pipeline_tasks_count << -1;
    ++_closed_tasks;
    if (_closed_tasks == _total_tasks) {
        _close_fragment_instance();
    }
}

Status PipelineFragmentContext::send_report(bool done) {
    Status exec_status = Status::OK();
    {
        std::lock_guard<std::mutex> l(_status_lock);
        exec_status = _query_ctx->exec_status();
    }

    // If plan is done successfully, but _is_report_success is false,
    // no need to send report.
    if (!_is_report_success && done && exec_status.ok()) {
        return Status::NeedSendAgain("");
    }

    // If both _is_report_success and _is_report_on_cancel are false,
    // which means no matter query is success or failed, no report is needed.
    // This may happen when the query limit reached and
    // a internal cancellation being processed
    if (!_is_report_success && !_is_report_on_cancel) {
        return Status::NeedSendAgain("");
    }

    return _report_status_cb(
            {false,
             exec_status,
             {},
             _runtime_state->enable_profile() ? _runtime_state->runtime_profile() : nullptr,
             _runtime_state->enable_profile() ? _runtime_state->load_channel_profile() : nullptr,
             done || !exec_status.ok(),
             _query_ctx->coord_addr,
             _query_id,
             _fragment_id,
             _fragment_instance_id,
             _backend_num,
             _runtime_state.get(),
             [this](Status st) { return update_status(st); },
             [this](const PPlanFragmentCancelReason& reason, const std::string& msg) {
                 cancel(reason, msg);
             }},
            std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
}

std::string PipelineFragmentContext::debug_string() {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "PipelineFragmentContext Info: QueryId = {}\n",
                   print_id(_query_ctx->query_id()));
    return fmt::to_string(debug_string_buffer);
}

} // namespace doris::pipeline
