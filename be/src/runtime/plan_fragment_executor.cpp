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
// This file is copied from
// https://github.com/cloudera/Impala/blob/v0.7refresh/be/src/runtime/plan-fragment-executor.cc
// and modified by Doris

#include "runtime/plan_fragment_executor.h"

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Planner_types.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>
#include <opentelemetry/trace/tracer.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <ostream>
#include <typeinfo>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/version_internal.h"
#include "exec/data_sink.h"
#include "exec/exec_node.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/query_statistics.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/thread_context.h"
#include "util/container_util.hpp"
#include "util/defer_op.h"
#include "util/pretty_printer.h"
#include "util/stack_util.h"
#include "util/telemetry/telemetry.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/exec/scan/new_es_scan_node.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/exec/scan/new_jdbc_scan_node.h"
#include "vec/exec/scan/new_odbc_scan_node.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/exec/scan/vmeta_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/vexchange_node.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris {
using namespace ErrorCode;

PlanFragmentExecutor::PlanFragmentExecutor(ExecEnv* exec_env,
                                           const report_status_callback& report_status_cb)
        : _exec_env(exec_env),
          _plan(nullptr),
          _report_status_cb(report_status_cb),
          _report_thread_active(false),
          _done(false),
          _prepared(false),
          _closed(false),
          _is_report_success(false),
          _is_report_on_cancel(true),
          _collect_query_statistics_with_every_batch(false),
          _cancel_reason(PPlanFragmentCancelReason::INTERNAL_ERROR) {
    _report_thread_future = _report_thread_promise.get_future();
}

PlanFragmentExecutor::~PlanFragmentExecutor() {
    if (_runtime_state != nullptr) {
        // The memory released by the query end is recorded in the query mem tracker, main memory in _runtime_state.
        SCOPED_ATTACH_TASK(_runtime_state.get());
        close();
        _runtime_state.reset();
    } else {
        close();
    }
    // at this point, the report thread should have been stopped
    DCHECK(!_report_thread_active);
}

Status PlanFragmentExecutor::prepare(const TExecPlanFragmentParams& request,
                                     QueryContext* query_ctx) {
    OpentelemetryTracer tracer = telemetry::get_noop_tracer();
    if (opentelemetry::trace::Tracer::GetCurrentSpan()->GetContext().IsValid()) {
        tracer = telemetry::get_tracer(print_id(_query_id));
    }
    _span = tracer->StartSpan("Plan_fragment_executor");
    OpentelemetryScope scope {_span};

    const TPlanFragmentExecParams& params = request.params;
    _query_id = params.query_id;

    LOG_INFO("PlanFragmentExecutor::prepare")
            .tag("query_id", print_id(_query_id))
            .tag("instance_id", print_id(params.fragment_instance_id))
            .tag("backend_num", request.backend_num)
            .tag("pthread_id", (uintptr_t)pthread_self());
    // VLOG_CRITICAL << "request:\n" << apache::thrift::ThriftDebugString(request);

    const TQueryGlobals& query_globals =
            query_ctx == nullptr ? request.query_globals : query_ctx->query_globals;
    _runtime_state =
            RuntimeState::create_unique(params, request.query_options, query_globals, _exec_env);
    _runtime_state->set_query_ctx(query_ctx);
    _runtime_state->set_query_mem_tracker(query_ctx == nullptr ? _exec_env->orphan_mem_tracker()
                                                               : query_ctx->query_mem_tracker);
    _runtime_state->set_tracer(std::move(tracer));

    SCOPED_ATTACH_TASK(_runtime_state.get());
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
        _is_report_success = request.query_options.is_report_success;
    }

    // set up desc tbl
    if (request.is_simplified_param) {
        _desc_tbl = query_ctx->desc_tbl;
    } else {
        DCHECK(request.__isset.desc_tbl);
        RETURN_IF_ERROR(
                DescriptorTbl::create(_runtime_state->obj_pool(), request.desc_tbl, &_desc_tbl));
    }
    _runtime_state->set_desc_tbl(_desc_tbl);

    // set up plan
    DCHECK(request.__isset.fragment);
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(ExecNode::create_tree(
            _runtime_state.get(), obj_pool(), request.fragment.plan, *_desc_tbl, &_plan));

    // set #senders of exchange nodes before calling Prepare()
    std::vector<ExecNode*> exch_nodes;
    _plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    for (ExecNode* exch_node : exch_nodes) {
        DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
        int num_senders = find_with_default(params.per_exch_num_senders, exch_node->id(), 0);
        DCHECK_GT(num_senders, 0);
        static_cast<doris::vectorized::VExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }

    // TODO Is it exception safe?
    RETURN_IF_ERROR(_plan->prepare(_runtime_state.get()));
    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    std::vector<TScanRangeParams> no_scan_ranges;
    _plan->collect_scan_nodes(&scan_nodes);
    VLOG_CRITICAL << "scan_nodes.size()=" << scan_nodes.size();
    VLOG_CRITICAL << "params.per_node_scan_ranges.size()=" << params.per_node_scan_ranges.size();

    for (int i = 0; i < scan_nodes.size(); ++i) {
        // TODO(cmy): this "if...else" should be removed once all ScanNode are derived from VScanNode.
        ExecNode* node = scan_nodes[i];
        if (typeid(*node) == typeid(vectorized::NewOlapScanNode) ||
            typeid(*node) == typeid(vectorized::NewFileScanNode) ||
            typeid(*node) == typeid(vectorized::NewOdbcScanNode) ||
            typeid(*node) == typeid(vectorized::NewEsScanNode) ||
            typeid(*node) == typeid(vectorized::NewJdbcScanNode) ||
            typeid(*node) == typeid(vectorized::VMetaScanNode)) {
            vectorized::VScanNode* scan_node = static_cast<vectorized::VScanNode*>(scan_nodes[i]);
            auto scan_ranges =
                    find_with_default(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
            scan_node->set_scan_ranges(runtime_state(), scan_ranges);
        } else {
            ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[i]);
            auto scan_ranges =
                    find_with_default(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
            scan_node->set_scan_ranges(runtime_state(), scan_ranges);
            VLOG_CRITICAL << "scan_node_Id=" << scan_node->id()
                          << " size=" << scan_ranges.get().size();
        }
    }

    _runtime_state->set_per_fragment_instance_idx(params.sender_id);
    _runtime_state->set_num_per_fragment_instances(params.num_senders);

    // set up sink, if required
    if (request.fragment.__isset.output_sink) {
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(DataSink::create_data_sink(
                obj_pool(), request.fragment.output_sink, request.fragment.output_exprs, params,
                row_desc(), runtime_state(), &_sink, *_desc_tbl));
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_sink->prepare(runtime_state()));

        RuntimeProfile* sink_profile = _sink->profile();
        if (sink_profile != nullptr) {
            profile()->add_child(sink_profile, true, nullptr);
        }

        _collect_query_statistics_with_every_batch =
                params.__isset.send_query_statistics_with_every_batch
                        ? params.send_query_statistics_with_every_batch
                        : false;
    } else {
        // _sink is set to nullptr
        _sink.reset(nullptr);
    }

    // set up profile counters
    profile()->add_child(_plan->runtime_profile(), true, nullptr);
    profile()->add_info_string("DorisBeVersion", version::doris_build_short_hash());
    _rows_produced_counter = ADD_COUNTER(profile(), "RowsProduced", TUnit::UNIT);
    _blocks_produced_counter = ADD_COUNTER(profile(), "BlocksProduced", TUnit::UNIT);
    _fragment_cpu_timer = ADD_TIMER(profile(), "FragmentCpuTime");

    VLOG_NOTICE << "plan_root=\n" << _plan->debug_string();
    _prepared = true;

    _query_statistics.reset(new QueryStatistics());
    if (_sink != nullptr) {
        _sink->set_query_statistics(_query_statistics);
    }
    return Status::OK();
}

Status PlanFragmentExecutor::open() {
    int64_t mem_limit = _runtime_state->query_mem_tracker()->limit();
    LOG_INFO("PlanFragmentExecutor::open")
            .tag("query_id", print_id(_query_id))
            .tag("instance_id", print_id(_runtime_state->fragment_instance_id()))
            .tag("mem_limit", PrettyPrinter::print(mem_limit, TUnit::BYTES));

    // we need to start the profile-reporting thread before calling Open(), since it
    // may block
    // TODO: if no report thread is started, make sure to send a final profile
    // at end, otherwise the coordinator hangs in case we finish w/ an error
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
    Status status = Status::OK();
    status = open_vectorized_internal();

    if (!status.ok() && !status.is<CANCELLED>() && _runtime_state->log_has_space()) {
        // Log error message in addition to returning in Status. Queries that do not
        // fetch results (e.g. insert) may not receive the message directly and can
        // only retrieve the log.
        _runtime_state->log_error(status.to_string());
    }
    if (status.is<CANCELLED>()) {
        if (_cancel_reason == PPlanFragmentCancelReason::CALL_RPC_ERROR) {
            status = Status::RuntimeError(_cancel_msg);
        } else if (_cancel_reason == PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED) {
            status = Status::MemoryLimitExceeded(_cancel_msg);
        }
    }

    {
        std::lock_guard<std::mutex> l(_status_lock);
        _status = status;
        if (status.is<MEM_LIMIT_EXCEEDED>()) {
            _runtime_state->set_mem_limit_exceeded(status.to_string());
        }
        if (_runtime_state->query_type() == TQueryType::EXTERNAL) {
            TUniqueId fragment_instance_id = _runtime_state->fragment_instance_id();
            _exec_env->result_queue_mgr()->update_queue_status(fragment_instance_id, status);
        }
    }

    stop_report_thread();
    send_report(true);
    return status;
}

Status PlanFragmentExecutor::open_vectorized_internal() {
    SCOPED_TIMER(profile()->total_time_counter());
    {
        SCOPED_CPU_TIMER(_fragment_cpu_timer);
        RETURN_IF_ERROR(_plan->open(_runtime_state.get()));
        RETURN_IF_CANCELLED(_runtime_state);
        if (_sink == nullptr) {
            return Status::OK();
        }
        RETURN_IF_ERROR(_sink->open(runtime_state()));
        doris::vectorized::Block block;
        bool eos = false;

        while (!eos) {
            RETURN_IF_CANCELLED(_runtime_state);
            RETURN_IF_ERROR(get_vectorized_internal(&block, &eos));

            // Collect this plan and sub plan statistics, and send to parent plan.
            if (_collect_query_statistics_with_every_batch) {
                _collect_query_statistics();
            }

            if (!eos || block.rows() > 0) {
                auto st = _sink->send(runtime_state(), &block);
                if (st.is<END_OF_FILE>()) {
                    break;
                }
                RETURN_IF_ERROR(st);
            }
        }
    }
    {
        _collect_query_statistics();
        Status status;
        {
            std::lock_guard<std::mutex> l(_status_lock);
            status = _status;
        }
        status = _sink->close(runtime_state(), status);
        RETURN_IF_ERROR(status);
    }
    // Setting to NULL ensures that the d'tor won't double-close the sink.
    _sink.reset(nullptr);
    _done = true;
    return Status::OK();
}

Status PlanFragmentExecutor::get_vectorized_internal(::doris::vectorized::Block* block, bool* eos) {
    while (!_done) {
        block->clear_column_data(_plan->row_desc().num_materialized_slots());
        RETURN_IF_ERROR(_plan->get_next_after_projects(
                _runtime_state.get(), block, &_done,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _plan, std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));

        if (block->rows() > 0) {
            COUNTER_UPDATE(_rows_produced_counter, block->rows());
            // Not very sure, if should contain empty block
            COUNTER_UPDATE(_blocks_produced_counter, 1);
            break;
        }
    }
    *eos = _done;

    return Status::OK();
}

void PlanFragmentExecutor::_collect_query_statistics() {
    _query_statistics->clear();
    Status status = _plan->collect_query_statistics(_query_statistics.get());
    if (!status.ok()) {
        LOG(INFO) << "collect query statistics failed, st=" << status;
        return;
    }
    _query_statistics->add_cpu_ms(_fragment_cpu_timer->value() / NANOS_PER_MILLIS);
    if (_runtime_state->backend_id() != -1) {
        _collect_node_statistics();
    }
}

void PlanFragmentExecutor::_collect_node_statistics() {
    DCHECK(_runtime_state->backend_id() != -1);
    NodeStatistics* node_statistics =
            _query_statistics->add_nodes_statistics(_runtime_state->backend_id());
    node_statistics->set_peak_memory(_runtime_state->query_mem_tracker()->peak_consumption());
}

void PlanFragmentExecutor::report_profile() {
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
            profile()->compute_time_in_profile();
            profile()->pretty_print(&ss);
            if (load_channel_profile()) {
                // load_channel_profile()->compute_time_in_profile(); // TODO load channel profile add timer
                load_channel_profile()->pretty_print(&ss);
            }
            VLOG_FILE << ss.str();
        }

        if (!_report_thread_active) {
            break;
        }

        send_report(false);
    }

    VLOG_FILE << "exiting reporting thread: instance_id=" << _runtime_state->fragment_instance_id();
}

void PlanFragmentExecutor::send_report(bool done) {
    Status status;
    {
        std::lock_guard<std::mutex> l(_status_lock);
        status = _status;
    }

    // If plan is done successfully, but _is_report_success is false,
    // no need to send report.
    if (!_is_report_success && done && status.ok()) {
        return;
    }

    // If both _is_report_success and _is_report_on_cancel are false,
    // which means no matter query is success or failed, no report is needed.
    // This may happen when the query limit reached and
    // a internal cancellation being processed
    if (!_is_report_success && !_is_report_on_cancel) {
        return;
    }
    // This will send a report even if we are cancelled.  If the query completed correctly
    // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
    // be waiting for a final report and profile.
    _report_status_cb(status, _is_report_success ? profile() : nullptr,
                      _is_report_success ? load_channel_profile() : nullptr, done || !status.ok());
}

void PlanFragmentExecutor::stop_report_thread() {
    if (!_report_thread_active) {
        return;
    }

    _report_thread_active = false;

    _stop_report_thread_cv.notify_one();
    // Wait infinitly until the thread is stopped and the future is set.
    // The reporting thread depends on the PlanFragmentExecutor object, if not wait infinitly here, the reporting
    // thread may crashed because the PlanFragmentExecutor is destroyed.
    _report_thread_future.wait();
}

void PlanFragmentExecutor::cancel(const PPlanFragmentCancelReason& reason, const std::string& msg) {
    LOG_INFO("PlanFragmentExecutor::cancel")
            .tag("query_id", print_id(_query_id))
            .tag("instance_id", _runtime_state->fragment_instance_id())
            .tag("reason", reason)
            .tag("error message", msg);
    DCHECK(_prepared);
    _cancel_reason = reason;
    _cancel_msg = msg;
    _runtime_state->set_is_cancelled(true, msg);
    // To notify wait_for_start()
    _runtime_state->get_query_ctx()->set_ready_to_execute(true);

    // must close stream_mgr to avoid dead lock in Exchange Node
    auto env = _runtime_state->exec_env();
    auto id = _runtime_state->fragment_instance_id();
    env->vstream_mgr()->cancel(id);
    // Cancel the result queue manager used by spark doris connector
    _exec_env->result_queue_mgr()->update_queue_status(id, Status::Aborted(msg));
}

const RowDescriptor& PlanFragmentExecutor::row_desc() {
    return _plan->row_desc();
}

RuntimeProfile* PlanFragmentExecutor::profile() {
    return _runtime_state->runtime_profile();
}

RuntimeProfile* PlanFragmentExecutor::load_channel_profile() {
    return _runtime_state->load_channel_profile();
}

void PlanFragmentExecutor::close() {
    if (_closed) {
        return;
    }

    // Prepare may not have been called, which sets _runtime_state
    if (_runtime_state != nullptr) {
        // _runtime_state init failed
        if (_plan != nullptr) {
            _plan->close(_runtime_state.get());
        }

        if (_sink != nullptr) {
            if (_prepared) {
                Status status;
                {
                    std::lock_guard<std::mutex> l(_status_lock);
                    status = _status;
                }
                _sink->close(runtime_state(), status);
            } else {
                _sink->close(runtime_state(), Status::InternalError("prepare failed"));
            }
        }

        if (_is_report_success) {
            std::stringstream ss;
            // Compute the _local_time_percent before pretty_print the runtime_profile
            // Before add this operation, the print out like that:
            // UNION_NODE (id=0):(Active: 56.720us, non-child: 00.00%)
            // After add the operation, the print out like that:
            // UNION_NODE (id=0):(Active: 56.720us, non-child: 82.53%)
            // We can easily know the exec node execute time without child time consumed.
            profile()->compute_time_in_profile();
            profile()->pretty_print(&ss);
            if (load_channel_profile()) {
                // load_channel_profile()->compute_time_in_profile();  // TODO load channel profile add timer
                load_channel_profile()->pretty_print(&ss);
            }
            LOG(INFO) << ss.str();
        }
        LOG(INFO) << "Close() fragment_instance_id="
                  << print_id(_runtime_state->fragment_instance_id());
    }

    profile()->add_to_span(_span);
    _closed = true;
}

} // namespace doris
