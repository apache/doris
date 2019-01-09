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

#include "runtime/plan_fragment_executor.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_map.hpp>
#include <boost/foreach.hpp>

#include "codegen/llvm_codegen.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "exec/data_sink.h"
#include "exec/exec_node.h"
#include "exec/exchange_node.h"
#include "exec/scan_node.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/descriptors.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/mem_tracker.h"
#include "util/cpu_info.h"
#include "util/uid_util.h"
#include "util/container_util.hpp"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/mem_info.h"

namespace doris {

PlanFragmentExecutor::PlanFragmentExecutor(
    ExecEnv* exec_env, const report_status_callback& report_status_cb)
    : _exec_env(exec_env),
      _report_status_cb(report_status_cb),
      _report_thread_active(false),
      _done(false),
      _prepared(false),
      _closed(false),
      _has_thread_token(false),
      _is_report_success(true) {
}

PlanFragmentExecutor::~PlanFragmentExecutor() {
    // if (_prepared) {
    close();
    // }
    // at this point, the report thread should have been stopped
    DCHECK(!_report_thread_active);
}

Status PlanFragmentExecutor::prepare(const TExecPlanFragmentParams& request) {
    const TPlanFragmentExecParams& params = request.params;
    _query_id = params.query_id;

    LOG(INFO) << "Prepare(): query_id=" << print_id(_query_id)
               << " fragment_instance_id=" << print_id(params.fragment_instance_id)
               << " backend_num=" << request.backend_num;
    // VLOG(2) << "request:\n" << apache::thrift::ThriftDebugString(request);

    _runtime_state.reset(new RuntimeState(
            request, request.query_options, request.query_globals.now_string, _exec_env));

    RETURN_IF_ERROR(_runtime_state->init_mem_trackers(_query_id));
    _runtime_state->set_be_number(request.backend_num);
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
        _is_report_success = request.query_options.is_report_success;
    }

    // Reserve one main thread from the pool
    _runtime_state->resource_pool()->acquire_thread_token();
    _has_thread_token = true;

    _average_thread_tokens = profile()->add_sampling_counter(
            "AverageThreadTokens",
            boost::bind<int64_t>(boost::mem_fn(
                    &ThreadResourceMgr::ResourcePool::num_threads),
                    _runtime_state->resource_pool()));

    // if (_exec_env->process_mem_tracker() != NULL) {
    //     // we have a global limit
    //     _runtime_state->mem_trackers()->push_back(_exec_env->process_mem_tracker());
    // }

    if (request.query_options.mem_limit > 0) {
        // we have a per-query limit
        int64_t bytes_limit = request.query_options.mem_limit;
        // NOTE: this MemTracker only for olap
        _mem_tracker.reset(
                new MemTracker(bytes_limit, "fragment mem-limit", _exec_env->process_mem_tracker()));
        _runtime_state->set_fragment_mem_tracker(_mem_tracker.get());

        if (bytes_limit > MemInfo::physical_mem()) {
            LOG(WARNING) << "Memory limit "
                         << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                         << " exceeds physical memory of "
                         << PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES);
        }

        LOG(INFO) << "Using query memory limit: "
                   << PrettyPrinter::print(bytes_limit, TUnit::BYTES);
    }

    RETURN_IF_ERROR(_runtime_state->create_block_mgr());

    // set up desc tbl
    DescriptorTbl* desc_tbl = NULL;
    DCHECK(request.__isset.desc_tbl);
    RETURN_IF_ERROR(DescriptorTbl::create(obj_pool(), request.desc_tbl, &desc_tbl));
    _runtime_state->set_desc_tbl(desc_tbl);

    // set up plan
    DCHECK(request.__isset.fragment);
    RETURN_IF_ERROR(
            ExecNode::create_tree(_runtime_state.get(), obj_pool(), request.fragment.plan, *desc_tbl, &_plan));
    _runtime_state->set_fragment_root_id(_plan->id());

    if (request.params.__isset.debug_node_id) {
        DCHECK(request.params.__isset.debug_action);
        DCHECK(request.params.__isset.debug_phase);
        ExecNode::set_debug_options(
            request.params.debug_node_id, request.params.debug_phase,
            request.params.debug_action, _plan);
    }

    // set #senders of exchange nodes before calling Prepare()
    std::vector<ExecNode*> exch_nodes;
    _plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    BOOST_FOREACH(ExecNode * exch_node, exch_nodes) {
        DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
        int num_senders = find_with_default(params.per_exch_num_senders, exch_node->id(), 0);
        DCHECK_GT(num_senders, 0);
        static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }

 
    RETURN_IF_ERROR(_plan->prepare(_runtime_state.get()));
    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    std::vector<TScanRangeParams> no_scan_ranges;
    _plan->collect_scan_nodes(&scan_nodes);
    VLOG(1) << "scan_nodes.size()=" << scan_nodes.size();
    VLOG(1) << "params.per_node_scan_ranges.size()=" << params.per_node_scan_ranges.size();

    for (int i = 0; i < scan_nodes.size(); ++i) {
        ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[i]);
        const std::vector<TScanRangeParams>& scan_ranges =
            find_with_default(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
        scan_node->set_scan_ranges(scan_ranges);
        VLOG(1) << "scan_node_Id=" << scan_node->id() << " size=" << scan_ranges.size();
    }

    print_volume_ids(params.per_node_scan_ranges);

    _runtime_state->set_per_fragment_instance_idx(params.sender_id);
    _runtime_state->set_num_per_fragment_instances(params.num_senders);

    // set up sink, if required
    if (request.fragment.__isset.output_sink) {
        RETURN_IF_ERROR(DataSink::create_data_sink(obj_pool(),
                        request.fragment.output_sink, request.fragment.output_exprs, params,
                        row_desc(), &_sink));
        RETURN_IF_ERROR(_sink->prepare(runtime_state()));

        RuntimeProfile* sink_profile = _sink->profile();

        if (sink_profile != NULL) {
            profile()->add_child(sink_profile, true, NULL);
        }
    } else {
        _sink.reset(NULL);
    }

    // set up profile counters
    profile()->add_child(_plan->runtime_profile(), true, NULL);
    _rows_produced_counter = ADD_COUNTER(profile(), "RowsProduced", TUnit::UNIT);
#if 0
    // After preparing the plan and initializing the output sink, all functions should
    // have been code-generated.  At this point we optimize all the functions.
    if (_runtime_state->llvm_codegen() != NULL) {
        Status status = _runtime_state->llvm_codegen()->optimize_module();

        if (!status.ok()) {
            LOG(ERROR) << "Error with codegen for this query: " << status.get_error_msg();
            // TODO: propagate this to the coordinator and user?  Not really actionable
            // for them but we'd like them to let us know.
        }

        // If codegen failed, we automatically fall back to not using codegen.
    }
#endif

    _row_batch.reset(new RowBatch(
            _plan->row_desc(),
            _runtime_state->batch_size(),
            _runtime_state->instance_mem_tracker()));
    // _row_batch->tuple_data_pool()->set_limits(*_runtime_state->mem_trackers());
    VLOG(3) << "plan_root=\n" << _plan->debug_string();
    _prepared = true;
    return Status::OK;
}

void PlanFragmentExecutor::optimize_llvm_module() {
    if (!_runtime_state->codegen_created()) {
        return;
    }
    LlvmCodeGen* codegen = NULL;
    Status status = _runtime_state->get_codegen(&codegen, /* initalize */ false);
    DCHECK(status.ok());
    DCHECK(codegen != NULL);
    status = codegen->finalize_module();
    if (!status.ok()) {
        std::stringstream ss;
        ss << "Error with codegen for this query: ";
        _runtime_state->log_error(status.get_error_msg());
    }
}


void PlanFragmentExecutor::print_volume_ids(
    const PerNodeScanRanges& per_node_scan_ranges) {
    if (per_node_scan_ranges.empty()) {
        return;
    }
}

Status PlanFragmentExecutor::open() {
    LOG(INFO) << "Open(): fragment_instance_id=" << print_id(_runtime_state->fragment_instance_id());

    // we need to start the profile-reporting thread before calling Open(), since it
    // may block
    // TODO: if no report thread is started, make sure to send a final profile
    // at end, otherwise the coordinator hangs in case we finish w/ an error
    if (!_report_status_cb.empty() && config::status_report_interval > 0) {
        boost::unique_lock<boost::mutex> l(_report_thread_lock);
        _report_thread = boost::thread(&PlanFragmentExecutor::report_profile, this);
        // make sure the thread started up, otherwise report_profile() might get into a race
        // with stop_report_thread()
        _report_thread_started_cv.wait(l);
        _report_thread_active = true;
    }

    optimize_llvm_module();

    Status status = open_internal();

    if (!status.ok() && !status.is_cancelled() && _runtime_state->log_has_space()) {
        // Log error message in addition to returning in Status. Queries that do not
        // fetch results (e.g. insert) may not receive the message directly and can
        // only retrieve the log.
        _runtime_state->log_error(status.get_error_msg());
    }

    update_status(status);
    return status;
}

Status PlanFragmentExecutor::open_internal() {
    {
        SCOPED_TIMER(profile()->total_time_counter());
        RETURN_IF_ERROR(_plan->open(_runtime_state.get()));
    }

    if (_sink.get() == NULL) {
        return Status::OK;
    }
    RETURN_IF_ERROR(_sink->open(runtime_state()));

    // If there is a sink, do all the work of driving it here, so that
    // when this returns the query has actually finished
    RowBatch* batch = NULL;

    while (true) {
        RETURN_IF_ERROR(get_next_internal(&batch));

        if (batch == NULL) {
            break;
        }

        if (VLOG_ROW_IS_ON) {
            VLOG_ROW << "open_internal: #rows=" << batch->num_rows()
                << " desc=" << row_desc().debug_string();

            for (int i = 0; i < batch->num_rows(); ++i) {
                TupleRow* row = batch->get_row(i);
                VLOG_ROW << row->to_string(row_desc());
            }
        }

        SCOPED_TIMER(profile()->total_time_counter());
        RETURN_IF_ERROR(_sink->send(runtime_state(), batch));
    }

    // Close the sink *before* stopping the report thread. Close may
    // need to add some important information to the last report that
    // gets sent. (e.g. table sinks record the files they have written
    // to in this method)
    // The coordinator report channel waits until all backends are
    // either in error or have returned a status report with done =
    // true, so tearing down any data stream state (a separate
    // channel) in Close is safe.

    // TODO: If this returns an error, the d'tor will call Close again. We should
    // audit the sinks to check that this is ok, or change that behaviour.
    {
        SCOPED_TIMER(profile()->total_time_counter());
        ExecNodeConsumptionProvider::Consumption consumption = runtime_state()->get_consumption(); 
        _sink->set_query_consumption(consumption);
        Status status = _sink->close(runtime_state(), _status);
        RETURN_IF_ERROR(status);
    }

    // Setting to NULL ensures that the d'tor won't double-close the sink.
    _sink.reset(NULL);
    _done = true;

    release_thread_token();

    stop_report_thread();
    send_report(true);

    return Status::OK;
}

void PlanFragmentExecutor::report_profile() {
    VLOG_FILE << "report_profile(): instance_id="
              << _runtime_state->fragment_instance_id();
    DCHECK(!_report_status_cb.empty());
    boost::unique_lock<boost::mutex> l(_report_thread_lock);
    // tell Open() that we started
    _report_thread_started_cv.notify_one();

    // Jitter the reporting time of remote fragments by a random amount between
    // 0 and the report_interval.  This way, the coordinator doesn't get all the
    // updates at once so its better for contention as well as smoother progress
    // reporting.
    int report_fragment_offset = rand() % config::status_report_interval;
    boost::system_time timeout =
        boost::get_system_time() + boost::posix_time::seconds(report_fragment_offset);
    // We don't want to wait longer than it takes to run the entire fragment.
    _stop_report_thread_cv.timed_wait(l, timeout);
    bool is_report_profile_interval = _is_report_success && config::status_report_interval > 0;
    while (_report_thread_active) {
        if (is_report_profile_interval) {
            boost::system_time timeout =
                boost::get_system_time() + boost::posix_time::seconds(config::status_report_interval);
            // timed_wait can return because the timeout occurred or the condition variable
            // was signaled.  We can't rely on its return value to distinguish between the
            // two cases (e.g. there is a race here where the wait timed out but before grabbing
            // the lock, the condition variable was signaled).  Instead, we will use an external
            // flag, _report_thread_active, to coordinate this.
            _stop_report_thread_cv.timed_wait(l, timeout);
        } else {
            // Artificial triggering, such as show proc "/current_queries".
            _stop_report_thread_cv.wait(l);
        }

        if (VLOG_FILE_IS_ON) {
            VLOG_FILE << "Reporting " << (!_report_thread_active ? "final " : " ")
                      << "profile for instance " << _runtime_state->fragment_instance_id();
            std::stringstream ss;
            profile()->pretty_print(&ss);
            VLOG_FILE << ss.str();
        }

        if (!_report_thread_active) {
            break;
        }

        send_report(false);
    }

    VLOG_FILE << "exiting reporting thread: instance_id="
              << _runtime_state->fragment_instance_id();
}

void PlanFragmentExecutor::send_report(bool done) {
    if (_report_status_cb.empty()) {
        return;
    }

    Status status;
    {
        boost::lock_guard<boost::mutex> l(_status_lock);
        status = _status;
    }

    if (!_is_report_success && done && status.ok()) {
        return;
    }

    // This will send a report even if we are cancelled.  If the query completed correctly
    // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
    // be waiting for a final report and profile.
    _report_status_cb(status, profile(), done || !status.ok());
}

void PlanFragmentExecutor::stop_report_thread() {
    if (!_report_thread_active) {
        return;
    }

    {
        boost::lock_guard<boost::mutex> l(_report_thread_lock);
        _report_thread_active = false;
    }

    _stop_report_thread_cv.notify_one();
    _report_thread.join();
}

Status PlanFragmentExecutor::get_next(RowBatch** batch) {
    VLOG_FILE << "GetNext(): instance_id=" << _runtime_state->fragment_instance_id();
    Status status = get_next_internal(batch);
    update_status(status);

    if (_done) {
        LOG(INFO) << "Finished executing fragment query_id=" << print_id(_query_id)
                   << " instance_id=" << print_id(_runtime_state->fragment_instance_id());
        // Query is done, return the thread token
        release_thread_token();
        stop_report_thread();
        send_report(true);
    }

    return status;
}

Status PlanFragmentExecutor::get_next_internal(RowBatch** batch) {
    if (_done) {
        *batch = NULL;
        return Status::OK;
    }

    while (!_done) {
        _row_batch->reset();
        SCOPED_TIMER(profile()->total_time_counter());
        RETURN_IF_ERROR(_plan->get_next(_runtime_state.get(), _row_batch.get(), &_done));

        if (_row_batch->num_rows() > 0) {
            COUNTER_UPDATE(_rows_produced_counter, _row_batch->num_rows());
            *batch = _row_batch.get();
            break;
        }

        *batch = NULL;
    }

    return Status::OK;
}

void PlanFragmentExecutor::update_status(const Status& status) {
    if (status.ok()) {
        return;
    }

    {
        boost::lock_guard<boost::mutex> l(_status_lock);

        if (_status.ok()) {
            if (status.is_mem_limit_exceeded()) {
                _runtime_state->set_mem_limit_exceeded();
            }
            _status = status;
        }
    }

    stop_report_thread();
    send_report(true);
}

void PlanFragmentExecutor::cancel() {
    LOG(INFO) << "cancel(): fragment_instance_id=" << print_id(_runtime_state->fragment_instance_id());
    DCHECK(_prepared);
    _runtime_state->set_is_cancelled(true);
    _runtime_state->exec_env()->stream_mgr()->cancel(_runtime_state->fragment_instance_id());
    _runtime_state->exec_env()->result_mgr()->cancel(_runtime_state->fragment_instance_id());
}

const RowDescriptor& PlanFragmentExecutor::row_desc() {
    return _plan->row_desc();
}

RuntimeProfile* PlanFragmentExecutor::profile() {
    return _runtime_state->runtime_profile();
}

void PlanFragmentExecutor::release_thread_token() {
    if (_has_thread_token) {
        _has_thread_token = false;
        _runtime_state->resource_pool()->release_thread_token(true);
        profile()->stop_sampling_counters_updates(_average_thread_tokens);
    }
}

void PlanFragmentExecutor::close() {
    if (_closed) {
        return;
    }

    _row_batch.reset(NULL);

    // Prepare may not have been called, which sets _runtime_state
    if (_runtime_state.get() != NULL) {
        
        // _runtime_state init failed
        if (_plan != nullptr) {
            _plan->close(_runtime_state.get());
        }

        if (_sink.get() != NULL) {
            if (_prepared) {
                _sink->close(runtime_state(), _status);
            } else {
                _sink->close(runtime_state(), Status("prepare failed"));
            }
        }

        _exec_env->thread_mgr()->unregister_pool(_runtime_state->resource_pool());

        {
            std::stringstream ss;
            _runtime_state->runtime_profile()->pretty_print(&ss);
            LOG(INFO) << ss.str();
        }
    }
     
    // _mem_tracker init failed
    if (_mem_tracker.get() != nullptr) {
        _mem_tracker->release(_mem_tracker->consumption());
    }
    _closed = true;
}

}
