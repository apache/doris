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

#include "exec/kudu_scan_node.h"

#include <boost/algorithm/string.hpp>
#include <kudu/client/row_result.h>
#include <kudu/client/schema.h>
#include <kudu/client/value.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>

#include "exec/kudu_scanner.h"
#include "exec/kudu_util.h"
#include "exprs/expr.h"
// #include "gutil/gscoped_ptr.h"
// #include "gutil/strings/substitute.h"
// #include "gutil/stl_util.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
// #include "util/disk-info.h"
// #include "util/jni-util.h"
// #include "util/periodic-counter-updater.h"
// #include "util/runtime-profile-counters.h"

// #include "common/names.h"

using boost::algorithm::to_lower_copy;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduRowResult;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::Slice;

namespace doris {

const string KuduScanNode::KUDU_ROUND_TRIPS = "TotalKuduScanRoundTrips";
const string KuduScanNode::KUDU_REMOTE_TOKENS = "KuduRemoteScanTokens";

KuduScanNode::KuduScanNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      _tuple_id(tnode.kudu_scan_node.tuple_id),
      _next_scan_token_idx(0),
      _num_active_scanners(0),
      _done(false),
      _thread_avail_cb_id(-1) {
  DCHECK(KuduIsAvailable());

  int max_row_batches = config::kudu_max_row_batches;
  // if (max_row_batches <= 0) {
    // TODO: See comment on hdfs-scan-node.
    // This value is built the same way as it assumes that the scan node runs co-located
    // with a Kudu tablet server and that the tablet server is using disks similarly as
    // a datanode would.
  //   max_row_batches = 10 * (DiskInfo::num_disks() + DiskIoMgr::REMOTE_NUM_DISKS);
  //}
  _materialized_row_batches.reset(new RowBatchQueue(max_row_batches));
}

KuduScanNode::~KuduScanNode() {
  DCHECK(is_closed());
}

Status KuduScanNode::prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::prepare(state));
  _runtime_state = state;

  _scan_ranges_complete_counter =
      ADD_COUNTER(runtime_profile(), _s_scan_ranges_complete_counter, TUnit::UNIT);
  _kudu_round_trips = ADD_COUNTER(runtime_profile(), KUDU_ROUND_TRIPS, TUnit::UNIT);
  _kudu_remote_tokens = ADD_COUNTER(runtime_profile(), KUDU_REMOTE_TOKENS, TUnit::UNIT);

  DCHECK(state->desc_tbl().get_tuple_descriptor(_tuple_id) != NULL);

  _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

  return Status::OK();
}


Status KuduScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    // Initialize the list of scan tokens to process from the TScanRangeParams.
    // int num_remote_tokens = 0;
    for (const TScanRangeParams& params: scan_ranges) {
        // if (params.__isset.is_remote && params.is_remote) ++num_remote_tokens;
        _scan_tokens.push_back(params.scan_range.kudu_scan_token);
    }
    // COUNTER_SET(kudu_remote_tokens_, num_remote_tokens);
    return Status::OK();
}

Status KuduScanNode::open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::open(state));
  RETURN_IF_CANCELLED(state);
  // RETURN_IF_ERROR(query_maintenance(state));
  SCOPED_TIMER(_runtime_profile->total_time_counter());

  const KuduTableDescriptor* table_desc =
      static_cast<const KuduTableDescriptor*>(_tuple_desc->table_desc());

  RETURN_IF_ERROR(CreateKuduClient(table_desc->kudu_master_addresses(), &_client));

  uint64_t latest_ts = static_cast<uint64_t>(
      std::max<int64_t>(0, state->query_options().kudu_latest_observed_ts));
  VLOG_RPC << "Latest observed Kudu timestamp: " << latest_ts;
  if (latest_ts > 0) _client->SetLatestObservedTimestamp(latest_ts);

  KUDU_RETURN_IF_ERROR(_client->OpenTable(table_desc->table_name(), &_table),
      "Unable to open Kudu table");

  _num_scanner_threads_started_counter =
      ADD_COUNTER(runtime_profile(), _s_num_scanner_threads_started, TUnit::UNIT);

  // Reserve one thread.
  // state->resource_pool()->ReserveOptionalTokens(1);
  // if (state->query_options().num_scanner_threads > 0) {
  //   state->resource_pool()->set_max_quota(
  //       state->query_options().num_scanner_threads);
  // }

  // thread_avail_cb_id_ = state->resource_pool()->AddThreadAvailableCb(
  //    bind<void>(mem_fn(&KuduScanNode::thread_available_cb), this, _1));
  thread_available_cb(state->resource_pool());
  return Status::OK();
}

Status KuduScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  DCHECK(row_batch != NULL);
  // RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  // RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(_runtime_profile->total_time_counter());
  SCOPED_TIMER(materialize_tuple_timer());

  if (reached_limit() || _scan_tokens.empty()) {
    *eos = true;
    return Status::OK();
  }

  *eos = false;
  RowBatch* materialized_batch = _materialized_row_batches->GetBatch();
  if (materialized_batch != NULL) {
    row_batch->acquire_state(materialized_batch);
    _num_rows_returned += row_batch->num_rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    if (reached_limit()) {
      int num_rows_over = _num_rows_returned - _limit;
      row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
      _num_rows_returned -= num_rows_over;
      COUNTER_SET(_rows_returned_counter, _num_rows_returned);
      *eos = true;

      std::unique_lock<mutex> l(_lock);
      _done = true;
      _materialized_row_batches->shutdown();
    }
    delete materialized_batch;
  } else {
    *eos = true;
  }

  Status status;
  {
    std::unique_lock<mutex> l(_lock);
    status = _status;
  }
  return status;
}

Status KuduScanNode::close(RuntimeState* state) {
  if (is_closed()) {
    return Status::OK();
  }
  SCOPED_TIMER(_runtime_profile->total_time_counter());
  // PeriodicCounterUpdater::StopRateCounter(total_throughput_counter());
  // PeriodicCounterUpdater::StopTimeSeriesCounter(bytes_read_timeseries_counter_);
  // if (thread_avail_cb_id_ != -1) {
  //  state->resource_pool()->RemoveThreadAvailableCb(thread_avail_cb_id_);
  // }

  if (!_done) {
    std::unique_lock<mutex> l(_lock);
    _done = true;
    _materialized_row_batches->shutdown();
  }

  _scanner_threads.join_all();
  // scanner_threads_.JoinAll();
  DCHECK_EQ(_num_active_scanners, 0);
  _materialized_row_batches->Cleanup();
  ExecNode::close(state);

  return Status::OK();
}

void KuduScanNode::debug_string(int indentation_level, stringstream* out) const {
  string indent(indentation_level * 2, ' ');
  *out << indent << "KuduScanNode(tupleid=" << _tuple_id << ")";
}

const string* KuduScanNode::get_next_scan_token() {
  std::unique_lock<mutex> lock(_lock);
  if (_next_scan_token_idx >= _scan_tokens.size()) return NULL;
  const string* token = &_scan_tokens[_next_scan_token_idx++];
  return token;
}

Status KuduScanNode::get_conjunct_ctxs(vector<ExprContext*>* ctxs) {
  return Expr::clone_if_not_exists(_conjunct_ctxs, _runtime_state, ctxs);
}

void KuduScanNode::thread_available_cb(ThreadResourceMgr::ResourcePool* pool) {
  while (true) {
    std::unique_lock<mutex> lock(_lock);
    // All done or all tokens are assigned.
    if (_done || _next_scan_token_idx >= _scan_tokens.size()) {
        break;
    }

    // Check if we can get a token.
    // if (!pool->TryAcquireThreadToken()) break;

    ++_num_active_scanners;
    COUNTER_UPDATE(_num_scanner_threads_started_counter, 1);

    // Reserve the first token so no other thread picks it up.
    const string* token = &_scan_tokens[_next_scan_token_idx++];
    // string name = Substitute("scanner-thread($0)",
    //    _num_scanner_threads_started_counter->value());
    std::stringstream ss;
    ss << "scanner-thread(" << _num_scanner_threads_started_counter->value() << ")";
    std::string name = ss.str();

    VLOG_RPC << "Thread started: " << name;
    _scanner_threads.create_thread(
            boost::bind(&KuduScanNode::run_scanner_thread, this, name, token));
    // _scanner_threads.AddThread(new Thread("kudu-scan-node", name,
    //   &KuduScanNode::RunScannerThread, this, name, token));
  }
}

Status KuduScanNode::process_scan_token(KuduScanner* scanner, const string& scan_token) {
  RETURN_IF_ERROR(scanner->open_next_scan_token(scan_token));
  bool eos = false;
  while (!eos) {
    std::auto_ptr<RowBatch> row_batch(new RowBatch(
        row_desc(), _runtime_state->batch_size(), mem_tracker()));
    RETURN_IF_ERROR(scanner->get_next(row_batch.get(), &eos));
    while (!_done) {
      scanner->keep_kudu_scanner_alive();
      if (_materialized_row_batches->AddBatchWithTimeout(row_batch.get(), 1000000)) {
        row_batch.release();
        break;
      }
    }
  }
  if (eos) scan_ranges_complete_counter()->update(1);
  return Status::OK();
}

void KuduScanNode::run_scanner_thread(KuduScanNode *scanNode, const string& name, const string* initial_token)  {
    scanNode->run_scanner(name, initial_token);
}

void KuduScanNode::run_scanner(const string& name, const string* initial_token) {
  DCHECK(initial_token != NULL);
  // SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  // SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());
  // Set to true if this thread observes that the number of optional threads has been
  // exceeded and is exiting early.
  bool optional_thread_exiting = false;
  KuduScanner scanner(this, _runtime_state);

  const string* scan_token = initial_token;
  Status status = scanner.open();
  if (status.ok()) {
    while (!_done && scan_token != NULL) {
      status = process_scan_token(&scanner, *scan_token);
      if (!status.ok()) break;

      /*
      // Check if the number of optional threads has been exceeded.
      if (_runtime_state->resource_pool()->optional_exceeded()) {
        std::unique_lock<mutex> l(_lock);
        // Don't exit if this is the last thread. Otherwise, the scan will indicate it's
        // done before all scan tokens have been processed.
        if (_num_active_scanners > 1) {
          --_num_active_scanners;
          optional_thread_exiting = true;
          break;
        }
      }
      */
      scan_token = get_next_scan_token();
    }
  }
  scanner.close();

  {
    std::unique_lock<mutex> l(_lock);
    if (!status.ok()) {
      if (_status.ok()) {
        _status = status;
        _done = true;
      }
    }
    // Decrement num_active_scanners_ unless handling the case of an early exit when
    // optional threads have been exceeded, in which case it already was decremented.
    if (!optional_thread_exiting) --_num_active_scanners;
    if (_num_active_scanners == 0) {
      _done = true;
      _materialized_row_batches->shutdown();
    }
  }

  // lock_ is released before calling ThreadResourceMgr::ReleaseThreadToken() which
  // invokes ThreadAvailableCb() which attempts to take the same lock.
  VLOG_RPC << "Thread done: " << name;
  // runtime_state_->resource_pool()->release_thread_token(false);
}

}  // namespace impala
