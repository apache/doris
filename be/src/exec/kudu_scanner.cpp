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

#include "exec/kudu_scanner.h"

#include <kudu/client/row_result.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>
#include <string>
#include <chrono>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exec/kudu_util.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
// #include "gutil/gscoped_ptr.h"
// #include "gutil/strings/substitute.h"
// #include "util/jni-util.h"
// #include "util/periodic-counter-updater.h"
#include "util/runtime_profile.h"

//#include "common/names.h"

using kudu::client::KuduClient;
using kudu::client::KuduScanBatch;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;

namespace doris {

const string MODE_READ_AT_SNAPSHOT = "READ_AT_SNAPSHOT";

KuduScanner::KuduScanner(KuduScanNode* scan_node, RuntimeState* state)
  : _scan_node(scan_node),
    _state(state),
    _cur_kudu_batch_num_read(0),
    _last_alive_time_micros(0) {
}

Status KuduScanner::open() {
  return _scan_node->get_conjunct_ctxs(&_conjunct_ctxs);
}

void KuduScanner::keep_kudu_scanner_alive() {
  if (_scanner == NULL) return;
  // int64_t now = MonotonicMicros();
  int64_t now = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch()).count();

  int64_t keepalive_us = config::kudu_scanner_keep_alive_period_sec * 1e6;
  if (now < _last_alive_time_micros + keepalive_us) {
    return;
  }
  // If we fail to send a keepalive, it isn't a big deal. The Kudu
  // client code doesn't handle cross-replica failover or retries when
  // the server is busy, so it's better to just ignore errors here. In
  // the worst case, we will just fail next time we try to fetch a batch
  // if the scan is unrecoverable.
  kudu::Status s = _scanner->KeepAlive();
  if (!s.ok()) {
    VLOG(1) << "Unable to keep the Kudu scanner alive: " << s.ToString();
    return;
  }
  _last_alive_time_micros = now;
}

Status KuduScanner::get_next(RowBatch* row_batch, bool* eos) {
  int64_t tuple_buffer_size = 0;
  uint8_t* tuple_buffer = NULL;
  RETURN_IF_ERROR(
      row_batch->resize_and_allocate_tuple_buffer(_state, &tuple_buffer_size, &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);

  // Main scan loop:
  // Tries to fill 'row_batch' with rows from cur_kudu_batch_.
  // If there are no rows to decode, tries to get the next row batch from kudu.
  // If this scanner has no more rows, the scanner is closed and eos is returned.
  while (!*eos) {
    RETURN_IF_CANCELLED(_state);

    if (_cur_kudu_batch_num_read < _cur_kudu_batch.NumRows()) {
      bool batch_done = false;
      RETURN_IF_ERROR(decode_rows_into_row_batch(row_batch, &tuple, &batch_done));
      if (batch_done) break;
    }

    if (_scanner->HasMoreRows()) {
      RETURN_IF_ERROR(get_next_scanner_batch());
      continue;
    }

    close_current_client_scanner();
    *eos = true;
  }
  return Status::OK();
}

void KuduScanner::close() {
  if (_scanner) close_current_client_scanner();
  Expr::close(_conjunct_ctxs, _state);
}

Status KuduScanner::open_next_scan_token(const string& scan_token)  {
  DCHECK(_scanner == NULL);
  kudu::client::KuduScanner* scanner;
  KUDU_RETURN_IF_ERROR(kudu::client::KuduScanToken::DeserializeIntoScanner(
      _scan_node->kudu_client(), scan_token, &scanner),
      "Unable to deserialize scan token");
  _scanner.reset(scanner);

  if (UNLIKELY(config::pick_only_leaders_for_tests)) {
    KUDU_RETURN_IF_ERROR(_scanner->SetSelection(kudu::client::KuduClient::LEADER_ONLY),
                         "Could not set replica selection.");
  }
  kudu::client::KuduScanner::ReadMode mode =
      MODE_READ_AT_SNAPSHOT == config::kudu_read_mode ?
          kudu::client::KuduScanner::READ_AT_SNAPSHOT :
          kudu::client::KuduScanner::READ_LATEST;
  KUDU_RETURN_IF_ERROR(_scanner->SetReadMode(mode), "Could not set scanner ReadMode");
  KUDU_RETURN_IF_ERROR(_scanner->SetTimeoutMillis(config::kudu_operation_timeout_ms),
      "Could not set scanner timeout");
  VLOG_ROW << "Starting KuduScanner with ReadMode=" << mode << " timeout=" <<
      config::kudu_operation_timeout_ms;

  {
    // SCOPED_TIMER(_state->total_storage_wait_timer());
    KUDU_RETURN_IF_ERROR(_scanner->Open(), "Unable to open scanner");
  }
  return Status::OK();
}

void KuduScanner::close_current_client_scanner() {
  DCHECK_NOTNULL(_scanner.get());
  _scanner->Close();
  _scanner.reset();
}

Status KuduScanner::handle_empty_projection(RowBatch* row_batch, bool* batch_done) {
  int num_rows_remaining = _cur_kudu_batch.NumRows() - _cur_kudu_batch_num_read;
  int rows_to_add = std::min(row_batch->capacity() - row_batch->num_rows(),
      num_rows_remaining);
  _cur_kudu_batch_num_read += rows_to_add;
  row_batch->commit_rows(rows_to_add);
  // If we've reached the capacity, or the LIMIT for the scan, return.
  if (row_batch->at_capacity() || _scan_node->reached_limit()) {
    *batch_done = true;
  }
  return Status::OK();
}

Status KuduScanner::decode_rows_into_row_batch(RowBatch* row_batch, Tuple** tuple_mem,
    bool* batch_done) {
  *batch_done = false;

  // Short-circuit the count(*) case.
  if (_scan_node->tuple_desc()->slots().empty()) {
    return handle_empty_projection(row_batch, batch_done);
  }

  // Iterate through the Kudu rows, evaluate conjuncts and deep-copy survivors into
  // 'row_batch'.
  bool has_conjuncts = !_conjunct_ctxs.empty();
  int num_rows = _cur_kudu_batch.NumRows();
  for (int krow_idx = _cur_kudu_batch_num_read; krow_idx < num_rows; ++krow_idx) {
    // Evaluate the conjuncts that haven't been pushed down to Kudu. Conjunct evaluation
    // is performed directly on the Kudu tuple because its memory layout is identical to
    // Impala's. We only copy the surviving tuples to Impala's output row batch.
    KuduScanBatch::RowPtr krow = _cur_kudu_batch.Row(krow_idx);
    Tuple* kudu_tuple = reinterpret_cast<Tuple*>(const_cast<void*>(krow.cell(0)));
    ++_cur_kudu_batch_num_read;
    if (has_conjuncts && !ExecNode::eval_conjuncts(&_conjunct_ctxs[0],
        _conjunct_ctxs.size(), reinterpret_cast<TupleRow*>(&kudu_tuple))) {
      continue;
    }
    // Deep copy the tuple, set it in a new row, and commit the row.
    kudu_tuple->deep_copy(*tuple_mem, *_scan_node->tuple_desc(),
        row_batch->tuple_data_pool());
    TupleRow* row = row_batch->get_row(row_batch->add_row());
    row->set_tuple(0, *tuple_mem);
    row_batch->commit_last_row();
    // If we've reached the capacity, or the LIMIT for the scan, return.
    if (row_batch->at_capacity() || _scan_node->reached_limit()) {
      *batch_done = true;
      break;
    }
    // Move to the next tuple in the tuple buffer.
    *tuple_mem = next_tuple(*tuple_mem);
  }
  ExprContext::free_local_allocations(_conjunct_ctxs);

  // Check the status in case an error status was set during conjunct evaluation.
  //return _state->get_query_status();
  return Status::OK();
}

Status KuduScanner::get_next_scanner_batch() {
  // SCOPED_TIMER(_state->total_storage_wait_timer());
  // int64_t now = MonotonicMicros();
  int64_t now = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch()).count();
  KUDU_RETURN_IF_ERROR(_scanner->NextBatch(&_cur_kudu_batch), "Unable to advance iterator");
  COUNTER_UPDATE(_scan_node->kudu_round_trips(), 1);
  _cur_kudu_batch_num_read = 0;
  COUNTER_UPDATE(_scan_node->rows_read_counter(), _cur_kudu_batch.NumRows());
  _last_alive_time_micros = now;
  return Status::OK();
}

}  // namespace impala
