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

#ifndef DORIS_EXEC_KUDU_SCANNER_H
#define DORIS_EXEC_KUDU_SCANNER_H

#include <boost/scoped_ptr.hpp>
#include <kudu/client/client.h>

#include "exec/kudu_scan_node.h"
#include "runtime/descriptors.h"

namespace doris {

class MemPool;
class RowBatch;
class RuntimeState;
class Tuple;

/// Wraps a Kudu client scanner to fetch row batches from Kudu. The Kudu client scanner
/// is created from a scan token in OpenNextScanToken(), which then provides rows fetched
/// by GetNext() until it reaches eos, and the caller may open another scan token.
class KuduScanner {
 public:
  KuduScanner(KuduScanNode* scan_node, RuntimeState* state);

  /// Prepares this scanner for execution.
  /// Does not actually open a kudu::client::KuduScanner.
  Status open();

  /// Opens a new kudu::client::KuduScanner using 'scan_token'.
  Status open_next_scan_token(const std::string& scan_token);

  /// Fetches the next batch from the current kudu::client::KuduScanner.
  Status get_next(RowBatch* row_batch, bool* eos);

  /// Sends a "Ping" to the Kudu TabletServer servicing the current scan, if there is
  /// one. This serves the purpose of making the TabletServer keep the server side
  /// scanner alive if the batch queue is full and no batches can be queued. If there are
  /// any errors, they are ignored here, since we assume that we will just fail the next
  /// time we try to read a batch.
  void keep_kudu_scanner_alive();

  /// Closes this scanner.
  void close();

 private:
  /// Handles the case where the projection is empty (e.g. count(*)).
  /// Does this by adding sets of rows to 'row_batch' instead of adding one-by-one.
  Status handle_empty_projection(RowBatch* row_batch, bool* batch_done);

  /// Decodes rows previously fetched from kudu, now in 'cur_rows_' into a RowBatch.
  ///  - 'batch' is the batch that will point to the new tuples.
  ///  - *tuple_mem should be the location to output tuples.
  ///  - Sets 'batch_done' to true to indicate that the batch was filled to capacity or
  ///    the limit was reached.
  Status decode_rows_into_row_batch(RowBatch* batch, Tuple** tuple_mem, bool* batch_done);

  /// Fetches the next batch of rows from the current kudu::client::KuduScanner.
  Status get_next_scanner_batch();

  /// Closes the current kudu::client::KuduScanner.
  void close_current_client_scanner();

  inline Tuple* next_tuple(Tuple* t) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    return reinterpret_cast<Tuple*>(mem + _scan_node->tuple_desc()->byte_size());
  }

  KuduScanNode* _scan_node;
  RuntimeState* _state;

  /// The kudu::client::KuduScanner for the current scan token. A new KuduScanner is
  /// created for each scan token using KuduScanToken::DeserializeIntoScanner().
  boost::scoped_ptr<kudu::client::KuduScanner> _scanner;

  /// The current batch of retrieved rows.
  kudu::client::KuduScanBatch _cur_kudu_batch;

  /// The number of rows already read from cur_kudu_batch_.
  int _cur_kudu_batch_num_read;

  /// The last time a keepalive request or successful RPC was sent.
  int64_t _last_alive_time_micros;

  /// The scanner's cloned copy of the conjuncts to apply.
  std::vector<ExprContext*> _conjunct_ctxs;
};

} /// namespace impala

#endif /// IMPALA_EXEC_KUDU_SCANNER_H_
