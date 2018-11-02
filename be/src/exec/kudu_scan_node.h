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

#ifndef IMPALA_EXEC_KUDU_SCAN_NODE_H_
#define IMPALA_EXEC_KUDU_SCAN_NODE_H_

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <gtest/gtest.h>
#include <kudu/client/client.h>

#include "exec/scan_node.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "common/status.h"
// #include "runtime/thread-resource-mgr.h"
// #include "gutil/gscoped_ptr.h"
// #include "util/thread.h"

namespace doris {

class KuduScanner;

/// A scan node that scans a Kudu table.
///
/// This takes a set of serialized Kudu scan tokens which encode the information needed
/// for this scan. A Kudu client deserializes the tokens into kudu scanners, and those
/// are used to retrieve the rows for this scan.
class KuduScanNode : public ScanNode {
 public:
  KuduScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~KuduScanNode();

  virtual Status prepare(RuntimeState* state);
  virtual Status open(RuntimeState* state);
  virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status close(RuntimeState* state);

 protected:
  virtual void debug_string(int indentation_level, std::stringstream* out) const;

 private:
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);

    friend class KuduScanner;

  kudu::client::KuduClient* kudu_client() { return _client.get(); }

  /// Tuple id resolved in Prepare() to set tuple_desc_.
  const TupleId _tuple_id;

  RuntimeState* _runtime_state;

  /// Descriptor of tuples read from Kudu table.
  const TupleDescriptor* _tuple_desc;

  /// The Kudu client and table. Scanners share these instances.
  kudu::client::sp::shared_ptr<kudu::client::KuduClient> _client;
  kudu::client::sp::shared_ptr<kudu::client::KuduTable> _table;

  /// Set of scan tokens to be deserialized into Kudu scanners.
  std::vector<std::string> _scan_tokens;

  /// The next index in 'scan_tokens_' to be assigned. Protected by lock_.
  int _next_scan_token_idx;

  // Outgoing row batches queue. Row batches are produced asynchronously by the scanner
  // threads and consumed by the main thread.
  boost::scoped_ptr<RowBatchQueue> _materialized_row_batches;

  /// Protects access to state accessed by scanner threads, such as 'status_' or
  /// 'num_active_scanners_'.
  boost::mutex _lock;

  /// The current status of the scan, set to non-OK if any problems occur, e.g. if an
  /// error occurs in a scanner.
  /// Protected by lock_
  Status _status;

  /// Number of active running scanner threads.
  /// Protected by lock_
  int _num_active_scanners;

  /// Set to true when the scan is complete (either because all scan tokens have been
  /// processed, the limit was reached or some error occurred).
  /// Protected by lock_
  volatile bool _done;

  /// Thread group for all scanner worker threads
  // TODO(dhc): change boost::thread_group to impala::thread_group
  // ThreadGroup scanner_threads_;
  boost::thread_group _scanner_threads;

  RuntimeProfile::Counter* _kudu_round_trips;
  RuntimeProfile::Counter* _kudu_remote_tokens;
  static const std::string KUDU_ROUND_TRIPS;
  static const std::string KUDU_REMOTE_TOKENS;

  /// The id of the callback added to the thread resource manager when a thread
  /// is available. Used to remove the callback before this scan node is destroyed.
  /// -1 if no callback is registered.
  int _thread_avail_cb_id;

  /// Called when scanner threads are available for this scan node. This will
  /// try to spin up as many scanner threads as the quota allows.
  void thread_available_cb(ThreadResourceMgr::ResourcePool* pool);

  /// Main function for scanner thread which executes a KuduScanner. Begins by processing
  /// 'initial_token', and continues processing scan tokens returned by
  /// 'GetNextScanToken()' until there are none left, an error occurs, or the limit is
  /// reached.
  static void run_scanner_thread(
        KuduScanNode *scanNode, const string& name, const string* initial_token);

  void run_scanner(const string& name, const string* initial_token);
  // void run_scanner_thread(const std::string& name, const std::string* initial_token);

  /// Processes a single scan token. Row batches are fetched using 'scanner' and enqueued
  /// in 'materialized_row_batches_' until the scanner reports eos, an error occurs, or
  /// the limit is reached.
  Status process_scan_token(KuduScanner* scanner, const std::string& scan_token);

  /// Returns the next scan token. Thread safe. Returns NULL if there are no more scan
  /// tokens.
  const std::string* get_next_scan_token();

  const TupleDescriptor* tuple_desc() const { return _tuple_desc; }

  // Returns a cloned copy of the scan node's conjuncts. Requires that the expressions
  // have been open previously.
  Status get_conjunct_ctxs(vector<ExprContext*>* ctxs);

  RuntimeProfile::Counter* kudu_round_trips() const { return _kudu_round_trips; }
};

}

#endif
