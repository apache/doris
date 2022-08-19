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

#pragma once

#include <memory>

#include "exec/es_http_scan_node.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"
#include "vec/exec/ves_http_scanner.h"

namespace doris {

class RuntimeState;
class Status;

namespace vectorized {

class VEsHttpScanNode : public ScanNode {
public:
    VEsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VEsHttpScanNode() = default;

    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented VEsHttpScanNode Node::get_next scalar");
    }
    virtual Status close(RuntimeState* state) override;
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const override;

    // Update process status to one failed status,
    // NOTE: Must hold the mutex of this scan node
    bool update_status(const Status& new_status) {
        if (_process_status.ok()) {
            _process_status = new_status;
            return true;
        }
        return false;
    }
    // One scanner worker, This scanner will handle 'length' ranges start from start_idx
    virtual void scanner_worker(int start_idx, int length, std::promise<Status>& p_status);

    TupleId _tuple_id;
    RuntimeState* _runtime_state;
    TupleDescriptor* _tuple_desc;

    int _num_running_scanners;
    std::atomic<bool> _scan_finished;
    bool _eos;
    int _max_buffered_batches;
    RuntimeProfile::Counter* _wait_scanner_timer;

    Status _process_status;

    std::map<std::string, std::string> _docvalue_context;

    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;

    // Create scanners to do scan job
    Status start_scanners();

    // Collect all scanners 's status
    Status collect_scanners_status();

    Status build_conjuncts_list();

    std::vector<std::thread> _scanner_threads;
    std::vector<std::promise<Status>> _scanners_status;
    std::map<std::string, std::string> _properties;
    std::map<std::string, std::string> _fields_context;
    std::vector<TScanRangeParams> _scan_ranges;
    std::vector<std::string> _column_names;

    std::mutex _batch_queue_lock;
    std::deque<std::shared_ptr<RowBatch>> _batch_queue;
    std::vector<EsPredicate*> _predicates;

    std::vector<int> _predicate_to_conjunct;
    std::vector<int> _conjunct_to_predicate;

    std::unique_ptr<RuntimeProfile> _scanner_profile;

    Status scanner_scan(std::unique_ptr<VEsHttpScanner> scanner);

    std::deque<std::shared_ptr<vectorized::Block>> _block_queue;
    std::mutex _block_queue_lock;
};
} // namespace vectorized
} // namespace doris