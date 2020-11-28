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

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "base_scanner.h"
#include "common/status.h"
#include "exec/scan_node.h"
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

class RuntimeState;
class PartRangeKey;
class PartitionInfo;
struct ScannerCounter;

class BrokerScanNode : public ScanNode {
public:
    BrokerScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~BrokerScanNode();

    // Called after create this scan node
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    // Prepare partition infos & set up timer
    virtual Status prepare(RuntimeState* state) override;

    // Start broker scan using ParquetScanner or BrokerScanner.
    virtual Status open(RuntimeState* state) override;

    // Fill the next row batch by calling next() on the scanner,
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

    // Close the scanner, and report errors.
    virtual Status close(RuntimeState* state) override;

    // No use
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

    // Called by broker scanners to get_partition_id
    // If there is no partition information, return -1
    // Return partition id if we find the partition match this row,
    // return -1, if there is no such partition.
    int64_t get_partition_id(const std::vector<ExprContext*>& partition_exprs, TupleRow* row) const;

protected:
    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    // Update process status to one failed status,
    // NOTE: Must hold the mutex of this scan node
    bool update_status(const Status& new_status) {
        if (_process_status.ok()) {
            _process_status = new_status;
            return true;
        }
        return false;
    }

    // Create scanners to do scan job
    Status start_scanners();

    // One scanner worker, This scanner will handle 'length' ranges start from start_idx
    void scanner_worker(int start_idx, int length);

    // Scan one range
    Status scanner_scan(const TBrokerScanRange& scan_range,
                        const std::vector<ExprContext*>& conjunct_ctxs,
                        const std::vector<ExprContext*>& partition_expr_ctxs,
                        ScannerCounter* counter);

    // Find partition id with PartRangeKey
    int64_t binary_find_partition_id(const PartRangeKey& key) const;

    std::unique_ptr<BaseScanner> create_scanner(const TBrokerScanRange& scan_range,
                                                ScannerCounter* counter);

private:
    TupleId _tuple_id;
    RuntimeState* _runtime_state;
    TupleDescriptor* _tuple_desc;
    std::map<std::string, SlotDescriptor*> _slots_map;
    std::vector<TScanRangeParams> _scan_ranges;

    std::mutex _batch_queue_lock;
    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;
    std::deque<std::shared_ptr<RowBatch>> _batch_queue;

    int _num_running_scanners;
    // Indicate if all scanners have been finished scan worker
    bool _all_scanners_finished;

    std::atomic<bool> _scan_finished;

    Status _process_status;

    std::vector<std::thread> _scanner_threads;

    int _max_buffered_batches;

    // Partition information
    std::vector<ExprContext*> _partition_expr_ctxs;
    std::vector<PartitionInfo*> _partition_infos;

    // Profile information
    //
    RuntimeProfile::Counter* _wait_scanner_timer;
};

} // namespace doris
