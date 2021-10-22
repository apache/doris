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

#ifndef BE_EXEC_ES_HTTP_SCAN_NODE_H
#define BE_EXEC_ES_HTTP_SCAN_NODE_H

#include <atomic>
#include <condition_variable>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "exec/es_http_scanner.h"
#include "exec/scan_node.h"
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

class RuntimeState;
class PartRangeKey;
class PartitionInfo;
class EsHttpScanCounter;
class EsPredicate;

class EsHttpScanNode : public ScanNode {
public:
    EsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~EsHttpScanNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    virtual Status close(RuntimeState* state) override;
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

    Status add_aggregation_op(string agg_name) {
        if (agg_name == "count") {
            _aggregate_ops.push_back(ScrollParser::EsAggregationOp::COUNT);
        } else if (agg_name == "min") {
            _aggregate_ops.push_back(ScrollParser::EsAggregationOp::MIN);
        } else if (agg_name == "max") {
            _aggregate_ops.push_back(ScrollParser::EsAggregationOp::MAX);
        } else if (agg_name == "sum") {
            _aggregate_ops.push_back(ScrollParser::EsAggregationOp::SUM);
        } else if (agg_name == "avg") {
            _aggregate_ops.push_back(ScrollParser::EsAggregationOp::AVG);
        } else {
            _aggregate_ops.push_back(ScrollParser::EsAggregationOp::OTHER);
            return Status::RuntimeError("the aggregate function is not supported, function name is:" + agg_name);
        }
        return Status::OK();
    }

    Status add_aggregation_for_es(std::string& col_name, ScrollParser::EsAggregationOp agg_op) {
        switch (agg_op) {
            case ScrollParser::EsAggregationOp::COUNT: {
                _aggregate_column_names.push_back(col_name);
                _aggregate_function_names.push_back("value_count");
                return Status::OK();
            }
            case ScrollParser::EsAggregationOp::MIN: {
                _aggregate_column_names.push_back(col_name);
                _aggregate_function_names.push_back("min");
                return Status::OK();
            }
            case ScrollParser::EsAggregationOp::MAX: {
                _aggregate_column_names.push_back(col_name);
                _aggregate_function_names.push_back("max");
                return Status::OK();
            }
            case ScrollParser::EsAggregationOp::SUM: {
                _aggregate_column_names.push_back(col_name);
                _aggregate_function_names.push_back("sum");
                return Status::OK();
            }
            case ScrollParser::EsAggregationOp::AVG: {
                _aggregate_column_names.push_back(col_name);
                _aggregate_function_names.push_back("sum");
                _aggregate_column_names.push_back(col_name);
                _aggregate_function_names.push_back("value_count");
                return Status::OK();
            }
            case ScrollParser::EsAggregationOp::COUNT_ONE_OR_STAR: {
                // wouldn't happen
                return Status::OK();
            }
            default: {
                // wouldn't happen
                return Status::RuntimeError("the aggregate function is not supported");
            }
        }
    }

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

    // Collect all scanners 's status
    Status collect_scanners_status();

    // One scanner worker, This scanner will handle 'length' ranges start from start_idx
    void scanner_worker(int start_idx, int length, std::promise<Status>& p_status);

    // Scan one range
    Status scanner_scan(std::unique_ptr<EsHttpScanner> scanner,
                        const std::vector<ExprContext*>& conjunct_ctxs, EsScanCounter* counter);

    Status build_conjuncts_list(const std::vector<ExprContext*>& conjunct_ctxs);

    TupleId _tuple_id;
    RuntimeState* _runtime_state;
    TupleDescriptor* _tuple_desc;

    int _num_running_scanners;
    std::atomic<bool> _scan_finished;
    bool _eos;
    int _max_buffered_batches;
    RuntimeProfile::Counter* _wait_scanner_timer;

    bool _all_scanners_finished;
    Status _process_status;

    std::vector<std::thread> _scanner_threads;
    std::vector<std::promise<Status>> _scanners_status;
    std::map<std::string, std::string> _properties;
    std::map<std::string, std::string> _docvalue_context;
    std::map<std::string, std::string> _fields_context;
    std::vector<TScanRangeParams> _scan_ranges;
    std::vector<std::string> _column_names;

    std::mutex _batch_queue_lock;
    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;
    std::deque<std::shared_ptr<RowBatch>> _batch_queue;
    std::vector<EsPredicate*> _predicates;

    std::vector<int> _predicate_to_conjunct;

    // for push down es aggregate
    bool _is_aggregated = false;

    std::vector<std::string> _aggregate_column_names;
    std::vector<std::string> _aggregate_function_names;
    std::vector<std::string> _group_by_column_names;
    std::vector<ScrollParser::EsAggregationOp> _aggregate_ops;

    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    std::unique_ptr<RowDescriptor> _scan_row_desc;
    std::vector<ExprContext*> _es_scan_conjunct_ctxs_when_aggregate;
};

} // namespace doris

#endif
