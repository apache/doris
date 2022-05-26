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

#include "exec/es_http_scan_node.h"

#include <chrono>
#include <sstream>

#include "common/object_pool.h"
#include "exec/es/es_predicate.h"
#include "exec/es/es_query_builder.h"
#include "exec/es/es_scan_reader.h"
#include "exec/es/es_scroll_query.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/runtime_profile.h"

namespace doris {

EsHttpScanNode::EsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.es_scan_node.tuple_id),
          _runtime_state(nullptr),
          _tuple_desc(nullptr),
          _num_running_scanners(0),
          _scan_finished(false),
          _eos(false),
          _max_buffered_batches(1024),
          _wait_scanner_timer(nullptr) {}

EsHttpScanNode::~EsHttpScanNode() {}

Status EsHttpScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::init(tnode));

    // use TEsScanNode
    _properties = tnode.es_scan_node.properties;

    if (tnode.es_scan_node.__isset.docvalue_context) {
        _docvalue_context = tnode.es_scan_node.docvalue_context;
    }

    if (tnode.es_scan_node.__isset.fields_context) {
        _fields_context = tnode.es_scan_node.fields_context;
    }
    return Status::OK();
}

Status EsHttpScanNode::prepare(RuntimeState* state) {
    VLOG_QUERY << "EsHttpScanNode prepare";
    RETURN_IF_ERROR(ScanNode::prepare(state));

    _scanner_profile.reset(new RuntimeProfile("EsHttpScanNode"));
    runtime_profile()->add_child(_scanner_profile.get(), true, nullptr);

    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Failed to get tuple descriptor, _tuple_id=" << _tuple_id;
        return Status::InternalError(ss.str());
    }

    // set up column name vector for ESScrollQueryBuilder
    for (auto slot_desc : _tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        _column_names.push_back(slot_desc->col_name());
    }

    _wait_scanner_timer = ADD_TIMER(runtime_profile(), "WaitScannerTime");

    return Status::OK();
}

// build predicate
Status EsHttpScanNode::build_conjuncts_list() {
    Status status = Status::OK();
    _conjunct_to_predicate.resize(_conjunct_ctxs.size());

    for (int i = 0; i < _conjunct_ctxs.size(); ++i) {
        EsPredicate* predicate = _pool->add(new EsPredicate(_conjunct_ctxs[i], _tuple_desc, _pool));
        predicate->set_field_context(_fields_context);
        status = predicate->build_disjuncts_list();
        if (status.ok()) {
            _conjunct_to_predicate[i] = _predicate_to_conjunct.size();
            _predicate_to_conjunct.push_back(i);

            _predicates.push_back(predicate);
        } else {
            _conjunct_to_predicate[i] = -1;

            VLOG_CRITICAL << status.get_error_msg();
            status = predicate->get_es_query_status();
            if (!status.ok()) {
                LOG(WARNING) << status.get_error_msg();
                return status;
            }
        }
    }

    return Status::OK();
}

Status EsHttpScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);

    // if conjunct is constant, compute direct and set eos = true
    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        if (_conjunct_ctxs[conj_idx]->root()->is_constant()) {
            void* value = _conjunct_ctxs[conj_idx]->get_value(nullptr);
            if (value == nullptr || *reinterpret_cast<bool*>(value) == false) {
                _eos = true;
            }
        }
    }

    RETURN_IF_ERROR(build_conjuncts_list());

    // remove those predicates which ES cannot support
    std::vector<bool> list;
    BooleanQueryBuilder::validate(_predicates, &list);

    DCHECK(list.size() == _predicate_to_conjunct.size());
    for (int i = list.size() - 1; i >= 0; i--) {
        if (!list[i]) {
            _predicate_to_conjunct.erase(_predicate_to_conjunct.begin() + i);
            _predicates.erase(_predicates.begin() + i);
        }
    }

    // filter the conjuncts and ES will process them later
    for (int i = _predicate_to_conjunct.size() - 1; i >= 0; i--) {
        int conjunct_index = _predicate_to_conjunct[i];
        _conjunct_ctxs[conjunct_index]->close(_runtime_state);
        _conjunct_ctxs.erase(_conjunct_ctxs.begin() + conjunct_index);
    }

    auto checker = [&](int index) {
        return _conjunct_to_predicate[index] != -1 && list[_conjunct_to_predicate[index]];
    };
    std::string vconjunct_information = _peel_pushed_vconjunct(state, checker);
    _scanner_profile->add_info_string("VconjunctExprTree", vconjunct_information);

    RETURN_IF_ERROR(start_scanners());

    return Status::OK();
}

Status EsHttpScanNode::start_scanners() {
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        _num_running_scanners = _scan_ranges.size();
    }

    _scanners_status.resize(_scan_ranges.size());
    for (int i = 0; i < _scan_ranges.size(); i++) {
        _scanner_threads.emplace_back(&EsHttpScanNode::scanner_worker, this, i, _scan_ranges.size(),
                                      std::ref(_scanners_status[i]));
    }
    return Status::OK();
}

Status EsHttpScanNode::collect_scanners_status() {
    // NOTE. if open() was called, but set_range() was NOT called for some reason.
    // then close() was called.
    // there would cause a core because _scanners_status's iterator was in [0, _scan_ranges) other than [0, _scanners_status)
    // it is said that the fragment-call-frame is calling scan-node in this way....
    // in my options, it's better fixed in fragment-call-frame. e.g. call close() according the return value of open()
    for (int i = 0; i < _scanners_status.size(); i++) {
        std::future<Status> f = _scanners_status[i].get_future();
        RETURN_IF_ERROR(f.get());
    }
    return Status::OK();
}

Status EsHttpScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        if (update_status(Status::Cancelled("Cancelled"))) {
            _queue_writer_cond.notify_all();
        }
    }

    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    if (_scan_finished.load()) {
        *eos = true;
        return Status::OK();
    }

    std::shared_ptr<RowBatch> scanner_batch;
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        while (_process_status.ok() && !_runtime_state->is_cancelled() &&
               _num_running_scanners > 0 && _batch_queue.empty()) {
            SCOPED_TIMER(_wait_scanner_timer);
            _queue_reader_cond.wait_for(l, std::chrono::seconds(1));
        }
        if (!_process_status.ok()) {
            // Some scanner process failed.
            return _process_status;
        }
        if (_runtime_state->is_cancelled()) {
            if (update_status(Status::Cancelled("Cancelled"))) {
                _queue_writer_cond.notify_all();
            }
            return _process_status;
        }
        if (!_batch_queue.empty()) {
            scanner_batch = _batch_queue.front();
            _batch_queue.pop_front();
        }
    }

    // All scanner has been finished, and all cached batch has been read
    if (scanner_batch == nullptr) {
        _scan_finished.store(true);
        *eos = true;
        return Status::OK();
    }

    // notify one scanner
    _queue_writer_cond.notify_one();

    // get scanner's batch memory
    row_batch->acquire_state(scanner_batch.get());
    _num_rows_returned += row_batch->num_rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    // This is first time reach limit.
    // Only valid when query 'select * from table1 limit 20'
    if (reached_limit()) {
        int num_rows_over = _num_rows_returned - _limit;
        row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
        _num_rows_returned -= num_rows_over;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);

        _scan_finished.store(true);
        _queue_writer_cond.notify_all();
        *eos = true;
    } else {
        *eos = false;
    }

    if (VLOG_ROW_IS_ON) {
        for (int i = 0; i < row_batch->num_rows(); ++i) {
            TupleRow* row = row_batch->get_row(i);
            VLOG_ROW << "EsHttpScanNode output row: "
                     << Tuple::to_string(row->get_tuple(0), *_tuple_desc);
        }
    }

    return Status::OK();
}

Status EsHttpScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _scan_finished.store(true);
    _queue_writer_cond.notify_all();
    _queue_reader_cond.notify_all();
    for (int i = 0; i < _scanner_threads.size(); ++i) {
        _scanner_threads[i].join();
    }

    _batch_queue.clear();

    //don't need to hold lock to update_status in close function
    //collect scanners status
    update_status(collect_scanners_status());

    //close exec node
    update_status(ExecNode::close(state));

    return _process_status;
}

// This function is called after plan node has been prepared.
Status EsHttpScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    _scan_ranges = scan_ranges;
    return Status::OK();
}

void EsHttpScanNode::debug_string(int ident_level, std::stringstream* out) const {
    (*out) << "EsHttpScanNode";
}

Status EsHttpScanNode::scanner_scan(std::unique_ptr<EsHttpScanner> scanner,
                                    const std::vector<ExprContext*>& conjunct_ctxs,
                                    EsScanCounter* counter) {
    RETURN_IF_ERROR(scanner->open());
    bool scanner_eof = false;

    while (!scanner_eof) {
        // Fill one row batch
        std::shared_ptr<RowBatch> row_batch(
                new RowBatch(row_desc(), _runtime_state->batch_size(), mem_tracker().get()));

        // create new tuple buffer for row_batch
        MemPool* tuple_pool = row_batch->tuple_data_pool();
        int tuple_buffer_size = row_batch->capacity() * _tuple_desc->byte_size();
        void* tuple_buffer = tuple_pool->allocate(tuple_buffer_size);
        if (tuple_buffer == nullptr) {
            return Status::InternalError("Allocate memory for row batch failed.");
        }

        Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
        while (!scanner_eof) {
            RETURN_IF_CANCELLED(_runtime_state);
            // If we have finished all works
            if (_scan_finished.load()) {
                return Status::OK();
            }

            // This row batch has been filled up, and break this
            if (row_batch->is_full()) {
                break;
            }

            int row_idx = row_batch->add_row();
            TupleRow* row = row_batch->get_row(row_idx);
            // scan node is the first tuple of tuple row
            row->set_tuple(0, tuple);
            memset(tuple, 0, _tuple_desc->num_null_bytes());

            // Get from scanner
            RETURN_IF_ERROR(scanner->get_next(tuple, tuple_pool, &scanner_eof, _docvalue_context));
            if (scanner_eof) {
                continue;
            }

            // eval conjuncts of this row.
            if (eval_conjuncts(&conjunct_ctxs[0], conjunct_ctxs.size(), row)) {
                row_batch->commit_last_row();
                char* new_tuple = reinterpret_cast<char*>(tuple);
                new_tuple += _tuple_desc->byte_size();
                tuple = reinterpret_cast<Tuple*>(new_tuple);
                counter->num_rows_returned++;
            } else {
                counter->num_rows_filtered++;
            }
        }

        // Row batch has been filled, push this to the queue
        if (row_batch->num_rows() > 0) {
            std::unique_lock<std::mutex> l(_batch_queue_lock);
            while (_process_status.ok() && !_scan_finished.load() &&
                   !_runtime_state->is_cancelled() &&
                   _batch_queue.size() >= _max_buffered_batches) {
                _queue_writer_cond.wait_for(l, std::chrono::seconds(1));
            }
            // Process already set failed, so we just return OK
            if (!_process_status.ok()) {
                return Status::OK();
            }
            // Scan already finished, just return
            if (_scan_finished.load()) {
                return Status::OK();
            }
            // Runtime state is canceled, just return cancel
            if (_runtime_state->is_cancelled()) {
                return Status::Cancelled("Cancelled");
            }
            // Queue size Must be smaller than _max_buffered_batches
            _batch_queue.push_back(row_batch);

            // Notify reader to
            _queue_reader_cond.notify_one();
        }
    }

    return Status::OK();
}

// Prefer to the local host
static std::string get_host_port(const std::vector<TNetworkAddress>& es_hosts) {
    std::string host_port;
    std::string localhost = BackendOptions::get_localhost();

    TNetworkAddress host = es_hosts[0];
    for (auto& es_host : es_hosts) {
        if (es_host.hostname == localhost) {
            host = es_host;
            break;
        }
    }

    host_port = host.hostname;
    host_port += ":";
    host_port += std::to_string(host.port);
    return host_port;
}

void EsHttpScanNode::scanner_worker(int start_idx, int length, std::promise<Status>& p_status) {
    // Clone expr context
    std::vector<ExprContext*> scanner_expr_ctxs;
    DCHECK(start_idx < length);
    auto status = Expr::clone_if_not_exists(_conjunct_ctxs, _runtime_state, &scanner_expr_ctxs);
    if (!status.ok()) {
        LOG(WARNING) << "Clone conjuncts failed.";
    }

    EsScanCounter counter;
    const TEsScanRange& es_scan_range = _scan_ranges[start_idx].scan_range.es_scan_range;

    // Collect the information from scan range to properties
    std::map<std::string, std::string> properties(_properties);
    properties[ESScanReader::KEY_INDEX] = es_scan_range.index;
    if (es_scan_range.__isset.type) {
        properties[ESScanReader::KEY_TYPE] = es_scan_range.type;
    }
    properties[ESScanReader::KEY_SHARD] = std::to_string(es_scan_range.shard_id);
    properties[ESScanReader::KEY_BATCH_SIZE] = std::to_string(_runtime_state->batch_size());
    properties[ESScanReader::KEY_HOST_PORT] = get_host_port(es_scan_range.es_hosts);
    // push down limit to Elasticsearch
    // if predicate in _conjunct_ctxs can not be processed by Elasticsearch, we can not push down limit operator to Elasticsearch
    if (limit() != -1 && limit() <= _runtime_state->batch_size() && _conjunct_ctxs.empty()) {
        properties[ESScanReader::KEY_TERMINATE_AFTER] = std::to_string(limit());
    }

    bool doc_value_mode = false;
    properties[ESScanReader::KEY_QUERY] = ESScrollQueryBuilder::build(
            properties, _column_names, _predicates, _docvalue_context, &doc_value_mode);

    // start scanner to scan
    if (!_vectorized) {
        std::unique_ptr<EsHttpScanner> scanner(
                new EsHttpScanner(_runtime_state, runtime_profile(), _tuple_id, properties,
                                  scanner_expr_ctxs, &counter, doc_value_mode));
        status = scanner_scan(std::move(scanner), scanner_expr_ctxs, &counter);
    } else {
        std::unique_ptr<vectorized::VEsHttpScanner> scanner(new vectorized::VEsHttpScanner(
                _runtime_state, runtime_profile(), _tuple_id, properties, scanner_expr_ctxs,
                &counter, doc_value_mode));
        status = scanner_scan(std::move(scanner));
    }
    if (!status.ok()) {
        LOG(WARNING) << "Scanner[" << start_idx
                     << "] process failed. status=" << status.get_error_msg();
    }

    // scanner is going to finish
    {
        std::lock_guard<std::mutex> l(_batch_queue_lock);
        if (!status.ok()) {
            update_status(status);
        }
        // This scanner will finish
        _num_running_scanners--;
    }
    _queue_reader_cond.notify_all();
    // If one scanner failed, others don't need scan any more
    if (!status.ok()) {
        _queue_writer_cond.notify_all();
    }

    p_status.set_value(status);
}
} // namespace doris
