// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/olap_scan_node.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <sstream>
#include <iostream>
#include <utility>
#include <string>

#include "codegen/llvm_codegen.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "exprs/binary_predicate.h"
#include "exprs/in_predicate.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/thread_pool.hpp"
#include "util/debug_util.h"
#include "agent/cgroups_mgr.h"
#include "common/resource_tls.h"
#include "olap/olap_reader.h"
#include <boost/variant.hpp>

using llvm::Function;

namespace palo {

#define DS_SUCCESS(x) ((x) >= 0)

OlapScanNode::OlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs):
        ScanNode(pool, tnode, descs),
        _thrift_plan_node(new TPlanNode(tnode)),
        _tuple_id(tnode.olap_scan_node.tuple_id),
        _olap_scan_node(tnode.olap_scan_node),
        _tuple_desc(NULL),
        _tuple_idx(0),
        _eos(false),
        _scanner_pool(new ObjectPool()),
        _max_materialized_row_batches(config::palo_scanner_queue_size),
        _start(false),
        _scanner_done(false),
        _transfer_done(false),
        _use_pushdown_conjuncts(true),
        _wait_duration(0, 0, 1, 0),
        _status(Status::OK),
        _resource_info(nullptr),
        _buffered_bytes(0),
        _running_thread(0),
        _eval_conjuncts_fn(nullptr) {
}

OlapScanNode::~OlapScanNode() {
}

Status OlapScanNode::init(const TPlanNode& tnode) {
    RETURN_IF_ERROR(ExecNode::init(tnode));
    _direct_conjunct_size = _conjunct_ctxs.size();

    if (tnode.olap_scan_node.__isset.sort_column) {
        _sort_column = tnode.olap_scan_node.sort_column;
        _is_result_order = true;
        LOG(INFO) << "SortColumn: " << _sort_column;
    } else {
        _is_result_order = false;
    }
    return Status::OK;
}

Status OlapScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::prepare(state));
    // create scanner profile
    _scanner_profile = state->obj_pool()->add(
        new RuntimeProfile(state->obj_pool(), "OlapScanner"));
    _runtime_profile->add_child(_scanner_profile, true, nullptr);
    OLAPReader::init_profile(_scanner_profile);
    // create timer
    _olap_thread_scan_timer = ADD_TIMER(_runtime_profile, "OlapScanTime");
    _eval_timer = ADD_TIMER(_runtime_profile, "EvalTime");
    _merge_timer = ADD_TIMER(_runtime_profile, "SortMergeTime");
    _pushdown_return_counter =
        ADD_COUNTER(runtime_profile(), "PushDownFilterReturnCount ", TUnit::UNIT);
    _direct_return_counter =
        ADD_COUNTER(runtime_profile(), "DirectFilterReturnCount ", TUnit::UNIT);
    _tablet_counter =
        ADD_COUNTER(runtime_profile(), "TabletCount ", TUnit::UNIT);

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == NULL) {
        // TODO: make sure we print all available diagnostic output to our error log
        return Status("Failed to get tuple descriptor.");
    }

    const std::vector<SlotDescriptor*>& slots = _tuple_desc->slots();

    for (int i = 0; i < slots.size(); ++i) {
        if (!slots[i]->is_materialized()) {
            continue;
        }

        if (!slots[i]->type().is_string_type()) {
            continue;
        }

        _string_slots.push_back(slots[i]);
    }

    if (state->codegen_level() > 0) {
        LlvmCodeGen* codegen = NULL;
        RETURN_IF_ERROR(state->get_codegen(&codegen));
        Function* codegen_eval_conjuncts_fn = codegen_eval_conjuncts(state, _conjunct_ctxs);
        if (codegen_eval_conjuncts_fn != NULL) {
            codegen->add_function_to_jit(codegen_eval_conjuncts_fn,
                                         reinterpret_cast<void**>(&_eval_conjuncts_fn));
            // AddRuntimeExecOption("Probe Side Codegen Enabled");
        }
    }

    _runtime_state = state;
    return Status::OK;
}

Status OlapScanNode::open(RuntimeState* state) {
    VLOG(1) << "OlapScanNode::Open";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));

    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        // if conjunct is constant, compute direct and set eos = true
        if (_conjunct_ctxs[conj_idx]->root()->is_constant()) {
            void* value = _conjunct_ctxs[conj_idx]->get_value(NULL);
            if (value == NULL || *reinterpret_cast<bool*>(value) == false) {
                _eos = true;
            }
        }
    }

    _resource_info = ResourceTls::get_resource_tls();

    return Status::OK;
}

Status OlapScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    // check if Canceled.
    if (state->is_cancelled()) {
        boost::unique_lock<boost::mutex> l(_row_batches_lock);
        _transfer_done = true;
        boost::lock_guard<boost::mutex> guard(_status_mutex);
        _status = Status::CANCELLED;
        return _status;
    }

    if (_eos) {
        *eos = true;
        return Status::OK;
    }

    // check if started.
    if (!_start) {
        Status status = start_scan(state);

        if (!status.ok()) {
            LOG(ERROR) << "StartScan Failed cause " << status.get_error_msg();
            *eos = true;
            return status;
        }

        _start = true;
    }

    // wait for batch from queue
    RowBatch* materialized_batch = NULL;
    {
        boost::unique_lock<boost::mutex> l(_row_batches_lock);

        while (_materialized_row_batches.empty() && !_transfer_done) {
            if (state->is_cancelled()) {
                _transfer_done = true;
            }

            _row_batch_added_cv.timed_wait(l, _wait_duration);
        }

        if (!_materialized_row_batches.empty()) {
            materialized_batch = dynamic_cast<RowBatch*>(_materialized_row_batches.front());
            DCHECK(materialized_batch != NULL);
            _materialized_row_batches.pop_front();
        }
    }

    // return batch
    if (NULL != materialized_batch) {
        // notify scanner
        _row_batch_consumed_cv.notify_one();
        // get scanner's batch memory
        row_batch->acquire_state(materialized_batch);
        _num_rows_returned += row_batch->num_rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);

        // reach scan node limit
        if (reached_limit()) {
            int num_rows_over = _num_rows_returned - _limit;
            row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
            _num_rows_returned -= num_rows_over;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);

            {
                boost::unique_lock<boost::mutex> l(_row_batches_lock);
                _transfer_done = true;
            }

            _row_batch_consumed_cv.notify_all();
            *eos = true;
            LOG(INFO) << "OlapScanNode ReachedLimit.";
        } else {
            *eos = false;
        }

        if (VLOG_ROW_IS_ON) {
            for (int i = 0; i < row_batch->num_rows(); ++i) {
                TupleRow* row = row_batch->get_row(i);
                VLOG_ROW << "OlapScanNode output row: "
                    << print_tuple(row->get_tuple(0), *_tuple_desc);
            }
        }
        __sync_fetch_and_sub(&_buffered_bytes,
                             row_batch->tuple_data_pool()->total_reserved_bytes());

        delete materialized_batch;
        return Status::OK;
    }

    // all scanner done, change *eos to true
    *eos = true;
    boost::lock_guard<boost::mutex> guard(_status_mutex);
    return _status;
}

Status OlapScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK;
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));

    // change done status
    {
        boost::unique_lock<boost::mutex> l(_row_batches_lock);
        _transfer_done = true;
    }
    // notify all scanner thread
    _row_batch_consumed_cv.notify_all();
    _row_batch_added_cv.notify_all();
    _scan_batch_added_cv.notify_all();

    // join transfer thread
    _transfer_thread.join_all();

    // clear some row batch in queue
    for (auto row_batch : _materialized_row_batches) {
        delete row_batch;
    }

    _materialized_row_batches.clear();

    for (auto row_batch : _scan_row_batches) {
        delete row_batch;
    }

    _scan_row_batches.clear();

    if (_is_result_order) {
        for (int i = 0; i < _merge_rowbatches.size(); ++i) {
            for (std::list<RowBatch*>::iterator it = _merge_rowbatches[i].begin();
                    it != _merge_rowbatches[i].end(); ++it) {
                delete *it;
            }
        }

        _merge_rowbatches.clear();

        for (int i = 0; i < _backup_rowbatches.size(); ++i) {
            if (_backup_rowbatches[i] != NULL) {
                delete _backup_rowbatches[i];
            }
        }
    }

    // OlapScanNode terminate by exception
    // so that initiative close the Scanner
    for (auto scanner : _all_olap_scanners) {
        scanner->close(state);
    }

    VLOG(1) << "OlapScanNode::close()";
    return ExecNode::close(state);
}

Status OlapScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    BOOST_FOREACH(const TScanRangeParams & scan_range, scan_ranges) {
        DCHECK(scan_range.scan_range.__isset.palo_scan_range);
        boost::shared_ptr<PaloScanRange> palo_scan_range(
            new PaloScanRange(scan_range.scan_range.palo_scan_range));
        RETURN_IF_ERROR(palo_scan_range->init());
        VLOG(1) << "palo_scan_range table=" << scan_range.scan_range.palo_scan_range.table_name <<
                " version" << scan_range.scan_range.palo_scan_range.version;
        _palo_scan_ranges.push_back(palo_scan_range);
        COUNTER_UPDATE(_tablet_counter, 1);
    }

    return Status::OK;
}

Status OlapScanNode::start_scan(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);

    VLOG(1) << "NormalizeConjuncts";
    // 1. Convert conjuncts to ColumnValueRange in each column
    RETURN_IF_ERROR(normalize_conjuncts());

    VLOG(1) << "BuildOlapFilters";
    // 2. Using ColumnValueRange to Build OlapEngine filters
    RETURN_IF_ERROR(build_olap_filters());

    VLOG(1) << "SelectScanRanges";
    // 3. Using `Partition Column`'s ColumnValueRange to select ScanRange
    RETURN_IF_ERROR(select_scan_ranges());

    VLOG(1) << "BuildScanKey";
    // 4. Using `Key Column`'s ColumnValueRange to split ScanRange to serval `Sub ScanRange`
    RETURN_IF_ERROR(build_scan_key());

    VLOG(1) << "SplitScanRange";
    // 5. Query OlapEngine to split `Sub ScanRange` to serval `Sub Sub ScanRange`
    RETURN_IF_ERROR(split_scan_range());

    VLOG(1) << "StartScanThread";
    // 6. Start multi thread to read serval `Sub Sub ScanRange`
    RETURN_IF_ERROR(start_scan_thread(state));

    return Status::OK;
}

Status OlapScanNode::normalize_conjuncts() {
    std::vector<SlotDescriptor*> slots = _tuple_desc->slots();

    for (int slot_idx = 0; slot_idx < slots.size(); ++slot_idx) {
        switch (slots[slot_idx]->type().type) {
            // TYPE_TINYINT use int32_t to present
            // because it's easy to convert to string for build Olap fetch Query
        case TYPE_TINYINT: {
            ColumnValueRange<int32_t> range(slots[slot_idx]->col_name(),
                                            slots[slot_idx]->type().type,
                                            std::numeric_limits<int8_t>::min(),
                                            std::numeric_limits<int8_t>::max());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_SMALLINT: {
            ColumnValueRange<int16_t> range(slots[slot_idx]->col_name(),
                                            slots[slot_idx]->type().type,
                                            std::numeric_limits<int16_t>::min(),
                                            std::numeric_limits<int16_t>::max());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_INT: {
            ColumnValueRange<int32_t> range(slots[slot_idx]->col_name(),
                                            slots[slot_idx]->type().type,
                                            std::numeric_limits<int32_t>::min(),
                                            std::numeric_limits<int32_t>::max());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_BIGINT: {
            ColumnValueRange<int64_t> range(slots[slot_idx]->col_name(),
                                            slots[slot_idx]->type().type,
                                            std::numeric_limits<int64_t>::min(),
                                            std::numeric_limits<int64_t>::max());
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_LARGEINT: {
            __int128 min = MIN_INT128;
            __int128 max = MAX_INT128;
            ColumnValueRange<__int128> range(slots[slot_idx]->col_name(),
                                             slots[slot_idx]->type().type,
                                             min,
                                             max);
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_CHAR:
        case TYPE_VARCHAR: 
        case TYPE_HLL: {
            static char min_char = 0x00;
            static char max_char = 0xff;
            ColumnValueRange<StringValue> range(slots[slot_idx]->col_name(),
                                                slots[slot_idx]->type().type,
                                                StringValue(&min_char, 1),
                                                StringValue(&max_char, 1));
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_DATE:
        case TYPE_DATETIME: {
            DateTimeValue max_value = DateTimeValue::datetime_max_value();
            DateTimeValue min_value = DateTimeValue::datetime_min_value();
            ColumnValueRange<DateTimeValue> range(slots[slot_idx]->col_name(),
                                                  slots[slot_idx]->type().type,
                                                  min_value,
                                                  max_value);
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        case TYPE_DECIMAL: {
            DecimalValue min = DecimalValue::get_min_decimal();
            DecimalValue max = DecimalValue::get_max_decimal();
            ColumnValueRange<DecimalValue> range(slots[slot_idx]->col_name(),
                                                 slots[slot_idx]->type().type,
                                                 min,
                                                 max);
            normalize_predicate(range, slots[slot_idx]);
            break;
        }

        default: {
            VLOG(2) << "Unsupport Normalize Slot [ColName="
                    << slots[slot_idx]->col_name() << "]";
            break;
        }
        }
    }

    return Status::OK;
}

Status OlapScanNode::build_olap_filters() {
    _olap_filter.clear();

    for (auto iter : _column_value_ranges) {
        ToOlapFilterVisitor visitor;
        boost::variant<std::list<TCondition>> filters;
        boost::apply_visitor(visitor, iter.second, filters);

        std::list<TCondition> new_filters = boost::get<std::list<TCondition>>(filters);
        if (new_filters.empty()) {
            continue;
        }

        for (auto filter : new_filters) {
           _olap_filter.push_back(filter);
        }
    }

    return Status::OK;
}

Status OlapScanNode::select_scan_ranges() {

    std::list<boost::shared_ptr<PaloScanRange> >::iterator scan_range_iter
        = _palo_scan_ranges.begin();

    while (scan_range_iter != _palo_scan_ranges.end()) {
        if (!select_scan_range(*scan_range_iter)) {
            if ((*scan_range_iter)->scan_range().partition_column_ranges.size() != 0) {
                VLOG(1) << "Remove ScanRange: ["
                        << (*scan_range_iter)->scan_range().partition_column_ranges[0].begin_key << ", "
                        << (*scan_range_iter)->scan_range().partition_column_ranges[0].end_key << ")";
            } else {
                VLOG(1) << "Remove ScanRange";
            }

            _palo_scan_ranges.erase(scan_range_iter++);
        } else {
            scan_range_iter++;
        }
    }

    return Status::OK;
}

Status OlapScanNode::build_scan_key() {
    const std::vector<std::string>& column_names = _olap_scan_node.key_column_name;
    const std::vector<TPrimitiveType::type>& column_types = _olap_scan_node.key_column_type;
    DCHECK(column_types.size() == column_names.size());

    // 1. construct scan key except last olap engine short key
    int order_column_index = -1;
    int column_index = 0;
    _scan_keys.set_is_convertible(limit() == -1);

    for (; column_index < column_names.size() && !_scan_keys.has_range_value(); ++column_index) {
        if (_is_result_order && _sort_column == column_names[column_index]) {
            order_column_index = column_index;
        }

        std::map<std::string, ColumnValueRangeType>::iterator column_range_iter
            = _column_value_ranges.find(column_names[column_index]);

        if (_column_value_ranges.end() == column_range_iter) {
            break;
        }

        ExtendScanKeyVisitor visitor(_scan_keys);
        RETURN_IF_ERROR(boost::apply_visitor(visitor, column_range_iter->second));
    }

    // 3. check order column
    if (_is_result_order && order_column_index == -1) {
        return Status("OlapScanNode unsupport order by " + _sort_column);
    }

    _scan_keys.debug();

    return Status::OK;
}

Status OlapScanNode::split_scan_range() {
    std::vector<OlapScanRange> sub_ranges;
    VLOG(1) << "_palo_scan_ranges.size()=" << _palo_scan_ranges.size();

    for (auto scan_range : _palo_scan_ranges) {
        sub_ranges.clear();
        RETURN_IF_ERROR(get_sub_scan_range(scan_range, &sub_ranges));

        for (auto sub_range : sub_ranges) {
            VLOG(1) << "SubScanKey=" << (sub_range.begin_include ? "[" : "(")
                    << OlapScanKeys::to_print_key(sub_range.begin_scan_range)
                    << " : " << OlapScanKeys::to_print_key(sub_range.end_scan_range) <<
                    (sub_range.end_include ? "]" : ")");
            _query_key_ranges.push_back(sub_range);
            _query_scan_ranges.push_back(scan_range);
        }
    }

    DCHECK(_query_key_ranges.size() == _query_scan_ranges.size());

    return Status::OK;
}

Status OlapScanNode::start_scan_thread(RuntimeState* state) {
    VLOG(1) << "Query ScanRange Num: " << _query_scan_ranges.size();

    // thread num
    if (0 == _query_scan_ranges.size()) {
        _transfer_done = true;
        return Status::OK;
    }

    int key_range_size = _query_key_ranges.size();
    int key_range_num_per_scanner = key_range_size / 64;

    if (0 == key_range_num_per_scanner) {
        key_range_num_per_scanner = 1;
    }

    // scan range per therad
    for (int i = 0; i < key_range_size;) {
        boost::shared_ptr<PaloScanRange> scan_range = _query_scan_ranges[i];
        std::vector<OlapScanRange> key_ranges;
        key_ranges.push_back(_query_key_ranges[i]);
        ++i;

        if (!_is_result_order) {
            for (int j = 1;
                    j < key_range_num_per_scanner
                    && i < key_range_size
                    && _query_scan_ranges[i] == _query_scan_ranges[i - 1]
                    && _query_key_ranges[i].end_include == _query_key_ranges[i - 1].end_include;
                    j++, i++) {
                key_ranges.push_back(_query_key_ranges[i]);
            }
        }

        OlapScanner* scanner = new OlapScanner(
            state,
            scan_range,
            key_ranges,
            _olap_filter,
            *_tuple_desc,
            _scanner_profile,
            _is_null_vector);
        scanner->set_aggregation(_olap_scan_node.is_preaggregation);

        _scanner_pool->add(scanner);
        _olap_scanners.push_back(scanner);
        _all_olap_scanners = _olap_scanners;
    }

    // init progress
    std::stringstream ss;
    ss << "ScanThread complete (node=" << id() << "):";
    _progress = ProgressUpdater(ss.str(), _olap_scanners.size(), 1);
    _progress.set_logging_level(1);

    if (_is_result_order) {
        _transfer_thread.add_thread(
            new boost::thread(
                &OlapScanNode::merge_transfer_thread, this, state));
    } else {
        _transfer_thread.add_thread(
            new boost::thread(
                &OlapScanNode::transfer_thread, this, state));
    }

    return Status::OK;
}

Status OlapScanNode::create_conjunct_ctxs(
        RuntimeState* state,
        std::vector<ExprContext*>* row_ctxs,
        std::vector<ExprContext*>* vec_ctxs,
        bool disable_codegen) {
    _direct_row_conjunct_size = -1;
    _direct_vec_conjunct_size = -1;
#if 0
    for (int i = 0; i < _conjunct_ctxs.size(); ++i) {
        if (/* _conjunct_ctxs[i]->is_vectorized() */false) {
            vec_expr->emplace_back();
            RETURN_IF_ERROR(Expr::copy_expr(_runtime_state->obj_pool(),
                                           _conjunct_ctxs[i],
                                           &vec_expr->back()));
            RETURN_IF_ERROR(Expr::prepare(vec_expr->back(),
                                          _runtime_state,
                                          row_desc(),
                                          disable_codegen));
            if (i >= _direct_conjunct_size) {
                vec_expr->back()->prepare_r();
                if (-1 == _direct_vec_conjunct_size) {
                    _direct_vec_conjunct_size = vec_expr->size() - 1;
                }
            }
        } else {
            row_expr->emplace_back();
            RETURN_IF_ERROR(Expr::copy_expr(_runtime_state->obj_pool(),
                                           _conjunct_ctxs[i],
                                           &row_expr->back()));
            RETURN_IF_ERROR(Expr::prepare(row_expr->back(),
                                          _runtime_state,
                                          row_desc(),
                                          disable_codegen));
            if (i >= _direct_conjunct_size) {
                row_expr->back()->prepare_r();
                if (-1 == _direct_row_conjunct_size) {
                    _direct_row_conjunct_size = row_expr->size() - 1;
                }
            }
        }
    }
#endif
    RETURN_IF_ERROR(Expr::clone_if_not_exists(_conjunct_ctxs, state, row_ctxs));

    if (-1 == _direct_vec_conjunct_size) {
        _direct_vec_conjunct_size = vec_ctxs->size();
    }
    if (-1 == _direct_row_conjunct_size) {
        _direct_row_conjunct_size = row_ctxs->size();
    }

    return Status::OK;
}

template<class T>
Status OlapScanNode::normalize_predicate(ColumnValueRange<T>& range, SlotDescriptor* slot) {
    // 1. Normalize InPredicate, add to ColumnValueRange
    RETURN_IF_ERROR(normalize_in_predicate(slot, &range));

    // 2. Normalize BinaryPredicate , add to ColumnValueRange
    RETURN_IF_ERROR(normalize_binary_predicate(slot, &range));

    // 3. Add range to Column->ColumnValueRange map
    _column_value_ranges[slot->col_name()] = range;

    return Status::OK;
}

static bool ignore_cast(SlotDescriptor* slot, Expr* expr) {
    if (slot->type().is_date_type() && expr->type().is_date_type()) {
        return true;
    }
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
        return true;
    }
    return false;
}

template<class T>
Status OlapScanNode::normalize_in_predicate(SlotDescriptor* slot, ColumnValueRange<T>* range) {
    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
        if (TExprOpcode::FILTER_IN == _conjunct_ctxs[conj_idx]->root()->op()) {
            InPredicate* pred = dynamic_cast<InPredicate*>(_conjunct_ctxs[conj_idx]->root());
            if (pred->is_not_in()) {
                continue;
            }

            if (Expr::type_without_cast(pred->get_child(0)) != TExprNodeType::SLOT_REF) {
                continue;
            }

            if (pred->get_child(0)->type() != slot->type()) {
                if (!ignore_cast(slot, pred->get_child(0))) {
                    continue;
                }
            }

            if (pred->is_not_in()) {
                continue;
            }
            std::vector<SlotId> slot_ids;

            if (1 == pred->get_child(0)->get_slot_ids(&slot_ids)) {
                // 1.1 Skip if slot not match conjunct
                if (slot_ids[0] != slot->id()) {
                    continue;
                }

                VLOG(1) << slot->col_name() << " fixed_values add num: "
                        << pred->hybird_set()->size();

                // 1.2 Skip if InPredicate value size larger then max_scan_key_num
                if (pred->hybird_set()->size() > config::palo_max_scan_key_num) {
                    LOG(WARNING) << "Predicate value num " << pred->hybird_set()->size()
                                 << " excede limit " << config::palo_max_scan_key_num;
                    continue;
                }

                // 1.3 Push InPredicate value into ColumnValueRange
                HybirdSetBase::IteratorBase* iter = pred->hybird_set()->begin();
                while (iter->has_next()) {
                    // column in (NULL,...) counldn't push down to OlapEngine
                    // so that discard whole ColumnValueRange
                    if (NULL == iter->get_value()) {
                        range->clear();
                        break;
                    }

                    switch (slot->type().type) {
                    case TYPE_TINYINT: {
                        int32_t v = *reinterpret_cast<int8_t*>(const_cast<void*>(iter->get_value()));
                        range->add_fixed_value(*reinterpret_cast<T*>(&v));
                        break;
                    }
                    case TYPE_DATE: {
                        DateTimeValue date_value =
                            *reinterpret_cast<const DateTimeValue*>(iter->get_value());
                        date_value.cast_to_date();
                        range->add_fixed_value(*reinterpret_cast<T*>(&date_value));
                        break;
                    }
                    case TYPE_DECIMAL:
                    case TYPE_LARGEINT:
                    case TYPE_CHAR:
                    case TYPE_VARCHAR:
	                case TYPE_HLL:
                    case TYPE_SMALLINT:
                    case TYPE_INT:
                    case TYPE_BIGINT:
                    case TYPE_DATETIME: {
                        range->add_fixed_value(*reinterpret_cast<T*>(const_cast<void*>(iter->get_value())));
                        break;
                    }
                    default: {
                        break;
                    }
                    }
                    iter->next();
                }
            }
        }

        // 2. Normalize eq conjuncts like 'where col = value'
        if (TExprNodeType::BINARY_PRED == _conjunct_ctxs[conj_idx]->root()->node_type()
                && FILTER_IN == to_olap_filter_type(_conjunct_ctxs[conj_idx]->root()->op(), false)) {
            Expr* pred = _conjunct_ctxs[conj_idx]->root();
            DCHECK(pred->get_num_children() == 2);

            for (int child_idx = 0; child_idx < 2; ++child_idx) {
                if (Expr::type_without_cast(pred->get_child(child_idx))
                        != TExprNodeType::SLOT_REF) {
                    continue;
                }
                if (pred->get_child(child_idx)->type() != slot->type()) {
                    if (!ignore_cast(slot, pred->get_child(child_idx))) {
                        continue;
                    }
                }

                std::vector<SlotId> slot_ids;
                if (1 == pred->get_child(child_idx)->get_slot_ids(&slot_ids)) {
                    if (slot_ids[0] != slot->id()) {
                        continue;
                    }

                    Expr* expr = pred->get_child(1 - child_idx);
                    if (!expr->is_constant()) {
                        continue;
                    }

                    void* value = _conjunct_ctxs[conj_idx]->get_value(expr, NULL);
                    // for case: where col = null
                    if (value == NULL) {
                        continue;
                    }

                    switch (slot->type().type) {
                    case TYPE_TINYINT: {
                        int32_t v = *reinterpret_cast<int8_t*>(value);
                        range->add_fixed_value(*reinterpret_cast<T*>(&v));
                        break;
                    }
                    case TYPE_DATE: {
                        DateTimeValue date_value =
                            *reinterpret_cast<DateTimeValue*>(value);
                        date_value.cast_to_date();
                        range->add_fixed_value(*reinterpret_cast<T*>(&date_value));
                        break;
                    }
                    case TYPE_DECIMAL:
                    case TYPE_CHAR:
                    case TYPE_VARCHAR:
                    case TYPE_HLL:
                    case TYPE_DATETIME:
                    case TYPE_SMALLINT:
                    case TYPE_INT:
                    case TYPE_BIGINT:
                    case TYPE_LARGEINT: {
                        range->add_fixed_value(*reinterpret_cast<T*>(value));
                        break;
                    }
                    default: {
                        LOG(WARNING) << "Normalize filter fail, Unsupport Primitive type. [type="
                                     << expr->type() << "]";
                        return Status("Normalize filter fail, Unsupport Primitive type");
                    }
                    }
                }
            }
        }
    }

    return Status::OK;
}

void OlapScanNode::construct_is_null_pred_in_where_pred(Expr* expr, SlotDescriptor* slot, std::string is_null_str) {
    if (Expr::type_without_cast(expr) != TExprNodeType::SLOT_REF) {
        return;
    }

    std::vector<SlotId> slot_ids;
    if (1 != expr->get_slot_ids(&slot_ids)) {
        return;
    }

    if (slot_ids[0] != slot->id()) {
        return;
    }
    TCondition is_null;
    is_null.column_name = slot->col_name();
    is_null.condition_op = "is";
    is_null.condition_values.push_back(is_null_str);
    _is_null_vector.push_back(is_null);
    return;
}

template<class T>
Status OlapScanNode::normalize_binary_predicate(SlotDescriptor* slot, ColumnValueRange<T>* range) {
    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        Expr *root_expr =  _conjunct_ctxs[conj_idx]->root();
        if (TExprNodeType::BINARY_PRED != root_expr->node_type()
                || FILTER_IN == to_olap_filter_type(root_expr->op(), false)
                || FILTER_NOT_IN == to_olap_filter_type(root_expr->op(), false)) {
            if (TExprNodeType::FUNCTION_CALL == root_expr->node_type()) {
                std::string is_null_str;
                if (root_expr->is_null_scalar_function(is_null_str)) {
                    construct_is_null_pred_in_where_pred(root_expr->get_child(0),
                        slot, is_null_str);
                }
            }
            continue;
        }

        Expr* pred = _conjunct_ctxs[conj_idx]->root();
        DCHECK(pred->get_num_children() == 2);

        for (int child_idx = 0; child_idx < 2; ++child_idx) {
            if (Expr::type_without_cast(pred->get_child(child_idx)) != TExprNodeType::SLOT_REF) {
                continue;
            }
            if (pred->get_child(child_idx)->type() != slot->type()) {
                if (!ignore_cast(slot, pred->get_child(child_idx))) {
                    continue;
                }
            }

            std::vector<SlotId> slot_ids;

            if (1 == pred->get_child(child_idx)->get_slot_ids(&slot_ids)) {
                if (slot_ids[0] != slot->id()) {
                    continue;
                }

                Expr* expr = pred->get_child(1 - child_idx);

                // for case: where col_a > col_b
                if (!expr->is_constant()) {
                    continue;
                }

                void* value = _conjunct_ctxs[conj_idx]->get_value(expr, NULL);
                // for case: where col > null
                if (value == NULL) {
                    continue;
                }

                switch (slot->type().type) {
                case TYPE_TINYINT: {
                    int32_t v = *reinterpret_cast<int8_t*>(value);
                    range->add_range(to_olap_filter_type(pred->op(), child_idx),
                                    *reinterpret_cast<T*>(&v));
                    break;
                }

                case TYPE_DATE: {
                    DateTimeValue date_value = *reinterpret_cast<DateTimeValue*>(value);
                    date_value.cast_to_date();
                    range->add_range(to_olap_filter_type(pred->op(), child_idx),
                                     *reinterpret_cast<T*>(&date_value));
                    break;
                }
                case TYPE_DECIMAL:
                case TYPE_CHAR:
                case TYPE_VARCHAR:
			    case TYPE_HLL:
                case TYPE_DATETIME:
                case TYPE_SMALLINT:
                case TYPE_INT:
                case TYPE_BIGINT:
                case TYPE_LARGEINT: {
                    range->add_range(to_olap_filter_type(pred->op(), child_idx),
                                    *reinterpret_cast<T*>(value));
                    break;
                }

                default: {
                    LOG(WARNING) << "Normalize filter fail, Unsupport Primitive type. [type="
                                 << expr->type() << "]";
                    return Status("Normalize filter fail, Unsupport Primitive type");
                }
                }

                VLOG(1) << slot->col_name() << " op: "
                        << static_cast<int>(to_olap_filter_type(pred->op(), child_idx))
                        << " value: " << *reinterpret_cast<T*>(value);
            }
        }
    }

    return Status::OK;
}

bool OlapScanNode::select_scan_range(boost::shared_ptr<PaloScanRange> scan_range) {
    std::map<std::string, ColumnValueRangeType>::iterator iter
        = _column_value_ranges.begin();

    while (iter != _column_value_ranges.end()) {
        // return false if it's partition column range has no intersection with
        // column range deduce by conjunct
        if (0 == scan_range->has_intersection(iter->first, iter->second)) {
            return false;
        }

        ++iter;
    }

    return true;
}

Status OlapScanNode::get_sub_scan_range(
    boost::shared_ptr<PaloScanRange> scan_range,
    std::vector<OlapScanRange>* sub_range) {
    std::vector<OlapScanRange> scan_key_range;
    RETURN_IF_ERROR(_scan_keys.get_key_range(&scan_key_range));

    if (_is_result_order ||
            limit() != -1 ||
            scan_key_range.size() > 64) {
        if (scan_key_range.size() != 0) {
            *sub_range = scan_key_range;
        } else { // [-oo, +oo]
            sub_range->resize(1);
        }
    } else {
        EngineMetaReader olap_meta_reader(scan_range);
        RETURN_IF_ERROR(olap_meta_reader.open());

        if (!olap_meta_reader.get_hints(
                    config::palo_scan_range_row_count,
                    _scan_keys.begin_include(),
                    _scan_keys.end_include(),
                    scan_key_range,
                    sub_range,
                    _scanner_profile).ok()) {
            if (scan_key_range.size() != 0) {
                *sub_range = scan_key_range;
            } else { // [-oo, +oo]
                sub_range->resize(1);
            }
        }

        RETURN_IF_ERROR(olap_meta_reader.close());
    }

    return Status::OK;
}

Status OlapScanNode::transfer_open_scanners(RuntimeState* state) {
    Status status = Status::OK;
    std::list<OlapScanner*>::iterator iter = _olap_scanners.begin();

    for (int i = 0; iter != _olap_scanners.end(); ++iter, ++i) {
        // 1.1 open each scanner
        status = (*iter)->open();

        if (!status.ok()) {
            return status;
        }

        status = create_conjunct_ctxs(
            state, (*iter)->row_conjunct_ctxs(), (*iter)->vec_conjunct_ctxs(), true);
        if (!status.ok()) {
            return status;
        }

        // 1.2 init result array
        (*iter)->set_id(i);
    }

    return status;
}

TransferStatus OlapScanNode::read_row_batch(RuntimeState* state) {
    // Get a RowBatch from the scanner which need to be read
    RowBatch* scan_batch = NULL;
    int cur_id = 0;

    while (true) {
        boost::unique_lock<boost::mutex> l(_scan_batches_lock);

        if (UNLIKELY(_transfer_done)) {
            while (!_scanner_done) {
                _scan_batch_added_cv.wait(l);
            }

            return FININSH;
        }

        while (LIKELY(!_scan_row_batches.empty())) {
            scan_batch = dynamic_cast<RowBatch*>(_scan_row_batches.front());
            _scan_row_batches.pop_front();
            DCHECK(scan_batch != NULL);

            // push RowBatch into scanner result array
            VLOG(1) << "Push RowBatch " << scan_batch->scanner_id();
            _merge_rowbatches[scan_batch->scanner_id()].push_back(scan_batch);
        }

        if (-1 == _merge_scanner_id) {
            for (; cur_id < _merge_rowbatches.size(); ++cur_id) {
                if (_merge_rowbatches[cur_id].empty()) {
                    // this scanner has finished
                    if (_fin_olap_scanners[cur_id] == NULL) {
                        break;
                    }
                }
            }

            if (cur_id == _merge_rowbatches.size()) {
                return INIT_HEAP;
            }
        } else {
            if (_merge_rowbatches[_merge_scanner_id].empty()
                    && _fin_olap_scanners[_merge_scanner_id] != NULL) {
                _scanner_fin_flags[_merge_scanner_id] = true;
                return MERGE;
            } else if (!_merge_rowbatches[_merge_scanner_id].empty()) {
                return MERGE;
            }
        }

        if (!_olap_scanners.empty()) {
            std::list<OlapScanner*>::iterator iter = _olap_scanners.begin();

            while (iter != _olap_scanners.end()) {
                if (-1 == _merge_scanner_id || _merge_scanner_id == (*iter)->id()) {
                    PriorityThreadPool::Task task;
                    task.work_function = boost::bind(&OlapScanNode::scanner_thread, this, *iter);
                    task.priority = _nice;
                    if (state->exec_env()->thread_pool()->offer(task)) {
                        _olap_scanners.erase(iter++);
                    } else {
                        LOG(FATAL) << "Failed to assign scanner task to thread pool!";
                    }
                } else {
                    ++iter;
                }
                ++_total_assign_num;
            }
            // scanner_row_num = 16k
            // 16k * 10 * 12 * 8 = 15M(>2s)  --> nice=10
            // 16k * 20 * 22 * 8 = 55M(>6s)  --> nice=0
            while (_nice > 0
                       && _total_assign_num > (22 - _nice) * (20 - _nice) * 6) {
                --_nice;
            }
        }

        // 2.2 wait when all scanner are running & no result in queue
        int completed = _progress.num_complete();
        while (completed == _progress.num_complete()
                && _scan_row_batches.empty()
                && !_scanner_done) {
            _scan_batch_added_cv.wait(l);
        }
    }
}

TransferStatus OlapScanNode::init_merge_heap(Heap& heap) {
    for (int i = 0; i < _merge_rowbatches.size(); ++i) {
        if (!_merge_rowbatches[i].empty()) {
            Tuple* tuple = _merge_rowbatches[i].front()->get_row(
                               _merge_row_idxs[i])->get_tuple(_tuple_idx);

            if (VLOG_ROW_IS_ON) {
                VLOG_ROW << "SortMerge input row: " << print_tuple(tuple, *_tuple_desc);
            }

            ++_merge_row_idxs[i];
            HeapType v;
            v.tuple = tuple;
            v.id = i;
            heap.push(v);
        }
    }

    return BUILD_ROWBATCH;
}

TransferStatus OlapScanNode::build_row_batch(RuntimeState* state) {
    // _merge_rowbatch = new RowBatch(this->row_desc(), state->batch_size(), mem_tracker());
    _merge_rowbatch = new RowBatch(
            this->row_desc(), state->batch_size(), state->fragment_mem_tracker());
    uint8_t* tuple_buf = _merge_rowbatch->tuple_data_pool()->allocate(
                             state->batch_size() * _tuple_desc->byte_size());
    DCHECK(tuple_buf != NULL);
    //bzero(tuple_buf, state->batch_size() * _tuple_desc->byte_size());
    _merge_tuple = reinterpret_cast<Tuple*>(tuple_buf);
    return MERGE;
}

TransferStatus OlapScanNode::sorted_merge(Heap& heap) {
    ScopedTimer<MonotonicStopWatch> merge_timer(_merge_timer);

    while (true) {
        if (heap.empty()) {
            return FININSH;
        }

        // 1. Break if RowBatch is Full, Try to read new RowBatch
        if (_merge_rowbatch->is_full()) {
            return ADD_ROWBATCH;
        }

        // 2. Check top tuple's scanner has rowbatch in result vec
        HeapType v = heap.top();
        Tuple* pop_tuple = v.tuple;
        _merge_scanner_id = v.id;

        if (!_scanner_fin_flags[_merge_scanner_id]) {
            if (_merge_row_idxs[_merge_scanner_id]
                    >= _merge_rowbatches[_merge_scanner_id].front()->num_rows()) {
                if (_backup_rowbatches[_merge_scanner_id] != NULL) {
                    delete _backup_rowbatches[_merge_scanner_id];
                }

                _backup_rowbatches[_merge_scanner_id]
                    = _merge_rowbatches[_merge_scanner_id].front();

                VLOG(1) << "Pop RowBatch " << _merge_scanner_id;
                _merge_rowbatches[_merge_scanner_id].pop_front();
                _merge_row_idxs[_merge_scanner_id] = 0;
            }

            if (_merge_rowbatches[_merge_scanner_id].empty()) {
                return READ_ROWBATCH;
            }
        }

        pop_tuple->deep_copy(_merge_tuple, *_tuple_desc, _merge_rowbatch->tuple_data_pool(), false);
        // 3. Get top tuple of heap and push into new rowbatch
        heap.pop();

        if (VLOG_ROW_IS_ON) {
            VLOG_ROW << "SortMerge output row: " << print_tuple(_merge_tuple, *_tuple_desc);
        }

        int row_idx = _merge_rowbatch->add_row();
        TupleRow* row = _merge_rowbatch->get_row(row_idx);
        row->set_tuple(_tuple_idx, _merge_tuple);
        _merge_rowbatch->commit_last_row();

        char* new_tuple = reinterpret_cast<char*>(_merge_tuple);
        new_tuple += _tuple_desc->byte_size();
        _merge_tuple = reinterpret_cast<Tuple*>(new_tuple);

        // 4. push scanner's next tuple into heap
        if (!_scanner_fin_flags[_merge_scanner_id]) {
            Tuple* push_tuple = _merge_rowbatches[_merge_scanner_id].front()->get_row(
                                    _merge_row_idxs[_merge_scanner_id])->get_tuple(_tuple_idx);
            ++_merge_row_idxs[_merge_scanner_id];

            v.tuple = push_tuple;

            if (VLOG_ROW_IS_ON) {
                VLOG_ROW << "SortMerge input row: " << print_tuple(v.tuple, *_tuple_desc);
            }

            heap.push(v);
        }
    }
}

void OlapScanNode::merge_transfer_thread(RuntimeState* state) {
    // 1. Prepare to Start MergeTransferThread
    VLOG(1) << "MergeTransferThread Start.";
    Status status = Status::OK;

    // 1.1 scanner open
    status = transfer_open_scanners(state);

    // 1.2 find sort column
    const std::vector<SlotDescriptor*>& slots = _tuple_desc->slots();
    int i = 0;

    while (i < slots.size()) {
        if (slots[i]->col_name() == _sort_column) {
            VLOG(1) << "Sort Slot: " << slots[i]->debug_string();
            break;
        }

        ++i;
    }

    if (i >= slots.size()) {
        status = Status("Counldn't find sort column");
    }

    // 2. Merge ScannerThread' result
    if (status.ok()) {
        // 2.1 init data structure
        _merge_scanner_id = -1;
        Heap heap(MergeComparison(get_compare_func(slots[i]->type().type), slots[i]->tuple_offset()));

        _merge_rowbatches.resize(_olap_scanners.size());
        _merge_row_idxs.resize(_olap_scanners.size(), 0);
        _fin_olap_scanners.resize(_olap_scanners.size(), NULL);
        _scanner_fin_flags.resize(_olap_scanners.size(), false);
        _backup_rowbatches.resize(_olap_scanners.size(), NULL);
        _total_assign_num = 0;
        _nice = 20;

        // 2.2 read from scanner and order by _sort_column
        TransferStatus transfer_status = READ_ROWBATCH;
        bool flag = true;

        // 1. read one row_batch from each scanner
        // 2. use each row_batch' first tuple_row to build heap
        // 3. pop one tuple_row & push one tuple_row from same row_batch
        //  3.1 if row_batch is empty, read one row_batch from corresponding scanner
        //  3.2 if scanner is finish, just pop the tuple_row without push
        // 4. finish when heap is empty
        while (flag) {
            switch (transfer_status) {
            case INIT_HEAP:
                transfer_status = init_merge_heap(heap);
                break;

            case READ_ROWBATCH:
                transfer_status = read_row_batch(state);
                break;

            case BUILD_ROWBATCH:
                transfer_status = build_row_batch(state);
                break;

            case MERGE:
                transfer_status = sorted_merge(heap);
                break;

            case ADD_ROWBATCH:
                add_one_batch(_merge_rowbatch);
                transfer_status = BUILD_ROWBATCH;
                break;

            case FININSH:
                add_one_batch(_merge_rowbatch);
                flag = false;
                break;

            default:
                DCHECK(false);
                break;
            }
        }
    } else {
        boost::lock_guard<boost::mutex> guard(_status_mutex);
        _status = status;
    }

    VLOG(1) << "MergeTransferThread finish.";
    boost::unique_lock<boost::mutex> l(_row_batches_lock);
    _transfer_done = true;
    _row_batch_added_cv.notify_all();
}

void OlapScanNode::transfer_thread(RuntimeState* state) {
    Status status = Status::OK;

    // scanner open pushdown to scanThread
    std::list<OlapScanner*>::iterator iter = _olap_scanners.begin();

    for (; iter != _olap_scanners.end(); ++iter) {
        status = create_conjunct_ctxs(
            state, (*iter)->row_conjunct_ctxs(), (*iter)->vec_conjunct_ctxs(), true);
        if (!status.ok()) {
            boost::lock_guard<boost::mutex> guard(_status_mutex);
            _status = status;
            break;
        }
    }

    /*********************************
     * 优先级调度基本策略:
     * 1. 通过查询拆分的Range个数来确定初始nice值
     *    Range个数越多，越倾向于认定为大查询，nice值越小
     * 2. 通过查询累计读取的数据量来调整nice值
     *    读取的数据越多，越倾向于认定为大查询，nice值越小
     * 3. 通过nice值来判断查询的优先级
     *    nice值越大的，越优先获得的查询资源
     * 4. 定期提高队列内残留任务的优先级，避免大查询完全饿死
     *********************************/
    PriorityThreadPool* thread_pool = state->exec_env()->thread_pool();
    _total_assign_num = 0;
    _nice = 18 + std::max(0, 2 - (int)_olap_scanners.size() / 5);
    std::list<OlapScanner*> olap_scanners;

    int64_t mem_limit = 512 * 1024 * 1024;
    // TODO(zc): use memory limit
    int64_t mem_consume = __sync_fetch_and_add(&_buffered_bytes, 0);
    if (state->fragment_mem_tracker() != nullptr) {
        mem_limit = state->fragment_mem_tracker()->limit();
        mem_consume = state->fragment_mem_tracker()->consumption();
    }
    int max_thread = _max_materialized_row_batches;
    if (config::palo_scanner_row_num > state->batch_size()) {
        max_thread /= config::palo_scanner_row_num / state->batch_size();
    }
    // read from scanner
    while (LIKELY(status.ok())) {
        int assigned_thread_num = 0;
        // copy to local
        {
            boost::unique_lock<boost::mutex> l(_scan_batches_lock);
            assigned_thread_num = _running_thread;
            // int64_t buf_bytes = __sync_fetch_and_add(&_buffered_bytes, 0);
            // How many thread can apply to this query
            size_t thread_slot_num = 0;
            mem_consume = __sync_fetch_and_add(&_buffered_bytes, 0);
            if (state->fragment_mem_tracker() != nullptr) {
                mem_consume = state->fragment_mem_tracker()->consumption();
            }
            if (mem_consume < (mem_limit * 6) / 10) {
                thread_slot_num = max_thread - assigned_thread_num;
            } else {
                // Memory already exceed
                if (_scan_row_batches.empty()) {
                    // NOTE(zc): here need to lock row_batches_lock_
                    //  be worried about dead lock, so don't check here
                    // if (materialized_row_batches_.empty()) {
                    //     LOG(FATAL) << "Scan_row_batches_ and materialized_row_batches_"
                    //         " are empty when memory exceed";
                    // }
                    // Just for notify if scan_row_batches_ is empty and no running thread
                    if (assigned_thread_num == 0) {
                        thread_slot_num = 1;
                        // NOTE: if olap_scanners_ is empty, scanner_done_ should be true
                    }
                }
            }
            thread_slot_num = std::min(thread_slot_num, _olap_scanners.size());
            for (int i = 0; i < thread_slot_num; ++i) {
                olap_scanners.push_back(_olap_scanners.front());
                _olap_scanners.pop_front();
                _running_thread++;
                assigned_thread_num++;
            }
        }

        iter = olap_scanners.begin();
        while (iter != olap_scanners.end()) {
            PriorityThreadPool::Task task;
            task.work_function = boost::bind(&OlapScanNode::scanner_thread, this, *iter);
            task.priority = _nice;
            if (thread_pool->offer(task)) {
                olap_scanners.erase(iter++);
            } else {
                LOG(FATAL) << "Failed to assign scanner task to thread pool!";
            }
            ++_total_assign_num;
        }

        RowBatchInterface* scan_batch = NULL;
        {
            // 1 scanner idle task not empty, assign new sanner task
            boost::unique_lock<boost::mutex> l(_scan_batches_lock);

            // scanner_row_num = 16k
            // 16k * 10 * 12 * 8 = 15M(>2s)  --> nice=10
            // 16k * 20 * 22 * 8 = 55M(>6s)  --> nice=0
            while (_nice > 0
                       && _total_assign_num > (22 - _nice) * (20 - _nice) * 6) {
                --_nice;
            }

            // 2 wait when all scanner are running & no result in queue
            while (UNLIKELY(_running_thread == assigned_thread_num
                            && _scan_row_batches.empty()
                            && !_scanner_done)) {
                _scan_batch_added_cv.wait(l);
            }

            // 3 transfer result row batch when queue is not empty
            if (LIKELY(!_scan_row_batches.empty())) {
                scan_batch = _scan_row_batches.front();
                _scan_row_batches.pop_front();

                // delete scan_batch if transfer thread should be stoped
                // because scan_batch wouldn't be useful anymore
                if (UNLIKELY(_transfer_done)) {
                    delete scan_batch;
                    scan_batch = NULL;
                }
            } else {
                if (_scanner_done) {
                    break;
                }
            }
        }

        if (NULL != scan_batch) {
            add_one_batch(scan_batch);
        }
    }

    VLOG(1) << "TransferThread finish.";
    boost::unique_lock<boost::mutex> l(_row_batches_lock);
    _transfer_done = true;
    _row_batch_added_cv.notify_all();
}

void OlapScanNode::scanner_thread(OlapScanner* scanner) {
    Status status = Status::OK;
    bool eos = false;
    RuntimeState* state = scanner->runtime_state();
    DCHECK(NULL != state);
    if (!scanner->is_open()) {
        status = scanner->open();
        if (!status.ok()) {
            boost::lock_guard<boost::mutex> guard(_status_mutex);
            _status = status;
            eos = true;
        }
        scanner->set_opened();
    }

    // apply to cgroup
    if (_resource_info != nullptr) {
        CgroupsMgr::apply_cgroup(_resource_info->user, _resource_info->group);
    }

    std::vector<RowBatch*> row_batchs;
    std::vector<ExprContext*>* row_conjunct_ctxs = scanner->row_conjunct_ctxs();
    //std::vector<ExprContext*>* vec_conjunct_ctxs = scanner->vec_conjunct_ctxs();

    bool _use_pushdown_conjuncts = true;
    int64_t total_rows_reader_counter = 0;
    while (!eos && total_rows_reader_counter < config::palo_scanner_row_num) {
        // 1. Allocate one row batch
        // RowBatch *row_batch = new RowBatch(this->row_desc(), state->batch_size(), mem_tracker());
        RowBatch *row_batch = new RowBatch(
                this->row_desc(), state->batch_size(), _runtime_state->fragment_mem_tracker());
        row_batch->set_scanner_id(scanner->id());
        // 2. Allocate Row's Tuple buf
        uint8_t *tuple_buf = row_batch->tuple_data_pool()->allocate(
                state->batch_size() * _tuple_desc->byte_size());
        bzero(tuple_buf, state->batch_size() * _tuple_desc->byte_size());
        Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);

        int direct_return_counter = 0;
        int pushdown_return_counter = 0;
        int rows_read_counter = 0;
        // 3. Read data to each tuple
        while (true) {
            // 3.1 Break if RowBatch is Full, Try to read new RowBatch
            if (row_batch->is_full()) {
                break;
            }
            // 3.2 Stoped if Scanner has been cancelled
            if (UNLIKELY(_transfer_done)) {
                eos = true;
                status = Status::CANCELLED;
                LOG(INFO) << "Scan thread cancelled, "
                    "cause query done, maybe reach limit.";
                break;
            }
            // 3.3 Read tuple from OlapEngine
            status = scanner->get_next(tuple, &total_rows_reader_counter, &eos);
            if (UNLIKELY(!status.ok())) {
                LOG(ERROR) << "Scan thread read OlapScanner failed!";
                eos = true;
                break;
            }
            if (UNLIKELY(eos)) {
                // this scanner read all data, break;
                break;
            }

            if (VLOG_ROW_IS_ON) {
                VLOG_ROW << "OlapScanner input row: " << print_tuple(tuple, *_tuple_desc);
            }
            // 3.4 Set tuple to RowBatch(not commited)
            int row_idx = row_batch->add_row();
            TupleRow* row = row_batch->get_row(row_idx);
            row->set_tuple(_tuple_idx, tuple);

            do {
                // SCOPED_TIMER(_eval_timer);

                // 3.5.1 Using direct conjuncts to filter data
                if (_eval_conjuncts_fn != NULL) {
                    if (!_eval_conjuncts_fn(&((*row_conjunct_ctxs)[0]), _direct_row_conjunct_size, row)) {
                        // check direct conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        break;
                    }
                } else {
                    if (!eval_conjuncts(&((*row_conjunct_ctxs)[0]), _direct_row_conjunct_size, row)) {
                        // check direct conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        break;
                    }
                }


                ++direct_return_counter;

                // 3.5.2 Using pushdown conjuncts to filter data
                if (_use_pushdown_conjuncts
                        && row_conjunct_ctxs->size() > _direct_conjunct_size) {
                    if (!eval_conjuncts(&((*row_conjunct_ctxs)[_direct_conjunct_size]),
                                        row_conjunct_ctxs->size() - _direct_conjunct_size, row)) {
                        // check pushdown conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());

                        break;
                    }
                }

                int string_slots_size = _string_slots.size();
                for (int i = 0; i < string_slots_size; ++i) {
                    StringValue* slot = tuple->get_string_slot(_string_slots[i]->tuple_offset());
                    if (0 != slot->len) {
                        uint8_t* v = row_batch->tuple_data_pool()->allocate(slot->len);
                        memory_copy(v, slot->ptr, slot->len);
                        slot->ptr = reinterpret_cast<char*>(v);
                    }
                }

                if (VLOG_ROW_IS_ON) {
                    VLOG_ROW << "OlapScanner output row: " << print_tuple(tuple, *_tuple_desc);
                }

                // check direct && pushdown conjuncts success then commit tuple
                row_batch->commit_last_row();
                char* new_tuple = reinterpret_cast<char*>(tuple);
                new_tuple += _tuple_desc->byte_size();
                tuple = reinterpret_cast<Tuple*>(new_tuple);

                ++pushdown_return_counter;
            } while (0);

            ++rows_read_counter;
            if (total_rows_reader_counter >= config::palo_scanner_row_num) {
                break;
            }
        }


        COUNTER_UPDATE(_pushdown_return_counter, pushdown_return_counter);
        COUNTER_UPDATE(_direct_return_counter, direct_return_counter);
        COUNTER_UPDATE(this->rows_read_counter(), rows_read_counter);

        // 4. if status not ok, change status_.
        if (UNLIKELY(0 == row_batch->num_rows())) {
            // may be failed, push already, scan node delete this batch.
            delete row_batch;
            row_batch = NULL;
        } else {
            // compute pushdown conjuncts filter rate
            if (_use_pushdown_conjuncts) {
                int32_t pushdown_return_rate
                    = _pushdown_return_counter->value() * 100 / _direct_return_counter->value();
                if (pushdown_return_rate > config::palo_max_pushdown_conjuncts_return_rate) {
                    _use_pushdown_conjuncts = false;
                    VLOG(2) << "Stop Using PushDown Conjuncts. "
                        << "PushDownReturnRate: " << pushdown_return_rate << "%"
                        << " MaxPushDownReturnRate: "
                        << config::palo_max_pushdown_conjuncts_return_rate << "%";
                } else {
                    //VLOG(1) << "PushDownReturnRate: " << pushdown_return_rate << "%";
                }
            }
            row_batchs.push_back(row_batch);
            __sync_fetch_and_add(&_buffered_bytes,
                                 row_batch->tuple_data_pool()->total_reserved_bytes());
        }
    }


    // update raw rows number readed from tablet
    RuntimeProfile::Counter* raw_rows_counter = _scanner_profile->get_counter("RawRowsRead");
    if (raw_rows_counter != NULL) {
        COUNTER_UPDATE(raw_rows_counter, total_rows_reader_counter);
    }

    boost::unique_lock<boost::mutex> l(_scan_batches_lock);
    // if we failed, check status.
    if (UNLIKELY(!status.ok())) {
        _transfer_done = true;
        boost::lock_guard<boost::mutex> guard(_status_mutex);
        _status = status;
    }

    bool global_status_ok = false;
    {
        boost::lock_guard<boost::mutex> guard(_status_mutex);
        global_status_ok = _status.ok();
    }
    if (UNLIKELY(!global_status_ok)) {
        eos = true;
        BOOST_FOREACH(RowBatch* rb, row_batchs) {
            delete rb;
        }
    } else {
        BOOST_FOREACH(RowBatch* rb, row_batchs) {
            _scan_row_batches.push_back(rb);
        }
    }
    // Scanner thread completed. Take a look and update the status
    if (UNLIKELY(eos)) {
        _progress.update(1);
        if (_progress.done()) {
            // this is the right out
            _scanner_done = true;
        }
        if (_is_result_order) {
            _fin_olap_scanners[scanner->id()] = scanner;
        }
    } else {
        _olap_scanners.push_front(scanner);
    }
    _running_thread--;
    _scan_batch_added_cv.notify_one();
}

#if 0
void OlapScanNode::vectorized_scanner_thread(OlapScanner* scanner) {
    Status status = Status::OK;
    std::vector<RowBatch*> row_batchs;
    std::vector<ExprContext*>* row_conjunct_ctxs = scanner->row_conjunct_ctxs();
    std::vector<ExprContext*>* vec_conjunct_ctxs = scanner->vec_conjunct_ctxs();
    RuntimeState* state = scanner->runtime_state();
    DCHECK(NULL != state);

    // read from scanner
    int total_rows_reader_counter = 0;
    bool eos = false;
    bool _use_pushdown_conjuncts = true;
    DCHECK_GE(row_desc().tuple_descriptors().size(), 1);
    std::shared_ptr<VectorizedRowBatch> vectorized_row_batch(
        new VectorizedRowBatch(*(row_desc().tuple_descriptors()[0]), 1024));
    //vectorized_row_batch->mem_pool()->set_limits(*state->mem_trackers());

    do {
        // 1. Allocate one row batch
        RowBatch* row_batch = new RowBatch(row_desc(), state->batch_size());
        row_batch->tuple_data_pool()->set_limits(*state->mem_trackers());
        row_batch->set_scanner_id(scanner->id());

        // 3. Read data to each tuple
        while (true) {
            // 3.1 Break if RowBatch is Full, Try to read new RowBatch
            if (row_batch->is_full()) {
                break;
            }

            // 3.2 Stoped if Scanner has been cancelled
            if (UNLIKELY(_transfer_done)) {
                eos = true;
                status = Status::CANCELLED;
                LOG(INFO) << "Scan thread cancelled, "
                          "cause query done, maybe reach limit.";
                break;
            }

            // 3.3 Read vectorized_row_batch from OlapEngine
            if (vectorized_row_batch->is_iterator_end()) {
                if (total_rows_reader_counter >= config::palo_scanner_row_num) {
                    break;
                }
                status = scanner->get_next(vectorized_row_batch.get(), &eos);
                if (UNLIKELY(!status.ok())) {
                    LOG(ERROR) << "Scan thread read OlapScanner failed!";
                    eos = true;
                    break;
                }
                if (UNLIKELY(eos)) {
                    // this scanner read all data, break;
                    LOG(INFO) << "Scan thread read OlapScanner finish.";
                    break;
                }
                COUNTER_UPDATE(rows_read_counter(), vectorized_row_batch->num_rows());
                total_rows_reader_counter += vectorized_row_batch->num_rows();

                eval_vectorized_conjuncts(vectorized_row_batch, vec_conjunct_ctxs);
            }

            if (row_conjunct_ctxs->size() > 0) {
                eval_row_based_conjuncts(vectorized_row_batch, row_batch, row_conjunct_ctxs);
            } else {
                vectorized_row_batch->to_row_batch(row_batch);
            }

            if (VLOG_ROW_IS_ON) {
                for (int i = 0; i < row_batch->num_rows(); ++i) {
                    TupleRow* row = row_batch->get_row(i);
                    VLOG_ROW << "VectorizedScannerThread ouput row: " << print_row(row, row_desc());
                }
            }
        }

        // 4. if status not ok, change _status.
        if (UNLIKELY(0 == row_batch->num_rows())) {
            // may be failed, push already, scan node delete this batch.
            delete row_batch;
            row_batch = NULL;
        } else {
            // compute pushdown conjuncts filter rate
            if (_use_pushdown_conjuncts) {
                int32_t pushdown_return_rate
                    = _pushdown_return_counter->value() * 100 / _direct_return_counter->value();
                if (pushdown_return_rate > config::palo_max_pushdown_conjuncts_return_rate) {
                    _use_pushdown_conjuncts = false;
                    VLOG(2) << "Stop Using PushDown Conjuncts. "
                            << "PushDownReturnRate: " << pushdown_return_rate << "%"
                            << " MaxPushDownReturnRate: "
                            << config::palo_max_pushdown_conjuncts_return_rate << "%";
                } else {
                    VLOG(2) << "PushDownReturnRate: " << pushdown_return_rate << "%";
                }
            }
            row_batchs.push_back(row_batch);
        }
    } while ((total_rows_reader_counter < config::palo_scanner_row_num
                 || !vectorized_row_batch->is_iterator_end())
                 && !eos);

    boost::unique_lock<boost::mutex> l(_scan_batches_lock);
    // if we failed, check status.
    if (UNLIKELY(!status.ok())) {
        _transfer_done = true;
        _status = status;
    }
    if (UNLIKELY(!_status.ok())) {
        eos = true;
        BOOST_FOREACH(RowBatch* rb, row_batchs) {
            delete rb;
        }
    } else {
        BOOST_FOREACH(RowBatch* rb, row_batchs) {
            _scan_row_batches.push_back(rb);
        }
    }
    // Scanner thread completed. Take a look and update the status
    if (UNLIKELY(eos)) {
        _progress.update(1);
        if (_progress.done()) {
            // this is the right out
            _scanner_done = true;
        }
        if (_is_result_order) {
            _fin_olap_scanners[scanner->id()] = scanner;
        }
    } else {
        _olap_scanners.push_front(scanner);
    }
    _scan_batch_added_cv.notify_one();
}

void OlapScanNode::eval_vectorized_conjuncts(
        std::shared_ptr<VectorizedRowBatch> vectorized_row_batch,
        std::vector<ExprContext*>* vec_conjunct_ctxs) {
    for (int i = 0; i < _direct_vec_conjunct_size; ++i) {
        (*vec_conjunct_ctxs)[i]->evaluate(vectorized_row_batch.get());
    }
    COUNTER_UPDATE(_direct_return_counter, vectorized_row_batch->num_rows());

    if (_use_pushdown_conjuncts) {
        for (int i = _direct_vec_conjunct_size; i < vec_conjunct_ctxs->size(); ++i) {
            (*vec_conjunct_ctxs)[i]->evaluate(vectorized_row_batch.get());
        }
    }
    COUNTER_UPDATE(_pushdown_return_counter, vectorized_row_batch->num_rows());
}


void OlapScanNode::eval_row_based_conjuncts(
        std::shared_ptr<VectorizedRowBatch> vectorized_row_batch,
        RowBatch* row_batch,
        std::vector<ExprContext*>* row_conjunct_ctxs) {
    int row_remain = row_batch->capacity() - row_batch->num_rows();
    uint8_t* tuple_buf = row_batch->tuple_data_pool()->allocate(
            row_remain * _tuple_desc->byte_size());
    bzero(tuple_buf, row_remain * _tuple_desc->byte_size());
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buf);

    while (vectorized_row_batch->get_next_tuple(tuple)) {
        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        row->set_tuple(_tuple_idx, tuple);

        do {
            // 3.5.1 Using direct conjuncts to filter data
            if (!eval_conjuncts(&((*row_conjunct_ctxs)[0]),
                               _direct_row_conjunct_size,
                               row)) {
                // check direct conjuncts fail then clear tuple for reuse
                // make sure to reset null indicators since we're overwriting
                // the tuple assembled for the previous row
                tuple->init(_tuple_desc->byte_size());
                break;
            }

            COUNTER_UPDATE(_direct_return_counter, 1);

            // 3.5.2 Using pushdown conjuncts to filter data
            if (_use_pushdown_conjuncts
                    && row_conjunct_ctxs->size() > _direct_conjunct_size) {
                if (!eval_conjuncts(&((*row_conjunct_ctxs)[_direct_conjunct_size]),
                                   row_conjunct_ctxs->size() - _direct_conjunct_size, row)) {
                    // check pushdown conjuncts fail then clear tuple for reuse
                    // make sure to reset null indicators since we're overwriting
                    // the tuple assembled for the previous row
                    tuple->init(_tuple_desc->byte_size());
                    break;
                }
            }

            int string_slots_size = _string_slots.size();
            for (int i = 0; i < string_slots_size; ++i) {
                StringValue* slot
                    = tuple->get_string_slot(_string_slots[i]->tuple_offset());
                uint8_t* v = row_batch->tuple_data_pool()->allocate(slot->len);
                memcpy(v, slot->ptr, slot->len);
                slot->ptr = reinterpret_cast<char*>(v);
            }

            // check direct && pushdown conjuncts success then commit tuple
            row_batch->commit_last_row();
            char* new_tuple = reinterpret_cast<char*>(tuple);
            new_tuple += _tuple_desc->byte_size();
            tuple = reinterpret_cast<Tuple*>(new_tuple);

            COUNTER_UPDATE(_pushdown_return_counter, 1);
        } while (0);

        if (row_batch->is_full()) {
            break;
        }
    }
}
#endif

Status OlapScanNode::add_one_batch(RowBatchInterface* row_batch) {
    {
        boost::unique_lock<boost::mutex> l(_row_batches_lock);

        while (UNLIKELY(_materialized_row_batches.size()
                        >= _max_materialized_row_batches
                        && !_transfer_done)) {
            _row_batch_consumed_cv.wait(l);
        }

        VLOG(2) << "Push row_batch to materialized_row_batches";
        _materialized_row_batches.push_back(row_batch);
    }
    // remove one batch, notify main thread
    _row_batch_added_cv.notify_one();
    return Status::OK;
}

void OlapScanNode::debug_string(
    int /* indentation_level */,
    std::stringstream* /* out */) const {
}

} // namespace palo
