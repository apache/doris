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
#include "util/priority_thread_pool.hpp"
#include "agent/cgroups_mgr.h"
#include "common/resource_tls.h"
#include <boost/variant.hpp>

using llvm::Function;

namespace doris {

#define DS_SUCCESS(x) ((x) >= 0)

OlapScanNode::OlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs):
        ScanNode(pool, tnode, descs),
        _tuple_id(tnode.olap_scan_node.tuple_id),
        _olap_scan_node(tnode.olap_scan_node),
        _tuple_desc(NULL),
        _tuple_idx(0),
        _eos(false),
        _scanner_pool(new ObjectPool()),
        _max_materialized_row_batches(config::doris_scanner_queue_size),
        _start(false),
        _scanner_done(false),
        _transfer_done(false),
        _wait_duration(0, 0, 1, 0),
        _status(Status::OK()),
        _resource_info(nullptr),
        _buffered_bytes(0),
        _running_thread(0),
        _eval_conjuncts_fn(nullptr) {
}

OlapScanNode::~OlapScanNode() {
}

Status OlapScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _direct_conjunct_size = _conjunct_ctxs.size();

    if (tnode.olap_scan_node.__isset.sort_column) {
        _is_result_order = true;
    } else {
        _is_result_order = false;
    }

    // Before, we support scan data ordered, but is not used in production
    // Now, we drop this functional
    DCHECK(!_is_result_order) << "ordered result don't support any more";

    return Status::OK();
}

void OlapScanNode::_init_counter(RuntimeState* state) {
#if 0
    ADD_TIMER(profile, "GetTabletTime");
    ADD_TIMER(profile, "InitReaderTime");
    ADD_TIMER(profile, "ShowHintsTime");
    ADD_TIMER(profile, "BlockLoadTime");
    ADD_TIMER(profile, "IndexLoadTime");
    ADD_TIMER(profile, "VectorPredicateEvalTime");
    ADD_TIMER(profile, "ScannerTimer");
    ADD_TIMER(profile, "IOTimer");
    ADD_TIMER(profile, "DecompressorTimer");
    ADD_TIMER(profile, "RLETimer");

    ADD_COUNTER(profile, "RawRowsRead", TUnit::UNIT);
    ADD_COUNTER(profile, "IndexStreamCacheMiss", TUnit::UNIT);
    ADD_COUNTER(profile, "IndexStreamCacheHit", TUnit::UNIT);
    ADD_COUNTER(profile, "BlockLoadCount", TUnit::UNIT);
#endif
    ADD_TIMER(_runtime_profile, "ShowHintsTime");

    _reader_init_timer = ADD_TIMER(_runtime_profile, "ReaderInitTime");
    _read_compressed_counter =
        ADD_COUNTER(_runtime_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter =
        ADD_COUNTER(_runtime_profile, "UncompressedBytesRead", TUnit::BYTES);
    _block_load_timer = ADD_TIMER(_runtime_profile, "BlockLoadTime");
    _block_load_counter =
        ADD_COUNTER(_runtime_profile, "BlocksLoad", TUnit::UNIT);
    _block_fetch_timer =
        ADD_TIMER(_runtime_profile, "BlockFetchTime");
    _raw_rows_counter =
        ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _block_convert_timer = ADD_TIMER(_runtime_profile, "BlockConvertTime");
    _block_seek_timer = ADD_TIMER(_runtime_profile, "BlockSeekTime");

    _rows_vec_cond_counter =
        ADD_COUNTER(_runtime_profile, "RowsVectorPredFiltered", TUnit::UNIT);
    _vec_cond_timer =
        ADD_TIMER(_runtime_profile, "VectorPredEvalTime");

    _stats_filtered_counter =
        ADD_COUNTER(_runtime_profile, "RowsStatsFiltered", TUnit::UNIT);
    _del_filtered_counter =
        ADD_COUNTER(_runtime_profile, "RowsDelFiltered", TUnit::UNIT);

    _io_timer = ADD_TIMER(_runtime_profile, "IOTimer");
    _decompressor_timer = ADD_TIMER(_runtime_profile, "DecompressorTimer");
    _index_load_timer = ADD_TIMER(_runtime_profile, "IndexLoadTime");

    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
}

Status OlapScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::prepare(state));
    // create scanner profile
    // create timer
    _tablet_counter =
        ADD_COUNTER(runtime_profile(), "TabletCount ", TUnit::UNIT);
    _rows_pushed_cond_filtered_counter =
        ADD_COUNTER(_runtime_profile, "RowsPushedCondFiltered", TUnit::UNIT);
    _init_counter(state);

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == NULL) {
        // TODO: make sure we print all available diagnostic output to our error log
        return Status::InternalError("Failed to get tuple descriptor.");
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
    return Status::OK();
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

    return Status::OK();
}

Status OlapScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    // check if Canceled.
    if (state->is_cancelled()) {
        boost::unique_lock<boost::mutex> l(_row_batches_lock);
        _transfer_done = true;
        boost::lock_guard<boost::mutex> guard(_status_mutex);
        if (LIKELY(_status.ok())) {
            _status = Status::Cancelled("Cancelled");
        }
        return _status;
    }

    if (_eos) {
        *eos = true;
        return Status::OK();
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
                    << Tuple::to_string(row->get_tuple(0), *_tuple_desc);
            }
        }
        __sync_fetch_and_sub(&_buffered_bytes,
                             row_batch->tuple_data_pool()->total_reserved_bytes());

        delete materialized_batch;
        return Status::OK();
    }

    // all scanner done, change *eos to true
    *eos = true;
    boost::lock_guard<boost::mutex> guard(_status_mutex);
    return _status;
}

Status OlapScanNode::collect_query_statistics(QueryStatistics* statistics) {
    RETURN_IF_ERROR(ExecNode::collect_query_statistics(statistics));
    statistics->add_scan_bytes(_read_compressed_counter->value());
    statistics->add_scan_rows(_raw_rows_counter->value());
    return Status::OK();
}

Status OlapScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
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

    // OlapScanNode terminate by exception
    // so that initiative close the Scanner
    for (auto scanner : _all_olap_scanners) {
        scanner->close(state);
    }

    VLOG(1) << "OlapScanNode::close()";
    return ScanNode::close(state);
}

// PlanFragmentExecutor will call this method to set scan range
// Doris scan range is defined in thrift file like this
// struct TPaloScanRange {
//  1: required list<Types.TNetworkAddress> hosts
//  2: required string schema_hash
//  3: required string version
//  4: required string version_hash
//  5: required Types.TTabletId tablet_id
//  6: required string db_name
//  7: optional list<TKeyRange> partition_column_ranges
//  8: optional string index_name
//  9: optional string table_name
//}
// every doris_scan_range is related with one tablet so that one olap scan node contains multiple tablet
Status OlapScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        DCHECK(scan_range.scan_range.__isset.palo_scan_range);
        boost::shared_ptr<DorisScanRange> doris_scan_range(
            new DorisScanRange(scan_range.scan_range.palo_scan_range));
        RETURN_IF_ERROR(doris_scan_range->init());
        VLOG(1) << "doris_scan_range table=" << scan_range.scan_range.palo_scan_range.table_name <<
                " version" << scan_range.scan_range.palo_scan_range.version;
        _doris_scan_ranges.push_back(doris_scan_range);
        COUNTER_UPDATE(_tablet_counter, 1);
    }

    return Status::OK();
}

Status OlapScanNode::start_scan(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);

    VLOG(1) << "NormalizeConjuncts";
    // 1. Convert conjuncts to ColumnValueRange in each column
    RETURN_IF_ERROR(normalize_conjuncts());

    VLOG(1) << "BuildOlapFilters";
    // 2. Using ColumnValueRange to Build StorageEngine filters
    RETURN_IF_ERROR(build_olap_filters());

    VLOG(1) << "SelectScanRanges";
    // 3. Using `Partition Column`'s ColumnValueRange to select ScanRange
    RETURN_IF_ERROR(select_scan_ranges());

    VLOG(1) << "BuildScanKey";
    // 4. Using `Key Column`'s ColumnValueRange to split ScanRange to serval `Sub ScanRange`
    RETURN_IF_ERROR(build_scan_key());

    VLOG(1) << "SplitScanRange";
    // 5. Query StorageEngine to split `Sub ScanRange` to serval `Sub Sub ScanRange`
    RETURN_IF_ERROR(split_scan_range());

    VLOG(1) << "StartScanThread";
    // 6. Start multi thread to read serval `Sub Sub ScanRange`
    RETURN_IF_ERROR(start_scan_thread(state));

    return Status::OK();
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
                                                StringValue(&min_char, 0),
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

        case TYPE_DECIMALV2: {
            DecimalV2Value min = DecimalV2Value::get_min_decimal();
            DecimalV2Value max = DecimalV2Value::get_max_decimal();
            ColumnValueRange<DecimalV2Value> range(slots[slot_idx]->col_name(),
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

    return Status::OK();
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

    return Status::OK();
}

Status OlapScanNode::select_scan_ranges() {

    std::list<boost::shared_ptr<DorisScanRange> >::iterator scan_range_iter
        = _doris_scan_ranges.begin();

    while (scan_range_iter != _doris_scan_ranges.end()) {
        if (!select_scan_range(*scan_range_iter)) {
            if ((*scan_range_iter)->scan_range().partition_column_ranges.size() != 0) {
                VLOG(1) << "Remove ScanRange: ["
                        << (*scan_range_iter)->scan_range().partition_column_ranges[0].begin_key << ", "
                        << (*scan_range_iter)->scan_range().partition_column_ranges[0].end_key << ")";
            } else {
                VLOG(1) << "Remove ScanRange";
            }

            _doris_scan_ranges.erase(scan_range_iter++);
        } else {
            scan_range_iter++;
        }
    }

    return Status::OK();
}

Status OlapScanNode::build_scan_key() {
    const std::vector<std::string>& column_names = _olap_scan_node.key_column_name;
    const std::vector<TPrimitiveType::type>& column_types = _olap_scan_node.key_column_type;
    DCHECK(column_types.size() == column_names.size());

    // 1. construct scan key except last olap engine short key
    int column_index = 0;
    _scan_keys.set_is_convertible(limit() == -1);

    for (; column_index < column_names.size() && !_scan_keys.has_range_value(); ++column_index) {
        std::map<std::string, ColumnValueRangeType>::iterator column_range_iter
            = _column_value_ranges.find(column_names[column_index]);

        if (_column_value_ranges.end() == column_range_iter) {
            break;
        }

        ExtendScanKeyVisitor visitor(_scan_keys);
        RETURN_IF_ERROR(boost::apply_visitor(visitor, column_range_iter->second));
    }

    _scan_keys.debug();

    return Status::OK();
}

Status OlapScanNode::split_scan_range() {
    std::vector<OlapScanRange> sub_ranges;
    VLOG(1) << "_doris_scan_ranges.size()=" << _doris_scan_ranges.size();

    // doris scan range is related with one tablet
    // split scan range for every tablet
    for (auto scan_range : _doris_scan_ranges) {
        sub_ranges.clear();
        RETURN_IF_ERROR(get_sub_scan_range(scan_range, &sub_ranges));

        for (auto sub_range : sub_ranges) {
            VLOG(1) << "SubScanKey=" << (sub_range.begin_include ? "[" : "(")
                    << sub_range.begin_scan_range
                    << " : " << sub_range.end_scan_range <<
                    (sub_range.end_include ? "]" : ")");
            // just to get sub_range related scan_range? why not create a object?
            _query_key_ranges.push_back(sub_range);
            _query_scan_ranges.push_back(scan_range);
        }
    }

    DCHECK(_query_key_ranges.size() == _query_scan_ranges.size());

    return Status::OK();
}

Status OlapScanNode::start_scan_thread(RuntimeState* state) {
    VLOG(1) << "Query ScanRange Num: " << _query_scan_ranges.size();

    // thread num
    if (0 == _query_scan_ranges.size()) {
        _transfer_done = true;
        return Status::OK();
    }

    int key_range_size = _query_key_ranges.size();
    int key_range_num_per_scanner = key_range_size / 64;

    if (0 == key_range_num_per_scanner) {
        key_range_num_per_scanner = 1;
    }

    // scan range per therad
    for (int i = 0; i < key_range_size;) {
        boost::shared_ptr<DorisScanRange> scan_range = _query_scan_ranges[i];
        std::vector<OlapScanRange> key_ranges;
        key_ranges.push_back(_query_key_ranges[i]);
        ++i;

        for (int j = 1;
             j < key_range_num_per_scanner
             && i < key_range_size
             && _query_scan_ranges[i] == _query_scan_ranges[i - 1]
             && _query_key_ranges[i].end_include == _query_key_ranges[i - 1].end_include;
             j++, i++) {
            key_ranges.push_back(_query_key_ranges[i]);
        }

        OlapScanner* scanner = new OlapScanner(
            state, this, _olap_scan_node.is_preaggregation,
            scan_range.get(), key_ranges);

        _scanner_pool->add(scanner);
        _olap_scanners.push_back(scanner);
        _all_olap_scanners = _olap_scanners;
    }

    // init progress
    std::stringstream ss;
    ss << "ScanThread complete (node=" << id() << "):";
    _progress = ProgressUpdater(ss.str(), _olap_scanners.size(), 1);
    _progress.set_logging_level(1);

    _transfer_thread.add_thread(
        new boost::thread(
            &OlapScanNode::transfer_thread, this, state));

    return Status::OK();
}

template<class T>
Status OlapScanNode::normalize_predicate(ColumnValueRange<T>& range, SlotDescriptor* slot) {
    // 1. Normalize InPredicate, add to ColumnValueRange
    RETURN_IF_ERROR(normalize_in_predicate(slot, &range));

    // 2. Normalize BinaryPredicate , add to ColumnValueRange
    RETURN_IF_ERROR(normalize_binary_predicate(slot, &range));

    // 3. Add range to Column->ColumnValueRange map
    _column_value_ranges[slot->col_name()] = range;

    return Status::OK();
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

            if (pred->get_child(0)->type().type != slot->type().type) {
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
                if (pred->hybird_set()->size() > config::doris_max_scan_key_num) {
                    VLOG(3) << "Predicate value num " << pred->hybird_set()->size()
                            << " excede limit " << config::doris_max_scan_key_num;
                    continue;
                }

                // 1.3 Push InPredicate value into ColumnValueRange
                HybirdSetBase::IteratorBase* iter = pred->hybird_set()->begin();
                while (iter->has_next()) {
                    // column in (NULL,...) counldn't push down to StorageEngine
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
                    case TYPE_DECIMALV2:
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
                if (pred->get_child(child_idx)->type().type != slot->type().type) {
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
                    case TYPE_DECIMALV2:
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
                        return Status::InternalError("Normalize filter fail, Unsupport Primitive type");
                    }
                    }
                }
            }
        }
    }

    return Status::OK();
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
            if (pred->get_child(child_idx)->type().type != slot->type().type) {
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
                case TYPE_DECIMALV2:
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
                    return Status::InternalError("Normalize filter fail, Unsupport Primitive type");
                }
                }

                VLOG(1) << slot->col_name() << " op: "
                        << static_cast<int>(to_olap_filter_type(pred->op(), child_idx))
                        << " value: " << *reinterpret_cast<T*>(value);
            }
        }
    }

    return Status::OK();
}

bool OlapScanNode::select_scan_range(boost::shared_ptr<DorisScanRange> scan_range) {
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
        boost::shared_ptr<DorisScanRange> scan_range,
        std::vector<OlapScanRange>* sub_range) {
    std::vector<OlapScanRange> scan_key_range;
    RETURN_IF_ERROR(_scan_keys.get_key_range(&scan_key_range));

    if (limit() != -1 ||
        scan_key_range.size() > 64) {
        if (scan_key_range.size() != 0) {
            *sub_range = scan_key_range;
        } else { // [-oo, +oo]
            sub_range->resize(1);
        }
    } else {
        if (!EngineMetaReader::get_hints(
                    scan_range,
                    config::doris_scan_range_row_count,
                    _scan_keys.begin_include(),
                    _scan_keys.end_include(),
                    scan_key_range,
                    sub_range,
                    _runtime_profile.get()).ok()) {
            if (scan_key_range.size() != 0) {
                *sub_range = scan_key_range;
            } else { // [-oo, +oo]
                sub_range->resize(1);
            }
        }
    }

    return Status::OK();
}

void OlapScanNode::transfer_thread(RuntimeState* state) {
    // scanner open pushdown to scanThread
    Status status = Status::OK();
    for (auto scanner : _olap_scanners) {
        status = Expr::clone_if_not_exists(_conjunct_ctxs, state, scanner->conjunct_ctxs());
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
    if (config::doris_scanner_row_num > state->batch_size()) {
        max_thread /= config::doris_scanner_row_num / state->batch_size();
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

        auto iter = olap_scanners.begin();
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
    Status status = Status::OK();
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

    // Because we use thread pool to scan data from storage. One scanner can't
    // use this thread too long, this can starve other query's scanner. So, we
    // need yield this thread when we do enough work. However, OlapStorage read
    // data in pre-aggregate mode, then we can't use storage returned data to
    // judge if we need to yield. So we record all raw data read in this round
    // scan, if this exceed threshold, we yield this thread.
    int64_t raw_rows_read = scanner->raw_rows_read();
    int64_t raw_rows_threshold = raw_rows_read + config::doris_scanner_row_num;
    while (!eos && raw_rows_read < raw_rows_threshold) {
        if (UNLIKELY(_transfer_done)) {
            eos = true;
            status = Status::Cancelled("Cancelled");
            LOG(INFO) << "Scan thread cancelled, cause query done, maybe reach limit.";
            break;
        }
        RowBatch *row_batch = new RowBatch(
                this->row_desc(), state->batch_size(), _runtime_state->fragment_mem_tracker());
        row_batch->set_scanner_id(scanner->id());
        status = scanner->get_batch(_runtime_state, row_batch, &eos);
        if (!status.ok()) {
            LOG(WARNING) << "Scan thread read OlapScanner failed!";
            eos = true;
            break;
        }
        // 4. if status not ok, change status_.
        if (UNLIKELY(row_batch->num_rows() == 0)) {
            // may be failed, push already, scan node delete this batch.
            delete row_batch;
            row_batch = NULL;
        } else {
            row_batchs.push_back(row_batch);
            __sync_fetch_and_add(&_buffered_bytes,
                                 row_batch->tuple_data_pool()->total_reserved_bytes());
        }
        raw_rows_read = scanner->raw_rows_read();
    }

    boost::unique_lock<boost::mutex> l(_scan_batches_lock);
    // if we failed, check status.
    if (UNLIKELY(!status.ok())) {
        _transfer_done = true;
        boost::lock_guard<boost::mutex> guard(_status_mutex);
        if (LIKELY(_status.ok())) {
            _status = status;
        }
    }

    bool global_status_ok = false;
    {
        boost::lock_guard<boost::mutex> guard(_status_mutex);
        global_status_ok = _status.ok();
    }
    if (UNLIKELY(!global_status_ok)) {
        eos = true;
        for (auto rb : row_batchs) {
            delete rb;
        }
    } else {
        for (auto rb : row_batchs) {
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
        scanner->close(_runtime_state);
    } else {
        _olap_scanners.push_front(scanner);
    }
    _running_thread--;
    _scan_batch_added_cv.notify_one();
}

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
    return Status::OK();
}

void OlapScanNode::debug_string(
    int /* indentation_level */,
    std::stringstream* /* out */) const {
}

} // namespace doris
