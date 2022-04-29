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

#include "olap_scanner.h"

#include <string>

#include "common/utils.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "olap/decimal12.h"
#include "olap/field.h"
#include "olap/uint24.h"
#include "olap_scan_node.h"
#include "olap_utils.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/mem_util.hpp"
#include "util/network_util.h"

namespace doris {

OlapScanner::OlapScanner(RuntimeState* runtime_state, OlapScanNode* parent, bool aggregation,
                         bool need_agg_finalize, const TPaloScanRange& scan_range,
                         const std::shared_ptr<MemTracker>& tracker)
        : _runtime_state(runtime_state),
          _parent(parent),
          _tuple_desc(parent->_tuple_desc),
          _id(-1),
          _is_open(false),
          _aggregation(aggregation),
          _need_agg_finalize(need_agg_finalize),
          _version(-1),
          _mem_tracker(MemTracker::create_tracker(
                  tracker->limit(), tracker->label() + ":OlapScanner:" + tls_ctx()->thread_id_str(),
                  tracker)) {}

Status OlapScanner::prepare(
        const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
        const std::vector<TCondition>& filters,
        const std::vector<std::pair<string, std::shared_ptr<IBloomFilterFuncBase>>>&
                bloom_filters) {
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    set_tablet_reader();
    // set limit to reduce end of rowset and segment mem use
    _tablet_reader->set_batch_size(
            _parent->limit() == -1
                    ? _parent->_runtime_state->batch_size()
                    : std::min(static_cast<int64_t>(_parent->_runtime_state->batch_size()),
                               _parent->limit()));

    // Get olap table
    TTabletId tablet_id = scan_range.tablet_id;
    SchemaHash schema_hash = strtoul(scan_range.schema_hash.c_str(), nullptr, 10);
    _version = strtoul(scan_range.version.c_str(), nullptr, 10);
    {
        std::string err;
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (_tablet.get() == nullptr) {
            std::stringstream ss;
            ss << "failed to get tablet. tablet_id=" << tablet_id
               << ", with schema_hash=" << schema_hash << ", reason=" << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        {
            std::shared_lock rdlock(_tablet->get_header_lock());
            const RowsetSharedPtr rowset = _tablet->rowset_with_max_version();
            if (rowset == nullptr) {
                std::stringstream ss;
                ss << "fail to get latest version of tablet: " << tablet_id;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }

            // acquire tablet rowset readers at the beginning of the scan node
            // to prevent this case: when there are lots of olap scanners to run for example 10000
            // the rowsets maybe compacted when the last olap scanner starts
            Version rd_version(0, _version);
            Status acquire_reader_st =
                    _tablet->capture_rs_readers(rd_version, &_tablet_reader_params.rs_readers);
            if (!acquire_reader_st.ok()) {
                LOG(WARNING) << "fail to init reader.res=" << acquire_reader_st;
                std::stringstream ss;
                ss << "failed to initialize storage reader. tablet=" << _tablet->full_name()
                   << ", res=" << acquire_reader_st
                   << ", backend=" << BackendOptions::get_localhost();
                return Status::InternalError(ss.str().c_str());
            }
        }
    }

    {
        // Initialize tablet_reader_params
        RETURN_IF_ERROR(_init_tablet_reader_params(key_ranges, filters, bloom_filters));
    }

    return Status::OK();
}

Status OlapScanner::open() {
    SCOPED_TIMER(_parent->_reader_init_timer);
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);

    if (_conjunct_ctxs.size() > _parent->_direct_conjunct_size) {
        _use_pushdown_conjuncts = true;
    }

    _runtime_filter_marks.resize(_parent->runtime_filter_descs().size(), false);

    auto res = _tablet_reader->init(_tablet_reader_params);
    if (!res.ok()) {
        std::stringstream ss;
        ss << "failed to initialize storage reader. tablet="
           << _tablet_reader_params.tablet->full_name() << ", res=" << res
           << ", backend=" << BackendOptions::get_localhost();
        return Status::InternalError(ss.str().c_str());
    }
    return Status::OK();
}

// it will be called under tablet read lock because capture rs readers need
Status OlapScanner::_init_tablet_reader_params(
        const std::vector<OlapScanRange*>& key_ranges, const std::vector<TCondition>& filters,
        const std::vector<std::pair<string, std::shared_ptr<IBloomFilterFuncBase>>>&
                bloom_filters) {
    RETURN_IF_ERROR(_init_return_columns());

    _tablet_reader_params.tablet = _tablet;
    _tablet_reader_params.reader_type = READER_QUERY;
    _tablet_reader_params.aggregation = _aggregation;
    _tablet_reader_params.version = Version(0, _version);

    // Condition
    for (auto& filter : filters) {
        _tablet_reader_params.conditions.push_back(filter);
    }
    std::copy(bloom_filters.cbegin(), bloom_filters.cend(),
              std::inserter(_tablet_reader_params.bloom_filters,
                            _tablet_reader_params.bloom_filters.begin()));

    // Range
    for (auto key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 &&
            key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _tablet_reader_params.start_key_include = key_range->begin_include;
        _tablet_reader_params.end_key_include = key_range->end_include;

        _tablet_reader_params.start_key.push_back(key_range->begin_scan_range);
        _tablet_reader_params.end_key.push_back(key_range->end_scan_range);
    }

    // TODO(zc)
    _tablet_reader_params.profile = _parent->runtime_profile();
    _tablet_reader_params.runtime_state = _runtime_state;
    // if the table with rowset [0-x] or [0-1] [2-y], and [0-1] is empty
    bool single_version =
            (_tablet_reader_params.rs_readers.size() == 1 &&
             _tablet_reader_params.rs_readers[0]->rowset()->start_version() == 0 &&
             !_tablet_reader_params.rs_readers[0]
                      ->rowset()
                      ->rowset_meta()
                      ->is_segments_overlapping()) ||
            (_tablet_reader_params.rs_readers.size() == 2 &&
             _tablet_reader_params.rs_readers[0]->rowset()->rowset_meta()->num_rows() == 0 &&
             _tablet_reader_params.rs_readers[1]->rowset()->start_version() == 2 &&
             !_tablet_reader_params.rs_readers[1]
                      ->rowset()
                      ->rowset_meta()
                      ->is_segments_overlapping());

    _tablet_reader_params.origin_return_columns = &_return_columns;
    _tablet_reader_params.tablet_columns_convert_to_null_set = &_tablet_columns_convert_to_null_set;

    if (_aggregation || single_version) {
        _tablet_reader_params.return_columns = _return_columns;
        _tablet_reader_params.direct_mode = true;
    } else {
        // we need to fetch all key columns to do the right aggregation on storage engine side.
        for (size_t i = 0; i < _tablet->num_key_columns(); ++i) {
            _tablet_reader_params.return_columns.push_back(i);
        }
        for (auto index : _return_columns) {
            if (_tablet->tablet_schema().column(index).is_key()) {
                continue;
            } else {
                _tablet_reader_params.return_columns.push_back(index);
            }
        }
    }

    // use _tablet_reader_params.return_columns, because reader use this to merge sort
    Status res =
            _read_row_cursor.init(_tablet->tablet_schema(), _tablet_reader_params.return_columns);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init row cursor.res = " << res;
        return Status::InternalError("failed to initialize storage read row cursor");
    }
    _read_row_cursor.allocate_memory_for_string_type(_tablet->tablet_schema());

    // If a agg node is this scan node direct parent
    // we will not call agg object finalize method in scan node,
    // to avoid the unnecessary SerDe and improve query performance
    _tablet_reader_params.need_agg_finalize = _need_agg_finalize;

    if (!config::disable_storage_page_cache) {
        _tablet_reader_params.use_page_cache = true;
    }

    return Status::OK();
}

Status OlapScanner::_init_return_columns() {
    for (auto slot : _tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        int32_t index = _tablet->field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "field name is invalid. field=" << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _return_columns.push_back(index);
        if (slot->is_nullable() && !_tablet->tablet_schema().column(index).is_nullable())
            _tablet_columns_convert_to_null_set.emplace(index);
        _query_slots.push_back(slot);
    }

    // expand the sequence column
    if (_tablet->tablet_schema().has_sequence_col()) {
        bool has_replace_col = false;
        for (auto col : _return_columns) {
            if (_tablet->tablet_schema().column(col).aggregation() ==
                FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE) {
                has_replace_col = true;
                break;
            }
        }
        if (auto sequence_col_idx = _tablet->tablet_schema().sequence_col_idx();
            has_replace_col && std::find(_return_columns.begin(), _return_columns.end(),
                                         sequence_col_idx) == _return_columns.end()) {
            _return_columns.push_back(sequence_col_idx);
        }
    }

    if (_return_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

Status OlapScanner::get_batch(RuntimeState* state, RowBatch* batch, bool* eof) {
    SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER(_mem_tracker);
    // 2. Allocate Row's Tuple buf
    uint8_t* tuple_buf =
            batch->tuple_data_pool()->allocate(state->batch_size() * _tuple_desc->byte_size());
    bzero(tuple_buf, state->batch_size() * _tuple_desc->byte_size());
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buf);

    std::unique_ptr<MemPool> mem_pool(new MemPool(_mem_tracker.get()));
    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    int64_t raw_bytes_threshold = config::doris_scanner_row_bytes;
    {
        SCOPED_TIMER(_parent->_scan_timer);
        // store the object which may can't pass the conjuncts temporarily.
        // otherwise, pushed all objects into agg_object_pool directly may lead to OOM.
        ObjectPool tmp_object_pool;
        // release the memory of the object which can't pass the conjuncts.
        ObjectPool unused_object_pool;
        while (true) {
            // Batch is full or reach raw_rows_threshold or raw_bytes_threshold, break
            if (batch->is_full() ||
                batch->tuple_data_pool()->total_reserved_bytes() >= raw_bytes_threshold ||
                raw_rows_read() >= raw_rows_threshold) {
                _update_realtime_counter();
                break;
            }

            if (tmp_object_pool.size() > 0) {
                unused_object_pool.acquire_data(&tmp_object_pool);
            }

            if (unused_object_pool.size() >= config::object_pool_buffer_size) {
                unused_object_pool.clear();
            }

            // Read one row from reader
            auto res = _tablet_reader->next_row_with_aggregation(&_read_row_cursor, mem_pool.get(),
                                                                 &tmp_object_pool, eof);
            if (!res.ok()) {
                std::stringstream ss;
                ss << "Internal Error: read storage fail. res=" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", backend=" << BackendOptions::get_localhost();
                return Status::InternalError(ss.str());
            }
            // If we reach end of this scanner, break
            if (UNLIKELY(*eof)) {
                break;
            }

            _num_rows_read++;

            _convert_row_to_tuple(tuple);
            if (VLOG_ROW_IS_ON) {
                VLOG_ROW << "OlapScanner input row: " << Tuple::to_string(tuple, *_tuple_desc);
            }

            if (_num_rows_read % RELEASE_CONTEXT_COUNTER == 0) {
                ExprContext::free_local_allocations(_conjunct_ctxs);
            }

            // 3.4 Set tuple to RowBatch(not committed)
            int row_idx = batch->add_row();
            TupleRow* row = batch->get_row(row_idx);
            row->set_tuple(_parent->_tuple_idx, tuple);

            auto direct_conjunct_size = _parent->_direct_conjunct_size;

            do {
                // 3.5.1 Using direct conjuncts to filter data
                if (_eval_conjuncts_fn != nullptr) {
                    if (!_eval_conjuncts_fn(&_conjunct_ctxs[0], direct_conjunct_size, row)) {
                        // check direct conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        break;
                    }
                } else {
                    if (!ExecNode::eval_conjuncts(&_conjunct_ctxs[0], direct_conjunct_size, row)) {
                        // check direct conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        break;
                    }
                }

                // 3.5.2 Using pushdown conjuncts to filter data
                if (_use_pushdown_conjuncts) {
                    if (!ExecNode::eval_conjuncts(&_conjunct_ctxs[direct_conjunct_size],
                                                  _conjunct_ctxs.size() - direct_conjunct_size,
                                                  row)) {
                        // check pushdown conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        _num_rows_pushed_cond_filtered++;
                        break;
                    }
                }

                // Copy string slot
                for (auto desc : _parent->_string_slots) {
                    StringValue* slot = tuple->get_string_slot(desc->tuple_offset());
                    if (slot->len != 0) {
                        uint8_t* v = batch->tuple_data_pool()->allocate(slot->len);
                        memory_copy(v, slot->ptr, slot->len);
                        slot->ptr = reinterpret_cast<char*>(v);
                    }
                }

                // Copy collection slot
                for (auto desc : _parent->_collection_slots) {
                    CollectionValue* slot = tuple->get_collection_slot(desc->tuple_offset());
                    const TypeDescriptor& item_type = desc->type().children.at(0);
                    auto pool = batch->tuple_data_pool();
                    CollectionValue::deep_copy_collection(
                            slot, item_type,
                            [pool](int size) -> MemFootprint {
                                int64_t offset = pool->total_allocated_bytes();
                                uint8_t* data = pool->allocate(size);
                                return {offset, data};
                            },
                            false);
                }
                // the memory allocate by mem pool has been copied,
                // so we should release these memory immediately
                mem_pool->clear();

                if (VLOG_ROW_IS_ON) {
                    VLOG_ROW << "OlapScanner output row: " << Tuple::to_string(tuple, *_tuple_desc);
                }

                // check direct && pushdown conjuncts success then commit tuple
                batch->commit_last_row();
                batch->agg_object_pool()->acquire_data(&tmp_object_pool);
                char* new_tuple = reinterpret_cast<char*>(tuple);
                new_tuple += _tuple_desc->byte_size();
                tuple = reinterpret_cast<Tuple*>(new_tuple);

                // compute pushdown conjuncts filter rate
                if (_use_pushdown_conjuncts) {
                    // check this rate after
                    if (_num_rows_read > 32768) {
                        int32_t pushdown_return_rate =
                                _num_rows_read * 100 /
                                (_num_rows_read + _num_rows_pushed_cond_filtered);
                        if (pushdown_return_rate >
                            config::doris_max_pushdown_conjuncts_return_rate) {
                            _use_pushdown_conjuncts = false;
                            VLOG_CRITICAL << "Stop Using PushDown Conjuncts. "
                                          << "PushDownReturnRate: " << pushdown_return_rate << "%"
                                          << " MaxPushDownReturnRate: "
                                          << config::doris_max_pushdown_conjuncts_return_rate
                                          << "%";
                        }
                    }
                }
            } while (false);
        }
    }

    return Status::OK();
}

void OlapScanner::_convert_row_to_tuple(Tuple* tuple) {
    size_t slots_size = _query_slots.size();
    for (int i = 0; i < slots_size; ++i) {
        SlotDescriptor* slot_desc = _query_slots[i];
        auto cid = _return_columns[i];
        if (_read_row_cursor.is_null(cid)) {
            tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }
        char* ptr = (char*)_read_row_cursor.cell_ptr(cid);
        size_t len = _read_row_cursor.column_size(cid);
        switch (slot_desc->type().type) {
        case TYPE_CHAR: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            StringValue* slot = tuple->get_string_slot(slot_desc->tuple_offset());
            slot->ptr = slice->data;
            slot->len = strnlen(slot->ptr, slice->size);
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_OBJECT:
        case TYPE_QUANTILE_STATE:
        case TYPE_HLL:
        case TYPE_STRING: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            StringValue* slot = tuple->get_string_slot(slot_desc->tuple_offset());
            slot->ptr = slice->data;
            slot->len = slice->size;
            break;
        }
        case TYPE_DECIMALV2: {
            DecimalV2Value* slot = tuple->get_decimalv2_slot(slot_desc->tuple_offset());
            auto packed_decimal = *reinterpret_cast<const decimal12_t*>(ptr);

            // We convert the format for storage to the format for computation.
            // Coding coverting in the opposite direction is in AggregateFuncTraits
            // for decimal.
            int64_t int_value = packed_decimal.integer;
            int32_t frac_value = packed_decimal.fraction;
            if (!slot->from_olap_decimal(int_value, frac_value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        case TYPE_DATETIME: {
            DateTimeValue* slot = tuple->get_datetime_slot(slot_desc->tuple_offset());
            uint64_t value = *reinterpret_cast<uint64_t*>(ptr);
            if (!slot->from_olap_datetime(value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        case TYPE_DATE: {
            DateTimeValue* slot = tuple->get_datetime_slot(slot_desc->tuple_offset());

            uint24_t date = *reinterpret_cast<const uint24_t*>(ptr);
            uint64_t value = uint32_t(date);

            if (!slot->from_olap_date(value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        case TYPE_ARRAY: {
            CollectionValue* array_v = reinterpret_cast<CollectionValue*>(ptr);
            CollectionValue* slot = tuple->get_collection_slot(slot_desc->tuple_offset());
            slot->shallow_copy(array_v);
            break;
        }
        default: {
            void* slot = tuple->get_slot(slot_desc->tuple_offset());
            memory_copy(slot, ptr, len);
            break;
        }
        }
    }
}

void OlapScanner::update_counter() {
    if (_has_update_counter) {
        return;
    }
    auto& stats = _tablet_reader->stats();

    COUNTER_UPDATE(_parent->rows_read_counter(), _num_rows_read);
    COUNTER_UPDATE(_parent->_rows_pushed_cond_filtered_counter, _num_rows_pushed_cond_filtered);

    COUNTER_UPDATE(_parent->_io_timer, stats.io_ns);
    COUNTER_UPDATE(_parent->_read_compressed_counter, stats.compressed_bytes_read);
    _compressed_bytes_read += stats.compressed_bytes_read;
    COUNTER_UPDATE(_parent->_decompressor_timer, stats.decompress_ns);
    COUNTER_UPDATE(_parent->_read_uncompressed_counter, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(_parent->bytes_read_counter(), stats.bytes_read);

    COUNTER_UPDATE(_parent->_block_load_timer, stats.block_load_ns);
    COUNTER_UPDATE(_parent->_block_load_counter, stats.blocks_load);
    COUNTER_UPDATE(_parent->_block_fetch_timer, stats.block_fetch_ns);
    COUNTER_UPDATE(_parent->_block_seek_timer, stats.block_seek_ns);
    COUNTER_UPDATE(_parent->_block_convert_timer, stats.block_convert_ns);

    COUNTER_UPDATE(_parent->_raw_rows_counter, stats.raw_rows_read);
    // if raw_rows_read is reset, scanNode will scan all table rows which may cause BE crash
    _raw_rows_read += _tablet_reader->mutable_stats()->raw_rows_read;
    // COUNTER_UPDATE(_parent->_filtered_rows_counter, stats.num_rows_filtered);
    COUNTER_UPDATE(_parent->_vec_cond_timer, stats.vec_cond_ns);
    COUNTER_UPDATE(_parent->_short_cond_timer, stats.short_cond_ns);
    COUNTER_UPDATE(_parent->_pred_col_read_timer, stats.pred_col_read_ns);
    COUNTER_UPDATE(_parent->_lazy_read_timer, stats.lazy_read_ns);
    COUNTER_UPDATE(_parent->_output_col_timer, stats.output_col_ns);
    COUNTER_UPDATE(_parent->_rows_vec_cond_counter, stats.rows_vec_cond_filtered);

    COUNTER_UPDATE(_parent->_stats_filtered_counter, stats.rows_stats_filtered);
    COUNTER_UPDATE(_parent->_bf_filtered_counter, stats.rows_bf_filtered);
    COUNTER_UPDATE(_parent->_del_filtered_counter, stats.rows_del_filtered);
    COUNTER_UPDATE(_parent->_del_filtered_counter, stats.rows_vec_del_cond_filtered);

    COUNTER_UPDATE(_parent->_conditions_filtered_counter, stats.rows_conditions_filtered);
    COUNTER_UPDATE(_parent->_key_range_filtered_counter, stats.rows_key_range_filtered);

    COUNTER_UPDATE(_parent->_index_load_timer, stats.index_load_ns);

    size_t timer_count = sizeof(stats.general_debug_ns) / sizeof(*stats.general_debug_ns);
    for (size_t i = 0; i < timer_count; ++i) {
        COUNTER_UPDATE(_parent->_general_debug_timer[i], stats.general_debug_ns[i]);
    }

    COUNTER_UPDATE(_parent->_total_pages_num_counter, stats.total_pages_num);
    COUNTER_UPDATE(_parent->_cached_pages_num_counter, stats.cached_pages_num);

    COUNTER_UPDATE(_parent->_bitmap_index_filter_counter, stats.rows_bitmap_index_filtered);
    COUNTER_UPDATE(_parent->_bitmap_index_filter_timer, stats.bitmap_index_filter_timer);
    COUNTER_UPDATE(_parent->_block_seek_counter, stats.block_seek_num);

    COUNTER_UPDATE(_parent->_filtered_segment_counter, stats.filtered_segment_number);
    COUNTER_UPDATE(_parent->_total_segment_counter, stats.total_segment_number);

    DorisMetrics::instance()->query_scan_bytes->increment(_compressed_bytes_read);
    DorisMetrics::instance()->query_scan_rows->increment(_raw_rows_read);

    _tablet->query_scan_bytes->increment(_compressed_bytes_read);
    _tablet->query_scan_rows->increment(_raw_rows_read);
    _tablet->query_scan_count->increment(1);

    _has_update_counter = true;
}

void OlapScanner::_update_realtime_counter() {
    auto& stats = _tablet_reader->stats();
    COUNTER_UPDATE(_parent->_read_compressed_counter, stats.compressed_bytes_read);
    _compressed_bytes_read += stats.compressed_bytes_read;
    _tablet_reader->mutable_stats()->compressed_bytes_read = 0;

    COUNTER_UPDATE(_parent->_raw_rows_counter, stats.raw_rows_read);
    // if raw_rows_read is reset, scanNode will scan all table rows which may cause BE crash
    _raw_rows_read += stats.raw_rows_read;
    _tablet_reader->mutable_stats()->raw_rows_read = 0;
}

Status OlapScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    // olap scan node will call scanner.close() when finished
    // will release resources here
    // if not clear rowset readers in read_params here
    // readers will be release when runtime state deconstructed but
    // deconstructor in reader references runtime state
    // so that it will core
    _tablet_reader_params.rs_readers.clear();
    update_counter();
    _tablet_reader.reset();
    Expr::close(_conjunct_ctxs, state);
    _is_closed = true;
    return Status::OK();
}

} // namespace doris
