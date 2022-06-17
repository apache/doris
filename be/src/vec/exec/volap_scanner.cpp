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

#include "vec/exec/volap_scanner.h"

#include <memory>

#include "olap/storage_engine.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exec/volap_scan_node.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VOlapScanner::VOlapScanner(RuntimeState* runtime_state, VOlapScanNode* parent, bool aggregation,
                           bool need_agg_finalize, const TPaloScanRange& scan_range,
                           const std::shared_ptr<MemTracker>& tracker)
        : _runtime_state(runtime_state),
          _parent(parent),
          _tuple_desc(parent->_tuple_desc),
          _id(-1),
          _is_open(false),
          _aggregation(aggregation),
          _need_agg_finalize(need_agg_finalize),
          _version(-1) {
#ifndef NDEBUG
    _mem_tracker = MemTracker::create_tracker(
            tracker->limit(), "VOlapScanner:" + tls_ctx()->thread_id_str(), tracker);
#else
    _mem_tracker = tracker;
#endif
}

Status VOlapScanner::prepare(
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

Status VOlapScanner::open() {
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
Status VOlapScanner::_init_tablet_reader_params(
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

Status VOlapScanner::_init_return_columns() {
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

Status VOlapScanner::get_block(RuntimeState* state, vectorized::Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);
    SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER(_mem_tracker);

    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    int64_t raw_bytes_threshold = config::doris_scanner_row_bytes;
    if (!block->mem_reuse()) {
        for (const auto slot_desc : _tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    {
        SCOPED_TIMER(_parent->_scan_timer);
        do {
            // Read one block from block reader
            auto res = _tablet_reader->next_block_with_aggregation(block, nullptr, nullptr, eof);
            if (!res) {
                std::stringstream ss;
                ss << "Internal Error: read storage fail. res=" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", backend=" << BackendOptions::get_localhost();
                return res;
            }
            _num_rows_read += block->rows();
            _update_realtime_counter();
            RETURN_IF_ERROR(
                    VExprContext::filter_block(_vconjunct_ctx, block, _tuple_desc->slots().size()));
        } while (block->rows() == 0 && !(*eof) && raw_rows_read() < raw_rows_threshold &&
                 block->allocated_bytes() < raw_bytes_threshold);
    }
    // NOTE:
    // There is no need to check raw_bytes_threshold since block->rows() == 0 is checked first.
    // But checking raw_bytes_threshold is still added here for consistency with raw_rows_threshold
    // and olap_scanner.cpp.

    return Status::OK();
}

void VOlapScanner::_update_realtime_counter() {
    auto& stats = _tablet_reader->stats();
    COUNTER_UPDATE(_parent->_read_compressed_counter, stats.compressed_bytes_read);
    _compressed_bytes_read += stats.compressed_bytes_read;
    _tablet_reader->mutable_stats()->compressed_bytes_read = 0;

    COUNTER_UPDATE(_parent->_raw_rows_counter, stats.raw_rows_read);
    // if raw_rows_read is reset, scanNode will scan all table rows which may cause BE crash
    _raw_rows_read += stats.raw_rows_read;
    _tablet_reader->mutable_stats()->raw_rows_read = 0;
}

Status VOlapScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    if (_vconjunct_ctx) _vconjunct_ctx->close(state);
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

void VOlapScanner::update_counter() {
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
    COUNTER_UPDATE(_parent->_first_read_timer, stats.first_read_ns);
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
} // namespace doris::vectorized
