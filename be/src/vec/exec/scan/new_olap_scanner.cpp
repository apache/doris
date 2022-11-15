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

#include "vec/exec/scan/new_olap_scanner.h"

#include "olap/storage_engine.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/olap/block_reader.h"

namespace doris::vectorized {

NewOlapScanner::NewOlapScanner(RuntimeState* state, NewOlapScanNode* parent, int64_t limit,
                               bool aggregation, bool need_agg_finalize,
                               const TPaloScanRange& scan_range, RuntimeProfile* profile)
        : VScanner(state, static_cast<VScanNode*>(parent), limit),
          _aggregation(aggregation),
          _need_agg_finalize(need_agg_finalize),
          _version(-1),
          _profile(profile) {
    _tablet_schema = std::make_shared<TabletSchema>();
}

Status NewOlapScanner::prepare(
        const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
        VExprContext** vconjunct_ctx_ptr, const std::vector<TCondition>& filters,
        const std::vector<std::pair<string, std::shared_ptr<BloomFilterFuncBase>>>& bloom_filters,
        const std::vector<std::pair<string, std::shared_ptr<HybridSetBase>>>& in_filters,
        const std::vector<FunctionFilter>& function_filters) {
    if (vconjunct_ctx_ptr != nullptr) {
        // Copy vconjunct_ctx_ptr from scan node to this scanner's _vconjunct_ctx.
        RETURN_IF_ERROR((*vconjunct_ctx_ptr)->clone(_state, &_vconjunct_ctx));
    }

    // set limit to reduce end of rowset and segment mem use
    _tablet_reader = std::make_unique<BlockReader>();
    _tablet_reader->set_batch_size(
            _parent->limit() == -1
                    ? _state->batch_size()
                    : std::min(static_cast<int64_t>(_state->batch_size()), _parent->limit()));

    // Get olap table
    TTabletId tablet_id = scan_range.tablet_id;
    _version = strtoul(scan_range.version.c_str(), nullptr, 10);
    {
        std::string err;
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (_tablet.get() == nullptr) {
            std::stringstream ss;
            ss << "failed to get tablet. tablet_id=" << tablet_id << ", reason=" << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _tablet_schema->copy_from(*_tablet->tablet_schema());

        TOlapScanNode& olap_scan_node = ((NewOlapScanNode*)_parent)->_olap_scan_node;
        if (olap_scan_node.__isset.columns_desc && !olap_scan_node.columns_desc.empty() &&
            olap_scan_node.columns_desc[0].col_unique_id >= 0) {
            // Originally scanner get TabletSchema from tablet object in BE.
            // To support lightweight schema change for adding / dropping columns,
            // tabletschema is bounded to rowset and tablet's schema maybe outdated,
            //  so we have to use schema from a query plan witch FE puts it in query plans.
            _tablet_schema->clear_columns();
            for (const auto& column_desc : olap_scan_node.columns_desc) {
                _tablet_schema->append_column(TabletColumn(column_desc));
            }
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
                return Status::InternalError(ss.str());
            }

            // Initialize tablet_reader_params
            RETURN_IF_ERROR(_init_tablet_reader_params(key_ranges, filters, bloom_filters,
                                                       in_filters, function_filters));
        }
    }

    return Status::OK();
}

Status NewOlapScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(VScanner::open(state));

    auto res = _tablet_reader->init(_tablet_reader_params);
    if (!res.ok()) {
        std::stringstream ss;
        ss << "failed to initialize storage reader. tablet="
           << _tablet_reader_params.tablet->full_name() << ", res=" << res
           << ", backend=" << BackendOptions::get_localhost();
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

// it will be called under tablet read lock because capture rs readers need
Status NewOlapScanner::_init_tablet_reader_params(
        const std::vector<OlapScanRange*>& key_ranges, const std::vector<TCondition>& filters,
        const std::vector<std::pair<string, std::shared_ptr<BloomFilterFuncBase>>>& bloom_filters,
        const std::vector<std::pair<string, std::shared_ptr<HybridSetBase>>>& in_filters,
        const std::vector<FunctionFilter>& function_filters) {
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
    auto real_parent = reinterpret_cast<NewOlapScanNode*>(_parent);
    if (_state->skip_storage_engine_merge()) {
        _tablet_reader_params.direct_mode = true;
        _aggregation = true;
    } else {
        _tablet_reader_params.direct_mode =
                _aggregation || single_version ||
                real_parent->_olap_scan_node.__isset.push_down_agg_type_opt;
    }

    RETURN_IF_ERROR(_init_return_columns());

    _tablet_reader_params.tablet = _tablet;
    _tablet_reader_params.tablet_schema = _tablet_schema;
    _tablet_reader_params.reader_type = READER_QUERY;
    _tablet_reader_params.aggregation = _aggregation;
    if (real_parent->_olap_scan_node.__isset.push_down_agg_type_opt) {
        _tablet_reader_params.push_down_agg_type_opt =
                real_parent->_olap_scan_node.push_down_agg_type_opt;
    }
    _tablet_reader_params.version = Version(0, _version);

    // Condition
    for (auto& filter : filters) {
        _tablet_reader_params.conditions.push_back(filter);
    }
    std::copy(bloom_filters.cbegin(), bloom_filters.cend(),
              std::inserter(_tablet_reader_params.bloom_filters,
                            _tablet_reader_params.bloom_filters.begin()));

    std::copy(in_filters.cbegin(), in_filters.cend(),
              std::inserter(_tablet_reader_params.in_filters,
                            _tablet_reader_params.in_filters.begin()));

    std::copy(function_filters.cbegin(), function_filters.cend(),
              std::inserter(_tablet_reader_params.function_filters,
                            _tablet_reader_params.function_filters.begin()));

    if (!_state->skip_delete_predicate()) {
        auto& delete_preds = _tablet->delete_predicates();
        std::copy(delete_preds.cbegin(), delete_preds.cend(),
                  std::inserter(_tablet_reader_params.delete_predicates,
                                _tablet_reader_params.delete_predicates.begin()));
    }

    // Merge the columns in delete predicate that not in latest schema in to current tablet schema
    for (auto& del_pred_pb : _tablet_reader_params.delete_predicates) {
        _tablet_schema->merge_dropped_columns(_tablet->tablet_schema(del_pred_pb->version()));
    }

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
    _tablet_reader_params.runtime_state = _state;

    _tablet_reader_params.origin_return_columns = &_return_columns;
    _tablet_reader_params.tablet_columns_convert_to_null_set = &_tablet_columns_convert_to_null_set;

    if (_tablet_reader_params.direct_mode) {
        _tablet_reader_params.return_columns = _return_columns;
    } else {
        // we need to fetch all key columns to do the right aggregation on storage engine side.
        for (size_t i = 0; i < _tablet_schema->num_key_columns(); ++i) {
            _tablet_reader_params.return_columns.push_back(i);
        }
        for (auto index : _return_columns) {
            if (_tablet_schema->column(index).is_key()) {
                continue;
            } else {
                _tablet_reader_params.return_columns.push_back(index);
            }
        }
        // expand the sequence column
        if (_tablet_schema->has_sequence_col()) {
            bool has_replace_col = false;
            for (auto col : _return_columns) {
                if (_tablet_schema->column(col).aggregation() ==
                    FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE) {
                    has_replace_col = true;
                    break;
                }
            }
            if (auto sequence_col_idx = _tablet_schema->sequence_col_idx();
                has_replace_col && std::find(_return_columns.begin(), _return_columns.end(),
                                             sequence_col_idx) == _return_columns.end()) {
                _tablet_reader_params.return_columns.push_back(sequence_col_idx);
            }
        }
    }

    // If a agg node is this scan node direct parent
    // we will not call agg object finalize method in scan node,
    // to avoid the unnecessary SerDe and improve query performance
    _tablet_reader_params.need_agg_finalize = _need_agg_finalize;

    if (!config::disable_storage_page_cache) {
        _tablet_reader_params.use_page_cache = true;
    }

    if (_tablet->enable_unique_key_merge_on_write()) {
        _tablet_reader_params.delete_bitmap = &_tablet->tablet_meta()->delete_bitmap();
    }

    if (!_state->skip_storage_engine_merge()) {
        TOlapScanNode& olap_scan_node = ((NewOlapScanNode*)_parent)->_olap_scan_node;
        if (olap_scan_node.__isset.sort_info && olap_scan_node.sort_info.is_asc_order.size() > 0) {
            _limit = _parent->_limit_per_scanner;
            _tablet_reader_params.read_orderby_key = true;
            if (!olap_scan_node.sort_info.is_asc_order[0]) {
                _tablet_reader_params.read_orderby_key_reverse = true;
            }
            _tablet_reader_params.read_orderby_key_num_prefix_columns =
                    olap_scan_node.sort_info.is_asc_order.size();
        }
    }

    return Status::OK();
}

Status NewOlapScanner::_init_return_columns() {
    for (auto slot : _output_tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }

        int32_t index = slot->col_unique_id() >= 0
                                ? _tablet_schema->field_index(slot->col_unique_id())
                                : _tablet_schema->field_index(slot->col_name());

        if (index < 0) {
            std::stringstream ss;
            ss << "field name is invalid. field=" << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _return_columns.push_back(index);
        if (slot->is_nullable() && !_tablet_schema->column(index).is_nullable()) {
            _tablet_columns_convert_to_null_set.emplace(index);
        }
    }

    if (_return_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

Status NewOlapScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    if (!_profile_updated) {
        _profile_updated = _tablet_reader->update_profile(_profile);
    }
    // Read one block from block reader
    // ATTN: Here we need to let the _get_block_impl method guarantee the semantics of the interface,
    // that is, eof can be set to true only when the returned block is empty.
    RETURN_IF_ERROR(_tablet_reader->next_block_with_aggregation(block, nullptr, nullptr, eof));
    if (block->rows() > 0) {
        *eof = false;
    }
    _update_realtime_counters();
    return Status::OK();
}

Status NewOlapScanner::close(RuntimeState* state) {
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
    _tablet_reader.reset();

    RETURN_IF_ERROR(VScanner::close(state));
    return Status::OK();
}

void NewOlapScanner::_update_realtime_counters() {
    NewOlapScanNode* olap_parent = (NewOlapScanNode*)_parent;
    auto& stats = _tablet_reader->stats();
    COUNTER_UPDATE(olap_parent->_read_compressed_counter, stats.compressed_bytes_read);
    _compressed_bytes_read += stats.compressed_bytes_read;
    _tablet_reader->mutable_stats()->compressed_bytes_read = 0;

    COUNTER_UPDATE(olap_parent->_raw_rows_counter, stats.raw_rows_read);
    // if raw_rows_read is reset, scanNode will scan all table rows which may cause BE crash
    _raw_rows_read += stats.raw_rows_read;
    _tablet_reader->mutable_stats()->raw_rows_read = 0;
}

void NewOlapScanner::_update_counters_before_close() {
    if (!_state->enable_profile() || _has_updated_counter) {
        return;
    }
    _has_updated_counter = true;

    VScanner::_update_counters_before_close();

    // Update counters for NewOlapScanner
    NewOlapScanNode* olap_parent = (NewOlapScanNode*)_parent;

    // Update counters from tablet reader's stats
    auto& stats = _tablet_reader->stats();
    COUNTER_UPDATE(olap_parent->_io_timer, stats.io_ns);
    COUNTER_UPDATE(olap_parent->_read_compressed_counter, stats.compressed_bytes_read);
    _compressed_bytes_read += stats.compressed_bytes_read;
    COUNTER_UPDATE(olap_parent->_decompressor_timer, stats.decompress_ns);
    COUNTER_UPDATE(olap_parent->_read_uncompressed_counter, stats.uncompressed_bytes_read);

    COUNTER_UPDATE(olap_parent->_block_load_timer, stats.block_load_ns);
    COUNTER_UPDATE(olap_parent->_block_load_counter, stats.blocks_load);
    COUNTER_UPDATE(olap_parent->_block_fetch_timer, stats.block_fetch_ns);
    COUNTER_UPDATE(olap_parent->_block_convert_timer, stats.block_convert_ns);

    COUNTER_UPDATE(olap_parent->_raw_rows_counter, stats.raw_rows_read);
    // if raw_rows_read is reset, scanNode will scan all table rows which may cause BE crash
    _raw_rows_read += _tablet_reader->mutable_stats()->raw_rows_read;
    COUNTER_UPDATE(olap_parent->_vec_cond_timer, stats.vec_cond_ns);
    COUNTER_UPDATE(olap_parent->_short_cond_timer, stats.short_cond_ns);
    COUNTER_UPDATE(olap_parent->_block_init_timer, stats.block_init_ns);
    COUNTER_UPDATE(olap_parent->_block_init_seek_timer, stats.block_init_seek_ns);
    COUNTER_UPDATE(olap_parent->_block_init_seek_counter, stats.block_init_seek_num);
    COUNTER_UPDATE(olap_parent->_first_read_timer, stats.first_read_ns);
    COUNTER_UPDATE(olap_parent->_first_read_seek_timer, stats.block_first_read_seek_ns);
    COUNTER_UPDATE(olap_parent->_first_read_seek_counter, stats.block_first_read_seek_num);
    COUNTER_UPDATE(olap_parent->_lazy_read_timer, stats.lazy_read_ns);
    COUNTER_UPDATE(olap_parent->_lazy_read_seek_timer, stats.block_lazy_read_seek_ns);
    COUNTER_UPDATE(olap_parent->_lazy_read_seek_counter, stats.block_lazy_read_seek_num);
    COUNTER_UPDATE(olap_parent->_output_col_timer, stats.output_col_ns);
    COUNTER_UPDATE(olap_parent->_rows_vec_cond_counter, stats.rows_vec_cond_filtered);

    COUNTER_UPDATE(olap_parent->_stats_filtered_counter, stats.rows_stats_filtered);
    COUNTER_UPDATE(olap_parent->_bf_filtered_counter, stats.rows_bf_filtered);
    COUNTER_UPDATE(olap_parent->_del_filtered_counter, stats.rows_del_filtered);
    COUNTER_UPDATE(olap_parent->_del_filtered_counter, stats.rows_del_by_bitmap);
    COUNTER_UPDATE(olap_parent->_del_filtered_counter, stats.rows_vec_del_cond_filtered);

    COUNTER_UPDATE(olap_parent->_conditions_filtered_counter, stats.rows_conditions_filtered);
    COUNTER_UPDATE(olap_parent->_key_range_filtered_counter, stats.rows_key_range_filtered);

    size_t timer_count = sizeof(stats.general_debug_ns) / sizeof(*stats.general_debug_ns);
    for (size_t i = 0; i < timer_count; ++i) {
        COUNTER_UPDATE(olap_parent->_general_debug_timer[i], stats.general_debug_ns[i]);
    }

    COUNTER_UPDATE(olap_parent->_total_pages_num_counter, stats.total_pages_num);
    COUNTER_UPDATE(olap_parent->_cached_pages_num_counter, stats.cached_pages_num);

    COUNTER_UPDATE(olap_parent->_bitmap_index_filter_counter, stats.rows_bitmap_index_filtered);
    COUNTER_UPDATE(olap_parent->_bitmap_index_filter_timer, stats.bitmap_index_filter_timer);

    COUNTER_UPDATE(olap_parent->_filtered_segment_counter, stats.filtered_segment_number);
    COUNTER_UPDATE(olap_parent->_total_segment_counter, stats.total_segment_number);

    // Update metrics
    DorisMetrics::instance()->query_scan_bytes->increment(_compressed_bytes_read);
    DorisMetrics::instance()->query_scan_rows->increment(_raw_rows_read);
    _tablet->query_scan_bytes->increment(_compressed_bytes_read);
    _tablet->query_scan_rows->increment(_raw_rows_read);
    _tablet->query_scan_count->increment(1);
}

} // namespace doris::vectorized
