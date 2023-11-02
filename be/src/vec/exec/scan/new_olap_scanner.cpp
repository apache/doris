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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdlib.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <ostream>
#include <set>
#include <shared_mutex>

#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "exec/olap_utils.h"
#include "exprs/function_filter.h"
#include "io/cache/block/block_file_cache_profile.h"
#include "io/io_common.h"
#include "olap/olap_common.h"
#include "olap/olap_tuple.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/schema_cache.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/olap/block_reader.h"

namespace doris::vectorized {

using ReadSource = TabletReader::ReadSource;

NewOlapScanner::NewOlapScanner(RuntimeState* state, NewOlapScanNode* parent, int64_t limit,
                               bool aggregation, const TPaloScanRange& scan_range,
                               const std::vector<OlapScanRange*>& key_ranges,
                               RuntimeProfile* profile)
        : VScanner(state, static_cast<VScanNode*>(parent), limit, profile),
          _aggregation(aggregation),
          _version(-1),
          _scan_range(scan_range),
          _key_ranges(key_ranges) {
    _tablet_schema = std::make_shared<TabletSchema>();
    _is_init = false;
}

NewOlapScanner::NewOlapScanner(RuntimeState* state, NewOlapScanNode* parent, int64_t limit,
                               bool aggregation, const TPaloScanRange& scan_range,
                               const std::vector<OlapScanRange*>& key_ranges,
                               ReadSource read_source, RuntimeProfile* profile)
        : VScanner(state, static_cast<VScanNode*>(parent), limit, profile),
          _aggregation(aggregation),
          _version(-1),
          _scan_range(scan_range),
          _key_ranges(key_ranges) {
    _tablet_reader_params.set_read_source(std::move(read_source));
    _tablet_schema = std::make_shared<TabletSchema>();
    _is_init = false;
}

static std::string read_columns_to_string(TabletSchemaSPtr tablet_schema,
                                          const std::vector<uint32_t>& read_columns) {
    // avoid too long for one line,
    // it is hard to display in `show profile` stmt if one line is too long.
    const int col_per_line = 10;
    int i = 0;
    std::string read_columns_string;
    read_columns_string += "[";
    for (auto it = read_columns.cbegin(); it != read_columns.cend(); it++) {
        if (it != read_columns.cbegin()) {
            read_columns_string += ", ";
        }
        read_columns_string += tablet_schema->columns().at(*it).name();
        if (i >= col_per_line) {
            read_columns_string += "\n";
            i = 0;
        } else {
            ++i;
        }
    }
    read_columns_string += "]";
    return read_columns_string;
}

Status NewOlapScanner::prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    return VScanner::prepare(state, conjuncts);
}

Status NewOlapScanner::init() {
    _is_init = true;
    auto parent = static_cast<NewOlapScanNode*>(_parent);

    for (auto& ctx : parent->_common_expr_ctxs_push_down) {
        VExprContextSPtr context;
        RETURN_IF_ERROR(ctx->clone(_state, context));
        _common_expr_ctxs_push_down.emplace_back(context);
    }

    // set limit to reduce end of rowset and segment mem use
    _tablet_reader = std::make_unique<BlockReader>();
    // batch size is passed down to segment iterator, use _state->batch_size()
    // instead of _parent->limit(), because if _parent->limit() is a very small
    // value (e.g. select a from t where a .. and b ... limit 1),
    // it will be very slow when reading data in segment iterator
    _tablet_reader->set_batch_size(_state->batch_size());

    // Get olap table
    TTabletId tablet_id = _scan_range.tablet_id;
    _version = strtoul(_scan_range.version.c_str(), nullptr, 10);
    TabletSchemaSPtr cached_schema;
    std::string schema_key;
    {
        auto [tablet, status] =
                StorageEngine::instance()->tablet_manager()->get_tablet_and_status(tablet_id, true);
        RETURN_IF_ERROR(status);
        _tablet = std::move(tablet);
        TOlapScanNode& olap_scan_node = ((NewOlapScanNode*)_parent)->_olap_scan_node;
        if (olap_scan_node.__isset.schema_version && olap_scan_node.__isset.columns_desc &&
            !olap_scan_node.columns_desc.empty() &&
            olap_scan_node.columns_desc[0].col_unique_id >= 0) {
            schema_key = SchemaCache::get_schema_key(tablet_id, olap_scan_node.columns_desc,
                                                     olap_scan_node.schema_version,
                                                     SchemaCache::Type::TABLET_SCHEMA);
            cached_schema = SchemaCache::instance()->get_schema<TabletSchemaSPtr>(schema_key);
        }
        if (cached_schema) {
            _tablet_schema = cached_schema;
        } else {
            _tablet_schema->copy_from(*_tablet->tablet_schema());
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
                if (olap_scan_node.__isset.schema_version) {
                    _tablet_schema->set_schema_version(olap_scan_node.schema_version);
                }
            }
            if (olap_scan_node.__isset.indexes_desc) {
                _tablet_schema->update_indexes_from_thrift(olap_scan_node.indexes_desc);
            }
        }

        if (_tablet_reader_params.rs_splits.empty()) {
            // Non-pipeline mode, Tablet : Scanner = 1 : 1
            // acquire tablet rowset readers at the beginning of the scan node
            // to prevent this case: when there are lots of olap scanners to run for example 10000
            // the rowsets maybe compacted when the last olap scanner starts
            Version rd_version(0, _version);
            ReadSource read_source;
            {
                std::shared_lock rdlock(_tablet->get_header_lock());
                auto st = _tablet->capture_rs_readers(rd_version, &read_source.rs_splits);
                if (!st.ok()) {
                    LOG(WARNING) << "fail to init reader.res=" << st;
                    return Status::InternalError(
                            "failed to initialize storage reader. tablet_id={} : {}",
                            _tablet->tablet_id(), st.to_string());
                }
            }
            if (!_state->skip_delete_predicate()) {
                read_source.fill_delete_predicates();
            }
            _tablet_reader_params.set_read_source(std::move(read_source));
        }

        // Initialize tablet_reader_params
        RETURN_IF_ERROR(_init_tablet_reader_params(_key_ranges, parent->_olap_filters,
                                                   parent->_filter_predicates,
                                                   parent->_push_down_functions));
    }

    // add read columns in profile
    if (_state->enable_profile()) {
        _profile->add_info_string("ReadColumns",
                                  read_columns_to_string(_tablet_schema, _return_columns));
    }

    if (!cached_schema && !schema_key.empty()) {
        SchemaCache::instance()->insert_schema(schema_key, _tablet_schema);
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

void NewOlapScanner::set_compound_filters(const std::vector<TCondition>& compound_filters) {
    _compound_filters = compound_filters;
}

// it will be called under tablet read lock because capture rs readers need
Status NewOlapScanner::_init_tablet_reader_params(
        const std::vector<OlapScanRange*>& key_ranges, const std::vector<TCondition>& filters,
        const FilterPredicates& filter_predicates,
        const std::vector<FunctionFilter>& function_filters) {
    // if the table with rowset [0-x] or [0-1] [2-y], and [0-1] is empty
    const bool single_version = _tablet_reader_params.has_single_version();

    if (_state->skip_storage_engine_merge()) {
        _tablet_reader_params.direct_mode = true;
        _aggregation = true;
    } else {
        _tablet_reader_params.direct_mode =
                _aggregation || single_version ||
                (_parent->get_push_down_agg_type() != TPushAggOp::NONE &&
                 _parent->get_push_down_agg_type() != TPushAggOp::COUNT_ON_INDEX);
    }

    RETURN_IF_ERROR(_init_return_columns());

    _tablet_reader_params.tablet = _tablet;
    _tablet_reader_params.tablet_schema = _tablet_schema;
    _tablet_reader_params.reader_type = ReaderType::READER_QUERY;
    _tablet_reader_params.aggregation = _aggregation;
    _tablet_reader_params.push_down_agg_type_opt = _parent->get_push_down_agg_type();
    _tablet_reader_params.version = Version(0, _version);

    // TODO: If a new runtime filter arrives after `_conjuncts` move to `_common_expr_ctxs_push_down`,
    if (_common_expr_ctxs_push_down.empty()) {
        for (auto& conjunct : _conjuncts) {
            _tablet_reader_params.remaining_conjunct_roots.emplace_back(conjunct->root());
        }
    } else {
        for (auto& ctx : _common_expr_ctxs_push_down) {
            _tablet_reader_params.remaining_conjunct_roots.emplace_back(ctx->root());
        }
    }

    _tablet_reader_params.common_expr_ctxs_push_down = _common_expr_ctxs_push_down;
    _tablet_reader_params.output_columns = ((NewOlapScanNode*)_parent)->_maybe_read_column_ids;

    // Condition
    for (auto& filter : filters) {
        _tablet_reader_params.conditions.push_back(filter);
    }

    std::copy(_compound_filters.cbegin(), _compound_filters.cend(),
              std::inserter(_tablet_reader_params.conditions_except_leafnode_of_andnode,
                            _tablet_reader_params.conditions_except_leafnode_of_andnode.begin()));

    std::copy(filter_predicates.bloom_filters.cbegin(), filter_predicates.bloom_filters.cend(),
              std::inserter(_tablet_reader_params.bloom_filters,
                            _tablet_reader_params.bloom_filters.begin()));
    std::copy(filter_predicates.bitmap_filters.cbegin(), filter_predicates.bitmap_filters.cend(),
              std::inserter(_tablet_reader_params.bitmap_filters,
                            _tablet_reader_params.bitmap_filters.begin()));

    std::copy(filter_predicates.in_filters.cbegin(), filter_predicates.in_filters.cend(),
              std::inserter(_tablet_reader_params.in_filters,
                            _tablet_reader_params.in_filters.begin()));

    std::copy(function_filters.cbegin(), function_filters.cend(),
              std::inserter(_tablet_reader_params.function_filters,
                            _tablet_reader_params.function_filters.begin()));

    // Merge the columns in delete predicate that not in latest schema in to current tablet schema
    for (auto& del_pred : _tablet_reader_params.delete_predicates) {
        _tablet_schema->merge_dropped_columns(*del_pred->tablet_schema());
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

    _tablet_reader_params.use_page_cache = _state->enable_page_cache();

    if (_tablet->enable_unique_key_merge_on_write() && !_state->skip_delete_bitmap()) {
        _tablet_reader_params.delete_bitmap = &_tablet->tablet_meta()->delete_bitmap();
    }

    if (!_state->skip_storage_engine_merge()) {
        TOlapScanNode& olap_scan_node = ((NewOlapScanNode*)_parent)->_olap_scan_node;
        // order by table keys optimization for topn
        // will only read head/tail of data file since it's already sorted by keys
        if (olap_scan_node.__isset.sort_info && olap_scan_node.sort_info.is_asc_order.size() > 0) {
            _limit = _parent->_limit_per_scanner;
            _tablet_reader_params.read_orderby_key = true;
            if (!olap_scan_node.sort_info.is_asc_order[0]) {
                _tablet_reader_params.read_orderby_key_reverse = true;
            }
            _tablet_reader_params.read_orderby_key_num_prefix_columns =
                    olap_scan_node.sort_info.is_asc_order.size();
            _tablet_reader_params.read_orderby_key_limit = _limit;
            _tablet_reader_params.filter_block_conjuncts = _conjuncts;
        }

        // runtime predicate push down optimization for topn
        _tablet_reader_params.use_topn_opt =
                ((NewOlapScanNode*)_parent)->_olap_scan_node.use_topn_opt;
    }

    // If this is a Two-Phase read query, and we need to delay the release of Rowset
    // by rowset->update_delayed_expired_timestamp().This could expand the lifespan of Rowset
    if (_tablet_schema->field_index(BeConsts::ROWID_COL) >= 0) {
        constexpr static int delayed_s = 60;
        for (auto rs_reader : _tablet_reader_params.rs_splits) {
            uint64_t delayed_expired_timestamp =
                    UnixSeconds() + _tablet_reader_params.runtime_state->execution_timeout() +
                    delayed_s;
            rs_reader.rs_reader->rowset()->update_delayed_expired_timestamp(
                    delayed_expired_timestamp);
            StorageEngine::instance()->add_quering_rowset(rs_reader.rs_reader->rowset());
        }
    }

    return Status::OK();
}

Status NewOlapScanner::_init_return_columns() {
    for (auto slot : _output_tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        if (!slot->need_materialize()) {
            continue;
        }
        int32_t index = slot->col_unique_id() >= 0
                                ? _tablet_schema->field_index(slot->col_unique_id())
                                : _tablet_schema->field_index(slot->col_name());

        if (index < 0) {
            return Status::InternalError(
                    "field name is invalid. field={}, field_name_to_index={}, col_unique_id={}",
                    slot->col_name(), _tablet_schema->get_all_field_names(), slot->col_unique_id());
        }
        _return_columns.push_back(index);
        if (slot->is_nullable() && !_tablet_schema->column(index).is_nullable()) {
            _tablet_columns_convert_to_null_set.emplace(index);
        } else if (!slot->is_nullable() && _tablet_schema->column(index).is_nullable()) {
            return Status::Error<ErrorCode::INVALID_SCHEMA>(
                    "slot(id: {}, name: {})'s nullable does not match "
                    "column(tablet id: {}, index: {}, name: {}) ",
                    slot->id(), slot->col_name(), _tablet_schema->table_id(), index,
                    _tablet_schema->column(index).name());
        }
    }

    if (_return_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

doris::TabletStorageType NewOlapScanner::get_storage_type() {
    int local_reader = 0;
    for (const auto& reader : _tablet_reader_params.rs_splits) {
        local_reader += reader.rs_reader->rowset()->is_local();
    }
    int total_reader = _tablet_reader_params.rs_splits.size();

    if (local_reader == total_reader) {
        return doris::TabletStorageType::STORAGE_TYPE_LOCAL;
    } else if (local_reader == 0) {
        return doris::TabletStorageType::STORAGE_TYPE_REMOTE;
    }
    return doris::TabletStorageType::STORAGE_TYPE_REMOTE_AND_LOCAL;
}

Status NewOlapScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    // Read one block from block reader
    // ATTN: Here we need to let the _get_block_impl method guarantee the semantics of the interface,
    // that is, eof can be set to true only when the returned block is empty.
    RETURN_IF_ERROR(_tablet_reader->next_block_with_aggregation(block, eof));
    if (!_profile_updated) {
        _profile_updated = _tablet_reader->update_profile(_profile);
    }
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
    _tablet_reader_params.rs_splits.clear();
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
    //  Please don't directly enable the profile here, we need to set QueryStatistics using the counter inside.
    if (_has_updated_counter) {
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
    COUNTER_UPDATE(olap_parent->_expr_filter_timer, stats.expr_filter_ns);
    COUNTER_UPDATE(olap_parent->_block_init_timer, stats.block_init_ns);
    COUNTER_UPDATE(olap_parent->_block_init_seek_timer, stats.block_init_seek_ns);
    COUNTER_UPDATE(olap_parent->_block_init_seek_counter, stats.block_init_seek_num);
    COUNTER_UPDATE(olap_parent->_block_conditions_filtered_timer,
                   stats.block_conditions_filtered_ns);
    COUNTER_UPDATE(olap_parent->_first_read_timer, stats.first_read_ns);
    COUNTER_UPDATE(olap_parent->_second_read_timer, stats.second_read_ns);
    COUNTER_UPDATE(olap_parent->_first_read_seek_timer, stats.block_first_read_seek_ns);
    COUNTER_UPDATE(olap_parent->_first_read_seek_counter, stats.block_first_read_seek_num);
    COUNTER_UPDATE(olap_parent->_lazy_read_timer, stats.lazy_read_ns);
    COUNTER_UPDATE(olap_parent->_lazy_read_seek_timer, stats.block_lazy_read_seek_ns);
    COUNTER_UPDATE(olap_parent->_lazy_read_seek_counter, stats.block_lazy_read_seek_num);
    COUNTER_UPDATE(olap_parent->_output_col_timer, stats.output_col_ns);
    COUNTER_UPDATE(olap_parent->_rows_vec_cond_filtered_counter, stats.rows_vec_cond_filtered);
    COUNTER_UPDATE(olap_parent->_rows_short_circuit_cond_filtered_counter,
                   stats.rows_short_circuit_cond_filtered);
    COUNTER_UPDATE(olap_parent->_rows_vec_cond_input_counter, stats.vec_cond_input_rows);
    COUNTER_UPDATE(olap_parent->_rows_short_circuit_cond_input_counter,
                   stats.short_circuit_cond_input_rows);

    for (auto& [id, info] : stats.filter_info) {
        olap_parent->add_filter_info(id, info);
    }

    COUNTER_UPDATE(olap_parent->_stats_filtered_counter, stats.rows_stats_filtered);
    COUNTER_UPDATE(olap_parent->_dict_filtered_counter, stats.rows_dict_filtered);
    COUNTER_UPDATE(olap_parent->_bf_filtered_counter, stats.rows_bf_filtered);
    COUNTER_UPDATE(olap_parent->_del_filtered_counter, stats.rows_del_filtered);
    COUNTER_UPDATE(olap_parent->_del_filtered_counter, stats.rows_del_by_bitmap);
    COUNTER_UPDATE(olap_parent->_del_filtered_counter, stats.rows_vec_del_cond_filtered);

    COUNTER_UPDATE(olap_parent->_conditions_filtered_counter, stats.rows_conditions_filtered);
    COUNTER_UPDATE(olap_parent->_key_range_filtered_counter, stats.rows_key_range_filtered);

    COUNTER_UPDATE(olap_parent->_total_pages_num_counter, stats.total_pages_num);
    COUNTER_UPDATE(olap_parent->_cached_pages_num_counter, stats.cached_pages_num);

    COUNTER_UPDATE(olap_parent->_bitmap_index_filter_counter, stats.rows_bitmap_index_filtered);
    COUNTER_UPDATE(olap_parent->_bitmap_index_filter_timer, stats.bitmap_index_filter_timer);

    COUNTER_UPDATE(olap_parent->_inverted_index_filter_counter, stats.rows_inverted_index_filtered);
    COUNTER_UPDATE(olap_parent->_inverted_index_filter_timer, stats.inverted_index_filter_timer);
    COUNTER_UPDATE(olap_parent->_inverted_index_query_cache_hit_counter,
                   stats.inverted_index_query_cache_hit);
    COUNTER_UPDATE(olap_parent->_inverted_index_query_cache_miss_counter,
                   stats.inverted_index_query_cache_miss);
    COUNTER_UPDATE(olap_parent->_inverted_index_query_timer, stats.inverted_index_query_timer);
    COUNTER_UPDATE(olap_parent->_inverted_index_query_bitmap_copy_timer,
                   stats.inverted_index_query_bitmap_copy_timer);
    COUNTER_UPDATE(olap_parent->_inverted_index_query_bitmap_op_timer,
                   stats.inverted_index_query_bitmap_op_timer);
    COUNTER_UPDATE(olap_parent->_inverted_index_searcher_open_timer,
                   stats.inverted_index_searcher_open_timer);
    COUNTER_UPDATE(olap_parent->_inverted_index_searcher_search_timer,
                   stats.inverted_index_searcher_search_timer);

    if (config::enable_file_cache) {
        io::FileCacheProfileReporter cache_profile(olap_parent->_segment_profile.get());
        cache_profile.update(&stats.file_cache_stats);
    }

    COUNTER_UPDATE(olap_parent->_output_index_result_column_timer,
                   stats.output_index_result_column_timer);

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
