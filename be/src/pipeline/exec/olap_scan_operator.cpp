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

#include "pipeline/exec/olap_scan_operator.h"

#include <fmt/format.h>

#include <memory>
#include <numeric>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/config.h"
#include "olap/parallel_scanner_builder.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "pipeline/exec/scan_operator.h"
#include "pipeline/query_cache/query_cache.h"
#include "runtime_filter/runtime_filter_consumer_helper.h"
#include "service/backend_options.h"
#include "util/runtime_profile.h"
#include "util/to_string.h"
#include "vec/exec/scan/olap_scanner.h"
#include "vec/exprs/score_runtime.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/in.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Status OlapScanLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    const TOlapScanNode& olap_scan_node = _parent->cast<OlapScanOperatorX>()._olap_scan_node;

    if (olap_scan_node.__isset.score_sort_info && olap_scan_node.__isset.score_sort_limit) {
        const doris::TExpr& ordering_expr = olap_scan_node.score_sort_info.ordering_exprs.front();
        const bool asc = olap_scan_node.score_sort_info.is_asc_order[0];
        const size_t limit = olap_scan_node.score_sort_limit;
        std::shared_ptr<vectorized::VExprContext> ordering_expr_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(ordering_expr, ordering_expr_ctx));
        _score_runtime = vectorized::ScoreRuntime::create_shared(ordering_expr_ctx, asc, limit);
    }

    RETURN_IF_ERROR(Base::init(state, info));
    RETURN_IF_ERROR(_sync_cloud_tablets(state));
    return Status::OK();
}

Status OlapScanLocalState::_init_profile() {
    RETURN_IF_ERROR(ScanLocalState<OlapScanLocalState>::_init_profile());
    // Rows read from storage.
    // Include the rows read from doris page cache.
    _scan_rows = ADD_COUNTER(custom_profile(), "ScanRows", TUnit::UNIT);
    // 1. init segment profile
    _segment_profile.reset(new RuntimeProfile("SegmentIterator"));
    _scanner_profile->add_child(_segment_profile.get(), true, nullptr);

    // 2. init timer and counters
    _reader_init_timer = ADD_TIMER(_scanner_profile, "ReaderInitTime");
    _scanner_init_timer = ADD_TIMER(_scanner_profile, "ScannerInitTime");
    _process_conjunct_timer = ADD_TIMER(custom_profile(), "ProcessConjunctTime");
    _read_compressed_counter = ADD_COUNTER(_segment_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter =
            ADD_COUNTER(_segment_profile, "UncompressedBytesRead", TUnit::BYTES);
    _block_load_timer = ADD_TIMER(_segment_profile, "BlockLoadTime");
    _block_load_counter = ADD_COUNTER(_segment_profile, "BlocksLoad", TUnit::UNIT);
    _block_fetch_timer = ADD_TIMER(_scanner_profile, "BlockFetchTime");
    _delete_bitmap_get_agg_timer = ADD_TIMER(_scanner_profile, "DeleteBitmapGetAggTime");
    if (config::is_cloud_mode()) {
        static const char* sync_rowset_timer_name = "SyncRowsetTime";
        _sync_rowset_timer = ADD_TIMER(_scanner_profile, sync_rowset_timer_name);
        _sync_rowset_tablet_meta_cache_hit =
                ADD_CHILD_COUNTER(_scanner_profile, "SyncRowsetTabletMetaCacheHitCount",
                                  TUnit::UNIT, sync_rowset_timer_name);
        _sync_rowset_tablet_meta_cache_miss =
                ADD_CHILD_COUNTER(_scanner_profile, "SyncRowsetTabletMetaCacheMissCount",
                                  TUnit::UNIT, sync_rowset_timer_name);
        _sync_rowset_get_remote_tablet_meta_rpc_timer = ADD_CHILD_TIMER(
                _scanner_profile, "SyncRowsetGetRemoteTabletMetaRpcTime", sync_rowset_timer_name);
        _sync_rowset_tablets_rowsets_total_num =
                ADD_CHILD_COUNTER(_scanner_profile, "SyncRowsetTabletsRowsetsTotatCount",
                                  TUnit::UNIT, sync_rowset_timer_name);
        _sync_rowset_get_remote_rowsets_num =
                ADD_CHILD_COUNTER(_scanner_profile, "SyncRowsetGetRemoteRowsetsCount", TUnit::UNIT,
                                  sync_rowset_timer_name);
        _sync_rowset_get_remote_rowsets_rpc_timer = ADD_CHILD_TIMER(
                _scanner_profile, "SyncRowsetGetRemoteRowsetsRpcTime", sync_rowset_timer_name);
        _sync_rowset_get_local_delete_bitmap_rowsets_num =
                ADD_CHILD_COUNTER(_scanner_profile, "SyncRowsetGetLocalDeleteBitmapRowsetsCount",
                                  TUnit::UNIT, sync_rowset_timer_name);
        _sync_rowset_get_remote_delete_bitmap_rowsets_num =
                ADD_CHILD_COUNTER(_scanner_profile, "SyncRowsetGetRemoteDeleteBitmapRowsetsCount",
                                  TUnit::UNIT, sync_rowset_timer_name);
        _sync_rowset_get_remote_delete_bitmap_key_count =
                ADD_CHILD_COUNTER(_scanner_profile, "SyncRowsetGetRemoteDeleteBitmapKeyCount",
                                  TUnit::UNIT, sync_rowset_timer_name);
        _sync_rowset_get_remote_delete_bitmap_bytes =
                ADD_CHILD_COUNTER(_scanner_profile, "SyncRowsetGetRemoteDeleteBitmapBytes",
                                  TUnit::BYTES, sync_rowset_timer_name);
        _sync_rowset_get_remote_delete_bitmap_rpc_timer = ADD_CHILD_TIMER(
                _scanner_profile, "SyncRowsetGetRemoteDeleteBitmapRpcTime", sync_rowset_timer_name);
    }
    _block_init_timer = ADD_TIMER(_segment_profile, "BlockInitTime");
    _block_init_seek_timer = ADD_TIMER(_segment_profile, "BlockInitSeekTime");
    _block_init_seek_counter = ADD_COUNTER(_segment_profile, "BlockInitSeekCount", TUnit::UNIT);
    _segment_generate_row_range_by_keys_timer =
            ADD_TIMER(_segment_profile, "GenerateRowRangeByKeysTime");
    _segment_generate_row_range_by_column_conditions_timer =
            ADD_TIMER(_segment_profile, "GenerateRowRangeByColumnConditionsTime");
    _segment_generate_row_range_by_bf_timer =
            ADD_TIMER(_segment_profile, "GenerateRowRangeByBloomFilterIndexTime");
    _collect_iterator_merge_next_timer = ADD_TIMER(_segment_profile, "CollectIteratorMergeTime");
    _segment_generate_row_range_by_zonemap_timer =
            ADD_TIMER(_segment_profile, "GenerateRowRangeByZoneMapIndexTime");
    _segment_generate_row_range_by_dict_timer =
            ADD_TIMER(_segment_profile, "GenerateRowRangeByDictTime");

    _rows_vec_cond_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsVectorPredFiltered", TUnit::UNIT);
    _rows_short_circuit_cond_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsShortCircuitPredFiltered", TUnit::UNIT);
    _rows_expr_cond_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsExprPredFiltered", TUnit::UNIT);
    _rows_vec_cond_input_counter =
            ADD_COUNTER(_segment_profile, "RowsVectorPredInput", TUnit::UNIT);
    _rows_short_circuit_cond_input_counter =
            ADD_COUNTER(_segment_profile, "RowsShortCircuitPredInput", TUnit::UNIT);
    _rows_expr_cond_input_counter = ADD_COUNTER(_segment_profile, "RowsExprPredInput", TUnit::UNIT);
    _vec_cond_timer = ADD_TIMER(_segment_profile, "VectorPredEvalTime");
    _short_cond_timer = ADD_TIMER(_segment_profile, "ShortPredEvalTime");
    _expr_filter_timer = ADD_TIMER(_segment_profile, "ExprFilterEvalTime");
    _predicate_column_read_timer = ADD_TIMER(_segment_profile, "PredicateColumnReadTime");
    _non_predicate_column_read_timer = ADD_TIMER(_segment_profile, "NonPredicateColumnReadTime");
    _predicate_column_read_seek_timer = ADD_TIMER(_segment_profile, "PredicateColumnReadSeekTime");
    _predicate_column_read_seek_counter =
            ADD_COUNTER(_segment_profile, "PredicateColumnReadSeekCount", TUnit::UNIT);

    _lazy_read_timer = ADD_TIMER(_segment_profile, "LazyReadTime");
    _lazy_read_seek_timer = ADD_TIMER(_segment_profile, "LazyReadSeekTime");
    _lazy_read_seek_counter = ADD_COUNTER(_segment_profile, "LazyReadSeekCount", TUnit::UNIT);

    _output_col_timer = ADD_TIMER(_segment_profile, "OutputColumnTime");

    _stats_filtered_counter = ADD_COUNTER(_segment_profile, "RowsStatsFiltered", TUnit::UNIT);
    _stats_rp_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsZoneMapRuntimePredicateFiltered", TUnit::UNIT);
    _bf_filtered_counter = ADD_COUNTER(_segment_profile, "RowsBloomFilterFiltered", TUnit::UNIT);
    _dict_filtered_counter = ADD_COUNTER(_segment_profile, "SegmentDictFiltered", TUnit::UNIT);
    _del_filtered_counter = ADD_COUNTER(_scanner_profile, "RowsDelFiltered", TUnit::UNIT);
    _conditions_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsConditionsFiltered", TUnit::UNIT);
    _key_range_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsKeyRangeFiltered", TUnit::UNIT);

    _io_timer = ADD_TIMER(_segment_profile, "IOTimer");
    _decompressor_timer = ADD_TIMER(_segment_profile, "DecompressorTimer");

    _total_pages_num_counter = ADD_COUNTER(_segment_profile, "TotalPagesNum", TUnit::UNIT);
    _cached_pages_num_counter = ADD_COUNTER(_segment_profile, "CachedPagesNum", TUnit::UNIT);

    _bitmap_index_filter_counter =
            ADD_COUNTER(_segment_profile, "RowsBitmapIndexFiltered", TUnit::UNIT);
    _bitmap_index_filter_timer = ADD_TIMER(_segment_profile, "BitmapIndexFilterTimer");

    _inverted_index_filter_counter =
            ADD_COUNTER(_segment_profile, "RowsInvertedIndexFiltered", TUnit::UNIT);
    _inverted_index_filter_timer = ADD_TIMER(_segment_profile, "InvertedIndexFilterTime");
    _inverted_index_query_cache_hit_counter =
            ADD_COUNTER(_segment_profile, "InvertedIndexQueryCacheHit", TUnit::UNIT);
    _inverted_index_query_cache_miss_counter =
            ADD_COUNTER(_segment_profile, "InvertedIndexQueryCacheMiss", TUnit::UNIT);
    _inverted_index_query_timer = ADD_TIMER(_segment_profile, "InvertedIndexQueryTime");
    _inverted_index_query_null_bitmap_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexQueryNullBitmapTime");
    _inverted_index_query_bitmap_copy_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexQueryBitmapCopyTime");
    _inverted_index_searcher_open_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexSearcherOpenTime");
    _inverted_index_searcher_search_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexSearcherSearchTime");
    _inverted_index_searcher_search_init_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexSearcherSearchInitTime");
    _inverted_index_searcher_search_exec_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexSearcherSearchExecTime");
    _inverted_index_searcher_cache_hit_counter =
            ADD_COUNTER(_segment_profile, "InvertedIndexSearcherCacheHit", TUnit::UNIT);
    _inverted_index_searcher_cache_miss_counter =
            ADD_COUNTER(_segment_profile, "InvertedIndexSearcherCacheMiss", TUnit::UNIT);
    _inverted_index_downgrade_count_counter =
            ADD_COUNTER(_segment_profile, "InvertedIndexDowngradeCount", TUnit::UNIT);
    _inverted_index_analyzer_timer = ADD_TIMER(_segment_profile, "InvertedIndexAnalyzerTime");
    _inverted_index_lookup_timer = ADD_TIMER(_segment_profile, "InvertedIndexLookupTimer");

    _output_index_result_column_timer = ADD_TIMER(_segment_profile, "OutputIndexResultColumnTime");
    _filtered_segment_counter = ADD_COUNTER(_segment_profile, "NumSegmentFiltered", TUnit::UNIT);
    _total_segment_counter = ADD_COUNTER(_segment_profile, "NumSegmentTotal", TUnit::UNIT);
    _tablet_counter = ADD_COUNTER(custom_profile(), "TabletNum", TUnit::UNIT);
    _key_range_counter = ADD_COUNTER(custom_profile(), "KeyRangesNum", TUnit::UNIT);
    _tablet_reader_init_timer = ADD_TIMER(_scanner_profile, "TabletReaderInitTimer");
    _tablet_reader_capture_rs_readers_timer =
            ADD_TIMER(_scanner_profile, "TabletReaderCaptureRsReadersTimer");
    _tablet_reader_init_return_columns_timer =
            ADD_TIMER(_scanner_profile, "TabletReaderInitReturnColumnsTimer");
    _tablet_reader_init_keys_param_timer =
            ADD_TIMER(_scanner_profile, "TabletReaderInitKeysParamTimer");
    _tablet_reader_init_orderby_keys_param_timer =
            ADD_TIMER(_scanner_profile, "TabletReaderInitOrderbyKeysParamTimer");
    _tablet_reader_init_conditions_param_timer =
            ADD_TIMER(_scanner_profile, "TabletReaderInitConditionsParamTimer");
    _tablet_reader_init_delete_condition_param_timer =
            ADD_TIMER(_scanner_profile, "TabletReaderInitDeleteConditionParamTimer");
    _block_reader_vcollect_iter_init_timer =
            ADD_TIMER(_scanner_profile, "BlockReaderVcollectIterInitTimer");
    _block_reader_rs_readers_init_timer =
            ADD_TIMER(_scanner_profile, "BlockReaderRsReadersInitTimer");
    _block_reader_build_heap_init_timer =
            ADD_TIMER(_scanner_profile, "BlockReaderBuildHeapInitTimer");

    _rowset_reader_get_segment_iterators_timer =
            ADD_TIMER(_scanner_profile, "RowsetReaderGetSegmentIteratorsTimer");
    _rowset_reader_create_iterators_timer =
            ADD_TIMER(_scanner_profile, "RowsetReaderCreateIteratorsTimer");
    _rowset_reader_init_iterators_timer =
            ADD_TIMER(_scanner_profile, "RowsetReaderInitIteratorsTimer");
    _rowset_reader_load_segments_timer =
            ADD_TIMER(_scanner_profile, "RowsetReaderLoadSegmentsTimer");

    _segment_iterator_init_timer = ADD_TIMER(_scanner_profile, "SegmentIteratorInitTimer");
    _segment_iterator_init_return_column_iterators_timer =
            ADD_TIMER(_scanner_profile, "SegmentIteratorInitReturnColumnIteratorsTimer");
    _segment_iterator_init_bitmap_index_iterators_timer =
            ADD_TIMER(_scanner_profile, "SegmentIteratorInitBitmapIndexIteratorsTimer");
    _segment_iterator_init_index_iterators_timer =
            ADD_TIMER(_scanner_profile, "SegmentIteratorInitIndexIteratorsTimer");

    _segment_create_column_readers_timer =
            ADD_TIMER(_scanner_profile, "SegmentCreateColumnReadersTimer");
    _segment_load_index_timer = ADD_TIMER(_scanner_profile, "SegmentLoadIndexTimer");

    _index_filter_profile = std::make_unique<RuntimeProfile>("IndexFilter");
    _scanner_profile->add_child(_index_filter_profile.get(), true, nullptr);

    return Status::OK();
}

Status OlapScanLocalState::_process_conjuncts(RuntimeState* state) {
    SCOPED_TIMER(_process_conjunct_timer);
    RETURN_IF_ERROR(ScanLocalState::_process_conjuncts(state));
    if (ScanLocalState::_eos) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_build_key_ranges_and_filters());
    return Status::OK();
}

bool OlapScanLocalState::_is_key_column(const std::string& key_name) {
    // all column in dup_keys table or unique_keys with merge on write table olap scan node threat
    // as key column
    if (_storage_no_merge()) {
        return true;
    }

    auto& p = _parent->cast<OlapScanOperatorX>();
    auto res = std::find(p._olap_scan_node.key_column_name.begin(),
                         p._olap_scan_node.key_column_name.end(), key_name);
    return res != p._olap_scan_node.key_column_name.end();
}

Status OlapScanLocalState::_should_push_down_function_filter(vectorized::VectorizedFnCall* fn_call,
                                                             vectorized::VExprContext* expr_ctx,
                                                             StringRef* constant_str,
                                                             doris::FunctionContext** fn_ctx,
                                                             PushDownType& pdt) {
    // Now only `like` function filters is supported to push down
    if (fn_call->fn().name.function_name != "like") {
        pdt = PushDownType::UNACCEPTABLE;
        return Status::OK();
    }

    const auto& children = fn_call->children();
    doris::FunctionContext* func_cxt = expr_ctx->fn_context(fn_call->fn_context_index());
    DCHECK(func_cxt != nullptr);
    DCHECK(children.size() == 2);
    for (size_t i = 0; i < children.size(); i++) {
        if (vectorized::VExpr::expr_without_cast(children[i])->node_type() !=
            TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            continue;
        }
        if (!children[1 - i]->is_constant()) {
            // only handle constant value
            pdt = PushDownType::UNACCEPTABLE;
            return Status::OK();
        } else {
            DCHECK(is_string_type(children[1 - i]->data_type()->get_primitive_type()));
            std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
            RETURN_IF_ERROR(children[1 - i]->get_const_col(expr_ctx, &const_col_wrapper));
            if (const auto* const_column = check_and_get_column<vectorized::ColumnConst>(
                        const_col_wrapper->column_ptr.get())) {
                *constant_str = const_column->get_data_at(0);
            } else {
                pdt = PushDownType::UNACCEPTABLE;
                return Status::OK();
            }
        }
    }
    *fn_ctx = func_cxt;
    pdt = PushDownType::ACCEPTABLE;
    return Status::OK();
}

bool OlapScanLocalState::_should_push_down_common_expr() {
    return state()->enable_common_expr_pushdown() && _storage_no_merge();
}

bool OlapScanLocalState::_storage_no_merge() {
    auto& p = _parent->cast<OlapScanOperatorX>();
    return (p._olap_scan_node.keyType == TKeysType::DUP_KEYS ||
            (p._olap_scan_node.keyType == TKeysType::UNIQUE_KEYS &&
             p._olap_scan_node.__isset.enable_unique_key_merge_on_write &&
             p._olap_scan_node.enable_unique_key_merge_on_write));
}

Status OlapScanLocalState::_init_scanners(std::list<vectorized::ScannerSPtr>* scanners) {
    if (_scan_ranges.empty()) {
        _eos = true;
        _scan_dependency->set_ready();
        return Status::OK();
    }
    SCOPED_TIMER(_scanner_init_timer);

    if (!_conjuncts.empty() && _state->enable_profile()) {
        std::string message;
        for (auto& conjunct : _conjuncts) {
            if (conjunct->root()) {
                if (!message.empty()) {
                    message += ", ";
                }
                message += conjunct->root()->debug_string();
            }
        }
        custom_profile()->add_info_string("RemainedDownPredicates", message);
    }
    auto& p = _parent->cast<OlapScanOperatorX>();

    for (auto uid : p._olap_scan_node.output_column_unique_ids) {
        _maybe_read_column_ids.emplace(uid);
    }

    // ranges constructed from scan keys
    RETURN_IF_ERROR(_scan_keys.get_key_range(&_cond_ranges));
    // if we can't get ranges from conditions, we give it a total range
    if (_cond_ranges.empty()) {
        _cond_ranges.emplace_back(new doris::OlapScanRange());
    }

    bool enable_parallel_scan = state()->enable_parallel_scan();
    bool has_cpu_limit = state()->query_options().__isset.resource_limit &&
                         state()->query_options().resource_limit.__isset.cpu_limit;

    // The flag of preagg's meaning is whether return pre agg data(or partial agg data)
    // PreAgg ON: The storage layer returns partially aggregated data without additional processing. (Fast data reading)
    // for example, if a table is select userid,count(*) from base table.
    // And the user send a query like select userid,count(*) from base table group by userid.
    // then the storage layer do not need do aggregation, it could just return the partial agg data, because the compute layer will do aggregation.
    // PreAgg OFF: The storage layer must complete pre-aggregation and return fully aggregated data. (Slow data reading)
    if (enable_parallel_scan && !p._should_run_serial && !has_cpu_limit &&
        p._push_down_agg_type == TPushAggOp::NONE &&
        (_storage_no_merge() || p._olap_scan_node.is_preaggregation)) {
        std::vector<OlapScanRange*> key_ranges;
        for (auto& range : _cond_ranges) {
            if (range->begin_scan_range.size() == 1 &&
                range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
                continue;
            }
            key_ranges.emplace_back(range.get());
        }

        ParallelScannerBuilder scanner_builder(this, _tablets, _read_sources, _scanner_profile,
                                               key_ranges, state(), p._limit, true,
                                               p._olap_scan_node.is_preaggregation);

        int max_scanners_count = state()->parallel_scan_max_scanners_count();

        // If the `max_scanners_count` was not set,
        // use `CpuInfo::num_cores()` as the default value.
        if (max_scanners_count <= 0) {
            max_scanners_count = CpuInfo::num_cores();
        }

        // Too small value of `min_rows_per_scanner` is meaningless.
        auto min_rows_per_scanner =
                std::max<int64_t>(1024, state()->parallel_scan_min_rows_per_scanner());
        scanner_builder.set_max_scanners_count(max_scanners_count);
        scanner_builder.set_min_rows_per_scanner(min_rows_per_scanner);

        RETURN_IF_ERROR(scanner_builder.build_scanners(*scanners));
        for (auto& scanner : *scanners) {
            auto* olap_scanner = assert_cast<vectorized::OlapScanner*>(scanner.get());
            RETURN_IF_ERROR(olap_scanner->prepare(state(), _conjuncts));
        }
        return Status::OK();
    }

    int scanners_per_tablet = std::max(1, 64 / (int)_scan_ranges.size());
    for (size_t scan_range_idx = 0; scan_range_idx < _scan_ranges.size(); scan_range_idx++) {
        int64_t version = 0;
        std::from_chars(_scan_ranges[scan_range_idx]->version.data(),
                        _scan_ranges[scan_range_idx]->version.data() +
                                _scan_ranges[scan_range_idx]->version.size(),
                        version);
        std::vector<std::unique_ptr<doris::OlapScanRange>>* ranges = &_cond_ranges;
        int size_based_scanners_per_tablet = 1;

        if (config::doris_scan_range_max_mb > 0) {
            size_based_scanners_per_tablet =
                    std::max(1, (int)(_tablets[scan_range_idx].tablet->tablet_footprint() /
                                      (config::doris_scan_range_max_mb << 20)));
        }
        int ranges_per_scanner =
                std::max(1, (int)ranges->size() /
                                    std::min(scanners_per_tablet, size_based_scanners_per_tablet));
        int64_t num_ranges = ranges->size();
        for (int64_t i = 0; i < num_ranges;) {
            std::vector<doris::OlapScanRange*> scanner_ranges;
            scanner_ranges.push_back((*ranges)[i].get());
            ++i;
            for (int64_t j = 1; i < num_ranges && j < ranges_per_scanner &&
                                (*ranges)[i]->end_include == (*ranges)[i - 1]->end_include;
                 ++j, ++i) {
                scanner_ranges.push_back((*ranges)[i].get());
            }

            COUNTER_UPDATE(_key_range_counter, scanner_ranges.size());
            // `rs_reader` should not be shared by different scanners
            for (auto& split : _read_sources[scan_range_idx].rs_splits) {
                split.rs_reader = split.rs_reader->clone();
            }
            auto scanner = vectorized::OlapScanner::create_shared(
                    this, vectorized::OlapScanner::Params {
                                  state(),
                                  _scanner_profile.get(),
                                  scanner_ranges,
                                  _tablets[scan_range_idx].tablet,
                                  version,
                                  _read_sources[scan_range_idx],
                                  p._limit,
                                  p._olap_scan_node.is_preaggregation,
                          });
            RETURN_IF_ERROR(scanner->prepare(state(), _conjuncts));
            scanners->push_back(std::move(scanner));
        }
    }
    _tablets.clear();
    _read_sources.clear();

    return Status::OK();
}

Status OlapScanLocalState::_sync_cloud_tablets(RuntimeState* state) {
    if (config::is_cloud_mode() && !_sync_tablet) {
        _pending_tablets_num = _scan_ranges.size();
        if (_pending_tablets_num > 0) {
            _sync_cloud_tablets_watcher.start();
            _cloud_tablet_dependency = Dependency::create_shared(
                    _parent->operator_id(), _parent->node_id(), "CLOUD_TABLET_DEP");
            _tablets.resize(_scan_ranges.size());
            std::vector<std::function<Status()>> tasks;
            _sync_statistics.resize(_scan_ranges.size());
            for (size_t i = 0; i < _scan_ranges.size(); i++) {
                auto* sync_stats = &_sync_statistics[i];
                int64_t version = 0;
                std::from_chars(_scan_ranges[i]->version.data(),
                                _scan_ranges[i]->version.data() + _scan_ranges[i]->version.size(),
                                version);
                auto task_ctx = state->get_task_execution_context();
                tasks.emplace_back([this, sync_stats, version, i, task_ctx]() {
                    auto task_lock = task_ctx.lock();
                    if (task_lock == nullptr) {
                        return Status::OK();
                    }
                    Defer defer([&] {
                        if (_pending_tablets_num.fetch_sub(1) == 1) {
                            _cloud_tablet_dependency->set_ready();
                            _sync_cloud_tablets_watcher.stop();
                        }
                    });
                    auto tablet =
                            DORIS_TRY(ExecEnv::get_tablet(_scan_ranges[i]->tablet_id, sync_stats));
                    _tablets[i] = {std::move(tablet), version};
                    SyncOptions options;
                    options.query_version = version;
                    options.merge_schema = true;
                    RETURN_IF_ERROR(std::dynamic_pointer_cast<CloudTablet>(_tablets[i].tablet)
                                            ->sync_rowsets(options, sync_stats));
                    // FIXME(plat1ko): Avoid pointer cast
                    ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(
                            *_tablets[i].tablet);
                    return Status::OK();
                });
            }
            RETURN_IF_ERROR(cloud::bthread_fork_join(std::move(tasks),
                                                     config::init_scanner_sync_rowsets_parallelism,
                                                     &_cloud_tablet_future));
        }
        _sync_tablet = true;
    }
    return Status::OK();
}

Status OlapScanLocalState::prepare(RuntimeState* state) {
    if (_prepared) {
        return Status::OK();
    }
    MonotonicStopWatch timer;
    timer.start();
    _read_sources.resize(_scan_ranges.size());

    if (config::is_cloud_mode()) {
        if (!_cloud_tablet_dependency ||
            _cloud_tablet_dependency->is_blocked_by(nullptr) != nullptr) {
            // Remote tablet still in-flight.
            return Status::OK();
        }
        DCHECK(_cloud_tablet_future.valid() && _cloud_tablet_future.get().ok());
        COUNTER_UPDATE(_sync_rowset_timer, _sync_cloud_tablets_watcher.elapsed_time());
        auto total_rowsets = std::accumulate(
                _tablets.cbegin(), _tablets.cend(), 0LL,
                [](long long acc, const auto& tabletWithVersion) {
                    return acc + tabletWithVersion.tablet->tablet_meta()->all_rs_metas().size();
                });
        COUNTER_UPDATE(_sync_rowset_tablets_rowsets_total_num, total_rowsets);
        for (const auto& sync_stats : _sync_statistics) {
            COUNTER_UPDATE(_sync_rowset_tablet_meta_cache_hit, sync_stats.tablet_meta_cache_hit);
            COUNTER_UPDATE(_sync_rowset_tablet_meta_cache_miss, sync_stats.tablet_meta_cache_miss);
            COUNTER_UPDATE(_sync_rowset_get_remote_tablet_meta_rpc_timer,
                           sync_stats.get_remote_tablet_meta_rpc_ns);
            COUNTER_UPDATE(_sync_rowset_get_remote_rowsets_num, sync_stats.get_remote_rowsets_num);
            COUNTER_UPDATE(_sync_rowset_get_remote_rowsets_rpc_timer,
                           sync_stats.get_remote_rowsets_rpc_ns);
            COUNTER_UPDATE(_sync_rowset_get_local_delete_bitmap_rowsets_num,
                           sync_stats.get_local_delete_bitmap_rowsets_num);
            COUNTER_UPDATE(_sync_rowset_get_remote_delete_bitmap_rowsets_num,
                           sync_stats.get_remote_delete_bitmap_rowsets_num);
            COUNTER_UPDATE(_sync_rowset_get_remote_delete_bitmap_key_count,
                           sync_stats.get_remote_delete_bitmap_key_count);
            COUNTER_UPDATE(_sync_rowset_get_remote_delete_bitmap_bytes,
                           sync_stats.get_remote_delete_bitmap_bytes);
            COUNTER_UPDATE(_sync_rowset_get_remote_delete_bitmap_rpc_timer,
                           sync_stats.get_remote_delete_bitmap_rpc_ns);
        }
        auto time_ms = _sync_cloud_tablets_watcher.elapsed_time_microseconds();
        if (time_ms >= config::sync_rowsets_slow_threshold_ms) {
            DorisMetrics::instance()->get_remote_tablet_slow_time_ms->increment(time_ms);
            DorisMetrics::instance()->get_remote_tablet_slow_cnt->increment(1);
            LOG_WARNING("get tablet takes too long")
                    .tag("query_id", print_id(PipelineXLocalState<>::_state->query_id()))
                    .tag("node_id", _parent->node_id())
                    .tag("total_time",
                         PrettyPrinter::print(_sync_cloud_tablets_watcher.elapsed_time(),
                                              TUnit::TIME_NS))
                    .tag("num_tablets", _tablets.size())
                    .tag("tablet_meta_cache_hit", _sync_rowset_tablet_meta_cache_hit->value())
                    .tag("tablet_meta_cache_miss", _sync_rowset_tablet_meta_cache_miss->value())
                    .tag("get_remote_tablet_meta_rpc_time",
                         PrettyPrinter::print(
                                 _sync_rowset_get_remote_tablet_meta_rpc_timer->value(),
                                 TUnit::TIME_NS))
                    .tag("remote_rowsets_num", _sync_rowset_get_remote_rowsets_num->value())
                    .tag("get_remote_rowsets_rpc_time",
                         PrettyPrinter::print(_sync_rowset_get_remote_rowsets_rpc_timer->value(),
                                              TUnit::TIME_NS))
                    .tag("local_delete_bitmap_rowsets_num",
                         _sync_rowset_get_local_delete_bitmap_rowsets_num->value())
                    .tag("remote_delete_bitmap_rowsets_num",
                         _sync_rowset_get_remote_delete_bitmap_rowsets_num->value())
                    .tag("remote_delete_bitmap_key_count",
                         _sync_rowset_get_remote_delete_bitmap_key_count->value())
                    .tag("remote_delete_bitmap_bytes",
                         PrettyPrinter::print(_sync_rowset_get_remote_delete_bitmap_bytes->value(),
                                              TUnit::BYTES))
                    .tag("get_remote_delete_bitmap_rpc_time",
                         PrettyPrinter::print(
                                 _sync_rowset_get_remote_delete_bitmap_rpc_timer->value(),
                                 TUnit::TIME_NS));
        }
    } else {
        _tablets.resize(_scan_ranges.size());
        for (size_t i = 0; i < _scan_ranges.size(); i++) {
            int64_t version = 0;
            std::from_chars(_scan_ranges[i]->version.data(),
                            _scan_ranges[i]->version.data() + _scan_ranges[i]->version.size(),
                            version);
            auto tablet = DORIS_TRY(ExecEnv::get_tablet(_scan_ranges[i]->tablet_id));
            _tablets[i] = {std::move(tablet), version};
        }
    }

    for (size_t i = 0; i < _scan_ranges.size(); i++) {
        RETURN_IF_ERROR(_tablets[i].tablet->capture_rs_readers({0, _tablets[i].version},
                                                               &_read_sources[i].rs_splits,
                                                               _state->skip_missing_version()));
        if (!PipelineXLocalState<>::_state->skip_delete_predicate()) {
            _read_sources[i].fill_delete_predicates();
        }
        if (config::enable_mow_verbose_log &&
            _tablets[i].tablet->enable_unique_key_merge_on_write()) {
            LOG_INFO("finish capture_rs_readers for tablet={}, query_id={}",
                     _tablets[i].tablet->tablet_id(),
                     print_id(PipelineXLocalState<>::_state->query_id()));
        }
    }
    timer.stop();
    double cost_secs = static_cast<double>(timer.elapsed_time()) / NANOS_PER_SEC;
    if (cost_secs > 1) {
        LOG_WARNING(
                "Try to hold tablets costs {} seconds, it costs too much. (Query-ID={}, NodeId={}, "
                "ScanRangeNum={})",
                cost_secs, print_id(PipelineXLocalState<>::_state->query_id()), _parent->node_id(),
                _scan_ranges.size());
    }
    _prepared = true;
    return Status::OK();
}

Status OlapScanLocalState::open(RuntimeState* state) {
    auto& p = _parent->cast<OlapScanOperatorX>();
    for (const auto& pair : p._slot_id_to_slot_desc) {
        const SlotDescriptor* slot_desc = pair.second;
        std::shared_ptr<doris::TExpr> virtual_col_expr = slot_desc->get_virtual_column_expr();
        if (virtual_col_expr) {
            std::shared_ptr<doris::vectorized::VExprContext> virtual_column_expr_ctx;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(*virtual_col_expr,
                                                                virtual_column_expr_ctx));
            RETURN_IF_ERROR(virtual_column_expr_ctx->prepare(state, p.intermediate_row_desc()));
            RETURN_IF_ERROR(virtual_column_expr_ctx->open(state));

            _slot_id_to_virtual_column_expr[slot_desc->id()] = virtual_column_expr_ctx;
            _slot_id_to_col_type[slot_desc->id()] = slot_desc->get_data_type_ptr();
            int col_pos = p.intermediate_row_desc().get_column_id(slot_desc->id());
            if (col_pos < 0) {
                return Status::InternalError(
                        "Invalid virtual slot, can not find its information. Slot desc:\n{}\nRow "
                        "desc:\n{}",
                        slot_desc->debug_string(), p.row_desc().debug_string());
            } else {
                _slot_id_to_index_in_block[slot_desc->id()] = col_pos;
            }
        }
    }

    if (_score_runtime) {
        RETURN_IF_ERROR(_score_runtime->prepare(state, p.intermediate_row_desc()));
    }

    RETURN_IF_ERROR(ScanLocalState<OlapScanLocalState>::open(state));

    return Status::OK();
}

TOlapScanNode& OlapScanLocalState::olap_scan_node() const {
    return _parent->cast<OlapScanOperatorX>()._olap_scan_node;
}

void OlapScanLocalState::set_scan_ranges(RuntimeState* state,
                                         const std::vector<TScanRangeParams>& scan_ranges) {
    const auto& cache_param = _parent->cast<OlapScanOperatorX>()._cache_param;
    bool hit_cache = false;
    if (!cache_param.digest.empty() && !cache_param.force_refresh_query_cache) {
        std::string cache_key;
        int64_t version = 0;
        auto status = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);
        if (!status.ok()) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, status.msg());
        }
        doris::QueryCacheHandle handle;
        hit_cache = QueryCache::instance()->lookup(cache_key, version, &handle);
    }

    if (!hit_cache) {
        for (auto& scan_range : scan_ranges) {
            DCHECK(scan_range.scan_range.__isset.palo_scan_range);
            _scan_ranges.emplace_back(new TPaloScanRange(scan_range.scan_range.palo_scan_range));
            COUNTER_UPDATE(_tablet_counter, 1);
        }
    }
}

static std::string olap_filter_to_string(const doris::TCondition& condition) {
    auto op_name = condition.condition_op;
    if (condition.condition_op == "*=") {
        op_name = "IN";
    } else if (condition.condition_op == "!*=") {
        op_name = "NOT IN";
    }
    return fmt::format("{{{} {} {}}}", condition.column_name, op_name,
                       condition.condition_values.size() > 128
                               ? "[more than 128 elements]"
                               : to_string(condition.condition_values));
}

static std::string olap_filters_to_string(const std::vector<FilterOlapParam<TCondition>>& filters) {
    std::string filters_string;
    filters_string += "[";
    for (auto it = filters.cbegin(); it != filters.cend(); it++) {
        if (it != filters.cbegin()) {
            filters_string += ", ";
        }
        filters_string += olap_filter_to_string(it->filter);
    }
    filters_string += "]";
    return filters_string;
}

static std::string tablets_id_to_string(
        const std::vector<std::unique_ptr<TPaloScanRange>>& scan_ranges) {
    if (scan_ranges.empty()) {
        return "[empty]";
    }
    std::stringstream ss;
    ss << "[" << scan_ranges[0]->tablet_id;
    for (int i = 1; i < scan_ranges.size(); ++i) {
        ss << ", " << scan_ranges[i]->tablet_id;
    }
    ss << "]";
    return ss.str();
}

inline std::string push_down_agg_to_string(const TPushAggOp::type& op) {
    if (op == TPushAggOp::MINMAX) {
        return "MINMAX";
    } else if (op == TPushAggOp::COUNT) {
        return "COUNT";
    } else if (op == TPushAggOp::MIX) {
        return "MIX";
    } else {
        return "NONE";
    }
}

Status OlapScanLocalState::_build_key_ranges_and_filters() {
    auto& p = _parent->cast<OlapScanOperatorX>();
    if (p._push_down_agg_type == TPushAggOp::NONE ||
        p._push_down_agg_type == TPushAggOp::COUNT_ON_INDEX) {
        const std::vector<std::string>& column_names = p._olap_scan_node.key_column_name;
        const std::vector<TPrimitiveType::type>& column_types = p._olap_scan_node.key_column_type;
        DCHECK(column_types.size() == column_names.size());

        // 1. construct scan key except last olap engine short key
        _scan_keys.set_is_convertible(p.limit() == -1);

        // we use `exact_range` to identify a key range is an exact range or not when we convert
        // it to `_scan_keys`. If `exact_range` is true, we can just discard it from `_olap_filters`.
        bool exact_range = true;

        // If the `_scan_keys` cannot extend by the range of column, should stop.
        bool should_break = false;

        bool eos = false;
        for (int column_index = 0; column_index < column_names.size() &&
                                   !_scan_keys.has_range_value() && !eos && !should_break;
             ++column_index) {
            auto iter = _colname_to_value_range.find(column_names[column_index]);
            if (_colname_to_value_range.end() == iter) {
                break;
            }

            RETURN_IF_ERROR(std::visit(
                    [&](auto&& range) {
                        // make a copy or range and pass to extend_scan_key, keep the range unchanged
                        // because extend_scan_key method may change the first parameter.
                        // but the original range may be converted to olap filters, if it's not a exact_range.
                        auto temp_range = range;
                        if (range.get_fixed_value_size() <= p._max_pushdown_conditions_per_column) {
                            RETURN_IF_ERROR(
                                    _scan_keys.extend_scan_key(temp_range, p._max_scan_key_num,
                                                               &exact_range, &eos, &should_break));
                            if (exact_range) {
                                _colname_to_value_range.erase(iter->first);
                            }
                        } else {
                            // if exceed max_pushdown_conditions_per_column, use whole_value_rang instead
                            // and will not erase from _colname_to_value_range, it must be not exact_range
                            temp_range.set_whole_value_range();
                            RETURN_IF_ERROR(
                                    _scan_keys.extend_scan_key(temp_range, p._max_scan_key_num,
                                                               &exact_range, &eos, &should_break));
                        }
                        return Status::OK();
                    },
                    iter->second));
        }
        if (eos) {
            _eos = true;
            _scan_dependency->set_ready();
        }

        for (auto& iter : _colname_to_value_range) {
            std::vector<FilterOlapParam<TCondition>> filters;
            std::visit([&](auto&& range) { range.to_olap_filter(filters); }, iter.second);

            for (const auto& filter : filters) {
                _olap_filters.emplace_back(filter);
            }
        }

        // Append value ranges in "_not_in_value_ranges"
        for (auto& range : _not_in_value_ranges) {
            std::visit([&](auto&& the_range) { the_range.to_in_condition(_olap_filters, false); },
                       range);
        }
    } else {
        custom_profile()->add_info_string("PushDownAggregate",
                                          push_down_agg_to_string(p._push_down_agg_type));
    }

    if (state()->enable_profile()) {
        custom_profile()->add_info_string("PushDownPredicates",
                                          olap_filters_to_string(_olap_filters));
        custom_profile()->add_info_string("KeyRanges", _scan_keys.debug_string());
        custom_profile()->add_info_string("TabletIds", tablets_id_to_string(_scan_ranges));
    }
    VLOG_CRITICAL << _scan_keys.debug_string();

    return Status::OK();
}

OlapScanOperatorX::OlapScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                     const DescriptorTbl& descs, int parallel_tasks,
                                     const TQueryCacheParam& param)
        : ScanOperatorX<OlapScanLocalState>(pool, tnode, operator_id, descs, parallel_tasks),
          _olap_scan_node(tnode.olap_scan_node),
          _cache_param(param) {
    _output_tuple_id = tnode.olap_scan_node.tuple_id;
    if (_olap_scan_node.__isset.sort_info && _olap_scan_node.__isset.sort_limit) {
        _limit_per_scanner = _olap_scan_node.sort_limit;
    }
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
