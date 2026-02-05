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

#include <stdint.h>

#include <string>

#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "olap/tablet_reader.h"
#include "operator.h"
#include "pipeline/exec/scan_operator.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {
class OlapScanner;
} // namespace doris::vectorized

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class OlapScanOperatorX;
class OlapScanLocalState final : public ScanLocalState<OlapScanLocalState> {
public:
    using Parent = OlapScanOperatorX;
    using Base = ScanLocalState<OlapScanLocalState>;
    ENABLE_FACTORY_CREATOR(OlapScanLocalState);
    OlapScanLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {}
    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status prepare(RuntimeState* state) override;
    TOlapScanNode& olap_scan_node() const;

    std::string name_suffix() const override {
        return fmt::format("(nereids_id={}. table_name={})" + operator_name_suffix,
                           std::to_string(_parent->nereids_id()), olap_scan_node().table_name,
                           std::to_string(_parent->node_id()));
    }
    std::vector<Dependency*> execution_dependencies() override {
        if (!_cloud_tablet_dependency) {
            return Base::execution_dependencies();
        }
        std::vector<Dependency*> res = Base::execution_dependencies();
        res.push_back(_cloud_tablet_dependency.get());
        return res;
    }

    Status open(RuntimeState* state) override;

private:
    friend class vectorized::OlapScanner;

    Status _sync_cloud_tablets(RuntimeState* state);
    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;
    Status _init_profile() override;
    Status _process_conjuncts(RuntimeState* state) override;
    bool _is_key_column(const std::string& col_name) override;

    Status _should_push_down_function_filter(vectorized::VectorizedFnCall* fn_call,
                                             vectorized::VExprContext* expr_ctx,
                                             StringRef* constant_str,
                                             doris::FunctionContext** fn_ctx,
                                             PushDownType& pdt) override;

    PushDownType _should_push_down_bloom_filter() const override {
        return PushDownType::ACCEPTABLE;
    }
    PushDownType _should_push_down_topn_filter() const override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_bitmap_filter() const override {
        return PushDownType::ACCEPTABLE;
    }

    PushDownType _should_push_down_is_null_predicate(
            vectorized::VectorizedFnCall* fn_call) const override {
        return fn_call->fn().name.function_name == "is_null_pred" ||
                               fn_call->fn().name.function_name == "is_not_null_pred"
                       ? PushDownType::ACCEPTABLE
                       : PushDownType::UNACCEPTABLE;
    }
    PushDownType _should_push_down_in_predicate() const override {
        return PushDownType::ACCEPTABLE;
    }
    PushDownType _should_push_down_binary_predicate(
            vectorized::VectorizedFnCall* fn_call, vectorized::VExprContext* expr_ctx,
            vectorized::Field& constant_val, const std::set<std::string> fn_name) const override;

    bool _should_push_down_common_expr() override;

    bool _storage_no_merge() override;

    bool _push_down_topn(const vectorized::RuntimePredicate& predicate) override {
        if (!predicate.target_is_slot(_parent->node_id())) {
            return false;
        }
        if (!olap_scan_node().__isset.columns_desc || olap_scan_node().columns_desc.empty() ||
            olap_scan_node().columns_desc[0].col_unique_id < 0) {
            // Disable topN filter if there is no schema info
            return false;
        }
        return _is_key_column(predicate.get_col_name(_parent->node_id()));
    }

    Status _init_scanners(std::list<vectorized::ScannerSPtr>* scanners) override;

    Status _build_key_ranges_and_filters();

    std::vector<std::unique_ptr<TPaloScanRange>> _scan_ranges;
    std::vector<SyncRowsetStats> _sync_statistics;
    MonotonicStopWatch _sync_cloud_tablets_watcher;
    std::shared_ptr<Dependency> _cloud_tablet_dependency;
    std::atomic<size_t> _pending_tablets_num = 0;
    bool _prepared = false;
    std::future<Status> _cloud_tablet_future;
    std::atomic_bool _sync_tablet = false;
    std::vector<std::unique_ptr<doris::OlapScanRange>> _cond_ranges;
    OlapScanKeys _scan_keys;
    // If column id in this set, indicate that we need to read data after index filtering
    std::set<int32_t> _output_column_ids;

    std::unique_ptr<RuntimeProfile> _segment_profile;
    std::unique_ptr<RuntimeProfile> _index_filter_profile;

    RuntimeProfile::Counter* _tablet_counter = nullptr;
    RuntimeProfile::Counter* _key_range_counter = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;
    RuntimeProfile::Counter* _scanner_init_timer = nullptr;
    RuntimeProfile::Counter* _process_conjunct_timer = nullptr;

    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompressor_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;

    RuntimeProfile::Counter* _rows_vec_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_short_circuit_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_expr_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_vec_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _rows_short_circuit_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _rows_expr_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _vec_cond_timer = nullptr;
    RuntimeProfile::Counter* _short_cond_timer = nullptr;
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _output_col_timer = nullptr;

    RuntimeProfile::Counter* _stats_filtered_counter = nullptr;
    RuntimeProfile::Counter* _stats_rp_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _dict_filtered_counter = nullptr;
    RuntimeProfile::Counter* _del_filtered_counter = nullptr;
    RuntimeProfile::Counter* _conditions_filtered_counter = nullptr;
    RuntimeProfile::Counter* _key_range_filtered_counter = nullptr;

    RuntimeProfile::Counter* _block_fetch_timer = nullptr;
    RuntimeProfile::Counter* _delete_bitmap_get_agg_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_tablet_meta_rpc_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_tablet_meta_cache_hit = nullptr;
    RuntimeProfile::Counter* _sync_rowset_tablet_meta_cache_miss = nullptr;
    RuntimeProfile::Counter* _sync_rowset_tablets_rowsets_total_num = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_rowsets_num = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_rowsets_rpc_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_local_delete_bitmap_rowsets_num = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_delete_bitmap_rowsets_num = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_delete_bitmap_key_count = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_delete_bitmap_bytes = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_delete_bitmap_rpc_timer = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    // Add more detail seek timer and counter profile
    // Read process is split into 3 stages: init, first read, lazy read
    RuntimeProfile::Counter* _block_init_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_counter = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_keys_timer = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_column_conditions_timer = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_bf_timer = nullptr;
    RuntimeProfile::Counter* _collect_iterator_merge_next_timer = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_zonemap_timer = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_dict_timer = nullptr;
    RuntimeProfile::Counter* _predicate_column_read_timer = nullptr;
    RuntimeProfile::Counter* _non_predicate_column_read_timer = nullptr;
    RuntimeProfile::Counter* _predicate_column_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _predicate_column_read_seek_counter = nullptr;
    RuntimeProfile::Counter* _lazy_read_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_counter = nullptr;

    // total pages read
    // used by segment v2
    RuntimeProfile::Counter* _total_pages_num_counter = nullptr;
    // page read from cache
    // used by segment v2
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;

    RuntimeProfile::Counter* _statistics_collect_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_filter_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_filter_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_null_bitmap_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_cache_hit_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_cache_miss_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_bitmap_copy_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_open_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_search_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_search_init_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_search_exec_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_cache_hit_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_cache_miss_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_downgrade_count_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_analyzer_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_lookup_timer = nullptr;

    RuntimeProfile::Counter* _ann_topn_filter_counter = nullptr;
    // topn_search_costs = index_load_costs + engine_search_costs + pre_process_costs + post_process_costs
    RuntimeProfile::Counter* _ann_topn_search_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_search_cnt = nullptr;

    RuntimeProfile::Counter* _ann_index_load_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_pre_process_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_engine_search_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_post_process_costs = nullptr;
    // post_process_costs = engine_convert_costs + result_convert_costs
    RuntimeProfile::Counter* _ann_topn_engine_convert_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_result_convert_costs = nullptr;

    RuntimeProfile::Counter* _ann_range_search_filter_counter = nullptr;
    // range_Search_costs = index_load_costs + engine_search_costs + pre_process_costs + post_process_costs
    RuntimeProfile::Counter* _ann_range_search_costs = nullptr;
    RuntimeProfile::Counter* _ann_range_search_cnt = nullptr;

    RuntimeProfile::Counter* _ann_range_pre_process_costs = nullptr;
    RuntimeProfile::Counter* _ann_range_engine_search_costs = nullptr;
    RuntimeProfile::Counter* _ann_range_post_process_costs = nullptr;

    RuntimeProfile::Counter* _ann_range_engine_convert_costs = nullptr;
    RuntimeProfile::Counter* _ann_range_result_convert_costs = nullptr;

    RuntimeProfile::Counter* _output_index_result_column_timer = nullptr;

    // number of segment filtered by column stat when creating seg iterator
    RuntimeProfile::Counter* _filtered_segment_counter = nullptr;
    // total number of segment related to this scan node
    RuntimeProfile::Counter* _total_segment_counter = nullptr;

    // condition cache filter stats
    RuntimeProfile::Counter* _condition_cache_hit_segment_counter = nullptr;
    RuntimeProfile::Counter* _condition_cache_filtered_rows_counter = nullptr;

    // timer about tablet reader
    RuntimeProfile::Counter* _tablet_reader_init_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_capture_rs_readers_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_return_columns_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_keys_param_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_orderby_keys_param_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_conditions_param_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_delete_condition_param_timer = nullptr;

    // timer about block reader
    RuntimeProfile::Counter* _block_reader_vcollect_iter_init_timer = nullptr;
    RuntimeProfile::Counter* _block_reader_rs_readers_init_timer = nullptr;
    RuntimeProfile::Counter* _block_reader_build_heap_init_timer = nullptr;

    RuntimeProfile::Counter* _rowset_reader_get_segment_iterators_timer = nullptr;
    RuntimeProfile::Counter* _rowset_reader_create_iterators_timer = nullptr;
    RuntimeProfile::Counter* _rowset_reader_init_iterators_timer = nullptr;
    RuntimeProfile::Counter* _rowset_reader_load_segments_timer = nullptr;

    RuntimeProfile::Counter* _segment_iterator_init_timer = nullptr;
    RuntimeProfile::Counter* _segment_iterator_init_return_column_iterators_timer = nullptr;
    RuntimeProfile::Counter* _segment_iterator_init_index_iterators_timer = nullptr;
    RuntimeProfile::Counter* _segment_iterator_init_segment_prefetchers_timer = nullptr;

    RuntimeProfile::Counter* _segment_create_column_readers_timer = nullptr;
    RuntimeProfile::Counter* _segment_load_index_timer = nullptr;

    // total uncompressed bytes read when scanning sparse columns in variant
    RuntimeProfile::Counter* _variant_scan_sparse_column_bytes = nullptr;

    // total time spent scanning sparse subcolumns
    RuntimeProfile::Counter* _variant_scan_sparse_column_timer = nullptr;
    // time to build/resolve subcolumn paths from the sparse column
    RuntimeProfile::Counter* _variant_fill_path_from_sparse_column_timer = nullptr;
    // Variant subtree: times falling back to default iterator due to missing path
    RuntimeProfile::Counter* _variant_subtree_default_iter_count = nullptr;
    // Variant subtree: times selecting leaf iterator (target subcolumn is a leaf)
    RuntimeProfile::Counter* _variant_subtree_leaf_iter_count = nullptr;
    // Variant subtree: times selecting hierarchical iterator (node has children and sparse columns)
    RuntimeProfile::Counter* _variant_subtree_hierarchical_iter_count = nullptr;
    // Variant subtree: times selecting sparse iterator (iterate over sparse subcolumn)
    RuntimeProfile::Counter* _variant_subtree_sparse_iter_count = nullptr;
    // Variant subtree: times selecting doc snapshot all iterator (merge doc snapshot into root)
    RuntimeProfile::Counter* _variant_doc_value_column_iter_count = nullptr;

    std::vector<TabletWithVersion> _tablets;
    std::vector<TabletReadSource> _read_sources;

    std::map<SlotId, vectorized::VExprContextSPtr> _slot_id_to_virtual_column_expr;
    std::map<SlotId, size_t> _slot_id_to_index_in_block;
    // this map is needed for scanner opening.
    std::map<SlotId, vectorized::DataTypePtr> _slot_id_to_col_type;
};

class OlapScanOperatorX final : public ScanOperatorX<OlapScanLocalState> {
public:
    OlapScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                      const DescriptorTbl& descs, int parallel_tasks,
                      const TQueryCacheParam& cache_param);

    int get_column_id(const std::string& col_name) const override {
        if (!_tablet_schema) {
            return -1;
        }
        const auto& column = *DORIS_TRY(_tablet_schema->column(col_name));
        return _tablet_schema->field_index(column.unique_id());
    }

private:
    friend class OlapScanLocalState;
    TOlapScanNode _olap_scan_node;
    TQueryCacheParam _cache_param;
    TabletSchemaSPtr _tablet_schema;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
