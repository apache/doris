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

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/scan_operator.h"

namespace doris {

namespace vectorized {
class NewOlapScanner;
}
} // namespace doris

namespace doris::pipeline {

class OlapScanOperatorX;
class OlapScanLocalState final : public ScanLocalState<OlapScanLocalState> {
public:
    using Parent = OlapScanOperatorX;
    ENABLE_FACTORY_CREATOR(OlapScanLocalState);
    OlapScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalState(state, parent) {}

    TOlapScanNode& olap_scan_node() const;

    std::string name_suffix() const override {
        return fmt::format(" (id={}. nereids_id={}. table name = {})",
                           std::to_string(_parent->node_id()),
                           std::to_string(_parent->nereids_id()), olap_scan_node().table_name);
    }

private:
    friend class vectorized::NewOlapScanner;

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

    PushDownType _should_push_down_bloom_filter() override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_bitmap_filter() override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_is_null_predicate() override { return PushDownType::ACCEPTABLE; }

    bool _should_push_down_common_expr() override;

    bool _storage_no_merge() override;

    bool _push_down_topn(const vectorized::RuntimePredicate& predicate) override {
        if (!predicate.target_is_slot(_parent->node_id())) {
            return false;
        }
        return _is_key_column(predicate.get_col_name(_parent->node_id())) || _storage_no_merge();
    }

    Status _init_scanners(std::list<vectorized::VScannerSPtr>* scanners) override;

    void add_filter_info(int id, const PredicateFilterInfo& info);

    Status _build_key_ranges_and_filters();

    std::vector<std::unique_ptr<TPaloScanRange>> _scan_ranges;
    std::vector<std::unique_ptr<doris::OlapScanRange>> _cond_ranges;
    OlapScanKeys _scan_keys;
    std::vector<TCondition> _olap_filters;
    // If column id in this set, indicate that we need to read data after index filtering
    std::set<int32_t> _maybe_read_column_ids;

    std::unique_ptr<RuntimeProfile> _segment_profile;

    RuntimeProfile::Counter* _num_disks_accessed_counter = nullptr;

    RuntimeProfile::Counter* _tablet_counter = nullptr;
    RuntimeProfile::Counter* _key_range_counter = nullptr;
    RuntimeProfile::Counter* _rows_pushed_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;
    RuntimeProfile::Counter* _scanner_init_timer = nullptr;
    RuntimeProfile::Counter* _process_conjunct_timer = nullptr;

    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompressor_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;

    RuntimeProfile::Counter* _rows_vec_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_short_circuit_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_vec_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _rows_short_circuit_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _vec_cond_timer = nullptr;
    RuntimeProfile::Counter* _short_cond_timer = nullptr;
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _output_col_timer = nullptr;
    std::map<int, PredicateFilterInfo> _filter_info;

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
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    // Add more detail seek timer and counter profile
    // Read process is split into 3 stages: init, first read, lazy read
    RuntimeProfile::Counter* _block_init_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_counter = nullptr;
    RuntimeProfile::Counter* _block_conditions_filtered_timer = nullptr;
    RuntimeProfile::Counter* _block_conditions_filtered_bf_timer = nullptr;
    RuntimeProfile::Counter* _collect_iterator_merge_next_timer = nullptr;
    RuntimeProfile::Counter* _collect_iterator_normal_next_timer = nullptr;
    RuntimeProfile::Counter* _block_conditions_filtered_zonemap_timer = nullptr;
    RuntimeProfile::Counter* _block_conditions_filtered_zonemap_rp_timer = nullptr;
    RuntimeProfile::Counter* _block_conditions_filtered_dict_timer = nullptr;
    RuntimeProfile::Counter* _first_read_timer = nullptr;
    RuntimeProfile::Counter* _second_read_timer = nullptr;
    RuntimeProfile::Counter* _first_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _first_read_seek_counter = nullptr;
    RuntimeProfile::Counter* _lazy_read_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_counter = nullptr;

    RuntimeProfile::Counter* _block_convert_timer = nullptr;

    // total pages read
    // used by segment v2
    RuntimeProfile::Counter* _total_pages_num_counter = nullptr;
    // page read from cache
    // used by segment v2
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;

    // row count filtered by bitmap inverted index
    RuntimeProfile::Counter* _bitmap_index_filter_counter = nullptr;
    // time fro bitmap inverted index read and filter
    RuntimeProfile::Counter* _bitmap_index_filter_timer = nullptr;

    RuntimeProfile::Counter* _inverted_index_filter_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_filter_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_null_bitmap_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_cache_hit_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_cache_miss_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_bitmap_copy_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_bitmap_op_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_open_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_search_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_cache_hit_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_cache_miss_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_downgrade_count_counter = nullptr;

    RuntimeProfile::Counter* _output_index_result_column_timer = nullptr;

    // number of segment filtered by column stat when creating seg iterator
    RuntimeProfile::Counter* _filtered_segment_counter = nullptr;
    // total number of segment related to this scan node
    RuntimeProfile::Counter* _total_segment_counter = nullptr;

    RuntimeProfile::Counter* _runtime_filter_info = nullptr;

    std::mutex _profile_mtx;
};

class OlapScanOperatorX final : public ScanOperatorX<OlapScanLocalState> {
public:
    OlapScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                      const DescriptorTbl& descs, int parallel_tasks,
                      const TQueryCacheParam& cache_param);

private:
    friend class OlapScanLocalState;
    TOlapScanNode _olap_scan_node;
    TQueryCacheParam _cache_param;
};

} // namespace doris::pipeline
