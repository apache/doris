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

#include "exec/operator/file_scan_operator.h"

#include <fmt/format.h>

#include <algorithm>
#include <memory>

#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/operator/scan_operator.h"
#include "exec/scan/file_scanner.h"
#include "exec/scan/file_scanner_v2.h"
#include "exec/scan/scanner_context.h"
#include "format/format_common.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_manager.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace {

bool contains_variant_type(const DataTypePtr& input) {
    const auto type = remove_nullable(input);
    switch (type->get_primitive_type()) {
    case TYPE_VARIANT:
        return true;
    case TYPE_ARRAY:
        return contains_variant_type(assert_cast<const DataTypeArray&>(*type).get_nested_type());
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*type);
        return contains_variant_type(map.get_key_type()) ||
               contains_variant_type(map.get_value_type());
    }
    case TYPE_STRUCT:
        return std::ranges::any_of(assert_cast<const DataTypeStruct&>(*type).get_elements(),
                                   contains_variant_type);
    default:
        return false;
    }
}

} // namespace

PushDownType FileScanLocalState::_should_push_down_binary_predicate(
        VectorizedFnCall* fn_call, VExprContext* expr_ctx, Field& constant_val,
        const std::set<std::string> fn_name) const {
    if (!fn_name.contains(fn_call->fn().name.function_name)) {
        return PushDownType::UNACCEPTABLE;
    }
    const auto& children = fn_call->children();
    DCHECK(children.size() == 2);
    DCHECK_EQ(children[0]->node_type(), TExprNodeType::SLOT_REF);
    if (children[1]->is_constant()) {
        std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
        THROW_IF_ERROR(children[1]->get_const_col(expr_ctx, &const_col_wrapper));
        const auto* const_column =
                assert_cast<const ColumnConst*>(const_col_wrapper->column_ptr.get());
        constant_val = const_column->operator[](0);
        return PushDownType::PARTIAL_ACCEPTABLE;
    } else {
        // only handle constant value
        return PushDownType::UNACCEPTABLE;
    }
}

bool FileScanLocalState::_push_down_topn(const RuntimePredicate& predicate) {
    if (!predicate.target_is_slot(_parent->node_id())) {
        return false;
    }
    auto& p = _parent->cast<FileScanOperatorX>();
    const auto slot_id = predicate.get_texpr(_parent->node_id()).nodes[0].slot_ref.slot_id;
    auto* slot = p._slot_id_to_slot_desc[slot_id];
    DCHECK(slot != nullptr);
    // External readers do not fully support VARBINARY column predicates yet.
    return slot->type()->get_primitive_type() != TYPE_VARBINARY;
}

int FileScanLocalState::max_scanners_concurrency(RuntimeState* state) const {
    // For select * from table limit 10; should just use one thread.
    if (should_run_serial()) {
        return 1;
    }
    /*
     * The max concurrency of file scanners for each FileScanLocalState is determined by:
     * 1. User specified max_file_scanners_concurrency which is set through session variable.
     * 2. Default: 16
     *
     * If this is a serial operator, the max concurrency should multiply by the number of parallel instances of the operator.
     */
    return (state->max_file_scanners_concurrency() > 0 ? state->max_file_scanners_concurrency()
                                                       : 16) *
           (state->query_parallel_instance_num() / _parent->parallelism(state));
}

int FileScanLocalState::min_scanners_concurrency(RuntimeState* state) const {
    if (should_run_serial()) {
        return 1;
    }
    /*
     * The min concurrency of scanners for each FileScanLocalState is determined by:
     * 1. User specified min_file_scanners_concurrency which is set through session variable.
     * 2. Default: 1
     *
     * If this is a serial operator, the max concurrency should multiply by the number of parallel instances of the operator.
     */
    return (state->min_file_scanners_concurrency() > 0 ? state->min_file_scanners_concurrency()
                                                       : 1) *
           (state->query_parallel_instance_num() / _parent->parallelism(state));
}

ScannerScheduler* FileScanLocalState::scan_scheduler(RuntimeState* state) const {
    return state->get_query_ctx()->get_remote_scan_scheduler();
}

#ifdef BE_TEST
bool FileScanLocalState::TEST_should_use_file_scanner_v2(const TQueryOptions& query_options,
                                                         bool is_load,
                                                         const TFileScanRangeParams& scan_params) {
    return _should_use_file_scanner_v2(query_options, is_load, scan_params);
}
#endif

bool FileScanLocalState::_should_use_file_scanner_v2(const TQueryOptions& query_options,
                                                     bool is_load,
                                                     const TFileScanRangeParams& scan_params) {
    const bool is_transactional_hive =
            scan_params.__isset.table_format_params &&
            scan_params.table_format_params.table_format_type == "transactional_hive";
    return query_options.__isset.enable_file_scanner_v2 && query_options.enable_file_scanner_v2 &&
           !is_load && scan_params.format_type != TFileFormatType::FORMAT_ES_HTTP &&
           !is_transactional_hive;
}

bool FileScanLocalState::_can_generate_physical_splits(const TQueryOptions& query_options,
                                                       bool is_load,
                                                       const TFileScanRangeParams& scan_params,
                                                       const TFileRangeDesc& range) {
    if (!_should_use_file_scanner_v2(query_options, is_load, scan_params)) {
        return false;
    }
    const auto format = range.__isset.format_type ? range.format_type : scan_params.format_type;
    if (format == TFileFormatType::FORMAT_PARQUET || format == TFileFormatType::FORMAT_ORC) {
        // Keep scanner creation aligned with the downstream refinement guard. Otherwise an
        // Iceberg delete split creates idle scanners even though it can never publish children.
        return FileScannerV2::can_refine_source_split(range);
    }
    if (format != TFileFormatType::FORMAT_JNI || !range.__isset.table_format_params ||
        range.table_format_params.table_format_type != "paimon" ||
        !range.table_format_params.__isset.paimon_params) {
        return false;
    }
    const auto& paimon = range.table_format_params.paimon_params;
    return paimon.__isset.file_format &&
           (paimon.file_format == "parquet" || paimon.file_format == "orc") &&
           !paimon.__isset.paimon_split;
}

int FileScanLocalState::_adjust_scanner_count(int requested, int initial_ranges,
                                              bool can_generate_physical_splits) {
    return can_generate_physical_splits ? requested : std::min(requested, initial_ranges);
}

Status FileScanLocalState::_init_scanners(std::list<ScannerSPtr>* scanners) {
    if (_split_source->num_scan_ranges() == 0) {
        _eos = true;
        return Status::OK();
    }

    auto& id_file_map = state()->get_id_file_map();
    if (id_file_map != nullptr) {
        id_file_map->set_external_scan_params(state()->get_query_ctx(), _max_scanners);
    }

    auto& p = _parent->cast<FileScanOperatorX>();
    // There's only one scan range for each backend in batch split mode. Each backend only starts up one ScanNode instance.
    uint32_t shard_num =
            std::min(ScannerScheduler::default_remote_scan_thread_num() / p.parallelism(state()),
                     _max_scanners);
    shard_num = std::max(shard_num, 1U);
    _kv_cache = std::make_unique<ShardedKVCache>(shard_num);
    const TFileScanRangeParams* scan_params = nullptr;
    if (state()->get_query_ctx() != nullptr &&
        state()->get_query_ctx()->file_scan_range_params_map.count(parent_id()) > 0) {
        scan_params = &state()->get_query_ctx()->file_scan_range_params_map[parent_id()];
    } else {
        scan_params = _split_source->get_params();
    }
    const bool is_load =
            state()->desc_tbl().get_tuple_descriptor(scan_params->src_tuple_id) != nullptr;
    // TODO: Use scanner v2 for all queries.
    const bool use_file_scanner_v2 =
            _should_use_file_scanner_v2(state()->query_options(), is_load, *scan_params);
    _operator_profile->add_info_string("UseScannerV2", use_file_scanner_v2 ? "true" : "false");
    const auto* output_tuple_desc = state()->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DORIS_CHECK(output_tuple_desc != nullptr);
    const bool metadata_only_count =
            is_count_star_pushdown() && _split_source->all_ranges_have_table_level_row_count();
    if (!is_load && !use_file_scanner_v2 && !metadata_only_count &&
        std::ranges::any_of(output_tuple_desc->slots(), [](const SlotDescriptor* slot) {
            return contains_variant_type(slot->get_data_type_ptr());
        })) {
        // A syntactic COUNT(*) alone is insufficient: every assigned range must prove that the
        // legacy scanner will emit metadata counts without decoding a Variant carrier.
        return Status::NotSupported(
                "External VARIANT columns require FileScannerV2; the legacy file scanner does "
                "not support VARIANT");
    }
    for (int i = 0; i < _max_scanners; ++i) {
        ScannerSPtr scanner;
        if (use_file_scanner_v2) {
            scanner = FileScannerV2::create_shared(state(), this, p._limit, _split_source,
                                                   _scanner_profile.get(), _kv_cache.get(),
                                                   &p._colname_to_slot_id);
        } else {
            scanner = FileScanner::create_shared(state(), this, p._limit, _split_source,
                                                 _scanner_profile.get(), _kv_cache.get(),
                                                 &p._colname_to_slot_id);
        }
        RETURN_IF_ERROR(scanner->init(state(), _conjuncts));
        scanners->push_back(std::move(scanner));
    }
    return Status::OK();
}

std::string FileScanLocalState::name_suffix() const {
    return fmt::format("(nereids_id={}. table_name={})" + operator_name_suffix,
                       std::to_string(_parent->nereids_id()),
                       _parent->cast<FileScanOperatorX>()._table_name,
                       std::to_string(_parent->node_id()));
}

void FileScanLocalState::set_scan_ranges(RuntimeState* state,
                                         const std::vector<TScanRangeParams>& scan_ranges) {
    auto& p = _parent->cast<FileScanOperatorX>();

    auto calc_max_scanners = [&](int parallel_instance_num) -> int {
        int max_scanners =
                ScannerScheduler::default_remote_scan_thread_num() / parallel_instance_num;
        // For external tables, each scanner is not bound to specific splits.
        // Instead, when a scanner is scheduled, it dynamically fetches the next scan range
        // from a unified split source for scanning.
        // Therefore, the number of scanners only needs to match "max_scanners_concurrency"
        // to ensure full-speed execution.
        // For 32 core node, the default "max_scanners_concurrency" should be 16
        max_scanners = std::min(max_scanners, max_scanners_concurrency(state));
        return max_scanners;
    };

    if (scan_ranges.size() == 1) {
        auto scan_range = scan_ranges[0].scan_range.ext_scan_range.file_scan_range;
        if (scan_range.__isset.split_source) {
            p._batch_split_mode = true;
            custom_profile()->add_info_string("BatchSplitMode", "true");
            auto split_source = scan_range.split_source;
            RuntimeProfile::Counter* get_split_timer = ADD_TIMER(custom_profile(), "GetSplitTime");

            _max_scanners = calc_max_scanners(p.parallelism(state));
            _split_source = std::make_shared<RemoteSplitSourceConnector>(
                    state, get_split_timer, split_source.split_source_id, split_source.num_splits,
                    _max_scanners);
        }
    }

    if (!p._batch_split_mode) {
        _max_scanners = calc_max_scanners(p.parallelism(state));
        if (_split_source == nullptr) {
            _split_source = std::make_shared<LocalSplitSourceConnector>(scan_ranges, _max_scanners);
        }
        // One FE columnar split can publish many format-local children after metadata planning.
        // Keep the requested scanner concurrency so those children do not run serially.
        bool can_generate_physical_splits = false;
        const TFileScanRangeParams* common_params = nullptr;
        if (state->get_query_ctx() != nullptr &&
            state->get_query_ctx()->file_scan_range_params_map.contains(parent_id())) {
            common_params = &state->get_query_ctx()->file_scan_range_params_map[parent_id()];
        }
        for (const auto& scan_range_params : scan_ranges) {
            const auto& file_scan_range =
                    scan_range_params.scan_range.ext_scan_range.file_scan_range;
            const auto* params =
                    file_scan_range.__isset.params ? &file_scan_range.params : common_params;
            if (params == nullptr) {
                continue;
            }
            const bool is_load =
                    state->desc_tbl().get_tuple_descriptor(params->src_tuple_id) != nullptr;
            can_generate_physical_splits =
                    std::ranges::any_of(file_scan_range.ranges, [&](const auto& range) {
                        return _can_generate_physical_splits(state->query_options(), is_load,
                                                             *params, range);
                    });
            if (can_generate_physical_splits) {
                break;
            }
        }
        // Currently the total number of remote splits cannot be accurately obtained, so batch
        // mode already skips this cap.
        _max_scanners = _adjust_scanner_count(_max_scanners, _split_source->num_scan_ranges(),
                                              can_generate_physical_splits);
    }

    if (!scan_ranges.empty() &&
        scan_ranges[0].scan_range.ext_scan_range.file_scan_range.__isset.params) {
        // for compatibility.
        // in new implement, the tuple id is set in prepare phase
        _output_tuple_id =
                scan_ranges[0].scan_range.ext_scan_range.file_scan_range.params.dest_tuple_id;
    }
}

Status FileScanLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(ScanLocalState<FileScanLocalState>::init(state, info));
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<FileScanOperatorX>();
    _output_tuple_id = p._output_tuple_id;
    _condition_cache_hit_counter = ADD_COUNTER(custom_profile(), "ConditionCacheHit", TUnit::UNIT);
    _condition_cache_filtered_rows_counter =
            ADD_COUNTER(custom_profile(), "ConditionCacheFilteredRows", TUnit::UNIT);
    return Status::OK();
}

Status FileScanLocalState::_process_conjuncts(RuntimeState* state) {
    RETURN_IF_ERROR(ScanLocalState<FileScanLocalState>::_process_conjuncts(state));
    if (Base::_eos) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
    return Status::OK();
}

Status FileScanOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanOperatorX<FileScanLocalState>::prepare(state));
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.contains(node_id())) {
        TFileScanRangeParams& params =
                state->get_query_ctx()->file_scan_range_params_map[node_id()];
        _output_tuple_id = params.dest_tuple_id;
    }
    return Status::OK();
}

} // namespace doris
