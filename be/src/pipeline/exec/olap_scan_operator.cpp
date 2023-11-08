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

#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "pipeline/exec/scan_operator.h"
#include "service/backend_options.h"
#include "util/to_string.h"
#include "vec/exec/runtime_filter_consumer.h"
#include "vec/exec/scan/new_olap_scanner.h"
#include "vec/exec/scan/pip_scanner_context.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/in.h"

namespace doris::pipeline {

Status OlapScanLocalState::_init_profile() {
    RETURN_IF_ERROR(ScanLocalState<OlapScanLocalState>::_init_profile());
    // 1. init segment profile
    _segment_profile.reset(new RuntimeProfile("SegmentIterator"));
    _scanner_profile->add_child(_segment_profile.get(), true, nullptr);

    // 2. init timer and counters
    _reader_init_timer = ADD_TIMER(_scanner_profile, "ReaderInitTime");
    _scanner_init_timer = ADD_TIMER(_scanner_profile, "ScannerInitTime");
    _process_conjunct_timer = ADD_TIMER(_runtime_profile, "ProcessConjunctTime");
    _read_compressed_counter = ADD_COUNTER(_segment_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter =
            ADD_COUNTER(_segment_profile, "UncompressedBytesRead", TUnit::BYTES);
    _block_load_timer = ADD_TIMER(_segment_profile, "BlockLoadTime");
    _block_load_counter = ADD_COUNTER(_segment_profile, "BlocksLoad", TUnit::UNIT);
    _block_fetch_timer = ADD_TIMER(_scanner_profile, "BlockFetchTime");
    _raw_rows_counter = ADD_COUNTER(_segment_profile, "RawRowsRead", TUnit::UNIT);
    _block_convert_timer = ADD_TIMER(_scanner_profile, "BlockConvertTime");
    _block_init_timer = ADD_TIMER(_segment_profile, "BlockInitTime");
    _block_init_seek_timer = ADD_TIMER(_segment_profile, "BlockInitSeekTime");
    _block_init_seek_counter = ADD_COUNTER(_segment_profile, "BlockInitSeekCount", TUnit::UNIT);
    _block_conditions_filtered_timer = ADD_TIMER(_segment_profile, "BlockConditionsFilteredTime");

    _rows_vec_cond_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsVectorPredFiltered", TUnit::UNIT);
    _rows_short_circuit_cond_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsShortCircuitPredFiltered", TUnit::UNIT);
    _rows_vec_cond_input_counter =
            ADD_COUNTER(_segment_profile, "RowsVectorPredInput", TUnit::UNIT);
    _rows_short_circuit_cond_input_counter =
            ADD_COUNTER(_segment_profile, "RowsShortCircuitPredInput", TUnit::UNIT);
    _rows_common_expr_filtered_counter =
            ADD_COUNTER(_segment_profile, "RowsCommonExprFiltered", TUnit::UNIT);
    _vec_cond_timer = ADD_TIMER(_segment_profile, "VectorPredEvalTime");
    _short_cond_timer = ADD_TIMER(_segment_profile, "ShortPredEvalTime");
    _expr_filter_timer = ADD_TIMER(_segment_profile, "ExprFilterEvalTime");
    _first_read_timer = ADD_TIMER(_segment_profile, "FirstReadTime");
    _second_read_timer = ADD_TIMER(_segment_profile, "SecondReadTime");
    _first_read_seek_timer = ADD_TIMER(_segment_profile, "FirstReadSeekTime");
    _first_read_seek_counter = ADD_COUNTER(_segment_profile, "FirstReadSeekCount", TUnit::UNIT);

    _lazy_read_timer = ADD_TIMER(_segment_profile, "LazyReadTime");
    _lazy_read_seek_timer = ADD_TIMER(_segment_profile, "LazyReadSeekTime");
    _lazy_read_seek_counter = ADD_COUNTER(_segment_profile, "LazyReadSeekCount", TUnit::UNIT);

    _output_col_timer = ADD_TIMER(_segment_profile, "OutputColumnTime");

    _stats_filtered_counter = ADD_COUNTER(_segment_profile, "RowsStatsFiltered", TUnit::UNIT);
    _bf_filtered_counter = ADD_COUNTER(_segment_profile, "RowsBloomFilterFiltered", TUnit::UNIT);
    _dict_filtered_counter = ADD_COUNTER(_segment_profile, "RowsDictFiltered", TUnit::UNIT);
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
    _inverted_index_query_bitmap_copy_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexQueryBitmapCopyTime");
    _inverted_index_query_bitmap_op_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexQueryBitmapOpTime");
    _inverted_index_searcher_open_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexSearcherOpenTime");
    _inverted_index_searcher_search_timer =
            ADD_TIMER(_segment_profile, "InvertedIndexSearcherSearchTime");

    _output_index_result_column_timer = ADD_TIMER(_segment_profile, "OutputIndexResultColumnTimer");

    _filtered_segment_counter = ADD_COUNTER(_segment_profile, "NumSegmentFiltered", TUnit::UNIT);
    _total_segment_counter = ADD_COUNTER(_segment_profile, "NumSegmentTotal", TUnit::UNIT);
    _tablet_counter = ADD_COUNTER(_runtime_profile, "TabletNum", TUnit::UNIT);
    return Status::OK();
}

Status OlapScanLocalState::_process_conjuncts() {
    SCOPED_TIMER(_process_conjunct_timer);
    RETURN_IF_ERROR(ScanLocalState::_process_conjuncts());
    if (ScanLocalState::_eos_dependency->read_blocked_by() == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_build_key_ranges_and_filters());
    return Status::OK();
}

bool OlapScanLocalState::_is_key_column(const std::string& key_name) {
    auto& p = _parent->cast<OlapScanOperatorX>();
    // all column in dup_keys table or unique_keys with merge on write table olap scan node threat
    // as key column
    if (p._olap_scan_node.keyType == TKeysType::DUP_KEYS ||
        (p._olap_scan_node.keyType == TKeysType::UNIQUE_KEYS &&
         p._olap_scan_node.__isset.enable_unique_key_merge_on_write &&
         p._olap_scan_node.enable_unique_key_merge_on_write)) {
        return true;
    }

    auto res = std::find(p._olap_scan_node.key_column_name.begin(),
                         p._olap_scan_node.key_column_name.end(), key_name);
    return res != p._olap_scan_node.key_column_name.end();
}

Status OlapScanLocalState::_should_push_down_function_filter(
        vectorized::VectorizedFnCall* fn_call, vectorized::VExprContext* expr_ctx,
        StringRef* constant_str, doris::FunctionContext** fn_ctx,
        vectorized::VScanNode::PushDownType& pdt) {
    // Now only `like` function filters is supported to push down
    if (fn_call->fn().name.function_name != "like") {
        pdt = vectorized::VScanNode::PushDownType::UNACCEPTABLE;
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
            pdt = vectorized::VScanNode::PushDownType::UNACCEPTABLE;
            return Status::OK();
        } else {
            DCHECK(children[1 - i]->type().is_string_type());
            std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
            RETURN_IF_ERROR(children[1 - i]->get_const_col(expr_ctx, &const_col_wrapper));
            if (const vectorized::ColumnConst* const_column =
                        check_and_get_column<vectorized::ColumnConst>(
                                const_col_wrapper->column_ptr)) {
                *constant_str = const_column->get_data_at(0);
            } else {
                pdt = vectorized::VScanNode::PushDownType::UNACCEPTABLE;
                return Status::OK();
            }
        }
    }
    *fn_ctx = func_cxt;
    pdt = vectorized::VScanNode::PushDownType::ACCEPTABLE;
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

Status OlapScanLocalState::_init_scanners(std::list<vectorized::VScannerSPtr>* scanners) {
    if (_scan_ranges.empty()) {
        ScanLocalState::_eos_dependency->set_ready_for_read();
        return Status::OK();
    }
    SCOPED_TIMER(_scanner_init_timer);
    auto span = opentelemetry::trace::Tracer::GetCurrentSpan();

    if (!_conjuncts.empty()) {
        std::string message;
        for (auto& conjunct : _conjuncts) {
            if (conjunct->root()) {
                if (!message.empty()) {
                    message += ", ";
                }
                message += conjunct->root()->debug_string();
            }
        }
        _runtime_profile->add_info_string("RemainedDownPredicates", message);
    }
    auto& p = _parent->cast<OlapScanOperatorX>();

    if (!p._olap_scan_node.output_column_unique_ids.empty()) {
        for (auto uid : p._olap_scan_node.output_column_unique_ids) {
            _maybe_read_column_ids.emplace(uid);
        }
    }

    // ranges constructed from scan keys
    RETURN_IF_ERROR(_scan_keys.get_key_range(&_cond_ranges));
    // if we can't get ranges from conditions, we give it a total range
    if (_cond_ranges.empty()) {
        _cond_ranges.emplace_back(new doris::OlapScanRange());
    }
    int scanners_per_tablet = std::max(1, 64 / (int)_scan_ranges.size());

    auto build_new_scanner = [&](BaseTabletSPtr tablet, int64_t version,
                                 const std::vector<OlapScanRange*>& key_ranges) {
        auto scanner = vectorized::NewOlapScanner::create_shared(
                this, vectorized::NewOlapScanner::Params {
                              state(),
                              _scanner_profile.get(),
                              key_ranges,
                              std::move(tablet),
                              version,
                              {},
                              p._limit_per_scanner,
                              p._olap_scan_node.is_preaggregation,
                      });
        RETURN_IF_ERROR(scanner->prepare(state(), _conjuncts));
        scanner->set_compound_filters(_compound_filters);
        scanners->push_back(scanner);
        return Status::OK();
    };
    for (auto& scan_range : _scan_ranges) {
        auto tablet = DORIS_TRY(ExecEnv::get_tablet(scan_range->tablet_id));
        int64_t version = 0;
        std::from_chars(scan_range->version.data(),
                        scan_range->version.data() + scan_range->version.size(), version);
        std::vector<std::unique_ptr<doris::OlapScanRange>>* ranges = &_cond_ranges;
        int size_based_scanners_per_tablet = 1;

        if (config::doris_scan_range_max_mb > 0) {
            size_based_scanners_per_tablet = std::max(
                    1, (int)(tablet->tablet_footprint() / (config::doris_scan_range_max_mb << 20)));
        }
        int ranges_per_scanner =
                std::max(1, (int)ranges->size() /
                                    std::min(scanners_per_tablet, size_based_scanners_per_tablet));
        int num_ranges = ranges->size();
        for (int i = 0; i < num_ranges;) {
            std::vector<doris::OlapScanRange*> scanner_ranges;
            scanner_ranges.push_back((*ranges)[i].get());
            ++i;
            for (int j = 1; i < num_ranges && j < ranges_per_scanner &&
                            (*ranges)[i]->end_include == (*ranges)[i - 1]->end_include;
                 ++j, ++i) {
                scanner_ranges.push_back((*ranges)[i].get());
            }
            RETURN_IF_ERROR(build_new_scanner(tablet, version, scanner_ranges));
        }
    }

    return Status::OK();
}

TOlapScanNode& OlapScanLocalState::olap_scan_node() {
    return _parent->cast<OlapScanOperatorX>()._olap_scan_node;
}

void OlapScanLocalState::set_scan_ranges(RuntimeState* state,
                                         const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        DCHECK(scan_range.scan_range.__isset.palo_scan_range);
        _scan_ranges.emplace_back(new TPaloScanRange(scan_range.scan_range.palo_scan_range));
        //        COUNTER_UPDATE(_tablet_counter, 1);
    }
    // telemetry::set_current_span_attribute(_tablet_counter);
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

static std::string olap_filters_to_string(const std::vector<doris::TCondition>& filters) {
    std::string filters_string;
    filters_string += "[";
    for (auto it = filters.cbegin(); it != filters.cend(); it++) {
        if (it != filters.cbegin()) {
            filters_string += ", ";
        }
        filters_string += olap_filter_to_string(*it);
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
        bool eos = false;
        for (int column_index = 0;
             column_index < column_names.size() && !_scan_keys.has_range_value() && !eos;
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
                            RETURN_IF_ERROR(_scan_keys.extend_scan_key(
                                    temp_range, p._max_scan_key_num, &exact_range, &eos));
                            if (exact_range) {
                                _colname_to_value_range.erase(iter->first);
                            }
                        } else {
                            // if exceed max_pushdown_conditions_per_column, use whole_value_rang instead
                            // and will not erase from _colname_to_value_range, it must be not exact_range
                            temp_range.set_whole_value_range();
                            RETURN_IF_ERROR(_scan_keys.extend_scan_key(
                                    temp_range, p._max_scan_key_num, &exact_range, &eos));
                        }
                        return Status::OK();
                    },
                    iter->second));
        }
        if (eos) {
            ScanLocalState::_eos_dependency->set_ready_for_read();
        }

        for (auto& iter : _colname_to_value_range) {
            std::vector<TCondition> filters;
            std::visit([&](auto&& range) { range.to_olap_filter(filters); }, iter.second);

            for (const auto& filter : filters) {
                _olap_filters.push_back(filter);
            }
        }

        for (auto& iter : _compound_value_ranges) {
            std::vector<TCondition> filters;
            std::visit(
                    [&](auto&& range) {
                        if (range.is_in_compound_value_range()) {
                            range.to_condition_in_compound(filters);
                        } else if (range.is_match_value_range()) {
                            range.to_match_condition(filters);
                        }
                    },
                    iter);
            for (const auto& filter : filters) {
                _compound_filters.push_back(filter);
            }
        }

        // Append value ranges in "_not_in_value_ranges"
        for (auto& range : _not_in_value_ranges) {
            std::visit([&](auto&& the_range) { the_range.to_in_condition(_olap_filters, false); },
                       range);
        }
    } else {
        _runtime_profile->add_info_string("PushDownAggregate",
                                          push_down_agg_to_string(p._push_down_agg_type));
    }

    if (state()->enable_profile()) {
        _runtime_profile->add_info_string("PushDownPredicates",
                                          olap_filters_to_string(_olap_filters));
        _runtime_profile->add_info_string("KeyRanges", _scan_keys.debug_string());
        _runtime_profile->add_info_string("TabletIds", tablets_id_to_string(_scan_ranges));
    }
    VLOG_CRITICAL << _scan_keys.debug_string();

    return Status::OK();
}

void OlapScanLocalState::add_filter_info(int id, const PredicateFilterInfo& update_info) {
    std::unique_lock lock(_profile_mtx);
    // update
    _filter_info[id].filtered_row += update_info.filtered_row;
    _filter_info[id].input_row += update_info.input_row;
    _filter_info[id].type = update_info.type;
    // to string
    auto& info = _filter_info[id];
    std::string filter_name = "RuntimeFilterInfo id ";
    filter_name += std::to_string(id);
    std::string info_str;
    info_str += "type = " + type_to_string(static_cast<PredicateType>(info.type)) + ", ";
    info_str += "input = " + std::to_string(info.input_row) + ", ";
    info_str += "filtered = " + std::to_string(info.filtered_row);
    info_str = "[" + info_str + "]";

    // add info
    _segment_profile->add_info_string(filter_name, info_str);
}

OlapScanOperatorX::OlapScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                     const DescriptorTbl& descs)
        : ScanOperatorX<OlapScanLocalState>(pool, tnode, operator_id, descs),
          _olap_scan_node(tnode.olap_scan_node) {
    _output_tuple_id = tnode.olap_scan_node.tuple_id;
    _col_distribute_ids = tnode.olap_scan_node.distribute_column_ids;
    if (_olap_scan_node.__isset.sort_info && _olap_scan_node.__isset.sort_limit) {
        _limit_per_scanner = _olap_scan_node.sort_limit;
    }
}

} // namespace doris::pipeline
