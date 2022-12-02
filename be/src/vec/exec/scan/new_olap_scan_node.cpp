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

#include "vec/exec/scan/new_olap_scan_node.h"

#include "common/status.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_fragment_context.h"
#include "util/to_string.h"
#include "vec/columns/column_const.h"
#include "vec/exec/scan/new_olap_scanner.h"

namespace doris::vectorized {

NewOlapScanNode::NewOlapScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs), _olap_scan_node(tnode.olap_scan_node) {
    _output_tuple_id = tnode.olap_scan_node.tuple_id;
    if (_olap_scan_node.__isset.sort_info && _olap_scan_node.__isset.sort_limit) {
        _limit_per_scanner = _olap_scan_node.sort_limit;
    }
}

Status NewOlapScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    return Status::OK();
}

Status NewOlapScanNode::_init_profile() {
    RETURN_IF_ERROR(VScanNode::_init_profile());

    _num_disks_accessed_counter = ADD_COUNTER(_runtime_profile, "NumDiskAccess", TUnit::UNIT);
    _tablet_counter = ADD_COUNTER(_runtime_profile, "TabletNum", TUnit::UNIT);

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

    _rows_vec_cond_counter = ADD_COUNTER(_segment_profile, "RowsVectorPredFiltered", TUnit::UNIT);
    _vec_cond_timer = ADD_TIMER(_segment_profile, "VectorPredEvalTime");
    _short_cond_timer = ADD_TIMER(_segment_profile, "ShortPredEvalTime");
    _first_read_timer = ADD_TIMER(_segment_profile, "FirstReadTime");
    _first_read_seek_timer = ADD_TIMER(_segment_profile, "FirstReadSeekTime");
    _first_read_seek_counter = ADD_COUNTER(_segment_profile, "FirstReadSeekCount", TUnit::UNIT);

    _lazy_read_timer = ADD_TIMER(_segment_profile, "LazyReadTime");
    _lazy_read_seek_timer = ADD_TIMER(_segment_profile, "LazyReadSeekTime");
    _lazy_read_seek_counter = ADD_COUNTER(_segment_profile, "LazyReadSeekCount", TUnit::UNIT);

    _output_col_timer = ADD_TIMER(_segment_profile, "OutputColumnTime");

    _stats_filtered_counter = ADD_COUNTER(_segment_profile, "RowsStatsFiltered", TUnit::UNIT);
    _bf_filtered_counter = ADD_COUNTER(_segment_profile, "RowsBloomFilterFiltered", TUnit::UNIT);
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

    _filtered_segment_counter = ADD_COUNTER(_segment_profile, "NumSegmentFiltered", TUnit::UNIT);
    _total_segment_counter = ADD_COUNTER(_segment_profile, "NumSegmentTotal", TUnit::UNIT);

    // for the purpose of debugging or profiling
    for (int i = 0; i < GENERAL_DEBUG_COUNT; ++i) {
        char name[64];
        snprintf(name, sizeof(name), "GeneralDebugTimer%d", i);
        _general_debug_timer[i] = ADD_TIMER(_segment_profile, name);
    }
    return Status::OK();
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

Status NewOlapScanNode::_process_conjuncts() {
    SCOPED_TIMER(_process_conjunct_timer);
    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
    if (_eos) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_build_key_ranges_and_filters());
    return Status::OK();
}

Status NewOlapScanNode::_build_key_ranges_and_filters() {
    if (!_olap_scan_node.__isset.push_down_agg_type_opt ||
        _olap_scan_node.push_down_agg_type_opt == TPushAggOp::NONE) {
        const std::vector<std::string>& column_names = _olap_scan_node.key_column_name;
        const std::vector<TPrimitiveType::type>& column_types = _olap_scan_node.key_column_type;
        DCHECK(column_types.size() == column_names.size());

        // 1. construct scan key except last olap engine short key
        _scan_keys.set_is_convertible(limit() == -1);

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
                        if (range.get_fixed_value_size() <= _max_pushdown_conditions_per_column) {
                            RETURN_IF_ERROR(_scan_keys.extend_scan_key(
                                    temp_range, _max_scan_key_num, &exact_range, &eos));
                            if (exact_range) {
                                _colname_to_value_range.erase(iter->first);
                            }
                        }
                        return Status::OK();
                    },
                    iter->second));
        }
        _eos |= eos;

        for (auto& iter : _colname_to_value_range) {
            std::vector<TCondition> filters;
            std::visit([&](auto&& range) { range.to_olap_filter(filters); }, iter.second);

            for (const auto& filter : filters) {
                _olap_filters.push_back(filter);
            }
        }

        // Append value ranges in "_not_in_value_ranges"
        for (auto& range : _not_in_value_ranges) {
            std::visit([&](auto&& the_range) { the_range.to_in_condition(_olap_filters, false); },
                       range);
        }
    } else {
        _runtime_profile->add_info_string(
                "PushDownAggregate",
                push_down_agg_to_string(_olap_scan_node.push_down_agg_type_opt));
    }

    if (_state->enable_profile()) {
        _runtime_profile->add_info_string("PushDownPredicates",
                                          olap_filters_to_string(_olap_filters));
        _runtime_profile->add_info_string("KeyRanges", _scan_keys.debug_string());
        _runtime_profile->add_info_string("TabletIds", tablets_id_to_string(_scan_ranges));
    }
    VLOG_CRITICAL << _scan_keys.debug_string();

    return Status::OK();
}

VScanNode::PushDownType NewOlapScanNode::_should_push_down_function_filter(
        VectorizedFnCall* fn_call, VExprContext* expr_ctx, StringVal* constant_str,
        doris_udf::FunctionContext** fn_ctx) {
    // Now only `like` function filters is supported to push down
    if (fn_call->fn().name.function_name != "like") {
        return PushDownType::UNACCEPTABLE;
    }

    const auto& children = fn_call->children();
    doris_udf::FunctionContext* func_cxt = expr_ctx->fn_context(fn_call->fn_context_index());
    DCHECK(func_cxt != nullptr);
    DCHECK(children.size() == 2);
    for (size_t i = 0; i < children.size(); i++) {
        if (VExpr::expr_without_cast(children[i])->node_type() != TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            continue;
        }
        if (!children[1 - i]->is_constant()) {
            // only handle constant value
            return PushDownType::UNACCEPTABLE;
        } else {
            DCHECK(children[1 - i]->type().is_string_type());
            if (const ColumnConst* const_column = check_and_get_column<ColumnConst>(
                        children[1 - i]->get_const_col(expr_ctx)->column_ptr)) {
                *constant_str = const_column->get_data_at(0).to_string_val();
            } else {
                return PushDownType::UNACCEPTABLE;
            }
        }
    }
    *fn_ctx = func_cxt;
    return PushDownType::ACCEPTABLE;
}

// PlanFragmentExecutor will call this method to set scan range
// Doris scan range is defined in thrift file like this
// struct TPaloScanRange {
//  1: required list<Types.TNetworkAddress> hosts
//  2: required string schema_hash
//  3: required string version
//  5: required Types.TTabletId tablet_id
//  6: required string db_name
//  7: optional list<TKeyRange> partition_column_ranges
//  8: optional string index_name
//  9: optional string table_name
//}
// every doris_scan_range is related with one tablet so that one olap scan node contains multiple tablet
void NewOlapScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        DCHECK(scan_range.scan_range.__isset.palo_scan_range);
        _scan_ranges.emplace_back(new TPaloScanRange(scan_range.scan_range.palo_scan_range));
        COUNTER_UPDATE(_tablet_counter, 1);
    }
    // telemetry::set_current_span_attribute(_tablet_counter);

    return;
}

std::string NewOlapScanNode::get_name() {
    return fmt::format("VNewOlapScanNode({0})", _olap_scan_node.table_name);
}

Status NewOlapScanNode::_init_scanners(std::list<VScanner*>* scanners) {
    if (_scan_ranges.empty()) {
        _eos = true;
        return Status::OK();
    }
    SCOPED_TIMER(_scanner_init_timer);
    auto span = opentelemetry::trace::Tracer::GetCurrentSpan();

    if (_vconjunct_ctx_ptr && (*_vconjunct_ctx_ptr)->root()) {
        _runtime_profile->add_info_string("RemainedDownPredicates",
                                          (*_vconjunct_ctx_ptr)->root()->debug_string());
    }

    // ranges constructed from scan keys
    std::vector<std::unique_ptr<doris::OlapScanRange>> cond_ranges;
    RETURN_IF_ERROR(_scan_keys.get_key_range(&cond_ranges));
    // if we can't get ranges from conditions, we give it a total range
    if (cond_ranges.empty()) {
        cond_ranges.emplace_back(new doris::OlapScanRange());
    }
    int scanners_per_tablet = std::max(1, 64 / (int)_scan_ranges.size());

    std::unordered_set<std::string> disk_set;
    for (auto& scan_range : _scan_ranges) {
        auto tablet_id = scan_range->tablet_id;
        std::string err;
        TabletSharedPtr tablet =
                StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (tablet == nullptr) {
            std::stringstream ss;
            ss << "failed to get tablet: " << tablet_id << ", reason: " << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        std::vector<std::unique_ptr<doris::OlapScanRange>>* ranges = &cond_ranges;
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

            NewOlapScanner* scanner = new NewOlapScanner(
                    _state, this, _limit_per_scanner, _olap_scan_node.is_preaggregation,
                    _need_agg_finalize, *scan_range, _scanner_profile.get());
            // add scanner to pool before doing prepare.
            // so that scanner can be automatically deconstructed if prepare failed.
            _scanner_pool.add(scanner);
            RETURN_IF_ERROR(scanner->prepare(*scan_range, scanner_ranges, _vconjunct_ctx_ptr.get(),
                                             _olap_filters, _filter_predicates,
                                             _push_down_functions));
            scanners->push_back((VScanner*)scanner);
            disk_set.insert(scanner->scan_disk());
        }
    }

    COUNTER_SET(_num_disks_accessed_counter, static_cast<int64_t>(disk_set.size()));
    // telemetry::set_span_attribute(span, _num_disks_accessed_counter);
    // telemetry::set_span_attribute(span, _num_scanners);

    return Status::OK();
}

bool NewOlapScanNode::_is_key_column(const std::string& key_name) {
    // all column in dup_keys table or unique_keys with merge on write table olap scan node threat
    // as key column
    if (_olap_scan_node.keyType == TKeysType::DUP_KEYS ||
        (_olap_scan_node.keyType == TKeysType::UNIQUE_KEYS &&
         _olap_scan_node.__isset.enable_unique_key_merge_on_write &&
         _olap_scan_node.enable_unique_key_merge_on_write)) {
        return true;
    }

    auto res = std::find(_olap_scan_node.key_column_name.begin(),
                         _olap_scan_node.key_column_name.end(), key_name);
    return res != _olap_scan_node.key_column_name.end();
}

}; // namespace doris::vectorized
