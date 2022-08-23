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

#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "vec/columns/column_const.h"
#include "vec/exec/scan/new_olap_scanner.h"
#include "vec/functions/in.h"

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
    _scanner_mem_tracker = std::make_unique<MemTracker>("OlapScanners");
    return Status::OK();
}

Status NewOlapScanNode::_init_profile() {
    return Status::OK();
}

Status NewOlapScanNode::_process_conjuncts() {
    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
    if (_eos) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_build_key_ranges_and_filters());
    return Status::OK();
}

Status NewOlapScanNode::_build_key_ranges_and_filters() {
    const std::vector<std::string>& column_names = _olap_scan_node.key_column_name;
    const std::vector<TPrimitiveType::type>& column_types = _olap_scan_node.key_column_type;
    DCHECK(column_types.size() == column_names.size());

    // 1. construct scan key except last olap engine short key
    _scan_keys.set_is_convertible(limit() == -1);

    // we use `exact_range` to identify a key range is an exact range or not when we convert
    // it to `_scan_keys`. If `exact_range` is true, we can just discard it from `_olap_filters`.
    bool exact_range = true;
    for (int column_index = 0; column_index < column_names.size() && !_scan_keys.has_range_value();
         ++column_index) {
        auto iter = _colname_to_value_range.find(column_names[column_index]);
        if (_colname_to_value_range.end() == iter) {
            break;
        }

        RETURN_IF_ERROR(std::visit(
                [&](auto&& range) {
                    RETURN_IF_ERROR(
                            _scan_keys.extend_scan_key(range, _max_scan_key_num, &exact_range));
                    if (exact_range) {
                        _colname_to_value_range.erase(iter->first);
                    }
                    return Status::OK();
                },
                iter->second));
    }

    for (auto& iter : _colname_to_value_range) {
        std::vector<TCondition> filters;
        std::visit([&](auto&& range) { range.to_olap_filter(filters); }, iter.second);

        for (const auto& filter : filters) {
            _olap_filters.push_back(std::move(filter));
        }
    }

    // _runtime_profile->add_info_string("PushDownPredicate", olap_filters_to_string(_olap_filters));
    // _runtime_profile->add_info_string("KeyRanges", _scan_keys.debug_string());
    VLOG_CRITICAL << _scan_keys.debug_string();

    return Status::OK();
}

bool NewOlapScanNode::_should_push_down_binary_predicate(
        VectorizedFnCall* fn_call, VExprContext* expr_ctx, StringRef* constant_val,
        int* slot_ref_child, const std::function<bool(const std::string&)>& fn_checker) {
    if (!fn_checker(fn_call->fn().name.function_name)) {
        return false;
    }

    const auto& children = fn_call->children();
    DCHECK(children.size() == 2);
    for (size_t i = 0; i < children.size(); i++) {
        if (VExpr::expr_without_cast(children[i])->node_type() != TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            continue;
        }
        if (!children[1 - i]->is_constant()) {
            // only handle constant value
            return false;
        } else {
            if (const ColumnConst* const_column = check_and_get_column<ColumnConst>(
                        children[1 - i]->get_const_col(expr_ctx)->column_ptr)) {
                *slot_ref_child = i;
                *constant_val = const_column->get_data_at(0);
            } else {
                return false;
            }
        }
    }
    return true;
}

bool NewOlapScanNode::_should_push_down_in_predicate(VInPredicate* pred, VExprContext* expr_ctx,
                                                     bool is_not_in) {
    if (pred->is_not_in() != is_not_in) {
        return false;
    }
    InState* state = reinterpret_cast<InState*>(
            expr_ctx->fn_context(pred->fn_context_index())
                    ->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    HybridSetBase* set = state->hybrid_set.get();

    // if there are too many elements in InPredicate, exceed the limit,
    // we will not push any condition of this column to storage engine.
    // because too many conditions pushed down to storage engine may even
    // slow down the query process.
    // ATTN: This is just an experience value. You may need to try
    // different thresholds to improve performance.
    if (set->size() > _max_pushdown_conditions_per_column) {
        VLOG_NOTICE << "Predicate value num " << set->size() << " exceed limit "
                    << _max_pushdown_conditions_per_column;
        return false;
    }
    return true;
}

bool NewOlapScanNode::_should_push_down_function_filter(VectorizedFnCall* fn_call,
                                                        VExprContext* expr_ctx,
                                                        StringVal* constant_str,
                                                        doris_udf::FunctionContext** fn_ctx) {
    // Now only `like` function filters is supported to push down
    if (fn_call->fn().name.function_name != "like") {
        return false;
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
            return false;
        } else {
            DCHECK(children[1 - i]->type().is_string_type());
            if (const ColumnConst* const_column = check_and_get_column<ColumnConst>(
                        children[1 - i]->get_const_col(expr_ctx)->column_ptr)) {
                *constant_str = const_column->get_data_at(0).to_string_val();
            } else {
                return false;
            }
        }
    }
    *fn_ctx = func_cxt;
    return true;
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
        // COUNTER_UPDATE(_tablet_counter, 1);
    }
    // telemetry::set_current_span_attribute(_tablet_counter);

    return;
}

Status NewOlapScanNode::_init_scanners(std::list<VScanner*>* scanners) {
    if (_scan_ranges.empty()) {
        _eos = true;
        return Status::OK();
    }
    auto span = opentelemetry::trace::Tracer::GetCurrentSpan();

    // ranges constructed from scan keys
    std::vector<std::unique_ptr<doris::OlapScanRange>> cond_ranges;
    RETURN_IF_ERROR(_scan_keys.get_key_range(&cond_ranges));
    // if we can't get ranges from conditions, we give it a total range
    if (cond_ranges.empty()) {
        cond_ranges.emplace_back(new doris::OlapScanRange());
    }
    int scanners_per_tablet = std::max(1, 64 / (int)_scan_ranges.size());

    // std::unordered_set<std::string> disk_set;
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
                    1, (int)tablet->tablet_footprint() / config::doris_scan_range_max_mb << 20);
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
                    _need_agg_finalize, *scan_range, _scanner_mem_tracker.get());
            // add scanner to pool before doing prepare.
            // so that scanner can be automatically deconstructed if prepare failed.
            _scanner_pool.add(scanner);
            RETURN_IF_ERROR(scanner->prepare(*scan_range, scanner_ranges, _vconjunct_ctx_ptr.get(),
                                             _olap_filters, _bloom_filters_push_down,
                                             _push_down_functions));
            scanners->push_back((VScanner*)scanner);
            // disk_set.insert(scanner->scan_disk());
        }
    }

    // COUNTER_SET(_num_disks_accessed_counter, static_cast<int64_t>(disk_set.size()));
    // COUNTER_SET(_num_scanners, static_cast<int64_t>(_volap_scanners.size()));
    // telemetry::set_span_attribute(span, _num_disks_accessed_counter);
    // telemetry::set_span_attribute(span, _num_scanners);

    // init progress
    // std::stringstream ss;
    // ss << "ScanThread complete (node=" << id() << "):";
    // _progress = ProgressUpdater(ss.str(), _volap_scanners.size(), 1);
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
