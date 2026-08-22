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

#include "exprs/vin_predicate.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/data_type/define_primitive_type.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/function/in.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_state.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris {

namespace {

size_t raw_in_value_size(PrimitiveType primitive_type) {
    switch (primitive_type) {
#define RETURN_RAW_IN_SIZE(TYPE) \
    case TYPE:                   \
        return sizeof(typename PrimitiveTypeTraits<TYPE>::CppType)
        RETURN_RAW_IN_SIZE(TYPE_BOOLEAN);
        RETURN_RAW_IN_SIZE(TYPE_TINYINT);
        RETURN_RAW_IN_SIZE(TYPE_SMALLINT);
        RETURN_RAW_IN_SIZE(TYPE_INT);
        RETURN_RAW_IN_SIZE(TYPE_BIGINT);
        RETURN_RAW_IN_SIZE(TYPE_LARGEINT);
        RETURN_RAW_IN_SIZE(TYPE_FLOAT);
        RETURN_RAW_IN_SIZE(TYPE_DOUBLE);
        RETURN_RAW_IN_SIZE(TYPE_DATE);
        RETURN_RAW_IN_SIZE(TYPE_DATETIME);
        RETURN_RAW_IN_SIZE(TYPE_DATEV2);
        RETURN_RAW_IN_SIZE(TYPE_DATETIMEV2);
        RETURN_RAW_IN_SIZE(TYPE_TIMESTAMP_NS);
        RETURN_RAW_IN_SIZE(TYPE_TIMESTAMPTZ);
        RETURN_RAW_IN_SIZE(TYPE_TIMEV2);
        RETURN_RAW_IN_SIZE(TYPE_DECIMAL32);
        RETURN_RAW_IN_SIZE(TYPE_DECIMAL64);
        RETURN_RAW_IN_SIZE(TYPE_DECIMALV2);
        RETURN_RAW_IN_SIZE(TYPE_DECIMAL128I);
        RETURN_RAW_IN_SIZE(TYPE_DECIMAL256);
        RETURN_RAW_IN_SIZE(TYPE_IPV4);
        RETURN_RAW_IN_SIZE(TYPE_IPV6);
#undef RETURN_RAW_IN_SIZE
    default:
        return 0;
    }
}

} // namespace

VInPredicate::VInPredicate(const TExprNode& node)
        : VExpr(node), _is_not_in(node.in_predicate.is_not_in) {}

Status VInPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                             VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_children.empty()) {
        return Status::InternalError("no Function operator in.");
    }

    _expr_name =
            fmt::format("({} {} set)", _children[0]->expr_name(), _is_not_in ? "not_in" : "in");

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(get_num_children());
    for (auto child : _children) {
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
    }

    // construct the proper function_name
    std::string head(_is_not_in ? "not_" : "");
    std::string real_function_name = head + std::string(function_name);
    auto arg_type = remove_nullable(argument_template[0].type);
    if (is_complex_type(arg_type->get_primitive_type())) {
        real_function_name = "collection_" + real_function_name;
    }
    _function = SimpleFunctionFactory::instance().get_function(real_function_name,
                                                               argument_template, _data_type, {});
    if (_function == nullptr) {
        return Status::NotSupported("Function {} is not implemented", real_function_name);
    }

    VExpr::register_function_context(state, context);
    _prepare_finished = true;

    if (state->query_options().__isset.in_list_value_count_threshold) {
        _in_list_value_count_threshold = state->query_options().in_list_value_count_threshold;
    }
    return Status::OK();
}

Status VInPredicate::open(RuntimeState* state, VExprContext* context,
                          FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& child : _children) {
        RETURN_IF_ERROR(child->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(state, context, scope, _function));
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }

    _is_args_all_constant = std::all_of(_children.begin() + 1, _children.end(),
                                        [](const VExprSPtr& expr) { return expr->is_constant(); });
    if (scope == FunctionContext::FRAGMENT_LOCAL && _is_args_all_constant &&
        !_zonemap_materialized) {
        RETURN_IF_ERROR(_materialize_for_zonemap_filter(context));
    }
    _open_finished = true;
    return Status::OK();
}

void VInPredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Status VInPredicate::evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) {
    DCHECK_GE(get_num_children(), 2);
    return _evaluate_inverted_index(context, _function, segment_num_rows);
}

Status VInPredicate::_materialize_for_zonemap_filter(VExprContext* context) {
    _seg_filter_values.clear();
    _seg_filter_contains_null = false;
    _seg_filter_contains_nan = false;
    _zonemap_materialized = false;
    _direct_filter_set.reset();
    if (_children.size() < 2) {
        return Status::OK();
    }

    auto bloom_probe = expr_zonemap::extract_bloom_filter_probe(_children[0]);
    if (!bloom_probe.has_value()) {
        return Status::OK();
    }
    // Materialization is shared by all pruning paths. Their capability checks keep ZoneMap,
    // dictionary, and raw evaluation direct-slot-only while Bloom may consume a nested leaf.
    const auto data_type = remove_nullable(bloom_probe->value_type);
    DORIS_CHECK(data_type != nullptr);
    if (is_complex_type(data_type->get_primitive_type())) {
        return Status::OK();
    }

    DORIS_CHECK(context != nullptr);
    auto* fn_ctx = context->fn_context(_fn_context_index);
    DORIS_CHECK(fn_ctx != nullptr);
    auto* in_state =
            reinterpret_cast<InState*>(fn_ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    DORIS_CHECK(in_state != nullptr);
    DORIS_CHECK(in_state->use_set);
    DORIS_CHECK(in_state->hybrid_set != nullptr);
    _direct_filter_set = in_state->hybrid_set;

    expr_zonemap::InZonemapMaterializedSet materialized;
    RETURN_IF_ERROR(expr_zonemap::materialize_hybrid_set_for_zonemap_filter(
            *in_state->hybrid_set, data_type, &materialized));
    _seg_filter_contains_null = materialized.contains_null;
    _seg_filter_contains_nan = materialized.contains_nan;
    _seg_filter_values = std::move(materialized.values);
    _seg_filter_min = std::move(materialized.min_value);
    _seg_filter_max = std::move(materialized.max_value);
    _zonemap_materialized = true;
    return Status::OK();
}

ZoneMapFilterResult VInPredicate::evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const {
    if (_is_not_in && _seg_filter_contains_null) {
        return ZoneMapFilterResult::kNoMatch;
    }
    return expr_zonemap::eval_in_zonemap(ctx, get_child(0), _is_not_in, _seg_filter_values,
                                         _seg_filter_contains_nan, _seg_filter_min,
                                         _seg_filter_max);
}

bool VInPredicate::can_evaluate_zonemap_filter() const {
    return _zonemap_materialized && std::dynamic_pointer_cast<VSlotRef>(get_child(0)) != nullptr;
}

ZoneMapFilterResult VInPredicate::evaluate_dictionary_filter(
        const DictionaryEvalContext& ctx) const {
    return expr_zonemap::eval_in_dictionary(ctx, get_child(0), _is_not_in, _seg_filter_values);
}

bool VInPredicate::can_evaluate_dictionary_filter() const {
    return _zonemap_materialized && !_is_not_in &&
           std::dynamic_pointer_cast<VSlotRef>(get_child(0)) != nullptr;
}

ZoneMapFilterResult VInPredicate::evaluate_bloom_filter(const BloomFilterEvalContext& ctx) const {
    return expr_zonemap::eval_in_bloom_filter(ctx, get_child(0), _is_not_in, _seg_filter_values);
}

bool VInPredicate::can_evaluate_bloom_filter() const {
    // A NaN member forces conservative retention regardless of the remaining finite probes.
    return _zonemap_materialized && !_is_not_in && !_seg_filter_contains_nan &&
           expr_zonemap::extract_bloom_filter_probe(get_child(0)).has_value();
}

bool VInPredicate::can_execute_on_raw_fixed_values(const DataTypePtr& data_type,
                                                   int column_id) const {
    if (!_zonemap_materialized || _direct_filter_set == nullptr || data_type == nullptr ||
        !_is_args_all_constant) {
        return false;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(get_child(0));
    if (slot == nullptr || slot->column_id() != column_id || slot->data_type() == nullptr) {
        return false;
    }
    const auto raw_type = remove_nullable(data_type);
    return remove_nullable(slot->data_type())->equals(*raw_type) &&
           raw_in_value_size(raw_type->get_primitive_type()) != 0;
}

Status VInPredicate::execute_on_raw_fixed_values(const uint8_t* values, size_t num_values,
                                                 size_t value_width, const DataTypePtr& data_type,
                                                 int column_id, uint8_t* matches) const {
    if (!can_execute_on_raw_fixed_values(data_type, column_id)) {
        return Status::NotSupported("IN predicate cannot evaluate raw fixed-width values");
    }
    DORIS_CHECK(values != nullptr || num_values == 0);
    DORIS_CHECK(matches != nullptr || num_values == 0);
    const size_t expected_width =
            raw_in_value_size(remove_nullable(data_type)->get_primitive_type());
    if (value_width != expected_width) {
        return Status::Corruption("Raw IN width {} does not match expected {}", value_width,
                                  expected_width);
    }
    // NOT IN with a NULL literal is UNKNOWN for every non-null physical value. Definition levels
    // handle input NULLs, while this guard preserves the remaining three-valued SQL invariant.
    if (_is_not_in && _seg_filter_contains_null) {
        std::fill(matches, matches + num_values, uint8_t {0});
    } else if (_is_not_in) {
        _direct_filter_set->find_batch_raw_fixed_negative(values, num_values, value_width, matches);
    } else {
        _direct_filter_set->find_batch_raw_fixed(values, num_values, value_width, matches);
    }
    return Status::OK();
}

bool VInPredicate::can_execute_on_raw_binary_values(const DataTypePtr& data_type,
                                                    int column_id) const {
    if (!_zonemap_materialized || _direct_filter_set == nullptr || data_type == nullptr ||
        !_is_args_all_constant ||
        !is_string_type(remove_nullable(data_type)->get_primitive_type())) {
        return false;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(get_child(0));
    return slot != nullptr && slot->column_id() == column_id && slot->data_type() != nullptr &&
           is_string_type(remove_nullable(slot->data_type())->get_primitive_type());
}

Status VInPredicate::execute_on_raw_binary_values(const StringRef* values, size_t num_values,
                                                  const DataTypePtr& data_type, int column_id,
                                                  uint8_t* matches) const {
    if (!can_execute_on_raw_binary_values(data_type, column_id)) {
        return Status::NotSupported("IN predicate cannot evaluate raw binary values");
    }
    DORIS_CHECK(values != nullptr || num_values == 0);
    DORIS_CHECK(matches != nullptr || num_values == 0);
    if (_is_not_in && _seg_filter_contains_null) {
        std::fill(matches, matches + num_values, uint8_t {0});
    } else if (_is_not_in) {
        _direct_filter_set->find_batch_raw_binary_negative(values, num_values, matches);
    } else {
        // Decoder-owned slices remain valid for the whole batch, so regular SQL IN/NOT IN can
        // probe the prepared hash set without constructing and then compacting a ColumnString.
        _direct_filter_set->find_batch_raw_binary(values, num_values, matches);
    }
    return Status::OK();
}

Status VInPredicate::execute_column_impl(VExprContext* context, const Block* block,
                                         const Selector* selector, size_t count,
                                         ColumnPtr& result_column) const {
    if (is_const_and_have_executed()) { // const have execute in open function
        result_column = get_result_from_const(count);
        return Status::OK();
    }
    if (fast_execute(context, selector, count, result_column)) {
        return Status::OK();
    }
    DCHECK(_open_finished || block == nullptr);

    // This is an optimization. For expressions like colA IN (1, 2, 3, 4),
    // where all values inside the IN clause are constants,
    // a hash set is created during open, and it will not be accessed again during execute
    //  Here, _children[0] is colA
    const size_t args_size = _is_args_all_constant ? 1 : _children.size();

    ColumnNumbers arguments;
    arguments.reserve(args_size);
    Block temp_block;
    for (int i = 0; i < args_size; ++i) {
        ColumnPtr column;
        RETURN_IF_ERROR(_children[i]->execute_column(context, block, selector, count, column));
        arguments.push_back(i);
        temp_block.insert({column, _children[i]->execute_type(block), _children[i]->expr_name()});
    }

    int num_columns_without_result = temp_block.columns();
    temp_block.insert({nullptr, _data_type, _expr_name});

    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), temp_block,
                                       arguments, num_columns_without_result, temp_block.rows()));
    result_column = temp_block.get_by_position(num_columns_without_result).column;
    DCHECK_EQ(result_column->size(), count);
    return Status::OK();
}

size_t VInPredicate::estimate_memory(const size_t rows) {
    if (is_const_and_have_executed()) {
        return 0;
    }

    size_t estimate_size = 0;

    for (int i = 0; i < _children.size(); ++i) {
        estimate_size += _children[i]->estimate_memory(rows);
    }

    if (_data_type->is_nullable()) {
        estimate_size += rows * sizeof(uint8_t);
    }

    estimate_size += rows * sizeof(uint8_t);

    return estimate_size;
}

const std::string& VInPredicate::expr_name() const {
    return _expr_name;
}

std::string VInPredicate::debug_string() const {
    std::stringstream out;
    out << "InPredicate(" << children()[0]->debug_string() << " " << _is_not_in << ",[";
    int num_children = get_num_children();

    for (int i = 1; i < num_children; ++i) {
        out << (i == 1 ? "" : " ") << children()[i]->debug_string();
    }

    out << "])";
    return out.str();
}

} // namespace doris
