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

#include "exprs/vbloom_predicate.h"

#include <cstddef>
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_state.h"

namespace doris {
class RowDescriptor;
class TExprNode;

} // namespace doris

namespace doris {
#include "common/compile_check_begin.h"

class VExprContext;

VBloomPredicate::VBloomPredicate(const TExprNode& node) : VExpr(node), _filter(nullptr) {}

Status VBloomPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_children.size() != 1) {
        return Status::InternalError("Invalid argument for VBloomPredicate.");
    }

    _prepare_finished = true;
    return Status::OK();
}

Status VBloomPredicate::open(RuntimeState* state, VExprContext* context,
                             FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

void VBloomPredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close(context, scope);
}

Status VBloomPredicate::_do_execute(VExprContext* context, const Block* block,
                                    const uint8_t* __restrict filter, Selector* selector,
                                    size_t count, ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr);
    DCHECK(!(filter != nullptr && selector != nullptr))
            << "filter and selector can not be both set";
    DCHECK_EQ(_children.size(), 1);

    ColumnPtr argument_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, argument_column));
    argument_column = argument_column->convert_to_full_column_if_const();

    size_t sz = argument_column->size();
    auto res_data_column = ColumnUInt8::create(sz);

    res_data_column->resize(sz);
    auto* ptr = ((ColumnUInt8*)res_data_column.get())->get_data().data();

    _filter->find_fixed_len(argument_column, ptr, filter);

    result_column = std::move(res_data_column);
    DCHECK_EQ(result_column->size(), count);
    return Status::OK();
}

Status VBloomPredicate::execute_column(VExprContext* context, const Block* block,
                                       Selector* selector, size_t count,
                                       ColumnPtr& result_column) const {
    return _do_execute(context, block, nullptr, selector, count, result_column);
}

Status VBloomPredicate::execute_runtime_filter(VExprContext* context, const Block* block,
                                               const uint8_t* __restrict filter, size_t count,
                                               ColumnPtr& result_column,
                                               ColumnPtr* arg_column) const {
    return _do_execute(context, block, filter, nullptr, count, result_column);
}

namespace {

bool bloom_filter_type_matches(PrimitiveType filter_type, const DataTypePtr& data_type) {
    if (data_type == nullptr) {
        return false;
    }
    const auto value_type = remove_nullable(data_type)->get_primitive_type();
    return filter_type == value_type || (is_string_type(filter_type) && is_string_type(value_type));
}

} // namespace

bool VBloomPredicate::can_execute_on_raw_fixed_values(const DataTypePtr& data_type,
                                                      int column_id) const {
    if (_filter == nullptr || !_filter->supports_raw_fixed_values() || _children.size() != 1) {
        return false;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(_children[0]);
    return slot != nullptr && slot->column_id() == column_id &&
           bloom_filter_type_matches(_filter->primitive_type(), slot->data_type()) &&
           bloom_filter_type_matches(_filter->primitive_type(), data_type);
}

Status VBloomPredicate::execute_on_raw_fixed_values(const uint8_t* values, size_t num_values,
                                                    size_t value_width,
                                                    const DataTypePtr& data_type, int column_id,
                                                    uint8_t* matches) const {
    if (!can_execute_on_raw_fixed_values(data_type, column_id)) {
        return Status::NotSupported("Bloom predicate cannot evaluate raw fixed-width values");
    }
    // Hash physical values inside BloomFilterFunc<T>; reconstructing an untyped hash here could
    // disagree with the build-side hash for dates, decimals, and other fixed-width wrappers.
    return _filter->find_batch_raw_fixed(values, num_values, value_width, matches);
}

bool VBloomPredicate::can_execute_on_raw_binary_values(const DataTypePtr& data_type,
                                                       int column_id) const {
    if (_filter == nullptr || !_filter->supports_raw_binary_values() || _children.size() != 1) {
        return false;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(_children[0]);
    return slot != nullptr && slot->column_id() == column_id &&
           bloom_filter_type_matches(_filter->primitive_type(), slot->data_type()) &&
           bloom_filter_type_matches(_filter->primitive_type(), data_type);
}

Status VBloomPredicate::execute_on_raw_binary_values(const StringRef* values, size_t num_values,
                                                     const DataTypePtr& data_type, int column_id,
                                                     uint8_t* matches) const {
    if (!can_execute_on_raw_binary_values(data_type, column_id)) {
        return Status::NotSupported("Bloom predicate cannot evaluate raw binary values");
    }
    return _filter->find_batch_raw_binary(values, num_values, matches);
}

ZoneMapFilterResult VBloomPredicate::evaluate_dictionary_filter(
        const DictionaryEvalContext& ctx) const {
    if (!can_evaluate_dictionary_filter()) {
        return ZoneMapFilterResult::kUnsupported;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(_children[0]);
    DORIS_CHECK(slot != nullptr);
    const auto* dictionary = ctx.slot(slot->column_id());
    if (dictionary == nullptr ||
        !bloom_filter_type_matches(_filter->primitive_type(), dictionary->data_type)) {
        return ZoneMapFilterResult::kUnsupported;
    }
    for (const auto& value : dictionary->values) {
        if (_filter->test_field(value)) {
            return ZoneMapFilterResult::kMayMatch;
        }
    }
    return ZoneMapFilterResult::kNoMatch;
}

bool VBloomPredicate::can_evaluate_dictionary_filter() const {
    if (_filter == nullptr || _children.size() != 1) {
        return false;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(_children[0]);
    return slot != nullptr &&
           bloom_filter_type_matches(_filter->primitive_type(), slot->data_type());
}

const std::string& VBloomPredicate::expr_name() const {
    return EXPR_NAME;
}

void VBloomPredicate::set_filter(std::shared_ptr<BloomFilterFuncBase> filter) {
    _filter = filter;
}

uint64_t VBloomPredicate::get_digest(uint64_t seed) const {
    seed = _children[0]->get_digest(seed);
    if (seed) {
        char* data;
        int len;
        _filter->get_data(&data, &len);
        return HashUtil::hash64(data, len, seed);
    }
    return 0;
}

#include "common/compile_check_end.h"
} // namespace doris
