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

#include "exprs/vbitmap_predicate.h"

#include <cstddef>

#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;

} // namespace doris

namespace doris {

class VExprContext;

VBitmapPredicate::VBitmapPredicate(const TExprNode& node) : VExpr(node), _filter(nullptr) {}

doris::Status VBitmapPredicate::prepare(doris::RuntimeState* state, const RowDescriptor& desc,
                                        VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_children.size() != 1) {
        return Status::InternalError("Invalid argument for VBitmapPredicate.");
    }

    _prepare_finished = true;
    return Status::OK();
}

doris::Status VBitmapPredicate::open(doris::RuntimeState* state, VExprContext* context,
                                     FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

Status VBitmapPredicate::_do_execute(VExprContext* context, const Block* block,
                                     const uint8_t* __restrict filter, const Selector* selector,
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
    auto* ptr = res_data_column->get_data().data();

    const uint8_t* nullmap = nullptr;
    ColumnPtr values_column = argument_column;
    if (argument_column->is_nullable()) {
        const auto* nullable = assert_cast<const ColumnNullable*>(argument_column.get());
        nullmap = nullable->get_null_map_column().get_data().data();
        values_column = nullable->get_nested_column_ptr();
    }
    const auto* ids = reinterpret_cast<const int64_t*>(values_column->get_raw_data().data);
    for (size_t i = 0; i < sz; ++i) {
        ptr[i] = (filter == nullptr || filter[i]) && (nullmap == nullptr || !nullmap[i]) &&
                 _filter->contains(static_cast<uint64_t>(ids[i]));
    }

    result_column = std::move(res_data_column);
    DCHECK_EQ(result_column->size(), count);
    return Status::OK();
}

Status VBitmapPredicate::execute_column_impl(VExprContext* context, const Block* block,
                                             const Selector* selector, size_t count,
                                             ColumnPtr& result_column) const {
    return _do_execute(context, block, nullptr, selector, count, result_column);
}

Status VBitmapPredicate::execute_runtime_filter(VExprContext* context, const Block* block,
                                                const uint8_t* __restrict filter, size_t count,
                                                ColumnPtr& result_column,
                                                ColumnPtr* arg_column) const {
    return _do_execute(context, block, filter, nullptr, count, result_column);
}

void VBitmapPredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close(context, scope);
}

const std::string& VBitmapPredicate::expr_name() const {
    return EXPR_NAME;
}

void VBitmapPredicate::set_filter(std::shared_ptr<BitmapValue> filter) {
    _filter = filter;
}

} // namespace doris
