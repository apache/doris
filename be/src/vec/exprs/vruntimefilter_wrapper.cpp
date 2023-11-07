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

#include "vec/exprs/vruntimefilter_wrapper.h"

#include <fmt/format.h>
#include <stddef.h>

#include <memory>
#include <utility>

#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;

namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

VRuntimeFilterWrapper::VRuntimeFilterWrapper(const TExprNode& node, const VExprSPtr& impl)
        : VExpr(node), _impl(impl), _always_true(false), _filtered_rows(0), _scan_rows(0) {}

Status VRuntimeFilterWrapper::prepare(RuntimeState* state, const RowDescriptor& desc,
                                      VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(_impl->prepare(state, desc, context));
    _expr_name = fmt::format("VRuntimeFilterWrapper({})", _impl->expr_name());
    return Status::OK();
}

Status VRuntimeFilterWrapper::open(RuntimeState* state, VExprContext* context,
                                   FunctionContext::FunctionStateScope scope) {
    return _impl->open(state, context, scope);
}

void VRuntimeFilterWrapper::close(VExprContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    _impl->close(context, scope);
}

Status VRuntimeFilterWrapper::execute(VExprContext* context, Block* block, int* result_column_id) {
    if (_always_true) {
        auto res_data_column = ColumnVector<UInt8>::create(block->rows(), 1);
        size_t num_columns_without_result = block->columns();
        if (_data_type->is_nullable()) {
            auto null_map = ColumnVector<UInt8>::create(block->rows(), 0);
            block->insert({ColumnNullable::create(std::move(res_data_column), std::move(null_map)),
                           _data_type, expr_name()});
        } else {
            block->insert({std::move(res_data_column), _data_type, expr_name()});
        }
        *result_column_id = num_columns_without_result;
        return Status::OK();
    } else {
        _scan_rows += block->rows();
        RETURN_IF_ERROR(_impl->execute(context, block, result_column_id));
        uint8_t* data = nullptr;
        const ColumnWithTypeAndName& result_column = block->get_by_position(*result_column_id);
        if (auto* nullable = check_and_get_column<ColumnNullable>(*result_column.column)) {
            data = ((ColumnVector<UInt8>*)nullable->get_nested_column_ptr().get())
                           ->get_data()
                           .data();
            _filtered_rows += doris::simd::count_zero_num(reinterpret_cast<const int8_t*>(data),
                                                          nullable->get_null_map_data().data(),
                                                          block->rows());
        } else if (auto* res_col =
                           check_and_get_column<ColumnVector<UInt8>>(*result_column.column)) {
            data = const_cast<uint8_t*>(res_col->get_data().data());
            _filtered_rows += doris::simd::count_zero_num(reinterpret_cast<const int8_t*>(data),
                                                          block->rows());
        } else {
            return Status::InternalError("Invalid type for runtime filters!");
        }

        calculate_filter(_filtered_rows, _scan_rows, _has_calculate_filter, _always_true);
        return Status::OK();
    }
}

const std::string& VRuntimeFilterWrapper::expr_name() const {
    return _expr_name;
}

} // namespace doris::vectorized