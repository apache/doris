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

#include "vec/exprs/vbloom_predicate.h"

#include "common/status.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

VBloomPredicate::VBloomPredicate(const TExprNode& node)
        : VExpr(node), _filter(nullptr), _expr_name("bloom_predicate") {}

Status VBloomPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_children.size() != 1) {
        return Status::InternalError("Invalid argument for VBloomPredicate.");
    }

    _be_exec_version = state->be_exec_version();
    return Status::OK();
}

Status VBloomPredicate::open(RuntimeState* state, VExprContext* context,
                             FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    return Status::OK();
}

void VBloomPredicate::close(RuntimeState* state, VExprContext* context,
                            FunctionContext::FunctionStateScope scope) {
    VExpr::close(state, context, scope);
}

Status VBloomPredicate::execute(VExprContext* context, Block* block, int* result_column_id) {
    doris::vectorized::ColumnNumbers arguments(_children.size());
    for (int i = 0; i < _children.size(); ++i) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        arguments[i] = column_id;
    }
    // call function
    size_t num_columns_without_result = block->columns();
    auto res_data_column = ColumnVector<UInt8>::create(block->rows());

    ColumnPtr argument_column =
            block->get_by_position(arguments[0]).column->convert_to_full_column_if_const();
    size_t sz = argument_column->size();
    res_data_column->resize(sz);
    auto ptr = ((ColumnVector<UInt8>*)res_data_column.get())->get_data().data();
    auto type = WhichDataType(remove_nullable(block->get_by_position(arguments[0]).type));
    if (type.is_string_or_fixed_string()) {
        for (size_t i = 0; i < sz; i++) {
            auto ele = argument_column->get_data_at(i);
            const StringValue v(ele.data, ele.size);
            ptr[i] = _filter->find(reinterpret_cast<const void*>(&v));
        }
    } else if (_be_exec_version > 0 && (type.is_int_or_uint() || type.is_float())) {
        if (argument_column->is_nullable()) {
            auto column_nested = reinterpret_cast<const ColumnNullable*>(argument_column.get())
                                         ->get_nested_column_ptr();
            auto column_nullmap = reinterpret_cast<const ColumnNullable*>(argument_column.get())
                                          ->get_null_map_column_ptr();
            _filter->find_fixed_len(column_nested->get_raw_data().data,
                                    (uint8*)column_nullmap->get_raw_data().data, sz, ptr);
        } else {
            _filter->find_fixed_len(argument_column->get_raw_data().data, nullptr, sz, ptr);
        }
    } else {
        for (size_t i = 0; i < sz; i++) {
            ptr[i] = _filter->find(
                    reinterpret_cast<const void*>(argument_column->get_data_at(i).data));
        }
    }

    if (_data_type->is_nullable()) {
        auto null_map = ColumnVector<UInt8>::create(block->rows(), 0);
        block->insert({ColumnNullable::create(std::move(res_data_column), std::move(null_map)),
                       _data_type, _expr_name});
    } else {
        block->insert({std::move(res_data_column), _data_type, _expr_name});
    }
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

const std::string& VBloomPredicate::expr_name() const {
    return _expr_name;
}
void VBloomPredicate::set_filter(std::shared_ptr<BloomFilterFuncBase>& filter) {
    _filter = filter;
}
} // namespace doris::vectorized