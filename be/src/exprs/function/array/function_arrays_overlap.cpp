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

#include "exprs/function/array/function_arrays_overlap.h"

#include "exprs/function/simple_function_factory.h"

namespace doris {

Status FunctionArraysOverlap::open(FunctionContext* context,
                                   FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
        return Status::OK();
    }
    const bool left_const = context->is_col_constant(0);
    const bool right_const = context->is_col_constant(1);
    // Prepare only when exactly one argument is constant. With two constants,
    // VectorizedFnCall caches the entire result; e.g. arrays_overlap([1, 2, ...], [])
    // must not build a set before the empty-array check.
    if (left_const == right_const) {
        return Status::OK();
    }
    const int const_arg = left_const ? 0 : 1;

    const auto& column = context->get_constant_col(const_arg)->column_ptr();
    const auto& array_type =
            assert_cast<const DataTypeArray&>(*remove_nullable(context->get_arg_type(const_arg)));
    const auto element_type = remove_nullable(array_type.get_nested_type());
    auto prepare = [&](const auto& type) {
        constexpr PrimitiveType PType = std::decay_t<decltype(type)>::PType;
        const auto view = ColumnArrayView<PType>::create(column);
        // A NULL array has no elements to prepare; execute propagates its outer NULL.
        if (!view.is_null_at(0)) {
            auto prepared = std::make_shared<PreparedSet<PType>>(const_arg);
            prepared->set.insert_array(view[0]);
            // For arrays_overlap(col, ['abc']), StringRefs point into the constant column.
            // FunctionContext and its clones retain that column for the shared state's lifetime.
            context->set_function_state(scope, std::move(prepared));
        }
        return true;
    };
    if (!dispatch_switch_all(element_type->get_primitive_type(), prepare)) {
        return Status::InvalidArgument("not support type {} in function {}",
                                       element_type->get_name(), get_name());
    }
    return Status::OK();
}

void register_function_arrays_overlap(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArraysOverlap>();
}

} // namespace doris
