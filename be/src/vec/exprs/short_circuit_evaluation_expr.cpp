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

#include "short_circuit_evaluation_expr.h"

#include "common/exception.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

Status ShortCircuitIfExpr::prepare(RuntimeState* state, const RowDescriptor& desc,
                                   VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    _prepare_finished = true;
    return Status::OK();
}

Status ShortCircuitIfExpr::open(RuntimeState* state, VExprContext* context,
                                FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

void ShortCircuitIfExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    VExpr::close(context, scope);
}

void execute_true_and_false_selector(const ColumnPtr& cond_column, size_t count,
                                     Selector& true_selector, Selector& false_selector) {
    if (const auto* column_nullable = check_and_get_column<ColumnNullable>(cond_column.get())) {
        const auto& null_map = column_nullable->get_null_map_data();
        const auto& nested_column = column_nullable->get_nested_column_ptr();
        const auto* column_uint8 = assert_cast<const ColumnBool*>(nested_column.get());
        const auto& data = column_uint8->get_data();
        for (size_t i = 0; i < count; ++i) {
            if (null_map[i]) {
                false_selector.push_back(i);
            } else {
                if (data[i]) {
                    true_selector.push_back(i);
                } else {
                    false_selector.push_back(i);
                }
            }
        }
    } else {
        const auto* column_uint8 = assert_cast<const ColumnBool*>(cond_column.get());
        const auto& data = column_uint8->get_data();
        for (size_t i = 0; i < count; ++i) {
            if (data[i]) {
                true_selector.push_back(i);
            } else {
                false_selector.push_back(i);
            }
        }
    }
}

template <PrimitiveType PType>
struct ScalarFillWithSelector {
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    using ArrayType = typename ColumnType::Container;

public:
    static ColumnPtr fill_if(const DataTypePtr& result_type, const ColumnPtr& true_column,
                             const Selector& true_selector, const ColumnPtr& false_column,
                             const Selector& false_selector, size_t count) {
        DCHECK_EQ(false_selector.size() + true_selector.size(), count);
        DCHECK_EQ(true_column->size(), true_selector.size());
        DCHECK_EQ(false_column->size(), false_selector.size());
        DCHECK_EQ(PType, result_type->get_primitive_type());
        auto result_column = result_type->create_column();
        result_column->resize(count);
        dispatch_const(result_column, true_column, true_selector);
        dispatch_const(result_column, false_column, false_selector);
        DCHECK_EQ(result_column->size(), count);
        return result_column;
    }

private:
    static void dispatch_const(MutableColumnPtr& result_column, const ColumnPtr& from_column,
                               const Selector& selector) {
        const auto& [from_data_column, is_const] = unpack_if_const(from_column);
        if (is_const) {
            dispatch_nullable<true>(result_column, from_data_column, selector);
        } else {
            dispatch_nullable<false>(result_column, from_data_column, selector);
        }
    }

    template <bool is_const>
    static void dispatch_nullable(MutableColumnPtr& result_column, const ColumnPtr& from_column,
                                  const Selector& selector) {
        NullMap* result_null_map_data = nullptr;
        const NullMap* from_null_map_data = nullptr;
        ArrayType* result_data = nullptr;
        const ArrayType* from_data = nullptr;

        if (auto* result_nullable_column =
                    check_and_get_column<ColumnNullable>(result_column.get())) {
            result_null_map_data = &result_nullable_column->get_null_map_data();
            auto& nested_result_column = result_nullable_column->get_nested_column();
            result_data = &(assert_cast<ColumnType&>(nested_result_column).get_data());
        } else {
            result_data = &(assert_cast<ColumnType&>(*result_column).get_data());
        }

        if (const auto* from_nullable_column =
                    check_and_get_column<ColumnNullable>(from_column.get())) {
            from_null_map_data = &from_nullable_column->get_null_map_data();
            const auto& nested_from_column = from_nullable_column->get_nested_column();
            from_data = &(assert_cast<const ColumnType&>(nested_from_column).get_data());
        } else {
            from_data = &(assert_cast<const ColumnType&>(*from_column).get_data());
        }

        insert_into_result<is_const>(*result_data, *from_data, result_null_map_data,
                                     from_null_map_data, selector);
    }

    template <bool is_const>
    static void insert_into_result(ArrayType& result_data, const ArrayType& from_data,
                                   NullMap* result_null_map_data, const NullMap* from_null_map_data,
                                   const Selector& selector) {
        insert_with_selector<is_const>(result_data, from_data, selector);
        if (result_null_map_data != nullptr && from_null_map_data != nullptr) {
            insert_with_selector<is_const>(*result_null_map_data, *from_null_map_data, selector);
        } else if (result_null_map_data != nullptr && from_null_map_data == nullptr) {
            // do nothing , because result_null_map_data default value is false
        } else if (result_null_map_data == nullptr && from_null_map_data != nullptr) {
            throw doris::Exception(doris::Status::InternalError(
                    "Internal error: null map data mismatch in ShortCircuitIfExpr"));
        } else {
            // do nothing
        }
    }

    template <bool is_const>
    static void insert_with_selector(auto& result_data, const auto& data,
                                     const Selector& selector) {
        for (size_t i = 0; i < selector.size(); ++i) {
            auto index = selector[i];
            result_data[index] = data[index_check_const<is_const>(i)];
        }
    }
};

struct NonScalarFillWithSelector {
    static ColumnPtr fill_if(const DataTypePtr& result_type, ColumnPtr& true_column,
                             const Selector& true_selector, ColumnPtr& false_column,
                             const Selector& false_selector, size_t count) {
        DCHECK_EQ(false_selector.size() + true_selector.size(), count);
        DCHECK_EQ(true_column->size(), true_selector.size());
        DCHECK_EQ(false_column->size(), false_selector.size());
        auto result_column = result_type->create_column();
        true_column = true_column->convert_to_full_column_if_const();
        false_column = false_column->convert_to_full_column_if_const();
        auto insert_nullable = [&](const ColumnPtr& source_column, size_t source_index) {
            if (is_column_nullable(*result_column) && !is_column_nullable(*source_column)) {
                result_column->insert_from(*make_nullable(source_column), source_index);
            } else {
                result_column->insert_from(*source_column, source_index);
            }
        };

        size_t true_index = 0;
        size_t false_index = 0;
        for (size_t i = 0; i < count; ++i) {
            if (true_index < true_selector.size() && i == true_selector[true_index]) {
                insert_nullable(true_column, true_index++);
            } else {
                insert_nullable(false_column, false_index++);
            }
        }

        DCHECK_EQ(result_column->size(), count);
        return result_column;
    }
};

Status ShortCircuitIfExpr::execute_column(VExprContext* context, const Block* block,
                                          Selector* selector, size_t count,
                                          ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK(selector == nullptr || selector->size() == count);
    ColumnPtr cond_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, cond_column));
    DCHECK_EQ(cond_column->size(), count);
    cond_column = cond_column->convert_to_full_column_if_const();

    Selector true_selector;
    Selector false_selector;
    true_selector.reserve(count);
    false_selector.reserve(count);
    execute_true_and_false_selector(cond_column, count, true_selector, false_selector);

    ColumnPtr true_column;
    RETURN_IF_ERROR(_children[1]->execute_column(context, block, &true_selector,
                                                 true_selector.size(), true_column));

    ColumnPtr false_column;
    RETURN_IF_ERROR(_children[2]->execute_column(context, block, &false_selector,
                                                 false_selector.size(), false_column));

    auto call = [&](const auto& type) -> bool {
        using DataType = std::decay_t<decltype(type)>;
        result_column = ScalarFillWithSelector<DataType::PType>::fill_if(
                _data_type, true_column, true_selector, false_column, false_selector, count);
        return true;
    };

    auto can_use_vec_exec = dispatch_switch_scalar(_data_type->get_primitive_type(), call);
    if (!can_use_vec_exec) {
        result_column = NonScalarFillWithSelector::fill_if(_data_type, true_column, true_selector,
                                                           false_column, false_selector, count);
    }
    return Status::OK();
}

std::string ShortCircuitIfExpr::debug_string() const {
    std::string result = expr_name() + "(";
    for (size_t i = 0; i < _children.size(); ++i) {
        if (i != 0) {
            result += ", ";
        }
        result += _children[i]->debug_string();
    }
    result += ")";
    return result;
}
} // namespace doris::vectorized