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

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
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
struct ScalarInsertResultWithSelector {
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;

public:
    static ColumnPtr insert(const DataTypePtr& result_type, const ColumnPtr& true_column,
                            const Selector& true_selector, const ColumnPtr& false_column,
                            const Selector& false_selector, size_t count) {
        DCHECK_EQ(false_selector.size() + true_selector.size(), count);
        DCHECK_EQ(true_column->size(), true_selector.size());
        DCHECK_EQ(false_column->size(), false_selector.size());
        DCHECK_EQ(PType, result_type->get_primitive_type());
        auto result_column = result_type->create_column();
        result_column->resize(count);
        dispatch_const(result_column, true_column, true_selector, false_column, false_selector);

        DCHECK_EQ(result_column->size(), count);
        return result_column;
    }

private:
    static void dispatch_const(MutableColumnPtr& result_column, const ColumnPtr& true_column,
                               const Selector& true_selector, const ColumnPtr& false_column,
                               const Selector& false_selector) {
        const auto& [true_data_column, true_is_const] = unpack_if_const(true_column);
        const auto& [false_data_column, false_is_const] = unpack_if_const(false_column);
        if (true_is_const && false_is_const) {
            return dispatch_nullable<true, true>(result_column, true_data_column, true_selector,
                                                 false_data_column, false_selector);
        } else if (true_is_const) {
            return dispatch_nullable<true, false>(result_column, true_data_column, true_selector,
                                                  false_data_column, false_selector);
        } else if (false_is_const) {
            return dispatch_nullable<false, true>(result_column, true_data_column, true_selector,
                                                  false_data_column, false_selector);
        } else {
            return dispatch_nullable<false, false>(result_column, true_data_column, true_selector,
                                                   false_data_column, false_selector);
        }
    }

    const auto& get_column_data(const ColumnPtr& column_ptr) {
        return assert_cast<const ColumnType&>(*column_ptr).get_data();
    }

    template <bool is_true_const, bool is_false_const>
    static void dispatch_nullable(MutableColumnPtr& result_column, const ColumnPtr& true_column,
                                  const Selector& true_selector, const ColumnPtr& false_column,
                                  const Selector& false_selector) {
        if (const auto* true_nullable_column = check_and_get_column<ColumnNullable>(*true_column)) {
            if (const auto* false_nullable_column =
                        check_and_get_column<ColumnNullable>(*false_column)) {
                DCHECK(is_column_nullable(*result_column));
                auto& result_nullable_column = assert_cast<ColumnNullable&>(*result_column);
                auto& result_null_map_data = result_nullable_column.get_null_map_data();
                auto& result_data =
                        assert_cast<ColumnType&>(result_nullable_column.get_nested_column())
                                .get_data();

                const auto& true_null_map_data = true_nullable_column->get_null_map_data();
                const auto& true_data =
                        assert_cast<const ColumnType&>(true_nullable_column->get_nested_column())
                                .get_data();

                const auto& false_null_map_data = false_nullable_column->get_null_map_data();
                const auto& false_data =
                        assert_cast<const ColumnType&>(false_nullable_column->get_nested_column())
                                .get_data();

                insert_with_selector<is_true_const>(result_data, true_data, true_selector);
                insert_with_selector<is_false_const>(result_data, false_data, false_selector);
                insert_with_selector<is_true_const>(result_null_map_data, true_null_map_data,
                                                    true_selector);
                insert_with_selector<is_false_const>(result_null_map_data, false_null_map_data,
                                                     false_selector);

            } else {
                DCHECK(is_column_nullable(*result_column));
                auto& result_nullable_column = assert_cast<ColumnNullable&>(*result_column);
                auto& result_null_map_data = result_nullable_column.get_null_map_data();
                auto& result_data =
                        assert_cast<ColumnType&>(result_nullable_column.get_nested_column())
                                .get_data();

                const auto& true_null_map_data = true_nullable_column->get_null_map_data();
                const auto& true_data =
                        assert_cast<const ColumnType&>(true_nullable_column->get_nested_column())
                                .get_data();
                const auto& false_data = assert_cast<const ColumnType&>(*false_column).get_data();

                insert_with_selector<is_true_const>(result_data, true_data, true_selector);
                insert_with_selector<is_false_const>(result_data, false_data, false_selector);
                insert_all<is_true_const>(result_null_map_data, true_null_map_data);
            }
        } else {
            if (const auto* false_nullable_column =
                        check_and_get_column<ColumnNullable>(*false_column)) {
                DCHECK(is_column_nullable(*result_column));
                auto& result_nullable_column = assert_cast<ColumnNullable&>(*result_column);
                auto& result_null_map_data = result_nullable_column.get_null_map_data();
                auto& result_data =
                        assert_cast<ColumnType&>(result_nullable_column.get_nested_column())
                                .get_data();
                const auto& true_data = assert_cast<const ColumnType&>(*true_column).get_data();
                const auto& false_null_map_data = false_nullable_column->get_null_map_data();
                const auto& false_data =
                        assert_cast<const ColumnType&>(false_nullable_column->get_nested_column())
                                .get_data();
                insert_with_selector<is_true_const>(result_data, true_data, true_selector);
                insert_with_selector<is_false_const>(result_data, false_data, false_selector);
                insert_all<is_false_const>(result_null_map_data, false_null_map_data);
            } else {
                DCHECK(!is_column_nullable(*result_column));
                auto& result_data = assert_cast<ColumnType&>(*result_column).get_data();
                const auto& true_data = assert_cast<const ColumnType&>(*true_column).get_data();
                const auto& false_data = assert_cast<const ColumnType&>(*false_column).get_data();
                insert_with_selector<is_true_const>(result_data, true_data, true_selector);
                insert_with_selector<is_false_const>(result_data, false_data, false_selector);
            }
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

    template <bool is_const>
    static void insert_all(auto& result_data, const auto& data) {
        for (size_t i = 0; i < data.size(); ++i) {
            result_data[i] = data[index_check_const<is_const>(i)];
        }
    }
};

struct InsertResultWithSelector {
    static ColumnPtr insert(const DataTypePtr& result_type, ColumnPtr& true_column,
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
        result_column = ScalarInsertResultWithSelector<DataType::PType>::insert(
                _data_type, true_column, true_selector, false_column, false_selector, count);
        return true;
    };

    auto can_use_vec_exec = dispatch_switch_scalar(_data_type->get_primitive_type(), call);
    if (!can_use_vec_exec) {
        result_column = InsertResultWithSelector::insert(_data_type, true_column, true_selector,
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