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

#include <type_traits>

#include "core/assert_cast.h"
#include "core/call_on_type_index.h"
#include "core/column/column_array.h"
#include "core/column/column_array_view.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

template <PrimitiveType PType>
struct ArrayExceptAllCountMap {
    using ElementType = typename ColumnElementView<PType>::ElementType;
    using KeyType = typename NativeType<ElementType>::Type;
    using Type = doris::flat_hash_map<KeyType, size_t>;
};

template <>
struct ArrayExceptAllCountMap<TYPE_STRING> {
    using Type = doris::flat_hash_map<StringRef, size_t, StringRefHash>;
};

class FunctionArrayExceptAll : public IFunction {
public:
    static constexpr auto name = "array_except_all";
    static FunctionPtr create() { return std::make_shared<FunctionArrayExceptAll>(); }

    String get_name() const override { return name; }
    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& left_column = block.get_by_position(arguments[0]).column;
        const auto& right_column = block.get_by_position(arguments[1]).column;

        const auto& array_type =
                assert_cast<const DataTypeArray&>(*block.get_by_position(arguments[0]).type);

        ColumnPtr result_column;
        auto execute = [&](const auto& type) -> bool {
            using DispatchType = std::decay_t<decltype(type)>;
            constexpr PrimitiveType PType = DispatchType::PType;
            result_column = execute_internal(ColumnArrayView<PType>::create(left_column),
                                             ColumnArrayView<PType>::create(right_column),
                                             array_type.get_nested_type()->create_column());
            return true;
        };
        if (!dispatch_switch_all(array_type.get_nested_type()->get_primitive_type(), execute)) {
            return Status::InvalidArgument("function {} does not support element type {}",
                                           get_name(), array_type.get_nested_type()->get_name());
        }

        DCHECK_EQ(result_column->size(), input_rows_count);
        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

private:
    template <PrimitiveType PType>
    static ColumnPtr execute_internal(const ColumnArrayView<PType>& left_view,
                                      const ColumnArrayView<PType>& right_view,
                                      MutableColumnPtr result_data) {
        using CountMap = typename ArrayExceptAllCountMap<PType>::Type;
        using ResultColumn = typename PrimitiveTypeTraits<PType>::ColumnType;

        auto& result_nullable = assert_cast<ColumnNullable&>(*result_data);
        auto& result_values = assert_cast<ResultColumn&>(result_nullable.get_nested_column());
        auto& result_null_map = result_nullable.get_null_map_data();
        auto result_offsets_column = ColumnArray::ColumnOffsets::create();
        auto& result_offsets = result_offsets_column->get_data();
        result_offsets.reserve(left_view.size());

        CountMap counts;
        size_t null_count = 0;
        size_t result_offset = 0;
        for (size_t row = 0; row < left_view.size(); ++row) {
            const auto right_array = right_view[row];
            for (size_t pos = 0; pos < right_array.size(); ++pos) {
                if (right_array.is_null_at(pos)) {
                    ++null_count;
                } else {
                    ++counts[right_array.value_at(pos)];
                }
            }

            const auto left_array = left_view[row];
            for (size_t pos = 0; pos < left_array.size(); ++pos) {
                if (left_array.is_null_at(pos)) {
                    if (null_count > 0) {
                        --null_count;
                    } else {
                        result_values.insert_default();
                        result_null_map.push_back(1);
                        ++result_offset;
                    }
                } else {
                    const auto value = left_array.value_at(pos);
                    auto count = counts.find(value);
                    if (count != counts.end() && count->second > 0) {
                        --count->second;
                    } else {
                        if constexpr (is_string_type(PType)) {
                            result_values.insert_data(value.data, value.size);
                        } else {
                            result_values.get_data().push_back(value);
                        }
                        result_null_map.push_back(0);
                        ++result_offset;
                    }
                }
            }
            result_offsets.push_back(result_offset);
            counts.clear();
            null_count = 0;
        }

        return ColumnArray::create(std::move(result_data), std::move(result_offsets_column));
    }
};

void register_function_array_except_all(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayExceptAll>();
}

} // namespace doris
