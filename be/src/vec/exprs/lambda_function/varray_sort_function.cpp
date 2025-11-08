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

#include <glog/logging.h>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_varbinary.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/exprs/vexpr.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class VExprContext;

using ConstColumnVariant =
        std::variant<const ColumnUInt8*, const ColumnInt8*, const ColumnInt16*, const ColumnInt32*,
                     const ColumnInt64*, const ColumnInt128*, const ColumnFloat32*,
                     const ColumnFloat64*, const ColumnString*, const ColumnVarbinary*,
                     const ColumnArray*, const ColumnIPv4*, const ColumnIPv6*,
                     const ColumnDecimal32*, const ColumnDecimal64*, const ColumnDecimal128V2*,
                     const ColumnDecimal128V3*, const ColumnDecimal256*, const ColumnDate*,
                     const ColumnDateTime*, const ColumnDateV2*, const ColumnDateTimeV2*,
                     const ColumnTime*, const ColumnTimeV2*>;

template <typename T>
struct is_column_vector : std::false_type {};

template <PrimitiveType T>
struct is_column_vector<ColumnVector<T>> : std::true_type {};

template <typename T>
inline constexpr bool is_column_vector_v = is_column_vector<T>::value;

class ArraySortFunction : public LambdaFunction {
    ENABLE_FACTORY_CREATOR(ArraySortFunction);

public:
    ~ArraySortFunction() override = default;
    static constexpr auto name = "array_sort";

    static LambdaFunctionPtr create() { return std::make_shared<ArraySortFunction>(); }

    std::string get_name() const override { return name; }

    Status execute(VExprContext* context, doris::vectorized::Block* block, int* result_column_id,
                   const DataTypePtr& result_type, const VExprSPtrs& children) const override {
        ///* array_sort(lambda, arg) *///

        DCHECK_EQ(children.size(), 3);

        // 1. get data
        int column_id = -1;
        RETURN_IF_ERROR(children[1]->execute(context, block, &column_id));

        const auto& column_with_type = block->get_by_position(column_id);
        auto column = column_with_type.column->convert_to_full_column_if_const();

        auto input_rows = column->size();
        auto outside_null_map = ColumnUInt8::create(input_rows, 0);

        auto arg_type = column_with_type.type;

        auto arg_column = column;
        if (arg_column->is_nullable()) {
            arg_column = assert_cast<const ColumnNullable*>(column.get())->get_nested_column_ptr();
            const auto& column_array_nullmap =
                    assert_cast<const ColumnNullable*>(column.get())->get_null_map_column();
            arg_type = assert_cast<const DataTypeNullable*>(column_with_type.type.get())
                               ->get_nested_type();
            VectorizedUtils::update_null_map(outside_null_map->get_data(),
                                             column_array_nullmap.get_data());
        }

        const auto& col_array = assert_cast<const ColumnArray&>(*arg_column);
        const auto& off_data =
                assert_cast<const ColumnArray::ColumnOffsets&>(col_array.get_offsets_column())
                        .get_data();

        const auto& nested_nullable_column =
                assert_cast<const ColumnNullable&>(*col_array.get_data_ptr());

        auto pType = assert_cast<const DataTypeArray*>(arg_type.get())
                             ->get_nested_type()
                             ->get_primitive_type();

        ConstColumnVariant src_data;
        RETURN_IF_ERROR(
                get_data_from_type(pType, nested_nullable_column.get_nested_column(), src_data));

        const auto& src_nullmap = nested_nullable_column.get_null_map_column();

        const auto& col_type = assert_cast<const DataTypeArray&>(*arg_type);

        // 2. prepare for lambda execution
        auto element_size = nested_nullable_column.size();
        IColumn::Permutation permutation(element_size);
        for (size_t i = 0; i < element_size; ++i) {
            permutation[i] = i;
        }

        Block lambda_block;
        for (int i = 0; i <= 2; i++) {
            lambda_block.insert(vectorized::ColumnWithTypeAndName(
                    nested_nullable_column.clone_empty(), col_type.get_nested_type(), "temp"));
        }

        MutableColumnPtr temp_data[2];
        NullMap* temp_nullmap_data[2] = {nullptr, nullptr};
        for (int i = 0; i < 2; i++) {
            auto* temp_column = assert_cast<ColumnNullable*>(
                    lambda_block.get_by_position(i).column->assume_mutable().get());
            temp_data[i] = temp_column->get_nested_column_ptr();
            auto& null_map_col = assert_cast<ColumnUInt8&>(temp_column->get_null_map_column());
            temp_nullmap_data[i] = &null_map_col.get_data();
            temp_data[i]->resize(1);
            temp_nullmap_data[i]->resize(1);
        };

        int lambda_res_id = 2;

        // 3. sort array by executing lambda function
        std::visit(
                [&](auto* data) {
                    using ColumnType = std::decay_t<decltype(*data)>;
                    ColumnType* data_vec[2] = {assert_cast<ColumnType*>(temp_data[0].get()),
                                               assert_cast<ColumnType*>(temp_data[1].get())};

                    auto get_data = [&](size_t i, size_t cid) {
                        if (src_nullmap.get_data()[i]) {
                            (*temp_nullmap_data[cid])[0] = 1;
                        } else {
                            (*temp_nullmap_data[cid])[0] = 0;
                            if constexpr (is_column_vector_v<ColumnType>) {
                                data_vec[cid]->get_data()[0] = data->get_data()[i];
                            } else {
                                data_vec[cid]->clear();
                                data_vec[cid]->insert_from(*data, i);
                            }
                        }
                    };

                    for (int row = 0; row < input_rows; ++row) {
                        auto start = off_data[row - 1];
                        auto end = off_data[row];
                        std::sort(&permutation[start], &permutation[end], [&](size_t i, size_t j) {
                            get_data(i, 0);
                            get_data(j, 1);
                            auto status =
                                    children[0]->execute(context, &lambda_block, &lambda_res_id);
                            if (!status.ok()) [[unlikely]] {
                                throw Status::InternalError(
                                        "when execute array_sort lambda function: {}",
                                        status.to_string());
                            }

                            // raw_res_col maybe columnVector or ColumnConst
                            ColumnPtr raw_res_col =
                                    lambda_block.get_by_position(lambda_res_id).column;
                            ColumnPtr full_res_col = raw_res_col->convert_to_full_column_if_const();

                            // only -1, 0, 1
                            long cmp = assert_cast<const ColumnInt8*>(full_res_col.get())
                                               ->get_data()[0];

                            return cmp < 0;
                        });
                    }
                },
                src_data);

        // 4. insert the result column to block
        ColumnWithTypeAndName result_arr;
        if (result_type->is_nullable()) {
            result_arr = {ColumnNullable::create(ColumnArray::create(nested_nullable_column.permute(
                                                                             permutation, 0),
                                                                     col_array.get_offsets_ptr()),
                                                 std::move(outside_null_map)),
                          result_type, "array_sort_result"};

        } else {
            DCHECK(!column->is_nullable());
            result_arr = {ColumnArray::create(nested_nullable_column.permute(permutation, 0),
                                              col_array.get_offsets_ptr()),
                          result_type, "array_sort_result"};
        }
        block->insert(std::move(result_arr));
        *result_column_id = block->columns() - 1;

        return Status::OK();
    }

#define DISPATCH_PRIMITIVE_TYPE(TYPE, COLUMN_CLASS)                 \
    case TYPE:                                                      \
        column_variant = &assert_cast<const COLUMN_CLASS&>(column); \
        break;

    Status get_data_from_type(PrimitiveType pType, const IColumn& column,
                              ConstColumnVariant& column_variant) const {
        switch (pType) {
            DISPATCH_PRIMITIVE_TYPE(TYPE_BOOLEAN, ColumnUInt8)
            DISPATCH_PRIMITIVE_TYPE(TYPE_TINYINT, ColumnInt8)
            DISPATCH_PRIMITIVE_TYPE(TYPE_SMALLINT, ColumnInt16)
            DISPATCH_PRIMITIVE_TYPE(TYPE_INT, ColumnInt32)
            DISPATCH_PRIMITIVE_TYPE(TYPE_BIGINT, ColumnInt64)
            DISPATCH_PRIMITIVE_TYPE(TYPE_LARGEINT, ColumnInt128)
            DISPATCH_PRIMITIVE_TYPE(TYPE_FLOAT, ColumnFloat32)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DOUBLE, ColumnFloat64)
            DISPATCH_PRIMITIVE_TYPE(TYPE_CHAR, ColumnString)
            DISPATCH_PRIMITIVE_TYPE(TYPE_STRING, ColumnString)
            DISPATCH_PRIMITIVE_TYPE(TYPE_VARCHAR, ColumnString)
            DISPATCH_PRIMITIVE_TYPE(TYPE_VARBINARY, ColumnVarbinary)
            DISPATCH_PRIMITIVE_TYPE(TYPE_ARRAY, ColumnArray)
            DISPATCH_PRIMITIVE_TYPE(TYPE_IPV4, ColumnIPv4)
            DISPATCH_PRIMITIVE_TYPE(TYPE_IPV6, ColumnIPv6)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DECIMAL32, ColumnDecimal32)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DECIMAL64, ColumnDecimal64)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DECIMAL128I, ColumnDecimal128V3)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DECIMALV2, ColumnDecimal128V2)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DECIMAL256, ColumnDecimal256)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DATE, ColumnDate)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DATETIME, ColumnDateTime)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DATEV2, ColumnDateV2)
            DISPATCH_PRIMITIVE_TYPE(TYPE_DATETIMEV2, ColumnDateTimeV2)
            DISPATCH_PRIMITIVE_TYPE(TYPE_TIME, ColumnTime)
            DISPATCH_PRIMITIVE_TYPE(TYPE_TIMEV2, ColumnTimeV2)
        default:
            return Status::InternalError("Unsupported type in array_sort");
        }
        return Status::OK();
    }

#undef DISPATCH_PRIMITIVE_TYPE
};

void register_function_array_sort(doris::vectorized::LambdaFunctionFactory& factory) {
    factory.register_function<ArraySortFunction>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
