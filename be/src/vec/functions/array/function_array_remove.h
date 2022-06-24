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

#pragma once

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

class FunctionArrayRemove : public IFunction {
public:
    static constexpr auto name = "array_remove";
    static FunctionPtr create() { return std::make_shared<FunctionArrayRemove>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return true; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "First argument for function: " << name << " should be DataTypeArray but it has type "
                << arguments[0]->get_name() << ".";
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {


//        auto dst_null_column = ColumnUInt8::create(input_rows_count);
//        UInt8* dst_null_map = dst_null_column->get_data().data();
//        const UInt8* src_null_map = nullptr;
//        ColumnsWithTypeAndName args;
//        auto col_left = block.get_by_position(arguments[0]);
//        if (col_left.column->is_nullable()) {
//            auto null_col = check_and_get_column<ColumnNullable>(*col_left.column);
//            src_null_map = null_col->get_null_map_column().get_data().data();
//            args = {{null_col->get_nested_column_ptr(), remove_nullable(col_left.type),
//                     col_left.name},
//                    block.get_by_position(arguments[1])};
//        } else {
//            args = {col_left, block.get_by_position(arguments[1])};
//        }
//        auto res_column = _execute_non_nullable(args, input_rows_count, src_null_map);

        // For default implementation of nulls args
        ColumnsWithTypeAndName args = {
                block.get_by_position(arguments[0]), block.get_by_position(arguments[1])
        };

        LOG(INFO) << "left type is " << args[0].type->get_name();
        LOG(INFO) << "right type is " << args[1].type->get_name();
        LOG(INFO) << "right column type is " << args[1].column->get_name();
        if (!is_column_const(*args[1].column)) {
            return Status::RuntimeError(
                    fmt::format("second argument of {} only support constant column now.",
                                get_name()));
        }

        auto res_column = _execute_non_nullable(args, input_rows_count);
        if (!res_column) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({}, {})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name(),
                                block.get_by_position(arguments[1]).type->get_name()));
        }
        LOG(INFO) << "origin size is " << args[0].column->size() << " and result size is " << res_column->size();
        DCHECK_EQ(args[0].column->size(), res_column->size());
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

private:

    template <typename NestedColumnType, typename RightColumnType>
    ColumnPtr _execute_number(const ColumnArray::Offsets& offsets, const IColumn& nested_column,
                              const UInt8* arr_null_map, const IColumn& target,
                              const UInt8* nested_null_map, UInt8* dst_null_map, bool target_is_null) {
        LOG(INFO) << "nested column type is " << nested_column.get_name();
        // check array nested column type and get data
        auto& src_column = reinterpret_cast<const NestedColumnType&>(nested_column);
        const auto& src_data = src_column.get_data();

        // check target column type and get data
        const auto& target_data = reinterpret_cast<const RightColumnType&>(target).get_data();

        // prepare dst array
        auto dst = ColumnArray::create(
                ColumnNullable::create(nested_column.clone_empty(), ColumnUInt8::create()));
        auto& dst_offsets = dst->get_offsets();
        dst_offsets.reserve(offsets.size());

        LOG(INFO) << "src column type is " << src_column.get_name();

        // prepare dst nested nullable column
        auto& dst_column = reinterpret_cast<ColumnNullable&>(dst->get_data());

        size_t cur = 0;
        for (size_t row = 0; row < offsets.size(); ++row) {
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;

            LOG(INFO) << "row: " << row;
            LOG(INFO) << "off: " << off;
            LOG(INFO) << "len: " << len;
            LOG(INFO) << "cur: " << cur;

            if (len == 0) {
                // case: array:[], target:1 ==> []
                dst_offsets.push_back(cur);
                continue;
            }

            size_t count = 0;
            for (size_t pos = 0; pos < len; ++pos) {
                LOG(INFO) << "pos: " << pos;
                LOG(INFO) << "nested_null_map: " << nested_null_map;
                LOG(INFO) << "target_is_null: " << target_is_null;
                if (!nested_null_map && target_is_null) {
                    // case: array:[1,2], target:Null ==> [1,2]
                    LOG(INFO) << "case: array:[1,2], target:Null ==> [1,2]. " << off + pos;
                    dst_column.insert_from_not_nullable(src_column, off + pos);
                    continue;
                }

                if (nested_null_map) {
                    LOG(INFO) << "nested_null_map is true";
                    LOG(INFO) << "nested_null_map[" << off + pos << "]: " << nested_null_map[off + pos];
                    if (nested_null_map[off + pos] && !target_is_null) {
                        // case: array:[Null], target:1 ==> [Null]
                        LOG(INFO) << "case: array:[Null], target:1 ==> [Null]. " << off + pos;
                        dst_column.insert_data(nullptr, 0);
                        continue;
                    } else if (!nested_null_map[off + pos] && target_is_null) {
                        // case: array:[1,2], target:Null ==> [1,2]
                        LOG(INFO) << "case: array:[1,2], target:Null ==> [1,2]. " << off + pos;
                        dst_column.insert_from_not_nullable(src_column, off + pos);
                        continue;
                    } else if (nested_null_map[off + pos] && target_is_null) {
                        // case: array:[Null], target:Null ==> []
                        LOG(INFO) << "case: array:[Null], target:Null ==> []. " << off + pos;
                        ++count;
                        LOG(INFO) << "count+1, count: " << count;
                        continue;
                    }
                }

                // only need to compare with the first element of right column's data
                // because from now right column is constant
                LOG(INFO) << "src_data[" << off + pos << "]";
                if (src_data[off + pos] == target_data[0]) {
                    LOG(INFO) << "count+1, count: " << count;
                    ++count;
                } else {
                    dst_column.insert_from_not_nullable(src_column, off + pos);
                }
            }

            cur += len - count;
            dst_offsets.push_back(cur);
            LOG(INFO) << "one array element end. " << cur;
            LOG(INFO) << "cur: " << cur;
            LOG(INFO) << "len: " << len;
            LOG(INFO) << "count: " << count;
        }

        return dst;
    }

    ColumnPtr _execute_string(const ColumnArray::Offsets& offsets, const IColumn& nested_column,
                              const UInt8* arr_null_map, const IColumn& target,
                              const UInt8* nested_null_map, UInt8* dst_null_map, bool target_is_null) {

        // prepare dst array
        auto dst = ColumnArray::create(
                ColumnNullable::create(nested_column.clone_empty(), ColumnUInt8::create()));
        return dst;
    }

    template <typename NestedColumnType>
    ColumnPtr _execute_number_expanded(const ColumnArray::Offsets& offsets, const IColumn& nested_column,
                                       const UInt8* arr_null_map, const IColumn& target,
                                       const UInt8* nested_null_map, UInt8* dst_null_map, bool target_is_null) {
        LOG(INFO) << "target column type is " << target.get_name();
        if (check_column<ColumnUInt8>(target)) {
            return _execute_number<NestedColumnType, ColumnUInt8>(offsets, nested_column, arr_null_map,
                                                                  target, nested_null_map, dst_null_map,
                                                                  target_is_null);
        } else if (check_column<ColumnInt8>(target)) {
            return _execute_number<NestedColumnType, ColumnInt8>(offsets, nested_column, arr_null_map,
                                                                 target, nested_null_map, dst_null_map,
                                                                 target_is_null);
        } else if (check_column<ColumnInt16>(target)) {
            return _execute_number<NestedColumnType, ColumnInt16>(offsets, nested_column, arr_null_map,
                                                                  target, nested_null_map, dst_null_map,
                                                                  target_is_null);
        } else if (check_column<ColumnInt32>(target)) {
            LOG(INFO) << "enter target int32";
            return _execute_number<NestedColumnType, ColumnInt32>(offsets, nested_column, arr_null_map,
                                                                  target, nested_null_map, dst_null_map,
                                                                  target_is_null);
        } else if (check_column<ColumnInt64>(target)) {
            return _execute_number<NestedColumnType, ColumnInt64>(offsets, nested_column, arr_null_map,
                                                                  target, nested_null_map, dst_null_map,
                                                                  target_is_null);
        } else if (check_column<ColumnInt128>(target)) {
            return _execute_number<NestedColumnType, ColumnInt128>(offsets, nested_column, arr_null_map,
                                                                   target, nested_null_map, dst_null_map,
                                                                   target_is_null);
        } else if (check_column<ColumnFloat32>(target)) {
            return _execute_number<NestedColumnType, ColumnFloat32>(offsets, nested_column, arr_null_map,
                                                                    target, nested_null_map, dst_null_map,
                                                                    target_is_null);
        } else if (check_column<ColumnFloat64>(target)) {
            return _execute_number<NestedColumnType, ColumnFloat64>(offsets, nested_column, arr_null_map,
                                                                    target, nested_null_map, dst_null_map,
                                                                    target_is_null);
        } else if (target.is_date_type()) {
            return _execute_number<NestedColumnType, ColumnDate>(offsets, nested_column, arr_null_map,
                                                                 target, nested_null_map, dst_null_map,
                                                                 target_is_null);
        } else if (target.is_datetime_type()) {
            return _execute_number<NestedColumnType, ColumnDateTime>(offsets, nested_column, arr_null_map,
                                                                     target, nested_null_map, dst_null_map,
                                                                     target_is_null);
        } else if (check_column<ColumnDecimal128>(target)) {
            return _execute_number<NestedColumnType, ColumnDecimal128>(offsets, nested_column, arr_null_map,
                                                                       target, nested_null_map, dst_null_map,
                                                                       target_is_null);
        }
        return nullptr;
    }

    ColumnPtr _execute_non_nullable(const ColumnsWithTypeAndName& arguments, size_t input_rows_count,
                                    const UInt8* src_null_map = nullptr, UInt8* dst_null_map = nullptr) {
        // check array nested column type and get data
        auto left_column = arguments[0].column->convert_to_full_column_if_const();
        const auto& array_column = reinterpret_cast<const ColumnArray&>(*left_column);
        const auto& offsets = array_column.get_offsets();
        DCHECK(offsets.size() == input_rows_count);

        // currently, only constant right column is supported
        const ColumnPtr target_ptr = arguments[1].column;
        DCHECK(is_column_const(*target_ptr));

        const UInt8* nested_null_map = nullptr;
        ColumnPtr nested_column = nullptr;
        if (is_column_nullable(array_column.get_data())) {
            const auto& nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_column.get_data());
            nested_null_map = nested_null_column.get_null_map_column().get_data().data();
            nested_column = nested_null_column.get_nested_column_ptr();
        } else {
            if (target_ptr->only_null()) {
                // case: array:[1,2,3], target:Null ==> [1,2,3]
                // return ColumnArray::Create(array_column);
                LOG(INFO) << "target column is only null while nested column isn't nullable"
                          << ", return immediately.";
                return array_column.clone_resized(input_rows_count);
            }
            nested_column = array_column.get_data_ptr();
        }

        // check and get target column
        const auto& const_col = reinterpret_cast<const ColumnConst&>(*target_ptr);
        const auto& target = const_col.get_data_column();
        const bool target_is_null = const_col.only_null();

        auto left_element_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*arguments[0].type).get_nested_type());
        auto right_type = remove_nullable((arguments[1]).type);

        LOG(INFO) << "left element type is " << left_element_type->get_name();
        LOG(INFO) << "right type is " << right_type->get_name();
        LOG(INFO) << "nested column type1 is " << nested_column->get_name();

        ColumnPtr res = nullptr;
        if (is_string(right_type) && is_string(left_element_type)) {
            res = _execute_string(offsets, *nested_column, src_null_map,
                                  target, nested_null_map, dst_null_map,
                                  target_is_null);
        } else if (is_number(right_type) && is_number(left_element_type)) {
            LOG(INFO) << "enternumber";
            if (check_column<ColumnUInt8>(*nested_column)) {
                res = _execute_number_expanded<ColumnUInt8>(offsets, *nested_column, src_null_map,
                                                            target, nested_null_map, dst_null_map,
                                                            target_is_null);
            } else if (check_column<ColumnInt8>(*nested_column)) {
                res = _execute_number_expanded<ColumnInt8>(offsets, *nested_column, src_null_map,
                                                           target, nested_null_map, dst_null_map,
                                                           target_is_null);
            } else if (check_column<ColumnInt16>(*nested_column)) {
                res = _execute_number_expanded<ColumnInt16>(offsets, *nested_column, src_null_map,
                                                            target, nested_null_map, dst_null_map,
                                                            target_is_null);
            } else if (check_column<ColumnInt32>(*nested_column)) {
                LOG(INFO) << "enterint32";
                res = _execute_number_expanded<ColumnInt32>(offsets, *nested_column, src_null_map,
                                                            target, nested_null_map, dst_null_map,
                                                            target_is_null);
            } else if (check_column<ColumnInt64>(*nested_column)) {
                res = _execute_number_expanded<ColumnInt64>(offsets, *nested_column, src_null_map,
                                                            target, nested_null_map, dst_null_map,
                                                            target_is_null);
            } else if (check_column<ColumnInt128>(*nested_column)) {
                res = _execute_number_expanded<ColumnInt128>(offsets, *nested_column, src_null_map,
                                                             target, nested_null_map, dst_null_map,
                                                             target_is_null);
            } else if (check_column<ColumnFloat32>(*nested_column)) {
                res = _execute_number_expanded<ColumnFloat32>(offsets, *nested_column, src_null_map,
                                                              target, nested_null_map, dst_null_map,
                                                              target_is_null);
            } else if (check_column<ColumnFloat64>(*nested_column)) {
                res = _execute_number_expanded<ColumnFloat64>(offsets, *nested_column, src_null_map,
                                                              target, nested_null_map, dst_null_map,
                                                              target_is_null);
            } else if (check_column<ColumnDecimal128>(*nested_column)) {
                res = _execute_number_expanded<ColumnDecimal128>(offsets, *nested_column, src_null_map,
                                                                 target, nested_null_map, dst_null_map,
                                                                 target_is_null);
            }
        } else if (is_date_or_datetime(right_type) && is_date_or_datetime(left_element_type)) {
            if (nested_column->is_date_type()) {
                res = _execute_number_expanded<ColumnDate>(offsets, *nested_column, src_null_map,
                                                           target, nested_null_map, dst_null_map,
                                                           target_is_null);
            } else if (nested_column->is_datetime_type()) {
                res = _execute_number_expanded<ColumnDateTime>(offsets, *nested_column, src_null_map,
                                                               target, nested_null_map, dst_null_map,
                                                               target_is_null);
            }
        }

        return res;
    }

};

}