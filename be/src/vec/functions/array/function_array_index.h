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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayIndex.h
// and modified by Doris
#pragma once

#include <stddef.h>

#include <memory>
#include <utility>

#include "common/status.h"
#include "olap/column_predicate.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

struct ArrayContainsAction {
    using ResultType = UInt8;
    static constexpr auto name = "array_contains";
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t) noexcept { current = 1; }
};

struct ArrayPositionAction {
    using ResultType = Int64;
    static constexpr auto name = "array_position";
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t j) noexcept { current = j + 1; }
};

struct ArrayCountEqual {
    using ResultType = Int64;
    static constexpr auto name = "countequal";
    static constexpr const bool resume_execution = true;
    static constexpr void apply(ResultType& current, size_t j) noexcept { ++current; }
};

struct ParamValue {
    PrimitiveType type;
    Field value;
};

template <typename ConcreteAction>
class FunctionArrayIndex : public IFunction {
public:
    using ResultType = typename ConcreteAction::ResultType;

    static constexpr auto name = ConcreteAction::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayIndex>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }

        DCHECK(context->get_num_args() >= 1);
        DCHECK(context->get_arg_type(0)->is_array_type());
        // now we only support same
        std::shared_ptr<ParamValue> state = std::make_shared<ParamValue>();
        Field field;
        if (context->get_constant_col(1)) {
            context->get_constant_col(1)->column_ptr->get(0, field);
            state->value = field;
            state->type = context->get_arg_type(1)->type;
            context->set_function_state(scope, state);
        }
        return Status::OK();
    }

    Status evaluate_inverted_index(
            const ColumnsWithTypeAndName& arguments,
            const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<segment_v2::InvertedIndexIterator*> iterators, uint32_t num_rows,
            segment_v2::InvertedIndexResultBitmap& bitmap_result) const override {
        DCHECK(arguments.size() == 1);
        DCHECK(data_type_with_names.size() == 1);
        DCHECK(iterators.size() == 1);
        auto* iter = iterators[0];
        auto data_type_with_name = data_type_with_names[0];
        if (iter == nullptr) {
            return Status::OK();
        }
        if (iter->get_inverted_index_reader_type() ==
            segment_v2::InvertedIndexReaderType::FULLTEXT) {
            // parser is not none we can not make sure the result is correct in expr combination
            // for example, filter: !array_index(array, 'tall:120cm, weight: 35kg')
            // here we have rows [tall:120cm, weight: 35kg, hobbies: reading book] which be tokenized
            // but query is also tokenized, and FULLTEXT reader will catch this row as matched,
            // so array_index(array, 'tall:120cm, weight: 35kg') return this rowid,
            // but we expect it to be filtered, because we want row is equal to 'tall:120cm, weight: 35kg'
            return Status::OK();
        }
        Field param_value;
        arguments[0].column->get(0, param_value);
        auto param_type = arguments[0].type->get_type_as_type_descriptor().type;
        // The current implementation for the inverted index of arrays cannot handle cases where the array contains null values,
        // meaning an item in the array is null.
        if (param_value.is_null()) {
            return Status::OK();
        }

        std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        if (iter->has_null()) {
            segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
            RETURN_IF_ERROR(iter->read_null_bitmap(&null_bitmap_cache_handle));
            null_bitmap = null_bitmap_cache_handle.get_bitmap();
        }
        std::unique_ptr<InvertedIndexQueryParamFactory> query_param = nullptr;
        RETURN_IF_ERROR(InvertedIndexQueryParamFactory::create_query_value(param_type, &param_value,
                                                                           query_param));
        if (is_string_type(param_type)) {
            Status st = iter->read_from_inverted_index(
                    data_type_with_name.first, query_param->get_value(),
                    segment_v2::InvertedIndexQueryType::EQUAL_QUERY, num_rows, roaring);
            if (st.code() == ErrorCode::INVERTED_INDEX_NO_TERMS) {
                // if analyzed param with no term, we do not filter any rows
                // return all rows with OK status
                roaring->addRange(0, num_rows);
            } else if (st != Status::OK()) {
                return st;
            }
        } else {
            RETURN_IF_ERROR(iter->read_from_inverted_index(
                    data_type_with_name.first, query_param->get_value(),
                    segment_v2::InvertedIndexQueryType::EQUAL_QUERY, num_rows, roaring));
        }
        // here debug for check array_contains function really filter rows by inverted index correctly
        DBUG_EXECUTE_IF("array_func.array_contains", {
            auto result_bitmap = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                    "array_func.array_contains", "result_bitmap", 0);
            if (result_bitmap < 0) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "result_bitmap count cannot be negative");
            }
            if (roaring->cardinality() != result_bitmap) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "array_contains really filtered {} by inverted index not equal to expected "
                        "{}",
                        roaring->cardinality(), result_bitmap);
            }
        })
        if (iter->has_null()) {
            segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
            RETURN_IF_ERROR(iter->read_null_bitmap(&null_bitmap_cache_handle));
            null_bitmap = null_bitmap_cache_handle.get_bitmap();
        }
        segment_v2::InvertedIndexResultBitmap result(roaring, null_bitmap);
        bitmap_result = result;
        bitmap_result.mask_out_null();

        return Status::OK();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments[0]->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeNumber<ResultType>>());
        } else {
            return std::make_shared<DataTypeNumber<ResultType>>();
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return _execute_dispatch(block, arguments, result, input_rows_count);
    }

private:
    ColumnPtr _execute_string(const ColumnArray::Offsets64& offsets, const UInt8* nested_null_map,
                              const IColumn& nested_column, const IColumn& right_column,
                              const UInt8* right_nested_null_map,
                              const UInt8* outer_null_map) const {
        // check array nested column type and get data
        const auto& str_offs = reinterpret_cast<const ColumnString&>(nested_column).get_offsets();
        const auto& str_chars = reinterpret_cast<const ColumnString&>(nested_column).get_chars();

        // check right column type and get data
        const auto& right_offs = reinterpret_cast<const ColumnString&>(right_column).get_offsets();
        const auto& right_chars = reinterpret_cast<const ColumnString&>(right_column).get_chars();

        // prepare return data
        auto dst = ColumnVector<ResultType>::create(offsets.size());
        auto& dst_data = dst->get_data();
        auto dst_null_column = ColumnUInt8::create(offsets.size());
        auto& dst_null_data = dst_null_column->get_data();

        // process
        for (size_t row = 0; row < offsets.size(); ++row) {
            if (outer_null_map && outer_null_map[row]) {
                dst_null_data[row] = true;
                continue;
            }
            dst_null_data[row] = false;
            ResultType res = 0;
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;

            size_t right_off = right_offs[row - 1];
            size_t right_len = right_offs[row] - right_off;
            for (size_t pos = 0; pos < len; ++pos) {
                // match null value
                if (right_nested_null_map && right_nested_null_map[row] && nested_null_map &&
                    nested_null_map[pos + off]) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
                // some is null while another is not
                if (right_nested_null_map && nested_null_map &&
                    right_nested_null_map[row] != nested_null_map[pos + off]) {
                    continue;
                }
                if (nested_null_map && nested_null_map[pos + off]) {
                    continue;
                }
                size_t str_pos = str_offs[pos + off - 1];
                size_t str_len = str_offs[pos + off] - str_pos;
                const char* left_raw_v = reinterpret_cast<const char*>(&str_chars[str_pos]);
                const char* right_raw_v = reinterpret_cast<const char*>(&right_chars[right_off]);
                // StringRef operator == using vec impl
                if (StringRef(left_raw_v, str_len) == StringRef(right_raw_v, right_len)) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
            }
            dst_data[row] = res;
        }

        if (outer_null_map == nullptr) {
            return dst;
        }
        return ColumnNullable::create(std::move(dst), std::move(dst_null_column));
    }

    template <typename NestedColumnType, typename RightColumnType>
    ColumnPtr _execute_number(const ColumnArray::Offsets64& offsets, const UInt8* nested_null_map,
                              const IColumn& nested_column, const IColumn& right_column,
                              const UInt8* right_nested_null_map,
                              const UInt8* outer_null_map) const {
        // check array nested column type and get data
        const auto& nested_data =
                reinterpret_cast<const NestedColumnType&>(nested_column).get_data();

        // check right column type and get data
        const auto& right_data = reinterpret_cast<const RightColumnType&>(right_column).get_data();

        // prepare return data
        auto dst = ColumnVector<ResultType>::create(offsets.size());
        auto& dst_data = dst->get_data();
        auto dst_null_column = ColumnUInt8::create(offsets.size());
        auto& dst_null_data = dst_null_column->get_data();

        // process
        for (size_t row = 0; row < offsets.size(); ++row) {
            if (outer_null_map && outer_null_map[row]) {
                dst_null_data[row] = true;
                continue;
            }
            dst_null_data[row] = false;
            ResultType res = 0;
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;
            for (size_t pos = 0; pos < len; ++pos) {
                // match null value
                if (right_nested_null_map && right_nested_null_map[row] && nested_null_map &&
                    nested_null_map[pos + off]) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
                // some is null while another is not
                if (right_nested_null_map && nested_null_map &&
                    right_nested_null_map[row] != nested_null_map[pos + off]) {
                    continue;
                }
                if (nested_null_map && nested_null_map[pos + off]) {
                    continue;
                }
                if (nested_data[pos + off] == right_data[row]) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
            }
            dst_data[row] = res;
        }

        if (outer_null_map == nullptr) {
            return dst;
        }
        return ColumnNullable::create(std::move(dst), std::move(dst_null_column));
    }

    template <typename NestedColumnType>
    ColumnPtr _execute_number_expanded(const ColumnArray::Offsets64& offsets,
                                       const UInt8* nested_null_map, const IColumn& nested_column,
                                       const IColumn& right_column,
                                       const UInt8* right_nested_null_map,
                                       const UInt8* outer_null_map) const {
        if (check_column<NestedColumnType>(right_column)) {
            return _execute_number<NestedColumnType, NestedColumnType>(
                    offsets, nested_null_map, nested_column, right_column, right_nested_null_map,
                    outer_null_map);
        }
        return nullptr;
    }

    Status _execute_dispatch(Block& block, const ColumnNumbers& arguments, size_t result,
                             size_t input_rows_count) const {
        // extract array offsets and nested data
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (!is_array(remove_nullable(block.get_by_position(arguments[0]).type))) {
            return Status::InvalidArgument(get_name() + " first argument must be array, but got " +
                                           block.get_by_position(arguments[0]).type->get_name());
        }
        const ColumnArray* array_column = nullptr;
        const UInt8* array_null_map = nullptr;
        if (left_column->is_nullable()) {
            auto nullable_array = reinterpret_cast<const ColumnNullable*>(left_column.get());
            array_column =
                    reinterpret_cast<const ColumnArray*>(&nullable_array->get_nested_column());
            array_null_map = nullable_array->get_null_map_column().get_data().data();
        } else {
            array_column = reinterpret_cast<const ColumnArray*>(left_column.get());
        }
        const auto& offsets = array_column->get_offsets();
        const UInt8* nested_null_map = nullptr;
        ColumnPtr nested_column = nullptr;
        if (array_column->get_data().is_nullable()) {
            const auto& nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_column->get_data());
            nested_null_map = nested_null_column.get_null_map_column().get_data().data();
            nested_column = nested_null_column.get_nested_column_ptr();
        } else {
            nested_column = array_column->get_data_ptr();
        }

        // get right column
        ColumnPtr right_full_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        ColumnPtr right_column = right_full_column;
        const UInt8* right_nested_null_map = nullptr;
        if (right_column->is_nullable()) {
            const auto& nested_null_column = assert_cast<const ColumnNullable&>(*right_full_column);
            right_column = nested_null_column.get_nested_column_ptr();
            right_nested_null_map = nested_null_column.get_null_map_column().get_data().data();
        }
        // execute
        auto array_type = remove_nullable(block.get_by_position(arguments[0]).type);
        auto left_element_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*array_type).get_nested_type());
        auto right_type = remove_nullable(block.get_by_position(arguments[1]).type);

        ColumnPtr return_column = nullptr;
        WhichDataType left_which_type(left_element_type);

        if (is_string(right_type) && is_string(left_element_type)) {
            return_column = _execute_string(offsets, nested_null_map, *nested_column, *right_column,
                                            right_nested_null_map, array_null_map);
        } else if (is_number(right_type) && is_number(left_element_type)) {
            if (left_which_type.is_uint8()) {
                return_column = _execute_number_expanded<ColumnUInt8>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int8()) {
                return_column = _execute_number_expanded<ColumnInt8>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int16()) {
                return_column = _execute_number_expanded<ColumnInt16>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int32()) {
                return_column = _execute_number_expanded<ColumnInt32>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int64()) {
                return_column = _execute_number_expanded<ColumnInt64>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int128()) {
                return_column = _execute_number_expanded<ColumnInt128>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_float32()) {
                return_column = _execute_number_expanded<ColumnFloat32>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_float64()) {
                return_column = _execute_number_expanded<ColumnFloat64>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal32()) {
                return_column = _execute_number_expanded<ColumnDecimal32>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal64()) {
                return_column = _execute_number_expanded<ColumnDecimal64>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal128v3()) {
                return_column = _execute_number_expanded<ColumnDecimal128V3>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal128v2()) {
                return_column = _execute_number_expanded<ColumnDecimal128V2>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal256()) {
                return_column = _execute_number_expanded<ColumnDecimal256>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            }
        } else if ((is_date_or_datetime(right_type) || is_date_v2_or_datetime_v2(right_type)) &&
                   (is_date_or_datetime(left_element_type) ||
                    is_date_v2_or_datetime_v2(left_element_type))) {
            if (left_which_type.is_date()) {
                return_column = _execute_number_expanded<ColumnDate>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_date_time()) {
                return_column = _execute_number_expanded<ColumnDateTime>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_date_v2()) {
                return_column = _execute_number_expanded<ColumnDateV2>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_date_time_v2()) {
                return_column = _execute_number_expanded<ColumnDateTimeV2>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            }
        }

        if (return_column) {
            block.replace_by_position(result, std::move(return_column));
            return Status::OK();
        }
        return Status::RuntimeError("execute failed or unsupported types for function {}({}, {})",
                                    get_name(),
                                    block.get_by_position(arguments[0]).type->get_name(),
                                    block.get_by_position(arguments[1]).type->get_name());
    }
};

} // namespace doris::vectorized
