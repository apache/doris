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

#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <utility>

#include "common/status.h"
#include "function_array_index.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris {
class FunctionContext;
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris::vectorized {

template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;

template <typename T>
struct OverlapSetImpl {
    using ElementNativeType = typename NativeType<typename T::value_type>::Type;
    using Set = phmap::flat_hash_set<ElementNativeType, DefaultHash<ElementNativeType>>;
    Set set;

    template <bool nullable>
    void insert_array(const IColumn* column, const UInt8* nullmap, size_t start, size_t size) {
        const auto& vec = assert_cast<const T&>(*column).get_data();
        for (size_t i = start; i < start + size; ++i) {
            if constexpr (nullable) {
                if (nullmap[i]) {
                    continue;
                }
            }
            set.insert(vec[i]);
        }
    }

    template <bool nullable>
    bool find_any(const IColumn* column, const UInt8* nullmap, size_t start, size_t size) {
        const auto& vec = assert_cast<const T&>(*column).get_data();
        for (size_t i = start; i < start + size; ++i) {
            if constexpr (nullable) {
                if (nullmap[i]) {
                    continue;
                }
            }

            if (set.contains(vec[i])) {
                return true;
            }
        }
        return false;
    }
};

template <>
struct OverlapSetImpl<ColumnString> {
    using Set = phmap::flat_hash_set<StringRef, DefaultHash<StringRef>>;
    Set set;

    template <bool nullable>
    void insert_array(const IColumn* column, const UInt8* nullmap, size_t start, size_t size) {
        for (size_t i = start; i < start + size; ++i) {
            if constexpr (nullable) {
                if (nullmap[i]) {
                    continue;
                }
            }
            set.insert(column->get_data_at(i));
        }
    }

    template <bool nullable>
    bool find_any(const IColumn* column, const UInt8* nullmap, size_t start, size_t size) {
        for (size_t i = start; i < start + size; ++i) {
            if constexpr (nullable) {
                if (nullmap[i]) {
                    continue;
                }
            }

            if (set.contains(column->get_data_at(i))) {
                return true;
            }
        }
        return false;
    }
};

class FunctionArraysOverlap : public IFunction {
public:
    static constexpr auto name = "arrays_overlap";
    static FunctionPtr create() { return std::make_shared<FunctionArraysOverlap>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto left_data_type = remove_nullable(arguments[0]);
        auto right_data_type = remove_nullable(arguments[1]);
        DCHECK(left_data_type->get_primitive_type() == TYPE_ARRAY) << arguments[0]->get_name();
        DCHECK(right_data_type->get_primitive_type() == TYPE_ARRAY) << arguments[1]->get_name();
        auto left_nested_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*left_data_type).get_nested_type());
        auto right_nested_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*right_data_type).get_nested_type());
        DCHECK(left_nested_type->equals(*right_nested_type))
                << "data type " << arguments[0]->get_name() << " not equal with "
                << arguments[1]->get_name();
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }

    /**
     * eval inverted index. we can filter array rows with inverted index iter
     * array_overlap(array, []) -> array_overlap(array, const value)
     */
    Status evaluate_inverted_index(
            const ColumnsWithTypeAndName& arguments,
            const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
            segment_v2::InvertedIndexResultBitmap& bitmap_result) const override {
        DCHECK(arguments.size() == 1);
        DCHECK(data_type_with_names.size() == 1);
        DCHECK(iterators.size() == 1);
        auto* iter = iterators[0];
        if (iter == nullptr) {
            return Status::OK();
        }
        auto data_type_with_name = data_type_with_names[0];
        if (!segment_v2::IndexReaderHelper::has_string_or_bkd_index(iter)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "Inverted index evaluate skipped, no inverted index reader can not support "
                    "array_overlap");
        }
        // in arrays_overlap param is array Field and const Field
        ColumnPtr arg_column = arguments[0].column;
        DataTypePtr arg_type = arguments[0].type;
        if ((is_column_nullable(*arg_column) && !is_column_const(*remove_nullable(arg_column))) ||
            (!is_column_nullable(*arg_column) && !is_column_const(*arg_column))) {
            // if not we should skip inverted index and evaluate in expression
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "Inverted index evaluate skipped, array_overlap only support const value");
        }

        Field param_value;
        arguments[0].column->get(0, param_value);
        DCHECK(arguments[0].type->get_primitive_type() == TYPE_ARRAY);
        auto nested_param_type =
                check_and_get_data_type<DataTypeArray>(remove_nullable(arguments[0].type).get())
                        ->get_nested_type()
                        ->get_primitive_type();
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
        const Array& query_val = param_value.get<Array>();

        InvertedIndexParam param;
        param.column_name = data_type_with_name.first;
        param.column_type = data_type_with_name.second;
        param.query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
        param.num_rows = num_rows;
        for (auto nested_query_val : query_val) {
            // any element inside array is NULL, return NULL
            // by current arrays_overlap execute logic.
            if (nested_query_val.is_null()) {
                return Status::OK();
            }
            RETURN_IF_ERROR(InvertedIndexQueryParamFactory::create_query_value(
                    nested_param_type, &nested_query_val, query_param));
            param.query_value = query_param->get_value();
            param.roaring = std::make_shared<roaring::Roaring>();
            ;
            RETURN_IF_ERROR(iter->read_from_index(&param));
            *roaring |= *param.roaring;
        }

        segment_v2::InvertedIndexResultBitmap result(roaring, null_bitmap);
        bitmap_result = result;
        bitmap_result.mask_out_null();

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DBUG_EXECUTE_IF("array_func.arrays_overlap", {
            auto req_id = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                    "array_func.arrays_overlap", "req_id", 0);
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "{} has already execute inverted index req_id {} , should not execute expr "
                    "with rows: {}",
                    get_name(), req_id, input_rows_count);
        });
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto right_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        ColumnArrayExecutionData left_exec_data;
        ColumnArrayExecutionData right_exec_data;

        Status ret = Status::InvalidArgument(
                "execute failed, unsupported types for function {}({}, {})", get_name(),
                block.get_by_position(arguments[0]).type->get_name(),
                block.get_by_position(arguments[1]).type->get_name());

        // extract array column
        if (!extract_column_array_info(*left_column, left_exec_data) ||
            !extract_column_array_info(*right_column, right_exec_data)) {
            return ret;
        }
        // prepare return column
        auto dst_nested_col = ColumnUInt8::create(input_rows_count, 0);
        auto dst_null_map = ColumnUInt8::create(input_rows_count, 0);
        UInt8* dst_null_map_data = dst_null_map->get_data().data();

        RETURN_IF_ERROR(_execute_nullable(left_exec_data, dst_null_map_data));
        RETURN_IF_ERROR(_execute_nullable(right_exec_data, dst_null_map_data));

        // execute overlap check
        auto array_type = remove_nullable(block.get_by_position(arguments[0]).type);
        auto left_element_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*array_type).get_nested_type());
        switch (left_element_type->get_primitive_type()) {
        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR:
            ret = _execute_internal<ColumnString>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data());
            break;
        case TYPE_DATE:
            ret = _execute_internal<ColumnDate>(left_exec_data, right_exec_data, dst_null_map_data,
                                                dst_nested_col->get_data().data());
            break;
        case TYPE_DATETIME:
            ret = _execute_internal<ColumnDateTime>(left_exec_data, right_exec_data,
                                                    dst_null_map_data,
                                                    dst_nested_col->get_data().data());
            break;
        case TYPE_DATEV2:
            ret = _execute_internal<ColumnDateV2>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data());
            break;
        case TYPE_DATETIMEV2:
            ret = _execute_internal<ColumnDateTimeV2>(left_exec_data, right_exec_data,
                                                      dst_null_map_data,
                                                      dst_nested_col->get_data().data());
            break;
        case TYPE_BOOLEAN:
            ret = _execute_internal<ColumnUInt8>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data());
            break;
        case TYPE_TINYINT:
            ret = _execute_internal<ColumnInt8>(left_exec_data, right_exec_data, dst_null_map_data,
                                                dst_nested_col->get_data().data());
            break;
        case TYPE_SMALLINT:
            ret = _execute_internal<ColumnInt16>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data());
            break;
        case TYPE_INT:
            ret = _execute_internal<ColumnInt32>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data());
            break;
        case TYPE_BIGINT:
            ret = _execute_internal<ColumnInt64>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data());
            break;
        case TYPE_LARGEINT:
            ret = _execute_internal<ColumnInt128>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data());
            break;
        case TYPE_FLOAT:
            ret = _execute_internal<ColumnFloat32>(left_exec_data, right_exec_data,
                                                   dst_null_map_data,
                                                   dst_nested_col->get_data().data());
            break;
        case TYPE_DOUBLE:
            ret = _execute_internal<ColumnFloat64>(left_exec_data, right_exec_data,
                                                   dst_null_map_data,
                                                   dst_nested_col->get_data().data());
            break;
        case TYPE_DECIMAL32:
            ret = _execute_internal<ColumnDecimal32>(left_exec_data, right_exec_data,
                                                     dst_null_map_data,
                                                     dst_nested_col->get_data().data());
            break;
        case TYPE_DECIMAL64:
            ret = _execute_internal<ColumnDecimal64>(left_exec_data, right_exec_data,
                                                     dst_null_map_data,
                                                     dst_nested_col->get_data().data());
            break;
        case TYPE_DECIMAL128I:
            ret = _execute_internal<ColumnDecimal128V3>(left_exec_data, right_exec_data,
                                                        dst_null_map_data,
                                                        dst_nested_col->get_data().data());
            break;
        case TYPE_DECIMALV2:
            ret = _execute_internal<ColumnDecimal128V2>(left_exec_data, right_exec_data,
                                                        dst_null_map_data,
                                                        dst_nested_col->get_data().data());
            break;
        case TYPE_DECIMAL256:
            ret = _execute_internal<ColumnDecimal256>(left_exec_data, right_exec_data,
                                                      dst_null_map_data,
                                                      dst_nested_col->get_data().data());
            break;
        default:
            break;
        }

        if (ret.ok()) {
            block.replace_by_position(result, ColumnNullable::create(std::move(dst_nested_col),
                                                                     std::move(dst_null_map)));
        }

        return ret;
    }

private:
    static Status _execute_nullable(const ColumnArrayExecutionData& data, UInt8* dst_nullmap_data) {
        for (ssize_t row = 0; row < data.offsets_ptr->size(); ++row) {
            if (dst_nullmap_data[row]) {
                continue;
            }

            if (data.array_nullmap_data && data.array_nullmap_data[row]) {
                dst_nullmap_data[row] = 1;
                continue;
            }

            if (data.nested_nullmap_data) {
                ssize_t start = (*data.offsets_ptr)[row - 1];
                ssize_t size = (*data.offsets_ptr)[row] - start;
                for (ssize_t i = start; i < start + size; ++i) {
                    if (data.nested_nullmap_data[i]) {
                        dst_nullmap_data[row] = 1;
                        break;
                    }
                }
            }
        }
        return Status::OK();
    }

    template <typename T>
    Status _execute_internal(const ColumnArrayExecutionData& left_data,
                             const ColumnArrayExecutionData& right_data, UInt8* dst_nullmap_data,
                             UInt8* dst_data) const {
        using ExecutorImpl = OverlapSetImpl<T>;
        for (ssize_t row = 0; row < left_data.offsets_ptr->size(); ++row) {
            ssize_t left_start = (*left_data.offsets_ptr)[row - 1];
            ssize_t left_size = (*left_data.offsets_ptr)[row] - left_start;
            ssize_t right_start = (*right_data.offsets_ptr)[row - 1];
            ssize_t right_size = (*right_data.offsets_ptr)[row] - right_start;
            if (left_size == 0 || right_size == 0) {
                dst_data[row] = 0;
                continue;
            }

            const auto* small_data = &left_data;
            const auto* large_data = &right_data;

            ssize_t small_start = left_start;
            ssize_t large_start = right_start;
            ssize_t small_size = left_size;
            ssize_t large_size = right_size;
            if (right_size < left_size) {
                std::swap(small_data, large_data);
                std::swap(small_start, large_start);
                std::swap(small_size, large_size);
            }

            ExecutorImpl impl;
            if (small_data->nested_nullmap_data) {
                impl.template insert_array<true>(small_data->nested_col.get(),
                                                 small_data->nested_nullmap_data, small_start,
                                                 small_size);
            } else {
                impl.template insert_array<true>(small_data->nested_col.get(),
                                                 small_data->nested_nullmap_data, small_start,
                                                 small_size);
            }

            if (large_data->nested_nullmap_data) {
                dst_data[row] = impl.template find_any<true>(large_data->nested_col.get(),
                                                             large_data->nested_nullmap_data,
                                                             large_start, large_size);
            } else {
                dst_data[row] = impl.template find_any<true>(large_data->nested_col.get(),
                                                             large_data->nested_nullmap_data,
                                                             large_start, large_size);
            }

            if (dst_data[row]) {
                dst_nullmap_data[row] = 0;
            }
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
