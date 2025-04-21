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

#include "util/simd/approx_distance_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

struct SparseItem {
    int32_t index;
    float value;

    SparseItem() = default;
    SparseItem(int32_t index, float value) : index(index), value(value) {}
};

// sort by id asc
inline bool compare_by_sparse_item_id(const SparseItem &s1, const SparseItem &s2) {
    return s1.index < s2.index;
}

    class ApproximateL2Distance {
    public:
        static constexpr auto name = "approx_l2_distance";
        struct State {
            double sum = 0;
        };
        static void accumulate(State& state, double x, double y) { state.sum += (x - y) * (x - y); }
        static double finalize(const State& state) { return state.sum; }
        static double compute_for_map_type(std::vector<SparseItem> s1, std::vector<SparseItem> s2) {
            return 0.0f;
        }
        static double compute_with_simd(float* x, float* y, size_t dim) {
            return simd::fvec_L2sqr(x, y, dim);
        }
        // This function is used to handle null values when the vector index is short-circuited.
        // In l2_distance, null values are changed to the maximum value to be consistent with the
        // index results in top-n sorting.
        static void deal_with_null_value(Float64& dst_data, UInt8& dst_null_data) {
            dst_data = std::numeric_limits<double>::max();
            dst_null_data = false;
        }
    };

    class ApproximateCosineDistance {
    public:
        static constexpr auto name = "approx_cosine_distance";
        struct State {
            double dot_prod = 0;
            double squared_x = 0;
            double squared_y = 0;
        };
        static void accumulate(State& state, double x, double y) {
            state.dot_prod += x * y;
            state.squared_x += x * x;
            state.squared_y += y * y;
        }
        static double finalize(const State& state) {
            return 1 - state.dot_prod / sqrt(state.squared_x * state.squared_y);
        }
        static double compute_for_map_type(std::vector<SparseItem> s1, std::vector<SparseItem> s2) {
            return 0.0f;
        }
        static double compute_with_simd(float* x, float* y, size_t dim) {
            simd::fvec_renorm_L2(dim, 1, x);
            simd::fvec_renorm_L2(dim, 1, y);
            return (simd::fvec_L2sqr(x, y, dim) / 2);
        }
        static void deal_with_null_value(Float64& dst_data, UInt8& dst_null_data) {
            // for now, do nothing
        }
    };

    class ApproximateCosineSimilarity {
    public:
        static constexpr auto name = "approx_cosine_similarity";
        struct State {
            double dot_prod = 0;
            double squared_x = 0;
            double squared_y = 0;
        };
        static void accumulate(State& state, double x, double y) {
            state.dot_prod += x * y;
            state.squared_x += x * x;
            state.squared_y += y * y;
        }
        static double finalize(const State& state) {
            return state.dot_prod / sqrt(state.squared_x * state.squared_y);
        }
        static double compute_for_map_type(std::vector<SparseItem> s1, std::vector<SparseItem> s2) {
            return 0.0f;
        }
        static double compute_with_simd(float* x, float* y, size_t dim) {
            simd::fvec_renorm_L2(dim, 1, x);
            simd::fvec_renorm_L2(dim, 1, y);
            return 1 - (simd::fvec_L2sqr(x, y, dim) / 2);
        }
        static void deal_with_null_value(Float64& dst_data, UInt8& dst_null_data) {
            // for now, do nothing
        }
    };

    class ApproximateInnerProduct {
    public:
        static constexpr auto name = "approx_inner_product";
        struct State {
            double dot_prod = 0;
        };
        static void accumulate(State& state, double x, double y) {
            state.dot_prod += x * y;
        }
        static double finalize(const State& state) {
            return state.dot_prod;
        }
        static double compute_for_map_type(std::vector<SparseItem> s1, std::vector<SparseItem> s2) {
            size_t count1 = s1.size();
            size_t count2 = s2.size();

            double product_sum = 0.0f;
            size_t i = 0;
            size_t j = 0;

            while (i < count1 && j < count2) {
                SparseItem& left = s1.at(i);
                SparseItem& right = s2.at(j);

                if (left.index < right.index) {
                    ++i;
                } else if (left.index > right.index) {
                    ++j;
                } else {
                    product_sum += left.value * right.value;
                    ++i;
                    ++j;
                }
            }
            return product_sum;
        }
        static double compute_with_simd(float* x, float* y, size_t dim) {
            return simd::fvec_inner_product(x, y, dim);
        }
        static void deal_with_null_value(Float64& dst_data, UInt8& dst_null_data) {
            // for now, do nothing
        }
    };

    template <typename DistanceImpl>
    class FunctionArrayApproximateDistance : public IFunction {
    public:
        static constexpr auto name = DistanceImpl::name;
        String get_name() const override { return name; }
        static FunctionPtr create() { return std::make_shared<FunctionArrayApproximateDistance<DistanceImpl>>(); }
        bool is_variadic() const override { return true; }
        size_t get_number_of_arguments() const override { return 0; }
        bool use_default_implementation_for_nulls() const override { return false; }

        DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
            return make_nullable(std::make_shared<DataTypeFloat64>());
        }

        void check_number_of_arguments(size_t number_of_arguments) const override {
            // 1 arg:  distance result get by vector index.
            // 2 args: vector column and query const vector.
            // 3 args: vector column, query const vector and distance get by vector index.
            if (number_of_arguments < 1 || number_of_arguments > 3) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                "Number of arguments for function {} doesn't match: passed {}, should be 1, 2 or 3.",
                       get_name(), number_of_arguments);
            }

        }

        Status execute_impl(FunctionContext* context,
                            Block& block,
                            const ColumnNumbers& arguments,
                            size_t result,
                            size_t input_rows_count) const override {

            if (arguments.size() == 1) {
                return _execute_impl_direct_from_virtual_column(context, block, arguments, result, input_rows_count);
            }

            const auto& arg1 = block.get_by_position(arguments[0]);
            const auto& arg2 = block.get_by_position(arguments[1]);

            int input_res1 = _valid_input_type(arg1.type);
            int input_res2 = _valid_input_type(arg2.type);

            if (input_res1 == 0 || input_res2 == 0 || input_res1 != input_res2) {
                return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})",
                                                        get_name(), arg1.type->get_name(),
                                                        arg2.type->get_name()));
            }

            if (input_res1 == 1) {
                return _execute_impl_for_array(context, block, arguments, result, input_rows_count);
            }
            return _execute_impl_for_map(context, block, arguments, result, input_rows_count);
        }

    private:
        // result:0: invalid input;
        // result:1: for dense vector, array of integer or float is valid;
        // result:2: for sparse vector, map of integer->float or integer->integer is valid;
        int _valid_input_type(const DataTypePtr& type) const {
            auto vector_type = remove_nullable(type);

            if (is_array(vector_type)) {
                return _valid_input_for_array_type(vector_type);
            }

            if (is_map(vector_type)) {
                return _valid_input_for_map_type(vector_type);
            }
            return 0;
        }

        int _valid_input_for_array_type(const DataTypePtr& array_type) const {
            auto nested_type = remove_nullable(assert_cast<const DataTypeArray&>(*array_type).get_nested_type());
            if (is_integer(nested_type) || is_float(nested_type)) {
                return 1;
            }
            return 0;
        }

        int _valid_input_for_map_type(const DataTypePtr& map_type) const {
            auto key_type = remove_nullable(assert_cast<const DataTypeMap&>(*map_type).get_key_type());
            auto value_type = remove_nullable(assert_cast<const DataTypeMap&>(*map_type).get_value_type());
            if (is_integer(key_type) && (is_integer(value_type) || is_float(value_type))) {
                return 2;
            }
            return 0;
        }

        Status _execute_impl_direct_from_virtual_column(FunctionContext* context,
                                                        Block& block,
                                                        const ColumnNumbers& arguments,
                                                        size_t result,
                                                        size_t input_rows_count) const {
            auto nullable_v_proj_col = check_and_get_column<ColumnNullable>(block.get_by_position(arguments[0]).column.get());
            if (UNLIKELY(!nullable_v_proj_col)) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                    "The nullable virtual projection column type for function {} doesn't match: "
                    "passed {}, should be Nullable(Float64).",
               get_name(), block.get_by_position(arguments[0]).column->get_name());
            }

            auto v_proj_col = check_and_get_column<ColumnFloat64>(nullable_v_proj_col->get_nested_column());
            if (UNLIKELY(!v_proj_col)) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                    "The virtual projection column type for function {} doesn't match: "
                    "passed {}, should be Nullable(Float64).",
               get_name(), block.get_by_position(arguments[0]).column->get_name());
            }

            // prepare return data
            auto dst = ColumnVector<Float64>::create(input_rows_count);
            auto& dst_data = dst->get_data();

            // the input column data may be null or the distance maybe nan
            // null or nan data should be marked in return block
            auto dst_null_column = ColumnUInt8::create(input_rows_count);
            auto& dst_null_data = dst_null_column->get_data();

            for (int row = 0; row < input_rows_count; row++) {
                if (UNLIKELY(nullable_v_proj_col->is_null_at(row))) {
                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                    "Can not get not null value from virtual column, rowid:{}: ", row);
                }
                dst_data[row] =  v_proj_col->get_element(row);
                dst_null_data[row] = std::isnan(dst_data[row]);
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(dst), std::move(dst_null_column)));
            return Status::OK();
        }

        Status _execute_impl_for_array(FunctionContext* context,
                                       Block& block,
                                       const ColumnNumbers& arguments,
                                       size_t result,
                                       size_t input_rows_count) const {

            const auto& arg1 = block.get_by_position(arguments[0]);
            const auto& arg2 = block.get_by_position(arguments[1]);

            auto col1 = arg1.column->convert_to_full_column_if_const();
            auto col2 = arg2.column->convert_to_full_column_if_const();
            if (col1->size() != col2->size()) {
                return Status::RuntimeError(
                        fmt::format("function {} have different input array sizes: {} and {}",
                                    get_name(), col1->size(), col2->size()));
            }

            ColumnArrayExecutionData arr1;
            ColumnArrayExecutionData arr2;

            if (!extract_column_array_info(*col1, arr1) || !extract_column_array_info(*col2, arr2)) {
                return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})",
                                                        get_name(), arg1.type->get_name(),
                                                        arg2.type->get_name()));
            }

            const auto& offsets1 = *arr1.offsets_ptr;
            const auto& offsets2 = *arr2.offsets_ptr;
            const auto& nested_col1 = arr1.nested_col;
            const auto& nested_col2 = arr2.nested_col;

            const ColumnNullable* nullable_v_proj_col = nullptr;
            const ColumnFloat64* v_proj_col = nullptr;

            bool push_down = arguments.size() == 3;
            if (push_down) {
                nullable_v_proj_col = check_and_get_column<ColumnNullable>(block.get_by_position(arguments[2]).column.get());
                if (UNLIKELY(!nullable_v_proj_col)) {
                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                        "The nullable virtual projection column type for function {} doesn't match: "
                        "passed {}, should be Nullable(Float64).",
                   get_name(), block.get_by_position(arguments[2]).column->get_name());
                }

                v_proj_col = check_and_get_column<ColumnFloat64>(nullable_v_proj_col->get_nested_column());
                if (UNLIKELY(!v_proj_col)) {
                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                        "The virtual projection column type for function {} doesn't match: "
                        "passed {}, should be Nullable(Float64).",
                   get_name(), block.get_by_position(arguments[2]).column->get_name());
                }
            }

            // prepare return data
            auto dst = ColumnVector<Float64>::create(input_rows_count);
            auto& dst_data = dst->get_data();
            // the input column data may be null or the distance maybe nan
            // null or nan data should be marked in return block
            auto dst_null_column = ColumnUInt8::create(input_rows_count);
            auto& dst_null_data = dst_null_column->get_data();

            for (int row = 0; row < input_rows_count; row++) {
                if (push_down && !nullable_v_proj_col->is_null_at(row)) {
                    dst_data[row] = v_proj_col->get_element(row);
                    dst_null_data[row] = std::isnan(dst_data[row]);
                } else {
                    // if proj is nullable then caculate this row.
                    if (arr1.array_nullmap_data && arr1.array_nullmap_data[row]) {
                        dst_null_data[row] = true;
                        DistanceImpl::deal_with_null_value(dst_data[row], dst_null_data[row]);
                        continue;
                    }
                    if (arr2.array_nullmap_data && arr2.array_nullmap_data[row]) {
                        dst_null_data[row] = true;
                        DistanceImpl::deal_with_null_value(dst_data[row], dst_null_data[row]);
                        continue;
                    }
                    // if offset is 0, then return null
                    auto offset1 = offsets1[row] - offsets1[row - 1];
                    auto offset2 = offsets2[row] - offsets2[row - 1];
                    if (offset1 == 0 || offset2 == 0) {
                        dst_null_data[row] = true;
                        DistanceImpl::deal_with_null_value(dst_data[row], dst_null_data[row]);
                        continue;
                    }

                    dst_null_data[row] = false;
                    // offset not equal, return error
                    if (offset1 != offset2) [[unlikely]] {
                        return Status::InvalidArgument(
                                "function {} have different input element sizes of array: {} and {}",
                                get_name(), offsets1[row] - offsets1[row - 1],
                                offsets2[row] - offsets2[row - 1]);
                    }

#if defined(__AVX2__)
                    float compute_arr_1[offset1];
                    float compute_arr_2[offset1];
                    for (int offset = 0; offset < offset1; offset++) {
                        ssize_t pos1 = offsets1[row - 1] + offset;
                        ssize_t pos2 = offsets2[row - 1] + offset;
                        if (arr1.nested_nullmap_data && arr1.nested_nullmap_data[pos1]) {
                            dst_null_data[row] = true;
                            DistanceImpl::deal_with_null_value(dst_data[row], dst_null_data[row]);
                            break;
                        }
                        if (arr2.nested_nullmap_data && arr2.nested_nullmap_data[pos2]) {
                            dst_null_data[row] = true;
                            DistanceImpl::deal_with_null_value(dst_data[row], dst_null_data[row]);
                            break;
                        }
                        compute_arr_1[offset] = nested_col1->get_float64(pos1);
                        compute_arr_2[offset] = nested_col2->get_float64(pos2);
                    }

                    if (!dst_null_data[row]) {
                        dst_data[row] = DistanceImpl::compute_with_simd(compute_arr_1, compute_arr_2, offset1);
                        dst_null_data[row] = std::isnan(dst_data[row]);
                    }
#else
                    typename DistanceImpl::State st;
                    for (int offset = 0; offset < offset1; offset++) {
                        ssize_t pos1 = offsets1[row - 1] + offset;
                        ssize_t pos2 = offsets2[row - 1] + offset;
                        if (arr1.nested_nullmap_data && arr1.nested_nullmap_data[pos1]) {
                            dst_null_data[row] = true;
                            break;
                        }
                        if (arr2.nested_nullmap_data && arr2.nested_nullmap_data[pos2]) {
                            dst_null_data[row] = true;
                            break;
                        }
                        DistanceImpl::accumulate(st, nested_col1->get_float64(pos1),
                                                 nested_col2->get_float64(pos2));
                    }
                    if (!dst_null_data[row]) {
                        dst_data[row] = DistanceImpl::finalize(st);
                        dst_null_data[row] = std::isnan(dst_data[row]);
                    }
#endif
                }
            }
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(dst), std::move(dst_null_column)));
            return Status::OK();
        }

        Status _execute_impl_for_map(FunctionContext* context,
                                     Block& block,
                                     const ColumnNumbers& arguments,
                                     size_t result,
                                     size_t input_rows_count) const {

            const auto& arg1 = block.get_by_position(arguments[0]);
            const auto& arg2 = block.get_by_position(arguments[1]);

            auto col1 = arg1.column->convert_to_full_column_if_const();
            auto col2 = arg2.column->convert_to_full_column_if_const();
            if (col1->size() != col2->size()) {
                return Status::RuntimeError(
                        fmt::format("function {} have different input array sizes: {} and {}",
                                    get_name(), col1->size(), col2->size()));
            }

            const ColumnMap* map_col1;
            ColumnPtr nullmap_col1 = nullptr;

            const ColumnMap* map_col2;
            ColumnPtr nullmap_col2 = nullptr;

            if (col1->is_nullable()) {
                auto nullable_column = reinterpret_cast<const ColumnNullable*>(col1.get());
                map_col1 = check_and_get_column<ColumnMap>(nullable_column->get_nested_column());
                nullmap_col1 = nullable_column->get_null_map_column_ptr();
            } else {
                map_col1 = check_and_get_column<ColumnMap>(*col1.get());
            }

            if (col2->is_nullable()) {
                auto nullable_column = reinterpret_cast<const ColumnNullable*>(col2.get());
                map_col2 = check_and_get_column<ColumnMap>(nullable_column->get_nested_column());
                nullmap_col2 = nullable_column->get_null_map_column_ptr();
            } else {
                map_col2 = check_and_get_column<ColumnMap>(*col2.get());
            }

            if (!map_col1 || !map_col2) {
                return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})",
                                                        get_name(), arg1.type->get_name(),
                                                        arg2.type->get_name()));
            }

            auto key_array_col1 = map_col1->get_keys_array_ptr()->convert_to_full_column_if_const();
            auto value_array_col1 = map_col1->get_values_array_ptr()->convert_to_full_column_if_const();

            auto key_array_col2 = map_col2->get_keys_array_ptr()->convert_to_full_column_if_const();
            auto value_array_col2 = map_col2->get_values_array_ptr()->convert_to_full_column_if_const();

            ColumnArrayExecutionData key_arr1;
            ColumnArrayExecutionData value_arr1;

            ColumnArrayExecutionData key_arr2;
            ColumnArrayExecutionData value_arr2;

            if (!extract_column_array_info(*key_array_col1, key_arr1) ||
                    !extract_column_array_info(*value_array_col1, value_arr1) ||
                    !extract_column_array_info(*key_array_col2, key_arr2) ||
                    !extract_column_array_info(*value_array_col2, value_arr2)) {
                return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})",
                                                        get_name(), arg1.type->get_name(),
                                                        arg2.type->get_name()));
            }

            const auto& key_offsets1 = *key_arr1.offsets_ptr;
            const auto& key_offsets2 = *key_arr2.offsets_ptr;

            const auto& key_nested_col1 = key_arr1.nested_col;
            const auto& value_nested_col1 = value_arr1.nested_col;

            const auto& key_nested_col2 = key_arr2.nested_col;
            const auto& value_nested_col2 = value_arr2.nested_col;

            const ColumnNullable* nullable_v_proj_col = nullptr;
            const ColumnFloat64* v_proj_col = nullptr;

            bool push_down = arguments.size() == 3;
            if (push_down) {
                nullable_v_proj_col = check_and_get_column<ColumnNullable>(block.get_by_position(arguments[2]).column.get());
                if (UNLIKELY(!nullable_v_proj_col)) {
                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                        "The nullable virtual projection column type for function {} doesn't match: "
                        "passed {}, should be Nullable(Float64).",
                   get_name(), block.get_by_position(arguments[2]).column->get_name());
                }

                v_proj_col = check_and_get_column<ColumnFloat64>(nullable_v_proj_col->get_nested_column());
                if (UNLIKELY(!v_proj_col)) {
                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                        "The virtual projection column type for function {} doesn't match: "
                        "passed {}, should be Nullable(Float64).",
                   get_name(), block.get_by_position(arguments[2]).column->get_name());
                }
            }

            // prepare return data
            auto dst = ColumnVector<Float64>::create(input_rows_count);
            auto& dst_data = dst->get_data();

            // the input column data may be null or the distance maybe nan
            // null or nan data should be marked in return block
            auto dst_null_column = ColumnUInt8::create(input_rows_count);
            auto& dst_null_data = dst_null_column->get_data();

            for (int row = 0; row < input_rows_count; row++) {
                if (push_down && !nullable_v_proj_col->is_null_at(row)) {
                    dst_data[row] = v_proj_col->get_element(row);
                    dst_null_data[row] = std::isnan(dst_data[row]);
                } else {
                    // if proj is nullable then caculate this row.
                    if (nullmap_col1 && nullmap_col1->get_uint(row)) {
                        dst_null_data[row] = true;
                        continue;
                    }

                    if (nullmap_col2 && nullmap_col2->get_uint(row)) {
                        dst_null_data[row] = true;
                        continue;
                    }

                    dst_null_data[row] = false;

                    // sparse vector #1:
                    std::vector<SparseItem> map1_row;
                    for (ssize_t pos = key_offsets1[row - 1]; pos < key_offsets1[row]; ++pos) {
                        if ((key_arr1.nested_nullmap_data && key_arr1.nested_nullmap_data[pos])
                                || (value_arr1.nested_nullmap_data && value_arr1.nested_nullmap_data[pos])) {
                            dst_null_data[row] = true;
                            break;
                        }
                        map1_row.emplace_back(key_nested_col1->get_int(pos), value_nested_col1->get_float64(pos));
                    }

                    // sparse vector #2:
                    std::vector<SparseItem> map2_row;
                    for (ssize_t pos = key_offsets2[row - 1]; pos < key_offsets2[row]; ++pos) {
                        if ((key_arr2.nested_nullmap_data && key_arr2.nested_nullmap_data[pos])
                                || (value_arr2.nested_nullmap_data && value_arr2.nested_nullmap_data[pos])) {
                            dst_null_data[row] = true;
                            break;
                        }
                        map2_row.emplace_back(key_nested_col2->get_int(pos), value_nested_col2->get_float64(pos));
                    }

                    if (!dst_null_data[row]) {
                        std::sort(map1_row.begin(), map1_row.end(), compare_by_sparse_item_id);
                        std::sort(map2_row.begin(), map2_row.end(), compare_by_sparse_item_id);

                        dst_data[row] = DistanceImpl::compute_for_map_type(map1_row, map2_row);
                        dst_null_data[row] = std::isnan(dst_data[row]);
                    }
                }
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(dst), std::move(dst_null_column)));
            return Status::OK();
        }
    };

} // namespace doris::vectorized
