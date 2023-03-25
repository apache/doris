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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionBitmap.h
// and modified by Doris

#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_always_not_nullable.h"
#include "vec/functions/function_bitmap_min_or_max.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"
namespace doris::vectorized {

struct BitmapEmpty {
    static constexpr auto name = "bitmap_empty";
    using ReturnColVec = ColumnBitmap;
    static DataTypePtr get_return_type() { return std::make_shared<DataTypeBitMap>(); }
    static auto init_value() { return BitmapValue {}; }
};

struct ToBitmap {
    static constexpr auto name = "to_bitmap";
    using ReturnType = DataTypeBitMap;

    template <typename ColumnType>
    static void vector(const ColumnType* col, MutableColumnPtr& col_res) {
        execute<ColumnType, false>(col, nullptr, col_res);
    }
    template <typename ColumnType>
    static void vector_nullable(const ColumnType* col, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        execute<ColumnType, true>(col, &nullmap, col_res);
    }
    template <typename ColumnType, bool arg_is_nullable>
    static void execute(const ColumnType* col, const NullMap* nullmap, MutableColumnPtr& col_res) {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const ColumnString::Chars& data = col->get_chars();
            const ColumnString::Offsets& offsets = col->get_offsets();

            auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = offsets.size();

            for (size_t i = 0; i < size; ++i) {
                if (arg_is_nullable && ((*nullmap)[i])) {
                    continue;
                } else {
                    const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                    size_t str_size = offsets[i] - offsets[i - 1];
                    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
                    uint64_t int_value = StringParser::string_to_unsigned_int<uint64_t>(
                            raw_str, str_size, &parse_result);
                    if (LIKELY(parse_result == StringParser::PARSE_SUCCESS)) {
                        res_data[i].add(int_value);
                    }
                }
            }
        } else if constexpr (std::is_same_v<ColumnType, ColumnInt64>) {
            auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = col->size();

            for (size_t i = 0; i < size; ++i) {
                if (arg_is_nullable && ((*nullmap)[i])) {
                    continue;
                } else {
                    int64_t int_value = col->get_data()[i];
                    if (LIKELY(int_value >= 0)) {
                        res_data[i].add(int_value);
                    }
                }
            }
        }
    }
};

struct ToBitmapWithCheck {
    static constexpr auto name = "to_bitmap_with_check";
    using ReturnType = DataTypeBitMap;

    template <typename ColumnType>
    static Status vector(const ColumnType* col, MutableColumnPtr& col_res) {
        return execute<ColumnType, false>(col, nullptr, col_res);
    }
    template <typename ColumnType>
    static Status vector_nullable(const ColumnType* col, const NullMap& nullmap,
                                  MutableColumnPtr& col_res) {
        return execute<ColumnType, true>(col, &nullmap, col_res);
    }
    template <typename ColumnType, bool arg_is_nullable>
    static Status execute(const ColumnType* col, const NullMap* nullmap,
                          MutableColumnPtr& col_res) {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const ColumnString::Chars& data = col->get_chars();
            const ColumnString::Offsets& offsets = col->get_offsets();
            auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = offsets.size();

            for (size_t i = 0; i < size; ++i) {
                if (arg_is_nullable && ((*nullmap)[i])) {
                    continue;
                } else {
                    const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                    size_t str_size = offsets[i] - offsets[i - 1];
                    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
                    uint64_t int_value = StringParser::string_to_unsigned_int<uint64_t>(
                            raw_str, str_size, &parse_result);
                    if (LIKELY(parse_result == StringParser::PARSE_SUCCESS)) {
                        res_data[i].add(int_value);
                    } else {
                        std::stringstream ss;
                        ss << "The input: " << std::string(raw_str, str_size)
                           << " is not valid, to_bitmap only support bigint value from 0 to "
                              "18446744073709551615 currently, cannot create MV with to_bitmap on "
                              "column with negative values or cannot load negative values to "
                              "column "
                              "with to_bitmap MV on it.";
                        LOG(WARNING) << ss.str();
                        return Status::InternalError(ss.str());
                    }
                }
            }
        } else if constexpr (std::is_same_v<ColumnType, ColumnInt64>) {
            auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = col->size();

            for (size_t i = 0; i < size; ++i) {
                if (arg_is_nullable && ((*nullmap)[i])) {
                    continue;
                } else {
                    int64_t int_value = col->get_data()[i];
                    if (LIKELY(int_value >= 0)) {
                        res_data[i].add(int_value);
                    } else {
                        std::stringstream ss;
                        ss << "The input: " << int_value
                           << " is not valid, to_bitmap only support bigint value from 0 to "
                              "18446744073709551615 currently, cannot create MV with to_bitmap on "
                              "column with negative values or cannot load negative values to "
                              "column "
                              "with to_bitmap MV on it.";
                        LOG(WARNING) << ss.str();
                        return Status::InternalError(ss.str());
                    }
                }
            }
        } else {
            return Status::InternalError("not support type");
        }
        return Status::OK();
    }
};

struct BitmapFromString {
    using ArgumentType = DataTypeString;

    static constexpr auto name = "bitmap_from_string";

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         std::vector<BitmapValue>& res, NullMap& null_map,
                         size_t input_rows_count) {
        res.reserve(input_rows_count);
        std::vector<uint64_t> bits;
        if (offsets.size() == 0 && input_rows_count == 1) {
            // For NULL constant
            res.emplace_back();
            null_map[0] = 1;
            return Status::OK();
        }
        for (size_t i = 0; i < input_rows_count; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int64_t str_size = offsets[i] - offsets[i - 1];

            if ((str_size > INT32_MAX) ||
                !(SplitStringAndParse({raw_str, (int)str_size}, ",", &safe_strtou64, &bits))) {
                res.emplace_back();
                null_map[i] = 1;
                continue;
            }
            res.emplace_back(bits);
            bits.clear();
        }
        return Status::OK();
    }
};

struct BitmapFromArray {
    using ArgumentType = DataTypeArray;
    static constexpr auto name = "bitmap_from_array";

    template <typename ColumnType>
    static Status vector(const ColumnArray::Offsets64& offset_column_data,
                         const IColumn& nested_column, const NullMap& nested_null_map,
                         std::vector<BitmapValue>& res, NullMap& null_map) {
        const auto& nested_column_data = static_cast<const ColumnType&>(nested_column).get_data();
        auto size = offset_column_data.size();
        res.reserve(size);
        std::vector<uint64_t> bits;
        for (size_t i = 0; i < size; ++i) {
            auto curr_offset = offset_column_data[i];
            auto prev_offset = offset_column_data[i - 1];
            for (auto j = prev_offset; j < curr_offset; ++j) {
                auto data = nested_column_data[j];
                // invaild value
                if (UNLIKELY(data < 0) || UNLIKELY(nested_null_map[j])) {
                    res.emplace_back();
                    null_map[i] = 1;
                    break;
                } else {
                    bits.push_back(data);
                }
            }
            //input is valid value
            if (!null_map[i]) {
                res.emplace_back(bits);
                bits.clear();
            }
        }
        return Status::OK();
    }
};

template <typename Impl>
class FunctionBitmapAlwaysNull : public IFunction {
public:
    static constexpr auto name = Impl::name;

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionBitmapAlwaysNull>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeBitMap>());
    }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return true; }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto res_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res_data_column = ColumnBitmap::create();
        auto& null_map = res_null_map->get_data();
        auto& res = res_data_column->get_data();

        ColumnPtr& argument_column = block.get_by_position(arguments[0]).column;
        if constexpr (std::is_same_v<typename Impl::ArgumentType, DataTypeString>) {
            const auto& str_column = static_cast<const ColumnString&>(*argument_column);
            const ColumnString::Chars& data = str_column.get_chars();
            const ColumnString::Offsets& offsets = str_column.get_offsets();
            Impl::vector(data, offsets, res, null_map, input_rows_count);
        } else if constexpr (std::is_same_v<typename Impl::ArgumentType, DataTypeArray>) {
            auto argument_type = remove_nullable(
                    assert_cast<const DataTypeArray&>(*block.get_by_position(arguments[0]).type)
                            .get_nested_type());
            const auto& array_column = static_cast<const ColumnArray&>(*argument_column);
            const auto& offset_column_data = array_column.get_offsets();
            const auto& nested_nullable_column =
                    static_cast<const ColumnNullable&>(array_column.get_data());
            const auto& nested_column = nested_nullable_column.get_nested_column();
            const auto& nested_null_map = nested_nullable_column.get_null_map_column().get_data();
            if (check_column<ColumnInt8>(nested_column)) {
                Impl::template vector<ColumnInt8>(offset_column_data, nested_column,
                                                  nested_null_map, res, null_map);
            } else if (check_column<ColumnInt16>(nested_column)) {
                Impl::template vector<ColumnInt16>(offset_column_data, nested_column,
                                                   nested_null_map, res, null_map);
            } else if (check_column<ColumnInt32>(nested_column)) {
                Impl::template vector<ColumnInt32>(offset_column_data, nested_column,
                                                   nested_null_map, res, null_map);
            } else if (check_column<ColumnInt64>(nested_column)) {
                Impl::template vector<ColumnInt64>(offset_column_data, nested_column,
                                                   nested_null_map, res, null_map);
            }
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_data_column), std::move(res_null_map));
        return Status::OK();
    }
};

template <int HashBits>
struct BitmapHashName {};

template <>
struct BitmapHashName<32> {
    static constexpr auto name = "bitmap_hash";
};

template <>
struct BitmapHashName<64> {
    static constexpr auto name = "bitmap_hash64";
};

template <int HashBits>
struct BitmapHash {
    static constexpr auto name = BitmapHashName<HashBits>::name;

    using ReturnType = DataTypeBitMap;

    template <typename ColumnType>
    static void vector(const ColumnType* col, MutableColumnPtr& col_res) {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const ColumnString::Chars& data = col->get_chars();
            const ColumnString::Offsets& offsets = col->get_offsets();
            auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = offsets.size();

            for (size_t i = 0; i < size; ++i) {
                const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                size_t str_size = offsets[i] - offsets[i - 1];
                if constexpr (HashBits == 32) {
                    uint32_t hash_value =
                            HashUtil::murmur_hash3_32(raw_str, str_size, HashUtil::MURMUR3_32_SEED);
                    res_data[i].add(hash_value);
                } else {
                    uint64_t hash_value = 0;
                    murmur_hash3_x64_64(raw_str, str_size, 0, &hash_value);
                    res_data[i].add(hash_value);
                }
            }
        }
    }

    template <typename ColumnType>
    static void vector_nullable(const ColumnType* col, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const ColumnString::Chars& data = col->get_chars();
            const ColumnString::Offsets& offsets = col->get_offsets();
            auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = offsets.size();

            for (size_t i = 0; i < size; ++i) {
                if (nullmap[i]) {
                    continue;
                } else {
                    const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                    size_t str_size = offsets[i] - offsets[i - 1];
                    if constexpr (HashBits == 32) {
                        uint32_t hash_value = HashUtil::murmur_hash3_32(raw_str, str_size,
                                                                        HashUtil::MURMUR3_32_SEED);
                        res_data[i].add(hash_value);
                    } else {
                        uint64_t hash_value = 0;
                        murmur_hash3_x64_64(raw_str, str_size, 0, &hash_value);
                        res_data[i].add(hash_value);
                    }
                }
            }
        }
    }
};

class FunctionBitmapCount : public IFunction {
public:
    static constexpr auto name = "bitmap_count";

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionBitmapCount>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto res_data_column = ColumnInt64::create();
        auto& res = res_data_column->get_data();
        auto data_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto& null_map = data_null_map->get_data();

        auto& column = block.get_by_position(arguments[0]).column;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*column)) {
            VectorizedUtils::update_null_map(null_map, nullable->get_null_map_data());
            column = nullable->get_nested_column_ptr();
        }
        auto str_col = assert_cast<const ColumnBitmap*>(column.get());
        const auto& col_data = str_col->get_data();

        res.reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                res.push_back(0);
                continue;
            }
            res.push_back(col_data[i].cardinality());
        }
        block.replace_by_position(result, std::move(res_data_column));
        return Status::OK();
    }
};

struct NameBitmapNot {
    static constexpr auto name = "bitmap_not";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapNot {
    using ResultDataType = DataTypeBitMap;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;

    static Status vector_vector(const TData& lvec, const TData& rvec, TData& res) {
        size_t size = lvec.size();
        for (size_t i = 0; i < size; ++i) {
            res[i] = lvec[i];
            res[i] -= rvec[i];
        }
        return Status::OK();
    }
};

struct NameBitmapAndNot {
    static constexpr auto name = "bitmap_and_not";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapAndNot {
    using ResultDataType = DataTypeBitMap;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;

    static Status vector_vector(const TData& lvec, const TData& rvec, TData& res) {
        size_t size = lvec.size();
        BitmapValue mid_data;
        for (size_t i = 0; i < size; ++i) {
            mid_data = lvec[i];
            mid_data &= rvec[i];
            res[i] = lvec[i];
            res[i] -= mid_data;
            mid_data.clear();
        }
        return Status::OK();
    }
};

struct NameBitmapAndNotCount {
    static constexpr auto name = "bitmap_and_not_count";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapAndNotCount {
    using ResultDataType = DataTypeInt64;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;
    using ResTData = typename ColumnVector<Int64>::Container::value_type;

    static Status vector_vector(const TData& lvec, const TData& rvec, ResTData* res) {
        size_t size = lvec.size();
        BitmapValue mid_data;
        for (size_t i = 0; i < size; ++i) {
            mid_data = lvec[i];
            mid_data &= rvec[i];
            res[i] = lvec[i].andnot_cardinality(mid_data);
            mid_data.clear();
        }
        return Status::OK();
    }
};

void update_bitmap_op_count(int64_t* __restrict count, const NullMap& null_map) {
    static constexpr int64_t flags[2] = {-1, 0};
    size_t size = null_map.size();
    auto* __restrict null_map_data = null_map.data();
    for (size_t i = 0; i < size; ++i) {
        count[i] &= flags[null_map_data[i]];
    }
}

// for bitmap_and_count, bitmap_xor_count and bitmap_and_not_count,
// result is 0 for rows that if any column is null value
ColumnPtr handle_bitmap_op_count_null_value(ColumnPtr& src, const Block& block,
                                            const ColumnNumbers& args, size_t result,
                                            size_t input_rows_count) {
    auto* nullable = assert_cast<const ColumnNullable*>(src.get());
    ColumnPtr src_not_nullable = nullable->get_nested_column_ptr();
    MutableColumnPtr src_not_nullable_mutable = (*std::move(src_not_nullable)).assume_mutable();
    auto* __restrict count_data =
            assert_cast<ColumnInt64*>(src_not_nullable_mutable.get())->get_data().data();

    for (const auto& arg : args) {
        const ColumnWithTypeAndName& elem = block.get_by_position(arg);
        if (!elem.type->is_nullable()) {
            continue;
        }

        bool is_const = is_column_const(*elem.column);
        /// Const Nullable that are NULL.
        if (is_const && assert_cast<const ColumnConst*>(elem.column.get())->only_null()) {
            return block.get_by_position(result).type->create_column_const(input_rows_count, 0);
        }
        if (is_const) {
            continue;
        }

        if (auto* nullable = assert_cast<const ColumnNullable*>(elem.column.get())) {
            const ColumnPtr& null_map_column = nullable->get_null_map_column_ptr();
            const NullMap& src_null_map =
                    assert_cast<const ColumnUInt8&>(*null_map_column).get_data();

            update_bitmap_op_count(count_data, src_null_map);
        }
    }

    return src;
}

Status execute_bitmap_op_count_null_to_zero(
        FunctionContext* context, Block& block, const ColumnNumbers& arguments, size_t result,
        size_t input_rows_count,
        const std::function<Status(FunctionContext*, Block&, const ColumnNumbers&, size_t, size_t)>&
                exec_impl_func) {
    NullPresence null_presence = get_null_presence(block, arguments);

    if (null_presence.has_null_constant) {
        block.get_by_position(result).column =
                block.get_by_position(result).type->create_column_const(input_rows_count, 0);
    } else if (null_presence.has_nullable) {
        auto [temporary_block, new_args, new_result] =
                create_block_with_nested_columns(block, arguments, result);
        RETURN_IF_ERROR(exec_impl_func(context, temporary_block, new_args, new_result,
                                       temporary_block.rows()));
        block.get_by_position(result).column = handle_bitmap_op_count_null_value(
                temporary_block.get_by_position(new_result).column, block, arguments, result,
                input_rows_count);
    } else {
        return exec_impl_func(context, block, arguments, result, input_rows_count);
    }
    return Status::OK();
}

class FunctionBitmapAndNotCount : public IFunction {
public:
    using LeftDataType = DataTypeBitMap;
    using RightDataType = DataTypeBitMap;
    using ResultDataType = typename BitmapAndNotCount<LeftDataType, RightDataType>::ResultDataType;

    static constexpr auto name = "bitmap_and_not_count";
    static FunctionPtr create() { return std::make_shared<FunctionBitmapAndNotCount>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        bool return_nullable = false;
        // result is nullable only when any columns is nullable for bitmap_and_not_count
        for (size_t i = 0; i < arguments.size(); ++i) {
            if (arguments[i]->is_nullable()) {
                return_nullable = true;
                break;
            }
        }
        auto result_type = std::make_shared<ResultDataType>();
        return return_nullable ? make_nullable(result_type) : result_type;
    }

    bool use_default_implementation_for_constants() const override { return true; }

    bool use_default_implementation_for_nulls() const override {
        // for bitmap_and_not_count, result is always not null, and if the bitmap op result is null,
        // the count is 0
        return false;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_EQ(arguments.size(), 2);

        return execute_bitmap_op_count_null_to_zero(
                context, block, arguments, result, input_rows_count,
                std::bind((Status(FunctionBitmapAndNotCount::*)(
                                  FunctionContext*, Block&, const ColumnNumbers&, size_t, size_t)) &
                                  FunctionBitmapAndNotCount::execute_impl_internal,
                          this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
                          std::placeholders::_4, std::placeholders::_5));
    }

    Status execute_impl_internal(FunctionContext* context, Block& block,
                                 const ColumnNumbers& arguments, size_t result,
                                 size_t input_rows_count) {
        const auto& left = block.get_by_position(arguments[0]);
        auto lcol = left.column->convert_to_full_column_if_const();
        const auto& right = block.get_by_position(arguments[1]);
        auto rcol = right.column->convert_to_full_column_if_const();

        using ResultType = typename ResultDataType::FieldType;
        using ColVecResult = ColumnVector<ResultType>;

        typename ColVecResult::MutablePtr col_res = ColVecResult::create();
        auto& vec_res = col_res->get_data();
        vec_res.resize(block.rows());

        const ColumnBitmap* l_bitmap_col = assert_cast<const ColumnBitmap*>(lcol.get());
        const ColumnBitmap* r_bitmap_col = assert_cast<const ColumnBitmap*>(rcol.get());
        BitmapAndNotCount<LeftDataType, RightDataType>::vector_vector(
                l_bitmap_col->get_data(), r_bitmap_col->get_data(), vec_res.data());

        auto& result_info = block.get_by_position(result);
        if (result_info.type->is_nullable()) {
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res),
                                                   ColumnUInt8::create(input_rows_count, 0)));
        } else {
            block.replace_by_position(result, std::move(col_res));
        }
        return Status::OK();
    }
};

struct NameBitmapContains {
    static constexpr auto name = "bitmap_contains";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapContains {
    using ResultDataType = DataTypeUInt8;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using LTData = std::vector<BitmapValue>;
    using RTData = typename ColumnVector<T1>::Container;
    using ResTData = typename ColumnVector<UInt8>::Container;

    static Status vector_vector(const LTData& lvec, const RTData& rvec, ResTData& res) {
        size_t size = lvec.size();
        for (size_t i = 0; i < size; ++i) {
            res[i] = lvec[i].contains(rvec[i]);
        }
        return Status::OK();
    }
};

struct NameBitmapHasAny {
    static constexpr auto name = "bitmap_has_any";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapHasAny {
    using ResultDataType = DataTypeUInt8;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;
    using ResTData = typename ColumnVector<UInt8>::Container;

    static Status vector_vector(const TData& lvec, const TData& rvec, ResTData& res) {
        size_t size = lvec.size();
        for (size_t i = 0; i < size; ++i) {
            auto bitmap = const_cast<BitmapValue&>(lvec[i]);
            bitmap &= rvec[i];
            res[i] = bitmap.cardinality() != 0;
        }
        return Status::OK();
    }
};

struct NameBitmapHasAll {
    static constexpr auto name = "bitmap_has_all";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapHasAll {
    using ResultDataType = DataTypeUInt8;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;
    using ResTData = typename ColumnVector<UInt8>::Container;

    static Status vector_vector(const TData& lvec, const TData& rvec, ResTData& res) {
        size_t size = lvec.size();
        for (size_t i = 0; i < size; ++i) {
            uint64_t lhs_cardinality = lvec[i].cardinality();
            auto bitmap = const_cast<BitmapValue&>(lvec[i]);
            bitmap |= rvec[i];
            res[i] = bitmap.cardinality() == lhs_cardinality;
        }
        return Status::OK();
    }
};

struct NameBitmapToString {
    static constexpr auto name = "bitmap_to_string";
};

struct BitmapToString {
    using ReturnType = DataTypeString;
    static constexpr auto TYPE_INDEX = TypeIndex::BitMap;
    using Type = DataTypeBitMap::FieldType;
    using ReturnColumnType = ColumnString;
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;

    static Status vector(const std::vector<BitmapValue>& data, Chars& chars, Offsets& offsets) {
        size_t size = data.size();
        offsets.resize(size);
        chars.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            StringOP::push_value_string(data[i].to_string(), i, chars, offsets);
        }
        return Status::OK();
    }
};

struct SubBitmap {
    static constexpr auto name = "sub_bitmap";
    using TData1 = std::vector<BitmapValue>;
    using TData2 = typename ColumnVector<Int64>::Container;

    static Status vector_vector(const TData1& bitmap_data, const TData2& offset_data,
                                const TData2& limit_data, NullMap& null_map,
                                size_t input_rows_count, TData1& res) {
        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                continue;
            }
            if (limit_data[i] <= 0) {
                null_map[i] = 1;
                continue;
            }
            if (const_cast<TData1&>(bitmap_data)[i].offset_limit(offset_data[i], limit_data[i],
                                                                 &res[i]) == 0) {
                null_map[i] = 1;
            }
        }
        return Status::OK();
    }
};

struct BitmapSubsetLimit {
    static constexpr auto name = "bitmap_subset_limit";
    using TData1 = std::vector<BitmapValue>;
    using TData2 = typename ColumnVector<Int64>::Container;

    static Status vector_vector(const TData1& bitmap_data, const TData2& offset_data,
                                const TData2& limit_data, NullMap& null_map,
                                size_t input_rows_count, TData1& res) {
        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                continue;
            }
            if (offset_data[i] < 0 || limit_data[i] < 0) {
                null_map[i] = 1;
                continue;
            }
            const_cast<TData1&>(bitmap_data)[i].sub_limit(offset_data[i], limit_data[i], &res[i]);
        }
        return Status::OK();
    }
};

struct BitmapSubsetInRange {
    static constexpr auto name = "bitmap_subset_in_range";
    using TData1 = std::vector<BitmapValue>;
    using TData2 = typename ColumnVector<Int64>::Container;

    static Status vector_vector(const TData1& bitmap_data, const TData2& range_start,
                                const TData2& range_end, NullMap& null_map, size_t input_rows_count,
                                TData1& res) {
        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                continue;
            }
            if (range_start[i] >= range_end[i] || range_start[i] < 0 || range_end[i] < 0) {
                null_map[i] = 1;
                continue;
            }
            const_cast<TData1&>(bitmap_data)[i].sub_range(range_start[i], range_end[i], &res[i]);
        }
        return Status::OK();
    }
};

template <typename Impl>
class FunctionBitmapSubs : public IFunction {
public:
    static constexpr auto name = Impl::name;
    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionBitmapSubs>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeBitMap>());
    }

    size_t get_number_of_arguments() const override { return 3; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_EQ(arguments.size(), 3);
        auto res_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res_data_column = ColumnBitmap::create(input_rows_count);
        ColumnPtr argument_columns[3];

        for (int i = 0; i < 3; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                VectorizedUtils::update_null_map(res_null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        auto bitmap_column = assert_cast<const ColumnBitmap*>(argument_columns[0].get());
        auto offset_column = assert_cast<const ColumnVector<Int64>*>(argument_columns[1].get());
        auto limit_column = assert_cast<const ColumnVector<Int64>*>(argument_columns[2].get());

        Impl::vector_vector(bitmap_column->get_data(), offset_column->get_data(),
                            limit_column->get_data(), res_null_map->get_data(), input_rows_count,
                            res_data_column->get_data());

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_data_column), std::move(res_null_map));
        return Status::OK();
    }
};

class FunctionBitmapToArray : public IFunction {
public:
    static constexpr auto name = "bitmap_to_array";

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionBitmapToArray>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto nested_type = make_nullable(std::make_shared<DataTypeInt64>());
        return std::make_shared<DataTypeArray>(nested_type);
    }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return true; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto return_nested_type = make_nullable(std::make_shared<DataTypeInt64>());
        auto dest_array_column_ptr = ColumnArray::create(return_nested_type->create_column(),
                                                         ColumnArray::ColumnOffsets::create());

        IColumn* dest_nested_column = &dest_array_column_ptr->get_data();
        ColumnNullable* dest_nested_nullable_col =
                reinterpret_cast<ColumnNullable*>(dest_nested_column);
        dest_nested_column = dest_nested_nullable_col->get_nested_column_ptr();
        auto& dest_nested_null_map = dest_nested_nullable_col->get_null_map_column().get_data();

        auto& arg_col = block.get_by_position(arguments[0]).column;
        auto bitmap_col = assert_cast<const ColumnBitmap*>(arg_col.get());
        const auto& bitmap_col_data = bitmap_col->get_data();
        auto& nested_column_data =
                assert_cast<ColumnVector<Int64>*>(dest_nested_column)->get_data();
        auto& dest_offsets = dest_array_column_ptr->get_offsets();
        dest_offsets.reserve(input_rows_count);

        for (int i = 0; i < input_rows_count; ++i) {
            bitmap_col_data[i].to_array(nested_column_data);
            dest_nested_null_map.resize_fill(nested_column_data.size(), 0);
            dest_offsets.push_back(nested_column_data.size());
        }

        block.replace_by_position(result, std::move(dest_array_column_ptr));
        return Status::OK();
    }
};

using FunctionBitmapEmpty = FunctionConst<BitmapEmpty, false>;
using FunctionToBitmap = FunctionAlwaysNotNullable<ToBitmap>;
using FunctionToBitmapWithCheck = FunctionAlwaysNotNullable<ToBitmapWithCheck, true>;

using FunctionBitmapFromString = FunctionBitmapAlwaysNull<BitmapFromString>;
using FunctionBitmapFromArray = FunctionBitmapAlwaysNull<BitmapFromArray>;
using FunctionBitmapHash = FunctionAlwaysNotNullable<BitmapHash<32>>;
using FunctionBitmapHash64 = FunctionAlwaysNotNullable<BitmapHash<64>>;

using FunctionBitmapMin = FunctionBitmapSingle<FunctionBitmapMinImpl>;
using FunctionBitmapMax = FunctionBitmapSingle<FunctionBitmapMaxImpl>;

using FunctionBitmapToString = FunctionUnaryToType<BitmapToString, NameBitmapToString>;
using FunctionBitmapNot =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapNot, NameBitmapNot>;
using FunctionBitmapAndNot =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapAndNot, NameBitmapAndNot>;
using FunctionBitmapContains =
        FunctionBinaryToType<DataTypeBitMap, DataTypeInt64, BitmapContains, NameBitmapContains>;

using FunctionBitmapHasAny =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapHasAny, NameBitmapHasAny>;
using FunctionBitmapHasAll =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapHasAll, NameBitmapHasAll>;
using FunctionSubBitmap = FunctionBitmapSubs<SubBitmap>;
using FunctionBitmapSubsetLimit = FunctionBitmapSubs<BitmapSubsetLimit>;
using FunctionBitmapSubsetInRange = FunctionBitmapSubs<BitmapSubsetInRange>;

void register_function_bitmap(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitmapEmpty>();
    factory.register_function<FunctionToBitmap>();
    factory.register_function<FunctionToBitmapWithCheck>();
    factory.register_function<FunctionBitmapFromString>();
    factory.register_function<FunctionBitmapFromArray>();
    factory.register_function<FunctionBitmapHash>();
    factory.register_function<FunctionBitmapHash64>();
    factory.register_function<FunctionBitmapCount>();
    factory.register_function<FunctionBitmapMin>();
    factory.register_function<FunctionBitmapMax>();
    factory.register_function<FunctionBitmapToString>();
    factory.register_function<FunctionBitmapNot>();
    factory.register_function<FunctionBitmapAndNot>();
    factory.register_function<FunctionBitmapAndNotCount>();
    factory.register_function<FunctionBitmapContains>();
    factory.register_function<FunctionBitmapHasAny>();
    factory.register_function<FunctionBitmapHasAll>();
    factory.register_function<FunctionSubBitmap>();
    factory.register_function<FunctionBitmapSubsetLimit>();
    factory.register_function<FunctionBitmapSubsetInRange>();
    factory.register_function<FunctionBitmapToArray>();
}

} // namespace doris::vectorized
