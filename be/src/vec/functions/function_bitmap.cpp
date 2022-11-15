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
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
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
    static constexpr auto name = "bitmap_from_string";

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         std::vector<BitmapValue>& res, NullMap& null_map) {
        auto size = offsets.size();
        res.reserve(size);
        std::vector<uint64_t> bits;
        for (size_t i = 0; i < size; ++i) {
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

        ColumnPtr argument_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnString* str_column = check_and_get_column<ColumnString>(argument_column.get());
        const ColumnString::Chars& data = str_column->get_chars();
        const ColumnString::Offsets& offsets = str_column->get_offsets();

        Impl::vector(data, offsets, res, null_map);

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

        auto column = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
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
    using ResTData = typename ColumnVector<Int64>::Container;

    static Status vector_vector(const TData& lvec, const TData& rvec, ResTData& res) {
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

        auto arg_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
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
using FunctionBitmapHash = FunctionAlwaysNotNullable<BitmapHash<32>>;
using FunctionBitmapHash64 = FunctionAlwaysNotNullable<BitmapHash<64>>;

using FunctionBitmapMin = FunctionBitmapSingle<FunctionBitmapMinImpl>;
using FunctionBitmapMax = FunctionBitmapSingle<FunctionBitmapMaxImpl>;

using FunctionBitmapToString = FunctionUnaryToType<BitmapToString, NameBitmapToString>;
using FunctionBitmapNot =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapNot, NameBitmapNot>;
using FunctionBitmapAndNot =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapAndNot, NameBitmapAndNot>;
using FunctionBitmapAndNotCount = FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap,
                                                       BitmapAndNotCount, NameBitmapAndNotCount>;
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
