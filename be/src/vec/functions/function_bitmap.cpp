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
#include "vec/functions/function_bitmap_min_or_max.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/functions/function_always_not_nullable.h"

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

    static void vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                       MutableColumnPtr& col_res) {
        execute<false>(data, offsets, nullptr, col_res);
    }

    static void vector_nullable(const ColumnString::Chars& data,
                                const ColumnString::Offsets& offsets, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        execute<true>(data, offsets, &nullmap, col_res);
    }
    template <bool arg_is_nullable>
    static void execute(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                        const NullMap* nullmap, MutableColumnPtr& col_res) {
        auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
        auto& res_data = res_column->get_data();
        size_t size = offsets.size();

        for (size_t i = 0; i < size; ++i) {
            if (arg_is_nullable && ((*nullmap)[i])) {
                continue;
            } else {
                const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                size_t str_size = offsets[i] - offsets[i - 1] - 1;
                StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
                uint64_t int_value = StringParser::string_to_unsigned_int<uint64_t>(
                        raw_str, str_size, &parse_result);
                if (LIKELY(parse_result == StringParser::PARSE_SUCCESS)) {
                    res_data[i].add(int_value);
                }
            }
        }
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
            int64_t str_size = offsets[i] - offsets[i - 1] - 1;

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

struct BitmapHash {
    static constexpr auto name = "bitmap_hash";

    using ReturnType = DataTypeBitMap;

    static void vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                       MutableColumnPtr& col_res) {
        auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
        auto& res_data = res_column->get_data();
        size_t size = offsets.size();

        for (size_t i = 0; i < size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t str_size = offsets[i] - offsets[i - 1] - 1;
            uint32_t hash_value =
                        HashUtil::murmur_hash3_32(raw_str, str_size, HashUtil::MURMUR3_32_SEED);
            res_data[i].add(hash_value);
        }
    }

    static void vector_nullable(const ColumnString::Chars& data,
                                const ColumnString::Offsets& offsets, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        auto* res_column = reinterpret_cast<ColumnBitmap*>(col_res.get());
        auto& res_data = res_column->get_data();
        size_t size = offsets.size();

        for (size_t i = 0; i < size; ++i) {
            if (nullmap[i]) {
                continue;
            } else {
                const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                size_t str_size = offsets[i] - offsets[i - 1] - 1;
                uint32_t hash_value =
                        HashUtil::murmur_hash3_32(raw_str, str_size, HashUtil::MURMUR3_32_SEED);
                res_data[i].add(hash_value);
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

using FunctionBitmapEmpty = FunctionConst<BitmapEmpty, false>;
using FunctionToBitmap = FunctionAlwaysNotNullable<ToBitmap>;
using FunctionBitmapFromString = FunctionBitmapAlwaysNull<BitmapFromString>;
using FunctionBitmapHash = FunctionAlwaysNotNullable<BitmapHash>;

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
    factory.register_function<FunctionBitmapFromString>();
    factory.register_function<FunctionBitmapHash>();
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
}

} // namespace doris::vectorized
