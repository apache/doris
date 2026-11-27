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

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <string_view>

#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "exprs/function/array/function_array_utils.h"
#include "exprs/function/function_test_util.h"

namespace doris {
using namespace ut_type;

TEST(FunctionParameterBoundaryTest, SubstringIndexMinimumCount) {
    // With no separator, an excessive negative count returns the entire one-character string.
    InputTypeSet types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                          PrimitiveType::TYPE_INT};
    DataSet rows = {{{std::string("a"), std::string(","), std::numeric_limits<Int32>::min()},
                     std::string("a")}};
    check_function_all_arg_comb<DataTypeString, true>("substring_index", types, rows);
}

TEST(FunctionParameterBoundaryTest, ArraySliceLengthBounds) {
    // Two elements are the minimum needed for start + INT64_MAX to overflow.
    auto values = ColumnInt32::create();
    values->insert_value(1);
    values->insert_value(2);
    auto ends = ColumnArray::ColumnOffsets::create();
    ends->insert_value(2);
    auto array = ColumnArray::create(std::move(values), std::move(ends));
    ColumnArrayExecutionData source;
    ASSERT_TRUE(extract_column_array_info(*array, source));
    for (Int64 offset : {Int64(2), Int64(-1)}) {
        auto offsets = ColumnInt64::create();
        offsets->insert_value(offset);
        auto lengths = ColumnInt64::create();
        lengths->insert_value(std::numeric_limits<Int64>::max());
        auto result = create_mutable_data(source.nested_col.get(), false);
        slice_array(result, source, *offsets, lengths.get());
        ASSERT_EQ(result.nested_col->size(), 1);
        EXPECT_EQ((*result.offsets_ptr)[0], 1);
        EXPECT_EQ(result.nested_col->get_int(0), 2);
    }
}

TEST(FunctionParameterBoundaryTest, BitmapNegativeElement) {
    // A single UINT64_MAX element makes the old -1 conversion observable in both functions.
    BitmapValue bitmap(std::numeric_limits<UInt64>::max());
    for (bool const_bitmap : {false, true}) {
        for (bool const_element : {false, true}) {
            InputTypeSet types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BIGINT};
            if (const_bitmap) {
                types[0] = Consted {PrimitiveType::TYPE_BITMAP};
            }
            if (const_element) {
                types[1] = Consted {PrimitiveType::TYPE_BIGINT};
            }
            DataSet contains = {{{&bitmap, Int64(-1)}, UInt8(0)}};
            auto status = check_function<DataTypeUInt8, true>("bitmap_contains", types, contains,
                                                              -1, -1, true);
            EXPECT_NE(status.to_string().find("element must be >= 0"), std::string::npos);
            DataSet remove = {{{&bitmap, Int64(-1)}, &bitmap}};
            status = check_function<DataTypeBitMap, true>("bitmap_remove", types, remove, -1, -1,
                                                          true);
            EXPECT_NE(status.to_string().find("element must be >= 0"), std::string::npos);
            EXPECT_TRUE(bitmap.contains(std::numeric_limits<UInt64>::max()));
        }
    }
}

namespace {
void check_bitmap_null_element_payload(const char* name, bool const_bitmap) {
    auto bitmaps = ColumnBitmap::create();
    bitmaps->get_data().assign(const_bitmap ? 1 : 2, BitmapValue(1));
    ColumnPtr bitmap_column = std::move(bitmaps);
    if (const_bitmap) {
        bitmap_column = ColumnConst::create(bitmap_column, 2);
    }
    auto elements = ColumnInt64::create();
    elements->get_data() = {-1, 1};
    auto nulls = ColumnUInt8::create();
    nulls->get_data() = {1, 0};
    ColumnPtr nullable_elements = ColumnNullable::create(std::move(elements), std::move(nulls));
    const bool contains = std::string_view(name) == "bitmap_contains";
    DataTypePtr result_type = contains ? make_nullable(std::make_shared<DataTypeUInt8>())
                                       : make_nullable(std::make_shared<DataTypeBitMap>());
    Block block {{bitmap_column, std::make_shared<DataTypeBitMap>(), "bitmap"},
                 {nullable_elements, make_nullable(std::make_shared<DataTypeInt64>()), "element"}};
    auto function = SimpleFunctionFactory::instance().get_function(
            name, block.get_columns_with_type_and_name(), result_type);
    ASSERT_NE(function, nullptr);
    block.insert({nullptr, result_type, "result"});
    auto status = function->execute(nullptr, block, {0, 1}, 2, 2);
    ASSERT_TRUE(status.ok()) << status;
    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_TRUE(result.is_null_at(0));
    EXPECT_FALSE(result.is_null_at(1));
    if (contains) {
        EXPECT_EQ(result.get_nested_column().get_int(1), 1);
    } else {
        const auto& values =
                assert_cast<const ColumnBitmap&>(result.get_nested_column()).get_data();
        EXPECT_TRUE(values[1].empty());
    }
}
} // namespace

TEST(FunctionParameterBoundaryTest, BitmapNullElementPayload) {
    // Two rows reproduce NULLIF(k, -1): the first -1 is hidden by NULL and the second
    // element is valid. Check a constant bitmap and a bitmap column for both functions.
    for (const auto* name : {"bitmap_contains", "bitmap_remove"}) {
        for (bool const_bitmap : {false, true}) {
            check_bitmap_null_element_payload(name, const_bitmap);
        }
    }
}

TEST(FunctionParameterBoundaryTest, FloatingNegativeRoundingScale) {
    InputTypeSet types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_INT};
    // One value at 10^19 distinguishes 10^20 from the old saturated UINT64_MAX factor.
    for (const std::string function : {"round", "round_bankers", "truncate", "floor"}) {
        DataSet rows = {{{1e19, Int32(-20)}, double(0)}};
        check_function_all_arg_comb<DataTypeFloat64, true>(function, types, rows);
    }
    DataSet ceiling = {{{1e19, Int32(-20)}, 1e20}};
    check_function_all_arg_comb<DataTypeFloat64, true>("ceil", types, ceiling);
    // The minimum supported scale has no finite factor. ROUND still returns zero.
    DataSet tiny = {{{double(1), Int32(-32768)}, double(0)}};
    check_function_all_arg_comb<DataTypeFloat64, true>("round", types, tiny);
    DataSet directed = {{{double(1), Int32(-309)}, double(0)}};
    auto status = check_function<DataTypeFloat64, true>("ceil", types, directed, -1, -1, true);
    EXPECT_NE(status.to_string().find("Arithmetic overflow"), std::string::npos);
    DataSet overflow = {{{1.7e308, Int32(-308)}, double(0)}};
    status = check_function<DataTypeFloat64, true>("ceil", types, overflow, -1, -1, true);
    EXPECT_NE(status.to_string().find("Arithmetic overflow"), std::string::npos);
    // A finite factor can underflow the quotient. CEIL must still return one unit.
    DataSet underflow = {{{1e-300, Int32(-308)}, 1e308}};
    check_function_all_arg_comb<DataTypeFloat64, true>("ceil", types, underflow);
}

TEST(FunctionParameterBoundaryTest, FloatingPositiveRoundingScale) {
    // A single subnormal value distinguishes a decimal unit from an infinite factor.
    // Enumerate constant/column arguments so all three dispatcher paths execute.
    InputTypeSet types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_INT};
    for (const std::string function : {"round", "round_bankers", "truncate"}) {
        for (double value : {1e-310, -1e-310}) {
            check_function_all_arg_comb<DataTypeFloat64, true>(function, types,
                                                               {{{value, Int32(309)}, double(0)}});
        }
    }
    check_function_all_arg_comb<DataTypeFloat64, true>("ceil", types,
                                                       {{{1e-310, Int32(309)}, 1e-309}});
    check_function_all_arg_comb<DataTypeFloat64, true>("floor", types,
                                                       {{{-1e-310, Int32(309)}, -1e-309}});
    // The last decimal place that can change the smallest DOUBLE is 323.
    const auto smallest = std::numeric_limits<double>::denorm_min();
    check_function_all_arg_comb<DataTypeFloat64, true>("round", types,
                                                       {{{smallest, Int32(323)}, double(0)}});
    check_function_all_arg_comb<DataTypeFloat64, true>("ceil", types,
                                                       {{{smallest, Int32(323)}, 1e-323}});
    for (Int32 precision : {Int32(324), Int32(32767)}) {
        for (const std::string function : {"round", "round_bankers", "truncate", "floor", "ceil"}) {
            check_function_all_arg_comb<DataTypeFloat64, true>(function, types,
                                                               {{{smallest, precision}, smallest}});
        }
    }
}

TEST(FunctionParameterBoundaryTest, NegativeRoundingDirection) {
    InputTypeSet doubles = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_INT};
    // Negative directed rounding must choose the adjacent negative unit, even when
    // the quotient underflows. CEIL of the same value must choose zero.
    check_function_all_arg_comb<DataTypeFloat64, true>("floor", doubles,
                                                       {{{-1e-300, Int32(-308)}, -1e308}});
    check_function_all_arg_comb<DataTypeFloat64, true>("ceil", doubles,
                                                       {{{double(-1), Int32(-309)}, double(0)}});
    check_function_all_arg_comb<DataTypeFloat64, true>("floor", doubles,
                                                       {{{double(1), Int32(-309)}, double(0)}});
    const auto status = check_function<DataTypeFloat64, true>(
            "floor", doubles, {{{double(-1), Int32(-309)}, double(0)}}, -1, -1, true);
    EXPECT_NE(status.to_string().find("Arithmetic overflow"), std::string::npos);
}

} // namespace doris
