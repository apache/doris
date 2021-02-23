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

#include "exprs/bitmap_function.cpp"

#include <gtest/gtest.h>

#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "exprs/aggregate_functions.h"
#include "exprs/anyval_util.h"
#include "testutil/function_utils.h"
#include "util/bitmap_value.h"
#include "util/logging.h"

namespace doris {

StringVal convert_bitmap_to_string(FunctionContext* ctx, BitmapValue& bitmap) {
    StringVal result(ctx, bitmap.getSizeInBytes());
    bitmap.write((char*)result.ptr);
    return result;
}

template <typename T>
StringVal convert_bitmap_intersect_to_string(FunctionContext* ctx, BitmapIntersect<T>& intersect) {
    StringVal result(ctx, intersect.size());
    intersect.serialize((char*)result.ptr);
    return result;
}

class BitmapFunctionsTest : public testing::Test {
public:
    BitmapFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(BitmapFunctionsTest, bitmap_empty) {
    StringVal result = BitmapFunctions::bitmap_empty(ctx);

    BitmapValue bitmap;
    StringVal expected = convert_bitmap_to_string(ctx, bitmap);

    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, to_bitmap) {
    std::vector<uint64_t> values = {0, 65535, 4294967295, std::numeric_limits<uint64_t>::max()};
    for (auto val : values) {
        StringVal input = AnyValUtil::from_string_temp(ctx, std::to_string(val));
        StringVal result = BitmapFunctions::to_bitmap(ctx, input);

        BitmapValue bitmap(val);
        StringVal expected = convert_bitmap_to_string(ctx, bitmap);

        ASSERT_EQ(expected, result);
    }
}

TEST_F(BitmapFunctionsTest, to_bitmap_null) {
    StringVal input = StringVal::null();
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);

    BitmapValue bitmap;
    StringVal expected = convert_bitmap_to_string(ctx, bitmap);

    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, to_bitmap_invalid_argument) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("-1"));
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);
    ASSERT_EQ(StringVal::null(), result);
    ASSERT_TRUE(ctx->has_error());
}

TEST_F(BitmapFunctionsTest, to_bitmap_out_of_range) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("18446744073709551616"));
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);
    ASSERT_EQ(StringVal::null(), result);
    ASSERT_TRUE(ctx->has_error());
}

TEST_F(BitmapFunctionsTest, bitmap_union_int) {
    StringVal dst;
    BitmapFunctions::bitmap_init(ctx, &dst);
    IntVal src1(1);
    BitmapFunctions::bitmap_update_int(ctx, src1, &dst);
    IntVal src2(1234567);
    BitmapFunctions::bitmap_update_int(ctx, src2, &dst);

    BigIntVal result = BitmapFunctions::bitmap_finalize(ctx, dst);
    BigIntVal expected(2);
    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, bitmap_get_value) {
    StringVal dst;
    BitmapFunctions::bitmap_init(ctx, &dst);
    IntVal src1(1);
    BitmapFunctions::bitmap_update_int(ctx, src1, &dst);

    BigIntVal result = BitmapFunctions::bitmap_get_value(ctx, dst);
    BigIntVal expected(1);
    ASSERT_EQ(expected, result);

    IntVal src2(1234567);
    BitmapFunctions::bitmap_update_int(ctx, src2, &dst);

    result = BitmapFunctions::bitmap_get_value(ctx, dst);
    expected.val = 2;
    ASSERT_EQ(expected, result);

    BigIntVal finalize_result = BitmapFunctions::bitmap_finalize(ctx, dst);
    ASSERT_EQ(result, finalize_result);

    BigIntVal null_bitmap = BitmapFunctions::bitmap_get_value(ctx, StringVal::null());
    ASSERT_EQ(BigIntVal(0), null_bitmap);
}

TEST_F(BitmapFunctionsTest, bitmap_union) {
    StringVal dst;
    BitmapFunctions::bitmap_init(ctx, &dst);

    BitmapValue bitmap1(1024);
    StringVal src1 = convert_bitmap_to_string(ctx, bitmap1);
    BitmapFunctions::bitmap_union(ctx, src1, &dst);

    StringVal src2 = convert_bitmap_to_string(ctx, bitmap1);
    BitmapFunctions::bitmap_union(ctx, src2, &dst);

    BitmapValue bitmap3(2048);
    StringVal src3 = convert_bitmap_to_string(ctx, bitmap3);
    BitmapFunctions::bitmap_union(ctx, src3, &dst);

    StringVal src4 = convert_bitmap_to_string(ctx, bitmap3);
    BitmapFunctions::bitmap_union(ctx, src4, &dst);

    StringVal src5 = StringVal::null();
    BitmapFunctions::bitmap_union(ctx, src5, &dst);

    StringVal serialized = BitmapFunctions::bitmap_serialize(ctx, dst);

    BigIntVal result = BitmapFunctions::bitmap_count(ctx, serialized);
    BigIntVal expected(2);
    ASSERT_EQ(expected, result);
}

// test bitmap_intersect
TEST_F(BitmapFunctionsTest, bitmap_intersect) {
    StringVal dst;
    BitmapFunctions::nullable_bitmap_init(ctx, &dst);

    BitmapValue bitmap1(1);
    bitmap1.add(2);
    bitmap1.add(3);
    StringVal src1 = convert_bitmap_to_string(ctx, bitmap1);
    BitmapFunctions::bitmap_intersect(ctx, src1, &dst);

    BitmapValue bitmap2(1);
    bitmap2.add(2);
    StringVal src2 = convert_bitmap_to_string(ctx, bitmap2);
    BitmapFunctions::bitmap_intersect(ctx, src2, &dst);

    StringVal serialized = BitmapFunctions::bitmap_serialize(ctx, dst);
    BigIntVal result = BitmapFunctions::bitmap_count(ctx, serialized);
    BigIntVal expected(2);
    ASSERT_EQ(expected, result);
}

// test bitmap_intersect with null dst
TEST_F(BitmapFunctionsTest, bitmap_intersect_empty) {
    StringVal dst;
    BitmapFunctions::nullable_bitmap_init(ctx, &dst);

    StringVal serialized = BitmapFunctions::bitmap_serialize(ctx, dst);
    BigIntVal result = BitmapFunctions::bitmap_count(ctx, serialized);
    BigIntVal expected(0);
    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, bitmap_count) {
    BitmapValue bitmap(1024);
    bitmap.add(1);
    bitmap.add(2019);
    StringVal bitmap_str = convert_bitmap_to_string(ctx, bitmap);

    BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);
    BigIntVal expected(3);
    ASSERT_EQ(expected, result);

    BigIntVal null_bitmap = BitmapFunctions::bitmap_count(ctx, StringVal::null());
    ASSERT_EQ(BigIntVal(0), null_bitmap);
}

// test intersect_count
template <typename ValType, typename ValueType>
void test_bitmap_intersect(FunctionContext* ctx, ValType key1, ValType key2) {
    StringVal bitmap_column("placeholder");
    StringVal filter_column("placeholder");
    std::vector<doris_udf::AnyVal*> const_vals;
    const_vals.push_back(&bitmap_column);
    const_vals.push_back(&filter_column);
    const_vals.push_back(&key1);
    const_vals.push_back(&key2);
    ctx->impl()->set_constant_args(const_vals);

    StringVal dst;
    BitmapFunctions::bitmap_intersect_init<ValueType, ValType>(ctx, &dst);

    BitmapValue bitmap1({1024, 1025, 1026});
    StringVal src1 = convert_bitmap_to_string(ctx, bitmap1);
    BitmapFunctions::bitmap_intersect_update<ValueType, ValType>(ctx, src1, key1, 1, nullptr, &dst);

    BitmapValue bitmap2({1024, 1023, 1022});
    StringVal src2 = convert_bitmap_to_string(ctx, bitmap2);
    BitmapFunctions::bitmap_intersect_update<ValueType, ValType>(ctx, src2, key2, 2, nullptr, &dst);

    StringVal intersect1 = BitmapFunctions::bitmap_intersect_serialize<ValueType>(ctx, dst);

    BitmapIntersect<ValueType> intersect2;
    for (size_t i = 2; i < const_vals.size(); i++) {
        ValType* arg = reinterpret_cast<ValType*>(const_vals[i]);
        intersect2.add_key(detail::get_val<ValType, ValueType>(*arg));
    }
    intersect2.update(detail::get_val<ValType, ValueType>(key1), bitmap1);
    intersect2.update(detail::get_val<ValType, ValueType>(key2), bitmap2);
    StringVal expected = convert_bitmap_intersect_to_string(ctx, intersect2);
    ASSERT_EQ(expected, intersect1);

    BitmapIntersect<ValueType> intersect2_serde((char*)expected.ptr);
    ASSERT_EQ(1, intersect2_serde.intersect_count());

    StringVal dst2;
    BitmapFunctions::bitmap_intersect_init<ValueType, ValType>(ctx, &dst2);
    BitmapFunctions::bitmap_intersect_merge<ValueType>(ctx, intersect1, &dst2);
    BigIntVal result = BitmapFunctions::bitmap_intersect_finalize<ValueType>(ctx, dst2);
    BigIntVal expected_count(1);

    ASSERT_EQ(expected_count, result);
}

TEST_F(BitmapFunctionsTest, test_bitmap_intersect) {
    test_bitmap_intersect<TinyIntVal, int8_t>(ctx, TinyIntVal(101), TinyIntVal(102));
    test_bitmap_intersect<SmallIntVal, int16_t>(ctx, SmallIntVal((int16_t)65535),
                                                SmallIntVal((int16_t)65534));
    test_bitmap_intersect<IntVal, int32_t>(ctx, IntVal(20191211), IntVal(20191212));
    test_bitmap_intersect<BigIntVal, int64_t>(ctx, BigIntVal(20191211), BigIntVal(20191212));
    test_bitmap_intersect<LargeIntVal, __int128>(ctx, LargeIntVal(20191211), LargeIntVal(20191212));
    test_bitmap_intersect<FloatVal, float>(ctx, FloatVal(1.01), FloatVal(1.02));
    test_bitmap_intersect<DoubleVal, double>(ctx, DoubleVal(1.01), DoubleVal(1.02));

    DecimalV2Val v1;
    DecimalV2Value("1.01").to_decimal_val(&v1);
    DecimalV2Val v2;
    DecimalV2Value("1.02").to_decimal_val(&v2);
    test_bitmap_intersect<DecimalV2Val, DecimalV2Value>(ctx, v1, v2);

    DateTimeVal datetime1;
    DateTimeValue date_time_value;
    date_time_value.from_date_int64(19880201);
    date_time_value.to_datetime_val(&datetime1);
    DateTimeVal datetime2;
    date_time_value.from_date_int64(19880202);
    date_time_value.to_datetime_val(&datetime2);
    test_bitmap_intersect<DateTimeVal, DateTimeValue>(ctx, datetime1, datetime2);

    test_bitmap_intersect<StringVal, StringValue>(ctx, StringVal("20191211"),
                                                  StringVal("20191212"));
}

TEST_F(BitmapFunctionsTest, bitmap_or) {
    BitmapValue bitmap1({1024, 1, 2019});
    BitmapValue bitmap2({33, 44, 55});

    StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

    StringVal bitmap_str = BitmapFunctions::bitmap_or(ctx, bitmap_src, bitmap_dst);
    BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);

    BigIntVal expected(6);
    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, bitmap_and) {
    BitmapValue bitmap1({1024, 1, 2019});
    BitmapValue bitmap2({33, 44, 2019});

    StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

    StringVal bitmap_str = BitmapFunctions::bitmap_and(ctx, bitmap_src, bitmap_dst);
    BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);

    BigIntVal expected(1);
    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, bitmap_not) {
    // result is bitmap
    BitmapValue bitmap1({1024, 1, 2019});
    BitmapValue bitmap2({33, 44, 2019});

    StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

    StringVal bitmap_str = BitmapFunctions::bitmap_not(ctx, bitmap_src, bitmap_dst);
    BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);
    BigIntVal expected(2);
    ASSERT_EQ(expected, result);

    // result is single
    bitmap1 = BitmapValue({1024, 1, 2019});
    bitmap2 = BitmapValue({33, 1024, 2019});

    bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

    bitmap_str = BitmapFunctions::bitmap_not(ctx, bitmap_src, bitmap_dst);
    result = BitmapFunctions::bitmap_count(ctx, bitmap_str);
    expected = BigIntVal(1);
    ASSERT_EQ(expected, result);

    // result is empty
    bitmap1 = BitmapValue({1024, 1, 2019});
    bitmap2 = BitmapValue({1, 1024, 2019});

    bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

    bitmap_str = BitmapFunctions::bitmap_not(ctx, bitmap_src, bitmap_dst);
    result = BitmapFunctions::bitmap_count(ctx, bitmap_str);
    expected = BigIntVal(0);
    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, bitmap_contains) {
    BitmapValue bitmap({4, 5});
    StringVal bitmap_str = convert_bitmap_to_string(ctx, bitmap);
    BooleanVal result = BitmapFunctions::bitmap_contains(ctx, bitmap_str, 5);
    BooleanVal expected(true);
    ASSERT_EQ(expected, result);

    BooleanVal result2 = BitmapFunctions::bitmap_contains(ctx, bitmap_str, 10);
    BooleanVal expected2(false);
    ASSERT_EQ(expected2, result2);
}

TEST_F(BitmapFunctionsTest, bitmap_has_any) {
    BitmapValue bitmap1({4, 5});
    BitmapValue bitmap2({4, 5});

    StringVal lhs = convert_bitmap_to_string(ctx, bitmap1);
    StringVal rhs = convert_bitmap_to_string(ctx, bitmap2);
    BooleanVal result = BitmapFunctions::bitmap_has_any(ctx, lhs, rhs);
    BooleanVal expected(true);
    ASSERT_EQ(expected, result);

    BitmapValue bitmap3(10);
    StringVal r3 = convert_bitmap_to_string(ctx, bitmap3);
    BooleanVal result1 = BitmapFunctions::bitmap_has_any(ctx, lhs, r3);
    BooleanVal expected2(false);
    ASSERT_EQ(expected2, result1);
}

TEST_F(BitmapFunctionsTest, bitmap_from_string) {
    FunctionUtils utils;
    {
        StringVal val = StringVal("0,1,2");
        auto bitmap_str = BitmapFunctions::bitmap_from_string(utils.get_fn_ctx(), val);
        ASSERT_FALSE(bitmap_str.is_null);
        BitmapValue bitmap((const char*)bitmap_str.ptr);
        ASSERT_TRUE(bitmap.contains(0));
        ASSERT_TRUE(bitmap.contains(1));
        ASSERT_TRUE(bitmap.contains(2));
    }
    {
        StringVal val = StringVal("a,b,1,2");
        auto bitmap_str = BitmapFunctions::bitmap_from_string(utils.get_fn_ctx(), val);
        ASSERT_TRUE(bitmap_str.is_null);
    }
    {
        StringVal val = StringVal("-1,1,2");
        auto bitmap_str = BitmapFunctions::bitmap_from_string(utils.get_fn_ctx(), val);
        ASSERT_TRUE(bitmap_str.is_null);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
