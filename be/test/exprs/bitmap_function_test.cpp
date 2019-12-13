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

#include "exprs/aggregate_functions.h"
#include "exprs/anyval_util.h"
#include "exprs/bitmap_function.cpp"
#include <iostream>
#include <string>
#include "testutil/function_utils.h"
#include <util/bitmap.h>
#include "util/logging.h"

#include <gtest/gtest.h>

namespace doris {

StringVal convert_bitmap_to_string(FunctionContext* ctx, RoaringBitmap& bitmap) {
    std::string buf;
    buf.resize(bitmap.size());
    bitmap.serialize((char*)buf.c_str());
    return AnyValUtil::from_string_temp(ctx, buf);
}

template<typename T>
StringVal convert_bitmap_intersect_to_string(FunctionContext* ctx, BitmapIntersect<T>& intersect) {
    std::string buf;
    buf.resize(intersect.size());
    intersect.serialize((char*)buf.c_str());
    return AnyValUtil::from_string_temp(ctx, buf);
}

class BitmapFunctionsTest : public testing::Test {
public:
    BitmapFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() {
        delete utils;
    }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(BitmapFunctionsTest, bitmap_empty) {
    StringVal result = BitmapFunctions::bitmap_empty(ctx);

    RoaringBitmap bitmap;
    StringVal expected = convert_bitmap_to_string(ctx, bitmap);

    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, to_bitmap) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("1024"));
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);

    RoaringBitmap bitmap(1024);
    StringVal expected = convert_bitmap_to_string(ctx, bitmap);

    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, to_bitmap_null) {
    StringVal input = StringVal::null();
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);

    RoaringBitmap bitmap;
    StringVal expected = convert_bitmap_to_string(ctx, bitmap);

    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, to_bitmap_invalid_argument) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("xxxxxx"));
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);

    StringVal expected = StringVal::null();

    ASSERT_EQ(expected, result);
    ASSERT_TRUE(ctx->has_error());

    std::string error_msg("The to_bitmap function argument: xxxxxx type isn't integer family or exceed unsigned integer max value 4294967295");
    ASSERT_EQ(error_msg, ctx->error_msg());
}

TEST_F(BitmapFunctionsTest, to_bitmap_out_of_range) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("4294967296"));
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);

    StringVal expected = StringVal::null();
    ASSERT_EQ(expected, result);

    ASSERT_TRUE(ctx->has_error());

    std::string error_msg("The to_bitmap function argument: 4294967296 type isn't integer family or exceed unsigned integer max value 4294967295");
    ASSERT_EQ(error_msg, ctx->error_msg());
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

TEST_F(BitmapFunctionsTest, bitmap_union) {
    StringVal dst;
    BitmapFunctions::bitmap_init(ctx, &dst);

    RoaringBitmap bitmap1(1024);
    StringVal src1 = convert_bitmap_to_string(ctx, bitmap1);
    BitmapFunctions::bitmap_union(ctx, src1, &dst);

    RoaringBitmap bitmap2;
    StringVal src2 = convert_bitmap_to_string(ctx, bitmap1);
    BitmapFunctions::bitmap_union(ctx, src2, &dst);

    StringVal serialized = BitmapFunctions::bitmap_serialize(ctx, dst);

    BigIntVal result = BitmapFunctions::bitmap_count(ctx, serialized);
    BigIntVal expected(1);
    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, bitmap_count) {
    RoaringBitmap bitmap(1024);
    bitmap.update(1);
    bitmap.update(2019);
    StringVal bitmap_str = convert_bitmap_to_string(ctx, bitmap);

    BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);
    BigIntVal expected(3);
    ASSERT_EQ(expected, result);
}

template<typename ValType, typename ValueType>
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

    RoaringBitmap bitmap1(1024);
    bitmap1.update(1025);
    bitmap1.update(1026);
    StringVal src1 = convert_bitmap_to_string(ctx, bitmap1);
    BitmapFunctions::bitmap_intersect_update<ValueType, ValType>(
        ctx, src1, key1, 1, nullptr, &dst);

    RoaringBitmap bitmap2(1024);
    bitmap2.update(1023);
    bitmap2.update(1022);
    StringVal src2 = convert_bitmap_to_string(ctx, bitmap2);
    BitmapFunctions::bitmap_intersect_update<ValueType, ValType>(
        ctx, src2, key2, 2, nullptr, &dst);

    StringVal intersect1 = BitmapFunctions::bitmap_intersect_serialize<ValueType>(ctx, dst);

    BitmapIntersect<ValueType> intersect2;
    for(size_t i = 2; i < const_vals.size(); i++) {
        ValType* arg = reinterpret_cast<ValType*>(const_vals[i]);
        intersect2.add_key(detail::get_val<ValType, ValueType>(*arg));
    }
    intersect2.update(detail::get_val<ValType, ValueType>(key1), bitmap1);
    intersect2.update(detail::get_val<ValType, ValueType>(key2), bitmap2);
    StringVal expected = convert_bitmap_intersect_to_string(ctx, intersect2);
    ASSERT_EQ(expected, intersect1);

    BitmapIntersect<ValueType> intersect2_serde((char *)expected.ptr);
    ASSERT_EQ(1, intersect2_serde.intersect_count());

    StringVal dst2;
    BitmapFunctions::bitmap_intersect_init<ValueType, ValType>(ctx, &dst2);
    BitmapFunctions::bitmap_intersect_merge<ValueType>(ctx, intersect1, &dst2);
    BigIntVal result = BitmapFunctions::bitmap_intersect_finalize<ValueType>(ctx, dst2);
    BigIntVal expected_count(1);

    ASSERT_EQ(expected_count, result);
}

TEST_F(BitmapFunctionsTest, test_bitmap_intersect) {
    test_bitmap_intersect<TinyIntVal, int8_t>(
        ctx, TinyIntVal(101), TinyIntVal(102));
    test_bitmap_intersect<SmallIntVal, int16_t>(
        ctx, SmallIntVal((int16_t)65535), SmallIntVal((int16_t)65534));
    test_bitmap_intersect<IntVal, int32_t>(
        ctx, IntVal(20191211), IntVal(20191212));
    test_bitmap_intersect<BigIntVal, int64_t>(
        ctx, BigIntVal(20191211), BigIntVal(20191212));
    test_bitmap_intersect<LargeIntVal, __int128>(
        ctx, LargeIntVal(20191211), LargeIntVal(20191212));
    test_bitmap_intersect<FloatVal, float>(
        ctx, FloatVal(1.01), FloatVal(1.02));
    test_bitmap_intersect<DoubleVal, double>(
        ctx, DoubleVal(1.01), DoubleVal(1.02));

    DecimalV2Val v1;
    DecimalV2Value("1.01").to_decimal_val(&v1);
    DecimalV2Val v2;
    DecimalV2Value("1.02").to_decimal_val(&v2);
    test_bitmap_intersect<DecimalV2Val, DecimalV2Value>(
        ctx, v1, v2);

    DateTimeVal datatime1;
    DateTimeValue date_time_value;
    date_time_value.from_date_int64(19880201);
    date_time_value.to_datetime_val(&datatime1);
    DateTimeVal datatime2;
    date_time_value.from_date_int64(19880202);
    date_time_value.to_datetime_val(&datatime2);
    test_bitmap_intersect<DateTimeVal, DateTimeValue>(
        ctx, datatime1, datatime2);

    test_bitmap_intersect<StringVal, StringValue>(
        ctx, StringVal("20191211"), StringVal("20191212"));
}


}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
