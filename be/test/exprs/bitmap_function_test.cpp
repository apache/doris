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

    EXPECT_EQ(BitmapFunctions::bitmap_empty(ctx), result);
}

TEST_F(BitmapFunctionsTest, to_bitmap_invalid_argument) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("-1"));
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);
    EXPECT_EQ(BitmapFunctions::bitmap_empty(ctx), result);
}

TEST_F(BitmapFunctionsTest, to_bitmap_out_of_range) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("18446744073709551616"));
    StringVal result = BitmapFunctions::to_bitmap(ctx, input);
    EXPECT_EQ(BitmapFunctions::bitmap_empty(ctx), result);
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

TEST_F(BitmapFunctionsTest, bitmap_min) {
    BigIntVal result = BitmapFunctions::bitmap_min(ctx, StringVal::null());
    ASSERT_TRUE(result.is_null);

    BitmapValue bitmap1;
    StringVal empty_str = convert_bitmap_to_string(ctx, bitmap1);
    result = BitmapFunctions::bitmap_min(ctx, empty_str);
    ASSERT_TRUE(result.is_null);

    BitmapValue bitmap2 = BitmapValue(1024);
    StringVal bitmap_str = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_min(ctx, bitmap_str);
    ASSERT_EQ(BigIntVal(1024), result);

    BitmapValue bitmap3 = BitmapValue({1024, 1});
    bitmap_str = convert_bitmap_to_string(ctx, bitmap3);
    result = BitmapFunctions::bitmap_min(ctx, bitmap_str);
    ASSERT_EQ(BigIntVal(1), result);

    BitmapValue bitmap4 = BitmapValue({1024, 3, 2});
    bitmap_str = convert_bitmap_to_string(ctx, bitmap4);
    result = BitmapFunctions::bitmap_min(ctx, bitmap_str);
    ASSERT_EQ(BigIntVal(2), result);
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
    DecimalV2Value(std::string("1.01")).to_decimal_val(&v1);
    DecimalV2Val v2;
    DecimalV2Value(std::string("1.02")).to_decimal_val(&v2);
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

TEST_F(BitmapFunctionsTest, bitmap_or_variable) {
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap_empty;  //test empty

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_src4 = convert_bitmap_to_string(ctx, bitmap_empty);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, bitmap_src4};

        StringVal bitmap_result = BitmapFunctions::bitmap_or(ctx, bitmap_src1, 3, bitmap_strs);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_result);

        BigIntVal expected(7);//0,1,5,33,1024,2019,18446744073709551615
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, StringVal::null()}; //test null

        StringVal bitmap_result = BitmapFunctions::bitmap_or(ctx, bitmap_src1, 3, bitmap_strs);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_result);

        BigIntVal expected(0);
        ASSERT_EQ(expected, result);
    }
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

TEST_F(BitmapFunctionsTest, bitmap_and_variable) {
    {
        BitmapValue bitmap1({1024, 1, 0});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::min()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[2] = {bitmap_src2, bitmap_src3};

        StringVal bitmap_result = BitmapFunctions::bitmap_and(ctx, bitmap_src1, 2, bitmap_strs);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_result);

        BigIntVal expected(1);//0
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap_empty;  //test empty

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_src4 = convert_bitmap_to_string(ctx, bitmap_empty);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, bitmap_src4};

        StringVal bitmap_result = BitmapFunctions::bitmap_and(ctx, bitmap_src1, 3, bitmap_strs);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_result);

        BigIntVal expected(0);
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, StringVal::null()}; //test null

        StringVal bitmap_result = BitmapFunctions::bitmap_and(ctx, bitmap_src1, 3, bitmap_strs);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_result);

        BigIntVal expected(0);
        ASSERT_EQ(expected, result);
    }
}

TEST_F(BitmapFunctionsTest, bitmap_xor) {
    BitmapValue bitmap1({1024, 1, 2019});
    BitmapValue bitmap2({33, 44, 2019});

    StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

    StringVal bitmap_str = BitmapFunctions::bitmap_xor(ctx, bitmap_src, bitmap_dst);
    BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);

    BigIntVal expected(4);
    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, bitmap_xor_variable) {
    {
        BitmapValue bitmap1({1024, 1, 0});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::min()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[2] = {bitmap_src2, bitmap_src3};

        StringVal bitmap_result = BitmapFunctions::bitmap_xor(ctx, bitmap_src1, 2, bitmap_strs);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_result);
        
        BigIntVal expected(5); //0,1,5,1024,18446744073709551615
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap_empty;  //test empty

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_src4 = convert_bitmap_to_string(ctx, bitmap_empty);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, bitmap_src4};

        StringVal bitmap_result = BitmapFunctions::bitmap_xor(ctx, bitmap_src1, 3, bitmap_strs);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_result);
        
        BigIntVal expected(6); //0,1,5,1024,2019,18446744073709551615
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, StringVal::null()}; //test null

        StringVal bitmap_result = BitmapFunctions::bitmap_xor(ctx, bitmap_src1, 3, bitmap_strs);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_result);

        BigIntVal expected(0);
        ASSERT_EQ(expected, result);
    }
}

TEST_F(BitmapFunctionsTest, bitmap_xor_count) {
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({3, 4, 5});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(4);
        ASSERT_EQ(expected.val, result.val);
    }
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({1, 2, 3});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(0);
        ASSERT_EQ(expected.val, result.val);
    }
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({4, 5, 6});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(6);
        ASSERT_EQ(expected.val, result.val);
    }
}

TEST_F(BitmapFunctionsTest, bitmap_xor_count_variable) {
    {
        BitmapValue bitmap1({1024, 1, 0});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::min()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[2] = {bitmap_src2, bitmap_src3};

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src1, 2, bitmap_strs);
        
        BigIntVal expected(5); //0,1,5,1024,18446744073709551615
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap_empty;  //test empty

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_src4 = convert_bitmap_to_string(ctx, bitmap_empty);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, bitmap_src4};

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src1, 3, bitmap_strs);
        
        BigIntVal expected(6); //0,1,5,1024,2019,18446744073709551615
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, StringVal::null()}; //test null

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src1, 3, bitmap_strs);
        ASSERT_EQ(BigIntVal::null(), result);
    }
}

TEST_F(BitmapFunctionsTest, bitmap_xor_count_64) {
    {
        BitmapValue bitmap1({14001230000000000501ull, 2, 1404560000000000503ull});
        BitmapValue bitmap2({1404560000000000503ull, 4, 14111000000000000505ull});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(4);
        ASSERT_EQ(expected.val, result.val);
    }
    {
        BitmapValue bitmap1({14123400000000000501ull, 2, 1498760000000000503ull});
        BitmapValue bitmap2({14123400000000000501ull, 2, 1498760000000000503ull});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(0);
        ASSERT_EQ(expected.val, result.val);
    }
    {
        BitmapValue bitmap1({15000000000000000501ull, 2, 1200000000000000503ull});
        BitmapValue bitmap2({13000000000000000504ull, 5, 1160000000000000506ull});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_xor_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(6);
        ASSERT_EQ(expected.val, result.val);
    }
}

TEST_F(BitmapFunctionsTest, bitmap_and_count) {
    BitmapValue bitmap1({0, 1, 2});
    BitmapValue bitmap2;
    StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    BigIntVal result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(0, result.val);

    result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, StringVal::null());
    ASSERT_EQ(0, result.val);

    bitmap1 = BitmapValue({0, 1, 2, std::numeric_limits<uint64_t>::min()});
    bitmap2 = BitmapValue({0, 1, 2, std::numeric_limits<uint64_t>::max()});
    bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(3, result.val);

    bitmap1 = BitmapValue({1, 2, 3});
    bitmap2 = BitmapValue({3, 4, 5});
    bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(1, result.val);
}

TEST_F(BitmapFunctionsTest, bitmap_and_count_variable) {
    {
        BitmapValue bitmap1({1024, 1, 0});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::min()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[2] = {bitmap_src2, bitmap_src3};

        BigIntVal result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, 2, bitmap_strs);
        BigIntVal expected(1);//0
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap_empty;  //test empty

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_src4 = convert_bitmap_to_string(ctx, bitmap_empty);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, bitmap_src4};

        BigIntVal result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, 3, bitmap_strs);
        BigIntVal expected(0);
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, StringVal::null()}; //test null

        BigIntVal result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, 3, bitmap_strs);
        ASSERT_EQ(BigIntVal::null(), result);
    }
}

TEST_F(BitmapFunctionsTest, bitmap_and_count_64) {
    BitmapValue bitmap1({14333000000000000501ull, 2, 1454100000000000503ull});
    BitmapValue bitmap2;
    StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    BigIntVal result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(0, result.val);

    result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, StringVal::null());
    ASSERT_EQ(0, result.val);

    bitmap1 = BitmapValue({11598000000000000501ull, 2, 1923400000000000503ull,
                           std::numeric_limits<uint64_t>::min()});
    bitmap2 = BitmapValue({11598000000000000501ull, 2, 1923400000000000503ull,
                           std::numeric_limits<uint64_t>::max()});
    bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(3, result.val);

    bitmap1 = BitmapValue({15555500000000000501ull, 2, 1400000000000000503ull});
    bitmap2 = BitmapValue({1400000000000000503ull, 5, 1400324000000000506ull});
    bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_and_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(1, result.val);
}

TEST_F(BitmapFunctionsTest, bitmap_or_count) {
    BitmapValue bitmap1({0, 1, 2});
    BitmapValue bitmap2;
    StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    BigIntVal result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(3, result.val);

    result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, StringVal::null());
    ASSERT_EQ(0, result.val);

    bitmap1 = BitmapValue({0, 1, 2, std::numeric_limits<uint64_t>::min()});
    bitmap2 = BitmapValue({0, 1, 2, std::numeric_limits<uint64_t>::max()});
    bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(4, result.val);

    bitmap1 = BitmapValue({1, 2, 3});
    bitmap2 = BitmapValue({3, 4, 5});
    bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(5, result.val);
}

TEST_F(BitmapFunctionsTest, bitmap_or_count_variable) {
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});
        BitmapValue bitmap_empty;  //test empty

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_src4 = convert_bitmap_to_string(ctx, bitmap_empty);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, bitmap_src4};

        BigIntVal result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, 3, bitmap_strs);

        BigIntVal expected(7);//0,1,5,33,1024,2019,18446744073709551615
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1024, 1, 2019});
        BitmapValue bitmap2({0, 33, std::numeric_limits<uint64_t>::min()});
        BitmapValue bitmap3({33, 5, std::numeric_limits<uint64_t>::max()});

        StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
        StringVal bitmap_src3 = convert_bitmap_to_string(ctx, bitmap3);
        StringVal bitmap_strs[3] = {bitmap_src2, bitmap_src3, StringVal::null()}; //test null

        BigIntVal result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, 3, bitmap_strs);
        ASSERT_EQ(BigIntVal::null(), result);
    }
}

TEST_F(BitmapFunctionsTest, bitmap_or_count_64) {
    BitmapValue bitmap1({14087600000000000501ull, 2, 1234500000000000503ull});
    BitmapValue bitmap2;
    StringVal bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    StringVal bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    BigIntVal result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(3, result.val);

    result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, StringVal::null());
    ASSERT_EQ(0, result.val);

    bitmap1 = BitmapValue({11870000000000000501ull, 2, 1378900000000000503ull,
                           std::numeric_limits<uint64_t>::min()});
    bitmap2 = BitmapValue({11870000000000000501ull, 2, 1378900000000000503ull,
                           std::numeric_limits<uint64_t>::max()});
    bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(5, result.val);

    bitmap1 = BitmapValue({17870000000000000501ull, 2, 1400000000000000503ull});
    bitmap2 = BitmapValue({1400000000000000503ull, 5, 1678900000000000503ull});
    bitmap_src1 = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_src2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_or_count(ctx, bitmap_src1, bitmap_src2);
    ASSERT_EQ(5, result.val);
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

    bitmap1 = BitmapValue(1);
    bitmap2 = BitmapValue({2, 1});

    bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);
    bitmap_str = BitmapFunctions::bitmap_not(ctx, bitmap_src, bitmap_dst);
    result = BitmapFunctions::bitmap_count(ctx, bitmap_str);
    ASSERT_EQ(expected, result);
}

TEST_F(BitmapFunctionsTest, bitmap_and_not) {
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({3, 4, 5});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        StringVal bitmap_str = BitmapFunctions::bitmap_and_not(ctx, bitmap_src, bitmap_dst);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);

        BigIntVal expected(2);
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({3, 2, 1});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        StringVal bitmap_str = BitmapFunctions::bitmap_and_not(ctx, bitmap_src, bitmap_dst);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);

        BigIntVal expected(0);
        ASSERT_EQ(expected, result);
    }
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({998, 999, 1000});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        StringVal bitmap_str = BitmapFunctions::bitmap_and_not(ctx, bitmap_src, bitmap_dst);
        BigIntVal result = BitmapFunctions::bitmap_count(ctx, bitmap_str);

        BigIntVal expected(3);
        ASSERT_EQ(expected, result);
    }
}

TEST_F(BitmapFunctionsTest, bitmap_and_not_count) {
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({3, 4, 5});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_and_not_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(2);
        ASSERT_EQ(expected.val, result.val);
    }
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({3, 2, 1});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_and_not_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(0);
        ASSERT_EQ(expected.val, result.val);
    }
    {
        BitmapValue bitmap1({1, 2, 3});
        BitmapValue bitmap2({998, 999, 1000});

        StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
        StringVal bitmap_dst = convert_bitmap_to_string(ctx, bitmap2);

        BigIntVal result = BitmapFunctions::bitmap_and_not_count(ctx, bitmap_src, bitmap_dst);

        BigIntVal expected(3);
        ASSERT_EQ(expected.val, result.val);
    }
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

TEST_F(BitmapFunctionsTest, bitmap_has_all) {
    BitmapValue bitmap1(
            {1, 4, 5, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::min()});
    BitmapValue bitmap2(
            {4, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::min()});
    StringVal string_val1 = convert_bitmap_to_string(ctx, bitmap1);
    StringVal string_val2 = convert_bitmap_to_string(ctx, bitmap2);
    BooleanVal result = BitmapFunctions::bitmap_has_all(ctx, string_val1, string_val2);
    ASSERT_EQ(BooleanVal {true}, result);

    bitmap1 = BitmapValue({0, 1, 2});
    bitmap2 = BitmapValue({0, 1, 2, std::numeric_limits<uint64_t>::max()});
    string_val1 = convert_bitmap_to_string(ctx, bitmap1);
    string_val2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_has_all(ctx, string_val1, string_val2);
    ASSERT_EQ(BooleanVal {false}, result);

    bitmap1 = BitmapValue();
    bitmap2 = BitmapValue({0, 1, 2});
    string_val1 = convert_bitmap_to_string(ctx, bitmap1);
    string_val2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_has_all(ctx, string_val1, string_val2);
    ASSERT_EQ(BooleanVal {false}, result);

    bitmap1 = BitmapValue();
    bitmap2 = BitmapValue();
    string_val1 = convert_bitmap_to_string(ctx, bitmap1);
    string_val2 = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_has_all(ctx, string_val1, string_val2);
    ASSERT_EQ(BooleanVal {true}, result);

    bitmap1 = BitmapValue();
    string_val1 = convert_bitmap_to_string(ctx, bitmap1);
    result = BitmapFunctions::bitmap_has_all(ctx, string_val1, StringVal::null());
    ASSERT_TRUE(result.is_null);
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

TEST_F(BitmapFunctionsTest, bitmap_max) {
    BigIntVal result = BitmapFunctions::bitmap_max(ctx, StringVal::null());
    ASSERT_TRUE(result.is_null);

    BitmapValue bitmap1;
    StringVal empty_str = convert_bitmap_to_string(ctx, bitmap1);
    result = BitmapFunctions::bitmap_max(ctx, empty_str);
    ASSERT_TRUE(result.is_null);

    BitmapValue bitmap2 = BitmapValue(1024);
    StringVal bitmap_str = convert_bitmap_to_string(ctx, bitmap2);
    result = BitmapFunctions::bitmap_max(ctx, bitmap_str);
    ASSERT_EQ(BigIntVal(1024), result);

    BitmapValue bitmap3 = BitmapValue({1024, 1});
    bitmap_str = convert_bitmap_to_string(ctx, bitmap3);
    result = BitmapFunctions::bitmap_max(ctx, bitmap_str);
    ASSERT_EQ(BigIntVal(1024), result);

    BitmapValue bitmap4 = BitmapValue({1024, 3, 2});
    bitmap_str = convert_bitmap_to_string(ctx, bitmap4);
    result = BitmapFunctions::bitmap_max(ctx, bitmap_str);
    ASSERT_EQ(BigIntVal(1024), result);
}

TEST_F(BitmapFunctionsTest, bitmap_subset_in_range) {
    // null
    StringVal res = BitmapFunctions::bitmap_subset_in_range(ctx, StringVal::null(), BigIntVal(1),
                                                            BigIntVal(3));
    ASSERT_TRUE(res.is_null);

    // empty
    BitmapValue bitmap0;
    StringVal empty_str = convert_bitmap_to_string(ctx, bitmap0);
    res = BitmapFunctions::bitmap_subset_in_range(ctx, empty_str, BigIntVal(1), BigIntVal(3));
    BigIntVal result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(0), result);

    // normal
    BitmapValue bitmap1({0,  1,  2,  3,  4,  5,  6,  7,  8,  9,   10,  11, 12,
                         13, 14, 15, 16, 17, 18, 19, 20, 21, 22,  208, 23, 24,
                         25, 26, 27, 28, 29, 30, 31, 32, 33, 100, 200, 500});

    StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(30), BigIntVal(200));
    result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(5), result);

    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(0), BigIntVal(1));
    result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(1), result);

    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(11), BigIntVal(15));
    result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(4), result);

    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(11),
                                                  DecimalV2Value::MAX_INT64);
    result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(27), result);

    // innormal
    // start >= end
    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(30), BigIntVal(20));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(20), BigIntVal(20));
    ASSERT_TRUE(res.is_null);

    // negative range
    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(-10), BigIntVal(20));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(10), BigIntVal(-20));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(-10), BigIntVal(-20));
    ASSERT_TRUE(res.is_null);

    // null range
    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal::null(),
                                                  BigIntVal(20));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal(10),
                                                  BigIntVal::null());
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_in_range(ctx, bitmap_src, BigIntVal::null(),
                                                  BigIntVal::null());
    ASSERT_TRUE(res.is_null);
}

TEST_F(BitmapFunctionsTest, sub_bitmap) {
    // normal
    BitmapValue bitmap1({0,  1,  2,  3,  4,  5,  6,  7,  8,   9,   10, 11, 12,
                         13, 14, 15, 16, 17, 18, 19, 20, 21,  22,  23, 24, 25,
                         26, 27, 28, 29, 30, 31, 32, 33, 100, 200, 500});
    StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);

    StringVal res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(30), BigIntVal(6));
    BitmapValue bitmap2({30, 31, 32, 33, 100, 200});
    ASSERT_EQ(res, convert_bitmap_to_string(ctx, bitmap2));

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(30), BigIntVal(100));
    BitmapValue bitmap3({30, 31, 32, 33, 100, 200, 500});
    ASSERT_EQ(res, convert_bitmap_to_string(ctx, bitmap3));

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(30), BigIntVal(INT64_MAX));
    BitmapValue bitmap4({30, 31, 32, 33, 100, 200, 500});
    ASSERT_EQ(res, convert_bitmap_to_string(ctx, bitmap4));

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(0), BigIntVal(2));
    BitmapValue bitmap5({0, 1});
    ASSERT_EQ(res, convert_bitmap_to_string(ctx, bitmap5));

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(-1), BigIntVal(2));
    BitmapValue bitmap6(500);
    ASSERT_EQ(res, convert_bitmap_to_string(ctx, bitmap6));

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(-7), BigIntVal(6));
    BitmapValue bitmap7({30, 31, 32, 33, 100, 200});
    ASSERT_EQ(res, convert_bitmap_to_string(ctx, bitmap7));

    // null
    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(0), BigIntVal(0));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(100), BigIntVal(6));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(-100), BigIntVal(6));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(30), BigIntVal(INT64_MIN));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::sub_bitmap(ctx, StringVal::null(), BigIntVal(1), BigIntVal(3));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal::null(), BigIntVal(20));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::sub_bitmap(ctx, bitmap_src, BigIntVal(10), BigIntVal::null());
    ASSERT_TRUE(res.is_null);

    // empty
    BitmapValue bitmap0;
    StringVal empty_str = convert_bitmap_to_string(ctx, bitmap0);
    res = BitmapFunctions::sub_bitmap(ctx, empty_str, BigIntVal(0), BigIntVal(3));
    ASSERT_TRUE(res.is_null);
}

TEST_F(BitmapFunctionsTest, bitmap_subset_limit) {
    // null
    StringVal res = BitmapFunctions::bitmap_subset_limit(ctx, StringVal::null(), BigIntVal(1),
                                                         BigIntVal(3));
    ASSERT_TRUE(res.is_null);

    // empty
    BitmapValue bitmap0;
    StringVal empty_str = convert_bitmap_to_string(ctx, bitmap0);
    res = BitmapFunctions::bitmap_subset_limit(ctx, empty_str, BigIntVal(10), BigIntVal(20));
    BigIntVal result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(0), result);

    // normal
    BitmapValue bitmap1({0,  1,  2,  3,  4,  5,  6,  7,  45, 47, 49,  43,  8,  9,
                         10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,  21,  22, 23,
                         24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 100, 200, 500});

    StringVal bitmap_src = convert_bitmap_to_string(ctx, bitmap1);
    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal(4), BigIntVal(10));
    result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(10), result);

    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal(0), BigIntVal(1));
    result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(1), result);

    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal(35), BigIntVal(10));
    result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(7), result);

    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal(31),
                                               DecimalV2Value::MAX_INT64);
    result = BitmapFunctions::bitmap_count(ctx, res);
    ASSERT_EQ(BigIntVal(10), result);

    // abnormal
    // negative range_start and cardinality_limit
    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal(-10), BigIntVal(20));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal(10), BigIntVal(-20));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal(-10), BigIntVal(-20));
    ASSERT_TRUE(res.is_null);

    // null range
    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal::null(), BigIntVal(20));
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal(10), BigIntVal::null());
    ASSERT_TRUE(res.is_null);

    res = BitmapFunctions::bitmap_subset_limit(ctx, bitmap_src, BigIntVal::null(),
                                               BigIntVal::null());
    ASSERT_TRUE(res.is_null);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
