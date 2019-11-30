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
#include "exprs/bitmap_function.h"
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

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
