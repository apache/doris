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

} // namespace doris
