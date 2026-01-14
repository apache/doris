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

#include "runtime/decimalv2_value.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

template <typename T>
void test_for_type() {
    PaddedPODArray<T> arr;
    for (int i = 0; i < 100; ++i) {
        arr.push_back(T {});
    }
    arr.resize(1000);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(T {}, arr[i]);
    }
}

TEST(PodArrayTypeTest, test) {
    test_for_type<int8_t>();
    test_for_type<int16_t>();
    test_for_type<int32_t>();
    test_for_type<int64_t>();
    test_for_type<uint8_t>();
    test_for_type<uint16_t>();
    test_for_type<uint32_t>();
    test_for_type<uint64_t>();
    test_for_type<float>();
    test_for_type<double>();
    test_for_type<VecDateTimeValue>();
    test_for_type<DateV2Value<DateV2ValueType>>();
    test_for_type<DateV2Value<DateTimeV2ValueType>>();
    test_for_type<DecimalV2Value>();
    test_for_type<Decimal32>();
    test_for_type<Decimal64>();
    test_for_type<Decimal128V2>();
    test_for_type<Decimal128V3>();
    test_for_type<Decimal256>();
}

} // namespace doris::vectorized