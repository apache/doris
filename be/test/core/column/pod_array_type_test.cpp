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

#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/value/decimalv2_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/function.h"
#include "testutil/column_helper.h"

namespace doris {

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

} // namespace doris