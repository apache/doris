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

#include "vec/data_types/number_traits.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/core/types.h"

namespace doris::vectorized {

static std::string getTypeString(UInt8) {
    return "UInt8";
}
static std::string getTypeString(UInt32) {
    return "UInt32";
}
static std::string getTypeString(UInt64) {
    return "UInt64";
}
static std::string getTypeString(Int8) {
    return "Int8";
}
static std::string getTypeString(Int16) {
    return "Int16";
}
static std::string getTypeString(Int32) {
    return "Int32";
}
static std::string getTypeString(Int64) {
    return "Int64";
}
static std::string getTypeString(Float32) {
    return "Float32";
}
static std::string getTypeString(Float64) {
    return "Float64";
}
void test() {
    std::cout << getTypeString(UInt8(0)) << " " << getTypeString(UInt16(0)) << " "
              << getTypeString(UInt32(0)) << " " << getTypeString(UInt64(0)) << " "
              << getTypeString(Float32(0)) << " ";
}

TEST(VNumberTraits, ResultOfAdditionMultiplication) {
    ASSERT_EQ(getTypeString(doris::vectorized::NumberTraits::ResultOfAdditionMultiplication<
                            UInt8, UInt8>::Type()),
              "Int32");
    ASSERT_EQ(getTypeString(doris::vectorized::NumberTraits::ResultOfAdditionMultiplication<
                            UInt8, Int32>::Type()),
              "Int64");
    ASSERT_EQ(getTypeString(doris::vectorized::NumberTraits::ResultOfAdditionMultiplication<
                            UInt8, Float32>::Type()),
              "Float64");
}

TEST(VNumberTraits, ResultOfSubtraction) {
    ASSERT_EQ(getTypeString(
                      doris::vectorized::NumberTraits::ResultOfSubtraction<UInt8, UInt8>::Type()),
              "Int16");
    ASSERT_EQ(getTypeString(
                      doris::vectorized::NumberTraits::ResultOfSubtraction<UInt16, UInt8>::Type()),
              "Int32");
    ASSERT_EQ(getTypeString(
                      doris::vectorized::NumberTraits::ResultOfSubtraction<UInt16, Int8>::Type()),
              "Int32");
}

TEST(VNumberTraits, Others) {
    ASSERT_EQ(getTypeString(doris::vectorized::NumberTraits::ResultOfFloatingPointDivision<
                            UInt16, Int16>::Type()),
              "Float64");
    ASSERT_EQ(getTypeString(doris::vectorized::NumberTraits::ResultOfFloatingPointDivision<
                            UInt32, Int16>::Type()),
              "Float64");
    ASSERT_EQ(
            getTypeString(
                    doris::vectorized::NumberTraits::ResultOfIntegerDivision<UInt8, Int16>::Type()),
            "Int8");
    ASSERT_EQ(getTypeString(doris::vectorized::NumberTraits::ResultOfModulo<UInt32, Int8>::Type()),
              "Int32");
}

} // namespace doris::vectorized
