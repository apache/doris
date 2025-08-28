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
#include "runtime/primitive_type.h"
#include "vec/core/types.h"

namespace doris::vectorized {
//
//TEST(VNumberTraits, ResultOfAdditionMultiplication) {
//    EXPECT_TRUE(doris::vectorized::NumberTraits::ResultOfAdditionMultiplication<UInt8, UInt8>::Type ==
//           PrimitiveType::TYPE_INT);
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfAdditionMultiplication<UInt8, Int32>::Type,
//              PrimitiveType::TYPE_BIGINT);
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfAdditionMultiplication<UInt8, Float32>::Type,
//              PrimitiveType::TYPE_DOUBLE);
//}
//
//TEST(VNumberTraits, ResultOfSubtraction) {
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfSubtraction<UInt8, UInt8>::Type,
//              PrimitiveType::TYPE_SMALLINT);
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfSubtraction<UInt16, UInt8>::Type,
//              PrimitiveType::TYPE_INT);
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfSubtraction<UInt16, Int8>::Type,
//              PrimitiveType::TYPE_INT);
//}
//
//TEST(VNumberTraits, Others) {
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfFloatingPointDivision<UInt16, Int16>::Type,
//              PrimitiveType::TYPE_DOUBLE);
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfFloatingPointDivision<UInt32, Int16>::Type,
//              PrimitiveType::TYPE_DOUBLE);
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfIntegerDivision<UInt8, Int16>::Type,
//              PrimitiveType::TYPE_TINYINT);
//    ASSERT_EQ(doris::vectorized::NumberTraits::ResultOfModulo<UInt32, Int8>::Type,
//              PrimitiveType::TYPE_INT);
//}

} // namespace doris::vectorized
