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

#include "runtime/raw_value.h"

#include <gtest/gtest.h>

namespace doris {

class RawValueTest : public testing::Test {};

TEST_F(RawValueTest, Compare) {
    int64_t v1;
    int64_t v2;
    v1 = -2128609280;
    v2 = 9223372036854775807;
    EXPECT_LT(RawValue::compare(&v1, &v2, TYPE_BIGINT), 0);
    EXPECT_GT(RawValue::compare(&v2, &v1, TYPE_BIGINT), 0);

    int32_t i1;
    int32_t i2;
    i1 = 2147483647;
    i2 = -2147483640;
    EXPECT_GT(RawValue::compare(&i1, &i2, TYPE_INT), 0);
    EXPECT_LT(RawValue::compare(&i2, &i1, TYPE_INT), 0);

    EXPECT_GT(RawValue::compare(&i1, &i2, TYPE_INT), 0);
    EXPECT_LT(RawValue::compare(&i2, &i1, TYPE_INT), 0);
}

} // namespace doris
