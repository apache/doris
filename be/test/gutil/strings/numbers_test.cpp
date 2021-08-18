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

#include "util/counts.h"

#include <gtest/gtest.h>
#include "gutil/strings/numbers.h"

namespace doris {

class NumbersTest : public testing::Test {};

TEST_F(NumbersTest, test_fast_float_to_buffer) {
    char buffer1[100];
    char buffer2[100];
char* DoubleToBuffer(double i, char* buffer);
char* FloatToBuffer(float i, char* buffer);

int FastDoubleToBuffer(double i, char* buffer);
int FastFloatToBuffer(float i, char* buffer);
    EXPECT_EQ(1, 1);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

