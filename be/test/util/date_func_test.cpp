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

#include "util/date_func.h"
#include <gtest/gtest.h>
#include <iostream>

namespace doris {

class DateFuncTest : public testing::Test {
public:
    DateFuncTest() {}
    virtual ~DateFuncTest() {}
};

TEST_F(DateFuncTest, convert_string_to_int) {
    uint64_t result1 = timestamp_from_datetime(std::string("2021-06-08 15:21:18"));
    ASSERT_EQ(20210608152118, result1);

    uint64_t abnormal_result1 = timestamp_from_datetime(std::string("2021-22-08 15:21:18"));
    ASSERT_EQ(14000101000000, abnormal_result1);

    uint24_t result2 = timestamp_from_date(std::string("2021-09-08"));
    ASSERT_EQ(std::string("2021-09-08"), result2.to_string());

    uint24_t abnormal_result2 = timestamp_from_date(std::string("2021-25-08"));
    ASSERT_EQ(std::string("1400-01-01"), abnormal_result2.to_string());
}

TEST_F(DateFuncTest, convert_double_to_string) {
    char buffer[100];
    int len = time_to_buffer_from_double(211331.21212, buffer);
    ASSERT_EQ(std::string("58:42:11"), std::string(buffer, len));

    len = time_to_buffer_from_double(-331.21212, buffer);
    ASSERT_EQ(std::string("-00:05:31"), std::string(buffer, len));

    len = time_to_buffer_from_double(-331, buffer);
    ASSERT_EQ(std::string("-00:05:31"), std::string(buffer, len));

    len = time_to_buffer_from_double(331, buffer);
    ASSERT_EQ(std::string("00:05:31"), std::string(buffer, len));

    len = time_to_buffer_from_double(0, buffer);
    ASSERT_EQ(std::string("00:00:00"), std::string(buffer, len));

    len = time_to_buffer_from_double(0.3131, buffer);
    ASSERT_EQ(std::string("00:00:00"), std::string(buffer, len));

    len = time_to_buffer_from_double(13020399, buffer);
    ASSERT_EQ(std::string("838:59:59"), std::string(buffer, len));

    len = time_to_buffer_from_double(-3020399, buffer);
    ASSERT_EQ(std::string("-838:59:59"), std::string(buffer, len));

    len = time_to_buffer_from_double(13020399, buffer);
    ASSERT_EQ(std::string("838:59:59"), std::string(buffer, len));

    len = time_to_buffer_from_double(-13020399, buffer);
    ASSERT_EQ(std::string("-838:59:59"), std::string(buffer, len));

}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

