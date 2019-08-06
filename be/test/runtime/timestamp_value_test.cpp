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

#include "runtime/timestamp_value.h"

#include <string>
#include <gtest/gtest.h>
#include "common/logging.h"
#include "util/logging.h"

namespace doris {

class TimestampValueTest : public testing::Test {
public:
    TimestampValueTest() {
        TimezoneDatabase::init();
    }

protected:
    virtual void SetUp() {
    }
    virtual void TearDown() {
    }
};

// Assert size
TEST_F(TimestampValueTest, struct_size) {
    ASSERT_EQ(8, sizeof(TimestampValue));
}

TEST_F(TimestampValueTest, construct) {
    DateTimeValue value1;
    value1.from_date_int64(20190806163857);
    TimestampValue ts;
    ts.from_date_time_value(value1, std::string("Asia/Shanghai"));
    ASSERT_EQ(1565080737, ts.val / 1000);
}

TEST_F(TimestampValueTest, timezone) {
    ASSERT_TRUE(TimezoneDatabase::find_timezone("Error timezone") == nullptr);
}
}
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
