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

#include "util/mysql_row_buffer.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <string.h>

#include <string>

#include "gtest/gtest_pred_impl.h"

namespace doris {

TEST(MysqlRowBufferTest, basic) {
    MysqlRowBuffer mrb;

    std::string s("test");
    mrb.push_tinyint(5);
    mrb.push_smallint(120);
    mrb.push_int(-30000);
    mrb.push_bigint(900000);
    mrb.push_unsigned_bigint(90000000);
    mrb.push_float(56.45);
    mrb.push_double(10.12);
    mrb.push_string(s.c_str(), 4);
    mrb.push_null();

    const char* buf = mrb.buf();

    // mem: size-data-size-data
    // 1-'5'-3-'120'-6-'-30000'-6-'900000'-8-'90000000'-5-'56.45'-5-'10.12'-4-'test'-251
    // 1b-1b-1b-3b--1b-----6b--1b----6b---1b-----8b----1b---5b---1b---5b---1b---4b---1b
    // 0  1  2  3   6      7   13    14   20     21    29   30   35   36   41   42   46
    EXPECT_EQ(47, mrb.length());

    EXPECT_EQ(1, *((int8_t*)(buf)));
    EXPECT_EQ(0, strncmp(buf + 1, "5", 1));

    EXPECT_EQ(3, *((int8_t*)(buf + 2)));
    EXPECT_EQ(0, strncmp(buf + 3, "120", 3));

    EXPECT_EQ(6, *((int8_t*)(buf + 6)));
    EXPECT_EQ(0, strncmp(buf + 7, "-30000", 6));

    EXPECT_EQ(6, *((int8_t*)(buf + 13)));
    EXPECT_EQ(0, strncmp(buf + 14, "900000", 6));

    EXPECT_EQ(8, *((int8_t*)(buf + 20)));
    EXPECT_EQ(0, strncmp(buf + 21, "90000000", 8));

    EXPECT_EQ(5, *((int8_t*)(buf + 29)));
    EXPECT_EQ(0, strncmp(buf + 30, "56.45", 5));

    EXPECT_EQ(5, *((int8_t*)(buf + 35)));
    EXPECT_EQ(0, strncmp(buf + 36, "10.12", 5));

    EXPECT_EQ(4, *((int8_t*)(buf + 41)));
    EXPECT_EQ(0, strncmp(buf + 42, "test", 4));

    EXPECT_EQ(251, *((uint8_t*)(buf + 46)));
}

TEST(MysqlRowBufferTest, dynamic_mode) {
    MysqlRowBuffer mrb;

    mrb.open_dynamic_mode();

    std::string s("test");
    mrb.push_tinyint(5);
    mrb.push_smallint(120);
    mrb.push_int(-30000);
    mrb.push_bigint(900000);
    mrb.push_unsigned_bigint(90000000);
    mrb.push_float(56.45);
    mrb.push_double(10.12);
    mrb.push_string(s.c_str(), 4);
    mrb.push_null();

    mrb.close_dynamic_mode();

    const char* buf = mrb.buf();

    // mem: size-data-data
    // 254-48-'5'-'120'-'-30000'-'900000'-'90000000'-'56.45'-'10.12'-'test'-''
    // 1b--8b-1b----3b-----6b-------6b--------8b-------5b------5b------4b---0b
    // 0   1  9     10     13       19        25       33      38      43   47
    EXPECT_EQ(47, mrb.length());

    EXPECT_EQ(254, *((uint8_t*)(buf)));
    EXPECT_EQ(38, *((int64_t*)(buf + 1)));

    EXPECT_EQ(0, strncmp(buf + 9, "5", 1));
    EXPECT_EQ(0, strncmp(buf + 10, "120", 3));
    EXPECT_EQ(0, strncmp(buf + 13, "-30000", 6));
    EXPECT_EQ(0, strncmp(buf + 19, "900000", 6));
    EXPECT_EQ(0, strncmp(buf + 25, "90000000", 8));
    EXPECT_EQ(0, strncmp(buf + 33, "56.45", 5));
    EXPECT_EQ(0, strncmp(buf + 38, "10.12", 5));
    EXPECT_EQ(0, strncmp(buf + 43, "test", 4));
}

} // namespace doris
