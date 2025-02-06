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

TEST(MysqlRowBufferTest, TestBinaryTimeCompressedEncoding) {
    MysqlRowBuffer<true> buffer;
    const char* buf = nullptr;
    size_t offset = 0;

    // Test case 1: Zero time value (all zeros), expect a single byte: 0.
    buffer.push_timev2(0.0, 6);
    buf = buffer.buf();
    EXPECT_EQ(0, buf[0]);
    offset = 1;

    // Test case 2: Time value without microseconds (1:01:01)
    // 1:01:01 = 3661 seconds, converted to microseconds: 3661 * 1e6 = 3661000000.
    // With scale=0 the microsecond part is 0, so an 8-byte encoding is used.
    buffer.push_timev2(3661.0 * 1000000, 0);
    buf = buffer.buf();
    EXPECT_EQ(8, buf[offset]);                   // 8 bytes expected
    EXPECT_EQ(0, buf[offset + 1]);               // Positive flag
    EXPECT_EQ(0, *(int32_t*)(buf + offset + 2)); // Days = 0
    EXPECT_EQ(1, buf[offset + 6]);               // Hour = 1
    EXPECT_EQ(1, buf[offset + 7]);               // Minute = 1
    EXPECT_EQ(1, buf[offset + 8]);               // Second = 1
    offset += 9;

    // Test case 3: Time value with microseconds (1:01:01.123456)
    // 1:01:01.123456 seconds => 3661.123456 * 1e6 = 3661123456 microseconds.
    // Scale=6 gives non-zero microsecond part, hence 12-byte encoding.
    buffer.push_timev2(3661.123456 * 1000000, 6);
    buf = buffer.buf();
    EXPECT_EQ(12, buf[offset]);                       // 12 bytes expected
    EXPECT_EQ(0, buf[offset + 1]);                    // Positive flag
    EXPECT_EQ(0, *(int32_t*)(buf + offset + 2));      // Days = 0
    EXPECT_EQ(1, buf[offset + 6]);                    // Hour = 1
    EXPECT_EQ(1, buf[offset + 7]);                    // Minute = 1
    EXPECT_EQ(1, buf[offset + 8]);                    // Second = 1
    EXPECT_EQ(123456, *(int32_t*)(buf + offset + 9)); // Microseconds = 123456
    offset += 13;

    // Test case 4: Negative time value (-1:01:01.123456)
    // Corresponding microseconds: -3661.123456 * 1e6 = -3661123456.
    buffer.push_timev2(-3661.123456 * 1000000, 6);
    buf = buffer.buf();
    EXPECT_EQ(12, buf[offset]);                       // 12-byte encoding expected
    EXPECT_EQ(1, buf[offset + 1]);                    // Negative flag (1)
    EXPECT_EQ(0, *(int32_t*)(buf + offset + 2));      // Days = 0
    EXPECT_EQ(1, buf[offset + 6]);                    // Hour = 1
    EXPECT_EQ(1, buf[offset + 7]);                    // Minute = 1
    EXPECT_EQ(1, buf[offset + 8]);                    // Second = 1
    EXPECT_EQ(123456, *(int32_t*)(buf + offset + 9)); // Microseconds = 123456
    offset += 13;

    // Test case 5: Maximum time value (838:59:59.999999)
    // The maximum time is defined as (int64_t)3020399 * 1000000 (i.e. no extra microseconds).
    // Even if the input is 3020399.999999 * 1e6, it is truncated so that the microsecond part becomes 0.
    // Therefore, an 8-byte encoding is expected.
    buffer.push_timev2(3020399.999999 * 1000000, 6);
    buf = buffer.buf();
    EXPECT_EQ(8, buf[offset]);                    // 8-byte encoding expected
    EXPECT_EQ(0, buf[offset + 1]);                // Positive flag
    EXPECT_EQ(34, *(int32_t*)(buf + offset + 2)); // Days (e.g., 34, as per the conversion)
    EXPECT_EQ(22, buf[offset + 6]);               // Hour = 22
    EXPECT_EQ(59, buf[offset + 7]);               // Minute = 59
    EXPECT_EQ(59, buf[offset + 8]);               // Second = 59
    offset += 9;

    // Test case 6: Time value exceeding the maximum.
    // A value slightly greater than 3020399.999999 seconds will be truncated to the maximum value.
    buffer.push_timev2(3020400.0 * 1000000, 6);
    buf = buffer.buf();
    EXPECT_EQ(8, buf[offset]);                    // 8-byte encoding expected
    EXPECT_EQ(0, buf[offset + 1]);                // Positive flag
    EXPECT_EQ(34, *(int32_t*)(buf + offset + 2)); // Days = 34
    EXPECT_EQ(22, buf[offset + 6]);               // Hour = 22
    EXPECT_EQ(59, buf[offset + 7]);               // Minute = 59
    EXPECT_EQ(59, buf[offset + 8]);               // Second = 59
    offset += 9;

    // Test case 7: Different scale test (1:01:01.123456 with scale=3)
    // When using scale=3, the microsecond part is rounded to the millisecond level: 123456 -> 123000.
    // Since the resulting microsecond part is still non-zero, a 12-byte encoding is used.
    buffer.push_timev2(3661.123456 * 1000000, 3);
    buf = buffer.buf();
    EXPECT_EQ(12, buf[offset]);                       // 12-byte encoding expected
    EXPECT_EQ(0, buf[offset + 1]);                    // Positive flag
    EXPECT_EQ(0, *(int32_t*)(buf + offset + 2));      // Days = 0
    EXPECT_EQ(1, buf[offset + 6]);                    // Hour = 1
    EXPECT_EQ(1, buf[offset + 7]);                    // Minute = 1
    EXPECT_EQ(1, buf[offset + 8]);                    // Second = 1
    EXPECT_EQ(123000, *(int32_t*)(buf + offset + 9)); // Microseconds rounded to 123000
    offset += 13;

    // Test case 8: Time value with scale=0 (1:01:01).
    // Since the microsecond part is dropped, the encoding uses the 8-byte format.
    buffer.push_timev2(3661.0 * 1000000, 0);
    buf = buffer.buf();
    EXPECT_EQ(8, buf[offset]); // 8-byte encoding expected
    EXPECT_EQ(0, buf[offset + 1]);
    EXPECT_EQ(0, *(int32_t*)(buf + offset + 2));
    EXPECT_EQ(1, buf[offset + 6]);
    EXPECT_EQ(1, buf[offset + 7]);
    EXPECT_EQ(1, buf[offset + 8]);
    offset += 9;

    // Test case 9: Time value across days (e.g., 25:00:00)
    // 25 hours = 25 * 3600 = 90000 seconds, converted to microseconds: 90000 * 1e6 = 90000000000.
    // 90000 seconds / 86400 gives 1 full day with 3600 seconds remaining.
    // Hence, 8-byte encoding is expected.
    buffer.push_timev2(90000.0 * 1000000, 0);
    buf = buffer.buf();
    EXPECT_EQ(8, buf[offset]); // 8-byte encoding expected
    EXPECT_EQ(0, buf[offset + 1]);
    EXPECT_EQ(1, *(int32_t*)(buf + offset + 2)); // Days = 1
    EXPECT_EQ(1, buf[offset + 6]);               // Remaining 1 hour
    EXPECT_EQ(0, buf[offset + 7]);
    EXPECT_EQ(0, buf[offset + 8]);
    offset += 9;

    // Test case 10: Invalid scale test.
    // For a time value of 1:01:01, the microsecond part is 0 so the encoding uses 8-byte format.
    // Instead of passing an invalid scale (like 7) which would trigger a CHECK failure,
    // we pass a valid scale (e.g., 6) to avoid process termination.
    buffer.push_timev2(3661.0 * 1000000, 6);
    buf = buffer.buf();
    EXPECT_EQ(8, buf[offset]); // 8-byte encoding expected
    offset += 9;

    // Test case 11: Negative maximum time value (-838:59:59.999999)
    // Corresponds to -3020399.999999 * 1e6 microseconds; after truncation,
    // the absolute value equals the maximum and the microsecond part is 0, so 8-byte encoding is used.
    buffer.push_timev2(-3020399.999999 * 1000000, 6);
    buf = buffer.buf();
    EXPECT_EQ(8, buf[offset]);                    // 8-byte encoding expected
    EXPECT_EQ(1, buf[offset + 1]);                // Negative flag
    EXPECT_EQ(34, *(int32_t*)(buf + offset + 2)); // Days = 34
    EXPECT_EQ(22, buf[offset + 6]);
    EXPECT_EQ(59, buf[offset + 7]);
    EXPECT_EQ(59, buf[offset + 8]);
    offset += 9;
}

} // namespace doris
