// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "util/mysql_row_buffer.h"

using namespace std;

namespace palo {

class MysqlRowBufferTest : public testing::Test {
public:
    MysqlRowBufferTest() {
    }

protected:
    virtual void SetUp() {
    }
};

TEST_F(MysqlRowBufferTest, tinyint) {
    MysqlRowBuffer buffer;

    ASSERT_EQ(0, buffer.PushTinyInt(-111));
    ASSERT_EQ(4, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-111", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushTinyInt(100));
    ASSERT_EQ(3, *(int8_t*)buffer.buf());
    ASSERT_STREQ("100", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushTinyInt(255));
    ASSERT_EQ(2, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-1", buffer.buf() + 1);
}

TEST_F(MysqlRowBufferTest, smallint) {
    MysqlRowBuffer buffer;

    ASSERT_EQ(0, buffer.PushSmallInt(-10000));
    ASSERT_EQ(6, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-10000", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushSmallInt(32767));
    ASSERT_EQ(5, *(int8_t*)buffer.buf());
    ASSERT_STREQ("32767", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushSmallInt(65535));
    ASSERT_EQ(2, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-1", buffer.buf() + 1);
}

TEST_F(MysqlRowBufferTest, int) {
    MysqlRowBuffer buffer;

    ASSERT_EQ(0, buffer.PushInt(-10000));
    ASSERT_EQ(6, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-10000", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushInt(32767));
    ASSERT_EQ(5, *(int8_t*)buffer.buf());
    ASSERT_STREQ("32767", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushInt(4294967295));
    ASSERT_EQ(2, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-1", buffer.buf() + 1);
}
TEST_F(MysqlRowBufferTest, bigint) {
    MysqlRowBuffer buffer;

    ASSERT_EQ(0, buffer.PushBigInt(-1000000000));
    ASSERT_EQ(11, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-1000000000", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushBigInt(1000032767));
    ASSERT_EQ(10, *(int8_t*)buffer.buf());
    ASSERT_STREQ("1000032767", buffer.buf() + 1);
}
TEST_F(MysqlRowBufferTest, float) {
    MysqlRowBuffer buffer;

    ASSERT_EQ(0, buffer.PushFloat(-1.1));
    ASSERT_EQ(4, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-1.1", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushFloat(1000.12));
    ASSERT_EQ(7, *(int8_t*)buffer.buf());
    ASSERT_STREQ("1000.12", buffer.buf() + 1);
}
TEST_F(MysqlRowBufferTest, double) {
    MysqlRowBuffer buffer;

    ASSERT_EQ(0, buffer.PushDouble(-1.1));
    ASSERT_EQ(4, *(int8_t*)buffer.buf());
    ASSERT_STREQ("-1.1", buffer.buf() + 1);

    buffer.Reset();
    ASSERT_EQ(0, buffer.PushDouble(1000.001));
    ASSERT_EQ(8, *(int8_t*)buffer.buf());
    ASSERT_STREQ("1000.001", buffer.buf() + 1);
}

TEST_F(MysqlRowBufferTest, string) {
    MysqlRowBuffer buffer;

    ASSERT_EQ(0, buffer.PushString("hello", 6));
    ASSERT_EQ(6, *(int8_t*)buffer.buf());
    ASSERT_STREQ("hello", buffer.buf() + 1);
    ASSERT_NE(0, buffer.PushString(NULL, 6));
}

TEST_F(MysqlRowBufferTest, long_buffer) {
    MysqlRowBuffer buffer;

    for (int i = 0; i < 5000; ++i) {
        ASSERT_EQ(0, buffer.PushInt(10000));
    }

    ASSERT_EQ(30000, buffer.length());
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("PALO_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
