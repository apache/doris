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

#include <string>
#include <gtest/gtest.h>

#include "env/env.h"
#include "util/logging.h"
#include "gutil/strings/util.h"
#include "util/mysql_row_buffer.h"

namespace doris {

using namespace strings;

TEST(MysqlRowBufferTest, basic) {
    
    MysqlRowBuffer mrb;
    
    mrb.push_int(1);
    mrb.push_string("test");
    mrb.push_null();
    
    char* buf = mrb.buf();
    
    ASSERT_EQ(*(uint8_t*)(buf + 1), 1);
    ASSERT_EQ(*(uint8_t*)(buf + 2), 1);
    ASSERT_EQ(*(uint8_t*)(buf + 3), 4);
    ASSERT_EQ(*(char*)(buf + 4), "test");
    ASSERT_EQ(*(uint_8*)(buf + 8), 251);

}



}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}