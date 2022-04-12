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

#include "runtime/string_buffer.hpp"

#include <gtest/gtest.h>

#include <string>

#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {

void validate_string(const std::string& std_str, const StringBuffer& str) {
    EXPECT_EQ(std_str.empty(), str.empty());
    EXPECT_EQ((int)std_str.size(), str.size());

    if (std_str.size() > 0) {
        EXPECT_EQ(strncmp(std_str.c_str(), str.str().ptr, std_str.size()), 0);
    }
}

TEST(StringBufferTest, Basic) {
    MemTracker tracker;
    MemPool pool(&tracker);
    StringBuffer str(&pool);
    std::string std_str;

    // Empty string
    validate_string(std_str, str);

    // Clear empty string
    std_str.clear();
    str.clear();
    validate_string(std_str, str);

    // Append to empty
    std_str.append("Hello");
    str.append("Hello", strlen("Hello"));
    validate_string(std_str, str);

    // Append some more
    std_str.append("World");
    str.append("World", strlen("World"));
    validate_string(std_str, str);

    // Assign
    std_str.assign("foo");
    str.assign("foo", strlen("foo"));
    validate_string(std_str, str);

    // Clear
    std_str.clear();
    str.clear();
    validate_string(std_str, str);

    // Underlying buffer size should be the length of the max string during the test.
    EXPECT_EQ(str.buffer_size(), strlen("HelloWorld"));
}

} // namespace doris
