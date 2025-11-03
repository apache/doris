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

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <thread>

#include "common/logging.h"

using doris::cloud::AnnotateTag;

int main(int argc, char** argv) {
    if (!doris::cloud::init_glog("log_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(LogTest, ConstructionTest) {
    // Heap allocation is disabled.
    // new AnnotateTag();

    // Arithmetics
    {
        char c = 0;
        bool b = false;
        int8_t i8 = 0;
        uint8_t u8 = 0;
        int16_t i16 = 0;
        uint16_t u16 = 0;
        int32_t i32 = 0;
        uint32_t u32 = 0;
        int64_t i64 = 0;
        uint64_t u64 = 0;

        AnnotateTag tag_char("char", c);
        AnnotateTag tag_bool("bool", b);
        AnnotateTag tag_i8("i8", i8);
        AnnotateTag tag_u8("u8", u8);
        AnnotateTag tag_i16("i16", i16);
        AnnotateTag tag_u16("u16", u16);
        AnnotateTag tag_i32("i32", i32);
        AnnotateTag tag_u32("u32", u32);
        AnnotateTag tag_i64("i64", i64);
        AnnotateTag tag_u64("u64", u64);
        LOG_INFO("hello");
    }

    // String literals.
    {
        const char* text = "hello";
        AnnotateTag tag_text("hello", text);
        LOG_INFO("hello");
    }

    // String view.
    {
        std::string test("abc");
        AnnotateTag tag_text("hello", std::string_view(test));
        LOG_INFO("hello");
    }

    // Const string.
    {
        const std::string test("abc");
        AnnotateTag tag_text("hello", test);
        LOG_INFO("hello");
    }
}

TEST(LogTest, ThreadTest) {
    // In pthread.
    {
        ASSERT_EQ(bthread_self(), 0);
        AnnotateTag tag("run_in_bthread", true);
        LOG_INFO("thread test");
    }

    // In bthread.
    {
        auto fn = +[](void*) -> void* {
            EXPECT_NE(bthread_self(), 0);
            AnnotateTag tag("run_in_bthread", true);
            LOG_INFO("thread test");
            return nullptr;
        };
        bthread_t tid;
        ASSERT_EQ(bthread_start_background(&tid, nullptr, fn, nullptr), 0);
        ASSERT_EQ(bthread_join(tid, nullptr), 0);
    }
}