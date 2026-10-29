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

#pragma once

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "util/jni-util.h"

namespace doris {

// Start a real JVM in an ASSERT_EXIT child: the parent retains its lazy-JVM state,
// and the child exits without running LeakSanitizer against the JVM's allocations.
class JniJvmTest : public testing::Test {
protected:
    void SetUp() override { GTEST_FLAG_SET(death_test_style, "threadsafe"); }

    static void run(void (*check)(JNIEnv*)) {
        JNIEnv* env = nullptr;
        init_jvm(&env);
        if (!HasFatalFailure()) {
            check(env);
        }
        std::_Exit(HasFailure() ? 1 : 0);
    }

    static void expect_string(JNIEnv* env, jobject value, const std::u16string& expected) {
        ASSERT_NE(nullptr, value);
        auto* string = static_cast<jstring>(value);
        ASSERT_EQ(expected.size(), env->GetStringLength(string));
        ASSERT_FALSE(env->ExceptionCheck());
        std::u16string actual(expected.size(), u'\0');
        env->GetStringRegion(string, 0, static_cast<jsize>(actual.size()),
                             reinterpret_cast<jchar*>(actual.data()));
        ASSERT_FALSE(env->ExceptionCheck());
        EXPECT_EQ(expected, actual);
    }

    static void init_jvm(JNIEnv** env) {
        JavaVMOption options[] = {{const_cast<char*>("-Xms16m"), nullptr},
                                  {const_cast<char*>("-Xmx64m"), nullptr},
                                  {const_cast<char*>("-Xcheck:jni"), nullptr}};
        JavaVMInitArgs args {};
        args.version = JNI_VERSION_1_8;
        args.nOptions = sizeof(options) / sizeof(options[0]);
        args.options = options;
        JavaVM* vm = nullptr;
        ASSERT_EQ(JNI_OK, JNI_CreateJavaVM(&vm, reinterpret_cast<void**>(env), &args));
        // Only java.lang/java.util are needed. Prime the RAII wrappers without requiring
        // a deployed Doris plugin SPI, and initialize the real HashMap method IDs.
        Jni::Env::tls_env_ = *env;
        auto status = Jni::Util::_init_collect_class();
        ASSERT_TRUE(status.ok()) << status;
    }
};

} // namespace doris
