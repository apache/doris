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

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "testutil/jni_test_util.h"
#include "util/defer_op.h"
#include "util/jni-util.h"

namespace doris {

class JniUtf8DeathTest : public JniJvmTest {
protected:
    static std::vector<std::pair<std::string, std::u16string>> valid_cases() {
        std::vector<std::pair<std::string, std::u16string>> cases = {
                {"", u""},
                {"ascii", u"ascii"},
                {"\u4E2D\u6587", u"\u4E2D\u6587"},
                {"\u0080\u07FF\u0800\uFFFF", u"\u0080\u07FF\u0800\uFFFF"},
                {"a\U0001F600bc", u"a\U0001F600bc"},
                {"\U00010000\U0010FFFF", u"\U00010000\U0010FFFF"},
                {"udf_\U00010400(STRING)", u"udf_\U00010400(STRING)"},
                {"{\"path\":\"/\U0001F600/plugin\"}", u"{\"path\":\"/\U0001F600/plugin\"}"},
                {std::string("\0", 1), std::u16string(u"\0", 1)},
                {std::string("a\0b", 3), std::u16string(u"a\0b", 3)},
                {"SELECT id FROM t WHERE source = 'a\U0001F600bc'",
                 u"SELECT id FROM t WHERE source = 'a\U0001F600bc'"}};
        cases.emplace_back(std::string(1023, 'a') + "\U0001F600bc",
                           std::u16string(1023, u'a') + u"\U0001F600bc");
        return cases;
    }

    template <Jni::RefType Ref>
    static void check_strings(JNIEnv* env) {
        for (const auto& [input, expected] : valid_cases()) {
            Jni::String<Ref> value;
            auto status = Jni::String<Ref>::new_string_utf8(env, input, &value);
            ASSERT_TRUE(status.ok()) << status;
            expect_string(env, value.get(), expected);
            EXPECT_EQ(Ref == Jni::Local ? JNILocalRefType : JNIGlobalRefType,
                      env->GetObjectRefType(value.get()));
        }
    }

    static void check_all_strings(JNIEnv* env) {
        check_strings<Jni::Local>(env);
        check_strings<Jni::Global>(env);
        Jni::LocalString empty;
        auto status = Jni::LocalString::new_string_utf8(env, {}, &empty);
        ASSERT_TRUE(status.ok()) << status;
        expect_string(env, empty.get(), u"");
    }

    static void check_strings_without_context(JNIEnv* env) {
        on_jni_thread(env, check_all_strings);
    }

    static void check_reads(JNIEnv* env) {
        for (const auto& [expected, input] : valid_cases()) {
            // Build the source independently from new_string_utf8 so paired bugs cannot cancel.
            Jni::LocalString local;
            local._obj = env->NewString(reinterpret_cast<const jchar*>(input.data()),
                                        static_cast<jsize>(input.size()));
            ASSERT_FALSE(env->ExceptionCheck());
            ASSERT_FALSE(local.uninitialized());
            std::string actual;
            auto status = local.get_string_utf8(env, &actual);
            ASSERT_TRUE(status.ok()) << status;
            EXPECT_EQ(expected, actual);
            Jni::GlobalString global;
            status = Jni::local_to_global_ref(env, local, &global);
            ASSERT_TRUE(status.ok()) << status;
            status = global.get_string_utf8(env, &actual);
            ASSERT_TRUE(status.ok()) << status;
            EXPECT_EQ(expected, actual);
        }
    }

    static void check_reads_without_context(JNIEnv* env) { on_jni_thread(env, check_reads); }

    template <Jni::RefType Ref>
    static void check_modified_utf8(JNIEnv* env) {
        const std::vector<std::pair<std::string, std::u16string>> cases = {
                {"", u""},
                {"ascii", u"ascii"},
                {"\u4E2D\u6587", u"\u4E2D\u6587"},
                {"a\xED\xA0\xBD\xED\xB8\x80"
                 "bc",
                 u"a\U0001F600bc"},
                {"a\xC0\x80"
                 "b",
                 std::u16string(u"a\0b", 3)}};
        for (const auto& [input, expected] : cases) {
            Jni::String<Ref> value;
            auto status = Jni::String<Ref>::new_string(env, input.c_str(), &value);
            ASSERT_TRUE(status.ok()) << status;
            expect_string(env, value.get(), expected);
            EXPECT_EQ(Ref == Jni::Local ? JNILocalRefType : JNIGlobalRefType,
                      env->GetObjectRefType(value.get()));
            Jni::StringBufferGuard<Ref> chars;
            status = value.get_string_chars(env, &chars);
            ASSERT_TRUE(status.ok()) << status;
            EXPECT_EQ(input, chars.get());
        }
    }

    static void check_legacy_strings(JNIEnv* env) {
        check_modified_utf8<Jni::Local>(env);
        check_modified_utf8<Jni::Global>(env);
    }

    template <Jni::RefType Ref>
    static void check_maps(JNIEnv* env) {
        std::map<std::string, std::string> params;
        for (const auto& [input, expected] : valid_cases()) {
            params[input] = input;
        }
        Jni::Object<Ref> java_map;
        auto status = Jni::Util::convert_to_java_map(env, params, &java_map);
        ASSERT_TRUE(status.ok()) << status;
        Jni::LocalClass map_class;
        status = Jni::Util::find_class(env, "java/util/Map", &map_class);
        ASSERT_TRUE(status.ok()) << status;
        Jni::MethodId get_method;
        status = map_class.get_method(env, "get", "(Ljava/lang/Object;)Ljava/lang/Object;",
                                      &get_method);
        ASSERT_TRUE(status.ok()) << status;
        for (const auto& [input, expected] : valid_cases()) {
            // Construct lookup keys independently from the helper under test.
            Jni::LocalString key;
            key._obj = env->NewString(reinterpret_cast<const jchar*>(expected.data()),
                                      static_cast<jsize>(expected.size()));
            ASSERT_FALSE(env->ExceptionCheck());
            ASSERT_FALSE(key.uninitialized());
            Jni::LocalString value;
            status = java_map.call_object_method(env, get_method).with_arg(key).call(&value);
            ASSERT_TRUE(status.ok()) << status;
            expect_string(env, value.get(), expected);
        }
        std::map<std::string, std::string> actual;
        status = Jni::Util::convert_to_cpp_map(env, java_map, &actual);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(params, actual);
    }

    static void check_all_maps(JNIEnv* env) {
        check_maps<Jni::Local>(env);
        check_maps<Jni::Global>(env);
    }

    static void check_invalid_utf8(JNIEnv* env) {
        const std::vector<std::string> invalid = {
                "\xF0\x9F", "\xC0\x80",  "\xED\xA0\x80",
                "\x80",     "\xFF",      "\xF4\x90\x80\x80",
                "abc\xFF",  "a\xE2\x82", std::string(127, 'a') + "\xF0\x9F"};
        for (const auto& input : invalid) {
            Jni::LocalString local_value;
            auto status = Jni::LocalString::new_string_utf8(env, input, &local_value);
            ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
            EXPECT_TRUE(local_value.uninitialized());
            Jni::GlobalString global_value;
            status = Jni::GlobalString::new_string_utf8(env, input, &global_value);
            ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
            EXPECT_TRUE(global_value.uninitialized());

            // Fail after a valid entry as well as after allocating a valid key.
            for (const auto& params :
                 {std::map<std::string, std::string> {{"", "ok"}, {input, "v"}},
                  std::map<std::string, std::string> {{"", "ok"}, {"key", input}}}) {
                Jni::LocalObject java_map;
                status = Jni::Util::convert_to_java_map(env, params, &java_map);
                ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
                EXPECT_FALSE(env->ExceptionCheck());
            }
        }
        Jni::LocalString value;
        auto status = Jni::LocalString::new_string_utf8(env, "abc\xFF", &value);
        EXPECT_NE(std::string::npos, status.to_string().find("at byte 3"));
        // Conversion still works after errors and their temporary JNI references are released.
        check_all_maps(env);
    }

    static void check_invalid_utf16_string(JNIEnv* env, const Jni::LocalString& value) {
        std::string actual = "unchanged";
        auto status = value.get_string_utf8(env, &actual);
        ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_EQ("unchanged", actual);
        EXPECT_FALSE(env->ExceptionCheck());
        Jni::GlobalString global;
        status = Jni::local_to_global_ref(env, value, &global);
        ASSERT_TRUE(status.ok()) << status;
        status = global.get_string_utf8(env, &actual);
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    }

    static void check_invalid_utf16_map(JNIEnv* env, const Jni::LocalString& value, bool bad_key) {
        Jni::LocalObject map;
        auto status = Jni::Util::convert_to_java_map(env, {}, &map);
        ASSERT_TRUE(status.ok()) << status;
        Jni::LocalString valid;
        status = Jni::LocalString::new_string_utf8(env, "valid", &valid);
        ASSERT_TRUE(status.ok()) << status;
        status = map.call_object_method(env, Jni::Util::hashmap_put)
                         .with_arg(bad_key ? value : valid)
                         .with_arg(bad_key ? valid : value)
                         .call();
        ASSERT_TRUE(status.ok()) << status;
        std::map<std::string, std::string> output;
        status = Jni::Util::convert_to_cpp_map(env, map, &output);
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_TRUE(output.empty());
    }

    static void check_invalid_utf16(JNIEnv* env) {
        const std::vector<std::u16string> invalid = {
                {0xD800},       {0xDC00},         {0xD800, u'a'},
                {u'a', 0xDC00}, {0xDC00, 0xD800}, std::u16string(127, u'a') + char16_t(0xD800)};
        for (const auto& input : invalid) {
            Jni::LocalString value;
            value._obj = env->NewString(reinterpret_cast<const jchar*>(input.data()),
                                        static_cast<jsize>(input.size()));
            ASSERT_FALSE(env->ExceptionCheck());
            ASSERT_FALSE(value.uninitialized());
            check_invalid_utf16_string(env, value);
            // Both malformed keys and malformed values must fail the reverse map conversion.
            check_invalid_utf16_map(env, value, true);
            check_invalid_utf16_map(env, value, false);
        }
        check_reads(env);
        check_all_maps(env);
    }
};

TEST_F(JniUtf8DeathTest, StringsPreserveUnicode) {
    ASSERT_EXIT(run(check_all_strings), testing::ExitedWithCode(0), "");
}

TEST_F(JniUtf8DeathTest, ReadStringsAsStandardUtf8) {
    ASSERT_EXIT(run(check_reads), testing::ExitedWithCode(0), "");
}

TEST_F(JniUtf8DeathTest, CreateStringsWithoutThreadContext) {
    ASSERT_EXIT(run(check_strings_without_context), testing::ExitedWithCode(0), "");
}

TEST_F(JniUtf8DeathTest, ReadStringsWithoutThreadContext) {
    ASSERT_EXIT(run(check_reads_without_context), testing::ExitedWithCode(0), "");
}

TEST_F(JniUtf8DeathTest, PreserveLegacyModifiedUtf8) {
    ASSERT_EXIT(run(check_legacy_strings), testing::ExitedWithCode(0), "");
}

TEST_F(JniUtf8DeathTest, ParameterMapsPreserveUnicode) {
    ASSERT_EXIT(run(check_all_maps), testing::ExitedWithCode(0), "");
}

TEST_F(JniUtf8DeathTest, RejectMalformedUtf8) {
    ASSERT_EXIT(run(check_invalid_utf8), testing::ExitedWithCode(0), "");
}

TEST_F(JniUtf8DeathTest, RejectMalformedUtf16) {
    ASSERT_EXIT(run(check_invalid_utf16), testing::ExitedWithCode(0), "");
}

TEST(JniUtf8Test, ReportAllocationFailureWithoutException) {
    JNINativeInterface_ functions {};
    functions.NewString = [](JNIEnv*, const jchar*, jsize) -> jstring { return nullptr; };
    functions.NewStringUTF = [](JNIEnv*, const char*) -> jstring { return nullptr; };
    functions.ExceptionCheck = [](JNIEnv*) -> jboolean { return JNI_FALSE; };
    JNIEnv env {&functions};
    Jni::LocalString value;
    auto status = Jni::LocalString::new_string_utf8(&env, "ascii", &value);
    EXPECT_TRUE(status.is<ErrorCode::JNI_ERROR>()) << status;
    EXPECT_TRUE(value.uninitialized());
    Jni::LocalString modified;
    status = Jni::LocalString::new_string(&env, "ascii", &modified);
    EXPECT_TRUE(status.is<ErrorCode::JNI_ERROR>()) << status;
    EXPECT_TRUE(modified.uninitialized());
}

TEST(JniUtf8Test, ReportGlobalReferenceFailureWithoutException) {
    _jstring local_string;
    JNINativeInterface_ functions {};
    functions.reserved0 = &local_string;
    functions.NewString = [](JNIEnv* env, const jchar*, jsize) -> jstring {
        return static_cast<jstring>(env->functions->reserved0);
    };
    functions.NewStringUTF = [](JNIEnv* env, const char*) -> jstring {
        return static_cast<jstring>(env->functions->reserved0);
    };
    functions.NewGlobalRef = [](JNIEnv*, jobject) -> jobject { return nullptr; };
    functions.DeleteLocalRef = [](JNIEnv*, jobject) {};
    functions.ExceptionCheck = [](JNIEnv*) -> jboolean { return JNI_FALSE; };
    JNIEnv env {&functions};
    auto* previous_env = Jni::Env::tls_env_;
    Jni::Env::tls_env_ = &env;
    DEFER(Jni::Env::tls_env_ = previous_env);
    Jni::GlobalString value;
    auto status = Jni::GlobalString::new_string_utf8(&env, "ascii", &value);
    EXPECT_TRUE(status.is<ErrorCode::JNI_ERROR>()) << status;
    EXPECT_TRUE(value.uninitialized());
    Jni::GlobalString modified;
    status = Jni::GlobalString::new_string(&env, "ascii", &modified);
    EXPECT_TRUE(status.is<ErrorCode::JNI_ERROR>()) << status;
    EXPECT_TRUE(modified.uninitialized());
}

} // namespace doris
