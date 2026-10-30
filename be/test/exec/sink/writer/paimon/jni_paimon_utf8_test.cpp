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

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "exec/sink/writer/paimon/jni_paimon_write_backend.h"
#include "exec/spill/spill_data_dir.h"
#include "exec/spill/spill_file_manager.h"
#include "runtime/exec_env.h"
#include "testutil/jni_test_util.h"
#include "testutil/mock/mock_query_context.h"
#include "util/defer_op.h"

namespace doris {

class JniPaimonUtf8DeathTest : public JniJvmTest {
protected:
    static void check_arrays(JNIEnv* env) {
        const std::vector<std::string> values = {"column_\U0001F600", "\u4E2D\u6587", "",
                                                 std::string("a\0b", 3)};
        const std::vector<std::u16string> expected = {u"column_\U0001F600", u"\u4E2D\u6587", u"",
                                                      std::u16string(u"a\0b", 3)};
        for (const auto& input : {std::vector<std::string> {}, values}) {
            Jni::LocalArray array;
            jobjectArray raw = nullptr;
            auto status = JniPaimonWriteBackend::_create_string_array(env, input, &raw);
            ASSERT_TRUE(status.ok()) << status;
            array._obj = raw;
            jsize size = -1;
            status = array.get_length(env, &size);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(input.size(), size);
            for (jsize i = 0; i < size; ++i) {
                Jni::LocalString value;
                status = array.get_object_array_element(env, i, &value);
                ASSERT_TRUE(status.ok()) << status;
                expect_string(env, value.get(), expected[i]);
            }
        }
    }

    static void check_invalid_array(JNIEnv* env) {
        jobjectArray result = nullptr;
        auto status = JniPaimonWriteBackend::_create_string_array(env, {"valid_column", "bad\xFF"},
                                                                  &result);
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_EQ(nullptr, result);
        EXPECT_FALSE(env->ExceptionCheck());
        check_arrays(env);
    }

    static void expect_io_exception(JNIEnv* env, const std::string& expected_message) {
        ASSERT_TRUE(env->ExceptionCheck());
        jthrowable exception = env->ExceptionOccurred();
        env->ExceptionClear();
        DEFER(env->DeleteLocalRef(exception));
        Jni::LocalClass exception_class;
        auto status = Jni::Util::find_class(env, "java/io/IOException", &exception_class);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(env->IsInstanceOf(exception, static_cast<jclass>(exception_class.get())));
        Jni::MethodId get_message;
        status =
                exception_class.get_method(env, "getMessage", "()Ljava/lang/String;", &get_message);
        ASSERT_TRUE(status.ok()) << status;
        Jni::LocalString message;
        message._obj = env->CallObjectMethod(exception, get_message._id);
        ASSERT_FALSE(env->ExceptionCheck());
        ASSERT_FALSE(message.uninitialized());
        std::string actual;
        status = message.get_string_utf8(env, &actual);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(expected_message, actual);
    }

    static void check_exceptions(JNIEnv* env) {
        const std::string message = "spill /\u4E2D\u6587/\U0001F600" + std::string("\0suffix", 7);
        JniPaimonWriteBackend::_throw_java_io_exception(env, message);
        expect_io_exception(env, message);
        JniPaimonWriteBackend::_throw_java_io_exception(env, "invalid\xFF");
        expect_io_exception(env, "Failed to convert Paimon JNI exception message to UTF-16");
        check_arrays(env);
    }

    static void check_callbacks_without_context(JNIEnv* env) {
        on_jni_thread(env, [](JNIEnv* callback_env) {
            check_arrays(callback_env);
            check_invalid_array(callback_env);
            check_exceptions(callback_env);
        });
    }

    static void check_spill_directory(JNIEnv* env, ExternalSpillSession* session,
                                      std::string* directory) {
        auto handle = reinterpret_cast<jlong>(session);
        Jni::LocalArray directories;
        directories._obj = JniPaimonWriteBackend::_get_spill_directories(env, nullptr, handle);
        ASSERT_FALSE(env->ExceptionCheck());
        ASSERT_FALSE(directories.uninitialized());
        Jni::LocalString java_directory;
        auto status = directories.get_object_array_element(env, 0, &java_directory);
        ASSERT_TRUE(status.ok()) << status;
        status = java_directory.get_string_utf8(env, directory);
        ASSERT_TRUE(status.ok()) << status;
        std::vector<std::string> native_directories;
        status = session->get_paths(&native_directories);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(1, native_directories.size());
        EXPECT_EQ(native_directories.front(), *directory);
    }

    static void check_spill_reservation(JNIEnv* env, ExternalSpillSession* session,
                                        const std::string& directory, LocalSpillDataDir* data_dir) {
        auto handle = reinterpret_cast<jlong>(session);
        Jni::LocalString path;
        auto status =
                Jni::LocalString::new_string_utf8(env, directory + "/channel_\U0001F600", &path);
        ASSERT_TRUE(status.ok()) << status;
        JniPaimonWriteBackend::_reserve_spill(env, nullptr, handle,
                                              static_cast<jstring>(path.get()), 1024);
        ASSERT_FALSE(env->ExceptionCheck());
        EXPECT_EQ(1024, data_dir->get_spill_data_bytes());
        JniPaimonWriteBackend::_update_spill_accounting(
                env, nullptr, handle, static_cast<jstring>(path.get()), -256, 0, 0);
        ASSERT_FALSE(env->ExceptionCheck());
        EXPECT_EQ(768, data_dir->get_spill_data_bytes());
    }

    static void check_invalid_spill_path(JNIEnv* env, ExternalSpillSession* session,
                                         LocalSpillDataDir* data_dir) {
        auto handle = reinterpret_cast<jlong>(session);
        Jni::LocalString invalid;
        const jchar surrogate = 0xD800;
        invalid._obj = env->NewString(&surrogate, 1);
        ASSERT_FALSE(env->ExceptionCheck());
        JniPaimonWriteBackend::_reserve_spill(env, nullptr, handle,
                                              static_cast<jstring>(invalid.get()), 1024);
        expect_io_exception(
                env,
                Status::InvalidArgument("Invalid UTF-16 in JNI string at code unit 0").to_string());
        EXPECT_EQ(768, data_dir->get_spill_data_bytes());
        JniPaimonWriteBackend::_update_spill_accounting(
                env, nullptr, handle, static_cast<jstring>(invalid.get()), -256, 0, 0);
        expect_io_exception(
                env,
                Status::InvalidArgument("Invalid UTF-16 in JNI string at code unit 0").to_string());
        EXPECT_EQ(768, data_dir->get_spill_data_bytes());
    }

    static void check_spill_paths(JNIEnv* env) {
        const std::string root = "./ut_dir/paimon_jni_utf8_\U0001F600";
        std::filesystem::remove_all(root);
        std::filesystem::create_directories(root);
        DEFER(std::filesystem::remove_all(root));
        auto store = std::make_unique<LocalSpillDataDir>(root, 128L * 1024 * 1024);
        auto* data_dir = store.get();
        std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> stores;
        stores.emplace(root, std::move(store));
        SpillFileManager manager(std::move(stores));
        auto* exec_env = ExecEnv::GetInstance();
        auto* previous_manager = exec_env->_spill_file_mgr;
        exec_env->_spill_file_mgr = &manager;
        DEFER(exec_env->_spill_file_mgr = previous_manager);
        auto status = manager.init();
        ASSERT_TRUE(status.ok()) << status;
        DEFER(manager.stop());
        auto query = MockQueryContext::create();
        std::unique_ptr<ExternalSpillSession> session;
        status = manager.create_external_spill_session("paimon_\u4E2D\u6587_\U0001F600",
                                                       query.get(), &session);
        ASSERT_TRUE(status.ok()) << status;
        on_jni_thread(env, [&](JNIEnv* callback_env) {
            std::string directory;
            ASSERT_NO_FATAL_FAILURE(check_spill_directory(callback_env, session.get(), &directory));
            ASSERT_NO_FATAL_FAILURE(
                    check_spill_reservation(callback_env, session.get(), directory, data_dir));
            ASSERT_NO_FATAL_FAILURE(
                    check_invalid_spill_path(callback_env, session.get(), data_dir));
        });
        session.reset();
        EXPECT_EQ(0, data_dir->get_spill_data_bytes());
    }
};

TEST_F(JniPaimonUtf8DeathTest, StringArraysPreserveUnicode) {
    ASSERT_EXIT(run(check_arrays), testing::ExitedWithCode(0), "");
}

TEST_F(JniPaimonUtf8DeathTest, RejectMalformedUtf8Array) {
    ASSERT_EXIT(run(check_invalid_array), testing::ExitedWithCode(0), "");
}

TEST_F(JniPaimonUtf8DeathTest, ExceptionMessagesPreserveUnicode) {
    ASSERT_EXIT(run(check_exceptions), testing::ExitedWithCode(0), "");
}

TEST_F(JniPaimonUtf8DeathTest, CallbacksWithoutThreadContext) {
    ASSERT_EXIT(run(check_callbacks_without_context), testing::ExitedWithCode(0), "");
}

TEST_F(JniPaimonUtf8DeathTest, SpillPathsRoundTripAndPreserveAccounting) {
    ASSERT_EXIT(run(check_spill_paths), testing::ExitedWithCode(0), "");
}

TEST(JniPaimonUtf8Test, ReportArrayAllocationFailureWithoutException) {
    _jclass string_class;
    JNINativeInterface_ functions {};
    functions.reserved0 = &string_class;
    functions.FindClass = [](JNIEnv* env, const char*) -> jclass {
        return static_cast<jclass>(env->functions->reserved0);
    };
    functions.NewObjectArray = [](JNIEnv*, jsize, jclass, jobject) -> jobjectArray {
        return nullptr;
    };
    functions.DeleteLocalRef = [](JNIEnv*, jobject) {};
    functions.ExceptionCheck = [](JNIEnv*) -> jboolean { return JNI_FALSE; };
    JNIEnv env {&functions};
    auto* previous_env = Jni::Env::tls_env_;
    Jni::Env::tls_env_ = &env;
    DEFER(Jni::Env::tls_env_ = previous_env);
    jobjectArray array = nullptr;
    auto status = JniPaimonWriteBackend::_create_string_array(&env, {}, &array);
    EXPECT_TRUE(status.is<ErrorCode::JNI_ERROR>()) << status;
    EXPECT_EQ(nullptr, array);
    functions.reserved0 = nullptr;
    status = JniPaimonWriteBackend::_create_string_array(&env, {}, &array);
    EXPECT_TRUE(status.is<ErrorCode::JNI_ERROR>()) << status;
    EXPECT_EQ(nullptr, array);
}

} // namespace doris
