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

TEST_F(JniPaimonUtf8DeathTest, SpillPathsRoundTripAndPreserveAccounting) {
    ASSERT_EXIT(run(check_spill_paths), testing::ExitedWithCode(0), "");
}

} // namespace doris
