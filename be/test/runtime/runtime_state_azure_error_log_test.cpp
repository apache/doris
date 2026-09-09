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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "cpp/sync_point.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "load/load_path_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/obj_storage_client_test_stub.h"

namespace doris {
namespace {

using testing::_;
using testing::AllOf;
using testing::AnyNumber;
using testing::Field;
using testing::Return;
using testing::StrictMock;

constexpr char ERROR_LOG_CONTENT[] = "rejected test row\n";
constexpr char SIGNED_ERROR_LOG_URL[] =
        "https://account.blob.core.windows.net/container/errors/error.log?sig=test-only";

class ErrorLogObjStorageClient : public io::ObjStorageClientTestStub {
public:
    MOCK_METHOD(ObjStorageUploadResult, create_multipart_upload, (const ObjStoragePath&),
                (override));
    MOCK_METHOD(ObjStorageResponse, put_object, (const ObjStoragePath&, std::string_view),
                (override));
    MOCK_METHOD(ObjStorageUploadResult, upload_part,
                (const ObjStoragePath&, const std::string&, std::string_view, int), (override));
    MOCK_METHOD(ObjStorageResponse, complete_multipart_upload,
                (const ObjStoragePath&, const std::string&,
                 const std::vector<ObjStorageCompletedPart>&),
                (override));
    MOCK_METHOD(ObjStorageHeadResult, head_object, (const ObjStoragePath&), (override));
    MOCK_METHOD(ObjStorageResponse, get_object,
                (const ObjStoragePath&, void*, size_t, size_t, size_t*), (override));
    MOCK_METHOD(ObjStorageResponse, delete_objects,
                (const ObjStoragePath&, std::vector<std::string>), (override));
    MOCK_METHOD(ObjStorageResponse, delete_object, (const ObjStoragePath&), (override));
    MOCK_METHOD(std::string, generate_presigned_url, (const ObjStoragePath&, int64_t), (override));
    MOCK_METHOD(ObjStorageListPageResult, list_objects_page,
                (const ObjStoragePath&, std::string_view), (override));
};

class RuntimeStateAzureErrorLogTest : public testing::Test {
protected:
    void SetUp() override {
        std::string directory_template =
                (std::filesystem::temp_directory_path() / "doris_azure_error_log_XXXXXX").string();
        const char* directory = mkdtemp(directory_template.data());
        ASSERT_NE(directory, nullptr);
        _directory = directory;

        // Only path joining is needed: do not initialize LoadPathMgr's cleaner or ExecEnv.
        _load_path_mgr._error_log_dir = _directory.string();
        _exec_env->_load_path_mgr = &_load_path_mgr;
        _state._error_log_file_path = "error.log";
        _state._s3_error_log_file_path = "error.log";
        _state._error_log_file = std::make_unique<std::ofstream>(_directory / "error.log");
        ASSERT_TRUE(_state._error_log_file->is_open());
        *_state._error_log_file << ERROR_LOG_CONTENT;
        ASSERT_TRUE(_state._error_log_file->good());

        S3Conf conf;
        conf.bucket = "container";
        conf.prefix = "errors";
        conf.client_conf.endpoint = "https://account.blob.core.windows.net";
        conf.client_conf.provider = ObjStorageProvider::AZURE;
        _client = std::make_shared<StrictMock<ErrorLogObjStorageClient>>();
        // BE UT permits private access. Bypass provider creation, not the upload or consumer.
        _state._s3_error_fs =
                std::shared_ptr<io::S3FileSystem>(new io::S3FileSystem(conf, "azure-error-log"));
        _state._s3_error_fs->client_holder()->_client = _client;

        auto* sync_point = SyncPoint::get_instance();
        ASSERT_FALSE(sync_point->has_point("UploadFileBuffer::submit"));
        sync_point->set_call_back("UploadFileBuffer::submit", [](auto&& args) {
            // Execute the real writer callback inline; no upload thread pool or network is used.
            try_any_cast<io::FileBuffer*>(args.at(0))->execute_async();
            auto result = try_any_cast_ret<Status>(args);
            result->first = Status::OK();
            result->second = true;
        });
        _callback_installed = true;
        sync_point->enable_processing();
    }

    void TearDown() override {
        _state._error_log_file.reset();
        _state._s3_error_fs.reset();
        _client.reset();
        _exec_env->_load_path_mgr = _saved_load_path_mgr;
        if (_callback_installed) {
            auto* sync_point = SyncPoint::get_instance();
            sync_point->clear_call_back("UploadFileBuffer::submit");
            if (!_sync_was_enabled) {
                sync_point->disable_processing();
            }
        }
        if (!_directory.empty()) {
            std::error_code ec;
            std::filesystem::remove(_directory / "error.log", ec);
            EXPECT_FALSE(ec) << ec.message();
            std::filesystem::remove(_directory, ec);
            EXPECT_FALSE(ec) << ec.message();
        }
    }

    void expect_upload(ObjStorageResponse response) {
        EXPECT_CALL(*_client, put_object(AllOf(Field(&ObjStoragePath::bucket, "container"),
                                               Field(&ObjStoragePath::key, "errors/error.log")),
                                         std::string_view(ERROR_LOG_CONTENT)))
                .Times(1)
                .WillOnce(Return(response));
        // Accommodate either setting of enable_s3_object_check_after_upload without changing it.
        EXPECT_CALL(*_client, head_object(_))
                .Times(AnyNumber())
                .WillRepeatedly(
                        Return(ObjStorageHeadResult {.resp = ObjStorageResponse::OK(),
                                                     .file_size = sizeof(ERROR_LOG_CONTENT) - 1}));
    }

    void expect_presign(std::string url) {
        EXPECT_CALL(*_client,
                    generate_presigned_url(AllOf(Field(&ObjStoragePath::bucket, "container"),
                                                 Field(&ObjStoragePath::key, "errors/error.log")),
                                           7 * 24 * 60 * 60 - 1))
                .Times(1)
                .WillOnce(Return(url));
    }

    ExecEnv* _exec_env = ExecEnv::GetInstance();
    LoadPathMgr* _saved_load_path_mgr = _exec_env->load_path_mgr();
    LoadPathMgr _load_path_mgr {_exec_env};
    RuntimeState _state;
    std::filesystem::path _directory;
    std::shared_ptr<StrictMock<ErrorLogObjStorageClient>> _client;
    bool _sync_was_enabled = SyncPoint::get_instance()->get_enable();
    bool _callback_installed = false;
};

TEST_F(RuntimeStateAzureErrorLogTest, RefusedPresignKeepsLocalPathAndUploadsOnlyOnce) {
    expect_upload(ObjStorageResponse::OK());
    // Azure SAS/OAuth2 presigning uses this empty-string failure contract. Its SDK behavior is
    // covered separately by AzureObjStorageClientLifecycleTest.
    expect_presign("");

    EXPECT_EQ(_state.get_error_log_file_path(), "error.log");
    EXPECT_FALSE(_state._error_log_file->is_open());
    EXPECT_EQ(_state.get_error_log_file_path(), "error.log");
    EXPECT_EQ(_state._error_log_file_path, "error.log");
    EXPECT_EQ(std::filesystem::file_size(_directory / "error.log"), sizeof(ERROR_LOG_CONTENT) - 1);
}

TEST_F(RuntimeStateAzureErrorLogTest, SignedUrlIsPublishedAndReusedWithoutAnotherUpload) {
    expect_upload(ObjStorageResponse::OK());
    expect_presign(SIGNED_ERROR_LOG_URL);

    EXPECT_EQ(_state.get_error_log_file_path(), SIGNED_ERROR_LOG_URL);
    EXPECT_FALSE(_state._error_log_file->is_open());
    EXPECT_EQ(_state.get_error_log_file_path(), SIGNED_ERROR_LOG_URL);
    EXPECT_EQ(_state._error_log_file_path, SIGNED_ERROR_LOG_URL);
}

TEST_F(RuntimeStateAzureErrorLogTest, FailedUploadKeepsLocalPathWithoutAttemptingPresign) {
    expect_upload({.status = {TStatusCode::IO_ERROR, "test upload failure"}});
    EXPECT_CALL(*_client, generate_presigned_url(_, _)).Times(0);

    EXPECT_EQ(_state.get_error_log_file_path(), "error.log");
    EXPECT_FALSE(_state._error_log_file->is_open());
    EXPECT_EQ(_state.get_error_log_file_path(), "error.log");
    EXPECT_EQ(_state._error_log_file_path, "error.log");
    EXPECT_EQ(std::filesystem::file_size(_directory / "error.log"), sizeof(ERROR_LOG_CONTENT) - 1);
}

} // namespace
} // namespace doris
