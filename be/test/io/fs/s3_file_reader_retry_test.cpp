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

#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_system.h"
#include "util/s3_util.h"

namespace doris::io {

// A controllable fake ObjStorageClient for testing S3FileReader retry behavior.
// get_object() returns responses from a pre-configured queue; when the queue is
// exhausted it returns a successful response with the requested bytes.
class FakeObjStorageClient : public ObjStorageClient {
public:
    struct GetObjectCall {
        ObjectStorageResponse resp;
        size_t bytes_to_return; // 0 = use bytes_read param
    };

    // Push a response that get_object will return on the next call.
    void push_get_response(ObjectStorageResponse resp, size_t bytes_to_return = 0) {
        _get_responses.push_back({std::move(resp), bytes_to_return});
    }

    int get_object_call_count() const { return _get_object_calls; }

    // --- ObjStorageClient interface ---
    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read,
                                     size_t* size_return) override {
        _get_object_calls++;
        if (!_get_responses.empty()) {
            auto entry = std::move(_get_responses.front());
            _get_responses.erase(_get_responses.begin());
            if (size_return) {
                *size_return = entry.bytes_to_return > 0 ? entry.bytes_to_return : bytes_read;
            }
            return entry.resp;
        }
        // Default: success, fill buffer with zeros
        if (buffer) memset(buffer, 0, bytes_read);
        if (size_return) *size_return = bytes_read;
        return ObjectStorageResponse::OK();
    }

    // Stubs for other methods (not exercised in reader retry tests)
    ObjectStorageUploadResponse create_multipart_upload(const ObjectStoragePathOptions&) override {
        return {};
    }
    ObjectStorageResponse put_object(const ObjectStoragePathOptions&, std::string_view) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions&, std::string_view,
                                            int) override {
        return {};
    }
    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions&,
            const std::vector<ObjectCompleteMultiPart>&) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions&) override {
        return {.resp = ObjectStorageResponse::OK(), .file_size = 1024};
    }
    ObjectStorageResponse list_objects(const ObjectStoragePathOptions&,
                                       std::vector<FileInfo>*) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions&,
                                         std::vector<std::string>) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_object(const ObjectStoragePathOptions&) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_objects_recursively(const ObjectStoragePathOptions&) override {
        return ObjectStorageResponse::OK();
    }
    std::string generate_presigned_url(const ObjectStoragePathOptions&, int64_t,
                                       const S3ClientConf&) override {
        return "";
    }

private:
    std::vector<GetObjectCall> _get_responses;
    int _get_object_calls = 0;
};

class S3FileReaderRetryTest : public testing::Test {
protected:
    void SetUp() override { S3ClientFactory::instance(); }

    // Helper: create an S3FileReader backed by the given fake client.
    std::pair<std::shared_ptr<FakeObjStorageClient>, std::shared_ptr<S3FileReader>> make_reader(
            size_t file_size = 1024) {
        auto fake = std::make_shared<FakeObjStorageClient>();
        auto holder = std::make_shared<ObjClientHolder>(S3ClientConf {});
        holder->_client = fake;
        auto reader = std::make_shared<S3FileReader>(std::const_pointer_cast<const ObjClientHolder>(holder),
                                                      "test-bucket", "test-key", file_size, nullptr);
        return {fake, reader};
    }

    // Build a retriable error response (mimics SDK flush error).
    static ObjectStorageResponse make_retriable_error() {
        return {.status = {.code = ErrorCode::INTERNAL_ERROR,
                           .msg = "Failed to flush response stream (eof: 0, bad: 1)"},
                .http_code = -1,
                .request_id = "",
                .error_type = 1, // INTERNAL_FAILURE
                .is_retriable = true};
    }

    // Build a non-retriable error response (e.g., NOT_FOUND).
    static ObjectStorageResponse make_not_found_error() {
        return {.status = {.code = ErrorCode::NOT_FOUND, .msg = "NoSuchKey"},
                .http_code = 404,
                .request_id = "req-123",
                .error_type = -1,
                .is_retriable = false};
    }

    // Build a 429 too-many-requests response.
    static ObjectStorageResponse make_429_error() {
        return {.status = {.code = ErrorCode::INTERNAL_ERROR, .msg = "SlowDown"},
                .http_code = 429,
                .request_id = "req-456",
                .error_type = -1,
                .is_retriable = false};
    }
};

// Transient error on first call, success on retry.
TEST_F(S3FileReaderRetryTest, transient_error_retry_succeeds) {
    auto [fake, reader] = make_reader(64);

    // First call: retriable error. Second call: default success.
    fake->push_get_response(make_retriable_error());

    char buf[64];
    size_t bytes_read = 0;
    auto st = reader->read_at(0, Slice(buf, 64), &bytes_read);

    EXPECT_TRUE(st.ok()) << st;
    EXPECT_EQ(bytes_read, 64);
    EXPECT_EQ(fake->get_object_call_count(), 2); // 1 fail + 1 success
}

// Transient error exceeds max_transient_retries (3), should fail.
TEST_F(S3FileReaderRetryTest, transient_error_retry_exhausted) {
    auto [fake, reader] = make_reader(64);

    // Push 4 retriable errors (max_transient_retries = 3, so 4th is not retried)
    for (int i = 0; i < 4; i++) {
        fake->push_get_response(make_retriable_error());
    }

    char buf[64];
    size_t bytes_read = 0;
    auto st = reader->read_at(0, Slice(buf, 64), &bytes_read);

    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.msg().find("Failed to flush") != std::string::npos) << st.msg();
    // 1 initial + 3 retries = 4 calls
    EXPECT_EQ(fake->get_object_call_count(), 4);
}

// NOT_FOUND error should NOT be retried.
TEST_F(S3FileReaderRetryTest, not_found_error_no_retry) {
    auto [fake, reader] = make_reader(64);

    fake->push_get_response(make_not_found_error());

    char buf[64];
    size_t bytes_read = 0;
    auto st = reader->read_at(0, Slice(buf, 64), &bytes_read);

    EXPECT_FALSE(st.ok());
    EXPECT_EQ(fake->get_object_call_count(), 1); // No retry
}

// is_retriable=false should NOT trigger transient retry even with http_code=-1.
TEST_F(S3FileReaderRetryTest, non_retriable_internal_error_no_retry) {
    auto [fake, reader] = make_reader(64);

    ObjectStorageResponse resp = {
            .status = {.code = ErrorCode::INTERNAL_ERROR, .msg = "some other error"},
            .http_code = -1,
            .error_type = 1, // INTERNAL_FAILURE
            .is_retriable = false};
    fake->push_get_response(resp);

    char buf[64];
    size_t bytes_read = 0;
    auto st = reader->read_at(0, Slice(buf, 64), &bytes_read);

    EXPECT_FALSE(st.ok());
    EXPECT_EQ(fake->get_object_call_count(), 1); // No retry
}

// Verify .append("failed to read") is removed: error message should NOT contain
// the double "failed to read" suffix.
TEST_F(S3FileReaderRetryTest, error_message_no_append_failed_to_read) {
    auto [fake, reader] = make_reader(64);

    ObjectStorageResponse resp = {
            .status = {.code = ErrorCode::INTERNAL_ERROR,
                       .msg = "failed to read from test-key: err code=-1 type=1, request_id="},
            .http_code = -1,
            .error_type = 1,
            .is_retriable = false};
    fake->push_get_response(resp);

    char buf[64];
    size_t bytes_read = 0;
    auto st = reader->read_at(0, Slice(buf, 64), &bytes_read);

    EXPECT_FALSE(st.ok());
    // The old code appended "failed to read" which produced "request_id=failed to read".
    // Verify the error message is passed through without extra append.
    EXPECT_TRUE(st.msg().find("request_id=failed to read") == std::string::npos) << st.msg();
}

} // namespace doris::io
