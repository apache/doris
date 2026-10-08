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

#include "io/fs/gcs_file_writer.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "io/file_factory.h"
#include "io/fs/gcs_file_system.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"

namespace doris::io {
namespace {

class ResumableUploadHandler final : public HttpHandler {
public:
    void set_base_url(std::string base_url) { _base_url = std::move(base_url); }

    void handle(HttpRequest* req) override {
        if (req->header(HttpHeaders::AUTHORIZATION) != "Bearer gcs-vended-token") {
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, "missing bearer");
            return;
        }
        if (req->method() == HttpMethod::POST) {
            ++init_requests;
            req->add_output_header("Location", (_base_url + "/upload/session-1").c_str());
            HttpChannel::send_reply(req, HttpStatus::OK);
            return;
        }

        const std::string content_range = req->header("Content-Range");
        ranges.push_back(content_range);
        if (content_range.starts_with("bytes */")) {
            req->add_output_header("Range",
                                   ("bytes=0-" + std::to_string(object.size() - 1)).c_str());
            HttpChannel::send_reply(req, HttpStatus::RESUME_INCOMPLETE);
            return;
        }

        std::string body = req->get_request_body();
        if (!_injected_failure) {
            object.append(body.data(), body.size() / 2);
            _injected_failure = true;
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, "retry");
            return;
        }
        object.append(body);
        if (content_range.ends_with("/*")) {
            req->add_output_header("Range",
                                   ("bytes=0-" + std::to_string(object.size() - 1)).c_str());
            HttpChannel::send_reply(req, HttpStatus::RESUME_INCOMPLETE);
            return;
        }
        HttpChannel::send_reply(req, HttpStatus::OK);
    }

    int init_requests = 0;
    std::vector<std::string> ranges;
    std::string object;

private:
    bool _injected_failure = false;
    std::string _base_url;
};

class GcsFileWriterTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        _server = std::make_unique<EvHttpServer>(0);
        _server->register_handler(POST, "/upload/storage/v1/b/delta-bucket/o", &_handler);
        _server->register_handler(PUT, "/upload/session-1", &_handler);
        _server->start();
        ASSERT_NE(_server->get_real_port(), 0);
        _base_url = "http://127.0.0.1:" + std::to_string(_server->get_real_port());
        _handler.set_base_url(_base_url);
    }

    static void TearDownTestSuite() { _server.reset(); }

protected:
    static std::map<std::string, std::string> properties() {
        return {{"provider", "GCP"},
                {"uri", _base_url},
                {"http.header.Authorization", "Bearer gcs-vended-token"},
                {"AWS_TOKEN_EXPIRATION_TIME_MS", "4102444800000"}};
    }

    inline static std::unique_ptr<EvHttpServer> _server;
    inline static ResumableUploadHandler _handler;
    inline static std::string _base_url;
};

TEST_F(GcsFileWriterTest, resumes_partial_chunk_and_completes_object) {
    constexpr size_t chunk_size = 2 * GcsFileWriter::MIN_CHUNK_SIZE;
    std::string expected(chunk_size, 'a');
    expected += "end";
    auto result = GcsFileWriter::create(Path("gs://delta-bucket/tables/events/part.parquet"),
                                        properties(), chunk_size);
    ASSERT_TRUE(result.has_value()) << result.error();
    FileWriterPtr writer = std::move(result).value();

    ASSERT_TRUE(writer->append(Slice(expected.data(), expected.size())).ok());
    ASSERT_TRUE(writer->close().ok());

    EXPECT_EQ(_handler.init_requests, 1);
    EXPECT_EQ(_handler.object, expected);
    ASSERT_EQ(_handler.ranges.size(), 4);
    EXPECT_EQ(_handler.ranges[0], "bytes 0-524287/*");
    EXPECT_EQ(_handler.ranges[1], "bytes */*");
    EXPECT_EQ(_handler.ranges[2], "bytes 262144-524287/*");
    EXPECT_EQ(_handler.ranges[3], "bytes 524288-524290/524291");
}

TEST_F(GcsFileWriterTest, rejects_expired_token_before_upload) {
    auto expired = properties();
    expired["AWS_TOKEN_EXPIRATION_TIME_MS"] = "1";
    auto result = GcsFileWriter::create(Path("gs://delta-bucket/tables/events/part.parquet"),
                                        expired, GcsFileWriter::MIN_CHUNK_SIZE);
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().to_string().find("expired"), std::string::npos);
}

TEST_F(GcsFileWriterTest, file_factory_routes_gcs_oauth_away_from_s3_signing) {
    auto config = properties();
    FSPropertiesRef fs_properties(TFileType::FILE_S3);
    fs_properties.properties = &config;
    FileDescription description = {.path = "gs://delta-bucket/tables/events/part.parquet",
                                   .fs_name = {}};

    auto result = FileFactory::create_fs(fs_properties, description);

    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_NE(dynamic_cast<GcsFileSystem*>(result->get()), nullptr);
    FileWriterPtr writer;
    ASSERT_TRUE((*result)->create_file(description.path, &writer).ok());
    EXPECT_NE(dynamic_cast<GcsFileWriter*>(writer.get()), nullptr);
}

} // namespace
} // namespace doris::io
