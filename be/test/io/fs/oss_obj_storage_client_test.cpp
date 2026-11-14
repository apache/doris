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

#include "cpp/obj-client/oss_obj_storage_client.h"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <gtest/gtest.h>

#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"

namespace doris {
namespace {

class OssHandler final : public HttpHandler {
public:
    void handle(HttpRequest* request) override {
        EXPECT_EQ(request->header(HttpHeaders::AUTHORIZATION).find("OSS4-HMAC-SHA256"), 0);
        if (request->uri().find("lifecycle") != std::string::npos) {
            auto rule = [](std::string prefix, int days, std::string status = "Enabled",
                           std::string filter = "") {
                return "<Rule><Status>" + status + "</Status><Prefix>" + prefix + "</Prefix>" +
                       filter + "<NoncurrentVersionExpiration><NoncurrentDays>" +
                       std::to_string(days) +
                       "</NoncurrentDays></NoncurrentVersionExpiration></Rule>";
            };
            HttpChannel::send_reply(
                    request, "<LifecycleConfiguration>" + rule("vault/cold/", 7) +
                                     rule("vault/", 30) + rule("vault/", 1, "Disabled") +
                                     rule("vault/", 2, "Enabled",
                                          "<Filter><Not><Prefix>vault/hot/</Prefix></Not>"
                                          "<Not><Prefix>vault/cold/</Prefix></Not></Filter>") +
                                     rule("vault-backup/", 90) + "</LifecycleConfiguration>");
        } else if (request->uri().find("versioning") != std::string::npos) {
            HttpChannel::send_reply(request,
                                    "<VersioningConfiguration><Status>Enabled</Status>"
                                    "</VersioningConfiguration>");
        } else if (request->header(HttpHeaders::RANGE) == "bytes=2-4") {
            request->add_output_header(HttpHeaders::CONTENT_RANGE, "bytes 2-4/6");
            HttpChannel::send_reply(request, HttpStatus::PARTIAL_CONTENT, "cde");
        } else {
            HttpChannel::send_reply(
                    request, HttpStatus::NOT_FOUND,
                    "<Error><Code>NoSuchKey</Code><Message>Missing</Message></Error>");
        }
    }
};

TEST(OssObjStorageClientTest, NativeRangeReadAndRecyclerBucketInspection) {
    OssHandler handler;
    EvHttpServer server(0);
    ASSERT_TRUE(server.register_handler(GET, "/bucket/key", &handler));
    ASSERT_TRUE(server.register_handler(GET, "/bucket/", &handler));
    server.start();
    Aws::Client::ClientConfiguration config;
    config.endpointOverride = "http://127.0.0.1:" + std::to_string(server.get_real_port());
    config.region = "cn-hangzhou";
    OssObjStorageClient client(config,
                               std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
                                       "test-key", "test-secret", "test-token"),
                               false, 0);
    ObjStoragePath path {.bucket = "bucket", .key = "key"};
    char buffer[3];
    size_t count = 0;
    auto read = client.get_object(path, buffer, 2, sizeof(buffer), &count);
    ASSERT_TRUE(read.ok()) << read.status.msg;
    EXPECT_EQ(std::string(buffer, count), "cde");
    EXPECT_EQ(client.get_object(path, buffer, 9, 3, &count).status.code,
              ObjStorageStatus::NOT_FOUND);
    auto version = client.check_versioning("bucket");
    ASSERT_TRUE(version.ok()) << version.status.msg;
    int64_t days = 0;
    auto lifecycle = [&](std::string prefix) {
        return client.get_lifecycle({.bucket = "bucket", .prefix = std::move(prefix)}, &days);
    };
    ASSERT_TRUE(lifecycle("vault/cold/tablets/").ok());
    EXPECT_EQ(days, 7);
    ASSERT_TRUE(lifecycle("vault/hot/").ok());
    EXPECT_EQ(days, 30);
    ASSERT_TRUE(lifecycle("vault/").ok());
    EXPECT_EQ(days, 2);
    ASSERT_TRUE(lifecycle("vault-backup/").ok());
    EXPECT_EQ(days, 90);
    ASSERT_TRUE(lifecycle("").ok());
    EXPECT_EQ(days, 2);
    EXPECT_FALSE(lifecycle("other/").ok());
    auto url = client.generate_presigned_url(path, 60);
    EXPECT_NE(url.find("x-oss-signature="), std::string::npos);
    EXPECT_NE(url.find("x-oss-security-token=test-token"), std::string::npos);
}

} // namespace
} // namespace doris
