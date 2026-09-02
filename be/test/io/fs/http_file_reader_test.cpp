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

#include "io/fs/http_file_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <string>

#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "util/slice.h"

namespace doris::io {

TEST(HttpFileReaderFactoryTest, ChunkResponseDisablesFileCache) {
    FileSystemProperties properties;
    properties.system_type = TFileType::FILE_HTTP;
    properties.properties = {{"http.enable.chunk.response", "true"}};
    FileDescription file_description;
    file_description.path = "http://127.0.0.1/stream";
    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;

    auto reader = FileFactory::create_file_reader(properties, file_description, opts);

    ASSERT_TRUE(reader.has_value()) << reader.error();
    EXPECT_NE(std::dynamic_pointer_cast<HttpFileReader>(reader.value()), nullptr);
}

// Full file content served by the test handlers.
static const std::string kFileContent = "0123456789abcdefghij"; // 20 bytes

static void serve_file_get(HttpRequest* req) {
    const std::string& range = req->header(HttpHeaders::RANGE);
    if (range.empty()) {
        HttpChannel::send_reply(req, HttpStatus::OK, kFileContent);
        return;
    }
    // Parse "bytes=start-end".
    size_t eq = range.find('=');
    size_t dash = range.find('-', eq + 1);
    size_t start = std::stoul(range.substr(eq + 1, dash - eq - 1));
    std::string end_str = range.substr(dash + 1);
    size_t end = end_str.empty() ? kFileContent.size() - 1 : std::stoul(end_str);
    end = std::min(end, kFileContent.size() - 1);
    std::string body = kFileContent.substr(start, end - start + 1);

    std::string content_range = "bytes " + std::to_string(start) + "-" + std::to_string(end) + "/" +
                                std::to_string(kFileContent.size());
    req->add_output_header(HttpHeaders::CONTENT_RANGE, content_range.c_str());
    HttpChannel::send_reply(req, HttpStatus::PARTIAL_CONTENT, body);
}

// Simulates a presigned URL that is signed for GET only:
// - HEAD is rejected with 403 (as object stores do when the signature covers the method).
// - GET (with or without Range) succeeds. Range requests get 206 + Content-Range.
// This reproduces the reported bug where the HEAD-based probe failed with 403.
class PresignedGetOnlyHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        if (req->method() == HttpMethod::HEAD) {
            HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, "forbidden");
            return;
        }
        serve_file_get(req);
    }
};

// HEAD succeeds but omits Content-Length, so BE must still use the ranged GET fallback.
class HeadWithoutContentLengthHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        if (req->method() == HttpMethod::HEAD) {
            HttpChannel::send_reply(req, HttpStatus::OK, "");
            return;
        }
        serve_file_get(req);
    }
};

// Returns a configured Content-Range response after rejecting HEAD.
class ProbeResponseHandler : public HttpHandler {
public:
    ProbeResponseHandler(HttpStatus status, std::string content_range)
            : _status(status), _content_range(std::move(content_range)) {}

    void handle(HttpRequest* req) override {
        if (req->method() == HttpMethod::HEAD) {
            HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, "forbidden");
            return;
        }
        if (!_content_range.empty()) {
            req->add_output_header(HttpHeaders::CONTENT_RANGE, _content_range.c_str());
        }
        HttpChannel::send_reply(req, _status, _status == HttpStatus::PARTIAL_CONTENT ? "0" : "");
    }

private:
    HttpStatus _status;
    std::string _content_range;
};

// A resource that is genuinely forbidden for all methods (HEAD and GET both fail).
class AlwaysForbiddenHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, "forbidden");
    }
};

// A well-behaved resource: HEAD returns 200 + Content-Length, and ranged GET returns
// 206 + Content-Range. Exercises the normal (non-fallback) open() path.
class NormalHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        if (req->method() == HttpMethod::HEAD) {
            req->add_output_header(HttpHeaders::CONTENT_LENGTH,
                                   std::to_string(kFileContent.size()).c_str());
            HttpChannel::send_reply(req, HttpStatus::OK, "");
            return;
        }
        serve_file_get(req);
    }
};

class HttpFileReaderTest : public testing::Test {
public:
    static void SetUpTestCase() {
        s_server = new EvHttpServer(0);
        s_server->register_handler(HttpMethod::HEAD, "/presigned", &s_presigned_handler);
        s_server->register_handler(HttpMethod::GET, "/presigned", &s_presigned_handler);
        s_server->register_handler(HttpMethod::HEAD, "/forbidden", &s_forbidden_handler);
        s_server->register_handler(HttpMethod::GET, "/forbidden", &s_forbidden_handler);
        s_server->register_handler(HttpMethod::HEAD, "/normal", &s_normal_handler);
        s_server->register_handler(HttpMethod::GET, "/normal", &s_normal_handler);
        s_server->register_handler(HttpMethod::HEAD, "/head-no-length", &s_head_no_length_handler);
        s_server->register_handler(HttpMethod::GET, "/head-no-length", &s_head_no_length_handler);
        register_probe_handler("/missing-content-range", &s_missing_content_range_handler);
        register_probe_handler("/malformed-content-range", &s_malformed_content_range_handler);
        register_probe_handler("/unknown-content-range", &s_unknown_content_range_handler);
        register_probe_handler("/empty", &s_empty_handler);
        s_server->start();
        s_port = s_server->get_real_port();
        ASSERT_NE(0, s_port);
        s_host = "http://127.0.0.1:" + std::to_string(s_port);
    }

    static void TearDownTestCase() { delete s_server; }

    static void register_probe_handler(const std::string& path, HttpHandler* handler) {
        s_server->register_handler(HttpMethod::HEAD, path, handler);
        s_server->register_handler(HttpMethod::GET, path, handler);
    }

    static EvHttpServer* s_server;
    static int s_port;
    static std::string s_host;
    static PresignedGetOnlyHandler s_presigned_handler;
    static AlwaysForbiddenHandler s_forbidden_handler;
    static NormalHandler s_normal_handler;
    static HeadWithoutContentLengthHandler s_head_no_length_handler;
    static ProbeResponseHandler s_missing_content_range_handler;
    static ProbeResponseHandler s_malformed_content_range_handler;
    static ProbeResponseHandler s_unknown_content_range_handler;
    static ProbeResponseHandler s_empty_handler;
};

EvHttpServer* HttpFileReaderTest::s_server = nullptr;
int HttpFileReaderTest::s_port = 0;
std::string HttpFileReaderTest::s_host;
PresignedGetOnlyHandler HttpFileReaderTest::s_presigned_handler;
AlwaysForbiddenHandler HttpFileReaderTest::s_forbidden_handler;
NormalHandler HttpFileReaderTest::s_normal_handler;
HeadWithoutContentLengthHandler HttpFileReaderTest::s_head_no_length_handler;
ProbeResponseHandler HttpFileReaderTest::s_missing_content_range_handler(
        HttpStatus::PARTIAL_CONTENT, "");
ProbeResponseHandler HttpFileReaderTest::s_malformed_content_range_handler(
        HttpStatus::PARTIAL_CONTENT, "not-a-content-range");
ProbeResponseHandler HttpFileReaderTest::s_unknown_content_range_handler(
        HttpStatus::PARTIAL_CONTENT, "bytes 0-0/*");
ProbeResponseHandler HttpFileReaderTest::s_empty_handler(HttpStatus::REQUESTED_RANGE_NOT_SATISFIED,
                                                         "bytes */0");

// The normal path: HEAD succeeds and returns the size via Content-Length; reads then work
// over Range requests without triggering any fallback.
TEST_F(HttpFileReaderTest, OpenUsesHeadSizeWhenHeadSucceeds) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/normal", props, opts, nullptr);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto reader = res.value();

    EXPECT_EQ(kFileContent.size(), reader->size());

    std::string buf;
    buf.resize(kFileContent.size());
    size_t bytes_read = 0;
    auto st = reader->read_at(0, Slice(buf.data(), buf.size()), &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(kFileContent.size(), bytes_read);
    EXPECT_EQ(kFileContent, buf);

    static_cast<void>(reader->close());
}

// The core regression: a URL whose HEAD returns 403 must still open and read
// via the GET-based fallback, recovering the size from Content-Range.
TEST_F(HttpFileReaderTest, OpenFallsBackToGetWhenHeadForbidden) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/presigned", props, opts, nullptr);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto reader = res.value();

    // Size recovered from the Content-Range header of the ranged GET probe.
    EXPECT_EQ(kFileContent.size(), reader->size());

    // Read the whole file back and verify contents.
    std::string buf;
    buf.resize(kFileContent.size());
    size_t bytes_read = 0;
    auto st = reader->read_at(0, Slice(buf.data(), buf.size()), &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(kFileContent.size(), bytes_read);
    EXPECT_EQ(kFileContent, buf);

    static_cast<void>(reader->close());
}

TEST_F(HttpFileReaderTest, OpenFallsBackToGetWhenHeadHasNoContentLength) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/head-no-length", props, opts, nullptr);
    ASSERT_TRUE(res.has_value()) << res.error();
    EXPECT_EQ(kFileContent.size(), res.value()->size());
    static_cast<void>(res.value()->close());
}

TEST_F(HttpFileReaderTest, OpenRejectsMissingContentRange) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/missing-content-range", props, opts, nullptr);
    ASSERT_FALSE(res.has_value());
    EXPECT_NE(std::string::npos, res.error().to_string().find("Content-Range"));
}

TEST_F(HttpFileReaderTest, OpenRejectsMalformedContentRange) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/malformed-content-range", props, opts, nullptr);
    ASSERT_FALSE(res.has_value());
    EXPECT_NE(std::string::npos, res.error().to_string().find("Content-Range"));
}

TEST_F(HttpFileReaderTest, OpenRejectsUnknownContentRangeTotal) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/unknown-content-range", props, opts, nullptr);
    ASSERT_FALSE(res.has_value());
    EXPECT_NE(std::string::npos, res.error().to_string().find("total size"));
}

TEST_F(HttpFileReaderTest, OpenRecognizesEmptyResource) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/empty", props, opts, nullptr);
    ASSERT_TRUE(res.has_value()) << res.error();
    EXPECT_EQ(0, res.value()->size());
    static_cast<void>(res.value()->close());
}

TEST_F(HttpFileReaderTest, OpenUsesPlannedFileSizeWithoutHeadProbe) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    opts.file_size = static_cast<int64_t>(kFileContent.size());
    auto res = HttpFileReader::create(s_host + "/presigned", props, opts, nullptr);
    ASSERT_TRUE(res.has_value()) << res.error();
    EXPECT_EQ(kFileContent.size(), res.value()->size());
    static_cast<void>(res.value()->close());
}

// A resource forbidden for every method (GET included) must fail to open,
// rather than being silently swallowed.
TEST_F(HttpFileReaderTest, OpenFailsWhenAllMethodsForbidden) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/forbidden", props, opts, nullptr);
    EXPECT_FALSE(res.has_value());
}

// Reading a sub-range must also work through the fallback path.
TEST_F(HttpFileReaderTest, ReadMiddleRangeAfterHeadForbidden) {
    std::map<std::string, std::string> props;
    FileReaderOptions opts;
    auto res = HttpFileReader::create(s_host + "/presigned", props, opts, nullptr);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto reader = res.value();

    std::string buf;
    buf.resize(5);
    size_t bytes_read = 0;
    auto st = reader->read_at(10, Slice(buf.data(), buf.size()), &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(5, bytes_read);
    EXPECT_EQ(kFileContent.substr(10, 5), buf);

    static_cast<void>(reader->close());
}

} // namespace doris::io
