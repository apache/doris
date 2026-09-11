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

#include <cstdio>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "io/file_factory.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"

namespace doris::io {
namespace {

class EofSensitiveRangeHandler final : public HttpHandler {
public:
    explicit EofSensitiveRangeHandler(std::string content) : _content(std::move(content)) {}

    void handle(HttpRequest* req) override {
        if (req->method() == HttpMethod::HEAD) {
            req->add_output_header(HttpHeaders::CONTENT_LENGTH,
                                   std::to_string(_content.size()).c_str());
            HttpChannel::send_reply(req);
            return;
        }

        const std::string range = req->header(HttpHeaders::RANGE);
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _ranges.push_back(range);
        }

        unsigned long long begin = 0;
        unsigned long long end = 0;
        if (std::sscanf(range.c_str(), "bytes=%llu-%llu", &begin, &end) != 2 || begin > end ||
            end >= _content.size()) {
            HttpChannel::send_reply(req, _content);
            return;
        }

        const std::string content_range = "bytes " + std::to_string(begin) + "-" +
                                          std::to_string(end) + "/" +
                                          std::to_string(_content.size());
        req->add_output_header(HttpHeaders::CONTENT_RANGE, content_range.c_str());
        HttpChannel::send_reply(req, HttpStatus::PARTIAL_CONTENT,
                                _content.substr(begin, end - begin + 1));
    }

    std::vector<std::string> ranges() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _ranges;
    }

private:
    std::string _content;
    std::mutex _mutex;
    std::vector<std::string> _ranges;
};

} // namespace

TEST(HttpFileReaderTest, ChunkResponseDisablesFileCache) {
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

TEST(HttpFileReaderTest, RangeReadDoesNotCrossKnownEof) {
    const std::string content = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    EofSensitiveRangeHandler handler(content);
    EvHttpServer server(0);
    ASSERT_TRUE(server.register_handler(HEAD, "/file", &handler));
    ASSERT_TRUE(server.register_handler(GET, "/file", &handler));
    server.start();
    ASSERT_NE(server.get_real_port(), 0);

    const std::string url = "http://127.0.0.1:" + std::to_string(server.get_real_port()) + "/file";
    auto reader = HttpFileReader::create(url, {}, FileReaderOptions::DEFAULT, nullptr);
    ASSERT_TRUE(reader.has_value()) << reader.error();

    constexpr size_t tail_size = 16;
    char output[tail_size];
    size_t bytes_read = 0;
    Status status = reader.value()->read_at(content.size() - tail_size, Slice(output, tail_size),
                                            &bytes_read);

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(bytes_read, tail_size);
    EXPECT_EQ(std::string(output, bytes_read), content.substr(content.size() - tail_size));
    EXPECT_EQ(handler.ranges(), (std::vector<std::string> {"bytes=0-0", "bytes=48-63"}));
}

} // namespace doris::io
