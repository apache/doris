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

#include "plugin/plugin_zip.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <set>

#include "env/env.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "testutil/test_util.h"
#include "util/file_utils.h"
#include "util/slice.h"

namespace doris {
class HttpTestHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string path = GetCurrentRunningDir();
        EXPECT_FALSE(path.empty());

        auto& file_name = req->param("FILE");

        FILE* fp = fopen((path + "/plugin_test/source/" + file_name).c_str(), "r");
        EXPECT_TRUE(fp != nullptr);

        std::string response;
        char f[1024];
        while (true) {
            auto size = fread(f, 1, 1024, fp);
            response.append(f, size);

            if (size < 1024) {
                break;
            }
        }

        fclose(fp);

        HttpChannel::send_reply(req, response);
    }
};

class PluginZipTest : public testing::Test {
public:
    PluginZipTest() {
        _path = GetCurrentRunningDir();
        EXPECT_FALSE(_path.empty());
        _server.reset(new EvHttpServer(29191));
        _server->register_handler(GET, "/{FILE}", &_handler);
        _server->start();
    }

public:
    std::string _path;
    std::unique_ptr<EvHttpServer> _server;
    HttpTestHandler _handler;
};

TEST_F(PluginZipTest, local_normal) {
    FileUtils::remove_all(_path + "/plugin_test/target");

    PluginZip zip(_path + "/plugin_test/source/test.zip");
    EXPECT_TRUE(zip.extract(_path + "/plugin_test/target/", "test").ok());

    EXPECT_TRUE(FileUtils::check_exist(_path + "/plugin_test/target/test"));
    EXPECT_TRUE(FileUtils::check_exist(_path + "/plugin_test/target/test/test.txt"));

    std::unique_ptr<RandomAccessFile> file;
    Env::Default()->new_random_access_file(_path + "/plugin_test/target/test/test.txt", &file);

    char f[11];
    Slice s(f, 11);
    file->read_at(0, &s);

    EXPECT_EQ("hello world", s.to_string());

    FileUtils::remove_all(_path + "/plugin_test/target/");
}

TEST_F(PluginZipTest, http_normal) {
    FileUtils::remove_all(_path + "/plugin_test/target");

    PluginZip zip("http://127.0.0.1:29191/test.zip");

    //    EXPECT_TRUE(zip.extract(_path + "/plugin_test/target/", "test").ok());
    Status st = (zip.extract(_path + "/plugin_test/target/", "test"));
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(FileUtils::check_exist(_path + "/plugin_test/target/test"));
    EXPECT_TRUE(FileUtils::check_exist(_path + "/plugin_test/target/test/test.txt"));

    std::unique_ptr<RandomAccessFile> file;
    Env::Default()->new_random_access_file(_path + "/plugin_test/target/test/test.txt", &file);

    char f[11];
    Slice s(f, 11);
    file->read_at(0, &s);

    EXPECT_EQ("hello world", s.to_string());

    std::set<std::string> dirs;
    std::set<std::string> files;
    EXPECT_TRUE(
            FileUtils::list_dirs_files(_path + "/plugin_test/target", &dirs, &files, Env::Default())
                    .ok());

    EXPECT_EQ(1, dirs.size());
    EXPECT_EQ(1, files.size());

    FileUtils::remove_all(_path + "/plugin_test/target/");
}

TEST_F(PluginZipTest, already_install) {
    // This test case will finish very soon, sleep 100 us to ensure that EvHttpServer worker has started
    // before this unit test case finished, or there may cause an ASAN error.
    usleep(100);
    FileUtils::remove_all(_path + "/plugin_test/target");

    PluginZip zip("http://127.0.0.1:29191/test.zip");
    EXPECT_FALSE(zip.extract(_path + "/plugin_test/", "source").ok());
}

} // namespace doris
