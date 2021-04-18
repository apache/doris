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

#include "runtime/small_file_mgr.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>

#include "common/logging.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "runtime/exec_env.h"

int main(int argc, char* argv[]);

namespace doris {

std::string g_download_path = "./be/test/runtime/test_data/small_file/downloaded/";
std::string g_src_path = "./be/test/runtime/test_data/small_file/source/";
std::string g_md5_12345 = "5dd39cab1c53c2c77cd352983f9641e1";
std::string g_md5_67890 = "a06e26ae5511b0acea8273cf180ca7bf";
int64_t g_file_12345 = 12345L; // ready to be downloaded file
int64_t g_file_67890 = 67890L; // already exist file
std::string src_file = "small_file.txt";

class SmallFileMgrTestHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        auto it = req->query_params().find("file_id");
        if (it == req->query_params().end()) {
            HttpChannel::send_error(req, INTERNAL_SERVER_ERROR);
            return;
        }
        if (std::stol(it->second) != g_file_12345) {
            HttpChannel::send_error(req, INTERNAL_SERVER_ERROR);
            return;
        }

        std::string file_path = g_src_path + "/small_file.txt";
        FILE* fp = fopen(file_path.c_str(), "r");
        if (fp == nullptr) {
            HttpChannel::send_error(req, INTERNAL_SERVER_ERROR);
            return;
        }
        std::string response;
        char buf[1024];
        while (true) {
            auto size = fread(buf, 1, 1024, fp);
            response.append(buf, size);
            if (size < 1024) {
                break;
            }
        }
        HttpChannel::send_reply(req, response);
        fclose(fp);
    }
};

static SmallFileMgrTestHandler s_test_handler = SmallFileMgrTestHandler();
static EvHttpServer* s_server = nullptr;
static int real_port = 0;

class SmallFileMgrTest : public testing::Test {
public:
    SmallFileMgrTest() {}
    virtual ~SmallFileMgrTest() {}

    static void SetUpTestCase() {
        s_server = new EvHttpServer(0);
        s_server->register_handler(GET, "/api/get_small_file", &s_test_handler);
        s_server->start();
        real_port = s_server->get_real_port();
        ASSERT_NE(0, real_port);
    }

    static void TearDownTestCase() {
        delete s_server;
        std::stringstream ss;
        ss << g_download_path << "/" << g_file_12345 << "." << g_md5_12345;
        remove(ss.str().c_str());
    }

    void SetUp() override {}
};

TEST_F(SmallFileMgrTest, test_get_file) {
    TNetworkAddress addr;
    addr.__set_hostname("http://127.0.0.1");
    TMasterInfo info;
    info.__set_network_address(addr);
    info.__set_http_port(real_port);
    ExecEnv* env = ExecEnv::GetInstance();
    env->_master_info = &info;
    SmallFileMgr mgr(env, g_download_path);
    Status st = mgr.init();
    ASSERT_TRUE(st.ok());

    // get already exist file
    std::string file_path;
    st = mgr.get_file(g_file_67890, g_md5_67890, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();
    std::stringstream expected;
    expected << g_download_path << "/" << g_file_67890 << "." << g_md5_67890;
    ASSERT_EQ(expected.str(), file_path);

    // get ready to be downloaded file
    std::string file_path_2;
    st = mgr.get_file(g_file_12345, g_md5_12345, &file_path_2);
    ASSERT_TRUE(st.ok()) << st.to_string();
    std::stringstream expected2;
    expected2 << g_download_path << "/" << g_file_12345 << "." << g_md5_12345;
    ASSERT_EQ(expected2.str(), file_path_2);

    // get wrong file
    std::string file_path_3;
    st = mgr.get_file(13579, "xxxx", &file_path_3);
    ASSERT_TRUE(!st.ok());
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
