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

#include "runtime/user_function_cache.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>

#include "common/logging.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "util/file_utils.h"
#include "util/md5.h"

int main(int argc, char* argv[]);

namespace doris {

bool k_is_downloaded = false;
class UserFunctionTestHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        auto& file_name = req->param("FILE");
        std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/lib";
        auto lib_file = lib_dir + "/" + file_name;
        FILE* fp = fopen(lib_file.c_str(), "r");
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
        k_is_downloaded = true;
        fclose(fp);
    }
};

static UserFunctionTestHandler s_test_handler = UserFunctionTestHandler();
static EvHttpServer* s_server = nullptr;
static int real_port = 0;
static std::string hostname = "";

std::string my_add_md5sum;

static std::string compute_md5(const std::string& file) {
    FILE* fp = fopen(file.c_str(), "r");
    Md5Digest md5;
    char buf[1024];
    while (true) {
        auto size = fread(buf, 1, 1024, fp);
        md5.update(buf, size);
        if (size < 1024) {
            break;
        }
    }
    fclose(fp);
    md5.digest();
    return md5.hex();
}
class UserFunctionCacheTest : public testing::Test {
public:
    UserFunctionCacheTest() {}
    virtual ~UserFunctionCacheTest() {}
    static void SetUpTestCase() {
        s_server = new EvHttpServer(0);
        s_server->register_handler(GET, "/{FILE}", &s_test_handler);
        s_server->start();
        real_port = s_server->get_real_port();
        ASSERT_NE(0, real_port);
        hostname = "http://127.0.0.1:" + std::to_string(real_port);

        // compile code to so
        ASSERT_EQ(system("g++ -shared -fPIC "
                         "./be/test/runtime/test_data/user_function_cache/lib/my_add.cc -o "
                         "./be/test/runtime/test_data/user_function_cache/lib/my_add.so"),
                  0);

        my_add_md5sum =
                compute_md5("./be/test/runtime/test_data/user_function_cache/lib/my_add.so");
    }
    static void TearDownTestCase() {
        delete s_server;
        ASSERT_EQ(system("rm -rf ./be/test/runtime/test_data/user_function_cache/lib/my_add.so"),
                  0);
    }
    void SetUp() override { k_is_downloaded = false; }
};

TEST_F(UserFunctionCacheTest, process_symbol) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/normal";
    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok());
    void* fn_ptr = nullptr;
    UserFunctionCacheEntry* entry = nullptr;
    st = cache.get_function_ptr(0, "main", "", "", &fn_ptr, &entry);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(&main, fn_ptr);
    ASSERT_EQ(nullptr, entry);
    cache.release_entry(entry);
}

TEST_F(UserFunctionCacheTest, download_normal) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/download";
    FileUtils::remove_all(lib_dir);

    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok());
    void* fn_ptr = nullptr;
    UserFunctionCacheEntry* entry = nullptr;
    // get my_add
    st = cache.get_function_ptr(1, "_Z6my_addv", hostname + "/my_add.so", my_add_md5sum, &fn_ptr,
                                &entry);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(k_is_downloaded);
    ASSERT_NE(nullptr, fn_ptr);
    ASSERT_NE(nullptr, entry);

    // get my_del
    st = cache.get_function_ptr(1, "_Z6my_delv", hostname + "/my_add.so", my_add_md5sum, &fn_ptr,
                                &entry);
    ASSERT_TRUE(st.ok());
    ASSERT_NE(nullptr, fn_ptr);
    ASSERT_NE(nullptr, entry);

    // get my_mul
    st = cache.get_function_ptr(1, "_Z6my_mulv", hostname + "/my_add.so", my_add_md5sum, &fn_ptr,
                                &entry);
    ASSERT_FALSE(st.ok());

    cache.release_entry(entry);
}

TEST_F(UserFunctionCacheTest, load_normal) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/download";
    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok());
    void* fn_ptr = nullptr;
    UserFunctionCacheEntry* entry = nullptr;
    st = cache.get_function_ptr(1, "_Z6my_addv", hostname + "/my_add.so", my_add_md5sum, &fn_ptr,
                                &entry);
    ASSERT_TRUE(st.ok());
    ASSERT_FALSE(k_is_downloaded);
    ASSERT_NE(nullptr, fn_ptr);
    ASSERT_NE(nullptr, entry);
    cache.release_entry(entry);
}

TEST_F(UserFunctionCacheTest, download_fail) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/download";
    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok());
    void* fn_ptr = nullptr;
    UserFunctionCacheEntry* entry = nullptr;
    st = cache.get_function_ptr(2, "_Z6my_delv", hostname + "/my_del.so", my_add_md5sum, &fn_ptr,
                                &entry);
    ASSERT_FALSE(st.ok());
}

TEST_F(UserFunctionCacheTest, md5_fail) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/download";
    FileUtils::remove_all(lib_dir);

    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok());
    void* fn_ptr = nullptr;
    UserFunctionCacheEntry* entry = nullptr;
    st = cache.get_function_ptr(1, "_Z6my_addv", hostname + "/my_add.so", "1234", &fn_ptr, &entry);
    ASSERT_FALSE(st.ok());
}

TEST_F(UserFunctionCacheTest, bad_so) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/bad";
    FileUtils::create_dir(lib_dir + "/2");
    auto so_file = lib_dir + "/2/2.abc.so";
    FILE* fp = fopen(so_file.c_str(), "w");
    fwrite(&fp, sizeof(FILE*), 1, fp);
    fclose(fp);
    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok());
    void* fn_ptr = nullptr;
    UserFunctionCacheEntry* entry = nullptr;
    st = cache.get_function_ptr(2, "_Z6my_addv", hostname + "/my_add.so", "abc", &fn_ptr, &entry);
    ASSERT_FALSE(st.ok());
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
