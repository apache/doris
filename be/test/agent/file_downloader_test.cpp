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

#include <sstream>
#include "curl/curl.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "agent/file_downloader.h"
#include "olap/file_helper.h"
#include "util/logging.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;
using std::stringstream;

namespace palo {

TEST(FileDownloaderTest, TestWriteFileCallback) {
    FileDownloader::FileDownloaderParam param;
    FileDownloader file_downloader(param);
    char buffer[] = {'x', 'y', 'z'};

    // _local_file is NULL
    size_t len = file_downloader._write_file_callback(
            buffer, 
            sizeof(char),
            sizeof(buffer),
            NULL);
    EXPECT_EQ(-1, len);

    // set _local_file
    FileHandler* file_handler = new FileHandler();
    file_handler->open_with_mode("./test_data/local_file",
            O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    len = file_downloader._write_file_callback(
            buffer,
            sizeof(char),
            sizeof(buffer),
            file_handler);
    EXPECT_EQ(3, len);

    delete file_handler;
    file_handler = NULL;
}

TEST(FileDownloaderTest, TestWriteStreamCallback) {
    FileDownloader::FileDownloaderParam param;
    FileDownloader file_downloader(param);
    char buffer[] = {'x', 'y', 'z'};

    // _local_file is NULL
    size_t len = file_downloader._write_stream_callback(
            buffer, 
            sizeof(char),
            sizeof(buffer),
            NULL);
    EXPECT_EQ(-1, len);

    // set _local_file
    stringstream* output_string_stream = new stringstream();
    len = file_downloader._write_stream_callback(
            buffer,
            sizeof(char),
            sizeof(buffer),
            output_string_stream);
    EXPECT_EQ(3, len);

    delete output_string_stream;
    output_string_stream = NULL;
}

TEST(FileDownloaderTest, TestInstallOpt) {
    CURL* curl = curl_easy_init();
    FileDownloader::FileDownloaderParam param;
    param.username = "username";
    param.password = "password";
    param.remote_file_path = "http://xxx";
    param.local_file_path = "./test_data/download_file";
    param.curl_opt_timeout = 100;

    FileDownloader file_downloader(param);
    AgentStatus ret = PALO_SUCCESS;

    // Test for get length
    char errbuf[CURL_ERROR_SIZE];
    ret = file_downloader._install_opt(FileDownloader::OutputType::NONE, curl, errbuf, NULL, NULL);
    EXPECT_EQ(PALO_SUCCESS, ret);
    
    // Test for get dir list
    stringstream* output_string_stream = NULL;
    ret = file_downloader._install_opt(
            FileDownloader::OutputType::STREAM, curl, errbuf, output_string_stream, NULL);
    EXPECT_EQ(PALO_SUCCESS, ret);
    
    // Test for downlod file
    FileHandler* file_handler = new FileHandler();
    ret = file_downloader._install_opt(
            FileDownloader::OutputType::FILE, curl, errbuf, NULL, file_handler);
    EXPECT_EQ(PALO_SUCCESS, ret);
}

}  // namespace palo

int main(int argc, char **argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    palo::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

