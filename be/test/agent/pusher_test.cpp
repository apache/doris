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

#include <ctime>
#include <memory>
#include "boost/filesystem.hpp"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "agent/mock_file_downloader.h"
#include "agent/mock_utils.h"
#include "agent/pusher.h"
#include "olap/mock_command_executor.h"
#include "olap/olap_define.h"
#include "olap/olap_table.h"
#include "util/logging.h"

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnPointee;
using ::testing::SetArgPointee;
using std::string;
using std::vector;

namespace doris {

MockFileDownloader::MockFileDownloader(const FileDownloaderParam& param):FileDownloader(param) {
}

TEST(PusherTest, TestInit) {
    TPushReq push_req;
    push_req.tablet_id = 1;
    push_req.schema_hash = 12345;
    Pusher pusher(nullptr, push_req);

    OLAPEngine* tmp = NULL;
    MockCommandExecutor mock_command_executor;
    tmp = pusher._engine;
    pusher._engine = &mock_command_executor;

    OLAPTable* olap_table = NULL;
    // not init, can not get olap table
    EXPECT_CALL(mock_command_executor, get_table(1, 12345))
            .Times(1)
            .WillOnce(Return(std::shared_ptr<OLAPTable>(olap_table)));
    AgentStatus ret = pusher.init();
    EXPECT_EQ(DORIS_PUSH_INVALID_TABLE, ret);

    // not init, can get olap table, and empty remote path
    olap_table = new OLAPTable(new OLAPHeader("./test_data/header"), nullptr);
    EXPECT_CALL(mock_command_executor, get_table(1, 12345))
            .Times(1)
            .WillOnce(Return(std::shared_ptr<OLAPTable>(olap_table)));
    ret = pusher.init();
    EXPECT_EQ(DORIS_SUCCESS, ret);
    EXPECT_TRUE(pusher._is_init);

    // has inited
    ret = pusher.init();
    EXPECT_EQ(DORIS_SUCCESS, ret);
    pusher._engine = tmp;

    // not inited, remote path not empty
    string http_file_path = "http://xx";
    string root_path_name = "./test_data/data";
    olap_table = new OLAPTable(new OLAPHeader("./test_data/header"), nullptr);
    push_req.__set_http_file_path(http_file_path);
    Pusher pusher2(nullptr, push_req);
    tmp = pusher2._engine;
    pusher2._engine = &mock_command_executor;
    olap_table->_storage_root_path = root_path_name;
    EXPECT_CALL(mock_command_executor, get_table(1, 12345))
            .Times(1)
            .WillOnce(Return(std::shared_ptr<OLAPTable>(olap_table)));
    ret = pusher2.init();
    EXPECT_EQ(DORIS_SUCCESS, ret);
    EXPECT_TRUE(pusher2._is_init);
    EXPECT_STREQ(http_file_path.c_str(), pusher2._downloader_param.remote_file_path.c_str());
    EXPECT_EQ(0, strncmp(
            pusher2._downloader_param.local_file_path.c_str(),
            root_path_name.c_str(),
            strlen(root_path_name.c_str())));

    pusher2._engine = tmp;
}

TEST(PusherTest, TestGetTmpFileDir) {
    TPushReq push_req;
    Pusher pusher(nullptr, push_req);

    // download path not exist
    string root_path = "./test_data/dpp_download_file";
    string download_path;
    AgentStatus ret = pusher._get_tmp_file_dir(root_path, &download_path);
    EXPECT_EQ(DORIS_SUCCESS, ret);
    EXPECT_STREQ("./test_data/dpp_download_file/dpp_download", download_path.c_str());

    // download path exist
    ret = pusher._get_tmp_file_dir(root_path, &download_path);
    EXPECT_EQ(DORIS_SUCCESS, ret);
}

TEST(PusherTest, TestDownloadFile){
    TPushReq push_req;
    Pusher pusher(nullptr, push_req);

    // download success
    FileDownloader::FileDownloaderParam param;
    MockFileDownloader mock_file_downloader(param);
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(1)
            .WillOnce(Return(DORIS_SUCCESS));
    pusher._file_downloader = &mock_file_downloader;
    AgentStatus ret = pusher._download_file();
    EXPECT_EQ(DORIS_SUCCESS, ret);

    // download failed
    EXPECT_CALL(mock_file_downloader, download_file())
            .Times(1)
            .WillOnce(Return(DORIS_ERROR));
    ret = pusher._download_file();
    EXPECT_EQ(DORIS_ERROR, ret);
}

TEST(PusherTest, TestGetFileNameFromPath) {
    TPushReq push_req;
    Pusher pusher(nullptr, push_req);

    string file_path = "/file_path/file_name";
    string file_name;
    pusher._get_file_name_from_path(file_path, &file_name);
    EXPECT_EQ(0, strncmp(file_name.c_str(), "file_name_", 10));
}

TEST(PusherTest, TestProcess) {
    TPushReq push_req;
    Pusher pusher(nullptr, push_req);
    vector<TTabletInfo> tablet_infos;

    // not init
    AgentStatus ret = pusher.process(&tablet_infos);
    EXPECT_EQ(DORIS_ERROR, ret);

    // init, remote file empty, push success, delete download file
    pusher._is_init = true;
    pusher._downloader_param.local_file_path = "./test_data/download_file";
    MockCommandExecutor mock_command_executor;
    OLAPEngine* tmp;
    tmp = pusher._engine;
    pusher._engine = &mock_command_executor;
    EXPECT_CALL(mock_command_executor, push(push_req, &tablet_infos))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));

    FILE* fp = fopen(pusher._downloader_param.local_file_path.c_str(), "w");
    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }
    boost::filesystem::path download_file_path(pusher._downloader_param.local_file_path);
    EXPECT_TRUE(boost::filesystem::exists(download_file_path));

    ret = pusher.process(&tablet_infos);
    EXPECT_EQ(DORIS_SUCCESS, ret);
    EXPECT_FALSE(boost::filesystem::exists(download_file_path));

    // init, remote file empty, push failed, delete download file
    EXPECT_CALL(mock_command_executor, push(push_req, &tablet_infos))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));

    fp = fopen(pusher._downloader_param.local_file_path.c_str(), "w");
    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }
    EXPECT_TRUE(boost::filesystem::exists(download_file_path));

    ret = pusher.process(&tablet_infos);
    EXPECT_EQ(DORIS_ERROR, ret);
    EXPECT_FALSE(boost::filesystem::exists(download_file_path));

    pusher._engine = tmp;

    // init, remote file not empty, not set file length
    push_req.__set_http_file_path("http://xxx");
    Pusher pusher2(nullptr, push_req);
    pusher2._is_init = true;
    FileDownloader::FileDownloaderParam param;
    MockFileDownloader mock_file_downloader(param);
    pusher2._file_downloader = &mock_file_downloader;

    // init, remote file not empty, get remote file length success, timeout
    time_t now = time(NULL);
    pusher2._push_req.timeout = now - 100;
    ret = pusher2.process(&tablet_infos);
    EXPECT_EQ(DORIS_PUSH_TIME_OUT, ret);

    // init, remote file not empty, get remote file length success, download file failed
    now = time(NULL);
    pusher2._push_req.timeout = now + 100;
    pusher2._download_status = DORIS_ERROR;
    ret = pusher2.process(&tablet_infos);
    EXPECT_EQ(DORIS_ERROR, ret);

    // init, remote file not empty, get remote file length success, download file success
    // size diff
    string file_path = "./test_data/download_file";
    fp = fopen(file_path.c_str(), "w");
    fputs("doris be test", fp);
    fclose(fp);
    boost::filesystem::path local_file_path(file_path);
    uint64_t local_file_size = boost::filesystem::file_size(local_file_path);
    now = time(NULL);
    pusher2._push_req.timeout = now + 100;
    pusher2._download_status = DORIS_SUCCESS;
    pusher2._push_req.__set_http_file_size(local_file_size + 1);
    pusher2._downloader_param.local_file_path = file_path;
    ret = pusher2.process(&tablet_infos);
    EXPECT_EQ(DORIS_FILE_DOWNLOAD_FAILED, ret);

    // init, remote file not empty, get remote file length success, download file success
    // size same, push failed
    fp = fopen(file_path.c_str(), "w");
    fputs("doris be test", fp);
    fclose(fp);
    now = time(NULL);
    tmp = pusher2._engine;
    pusher2._engine = &mock_command_executor;
    pusher2._push_req.timeout = now + 100;
    pusher2._push_req.__set_http_file_size(local_file_size);
    EXPECT_CALL(mock_command_executor, push(_, &tablet_infos))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_ERR_OTHER_ERROR));
    ret = pusher2.process(&tablet_infos);
    EXPECT_EQ(DORIS_ERROR, ret);

    // init, remote file not empty, get remote file length success, download file success
    // size same, push success
    fp = fopen(file_path.c_str(), "w");
    fputs("doris be test", fp);
    fclose(fp);
    now = time(NULL);
    pusher2._push_req.timeout = now + 100;
    EXPECT_CALL(mock_command_executor, push(_, &tablet_infos))
            .Times(1)
            .WillOnce(Return(OLAPStatus::OLAP_SUCCESS));
    ret = pusher2.process(&tablet_infos);
    EXPECT_EQ(DORIS_SUCCESS, ret);

    pusher2._engine = tmp;
}

}  // namespace doris

int main(int argc, char **argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
