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

#include "util/broker_storage_backend.h"

#include <gtest/gtest.h>

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fstream>
#include <map>
#include <string>

#include "runtime/exec_env.h"
#include "util/file_utils.h"
#include "util/storage_backend.h"

namespace doris {
static const std::string AK = "AK";
static const std::string SK = "SK";
static const std::string ENDPOINT = "http://bj.bcebos.com";
static const std::string BUCKET = "bos://yang-repo/";
static const std::string BROKER_IP = "127.0.0.1";

#define StorageBackendTest DISABLED_StorageBackendTest
class StorageBackendTest : public testing::Test {
public:
    StorageBackendTest()
            : _broker_properties({{"bos_accesskey", AK},
                                  {"bos_secret_accesskey", SK},
                                  {"bos_endpoint", ENDPOINT}}),
              _env(ExecEnv::GetInstance()) {
        _broker_addr.__set_hostname("127.0.0.1");
        _broker_addr.__set_port(8111);
        _broker.reset(new BrokerStorageBackend(_env, _broker_addr, _broker_properties));
        _broker_base_path = BUCKET + "broker/" + gen_uuid();
    }
    virtual ~StorageBackendTest() {}

protected:
    virtual void SetUp() {
        _test_file = "/tmp/" + gen_uuid();
        std::ofstream out(_test_file);
        out << _content;
        out.close();
    }
    virtual void TearDown() { remove(_test_file.c_str()); }
    std::string gen_uuid() {
        auto id = boost::uuids::random_generator()();
        return boost::uuids::to_string(id);
    }
    std::unique_ptr<BrokerStorageBackend> _broker;
    std::map<std::string, std::string> _broker_properties;
    std::string _test_file;
    ExecEnv* _env;
    std::string _broker_base_path;
    TNetworkAddress _broker_addr;
    std::string _content =
            "O wild West Wind, thou breath of Autumn's being\n"
            "Thou, from whose unseen presence the leaves dead\n"
            "Are driven, like ghosts from an enchanter fleeing,\n"
            "Yellow, and black, and pale, and hectic red,\n"
            "Pestilence-stricken multitudes:O thou\n"
            "Who chariotest to their dark wintry bed\n"
            "The winged seeds, where they lie cold and low,\n"
            "Each like a corpse within its grave, until\n"
            "Thine azure sister of the Spring shall blow\n"
            "Her clarion o'er the dreaming earth, and fill\n"
            "(Driving sweet buds like flocks to feed in air)\n"
            "With living hues and odors plain and hill:\n"
            "Wild Spirit, which art moving everywhere;\n"
            "Destroyer and preserver; hear, oh, hear!";
};

TEST_F(StorageBackendTest, broker_upload) {
    Status status = _broker->upload(_test_file, _broker_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _broker->exist(_broker_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _broker->download(_broker_base_path + "/Ode_to_the_West_Wind.txt",
                               _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
    status = _broker->upload(_test_file + "_not_found",
                             _broker_base_path + "/Ode_to_the_West_Wind1.txt");
    EXPECT_FALSE(status.ok());
    status = _broker->exist(_broker_base_path + "/Ode_to_the_West_Wind1.txt");
    EXPECT_EQ(TStatusCode::NOT_FOUND, status.code());
}

TEST_F(StorageBackendTest, broker_direct_upload) {
    Status status =
            _broker->direct_upload(_broker_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _broker->exist(_broker_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _broker->download(_broker_base_path + "/Ode_to_the_West_Wind.txt",
                               _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
}

TEST_F(StorageBackendTest, broker_download) {
    Status status = _broker->upload(_test_file, _broker_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _broker->download(_broker_base_path + "/Ode_to_the_West_Wind.txt",
                               _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
    status = _broker->download(_broker_base_path + "/Ode_to_the_West_Wind.txt.not_found",
                               _test_file + ".download");
    EXPECT_FALSE(status.ok());
    status = _broker->download(_broker_base_path + "/Ode_to_the_West_Wind.txt.not_found",
                               "/not_permitted.download");
    EXPECT_FALSE(status.ok());
}

TEST_F(StorageBackendTest, broker_rename) {
    Status status =
            _broker->direct_upload(_broker_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _broker->rename(_broker_base_path + "/Ode_to_the_West_Wind.txt",
                             _broker_base_path + "/Ode_to_the_West_Wind.txt.new");
    EXPECT_TRUE(status.ok());
    // rm by broker old file may exist for a few moment
    // status = _broker->exist(_broker_base_path + "/Ode_to_the_West_Wind.txt");
    // EXPECT_TRUE(status.code() == TStatusCode::NOT_FOUND);
    status = _broker->exist(_broker_base_path + "/Ode_to_the_West_Wind.txt.new");
    EXPECT_TRUE(status.ok());
}

TEST_F(StorageBackendTest, broker_list) {
    Status status =
            _broker->direct_upload(_broker_base_path + "/Ode_to_the_West_Wind.md5", _content);
    EXPECT_TRUE(status.ok());
    status = _broker->direct_upload(_broker_base_path + "/Ode_to_the_West_Wind1.md5", _content);
    EXPECT_TRUE(status.ok());
    status = _broker->direct_upload(_broker_base_path + "/Ode_to_the_West_Wind2.md5", _content);
    EXPECT_TRUE(status.ok());
    std::map<std::string, FileStat> files;
    status = _broker->list(_broker_base_path, true, false, &files);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind") != files.end());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind1") != files.end());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind2") != files.end());
    EXPECT_EQ(3, files.size());
}

TEST_F(StorageBackendTest, broker_rm) {
    Status status =
            _broker->direct_upload(_broker_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _broker->exist(_broker_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _broker->rm(_broker_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _broker->exist(_broker_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.code() == TStatusCode::NOT_FOUND);
}

} // end namespace doris
