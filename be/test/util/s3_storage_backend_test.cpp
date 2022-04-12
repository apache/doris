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

#include "util/s3_storage_backend.h"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fstream>
#include <map>
#include <string>

#include "util/file_utils.h"
#include "util/storage_backend.h"

namespace doris {
static const std::string AK = "";
static const std::string SK = "";
static const std::string ENDPOINT = "http://s3.bj.bcebos.com";
static const std::string USE_PATH_STYLE = "false";
static const std::string REGION = "bj";
static const std::string BUCKET = "s3://cmy-repo/";

#define S3StorageBackendTest DISABLED_S3StorageBackendTest

class S3StorageBackendTest : public testing::Test {
public:
    S3StorageBackendTest()
            : _aws_properties({{"AWS_ACCESS_KEY", AK},
                               {"AWS_SECRET_KEY", SK},
                               {"AWS_ENDPOINT", ENDPOINT},
                               {"USE_PATH_STYLE", USE_PATH_STYLE},
                               {"AWS_REGION", REGION}}) {
        _s3.reset(new S3StorageBackend(_aws_properties));
        _s3_base_path = BUCKET + "s3/" + gen_uuid();
    }
    virtual ~S3StorageBackendTest() {}

protected:
    virtual void SetUp() {
        _test_file = "/tmp/" + gen_uuid();
        std::ofstream out(_test_file);
        out << _content;
        out.close();
    }
    virtual void TearDown() {
        remove(_test_file.c_str());
        _s3->rm(_s3_base_path);
    }
    std::string gen_uuid() {
        auto id = boost::uuids::random_generator()();
        return boost::lexical_cast<std::string>(id);
    }
    std::unique_ptr<S3StorageBackend> _s3;
    std::map<std::string, std::string> _aws_properties;
    std::string _test_file;
    std::string _s3_base_path;
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

TEST_F(S3StorageBackendTest, s3_upload) {
    Status status = _s3->upload(_test_file, _s3_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _s3->exist(_s3_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _s3->download(_s3_base_path + "/Ode_to_the_West_Wind.txt", _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
    status = _s3->upload(_test_file + "_not_found", _s3_base_path + "/Ode_to_the_West_Wind1.txt");
    EXPECT_FALSE(status.ok());
    status = _s3->exist(_s3_base_path + "/Ode_to_the_West_Wind1.txt");
    EXPECT_TRUE(status.code() == TStatusCode::NOT_FOUND);
}

TEST_F(S3StorageBackendTest, s3_direct_upload) {
    Status status = _s3->direct_upload(_s3_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _s3->exist(_s3_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _s3->download(_s3_base_path + "/Ode_to_the_West_Wind.txt", _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
}

TEST_F(S3StorageBackendTest, s3_download) {
    Status status = _s3->upload(_test_file, _s3_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _s3->download(_s3_base_path + "/Ode_to_the_West_Wind.txt", _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
    status = _s3->download(_s3_base_path + "/Ode_to_the_West_Wind.txt.not_found",
                           _test_file + ".download");
    EXPECT_FALSE(status.ok());
    status = _s3->download(_s3_base_path + "/Ode_to_the_West_Wind.txt.not_found",
                           "/not_permitted.download");
    EXPECT_FALSE(status.ok());
}

TEST_F(S3StorageBackendTest, s3_rename) {
    Status status = _s3->direct_upload(_s3_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _s3->rename(_s3_base_path + "/Ode_to_the_West_Wind.txt",
                         _s3_base_path + "/Ode_to_the_West_Wind.txt.new");
    EXPECT_TRUE(status.ok());
    status = _s3->exist(_s3_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.code() == TStatusCode::NOT_FOUND);
    status = _s3->exist(_s3_base_path + "/Ode_to_the_West_Wind.txt.new");
    EXPECT_TRUE(status.ok());
}

TEST_F(S3StorageBackendTest, s3_list) {
    Status status = _s3->direct_upload(_s3_base_path + "/Ode_to_the_West_Wind.md5", _content);
    EXPECT_TRUE(status.ok());
    status = _s3->direct_upload(_s3_base_path + "/Ode_to_the_West_Wind1.md5", _content);
    EXPECT_TRUE(status.ok());
    status = _s3->direct_upload(_s3_base_path + "/Ode_to_the_West_Wind2.md5", _content);
    EXPECT_TRUE(status.ok());
    std::map<std::string, FileStat> files;
    status = _s3->list(_s3_base_path, true, false, &files);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind") != files.end());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind1") != files.end());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind2") != files.end());
    EXPECT_EQ(3, files.size());
}

TEST_F(S3StorageBackendTest, s3_rm) {
    Status status = _s3->direct_upload(_s3_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _s3->exist(_s3_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _s3->rm(_s3_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _s3->exist(_s3_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.code() == TStatusCode::NOT_FOUND);
}

TEST_F(S3StorageBackendTest, s3_mkdir) {
    Status status = _s3->mkdir(_s3_base_path + "/dir");
    EXPECT_TRUE(status.ok());
    status = _s3->exist(_s3_base_path + "/dir");
    EXPECT_TRUE(status.code() == TStatusCode::NOT_FOUND);
}

} // end namespace doris
