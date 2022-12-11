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

#include "util/hdfs_storage_backend.h"

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
using namespace ErrorCode;
static const std::string fs_name = "hdfs://127.0.0.1:9000"; // An invalid address
static const std::string user = "test";
static const std::string base_path = "/user/test";

#define HDFSStorageBackendTest DISABLED_HDFSStorageBackendTest

class HDFSStorageBackendTest : public testing::Test {
public:
    HDFSStorageBackendTest() : _properties({{FS_KEY, fs_name}, {USER, user}}) {
        _fs.reset(new HDFSStorageBackend(_properties));
        _base_path = base_path + "/" + gen_uuid();
    }
    virtual ~HDFSStorageBackendTest() {}

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
        return boost::lexical_cast<std::string>(id);
    }

    std::unique_ptr<HDFSStorageBackend> _fs;
    std::map<std::string, std::string> _properties;
    std::string _test_file;
    std::string _base_path;
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

TEST_F(HDFSStorageBackendTest, hdfs_upload) {
    Status status = _fs->upload(_test_file, _base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _fs->exist(_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _fs->download(_base_path + "/Ode_to_the_West_Wind.txt", _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
    status = _fs->upload(_test_file + "_not_found", _base_path + "/Ode_to_the_West_Wind1.txt");
    EXPECT_FALSE(status.ok());
    status = _fs->exist(_base_path + "/Ode_to_the_West_Wind1.txt");
    EXPECT_EQ(NOT_FOUND, status.code());
}

TEST_F(HDFSStorageBackendTest, hdfs_direct_upload) {
    Status status = _fs->direct_upload(_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _fs->exist(_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _fs->download(_base_path + "/Ode_to_the_West_Wind.txt", _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
}

TEST_F(HDFSStorageBackendTest, hdfs_download) {
    Status status = _fs->upload(_test_file, _base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    std::string orig_md5sum;
    FileUtils::md5sum(_test_file, &orig_md5sum);
    status = _fs->download(_base_path + "/Ode_to_the_West_Wind.txt", _test_file + ".download");
    EXPECT_TRUE(status.ok());
    std::string download_md5sum;
    FileUtils::md5sum(_test_file + ".download", &download_md5sum);
    EXPECT_EQ(orig_md5sum, download_md5sum);
    status = _fs->download(_base_path + "/Ode_to_the_West_Wind.txt.not_found",
                           _test_file + ".download");
    EXPECT_FALSE(status.ok());
    status = _fs->download(_base_path + "/Ode_to_the_West_Wind.txt.not_found",
                           "/not_permitted.download");
    EXPECT_FALSE(status.ok());
}

TEST_F(HDFSStorageBackendTest, hdfs_rename) {
    Status status = _fs->direct_upload(_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _fs->rename(_base_path + "/Ode_to_the_West_Wind.txt",
                         _base_path + "/Ode_to_the_West_Wind.txt.new");
    EXPECT_TRUE(status.ok());
    status = _fs->exist(_base_path + "/Ode_to_the_West_Wind.txt.new");
    EXPECT_TRUE(status.ok());
}

TEST_F(HDFSStorageBackendTest, hdfs_list) {
    Status status = _fs->direct_upload(_base_path + "/Ode_to_the_West_Wind.md5", _content);
    EXPECT_TRUE(status.ok());
    status = _fs->direct_upload(_base_path + "/Ode_to_the_West_Wind1.md5", _content);
    EXPECT_TRUE(status.ok());
    status = _fs->direct_upload(_base_path + "/Ode_to_the_West_Wind2.md5", _content);
    EXPECT_TRUE(status.ok());
    std::map<std::string, FileStat> files;
    status = _fs->list(_base_path, true, false, &files);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind") != files.end());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind1") != files.end());
    EXPECT_TRUE(files.find("Ode_to_the_West_Wind2") != files.end());
    EXPECT_EQ(3, files.size());
}

TEST_F(HDFSStorageBackendTest, hdfs_rm) {
    Status status = _fs->direct_upload(_base_path + "/Ode_to_the_West_Wind.txt", _content);
    EXPECT_TRUE(status.ok());
    status = _fs->exist(_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _fs->rm(_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.ok());
    status = _fs->exist(_base_path + "/Ode_to_the_West_Wind.txt");
    EXPECT_TRUE(status.code() == NOT_FOUND);
}

} // namespace doris