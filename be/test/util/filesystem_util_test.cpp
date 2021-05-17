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

#include "util/filesystem_util.h"

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <filesystem>

#include "common/configbase.h"
#include "util/logging.h"

namespace doris {

namespace filesystem = std::filesystem;
using filesystem::path;

TEST(FileSystemUtil, rlimit) {
    ASSERT_LT(0ul, FileSystemUtil::max_num_file_handles());
}

TEST(FileSystemUtil, CreateDirectory) {
    char filename[] = "temp-XXXXXX";
    // Setup a temporary directory with one subdir
    std::string dir_name = mkdtemp(filename);
    path dir{dir_name};
    path subdir1 = dir / "path1";
    path subdir2 = dir / "path2";
    path subdir3 = dir / "a" / "longer" / "path";
    filesystem::create_directories(subdir1);
    // Test error cases by removing write permissions on root dir to prevent
    // creation/deletion of subdirs
    chmod(dir.string().c_str(), 0);
    if (getuid() == 0) { // User root
        EXPECT_TRUE(FileSystemUtil::create_directory(subdir1.string()).ok());
        EXPECT_TRUE(FileSystemUtil::create_directory(subdir2.string()).ok());
    } else { // User other
        EXPECT_FALSE(FileSystemUtil::create_directory(subdir1.string()).ok());
        EXPECT_FALSE(FileSystemUtil::create_directory(subdir2.string()).ok());
    }
    // Test success cases by adding write permissions back
    chmod(dir.string().c_str(), S_IRWXU);
    EXPECT_TRUE(FileSystemUtil::create_directory(subdir1.string()).ok());
    EXPECT_TRUE(FileSystemUtil::create_directory(subdir2.string()).ok());
    // Check that directories were created
    EXPECT_TRUE(filesystem::exists(subdir1) && filesystem::is_directory(subdir1));
    EXPECT_TRUE(filesystem::exists(subdir2) && filesystem::is_directory(subdir2));
    // Exercise VerifyIsDirectory
    EXPECT_TRUE(FileSystemUtil::verify_is_directory(subdir1.string()).ok());
    EXPECT_TRUE(FileSystemUtil::verify_is_directory(subdir2.string()).ok());
    EXPECT_FALSE(FileSystemUtil::verify_is_directory(subdir3.string()).ok());
    // Check that nested directories can be created
    EXPECT_TRUE(FileSystemUtil::create_directory(subdir3.string()).ok());
    EXPECT_TRUE(filesystem::exists(subdir3) && filesystem::is_directory(subdir3));
    // Cleanup
    filesystem::remove_all(dir);
}

TEST(FilesystemUtil, contain_path) {
    {
        std::string parent("/a/b");
        std::string sub("/a/b/c");
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, sub));
        EXPECT_FALSE(FileSystemUtil::contain_path(sub, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(sub, sub));
    }

    {
        std::string parent("/a/b/");
        std::string sub("/a/b/c/");
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, sub));
        EXPECT_FALSE(FileSystemUtil::contain_path(sub, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(sub, sub));
    }

    {
        std::string parent("/a///./././/./././b/"); // "/a/b/."
        std::string sub("/a/b/../././b/c/");        // "/a/b/c/"
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, sub));
        EXPECT_FALSE(FileSystemUtil::contain_path(sub, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(sub, sub));
    }

    {
        // relative path
        std::string parent("a/b/"); // "a/b/"
        std::string sub("a/b/c/");  // "a/b/c/"
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, sub));
        EXPECT_FALSE(FileSystemUtil::contain_path(sub, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(sub, sub));
    }
    {
        // relative path
        std::string parent("a////./././b/"); // "a/b/"
        std::string sub("a/b/../././b/c/");  // "a/b/c/"
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, sub));
        EXPECT_FALSE(FileSystemUtil::contain_path(sub, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(sub, sub));
    }
    {
        // absolute path and relative path
        std::string parent("/a////./././b/"); // "/a/b/"
        std::string sub("a/b/../././b/c/");   // "a/b/c/"
        EXPECT_FALSE(FileSystemUtil::contain_path(parent, sub));
        EXPECT_FALSE(FileSystemUtil::contain_path(sub, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(parent, parent));
        EXPECT_TRUE(FileSystemUtil::contain_path(sub, sub));
    }
}

} // end namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
