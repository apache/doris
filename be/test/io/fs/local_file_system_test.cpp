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

#include "io/fs/local_file_system.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <set>
#include <vector>

#include "common/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"

namespace doris {

class LocalFileSystemTest : public testing::Test {
public:
    virtual void SetUp() {
        EXPECT_TRUE(
                io::global_local_filesystem()->delete_and_create_directory(_s_test_data_path).ok());
    }

    Status save_string_file(const std::filesystem::path& filename, const std::string& content) {
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(filename, &file_writer));
        RETURN_IF_ERROR(file_writer->append(content));
        return file_writer->close();
    }

    bool check_exists(const std::string& file) {
        bool exists = true;
        EXPECT_TRUE(io::global_local_filesystem()->exists(file, &exists).ok());
        return exists;
    }

    bool is_dir(const std::string& path) {
        bool is_dir = true;
        EXPECT_TRUE(io::global_local_filesystem()->is_directory(path, &is_dir).ok());
        return is_dir;
    }

    Status list_dirs_files(const std::string& path, std::vector<std::string>* dirs,
                           std::vector<std::string>* files) {
        bool only_file = true;
        if (dirs != nullptr) {
            only_file = false;
        }
        std::vector<io::FileInfo> file_infos;
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->list(path, only_file, &file_infos, &exists));
        for (auto& file_info : file_infos) {
            if (file_info.is_file && files != nullptr) {
                files->push_back(file_info.file_name);
            }
            if (!file_info.is_file && dirs != nullptr) {
                dirs->push_back(file_info.file_name);
            }
        }
        return Status::OK();
    }

    Status delete_file_paths(const std::vector<std::string>& ps) {
        for (auto& p : ps) {
            bool exists = true;
            RETURN_IF_ERROR(io::global_local_filesystem()->exists(p, &exists));
            if (!exists) {
                continue;
            }
            bool is_dir = true;
            RETURN_IF_ERROR(io::global_local_filesystem()->is_directory(p, &is_dir));
            if (is_dir) {
                RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(p));
            } else {
                RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(p));
            }
        }
        return Status::OK();
    }

    // delete the mock cgroup folder
    virtual void TearDown() {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_s_test_data_path).ok());
    }

    static std::string _s_test_data_path;
};

std::string LocalFileSystemTest::_s_test_data_path = "./file_utils_testxxxx123";

TEST_F(LocalFileSystemTest, TestRemove) {
    // remove_all
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test").ok());
    EXPECT_FALSE(check_exists("./file_test"));

    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/123/456/789").ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/abc/def/zxc").ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/abc/123").ok());

    save_string_file("./file_test/s1", "123");
    save_string_file("./file_test/123/s2", "123");

    EXPECT_TRUE(check_exists("./file_test"));
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test").ok());
    EXPECT_FALSE(check_exists("./file_test"));

    // remove
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/abc/123").ok());
    save_string_file("./file_test/abc/123/s2", "123");

    EXPECT_TRUE(check_exists("./file_test/abc/123/s2"));
    EXPECT_TRUE(io::global_local_filesystem()->delete_file("./file_test/abc/123/s2").ok());
    EXPECT_FALSE(check_exists("./file_test/abc/123/s2"));

    EXPECT_TRUE(check_exists("./file_test/abc/123"));
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test/abc/123").ok());
    EXPECT_FALSE(check_exists("./file_test/abc/123"));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test").ok());
    EXPECT_FALSE(check_exists("./file_test"));

    // remove paths
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/123/456/789").ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/abc/def/zxc").ok());
    save_string_file("./file_test/s1", "123");
    save_string_file("./file_test/s2", "123");

    std::vector<std::string> ps;
    ps.push_back("./file_test/123/456/789");
    ps.push_back("./file_test/123/456");
    ps.push_back("./file_test/123");

    EXPECT_TRUE(check_exists("./file_test/123"));
    EXPECT_TRUE(delete_file_paths(ps).ok());
    EXPECT_FALSE(check_exists("./file_test/123"));

    ps.clear();
    ps.push_back("./file_test/s1");
    ps.push_back("./file_test/abc/def");

    EXPECT_TRUE(delete_file_paths(ps).ok());
    EXPECT_FALSE(check_exists("./file_test/s1"));
    EXPECT_FALSE(check_exists("./file_test/abc/def/"));

    ps.clear();
    ps.push_back("./file_test/abc/def/zxc");
    ps.push_back("./file_test/s2");
    ps.push_back("./file_test/abc/def");
    ps.push_back("./file_test/abc");

    EXPECT_TRUE(delete_file_paths(ps).ok());
    EXPECT_FALSE(check_exists("./file_test/s2"));
    EXPECT_FALSE(check_exists("./file_test/abc"));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test").ok());
}

TEST_F(LocalFileSystemTest, TestCreateDir) {
    // normal
    std::string path = "./file_test/123/456/789";
    io::global_local_filesystem()->delete_directory("./file_test");
    EXPECT_FALSE(check_exists(path));

    EXPECT_TRUE(io::global_local_filesystem()->create_directory(path).ok());

    EXPECT_TRUE(check_exists(path));
    EXPECT_TRUE(is_dir("./file_test"));
    EXPECT_TRUE(is_dir("./file_test/123"));
    EXPECT_TRUE(is_dir("./file_test/123/456"));
    EXPECT_TRUE(is_dir("./file_test/123/456/789"));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test").ok());

    // normal
    path = "./file_test/123/456/789/";
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test").ok());
    EXPECT_FALSE(check_exists(path));

    EXPECT_TRUE(io::global_local_filesystem()->create_directory(path).ok());

    EXPECT_TRUE(check_exists(path));
    EXPECT_TRUE(is_dir("./file_test"));
    EXPECT_TRUE(is_dir("./file_test/123"));
    EXPECT_TRUE(is_dir("./file_test/123/456"));
    EXPECT_TRUE(is_dir("./file_test/123/456/789"));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test").ok());

    // absolute path;
    std::string real_path;
    EXPECT_TRUE(io::global_local_filesystem()->canonicalize(".", &real_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()
                        ->create_directory(real_path + "/file_test/absolute/path/123/asdf")
                        .ok());
    EXPECT_TRUE(is_dir("./file_test/absolute/path/123/asdf"));
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./file_test").ok());

    char filename[] = "temp-XXXXXX";
    // Setup a temporary directory with one subdir
    std::string dir_name = mkdtemp(filename);
    io::Path dir {dir_name};
    io::Path subdir1 = dir / "path1";
    io::Path subdir2 = dir / "path2";
    io::Path subdir3 = dir / "a" / "longer" / "path";
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(subdir1).ok());
    // Test error cases by removing write permissions on root dir to prevent
    // creation/deletion of subdirs
    chmod(dir.string().c_str(), 0);
    if (getuid() == 0) { // User root
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(subdir1).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(subdir2).ok());
    } else { // User other
        EXPECT_FALSE(io::global_local_filesystem()->create_directory(subdir1).ok());
        EXPECT_FALSE(io::global_local_filesystem()->create_directory(subdir2).ok());
    }
    // Test success cases by adding write permissions back
    chmod(dir.string().c_str(), S_IRWXU);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(subdir1).ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(subdir2).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(subdir1).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(subdir2).ok());
    // Check that directories were created
    bool is_dir = false;
    EXPECT_TRUE(io::global_local_filesystem()->is_directory(subdir1, &is_dir).ok());
    EXPECT_TRUE(is_dir);
    EXPECT_TRUE(io::global_local_filesystem()->is_directory(subdir2, &is_dir).ok());
    EXPECT_TRUE(is_dir);
    EXPECT_FALSE(io::global_local_filesystem()->is_directory(subdir3, &is_dir).ok());
    // Check that nested directories can be created
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(subdir3).ok());
    EXPECT_TRUE(io::global_local_filesystem()->is_directory(subdir3, &is_dir).ok());
    EXPECT_TRUE(is_dir);
    // Cleanup
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(dir).ok());
}

TEST_F(LocalFileSystemTest, TestContainPath) {
    {
        std::string parent("/a/b");
        std::string sub("/a/b/c");
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, sub));
        EXPECT_FALSE(io::global_local_filesystem()->contain_path(sub, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(sub, sub));
    }

    {
        std::string parent("/a/b/");
        std::string sub("/a/b/c/");
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, sub));
        EXPECT_FALSE(io::global_local_filesystem()->contain_path(sub, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(sub, sub));
    }

    {
        std::string parent("/a///./././/./././b/"); // "/a/b/."
        std::string sub("/a/b/../././b/c/");        // "/a/b/c/"
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, sub));
        EXPECT_FALSE(io::global_local_filesystem()->contain_path(sub, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(sub, sub));
    }

    {
        // relative path
        std::string parent("a/b/"); // "a/b/"
        std::string sub("a/b/c/");  // "a/b/c/"
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, sub));
        EXPECT_FALSE(io::global_local_filesystem()->contain_path(sub, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(sub, sub));
    }
    {
        // relative path
        std::string parent("a////./././b/"); // "a/b/"
        std::string sub("a/b/../././b/c/");  // "a/b/c/"
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, sub));
        EXPECT_FALSE(io::global_local_filesystem()->contain_path(sub, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(sub, sub));
    }
    {
        // absolute path and relative path
        std::string parent("/a////./././b/"); // "/a/b/"
        std::string sub("a/b/../././b/c/");   // "a/b/c/"
        EXPECT_FALSE(io::global_local_filesystem()->contain_path(parent, sub));
        EXPECT_FALSE(io::global_local_filesystem()->contain_path(sub, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(parent, parent));
        EXPECT_TRUE(io::global_local_filesystem()->contain_path(sub, sub));
    }
}

TEST_F(LocalFileSystemTest, TestListDirsFiles) {
    std::string path = "./file_test/";
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/1").ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/2").ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/3").ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/4").ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./file_test/5").ok());

    std::vector<std::string> dirs;
    std::vector<std::string> files;

    EXPECT_TRUE(list_dirs_files("./file_test", &dirs, &files).ok());
    EXPECT_EQ(5, dirs.size());
    EXPECT_EQ(0, files.size());

    dirs.clear();
    files.clear();

    EXPECT_TRUE(list_dirs_files("./file_test", &dirs, nullptr).ok());
    EXPECT_EQ(5, dirs.size());
    EXPECT_EQ(0, files.size());

    save_string_file("./file_test/f1", "just test");
    save_string_file("./file_test/f2", "just test");
    save_string_file("./file_test/f3", "just test");

    dirs.clear();
    files.clear();

    EXPECT_TRUE(list_dirs_files("./file_test", &dirs, &files).ok());
    EXPECT_EQ(5, dirs.size());
    EXPECT_EQ(3, files.size());

    dirs.clear();
    files.clear();

    EXPECT_TRUE(list_dirs_files("./file_test", nullptr, &files).ok());
    EXPECT_EQ(0, dirs.size());
    EXPECT_EQ(3, files.size());

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(path).ok());
}

TEST_F(LocalFileSystemTest, TestRandomAccess) {
    std::string fname = "./ut_dir/local_filesystem/random_access";
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./ut_dir/local_filesystem/").ok());
    io::FileWriterPtr file_writer;
    EXPECT_TRUE(io::global_local_filesystem()->create_file(fname, &file_writer).ok());
    Slice field1("123456789");
    EXPECT_TRUE(file_writer->append(field1).ok());

    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    EXPECT_TRUE(file_writer->append(buf).ok());
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2] {abc, bcd};
    EXPECT_TRUE(file_writer->appendv(slices, 2).ok());
    EXPECT_TRUE(file_writer->close().ok());

    int64_t size;
    EXPECT_TRUE(io::global_local_filesystem()->file_size(fname, &size).ok());
    EXPECT_EQ(115, size);
    {
        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(io::global_local_filesystem()->open_file(fname, &file_reader).ok());

        char mem[1024];
        Slice slice1(mem, 9);
        Slice slice2(mem + 9, 100);
        Slice slice3(mem + 9 + 100, 3);
        Slice slice4(mem + 9 + 100 + 3, 3);
        size_t bytes_read = 0;
        EXPECT_TRUE(file_reader->read_at(0, slice1, &bytes_read).ok());
        EXPECT_STREQ("123456789", std::string(slice1.data, slice1.size).c_str());
        EXPECT_EQ(9, bytes_read);

        EXPECT_TRUE(file_reader->read_at(9, slice2, &bytes_read).ok());
        EXPECT_EQ(100, bytes_read);

        EXPECT_TRUE(file_reader->read_at(109, slice3, &bytes_read).ok());
        EXPECT_STREQ("abc", std::string(slice3.data, slice3.size).c_str());
        EXPECT_EQ(3, bytes_read);

        EXPECT_TRUE(file_reader->read_at(112, slice4, &bytes_read).ok());
        EXPECT_STREQ("bcd", std::string(slice4.data, slice4.size).c_str());
        EXPECT_EQ(3, bytes_read);

        EXPECT_TRUE(file_reader->close().ok());
    }
}

TEST_F(LocalFileSystemTest, TestRandomWrite) {
    std::string fname = "./ut_dir/env_posix/random_rw";
    EXPECT_TRUE(io::global_local_filesystem()->create_directory("./ut_dir/env_posix").ok());

    io::FileWriterPtr file_writer;
    EXPECT_TRUE(io::global_local_filesystem()->create_file(fname, &file_writer).ok());

    // write data
    Slice field1("123456789");
    EXPECT_TRUE(file_writer->write_at(0, field1).ok());
    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    EXPECT_TRUE(file_writer->write_at(9, buf).ok());
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2] {abc, bcd};
    EXPECT_TRUE(file_writer->write_at(0, slices[0]).ok());
    EXPECT_TRUE(file_writer->write_at(3, slices[1]).ok());
    EXPECT_TRUE(file_writer->close().ok());

    int64_t size = 0;
    EXPECT_TRUE(io::global_local_filesystem()->file_size(fname, &size).ok());
    EXPECT_EQ(109, size);
    {
        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(io::global_local_filesystem()->open_file(fname, &file_reader).ok());

        char mem[1024];
        Slice slice1(mem, 3);
        Slice slice2(mem + 3, 3);
        Slice slice3(mem + 6, 3);

        size_t bytes_read = 0;
        EXPECT_TRUE(file_reader->read_at(0, slice1, &bytes_read).ok());
        EXPECT_STREQ("abc", std::string(slice1.data, slice1.size).c_str());
        EXPECT_EQ(3, bytes_read);

        EXPECT_TRUE(file_reader->read_at(3, slice2, &bytes_read).ok());
        EXPECT_STREQ("bcd", std::string(slice2.data, slice2.size).c_str());
        EXPECT_EQ(3, bytes_read);

        EXPECT_TRUE(file_reader->read_at(6, slice3, &bytes_read).ok());
        EXPECT_STREQ("789", std::string(slice3.data, slice3.size).c_str());
        EXPECT_EQ(3, bytes_read);

        Slice slice4(mem, 100);
        EXPECT_TRUE(file_reader->read_at(9, slice4, &bytes_read).ok());
        EXPECT_EQ(100, bytes_read);

        EXPECT_TRUE(file_reader->close().ok());
    }
}
} // namespace doris
