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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <vector>

#include "common/status.h"
#include "common/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "util/slice.h"

namespace doris {

static constexpr std::string_view test_dir = "ut_dir/local_fs_test";

class LocalFileSystemTest : public testing::Test {
public:
    void SetUp() override {
        Status st;
        st = io::global_local_filesystem()->delete_directory(test_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(test_dir);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        Status st;
        st = io::global_local_filesystem()->delete_directory(test_dir);
        EXPECT_TRUE(st.ok()) << st;
    }
};

bool check_exist(const std::string& path) {
    bool res = false;
    auto st = io::global_local_filesystem()->exists(path, &res);
    EXPECT_TRUE(st.ok()) << st;
    return res;
}

Status save_string_file(const std::string& path, const std::string& content) {
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &file_writer));
    RETURN_IF_ERROR(file_writer->append(content));
    return file_writer->close();
}

TEST_F(LocalFileSystemTest, WriteRead) {
    auto fname = fmt::format("{}/abc", test_dir);
    io::FileWriterPtr file_writer;
    auto st = io::global_local_filesystem()->create_file(fname, &file_writer);
    ASSERT_TRUE(st.ok()) << st;
    Slice field1("123456789");
    st = file_writer->append(field1);
    ASSERT_TRUE(st.ok()) << st;

    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    st = file_writer->append(buf);
    ASSERT_TRUE(st.ok()) << st;
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2] {abc, bcd};
    st = file_writer->appendv(slices, 2);
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->finalize();
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->close();
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(file_writer->bytes_appended(), 115);

    int64_t fsize;
    st = io::global_local_filesystem()->file_size(fname, &fsize);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(115, fsize);
    {
        io::FileReaderSPtr file_reader;
        st = io::global_local_filesystem()->open_file(fname, &file_reader);
        ASSERT_TRUE(st.ok()) << st;

        char mem[1024];
        Slice slice1(mem, 9);
        Slice slice2(mem + 9, 100);
        Slice slice3(mem + 9 + 100, 3);
        Slice slice4(mem + 9 + 100 + 3, 3);
        size_t bytes_read = 0;
        st = file_reader->read_at(0, slice1, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(9, bytes_read);
        EXPECT_EQ(std::string_view(slice1.data, slice1.size), "123456789");
        st = file_reader->read_at(9, slice2, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(100, bytes_read);
        for (int i = 0; i < 100; ++i) {
            EXPECT_EQ((int)slice2.data[i], i);
        }

        st = file_reader->read_at(109, slice3, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(3, bytes_read);
        EXPECT_EQ(std::string_view(slice3.data, slice3.size), "abc");
        st = file_reader->read_at(112, slice4, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(3, bytes_read);
        EXPECT_EQ(std::string_view(slice4.data, slice4.size), "bcd");

        st = file_reader->close();
        ASSERT_TRUE(st.ok()) << st;
    }
}

TEST_F(LocalFileSystemTest, Exist) {
    auto fname = fmt::format("{}/abc", test_dir);
    ASSERT_FALSE(check_exist(fname));
    io::FileWriterPtr file_writer;
    auto st = io::global_local_filesystem()->create_file(fname, &file_writer);
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->finalize();
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->close();
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(check_exist(fname));
}

TEST_F(LocalFileSystemTest, List) {
    io::FileWriterPtr file_writer;
    auto fname = fmt::format("{}/abc", test_dir);
    auto st = io::global_local_filesystem()->create_file(fname, &file_writer);
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->finalize();
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->close();
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(check_exist(fname));
    std::vector<io::FileInfo> files;
    bool exists;
    st = io::global_local_filesystem()->list(fname, false, &files, &exists);
    ASSERT_FALSE(st.ok()) << st; // Not a dir, can not list

    auto dname = fmt::format("{}/dir/dir1/dir2", test_dir);
    st = io::global_local_filesystem()->create_directory(dname);
    ASSERT_TRUE(st.ok()) << st;
    files.clear();
    st = io::global_local_filesystem()->list(dname, false, &files, &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(files.empty());
    for (int i = 0; i < 10; ++i) {
        st = save_string_file(fmt::format("{}/{}", dname, i), "abc");
        ASSERT_TRUE(st.ok()) << st;
    }
    files.clear();
    st = io::global_local_filesystem()->list(dname, false, &files, &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(files.size(), 10);
    std::sort(files.begin(), files.end(),
              [](auto&& file1, auto&& file2) { return file1.file_name < file2.file_name; });
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(std::to_string(i), files[i].file_name);
    }
}

TEST_F(LocalFileSystemTest, Delete) {
    io::FileWriterPtr file_writer;
    auto fname = fmt::format("{}/abc", test_dir);
    auto st = save_string_file(fname, "abc");
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(check_exist(fname));
    st = io::global_local_filesystem()->delete_directory(fname);
    ASSERT_FALSE(st.ok()) << st;
    st = io::global_local_filesystem()->delete_file(fname);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(check_exist(fname));
    st = io::global_local_filesystem()->delete_file(fname);
    ASSERT_TRUE(st.ok()) << st; // Delete non-existed file is ok

    auto dname = fmt::format("{}/dir/dir1/dir2", test_dir);
    st = io::global_local_filesystem()->create_directory(dname);
    ASSERT_TRUE(st.ok()) << st;
    for (int i = 0; i < 10; ++i) {
        st = save_string_file(fmt::format("{}/{}", dname, i), "abc");
        ASSERT_TRUE(st.ok()) << st;
    }
    st = io::global_local_filesystem()->delete_file(dname);
    ASSERT_FALSE(st.ok()) << st;
    st = io::global_local_filesystem()->delete_directory(dname);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(check_exist(dname));
    st = io::global_local_filesystem()->delete_directory(dname);
    ASSERT_TRUE(st.ok()) << st;                                     // Delete non-existed dir is ok
    ASSERT_TRUE(check_exist(fmt::format("{}/dir/dir1", test_dir))); // Parent should exist
}

TEST_F(LocalFileSystemTest, AbnormalFileWriter) {
    auto fname = fmt::format("{}/abc", test_dir);
    {
        io::FileWriterPtr file_writer;
        auto st = io::global_local_filesystem()->create_file(fname, &file_writer);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->append("abc");
        ASSERT_TRUE(st.ok()) << st;
        // ~LocalFileWriter
    }
    ASSERT_FALSE(check_exist(fname));
    io::FileReaderSPtr file_reader;
    auto st = io::global_local_filesystem()->open_file(fname, &file_reader);
    ASSERT_FALSE(st.ok()) << st; // Cannot open non-existed file

    io::FileWriterPtr file_writer;
    st = io::global_local_filesystem()->create_file(fname, &file_writer);
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->append("abc");
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->close();
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->append("abc");
    ASSERT_FALSE(st.ok()) << st;
    ASSERT_EQ(file_writer->bytes_appended(), 3);
    st = io::global_local_filesystem()->open_file(fname, &file_reader);
    ASSERT_TRUE(st.ok()) << st;
    char buf[1024];
    size_t bytes_read;
    st = file_reader->read_at(0, {buf, 3}, &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(bytes_read, 3);
    EXPECT_EQ(std::string_view(buf, 3), "abc");
    st = file_reader->read_at(3, {buf + 3, 1}, &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(bytes_read, 0);
}

TEST_F(LocalFileSystemTest, AbnormalWriteRead) {
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("LocalFileWriter::writev");
        sp->clear_call_back("LocalFileReader::pread");
    }};
    sp->enable_processing();

    // Test EIO
    auto fname = fmt::format("{}/abc", test_dir);
    io::FileWriterPtr file_writer;
    auto st = io::global_local_filesystem()->create_file(fname, &file_writer);
    ASSERT_TRUE(st.ok()) << st;
    sp->set_call_back("LocalFileWriter::writev", [](auto&& args) {
        auto* ret = try_any_cast_ret<ssize_t>(args);
        ret->first = -1;
        ret->second = true;
        errno = EIO;
    });
    st = file_writer->append("abc");
    ASSERT_FALSE(st.ok()) << st;

    // Test EINTR
    int retry = 2;
    sp->set_call_back("LocalFileWriter::writev", [&retry](auto&& args) {
        if (retry-- > 0) {
            auto* ret = try_any_cast_ret<ssize_t>(args);
            ret->first = -1;
            ret->second = true;
            errno = EINTR;
        } else {
            auto* ret = try_any_cast_ret<ssize_t>(args);
            ret->second = false;
        }
    });
    st = file_writer->append("abc");
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(file_writer->bytes_appended(), 3);

    // Test partial write
    std::vector<Slice> content {"defg", "hijklmn", "opqrstu", "vwxyz"};
    std::vector<std::vector<iovec>> partial_content {
            {{content[0].data, 4}, {content[1].data, 2}},
            {{content[1].data + 2, 5}},
            {{content[2].data, 4}},
            {{content[2].data + 4, 3}, {content[3].data, 5}}};
    size_t idx = 0;
    sp->set_call_back("LocalFileWriter::writev", [&partial_content, &idx](auto&& args) {
        // Mock partial write
        auto fd = try_any_cast<int>(args[0]);
        auto* ret = try_any_cast_ret<ssize_t>(args);
        ret->first = ::writev(fd, partial_content[idx].data(), partial_content[idx].size());
        ret->second = true;
        ++idx;
    });
    st = file_writer->appendv(content.data(), content.size());
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(file_writer->bytes_appended(), 26);

    st = file_writer->close();
    ASSERT_TRUE(st.ok()) << st;

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(fname, &file_reader);
    ASSERT_TRUE(st.ok()) << st;
    char buf[1024];
    size_t bytes_read = 0;
    st = file_reader->read_at(0, {buf, 26}, &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(bytes_read, 26);
    EXPECT_EQ(std::string_view(buf, 26), "abcdefghijklmnopqrstuvwxyz");

    // Test EIO
    sp->set_call_back("LocalFileReader::pread", [](auto&& args) {
        auto* ret = try_any_cast_ret<ssize_t>(args);
        ret->first = -1;
        ret->second = true;
        errno = EIO;
    });
    st = file_reader->read_at(0, {buf, 26}, &bytes_read);
    ASSERT_FALSE(st.ok()) << st;

    // Test EINTR
    retry = 2;
    sp->set_call_back("LocalFileReader::pread", [&retry](auto&& args) {
        if (retry-- > 0) {
            auto* ret = try_any_cast_ret<ssize_t>(args);
            ret->first = -1;
            ret->second = true;
            errno = EINTR;
        } else {
            auto* ret = try_any_cast_ret<ssize_t>(args);
            ret->second = false;
        }
    });
    memset(buf, 0, sizeof(buf));
    st = file_reader->read_at(0, {buf, 26}, &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(bytes_read, 26);
    EXPECT_EQ(std::string_view(buf, 26), "abcdefghijklmnopqrstuvwxyz");

    // Test partial read
    size_t offset = 0;
    sp->set_call_back("LocalFileReader::pread", [&offset](auto&& args) {
        // Mock partial read
        auto fd = try_any_cast<int>(args[0]);
        auto* buf = try_any_cast<char*>(args[1]);
        auto* ret = try_any_cast_ret<ssize_t>(args);
        ret->first = ::pread(fd, buf, 5, offset);
        ret->second = true;
        offset += ret->first;
    });
    memset(buf, 0, sizeof(buf));
    st = file_reader->read_at(0, {buf, 26}, &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(bytes_read, 26);
    EXPECT_EQ(std::string_view(buf, 26), "abcdefghijklmnopqrstuvwxyz");
}

TEST_F(LocalFileSystemTest, TestGlob) {
    std::string path = "./be/ut_build_ASAN/test/file_path/";
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(path).ok());
    EXPECT_TRUE(io::global_local_filesystem()
                        ->create_directory("./be/ut_build_ASAN/test/file_path/1")
                        .ok());
    EXPECT_TRUE(io::global_local_filesystem()
                        ->create_directory("./be/ut_build_ASAN/test/file_path/2")
                        .ok());
    EXPECT_TRUE(io::global_local_filesystem()
                        ->create_directory("./be/ut_build_ASAN/test/file_path/3")
                        .ok());

    auto st = save_string_file("./be/ut_build_ASAN/test/file_path/1/f1.txt", "just test");
    ASSERT_TRUE(st.ok()) << st;
    st = save_string_file("./be/ut_build_ASAN/test/file_path/1/f2.txt", "just test");
    ASSERT_TRUE(st.ok()) << st;
    st = save_string_file("./be/ut_build_ASAN/test/file_path/f3.txt", "just test");
    ASSERT_TRUE(st.ok()) << st;

    std::vector<io::FileInfo> files;
    EXPECT_FALSE(io::global_local_filesystem()->safe_glob("./../*.txt", &files).ok());
    EXPECT_FALSE(io::global_local_filesystem()->safe_glob("/*.txt", &files).ok());
    EXPECT_TRUE(io::global_local_filesystem()->safe_glob("./file_path/1/*.txt", &files).ok());
    EXPECT_EQ(2, files.size());
    files.clear();
    EXPECT_TRUE(io::global_local_filesystem()->safe_glob("./file_path/*/*.txt", &files).ok());
    EXPECT_EQ(2, files.size());

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(path).ok());
}

TEST_F(LocalFileSystemTest, TestConvertToAbsPath) {
    io::Path abs_path;
    Status st;

    // suppurt path:
    st = doris::io::LocalFileSystem::convert_to_abs_path("/abc/def", abs_path);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("/abc/def", abs_path);

    st = doris::io::LocalFileSystem::convert_to_abs_path("file:/def/hij", abs_path);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("/def/hij", abs_path);

    st = doris::io::LocalFileSystem::convert_to_abs_path("file://host:80/hij/abc", abs_path);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("/hij/abc", abs_path);

    st = doris::io::LocalFileSystem::convert_to_abs_path("file://host/abc/def", abs_path);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("/abc/def", abs_path);

    st = doris::io::LocalFileSystem::convert_to_abs_path("file:///def", abs_path);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("/def", abs_path);

    st = doris::io::LocalFileSystem::convert_to_abs_path("file:///", abs_path);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("/", abs_path);

    st = doris::io::LocalFileSystem::convert_to_abs_path("file://auth/", abs_path);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("/", abs_path);

    st = doris::io::LocalFileSystem::convert_to_abs_path("abc", abs_path);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("abc", abs_path);

    // not support path:
    st = doris::io::LocalFileSystem::convert_to_abs_path("file://auth", abs_path);
    ASSERT_TRUE(!st.ok());

    st = doris::io::LocalFileSystem::convert_to_abs_path("fileee:/abc", abs_path);
    ASSERT_TRUE(!st.ok());

    st = doris::io::LocalFileSystem::convert_to_abs_path("hdfs:///abc", abs_path);
    ASSERT_TRUE(!st.ok());

    st = doris::io::LocalFileSystem::convert_to_abs_path("hdfs:/abc", abs_path);
    ASSERT_TRUE(!st.ok());
}

} // namespace doris
