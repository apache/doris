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

#include <gtest/gtest.h>

#include <algorithm>

#include "common/logging.h"
#include "env/env.h"
#include "util/file_utils.h"

namespace doris {

class EnvPosixTest : public testing::Test {
public:
    EnvPosixTest() {}
    virtual ~EnvPosixTest() {}
    void SetUp() override {
        auto st = FileUtils::create_dir("./ut_dir/env_posix");
        EXPECT_TRUE(st.ok());
    }
    void TearDown() override {}
};

TEST_F(EnvPosixTest, file_path_desc) {
    FilePathDesc path_desc("/local");
    path_desc.storage_medium = TStorageMedium::S3;
    path_desc.remote_path = "/remote";
    FilePathDescStream path_desc_stream;
    path_desc_stream << path_desc << "/test"
                     << "/" << 1;
    FilePathDesc dest_path_desc = path_desc_stream.path_desc();
    EXPECT_EQ("/local/test/1", dest_path_desc.filepath);
    EXPECT_EQ("/remote/test/1", dest_path_desc.remote_path);
}

TEST_F(EnvPosixTest, random_access) {
    std::string fname = "./ut_dir/env_posix/random_access";
    std::unique_ptr<WritableFile> wfile;
    auto env = Env::Default();
    auto st = env->new_writable_file(fname, &wfile);
    EXPECT_TRUE(st.ok());
    st = wfile->pre_allocate(1024);
#ifndef __APPLE__
    EXPECT_TRUE(st.ok());
#else
    EXPECT_FALSE(st.ok());
#endif
    // write data
    Slice field1("123456789");
    st = wfile->append(field1);
    EXPECT_TRUE(st.ok());
    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    st = wfile->append(buf);
    EXPECT_TRUE(st.ok());
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2] {abc, bcd};
    st = wfile->appendv(slices, 2);
    EXPECT_TRUE(st.ok());
    st = wfile->flush(WritableFile::FLUSH_ASYNC);
    EXPECT_TRUE(st.ok());
    st = wfile->sync();
    EXPECT_TRUE(st.ok());
    st = wfile->close();
    EXPECT_TRUE(st.ok());

    EXPECT_EQ(115, wfile->size());

    uint64_t size;
    st = env->get_file_size(fname, &size);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(115, size);
    {
        char mem[1024];
        std::unique_ptr<RandomAccessFile> rfile;
        st = env->new_random_access_file(fname, &rfile);
        EXPECT_TRUE(st.ok());

        Slice slice1(mem, 9);
        Slice slice2(mem + 9, 100);
        Slice slice3(mem + 9 + 100, 3);

        Slice read_slices[3] {slice1, slice2, slice3};
        st = rfile->readv_at(0, read_slices, 3);
        EXPECT_TRUE(st.ok());
        EXPECT_STREQ("123456789", std::string(slice1.data, slice1.size).c_str());
        EXPECT_STREQ("abc", std::string(slice3.data, slice3.size).c_str());

        Slice slice4(mem, 3);
        st = rfile->read_at(112, &slice4);
        EXPECT_TRUE(st.ok());
        EXPECT_STREQ("bcd", std::string(slice4.data, slice4.size).c_str());

        // end of file
        st = rfile->read_at(114, &slice4);
        EXPECT_EQ(TStatusCode::END_OF_FILE, st.code());
        LOG(INFO) << "st=" << st.to_string();
    }
}

TEST_F(EnvPosixTest, random_rw) {
    std::string fname = "./ut_dir/env_posix/random_rw";
    std::unique_ptr<RandomRWFile> wfile;
    auto env = Env::Default();
    auto st = env->new_random_rw_file(fname, &wfile);
    EXPECT_TRUE(st.ok());
    // write data
    Slice field1("123456789");
    st = wfile->write_at(0, field1);
    EXPECT_TRUE(st.ok());
    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    st = wfile->write_at(9, buf);
    EXPECT_TRUE(st.ok());
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2] {abc, bcd};
    st = wfile->writev_at(0, slices, 2);
    EXPECT_TRUE(st.ok());

    uint64_t size;
    st = wfile->size(&size);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(109, size);

    st = wfile->flush(RandomRWFile::FLUSH_ASYNC, 0, 0);
    EXPECT_TRUE(st.ok());
    st = wfile->sync();
    EXPECT_TRUE(st.ok());
    st = wfile->close();
    EXPECT_TRUE(st.ok());

    st = env->get_file_size(fname, &size);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(109, size);
    {
        char mem[1024];
        std::unique_ptr<RandomRWFile> rfile;
        RandomRWFileOptions opts;
        opts.mode = Env::MUST_EXIST;
        st = env->new_random_rw_file(opts, fname, &rfile);
        EXPECT_TRUE(st.ok());

        Slice slice1(mem, 3);
        Slice slice2(mem + 3, 3);
        Slice slice3(mem + 6, 3);

        Slice read_slices[3] {slice1, slice2, slice3};
        st = rfile->readv_at(0, read_slices, 3);
        LOG(INFO) << st.to_string();
        EXPECT_TRUE(st.ok());
        EXPECT_STREQ("abc", std::string(slice1.data, slice1.size).c_str());
        EXPECT_STREQ("bcd", std::string(slice2.data, slice2.size).c_str());
        EXPECT_STREQ("789", std::string(slice3.data, slice3.size).c_str());

        Slice slice4(mem, 100);
        st = rfile->read_at(9, slice4);
        EXPECT_TRUE(st.ok());

        // end of file
        st = rfile->read_at(102, slice4);
        EXPECT_EQ(TStatusCode::END_OF_FILE, st.code());
        LOG(INFO) << "st=" << st.to_string();
    }
}

TEST_F(EnvPosixTest, iterate_dir) {
    std::string dir_path = "./ut_dir/env_posix/iterate_dir";
    FileUtils::remove_all(dir_path);
    auto st = Env::Default()->create_dir_if_missing(dir_path);
    EXPECT_TRUE(st.ok());

    st = Env::Default()->create_dir_if_missing(dir_path + "/abc");
    EXPECT_TRUE(st.ok());

    st = Env::Default()->create_dir_if_missing(dir_path + "/123");
    EXPECT_TRUE(st.ok());

    {
        std::vector<std::string> children;
        st = Env::Default()->get_children(dir_path, &children);
        EXPECT_EQ(4, children.size());
        std::sort(children.begin(), children.end());

        EXPECT_STREQ(".", children[0].c_str());
        EXPECT_STREQ("..", children[1].c_str());
        EXPECT_STREQ("123", children[2].c_str());
        EXPECT_STREQ("abc", children[3].c_str());
    }
    {
        std::vector<std::string> children;
        st = FileUtils::list_files(Env::Default(), dir_path, &children);
        EXPECT_EQ(2, children.size());
        std::sort(children.begin(), children.end());

        EXPECT_STREQ("123", children[0].c_str());
        EXPECT_STREQ("abc", children[1].c_str());
    }

    FileUtils::remove_all(dir_path);
}

} // namespace doris
