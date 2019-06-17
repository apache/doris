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

#include "env/env.h"

#include <gtest/gtest.h>

#include "common/logging.h"

namespace doris {

class EnvPosixTest : public testing::Test {
public:
    EnvPosixTest() { }
    virtual ~EnvPosixTest() { }
    void SetUp() override {
        auto st = Env::Default()->create_dir_if_missing("./ut_dir/env_posix");
        ASSERT_TRUE(st.ok());
    }
    void TearDown() override {
    }
};

TEST_F(EnvPosixTest, random_access) {
    std::string fname = "./ut_dir/env_posix/random_access";
    WritableFileOptions ops;
    std::unique_ptr<WritableFile> wfile;
    auto env = Env::Default();
    auto st = env->new_writable_file(fname, &wfile);
    ASSERT_TRUE(st.ok());
    st = wfile->pre_allocate(1024);
    ASSERT_TRUE(st.ok());
    // wirte data
    Slice field1("123456789");
    st = wfile->append(field1);
    ASSERT_TRUE(st.ok());
    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    st = wfile->append(buf);
    ASSERT_TRUE(st.ok());
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2]{abc, bcd};
    st = wfile->appendv(slices, 2);
    ASSERT_TRUE(st.ok());
    st = wfile->flush(WritableFile::FLUSH_ASYNC);
    ASSERT_TRUE(st.ok());
    st = wfile->sync();
    ASSERT_TRUE(st.ok());
    st = wfile->close();
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(115, wfile->size());

    uint64_t size;
    st = env->get_file_size(fname, &size);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(115, size);
    {
        char mem[1024];
        std::unique_ptr<RandomAccessFile> rfile;
        st = env->new_random_access_file(fname, &rfile);
        ASSERT_TRUE(st.ok());

        Slice slice1(mem, 9);
        Slice slice2(mem + 9, 100);
        Slice slice3(mem + 9 + 100, 3);

        Slice read_slices[3] {slice1, slice2, slice3};
        st = rfile->readv_at(0, read_slices, 3);
        ASSERT_TRUE(st.ok());
        ASSERT_STREQ("123456789", std::string(slice1.data, slice1.size).c_str());
        ASSERT_STREQ("abc", std::string(slice3.data, slice3.size).c_str());

        Slice slice4(mem, 3);
        st = rfile->read_at(112, slice4);
        ASSERT_TRUE(st.ok());
        ASSERT_STREQ("bcd", std::string(slice4.data, slice4.size).c_str());

        // end of file
        st = rfile->read_at(114, slice4);
        ASSERT_EQ(TStatusCode::END_OF_FILE, st.code());
        LOG(INFO) << "st=" << st.to_string();
    }
}

TEST_F(EnvPosixTest, random_rw) {
    std::string fname = "./ut_dir/env_posix/random_rw";
    WritableFileOptions ops;
    std::unique_ptr<RandomRWFile> wfile;
    auto env = Env::Default();
    auto st = env->new_random_rw_file(fname, &wfile);
    ASSERT_TRUE(st.ok());
    // wirte data
    Slice field1("123456789");
    st = wfile->write_at(0, field1);
    ASSERT_TRUE(st.ok());
    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    st = wfile->write_at(9, buf);
    ASSERT_TRUE(st.ok());
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2]{abc, bcd};
    st = wfile->writev_at(0, slices, 2);
    ASSERT_TRUE(st.ok());

    uint64_t size;
    st = wfile->size(&size);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(109, size);

    st = wfile->flush(RandomRWFile::FLUSH_ASYNC, 0, 0);
    ASSERT_TRUE(st.ok());
    st = wfile->sync();
    ASSERT_TRUE(st.ok());
    st = wfile->close();
    ASSERT_TRUE(st.ok());

    st = env->get_file_size(fname, &size);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(109, size);
    {
        char mem[1024];
        std::unique_ptr<RandomRWFile> rfile;
        RandomRWFileOptions opts;
        opts.mode = Env::OPEN_EXISTING;
        st = env->new_random_rw_file(opts, fname, &rfile);
        ASSERT_TRUE(st.ok());

        Slice slice1(mem, 3);
        Slice slice2(mem + 3, 3);
        Slice slice3(mem + 6, 3);

        Slice read_slices[3] {slice1, slice2, slice3};
        st = rfile->readv_at(0, read_slices, 3);
        LOG(INFO) << st.to_string();
        ASSERT_TRUE(st.ok());
        ASSERT_STREQ("abc", std::string(slice1.data, slice1.size).c_str());
        ASSERT_STREQ("bcd", std::string(slice2.data, slice2.size).c_str());
        ASSERT_STREQ("789", std::string(slice3.data, slice3.size).c_str());

        Slice slice4(mem, 100);
        st = rfile->read_at(9, slice4);
        ASSERT_TRUE(st.ok());

        // end of file
        st = rfile->read_at(102, slice4);
        ASSERT_EQ(TStatusCode::END_OF_FILE, st.code());
        LOG(INFO) << "st=" << st.to_string();
    }
    // SequentialFile
    {
        char mem[1024];
        std::unique_ptr<SequentialFile> rfile;
        st = env->new_sequential_file(fname, &rfile);
        ASSERT_TRUE(st.ok());

        Slice slice1(mem, 3);
        st = rfile->read(&slice1);
        ASSERT_TRUE(st.ok());
        ASSERT_STREQ("abc", std::string(slice1.data, slice1.size).c_str());

        st = rfile->skip(3);
        ASSERT_TRUE(st.ok());

        Slice slice3(mem, 3);
        st = rfile->read(&slice3);
        ASSERT_STREQ("789", std::string(slice3.data, slice3.size).c_str());

        st = rfile->skip(90);
        ASSERT_TRUE(st.ok());

        Slice slice4(mem, 15);
        st = rfile->read(&slice4);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(10, slice4.size);


        st = rfile->read(&slice4);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(0, slice4.size);
    }
}

}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
