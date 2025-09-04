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

#include "enterprise/encrypted_file_system.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <complex>
#include <memory>
#include <random>
#include <string_view>

#include "enterprise/encryption_common.h"
#include "enterprise/key_cache.h"
#include "io/fs/encrypted_fs_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"

namespace doris::io {

static constexpr std::string_view test_dir = "ut_dir/encrypted_fs_test";

FileSystemSPtr encrypted_fs_instance() {
    FileSystemSPtr local_fs = global_local_filesystem();
    return make_file_system(local_fs, EncryptionAlgorithmPB::AES_256_CTR);
}

class LocalEncryptedFileSystemTest : public testing::Test {
public:
    void SetUp() override {
        Status st;
        std::shared_ptr<EncryptionKeyPB> master_key = std::make_shared<EncryptionKeyPB>();
        master_key->mutable_plaintext()->assign(32, 10);
        master_key->set_version(1);
        *master_key->mutable_id() = "master_key_id";
        key_cache._aes256_master_keys[1] = master_key;

        //ExecEnv::GetInstance()->set_key_cache(&_key_cache);
        auto fs = encrypted_fs_instance();
        st = fs->delete_file(test_dir);
        ASSERT_TRUE(st) << st;
        st = fs->create_directory(test_dir);
        ASSERT_TRUE(st) << st;
    }

    void TearDown() override {
        Status st;
        st = encrypted_fs_instance()->delete_directory(test_dir);
        EXPECT_TRUE(st) << st;
        //ExecEnv::GetInstance()->set_key_cache(nullptr);
    }

private:
    //KeyCache _key_cache;
};

TEST_F(LocalEncryptedFileSystemTest, WriteRead) {
    auto fname = fmt::format("{}/abc", test_dir);
    auto fs = encrypted_fs_instance();
    io::FileWriterPtr file_writer;
    auto st = fs->create_file(fname, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    std::string field1("123456789");
    st = file_writer->append(field1);
    ASSERT_TRUE(st.ok()) << st;

    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    st = file_writer->append(buf);
    ASSERT_TRUE(st.ok()) << st;
    std::string abc("abc");
    std::string bcd("bcd");
    Slice slices[2] {abc, bcd};
    st = file_writer->appendv(slices, 2);
    ASSERT_TRUE(st.ok()) << st;
    st = file_writer->close();
    ASSERT_TRUE(st.ok()) << st;

    int64_t fsize;
    st = encrypted_fs_instance()->file_size(fname, &fsize);

    ASSERT_TRUE(st.ok()) << st;
    {
        io::FileReaderSPtr file_reader;
        st = fs->open_file(fname, &file_reader);
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

std::vector<uint8_t> generate_random_data(size_t size) {
    std::vector<uint8_t> data(size);
    auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 engine(static_cast<uint32_t>(seed));
    std::uniform_int_distribution<uint16_t> distribution(0, 255);

    for (auto& byte : data) {
        byte = static_cast<uint8_t>(distribution(engine));
    }

    return data;
}

TEST_F(LocalEncryptedFileSystemTest, RandomRead) {
    io::FileWriterPtr file_writer;
    auto fname = fmt::format("{}/random_write_read", test_dir);
    auto fs = encrypted_fs_instance();
    auto st = fs->create_file(fname, &file_writer);
    ASSERT_TRUE(st) << st;

    auto total_size = 50 * 1024 * 1024;
    auto data = generate_random_data(total_size);

    auto chunk_size = total_size / 4;
    for (size_t i = 0; i < 4; ++i) {
        Slice chunk(data.data() + (i * chunk_size), chunk_size);
        auto st = file_writer->append(chunk);
        ASSERT_TRUE(st) << st;
        ASSERT_EQ(file_writer->bytes_appended(), (i + 1) * chunk_size);
    }
    st = file_writer->close();
    ASSERT_TRUE(st) << st;
    ASSERT_EQ(file_writer->bytes_appended(), total_size);

    std::vector<int64_t> file_sizes {-1, total_size};
    for (auto file_size : file_sizes) {
        io::FileReaderSPtr file_reader;
        Defer defer([&]() { ASSERT_TRUE(file_reader->close()); });
        FileReaderOptions opts;
        opts.file_size = file_size;
        st = fs->open_file(fname, &file_reader, &opts);
        ASSERT_TRUE(st) << st;

        auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        std::mt19937 engine(static_cast<uint32_t>(seed));
        std::uniform_int_distribution<uint16_t> distribution(0, total_size);

        for (size_t i = 0; i < 10; ++i) {
            auto offset = distribution(engine);
            std::uniform_int_distribution<uint16_t> distribution2(0, total_size - offset);
            auto length = distribution2(engine);
            std::vector<uint8_t> result(length);
            Slice ret(result.data(), length);
            size_t bytes_read = 0;
            auto st = file_reader->read_at(offset, ret, &bytes_read);
            ASSERT_TRUE(st) << st;
            ASSERT_EQ(bytes_read, length);
            Slice answer(data.data() + offset, length);
            ASSERT_EQ(answer.compare(ret), 0);
        }
    }
}

TEST_F(LocalEncryptedFileSystemTest, BoundaryRead) {
    io::FileWriterPtr file_writer;
    auto fname = fmt::format("{}/random_write_read_out_of_range", test_dir);
    auto fs = encrypted_fs_instance();
    auto st = fs->create_file(fname, &file_writer);
    ASSERT_TRUE(st) << st;

    auto total_size = 1024;
    auto data = generate_random_data(total_size);

    st = file_writer->append(Slice(data.data(), total_size));
    ASSERT_TRUE(st) << st;
    ASSERT_EQ(file_writer->bytes_appended(), total_size);

    st = file_writer->close();
    ASSERT_TRUE(st) << st;
    ASSERT_EQ(file_writer->bytes_appended(), total_size);

    std::vector<std::pair<size_t, size_t>> cases = {{total_size - 100, 500}, {total_size - 1, 1},
                                                    {total_size - 1, 100},   {total_size, 1},
                                                    {total_size, 100},       {total_size + 1, 0}};
    std::vector<int64_t> file_sizes {-1, total_size};
    for (auto file_size : file_sizes) {
        io::FileReaderSPtr file_reader;
        Defer defer([&]() { ASSERT_TRUE(file_reader->close()); });
        FileReaderOptions opts;
        opts.file_size = file_size;
        st = fs->open_file(fname, &file_reader, &opts);
        ASSERT_TRUE(st) << st;

        for (const auto& c : cases) {
            auto offset = c.first;
            auto length = c.second;
            std::vector<uint8_t> result(length);
            Slice ret(result.data(), length);
            size_t bytes_req;
            auto st = file_reader->read_at(offset, ret, &bytes_req);
            if (offset > total_size) {
                ASSERT_FALSE(st);
            } else if (offset == total_size) {
                ASSERT_TRUE(st) << st;
                ASSERT_EQ(bytes_req, 0);
            } else {
                ASSERT_TRUE(st) << st;
                auto real_len = std::min(length, total_size - offset);
                ASSERT_EQ(bytes_req, real_len);
                Slice answer(data.data() + offset, real_len);
                Slice real_ret(ret.data, real_len);
                ASSERT_EQ(real_ret.compare(answer), 0)
                        << "offset=" << offset << ", length=" << length;
            }
        }
    }
}

TEST_F(LocalEncryptedFileSystemTest, SmallFile) {
    io::FileWriterPtr file_writer;
    auto fname = fmt::format("{}/small", test_dir);
    auto fs = encrypted_fs_instance();
    auto st = fs->create_file(fname, &file_writer);
    ASSERT_TRUE(st) << st;

    std::string s = "1";
    Slice input(s.data(), 1);
    ASSERT_TRUE(file_writer->append(input));
    ASSERT_TRUE(file_writer->close());
    ASSERT_EQ(1, file_writer->bytes_appended());

    std::vector<int64_t> file_sizes {-1, 1};
    for (auto file_size : file_sizes) {
        io::FileReaderSPtr file_reader;
        Defer defer([&]() { ASSERT_TRUE(file_reader->close()); });
        FileReaderOptions opts;
        opts.file_size = file_size;
        st = encrypted_fs_instance()->open_file(fname, &file_reader, &opts);
        ASSERT_TRUE(st) << st;
        size_t bytes_read;
        std::vector<uint8_t> result(1);
        Slice ret(result.data(), 1);
        ASSERT_TRUE(file_reader->read_at(0, ret, &bytes_read));
        Slice answer("1", 1);
        ASSERT_EQ(answer.compare(ret), 0);

        ret = Slice(result.data(), 0);
        ASSERT_TRUE(file_reader->read_at(0, ret, &bytes_read));
        ASSERT_EQ(bytes_read, 0);
    }
}

TEST_F(LocalEncryptedFileSystemTest, EmptyFile) {
    io::FileWriterPtr file_writer;
    auto fname = fmt::format("{}/empty", test_dir);
    auto fs = encrypted_fs_instance();
    auto st = fs->create_file(fname, &file_writer);
    ASSERT_TRUE(st) << st;

    ASSERT_TRUE(file_writer->close());
    ASSERT_EQ(0, file_writer->bytes_appended());

    std::vector<int64_t> file_sizes {-1, 0};
    for (auto file_size : file_sizes) {
        io::FileReaderSPtr file_reader;
        Defer defer([&]() { ASSERT_TRUE(file_reader->close()); });
        FileReaderOptions opts;
        opts.file_size = file_size;
        st = encrypted_fs_instance()->open_file(fname, &file_reader, &opts);
        ASSERT_TRUE(st) << st;
        ASSERT_EQ(file_reader->size(), 0);

        size_t bytes_read;
        std::vector<uint8_t> result(1);
        Slice ret(result.data(), 1);
        ASSERT_TRUE(file_reader->read_at(0, ret, &bytes_read));
        ASSERT_EQ(bytes_read, 0);
    }
}

// It will take more than 60s, verified, run it if necessary
TEST_F(LocalEncryptedFileSystemTest, DISABLED_LargeFile) {
    io::FileWriterPtr file_writer;
    auto fname = fmt::format("{}/large", test_dir);
    auto fs = encrypted_fs_instance();
    auto st = fs->create_file(fname, &file_writer);
    ASSERT_TRUE(st) << st;

    auto total_size = 1024 * 1024 * 1024;
    auto data = generate_random_data(total_size);

    Slice input(data.data(), total_size);
    ASSERT_TRUE(file_writer->append(input));
    ASSERT_TRUE(file_writer->close());
    ASSERT_EQ(file_writer->bytes_appended(), total_size);

    std::vector<int64_t> file_sizes {-1, total_size};
    for (auto file_size : file_sizes) {
        io::FileReaderSPtr file_reader;
        Defer defer([&]() { ASSERT_TRUE(file_reader->close()); });
        FileReaderOptions opts;
        opts.file_size = file_size;
        st = fs->open_file(fname, &file_reader, &opts);
        ASSERT_TRUE(st) << st;

        size_t bytes_read;
        std::vector<uint8_t> result(total_size);
        Slice ret(result.data(), total_size);
        st = file_reader->read_at(0, ret, &bytes_read);
        ASSERT_TRUE(st);
        Slice answer(data.data(), total_size);
        ASSERT_EQ(answer.compare(ret), 0);
    }
}

TEST_F(LocalEncryptedFileSystemTest, x86_arm_test) {
    std::vector<std::string> fnames = {"./be/test/io/fs/test_data/encrypted_x86_file",
                                       "./be/test/io/fs/test_data/encrypted_arm_file"};
    for (std::string fname : fnames) {
        auto fs = encrypted_fs_instance();
        io::FileReaderSPtr file_reader;
        Status st = fs->open_file(fname, &file_reader);
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

} // namespace doris::io
