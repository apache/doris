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

#include <fcntl.h>
#include <gtest/gtest.h>

#include <memory>

#include "filesystem/buffered_read_stream.h"
#include "filesystem/local_read_stream.h"
#include "filesystem/local_write_stream.h"

namespace doris {

class LocalStreamTest : public testing::Test {
public:
    LocalStreamTest() = default;
    ~LocalStreamTest() override = default;

protected:
    const std::string _content =
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

TEST_F(LocalStreamTest, read_stream) {
    int fd = ::open("ut_dir/read_file", O_RDWR | O_CREAT | O_CLOEXEC, 0666);
    size_t file_size = _content.size();
    ASSERT_EQ(::write(fd, _content.c_str(), file_size), file_size);

    constexpr size_t kBufferSize = 64;
    BufferedReadStream istream(std::make_unique<LocalReadStream>(fd, file_size), kBufferSize);

    char buf1[BUFSIZ];
    size_t read_n1 = 0;

    auto s = istream.read(buf1, file_size, &read_n1);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(std::string_view(buf1, read_n1), _content);

    size_t off = 0;
    s = istream.seek(off);
    ASSERT_TRUE(s.ok());
    istream.read(buf1, 16, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 16));
    off += 16;
    istream.read(buf1, 32, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 32));
    off += 32;
    istream.read(buf1, 128, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 128));

    off = 60;
    s = istream.seek(off);
    ASSERT_TRUE(s.ok());
    istream.read(buf1, 16, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 16));
    off += 16;
    istream.read(buf1, 64, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 64));
    off += 64;
    istream.read(buf1, 128, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 128));

    off = file_size - 108;
    s = istream.seek(off);
    ASSERT_TRUE(s.ok());
    istream.read(buf1, 16, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 16));
    off += 16;
    istream.read(buf1, 32, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 32));
    off += 32;
    istream.read(buf1, 64, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(_content.data() + off, 60));

    ASSERT_TRUE(istream.close().ok());

    ::close(fd);
}

TEST_F(LocalStreamTest, write_stream) {
    int fd = ::open("ut_dir/write_file", O_RDWR | O_CREAT | O_CLOEXEC, 0666);

    constexpr size_t kBufferSize = 64;
    LocalWriteStream ostream(fd, kBufferSize);

    size_t off = 0;
    ASSERT_TRUE(ostream.write(_content.data(), 16).ok());
    off += 16;
    ASSERT_TRUE(ostream.write(_content.data() + off, 32).ok());
    off += 32;
    ASSERT_TRUE(ostream.write(_content.data() + off, 64).ok());
    off += 64;
    ASSERT_TRUE(ostream.write(_content.data() + off, 64).ok());
    off += 64;
    ASSERT_TRUE(ostream.write(_content.data() + off, 128).ok());
    off += 128;
    ASSERT_TRUE(ostream.sync().ok());

    char buf1[BUFSIZ];
    auto res = ::pread(fd, buf1, BUFSIZ, 0);
    ASSERT_EQ(std::string_view(buf1, res), std::string_view(_content.data(), off));

    ASSERT_TRUE(ostream.write(_content.data() + off, _content.size() - off).ok());
    ASSERT_TRUE(ostream.sync().ok());

    res = ::pread(fd, buf1, BUFSIZ, 0);
    ASSERT_EQ(std::string_view(buf1, res), _content);

    ASSERT_TRUE(ostream.close().ok());
}

} // namespace doris
