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

#include "olap/byte_buffer.h"

#include <gtest/gtest.h>
#include <sys/mman.h>

#include <filesystem>

#include "common/configbase.h"
#include "olap/file_helper.h"
#include "util/logging.h"

namespace doris {

class TestByteBuffer : public testing::Test {
public:
    virtual ~TestByteBuffer() {}
    virtual void SetUp() {}
    virtual void TearDown() {
        if (std::filesystem::exists(".test_byte_buffer")) {
            EXPECT_TRUE(std::filesystem::remove_all(".test_byte_buffer"));
        }
    }
};

// 测试基本的读写功能
TEST_F(TestByteBuffer, TestReadWrite) {
    StorageByteBuffer* buf1 = nullptr;

    buf1 = StorageByteBuffer::create(100);
    EXPECT_TRUE(buf1 != nullptr);

    char in[10] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(Status::OK(), buf1->put(in, sizeof(in)));
        EXPECT_EQ(100u - (i + 1) * sizeof(in), buf1->remaining());
        EXPECT_EQ((i + 1) * sizeof(in), buf1->position());
    }

    // 参数错误的指定写
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND), buf1->put(in, sizeof(in), 5, 10));

    for (int i = 0; i < 50; i++) {
        EXPECT_EQ(Status::OK(), buf1->put(i));
        EXPECT_EQ(50u - (i + 1), buf1->remaining());
        EXPECT_EQ(50u + i + 1, buf1->position());
    }

    // 再写就失败了
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW), buf1->put(0));
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW), buf1->put(in, sizeof(in)));

    // 转为读模式
    buf1->flip();

    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 10; j++) {
            char byte;
            EXPECT_EQ(Status::OK(), buf1->get(&byte));
            EXPECT_EQ(100u - (i * 10 + j + 1), buf1->remaining());
            EXPECT_EQ(i * 10 + j + 1, buf1->position());
            EXPECT_EQ('a' + j, byte);
        }
    }
    char buf[50];
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND), buf1->get(buf, 100));
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW), buf1->get(buf, 10, 50));
    EXPECT_EQ(Status::OK(), buf1->get(buf, sizeof(buf)));
    EXPECT_EQ(0u, buf1->remaining());
    EXPECT_EQ(100u, buf1->position());

    for (int i = 0; i < 50; i++) {
        EXPECT_EQ(i, buf[i]);
    }
    char byte;
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND), buf1->get(&byte));
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND), buf1->get(&byte, 1));

    EXPECT_EQ(Status::OK(), buf1->put(10, 'x'));
    EXPECT_EQ(Status::OK(), buf1->get(10, &byte));
    EXPECT_EQ('x', byte);

    EXPECT_EQ(Status::OK(), buf1->set_limit(11));
    EXPECT_EQ(11u, buf1->limit());
    EXPECT_EQ(11u, buf1->position());
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), buf1->set_limit(101));
    EXPECT_EQ(Status::OK(), buf1->set_position(10));
    EXPECT_EQ(Status::OK(), buf1->get(&byte));
    EXPECT_EQ('x', byte);
    EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), buf1->set_position(12));

    SAFE_DELETE(buf1);
}

// 测试ByteBuffer对内存的引用, 尤其是智能指针的引用传递
// 使用valgrind进行内存泄露检查
TEST_F(TestByteBuffer, TestRef) {
    StorageByteBuffer* buf1 = nullptr;

    buf1 = StorageByteBuffer::create(1000);
    EXPECT_TRUE(buf1 != nullptr);

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(Status::OK(), buf1->put(i));
    }
    StorageByteBuffer buf2 = *buf1;
    EXPECT_EQ(buf2.array(), buf1->array());
    StorageByteBuffer buf4(*buf1);
    EXPECT_EQ(buf2.array(), buf1->array());

    StorageByteBuffer* buf3 = nullptr;
    buf3 = StorageByteBuffer::reference_buffer(buf1, 10, 90);

    EXPECT_EQ(90u, buf3->capacity());
    EXPECT_EQ(90u, buf3->limit());
    EXPECT_EQ(0u, buf3->position());

    for (int i = 0; i < 90; i++) {
        char byte;
        EXPECT_EQ(Status::OK(), buf3->get(&byte));
        EXPECT_EQ(i + 10, byte);
    }

    EXPECT_EQ(4u, buf1->_buf.use_count());

    SAFE_DELETE(buf1);
    SAFE_DELETE(buf3);
    EXPECT_EQ(2u, buf2._buf.use_count());
}

TEST_F(TestByteBuffer, TestMmap) {
    FileHandler file_handle;
    std::string file_name = ".test_byte_buffer";
    Status res = file_handle.open_with_mode(file_name, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    EXPECT_EQ(Status::OK(), res);

    char buf[100];
    for (int i = 0; i < 100; i++) {
        buf[i] = i;
    }
    EXPECT_EQ(Status::OK(), file_handle.write(buf, 100));
    file_handle.close();

    res = file_handle.open(file_name, O_RDWR);
    EXPECT_EQ(Status::OK(), res);
    StorageByteBuffer* buf1 = StorageByteBuffer::mmap(nullptr, 80, PROT_READ | PROT_WRITE,
                                                      MAP_SHARED, file_handle.fd(), 0);
    // mmap完成后就可以关闭原fd
    file_handle.close();
    EXPECT_TRUE(buf1 != nullptr);

    for (int i = 0; i < 80; i++) {
        char byte;
        EXPECT_EQ(Status::OK(), buf1->get(&byte));
        EXPECT_EQ(i, byte);
    }

    // 测试通过mmap写入数据
    buf1->set_position(0);
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(Status::OK(), buf1->put('x'));
    }

    SAFE_DELETE(buf1);

    res = file_handle.open(file_name, O_RDONLY);
    EXPECT_EQ(Status::OK(), res);
    EXPECT_EQ(Status::OK(), file_handle.pread(buf, 10, SEEK_SET));
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ('x', buf[i]);
    }
}

} // namespace doris
