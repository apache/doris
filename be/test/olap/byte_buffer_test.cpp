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
            ASSERT_TRUE(std::filesystem::remove_all(".test_byte_buffer"));
        }
    }
};

// 测试基本的读写功能
TEST_F(TestByteBuffer, TestReadWrite) {
    StorageByteBuffer* buf1 = nullptr;

    buf1 = StorageByteBuffer::create(100);
    ASSERT_TRUE(buf1 != nullptr);

    char in[10] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(OLAP_SUCCESS, buf1->put(in, sizeof(in)));
        ASSERT_EQ(100u - (i + 1) * sizeof(in), buf1->remaining());
        ASSERT_EQ((i + 1) * sizeof(in), buf1->position());
    }

    // 参数错误的指定写
    ASSERT_EQ(OLAP_ERR_OUT_OF_BOUND, buf1->put(in, sizeof(in), 5, 10));

    for (int i = 0; i < 50; i++) {
        ASSERT_EQ(OLAP_SUCCESS, buf1->put(i));
        ASSERT_EQ(50u - (i + 1), buf1->remaining());
        ASSERT_EQ(50u + i + 1, buf1->position());
    }

    // 再写就失败了
    ASSERT_EQ(OLAP_ERR_BUFFER_OVERFLOW, buf1->put(0));
    ASSERT_EQ(OLAP_ERR_BUFFER_OVERFLOW, buf1->put(in, sizeof(in)));

    // 转为读模式
    buf1->flip();

    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 10; j++) {
            char byte;
            ASSERT_EQ(OLAP_SUCCESS, buf1->get(&byte));
            ASSERT_EQ(100u - (i * 10 + j + 1), buf1->remaining());
            ASSERT_EQ(i * 10 + j + 1, buf1->position());
            ASSERT_EQ('a' + j, byte);
        }
    }
    char buf[50];
    ASSERT_EQ(OLAP_ERR_OUT_OF_BOUND, buf1->get(buf, 100));
    ASSERT_EQ(OLAP_ERR_BUFFER_OVERFLOW, buf1->get(buf, 10, 50));
    ASSERT_EQ(OLAP_SUCCESS, buf1->get(buf, sizeof(buf)));
    ASSERT_EQ(0u, buf1->remaining());
    ASSERT_EQ(100u, buf1->position());

    for (int i = 0; i < 50; i++) {
        ASSERT_EQ(i, buf[i]);
    }
    char byte;
    ASSERT_EQ(OLAP_ERR_OUT_OF_BOUND, buf1->get(&byte));
    ASSERT_EQ(OLAP_ERR_OUT_OF_BOUND, buf1->get(&byte, 1));

    ASSERT_EQ(OLAP_SUCCESS, buf1->put(10, 'x'));
    ASSERT_EQ(OLAP_SUCCESS, buf1->get(10, &byte));
    ASSERT_EQ('x', byte);

    ASSERT_EQ(OLAP_SUCCESS, buf1->set_limit(11));
    ASSERT_EQ(11u, buf1->limit());
    ASSERT_EQ(11u, buf1->position());
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, buf1->set_limit(101));
    ASSERT_EQ(OLAP_SUCCESS, buf1->set_position(10));
    ASSERT_EQ(OLAP_SUCCESS, buf1->get(&byte));
    ASSERT_EQ('x', byte);
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, buf1->set_position(12));

    SAFE_DELETE(buf1);
}

// 测试ByteBuffer对内存的引用, 尤其是智能指针的引用传递
// 使用valgrind进行内存泄露检查
TEST_F(TestByteBuffer, TestRef) {
    StorageByteBuffer* buf1 = nullptr;

    buf1 = StorageByteBuffer::create(1000);
    ASSERT_TRUE(buf1 != nullptr);

    for (int i = 0; i < 256; i++) {
        ASSERT_EQ(OLAP_SUCCESS, buf1->put(i));
    }
    StorageByteBuffer buf2 = *buf1;
    ASSERT_EQ(buf2.array(), buf1->array());
    StorageByteBuffer buf4(*buf1);
    ASSERT_EQ(buf2.array(), buf1->array());

    StorageByteBuffer* buf3 = nullptr;
    buf3 = StorageByteBuffer::reference_buffer(buf1, 10, 90);

    ASSERT_EQ(90u, buf3->capacity());
    ASSERT_EQ(90u, buf3->limit());
    ASSERT_EQ(0u, buf3->position());

    for (int i = 0; i < 90; i++) {
        char byte;
        ASSERT_EQ(OLAP_SUCCESS, buf3->get(&byte));
        ASSERT_EQ(i + 10, byte);
    }

    ASSERT_EQ(4u, buf1->_buf.use_count());

    SAFE_DELETE(buf1);
    SAFE_DELETE(buf3);
    ASSERT_EQ(2u, buf2._buf.use_count());
}

TEST_F(TestByteBuffer, TestMmap) {
    FileHandler file_handle;
    std::string file_name = ".test_byte_buffer";
    OLAPStatus res = file_handle.open_with_mode(file_name, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    ASSERT_EQ(OLAP_SUCCESS, res);

    char buf[100];
    for (int i = 0; i < 100; i++) {
        buf[i] = i;
    }
    ASSERT_EQ(OLAP_SUCCESS, file_handle.write(buf, 100));
    file_handle.close();

    res = file_handle.open(file_name, O_RDWR);
    ASSERT_EQ(OLAP_SUCCESS, res);
    StorageByteBuffer* buf1 = StorageByteBuffer::mmap(nullptr, 80, PROT_READ | PROT_WRITE,
                                                      MAP_SHARED, file_handle.fd(), 0);
    // mmap完成后就可以关闭原fd
    file_handle.close();
    ASSERT_TRUE(buf1 != nullptr);

    for (int i = 0; i < 80; i++) {
        char byte;
        ASSERT_EQ(OLAP_SUCCESS, buf1->get(&byte));
        ASSERT_EQ(i, byte);
    }

    // 测试通过mmap写入数据
    buf1->set_position(0);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(OLAP_SUCCESS, buf1->put('x'));
    }

    SAFE_DELETE(buf1);

    res = file_handle.open(file_name, O_RDONLY);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(OLAP_SUCCESS, file_handle.pread(buf, 10, SEEK_SET));
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ('x', buf[i]);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
