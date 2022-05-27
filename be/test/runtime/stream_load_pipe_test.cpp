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

#include "runtime/stream_load/stream_load_pipe.h"

#include <gtest/gtest.h>

#include <thread>

namespace doris {

class StreamLoadPipeTest : public testing::Test {
public:
    StreamLoadPipeTest() {}
    virtual ~StreamLoadPipeTest() {}
    void SetUp() override {}
};

TEST_F(StreamLoadPipeTest, append_buffer) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        int k = 0;
        for (int i = 0; i < 2; ++i) {
            auto byte_buf = ByteBuffer::allocate(64);
            char buf[64];
            for (int j = 0; j < 64; ++j) {
                buf[j] = '0' + (k++ % 10);
            }
            byte_buf->put_bytes(buf, 64);
            byte_buf->flip();
            pipe.append(byte_buf);
        }
        pipe.finish();
    };
    std::thread t1(appender);

    char buf[256];
    int64_t buf_len = 256;
    int64_t read_bytes = 0;
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(128, read_bytes);
    EXPECT_FALSE(eof);
    for (int i = 0; i < 128; ++i) {
        EXPECT_EQ('0' + (i % 10), buf[i]);
    }
    st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(0, read_bytes);
    EXPECT_TRUE(eof);

    t1.join();
}

TEST_F(StreamLoadPipeTest, append_bytes) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        for (int i = 0; i < 128; ++i) {
            char buf = '0' + (i % 10);
            pipe.append(&buf, 1);
        }
        pipe.finish();
    };
    std::thread t1(appender);

    char buf[256];
    int64_t buf_len = 256;
    int64_t read_bytes = 0;
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(128, read_bytes);
    EXPECT_FALSE(eof);
    for (int i = 0; i < 128; ++i) {
        EXPECT_EQ('0' + (i % 10), buf[i]);
    }
    st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(0, read_bytes);
    EXPECT_TRUE(eof);

    t1.join();
}

TEST_F(StreamLoadPipeTest, append_bytes2) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        for (int i = 0; i < 128; ++i) {
            char buf = '0' + (i % 10);
            pipe.append(&buf, 1);
        }
        pipe.finish();
    };
    std::thread t1(appender);

    char buf[128];
    int64_t buf_len = 62;
    int64_t read_bytes = 0;
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(62, read_bytes);
    EXPECT_FALSE(eof);
    for (int i = 0; i < 62; ++i) {
        EXPECT_EQ('0' + (i % 10), buf[i]);
    }
    for (int i = 62; i < 128; ++i) {
        char ch;
        buf_len = 1;
        auto st = pipe.read((uint8_t*)&ch, buf_len, &read_bytes, &eof);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(1, read_bytes);
        EXPECT_FALSE(eof);
        EXPECT_EQ('0' + (i % 10), ch);
    }
    st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(0, read_bytes);
    EXPECT_TRUE(eof);

    t1.join();
}

TEST_F(StreamLoadPipeTest, append_mix) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        // 10
        int k = 0;
        for (int i = 0; i < 10; ++i) {
            char buf = '0' + (k++ % 10);
            pipe.append(&buf, 1);
        }
        // 60
        {
            auto byte_buf = ByteBuffer::allocate(60);
            char buf[60];
            for (int j = 0; j < 60; ++j) {
                buf[j] = '0' + (k++ % 10);
            }
            byte_buf->put_bytes(buf, 60);
            byte_buf->flip();
            pipe.append(byte_buf);
        }
        // 8
        for (int i = 0; i < 8; ++i) {
            char buf = '0' + (k++ % 10);
            pipe.append(&buf, 1);
        }
        // 50
        {
            auto byte_buf = ByteBuffer::allocate(50);
            char buf[50];
            for (int j = 0; j < 50; ++j) {
                buf[j] = '0' + (k++ % 10);
            }
            byte_buf->put_bytes(buf, 50);
            byte_buf->flip();
            pipe.append(byte_buf);
        }
        pipe.finish();
    };
    std::thread t1(appender);

    char buf[128];
    int64_t buf_len = 128;
    int64_t read_bytes = 0;
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(128, read_bytes);
    EXPECT_FALSE(eof);
    for (int i = 0; i < 128; ++i) {
        EXPECT_EQ('0' + (i % 10), buf[i]);
    }
    st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(0, read_bytes);
    EXPECT_TRUE(eof);

    t1.join();
}

TEST_F(StreamLoadPipeTest, cancel) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        int k = 0;
        for (int i = 0; i < 10; ++i) {
            char buf = '0' + (k++ % 10);
            pipe.append(&buf, 1);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        pipe.cancel("test");
    };
    std::thread t1(appender);

    char buf[128];
    int64_t buf_len = 128;
    int64_t read_bytes = 0;
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, buf_len, &read_bytes, &eof);
    EXPECT_FALSE(st.ok());
    t1.join();
}

TEST_F(StreamLoadPipeTest, close) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        int k = 0;
        {
            auto byte_buf = ByteBuffer::allocate(64);
            char buf[64];
            for (int j = 0; j < 64; ++j) {
                buf[j] = '0' + (k++ % 10);
            }
            byte_buf->put_bytes(buf, 64);
            byte_buf->flip();
            pipe.append(byte_buf);
        }
        {
            auto byte_buf = ByteBuffer::allocate(64);
            char buf[64];
            for (int j = 0; j < 64; ++j) {
                buf[j] = '0' + (k++ % 10);
            }
            byte_buf->put_bytes(buf, 64);
            byte_buf->flip();
            auto st = pipe.append(byte_buf);
            EXPECT_FALSE(st.ok());
        }
    };
    std::thread t1(appender);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pipe.close();

    t1.join();
}

} // namespace doris
