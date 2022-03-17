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

#include "runtime/message_body_sink.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace doris {

class MessageBodySinkTest : public testing::Test {
public:
    MessageBodySinkTest() {}
    virtual ~MessageBodySinkTest() {}

    void SetUp() override {}

private:
};

TEST_F(MessageBodySinkTest, file_sink) {
    char data[] = "hello world";

    MessageBodyFileSink sink("./body_sink_test_file_sink");
    ASSERT_TRUE(sink.open().ok());
    ASSERT_TRUE(sink.append(data, sizeof(data)).ok());
    ASSERT_TRUE(sink.finish().ok());

    {
        char buf[256];
        memset(buf, 0, 256);
        int fd = open("././body_sink_test_file_sink", O_RDONLY);
        auto readed_size = read(fd, buf, 256);
        ASSERT_NE(readed_size, -1);
        close(fd);
        ASSERT_STREQ("hello world", buf);
        unlink("././body_sink_test_file_sink");
    }
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
