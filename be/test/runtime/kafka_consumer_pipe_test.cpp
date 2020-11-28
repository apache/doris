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

#include "runtime/routine_load/kafka_consumer_pipe.h"

#include <gtest/gtest.h>

namespace doris {

class KafkaConsumerPipeTest : public testing::Test {
public:
    KafkaConsumerPipeTest() {}
    virtual ~KafkaConsumerPipeTest() {}

    void SetUp() override {}

    void TearDown() override {}

private:
};

TEST_F(KafkaConsumerPipeTest, append_read) {
    KafkaConsumerPipe k_pipe(1024 * 1024, 64 * 1024);

    std::string msg1 = "i have a dream";
    std::string msg2 = "This is from kafka";

    Status st;
    st = k_pipe.append_with_line_delimiter(msg1.c_str(), msg1.length());
    ASSERT_TRUE(st.ok());
    st = k_pipe.append_with_line_delimiter(msg2.c_str(), msg2.length());
    ASSERT_TRUE(st.ok());
    st = k_pipe.finish();
    ASSERT_TRUE(st.ok());

    char buf[1024];
    size_t data_size = 1024;
    bool eof = false;
    st = k_pipe.read((uint8_t*)buf, &data_size, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(data_size, msg1.length() + msg2.length() + 2);
    ASSERT_EQ(eof, false);

    data_size = 1024;
    st = k_pipe.read((uint8_t*)buf, &data_size, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(data_size, 0);
    ASSERT_EQ(eof, true);
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
