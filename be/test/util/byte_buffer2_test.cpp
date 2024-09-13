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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "util/byte_buffer.h"

namespace doris {

class ByteBufferTest : public testing::Test {
public:
    ByteBufferTest() {}
    virtual ~ByteBufferTest() {}
};

TEST_F(ByteBufferTest, normal) {
    ByteBufferPtr buf;
    Status st = ByteBuffer::allocate(4, &buf);
    EXPECT_EQ(0, buf->pos);
    EXPECT_EQ(4, buf->limit);
    EXPECT_EQ(4, buf->capacity);

    char test[] = {1, 2, 3};
    buf->put_bytes(test, 3);

    EXPECT_EQ(3, buf->pos);
    EXPECT_EQ(4, buf->limit);
    EXPECT_EQ(4, buf->capacity);

    EXPECT_EQ(1, buf->remaining());
    buf->flip();
    EXPECT_EQ(0, buf->pos);
    EXPECT_EQ(3, buf->limit);
    EXPECT_EQ(4, buf->capacity);
    EXPECT_EQ(3, buf->remaining());
}

} // namespace doris
