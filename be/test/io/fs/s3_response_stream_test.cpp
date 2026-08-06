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

#include <sstream>
#include <string>
#include <vector>

#include "io/fs/s3_common.h"

namespace doris {

namespace {

// What the SDK does with the body of a response it has to build an error from.
std::string drain(std::iostream& stream) {
    std::stringstream out;
    out << stream.rdbuf();
    return out.str();
}

// The XML body a MinIO answers a throttled ranged read with, shortened.
constexpr char SLOW_DOWN_BODY[] =
        R"(<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>Please )"
        R"(reduce your request rate.</Message><Key>data/packed_file/2666/x.bin</Key></Error>)";

} // namespace

// A body of the requested size lands in the buffer of the caller, without a copy.
TEST(ResponseStreamTest, BodyFits) {
    std::string body(64, 'a');
    std::vector<char> buffer(body.size());

    ResponseStream stream(buffer.data(), buffer.size());
    stream.write(body.data(), body.size());
    stream.flush();

    EXPECT_FALSE(stream.fail());
    EXPECT_EQ(body, std::string(buffer.data(), buffer.size()));
    EXPECT_EQ(static_cast<std::streampos>(body.size()), stream.tellp());
    EXPECT_EQ(body, drain(stream));
}

// An error body larger than the range of the read leaves the stream usable, which is what
// keeps curl from aborting the transfer and the SDK from losing the status code.
TEST(ResponseStreamTest, ErrorBodyOverflowsInOneWrite) {
    std::string body(SLOW_DOWN_BODY);
    // A read of the footer of a packed file is far smaller than the error document.
    std::vector<char> buffer(12);

    ResponseStream stream(buffer.data(), buffer.size());
    stream.write(body.data(), body.size());
    stream.flush();

    EXPECT_FALSE(stream.fail());
    EXPECT_EQ(static_cast<std::streampos>(body.size()), stream.tellp());
    EXPECT_EQ(body, drain(stream));
}

// curl hands the body over in chunks, so the overflow can happen in the middle of one.
TEST(ResponseStreamTest, ErrorBodyOverflowsAcrossWrites) {
    std::string body(SLOW_DOWN_BODY);
    std::vector<char> buffer(16);

    ResponseStream stream(buffer.data(), buffer.size());
    size_t chunk = 7;
    for (size_t pos = 0; pos < body.size(); pos += chunk) {
        stream.write(body.data() + pos, std::min(chunk, body.size() - pos));
    }
    stream.flush();

    EXPECT_FALSE(stream.fail());
    EXPECT_EQ(static_cast<std::streampos>(body.size()), stream.tellp());
    // The bytes written before the overflow are kept, so the body stays contiguous.
    EXPECT_EQ(body, drain(stream));
}

// A body written one character at a time goes through overflow() instead of xsputn().
TEST(ResponseStreamTest, ErrorBodyOverflowsCharByChar) {
    std::string body(SLOW_DOWN_BODY);
    std::vector<char> buffer(4);

    ResponseStream stream(buffer.data(), buffer.size());
    for (char c : body) {
        stream.put(c);
    }
    stream.flush();

    EXPECT_FALSE(stream.fail());
    EXPECT_EQ(body, drain(stream));
}

// A server answering a ranged read with the whole object must not blow up the memory of the
// backend. The body is truncated, the stream stays good and the read is rejected later on by
// the length check of the caller.
TEST(ResponseStreamTest, OversizedBodyIsTruncated) {
    std::string body(ResponseStreamBuf::MAX_SPILL_SIZE + 4096, 'x');
    std::vector<char> buffer(8);

    ResponseStream stream(buffer.data(), buffer.size());
    stream.write(body.data(), body.size());
    stream.flush();

    EXPECT_FALSE(stream.fail());
    EXPECT_EQ(static_cast<std::streampos>(ResponseStreamBuf::MAX_SPILL_SIZE), stream.tellp());
    EXPECT_EQ(ResponseStreamBuf::MAX_SPILL_SIZE, drain(stream).size());
}

// The SDK rewinds the body before parsing an error out of it.
TEST(ResponseStreamTest, SeekBackAndForth) {
    std::string body(SLOW_DOWN_BODY);
    std::vector<char> buffer(12);

    ResponseStream stream(buffer.data(), buffer.size());
    stream.write(body.data(), body.size());

    EXPECT_EQ(body, drain(stream));
    stream.clear();
    stream.seekg(0);
    EXPECT_EQ(body, drain(stream));

    stream.clear();
    stream.seekg(2);
    EXPECT_EQ(body.substr(2), drain(stream));
}

// An empty body is what tells the SDK to build the error out of the status code alone.
TEST(ResponseStreamTest, EmptyBody) {
    std::vector<char> buffer(16);
    ResponseStream stream(buffer.data(), buffer.size());

    EXPECT_EQ(std::streampos(0), stream.tellp());
    EXPECT_TRUE(drain(stream).empty());
}

} // namespace doris
