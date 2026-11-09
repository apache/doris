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
#include <thrift/TToString.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "exec/scan/file_scan_range_utils.h"

namespace doris {
namespace {

TFileEncryptionInfo encryption_info() {
    TFileEncryptionInfo info;
    info.__set_version(1);
    std::string key(16, 'k');
    key[0] = '\0';
    key[1] = static_cast<char>(255);
    info.__set_key(key);
    info.__set_aad_prefix(std::string("\0aad\xff", 5));
    return info;
}

TEST(FileEncryptionInfoTest, ValidatePresenceVersionAndKeySizes) {
    auto info = encryption_info();
    EXPECT_EQ(file_encryption_validation_error(info), nullptr);
    info.__set_aad_prefix("");
    for (size_t length : {16, 24, 32}) {
        info.__set_key(std::string(length, 'k'));
        EXPECT_EQ(file_encryption_validation_error(info), nullptr);
    }
    info.__isset.aad_prefix = false;
    EXPECT_STREQ(file_encryption_validation_error(info), "File encryption [INVALID_DESCRIPTOR]");
    info = encryption_info();
    info.__set_key("");
    EXPECT_STREQ(file_encryption_validation_error(info), "File encryption [INVALID_DESCRIPTOR]");
    info = encryption_info();
    info.__set_version(9);
    EXPECT_STREQ(file_encryption_validation_error(info), "File encryption [UNSUPPORTED_VERSION]");
    EXPECT_STREQ(file_encryption_validation_error(TFileEncryptionInfo()),
                 "File encryption [INVALID_DESCRIPTOR]");
}

template <typename Protocol>
void round_trip() {
    auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    Protocol protocol(buffer);
    const auto original = encryption_info();
    original.write(&protocol);
    TFileEncryptionInfo decoded;
    decoded.read(&protocol);
    EXPECT_EQ(decoded, original);
}

TEST(FileEncryptionInfoTest, BinaryAndCompactPreserveEveryByte) {
    using namespace apache::thrift::protocol;
    using apache::thrift::transport::TMemoryBuffer;
    round_trip<TBinaryProtocol>();
    round_trip<TCompactProtocol>();
    round_trip<TBinaryProtocolT<TMemoryBuffer>>();
    round_trip<TCompactProtocolT<TMemoryBuffer>>();
}

TEST(FileEncryptionInfoTest, NestedPrintAndThriftDebugStringAreRedacted) {
    const auto info = encryption_info();
    std::ostringstream stream;
    stream << info;
    EXPECT_EQ(stream.str(), "TFileEncryptionInfo(<redacted>)");
    const auto nested = apache::thrift::to_string(std::vector<TFileEncryptionInfo> {info});
    EXPECT_NE(nested.find("<redacted>"), std::string::npos);
    EXPECT_EQ(nested.find("kkkk"), std::string::npos);
    const auto debug = apache::thrift::ThriftDebugString(info);
    EXPECT_NE(debug.find("redacted"), std::string::npos);
    EXPECT_EQ(debug.find("kkkk"), std::string::npos);
}

TEST(FileEncryptionInfoTest, JsonIsDiagnosticOnly) {
    auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TJSONProtocol protocol(buffer);
    encryption_info().write(&protocol);
    const auto diagnostic = buffer->getBufferAsString();
    EXPECT_NE(diagnostic.find("redacted"), std::string::npos);
    EXPECT_EQ(diagnostic.find("a2tr"), std::string::npos);
}

} // namespace
} // namespace doris
