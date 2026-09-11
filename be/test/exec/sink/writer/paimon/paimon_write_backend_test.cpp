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
#include <paimon/commit_message.h>
#include <paimon/factories/factory_creator.h>

#include "exec/sink/writer/paimon/cpp_paimon_write_backend.h"

namespace doris {

TEST(CppPaimonWriteBackendTest, DpcmFrame) {
    TPaimonCommitMessage message;
    ASSERT_TRUE(frame_paimon_cpp_commit(std::string("a\0b", 3), 11, &message).ok());
    EXPECT_EQ(std::string("DPCM\0\0\0\x0b\0\0\0\x03", 12) + std::string("a\0b", 3),
              message.payload);
    EXPECT_TRUE(message.__isset.payload);
    EXPECT_FALSE(frame_paimon_cpp_commit(std::string(8 * 1024 * 1024, 'x'), 11, &message).ok());
    EXPECT_FALSE(frame_paimon_cpp_commit("", -1, &message).ok());
}

TEST(CppPaimonWriteBackendTest, LinkedFormatsAndCommitVersion) {
    // Exercise link-time registration without dlopen. An SDK upgrade must revalidate the
    // native serializer against Java FE before changing this expected version.
    EXPECT_NE(nullptr, paimon::FactoryCreator::GetInstance()->Create("parquet"));
    EXPECT_NE(nullptr, paimon::FactoryCreator::GetInstance()->Create("orc"));
    EXPECT_NE(nullptr, paimon::FactoryCreator::GetInstance()->Create("avro"));
    EXPECT_EQ(12, paimon::CommitMessage::CurrentVersion());
}

} // namespace doris
