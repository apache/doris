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

#include "exec/sink/writer/paimon/paimon_write_backend.h"

#include <gtest/gtest.h>

#include "exec/sink/writer/paimon/cpp_paimon_write_backend.h"
#include "exec/sink/writer/paimon/jni_paimon_write_backend.h"

#ifdef USE_PAIMON_CPP
#include <paimon/commit_message.h>
#include <paimon/factories/factory_creator.h>
#endif

namespace doris {

TEST(PaimonWriteBackendFactoryTest, SelectBackendType) {
    TPaimonTableSink sink;
    EXPECT_EQ(PaimonBackendType::JNI, PaimonWriteBackendFactory::select_backend_type(sink));

    sink.__set_backend_type(TPaimonWriteBackendType::FFI);
    EXPECT_EQ(PaimonBackendType::UNKNOWN, PaimonWriteBackendFactory::select_backend_type(sink));
    std::unique_ptr<IPaimonWriteBackend> backend;
    EXPECT_FALSE(PaimonWriteBackendFactory::create(sink, &backend).ok());
    EXPECT_EQ(nullptr, backend);

    sink.__set_backend_type(TPaimonWriteBackendType::CPP);
    EXPECT_EQ(PaimonBackendType::CPP, PaimonWriteBackendFactory::select_backend_type(sink));

    sink.__set_backend_type(static_cast<TPaimonWriteBackendType::type>(99));
    EXPECT_EQ(PaimonBackendType::UNKNOWN, PaimonWriteBackendFactory::select_backend_type(sink));
    EXPECT_FALSE(PaimonWriteBackendFactory::create(sink, &backend).ok());
    EXPECT_EQ(nullptr, backend);
}

TEST(CppPaimonWriteBackendTest, DpcmFrame) {
    TPaimonCommitMessage message;
    ASSERT_TRUE(frame_paimon_cpp_commit(std::string("a\0b", 3), 11, &message).ok());
    EXPECT_EQ(std::string("DPCM\0\0\0\x0b\0\0\0\x03", 12) + std::string("a\0b", 3),
              message.payload);
    EXPECT_TRUE(message.__isset.payload);
    EXPECT_FALSE(frame_paimon_cpp_commit(std::string(8 * 1024 * 1024, 'x'), 11, &message).ok());
    EXPECT_FALSE(frame_paimon_cpp_commit("", -1, &message).ok());
}

#ifdef USE_PAIMON_CPP
TEST(CppPaimonWriteBackendTest, LinkedFormatsAndCommitVersion) {
    // Exercise link-time registration without dlopen. An SDK upgrade must revalidate the
    // native serializer against Java FE before changing this expected version.
    EXPECT_NE(nullptr, paimon::FactoryCreator::GetInstance()->Create("parquet"));
    EXPECT_NE(nullptr, paimon::FactoryCreator::GetInstance()->Create("avro"));
    EXPECT_EQ(12, paimon::CommitMessage::CurrentVersion());
}

TEST(CppPaimonWriteBackendTest, FailedOpenCanBeClosedRepeatedly) {
    CppPaimonWriteBackend backend;
    EXPECT_TRUE(backend.close().ok());
    // Missing descriptor is rejected before any RuntimeState/profile access.
    EXPECT_FALSE(backend.open(TPaimonTableSink {}, nullptr, nullptr).ok());
    EXPECT_TRUE(backend.close().ok());
    EXPECT_TRUE(backend.close().ok());
}
#endif

TEST(JniPaimonWriteBackendTest, OpenAbiAndWriteModes) {
    EXPECT_STREQ(
            "(Ljava/lang/String;Ljava/util/Map;[Ljava/lang/String;JLjava/lang/String;ZZLjava/lang/"
            "String;JJJ)V",
            PAIMON_JNI_WRITER_OPEN_SIGNATURE);

    auto append = PaimonJniWriterOpenMode::from_write_mode(TPaimonWriteMode::APPEND);
    EXPECT_FALSE(append.overwrite);
    EXPECT_FALSE(append.changelog);

    auto overwrite = PaimonJniWriterOpenMode::from_write_mode(TPaimonWriteMode::OVERWRITE);
    EXPECT_TRUE(overwrite.overwrite);
    EXPECT_FALSE(overwrite.changelog);

    auto changelog = PaimonJniWriterOpenMode::from_write_mode(TPaimonWriteMode::CHANGELOG);
    EXPECT_FALSE(changelog.overwrite);
    EXPECT_TRUE(changelog.changelog);
}

} // namespace doris
