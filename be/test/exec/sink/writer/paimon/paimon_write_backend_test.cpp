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

#include "exec/sink/writer/paimon/jni_paimon_write_backend.h"

namespace doris {

TEST(PaimonWriteBackendFactoryTest, SelectBackendType) {
    TPaimonTableSink sink;
    EXPECT_EQ(PaimonBackendType::JNI, PaimonWriteBackendFactory::select_backend_type(sink));

    sink.__set_backend_type(TPaimonWriteBackendType::FFI);
    EXPECT_EQ(PaimonBackendType::FFI, PaimonWriteBackendFactory::select_backend_type(sink));
}

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
