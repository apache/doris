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

#include "agent/be_exec_version_manager.h"
#include "exec/operator/paimon_table_sink_operator.h"
#include "exec/sink/writer/paimon/ffi_paimon_write_backend.h"
#include "exec/sink/writer/paimon/jni_paimon_write_backend.h"
#include "testutil/mock/mock_runtime_state.h"

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

TEST(PaimonWriteBackendTest, ReportsConcreteBackendType) {
    FfiPaimonWriteBackend ffi_backend;
    JniPaimonWriteBackend jni_backend;

    EXPECT_EQ(PaimonBackendType::FFI, ffi_backend.type());
    EXPECT_EQ(PaimonBackendType::JNI, jni_backend.type());
}

TEST(PaimonWriteBackendTest, AdvertisesPaimonWriteExecVersion) {
    EXPECT_EQ(SUPPORT_PAIMON_WRITE_VERSION, BeExecVersionManager::get_newest_version());
    EXPECT_TRUE(BeExecVersionManager::check_be_exec_version(SUPPORT_PAIMON_WRITE_VERSION).ok());
    EXPECT_FALSE(
            BeExecVersionManager::check_be_exec_version(SUPPORT_PAIMON_WRITE_VERSION + 1).ok());
}

TEST(PaimonTableSinkOperatorTest, InitializesAsBlockingSink) {
    RowDescriptor row_descriptor;
    std::vector<TExpr> output_exprs;
    PaimonTableSinkOperatorX sink_operator(1, row_descriptor, output_exprs);
    PaimonTableSinkLocalState local_state(&sink_operator, nullptr);
    EXPECT_TRUE(local_state.is_blockable());

    TPaimonTableSink paimon_sink;
    TDataSink data_sink;
    data_sink.__set_type(TDataSinkType::PAIMON_TABLE_SINK);
    data_sink.__set_paimon_table_sink(paimon_sink);
    ASSERT_TRUE(sink_operator.init(data_sink).ok());

    MockRuntimeState state;
    EXPECT_TRUE(sink_operator.prepare(&state).ok());
}

} // namespace doris
