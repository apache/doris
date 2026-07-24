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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#pragma clang diagnostic ignored "-Wshadow-field"
#define private public
#include "exec/sink/writer/paimon/vpaimon_jni_table_writer.h"
#undef private
#pragma clang diagnostic pop

#include <gtest/gtest.h>

#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"

namespace doris {

namespace {

TDataSink build_sink() {
    TDataSink sink;
    sink.__set_type(TDataSinkType::PAIMON_TABLE_SINK);
    sink.__isset.paimon_table_sink = true;
    sink.paimon_table_sink.__set_column_names(std::vector<std::string> {"id", "pt"});
    return sink;
}

Block build_block() {
    Block block;
    auto id_col = ColumnInt32::create();
    id_col->insert_value(1);
    id_col->insert_value(2);
    block.insert(ColumnWithTypeAndName(std::move(id_col), std::make_shared<DataTypeInt32>(), "id"));

    auto pt_col = ColumnString::create();
    pt_col->insert_data("p1", 2);
    pt_col->insert_data("p2", 2);
    block.insert(
            ColumnWithTypeAndName(std::move(pt_col), std::make_shared<DataTypeString>(), "pt"));
    return block;
}

} // namespace

TEST(VPaimonJniTableWriterTest, InitPropertiesReturnsOk) {
    TDataSink sink = build_sink();
    VPaimonJniTableWriter writer(sink, {});
    ASSERT_TRUE(writer.init_properties(nullptr).ok());
}

TEST(VPaimonJniTableWriterTest, GetJniEnvFailsWhenNoJvmCreated) {
    TDataSink sink = build_sink();
    VPaimonJniTableWriter writer(sink, {});
    JNIEnv* env = nullptr;
    Status st = writer._get_jni_env(&env);
    ASSERT_FALSE(st.ok());
    ASSERT_NE(std::string::npos, st.to_string().find("Failed to get created JavaVM"));
}

TEST(VPaimonJniTableWriterTest, WriteReturnsOkForEmptyBlock) {
    TDataSink sink = build_sink();
    VPaimonJniTableWriter writer(sink, {});
    Block block;
    ASSERT_TRUE(writer.write(nullptr, block).ok());
}

TEST(VPaimonJniTableWriterTest, AppendToBufferTracksRowsAndBytes) {
    TDataSink sink = build_sink();
    VPaimonJniTableWriter writer(sink, {});
    Block block = build_block();

    ASSERT_TRUE(writer._append_to_buffer(block).ok());
    ASSERT_NE(nullptr, writer._buffer.get());
    ASSERT_EQ(block.rows(), writer._buffered_rows);
    ASSERT_EQ(block.bytes(), writer._buffered_bytes);
}

TEST(VPaimonJniTableWriterTest, FlushBufferWithoutRowsIsNoop) {
    TDataSink sink = build_sink();
    VPaimonJniTableWriter writer(sink, {});

    ASSERT_TRUE(writer._flush_buffer().ok());
    ASSERT_EQ(nullptr, writer._buffer.get());
    ASSERT_EQ(0, writer._buffered_rows);
    ASSERT_EQ(0, writer._buffered_bytes);
}

TEST(VPaimonJniTableWriterTest, CloseReturnsInputStatusWhenJvmUnavailable) {
    TDataSink sink = build_sink();
    VPaimonJniTableWriter writer(sink, {});
    Status input = Status::InternalError("jni-close-error");
    Status result = writer.close(input);
    ASSERT_FALSE(result.ok());
    ASSERT_NE(std::string::npos, result.to_string().find("jni-close-error"));
}

TEST(VPaimonJniTableWriterTest, CloseReturnsOkWhenJvmUnavailableAndInputOk) {
    TDataSink sink = build_sink();
    VPaimonJniTableWriter writer(sink, {});
    ASSERT_TRUE(writer.close(Status::OK()).ok());
}

} // namespace doris
