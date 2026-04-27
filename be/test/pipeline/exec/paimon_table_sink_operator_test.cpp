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

#include "pipeline/exec/paimon_table_sink_operator.h"

#include <gtest/gtest.h>

#include "vec/sink/vpaimon_jni_table_writer.h"

namespace doris {

namespace {

TDataSink build_sink_with_option(const std::string& value) {
    TDataSink sink;
    sink.__set_type(TDataSinkType::PAIMON_TABLE_SINK);
    sink.__isset.paimon_table_sink = true;
    sink.paimon_table_sink.__set_options({{"paimon_use_jni", value}});
    sink.paimon_table_sink.__set_column_names(std::vector<std::string> {"id"});
    return sink;
}

} // namespace

TEST(PaimonTableSinkOperatorTest, UsesJniWriterOnlyForTruthyOption) {
    EXPECT_FALSE(should_use_paimon_jni_writer(TDataSink()));
    EXPECT_FALSE(should_use_paimon_jni_writer(build_sink_with_option("false")));
    EXPECT_FALSE(should_use_paimon_jni_writer(build_sink_with_option("0")));
    EXPECT_TRUE(should_use_paimon_jni_writer(build_sink_with_option("true")));
    EXPECT_TRUE(should_use_paimon_jni_writer(build_sink_with_option("1")));
}

TEST(PaimonTableSinkOperatorTest, CreatesNativeWriterByDefault) {
    TDataSink sink = build_sink_with_option("false");
    auto writer = create_paimon_table_writer(sink, {});
    ASSERT_NE(nullptr, writer.get());
    EXPECT_EQ(nullptr, dynamic_cast<vectorized::VPaimonJniTableWriter*>(writer.get()));
}

TEST(PaimonTableSinkOperatorTest, CreatesJniWriterWhenEnabled) {
    TDataSink sink = build_sink_with_option("true");
    auto writer = create_paimon_table_writer(sink, {});
    ASSERT_NE(nullptr, writer.get());
    EXPECT_NE(nullptr, dynamic_cast<vectorized::VPaimonJniTableWriter*>(writer.get()));
}

} // namespace doris
