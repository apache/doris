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

#include "exec/sink/writer/vjdbc_table_writer.h"

#include <gen_cpp/DataSinks_types.h>
#include <gtest/gtest.h>

#include "exec/pipeline/dependency.h"
#include "runtime/runtime_state.h"

namespace doris {

TEST(VJdbcTableWriterTest, ParametersCarryDialectNamesForJavaDispatch) {
    const std::pair<TOdbcTableType::type, std::string> dialects[] = {
            {TOdbcTableType::MYSQL, "MYSQL"},
            {TOdbcTableType::ORACLE, "ORACLE"},
            {TOdbcTableType::POSTGRESQL, "POSTGRESQL"},
            {TOdbcTableType::SQLSERVER, "SQLSERVER"},
            {TOdbcTableType::CLICKHOUSE, "CLICKHOUSE"},
            {TOdbcTableType::SAP_HANA, "SAP_HANA"},
            {TOdbcTableType::TRINO, "TRINO"},
            {TOdbcTableType::PRESTO, "PRESTO"},
            {TOdbcTableType::OCEANBASE, "OCEANBASE"},
            {TOdbcTableType::OCEANBASE_ORACLE, "OCEANBASE_ORACLE"},
            {TOdbcTableType::DB2, "DB2"},
            {TOdbcTableType::GBASE, "GBASE"}};
    for (const auto& [type, name] : dialects) {
        SCOPED_TRACE(name);
        TDataSink sink;
        sink.jdbc_table_sink.jdbc_table.__set_jdbc_driver_url("file:///driver.jar");
        sink.jdbc_table_sink.__set_table_type(type);
        // Exercise the BE-produced map: Java dispatches on names, not numeric enum values.
        EXPECT_EQ(name, VJdbcTableWriter::_build_writer_params(sink).at("table_type"));
    }
}

TEST(VJdbcTableWriterTest, TerminalStatusPreservesPrimaryError) {
    const VExprContextSPtrs exprs;
    TDataSink sink;
    VJdbcTableWriter writer(sink, exprs, std::make_shared<Dependency>(0, 0, "writer", true),
                            std::make_shared<Dependency>(0, 0, "finish", false));
    RuntimeState state;
    writer._state = &state;
    state.cancel(Status::Cancelled("query cancellation"));
    Status primary = Status::IOError("batch failed");
    EXPECT_EQ(primary.to_string(), writer._transaction_status(primary).to_string());
}

TEST(VJdbcTableWriterTest, RefreshesForceCloseAfterSuccessfulSnapshot) {
    const VExprContextSPtrs exprs;
    TDataSink sink;
    VJdbcTableWriter writer(sink, exprs, std::make_shared<Dependency>(0, 0, "writer", true),
                            std::make_shared<Dependency>(0, 0, "finish", false));
    RuntimeState state;
    writer._state = &state;
    Status snapshot = writer.get_writer_status();
    ASSERT_TRUE(snapshot.ok());
    Status cancelled = Status::Cancelled("force close before JNI");
    writer.force_close(cancelled);
    EXPECT_EQ(cancelled.to_string(), writer._transaction_status(snapshot).to_string());
}

TEST(VJdbcTableWriterTest, QueryCancellationBeforeForceCloseRejectsCommit) {
    const VExprContextSPtrs exprs;
    TDataSink sink;
    VJdbcTableWriter writer(sink, exprs, std::make_shared<Dependency>(0, 0, "writer", true),
                            std::make_shared<Dependency>(0, 0, "finish", false));
    RuntimeState state;
    writer._state = &state;
    EXPECT_FALSE(writer._successful_eos);
    EXPECT_TRUE(writer._transaction_status(Status::OK()).ok());
    ASSERT_TRUE(writer.finish(&state).ok());
    EXPECT_TRUE(writer._successful_eos);
    Status cancelled = Status::Cancelled("cancel after finish before close");
    state.cancel(cancelled);
    EXPECT_TRUE(writer.get_writer_status().ok());
    EXPECT_EQ(cancelled.to_string(), writer._transaction_status(Status::OK()).to_string());
}

} // namespace doris
