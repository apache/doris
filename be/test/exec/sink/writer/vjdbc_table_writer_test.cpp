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

} // namespace doris
