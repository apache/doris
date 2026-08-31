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

#include "exec/sink/writer/iceberg/viceberg_table_writer.h"

#include <gtest/gtest.h>

#include "common/exception.h"
#include "format/table/iceberg/partition_spec_parser.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/types.h"

namespace doris {

TEST(VIcebergTableWriterTest, RejectMissingPartitionSource) {
    std::vector<iceberg::NestedField> columns;
    columns.emplace_back(false, 3, "id", std::make_unique<iceberg::IntegerType>(), std::nullopt);
    auto schema = std::make_shared<iceberg::Schema>(std::move(columns));
    const std::string spec_json =
            R"({"spec-id":1,"fields":[{"name":"missing","transform":"identity",)"
            R"("source-id":1,"field-id":1000}]})";

    TIcebergTableSink iceberg_sink;
    TDataSink data_sink;
    data_sink.__set_iceberg_table_sink(iceberg_sink);
    VExprContextSPtrs output_exprs;
    VIcebergTableWriter writer(data_sink, output_exprs);
    writer._schema = schema;
    writer._partition_spec = iceberg::PartitionSpecParser::from_json(schema, spec_json);

    try {
        static_cast<void>(writer._to_iceberg_partition_columns());
        FAIL() << "missing partition source must fail writer initialization";
    } catch (const Exception& exception) {
        EXPECT_NE(exception.to_string().find("source field 1 outside writer schema"),
                  std::string::npos);
    }
}

} // namespace doris
