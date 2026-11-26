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

#include "format_v2/table/delta_history_reader.h"

#include <gtest/gtest.h>

#include "core/block/block.h"
#include "core/value/timestamptz_value.h"

namespace doris::format::deltalake {
namespace {

TEST(DeltaHistoryReaderTest, ReadsTypedColumnsInBatches) {
    TDeltaLakeHistoryJob job;
    job.__set_job_id("job-1");
    job.__set_run_id("run-8");
    TDeltaLakeHistoryNotebook notebook;
    notebook.__set_notebook_id("notebook-1");
    TDeltaLakeHistoryMapEntry parameter;
    parameter.__set_key("mode");
    parameter.__set_value("Append");
    TDeltaLakeHistoryMapEntry null_parameter;
    null_parameter.__set_key("unknown");
    TDeltaLakeHistoryMapEntry metric;
    metric.__set_key("numFiles");
    metric.__set_value("2");

    std::vector<TDeltaLakeHistoryRow> commits(2);
    auto& commit = commits[0];
    commit.__set_version(3);
    commit.__set_timestamp_millis(-1);
    commit.__set_user_id("42");
    commit.__set_user_name("spark");
    commit.__set_operation("WRITE");
    commit.__set_operation_parameters({parameter, null_parameter});
    commit.__set_job(job);
    commit.__set_notebook(notebook);
    commit.__set_cluster_id("cluster-a");
    commit.__set_read_version(2);
    commit.__set_isolation_level("Serializable");
    commit.__set_is_blind_append(false);
    commit.__set_operation_metrics({metric});
    commit.__set_user_metadata("metadata");
    commit.__set_engine_info("Delta");
    commits[1].__set_version(2);
    commits[1].__set_timestamp_millis(-1001);

    auto fs = std::make_shared<io::FileSystemProperties>();
    auto file = std::make_unique<io::FileDescription>();
    DeltaHistoryFileReader reader(fs, file, nullptr, nullptr, commits);
    ASSERT_TRUE(reader.init(nullptr).ok());
    reader.set_batch_size(1);

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader.get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 15);
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    Block block;
    for (const auto& column : schema) {
        ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(column.local_id)).ok());
        block.insert({column.type->create_column(), column.type, column.name});
    }
    ASSERT_TRUE(reader.open(request).ok());
    size_t rows = 0;
    bool eof = false;
    const auto status = reader.get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(rows, 1);
    ASSERT_FALSE(eof);
    auto field = [&](size_t position) { return (*block.get_by_position(position).column)[0]; };
    EXPECT_EQ(field(0).get<TYPE_BIGINT>(), 3);
    const TimestampTzValue timestamp(field(1).get<TYPE_TIMESTAMPTZ>());
    EXPECT_EQ(timestamp.year(), 1969);
    EXPECT_EQ(timestamp.second(), 59);
    EXPECT_EQ(timestamp.microsecond(), 999000);
    EXPECT_EQ(field(2).get<TYPE_STRING>(), "42");
    EXPECT_EQ(field(3).get<TYPE_STRING>(), "spark");
    EXPECT_EQ(field(4).get<TYPE_STRING>(), "WRITE");
    const Field parameters = field(5);
    const auto& map = parameters.get<TYPE_MAP>();
    EXPECT_EQ(map[0].get<TYPE_ARRAY>()[0].get<TYPE_STRING>(), "mode");
    EXPECT_EQ(map[1].get<TYPE_ARRAY>()[0].get<TYPE_STRING>(), "Append");
    EXPECT_TRUE(map[1].get<TYPE_ARRAY>()[1].is_null());
    const Field job_field = field(6);
    const auto& job_fields = job_field.get<TYPE_STRUCT>();
    EXPECT_EQ(job_fields[0].get<TYPE_STRING>(), "job-1");
    EXPECT_TRUE(job_fields[1].is_null());
    EXPECT_EQ(job_fields[3].get<TYPE_STRING>(), "run-8");
    EXPECT_EQ(field(7).get<TYPE_STRUCT>()[0].get<TYPE_STRING>(), "notebook-1");
    EXPECT_EQ(field(8).get<TYPE_STRING>(), "cluster-a");
    EXPECT_EQ(field(9).get<TYPE_BIGINT>(), 2);
    EXPECT_EQ(field(10).get<TYPE_STRING>(), "Serializable");
    EXPECT_FALSE(field(11).get<TYPE_BOOLEAN>());
    EXPECT_EQ(field(12).get<TYPE_MAP>()[1].get<TYPE_ARRAY>()[0].get<TYPE_STRING>(), "2");
    EXPECT_EQ(field(13).get<TYPE_STRING>(), "metadata");
    EXPECT_EQ(field(14).get<TYPE_STRING>(), "Delta");

    block.clear_column_data();
    ASSERT_TRUE(reader.get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    ASSERT_TRUE(eof);
    EXPECT_EQ(field(0).get<TYPE_BIGINT>(), 2);
    for (size_t i = 2; i < schema.size(); ++i) {
        EXPECT_TRUE(field(i).is_null()) << i;
    }
}

TEST(DeltaHistoryReaderTest, VersionProjectionDoesNotRequireTimestampOrCommitInfo) {
    std::vector<TDeltaLakeHistoryRow> commits(1);
    commits[0].__set_version(7);
    auto fs = std::make_shared<io::FileSystemProperties>();
    auto file = std::make_unique<io::FileDescription>();
    DeltaHistoryFileReader reader(fs, file, nullptr, nullptr, commits);
    ASSERT_TRUE(reader.init(nullptr).ok());
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader.get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader.open(request).ok());
    Block block;
    block.insert({schema[0].type->create_column(), schema[0].type, schema[0].name});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader.get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_TRUE(eof);
    EXPECT_EQ((*block.get_by_position(0).column)[0].get<TYPE_BIGINT>(), 7);
}

} // namespace
} // namespace doris::format::deltalake
