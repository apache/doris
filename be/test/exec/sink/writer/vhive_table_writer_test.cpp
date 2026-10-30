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

#include "exec/sink/writer/vhive_table_writer.h"

#include <gtest/gtest.h>

#include <tuple>

#include "common/config.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/sink/writer/vhive_partition_writer.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {
namespace {

class FailingCloseTransformer final : public VFileFormatTransformer {
public:
    FailingCloseTransformer(const VExprContextSPtrs& output_exprs, int& close_calls)
            : VFileFormatTransformer(nullptr, output_exprs, false), _close_calls(close_calls) {}

    Status open() override { return Status::OK(); }
    Status write(const Block&) override { return Status::OK(); }
    Status close() override {
        ++_close_calls;
        return Status::IOError("injected rollover close failure");
    }
    int64_t written_len() override { return config::hive_sink_max_file_size + 1; }

private:
    int& _close_calls;
};

} // namespace

class VHiveTableWriterTest : public testing::TestWithParam<std::tuple<bool, bool>> {};

TEST_P(VHiveTableWriterTest, RolloverCloseFailureStopsWriteBeforeReplacingFile) {
    const auto [partitioned, connector_file_sink] = GetParam();
    auto type = std::make_shared<DataTypeInt32>();
    auto output_exprs = MockSlotRef::create_mock_contexts(type);
    auto column = ColumnInt32::create();
    column->insert_value(1);
    Block block {{std::move(column), type, "value"}};

    THiveColumn hive_column;
    hive_column.__set_name("value");
    hive_column.__set_column_type(partitioned ? THiveColumnType::PARTITION_KEY
                                              : THiveColumnType::REGULAR);
    THiveTableSink hive_sink;
    hive_sink.__set_columns({hive_column});
    hive_sink.__set_connector_file_sink(connector_file_sink);
    // A replacement writer must never open a file after the injected close failure.
    THiveLocationParams location;
    location.__set_file_type(TFileType::FILE_LOCAL);
    location.__set_write_path("/dev/null");
    hive_sink.__set_location(location);
    TDataSink sink;
    sink.__set_type(TDataSinkType::HIVE_TABLE_SINK);
    sink.__set_hive_table_sink(hive_sink);

    MockRuntimeState state;
    RuntimeProfile profile("VHiveTableWriterTest");
    RuntimeProfile custom_profile("CustomCounters");
    profile.add_child(&custom_profile, true);
    VHiveTableWriter writer(sink, output_exprs, nullptr, nullptr);
    ASSERT_TRUE(writer.open(&state, &profile).ok());

    const std::string partition_name = partitioned ? "value=1" : "";
    auto old_writer = std::make_shared<VHivePartitionWriter>(
            sink, partition_name, TUpdateMode::APPEND, output_exprs, std::vector<std::string> {},
            VHivePartitionWriter::WriteInfo {.write_path = "unused",
                                             .original_write_path = "unused",
                                             .target_path = "unused",
                                             .file_type = TFileType::FILE_LOCAL,
                                             .broker_addresses = {}},
            std::map<std::string, std::string> {}, std::set<std::string> {}, "part", 0,
            TFileFormatType::FORMAT_PARQUET, TFileCompressType::PLAIN, nullptr,
            sink.hive_table_sink.hadoop_config);
    int close_calls = 0;
    old_writer->_file_format_transformer =
            std::make_unique<FailingCloseTransformer>(output_exprs, close_calls);
    old_writer->_state = &state;
    writer._partitions_to_writers.emplace(partition_name, old_writer);

    Status status = writer.write(&state, block);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("injected rollover close failure"));
    EXPECT_EQ(1, close_calls);
    ASSERT_EQ(1, writer._partitions_to_writers.size());
    EXPECT_EQ(old_writer, writer._partitions_to_writers.at(partition_name));
    EXPECT_EQ(0, writer._write_file_count);
    EXPECT_TRUE(state.connector_file_commit_datas().empty());
    EXPECT_TRUE(state.hive_partition_updates().empty());
}

INSTANTIATE_TEST_SUITE_P(PartitionAndSinkKinds, VHiveTableWriterTest,
                         testing::Combine(testing::Bool(), testing::Bool()));

} // namespace doris
