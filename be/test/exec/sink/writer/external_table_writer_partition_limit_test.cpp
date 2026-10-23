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

#include <gtest/gtest.h>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/sink/writer/iceberg/viceberg_table_writer.h"
#include "exec/sink/writer/vhive_table_writer.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/types.h"
#include "runtime/runtime_state.h"

namespace doris {

class ExternalTableWriterPartitionLimitTest : public testing::TestWithParam<int32_t> {
protected:
    void SetUp() override {
        _original_limit = config::table_sink_partition_write_max_partition_nums_per_writer;
        config::table_sink_partition_write_max_partition_nums_per_writer = GetParam();
    }

    void TearDown() override {
        config::table_sink_partition_write_max_partition_nums_per_writer = _original_limit;
    }

    Block new_partition_block() {
        auto column = ColumnInt32::create();
        column->get_data().push_back(GetParam());
        return Block(
                {ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeInt32>(), "p")});
    }

    void check_limit_status(const Status& status, const std::string& writer_type) {
        EXPECT_EQ(TStatusCode::INVALID_ARGUMENT, status.to_thrift().status_code);
        const auto message = status.to_string();
        EXPECT_NE(std::string::npos, message.find(writer_type + " writer"));
        EXPECT_NE(std::string::npos, message.find("open partitions=" + std::to_string(GetParam())));
        EXPECT_NE(std::string::npos,
                  message.find("attempted partitions=" + std::to_string(GetParam() + 1)));
        EXPECT_NE(std::string::npos, message.find("limit=" + std::to_string(GetParam())));
        EXPECT_NE(std::string::npos, message.find("per-writer limit"));
        EXPECT_NE(std::string::npos,
                  message.find("table_sink_partition_write_max_partition_nums_per_writer"));
        EXPECT_NE(std::string::npos, message.find("Split the write into multiple statements"));
    }

private:
    int32_t _original_limit = 0;
};

TEST_P(ExternalTableWriterPartitionLimitTest, HiveRejectsNextPartitionBeforeOpeningAFile) {
    THiveColumn partition_column;
    partition_column.__set_name("p");
    partition_column.__set_column_type(THiveColumnType::PARTITION_KEY);
    THiveTableSink hive_sink;
    hive_sink.__set_columns({partition_column});
    TDataSink sink;
    sink.__set_hive_table_sink(hive_sink);
    auto type = std::make_shared<DataTypeInt32>();
    VExprContextSPtrs exprs = {
            VExprContext::create_shared(VSlotRef::create_shared(0, 0, -1, type, "p"))};
    VHiveTableWriter writer(sink, exprs, nullptr, nullptr);
    writer._partition_columns_input_index = {0};
    // Existing writers are never dereferenced when rejecting a new partition. No filesystem is needed.
    for (int32_t i = 0; i < GetParam(); ++i) {
        writer._partitions_to_writers.emplace("p=" + std::to_string(i), nullptr);
    }
    RuntimeState state;
    auto block = new_partition_block();
    check_limit_status(writer.write(&state, block), "Hive");
    EXPECT_EQ(static_cast<size_t>(GetParam()), writer._partitions_to_writers.size());
}

TEST_P(ExternalTableWriterPartitionLimitTest, IcebergRejectsNextPartitionBeforeOpeningAFile) {
    TDataSink sink;
    sink.__set_iceberg_table_sink(TIcebergTableSink());
    VExprContextSPtrs exprs;
    iceberg::PartitionField field(1, 1000, "p", "identity");
    VIcebergTableWriter writer(sink, exprs, nullptr, nullptr);
    // open() pins the table schema, and every written block is validated against it.
    std::vector<iceberg::NestedField> columns;
    columns.emplace_back(true, 1, "p", std::make_unique<iceberg::IntegerType>(), std::nullopt);
    writer._schema = std::make_shared<iceberg::Schema>(std::move(columns));
    writer._iceberg_partition_columns.emplace_back(
            field, TYPE_INT, 0, std::vector<size_t> {},
            std::make_unique<IdentityPartitionColumnTransform>(std::make_shared<DataTypeInt32>()));
    for (int32_t i = 0; i < GetParam(); ++i) {
        writer._partitions_to_writers.emplace("existing-" + std::to_string(i), nullptr);
    }
    auto block = new_partition_block();
    check_limit_status(writer.write_prepared_block(block), "Iceberg");
    EXPECT_EQ(static_cast<size_t>(GetParam()), writer._partitions_to_writers.size());
}

INSTANTIATE_TEST_SUITE_P(DefaultAndConfiguredLimits, ExternalTableWriterPartitionLimitTest,
                         testing::Values(128, 2));

} // namespace doris
