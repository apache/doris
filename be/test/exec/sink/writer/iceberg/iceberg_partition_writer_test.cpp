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

#include <optional>

#include "exec/sink/writer/iceberg/viceberg_partition_writer.h"
#include "exec/sink/writer/iceberg/viceberg_sort_writer.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

namespace {

class FakeFileFormatTransformer final : public VFileFormatTransformer {
public:
    explicit FakeFileFormatTransformer(const VExprContextSPtrs& output_exprs,
                                       Status close_status = Status::OK())
            : VFileFormatTransformer(nullptr, output_exprs, false),
              _close_status(std::move(close_status)) {}

    Status open() override { return Status::OK(); }
    Status write(const Block&) override { return Status::OK(); }
    Status close() override { return _close_status; }
    int64_t written_len() override { return 64; }

private:
    Status _close_status;
};

TDataSink make_table_sink(std::optional<bool> collect_column_stats) {
    TIcebergTableSink iceberg_sink;
    if (collect_column_stats.has_value()) {
        iceberg_sink.__set_collect_column_stats(*collect_column_stats);
    }
    TDataSink sink;
    sink.__set_type(TDataSinkType::ICEBERG_TABLE_SINK);
    sink.__set_iceberg_table_sink(iceberg_sink);
    return sink;
}

} // namespace

class VIcebergPartitionWriterTest : public testing::Test {
protected:
    static std::unique_ptr<VIcebergPartitionWriter> make_writer(
            const TDataSink& sink, const VExprContextSPtrs& output_exprs,
            const iceberg::Schema& schema, const std::string* schema_json,
            const std::map<std::string, std::string>& hadoop_conf) {
        IPartitionWriterBase::WriteInfo write_info;
        write_info.file_type = TFileType::FILE_LOCAL;
        return std::make_unique<VIcebergPartitionWriter>(
                sink, std::vector<std::string> {}, output_exprs, schema, schema_json,
                std::vector<std::string> {}, std::move(write_info), "data", 0,
                TFileFormatType::FORMAT_ORC, TFileCompressType::ZLIB, hadoop_conf);
    }

    static void install_fake_transformer(VIcebergPartitionWriter* writer,
                                         const VExprContextSPtrs& output_exprs,
                                         Status close_status = Status::OK()) {
        writer->_file_format_transformer =
                std::make_unique<FakeFileFormatTransformer>(output_exprs, std::move(close_status));
    }

    static Status build_commit_data(VIcebergPartitionWriter* writer,
                                    TIcebergCommitData* commit_data) {
        return writer->_build_iceberg_commit_data(commit_data);
    }

    static bool collect_column_stats(const VIcebergPartitionWriter& writer) {
        return writer._collect_column_stats;
    }
};

TEST_F(VIcebergPartitionWriterTest, OrcSkipsFooterCollectionWhenMetricsAreDisabled) {
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto writer =
            make_writer(make_table_sink(false), output_exprs, schema, &schema_json, hadoop_conf);
    install_fake_transformer(writer.get(), output_exprs);

    TIcebergCommitData commit_data;
    ASSERT_TRUE(build_commit_data(writer.get(), &commit_data).ok());
    EXPECT_FALSE(commit_data.__isset.column_stats);
}

TEST_F(VIcebergPartitionWriterTest, MissingPolicyKeepsCollectionEnabledForRollingUpgrade) {
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto writer = make_writer(make_table_sink(std::nullopt), output_exprs, schema, &schema_json,
                              hadoop_conf);

    EXPECT_TRUE(collect_column_stats(*writer));
}

TEST_F(VIcebergPartitionWriterTest, SortWriterPropagatesUnderlyingCloseFailure) {
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto partition_writer = std::shared_ptr<VIcebergPartitionWriter>(
            make_writer(make_table_sink(false), output_exprs, schema, &schema_json, hadoop_conf));
    install_fake_transformer(partition_writer.get(), output_exprs,
                             Status::IOError("injected close failure"));
    VIcebergSortWriter sort_writer(partition_writer, TSortInfo(), 1024);
    MockRuntimeState state;
    sort_writer._runtime_state = &state;

    Status status = sort_writer.close(Status::OK());

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("injected close failure"), std::string::npos);
}

TEST_F(VIcebergPartitionWriterTest, EosReservationIncludesActualSpillFanIn) {
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto partition_writer = std::shared_ptr<VIcebergPartitionWriter>(
            make_writer(make_table_sink(false), output_exprs, schema, &schema_json, hadoop_conf));
    VIcebergSortWriter sort_writer(partition_writer, TSortInfo(), 1024);
    MockRuntimeState state;
    ObjectPool pool;
    auto row_desc = std::make_unique<MockRowDescriptor>(
            std::vector<DataTypePtr> {std::make_shared<DataTypeInt64>()}, &pool);
    auto ordering_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};
    sort_writer._sorter = FullSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                                    nulls_first, *row_desc, &state, nullptr);
    sort_writer._sorted_spill_files.resize(12);

    const auto reservation = sort_writer.get_reserve_mem_size_components(&state, true, 0, 0);

    EXPECT_EQ(72 * 1024 * 1024, reservation.transient_workspace);
}

TEST_F(VIcebergPartitionWriterTest, NonEosTargetRolloverReservesWideMergeOutput) {
    constexpr size_t MB = 1024 * 1024;
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto partition_writer = std::shared_ptr<VIcebergPartitionWriter>(
            make_writer(make_table_sink(false), output_exprs, schema, &schema_json, hadoop_conf));
    VIcebergSortWriter sort_writer(partition_writer, TSortInfo(), 64 * MB);
    MockRuntimeState state;
    ObjectPool pool;
    auto row_desc = std::make_unique<MockRowDescriptor>(
            std::vector<DataTypePtr> {std::make_shared<DataTypeString>()}, &pool);
    auto ordering_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeString>());
    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};
    sort_writer._sorter = FullSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                                    nulls_first, *row_desc, &state, nullptr);
    sort_writer._runtime_state = &state;
    Block wide = ColumnHelper::create_block<DataTypeString>(
            {std::string(2 * MB, 'b'), std::string(2 * MB, 'a')});
    ASSERT_TRUE(sort_writer.write(wide).ok());
    ASSERT_TRUE(sort_writer._sorter->do_sort().ok());
    sort_writer._target_file_size_bytes = sort_writer._sorter->data_size() + 1;
    Block tiny = ColumnHelper::create_block<DataTypeString>({"c"});

    const auto reservation = sort_writer.get_reserve_mem_size_components(&state, false, tiny.rows(),
                                                                         tiny.allocated_bytes());

    EXPECT_GE(reservation.transient_workspace, 8 * MB);
}

TEST_F(VIcebergPartitionWriterTest, EosReservationCoversWideNonSpillMergeOutput) {
    constexpr size_t MB = 1024 * 1024;
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto partition_writer = std::shared_ptr<VIcebergPartitionWriter>(
            make_writer(make_table_sink(false), output_exprs, schema, &schema_json, hadoop_conf));
    VIcebergSortWriter sort_writer(partition_writer, TSortInfo(), 64 * MB);
    MockRuntimeState state;
    ObjectPool pool;
    auto row_desc = std::make_unique<MockRowDescriptor>(
            std::vector<DataTypePtr> {std::make_shared<DataTypeString>()}, &pool);
    auto ordering_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeString>());
    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};
    sort_writer._runtime_state = &state;
    sort_writer._sorter = FullSorter::create_unique(ordering_expr_ctxs, -1, 0, &pool, is_asc_order,
                                                    nulls_first, *row_desc, &state, nullptr);

    Block wide = ColumnHelper::create_block<DataTypeString>(
            {std::string(2 * MB, 'b'), std::string(2 * MB, 'a')});
    ASSERT_TRUE(sort_writer.write(wide).ok());
    ASSERT_TRUE(sort_writer._sorter->do_sort().ok());
    Block tiny = ColumnHelper::create_block<DataTypeString>({"c"});
    ASSERT_TRUE(sort_writer.write(tiny).ok());

    const auto reservation = sort_writer.get_reserve_mem_size_components(&state, true, 0, 0);

    EXPECT_GE(reservation.transient_workspace, 8 * MB);
    EXPECT_LT(sort_writer._spill_block_batch_row_count, 4096);
    EXPECT_GE(sort_writer._spill_block_batch_row_count, 1);
}

} // namespace doris
