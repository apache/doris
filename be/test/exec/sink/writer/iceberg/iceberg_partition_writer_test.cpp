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

namespace doris {

namespace {

class FakeFileFormatTransformer final : public VFileFormatTransformer {
public:
    explicit FakeFileFormatTransformer(const VExprContextSPtrs& output_exprs)
            : VFileFormatTransformer(nullptr, output_exprs, false) {}

    Status open() override { return Status::OK(); }
    Status write(const Block&) override { return Status::OK(); }
    Status close() override { return Status::OK(); }
    int64_t written_len() override { return 64; }
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
                                         const VExprContextSPtrs& output_exprs) {
        writer->_file_format_transformer =
                std::make_unique<FakeFileFormatTransformer>(output_exprs);
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

} // namespace doris
