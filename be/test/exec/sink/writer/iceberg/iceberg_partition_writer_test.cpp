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

#include <arrow/memory_pool.h>
#include <gtest/gtest.h>

#include <optional>

#include "common/sync_point.h"
#include "core/data_type/data_type_number.h"
#include "exec/sink/writer/iceberg/viceberg_partition_writer.h"
#include "exec/sink/writer/iceberg/viceberg_sort_writer.h"
#include "format/table/iceberg/schema_parser.h"
#include "format/transformer/viceberg_parquet_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/defer_op.h"
#include "util/timezone_utils.h"
#include "util/uid_util.h"

namespace doris {

namespace {

class FakeFileFormatTransformer final : public VFileFormatTransformer {
public:
    explicit FakeFileFormatTransformer(const VExprContextSPtrs& output_exprs,
                                       Status close_status = Status::OK(),
                                       bool* destroyed = nullptr)
            : VFileFormatTransformer(nullptr, output_exprs, false),
              _close_status(std::move(close_status)),
              _destroyed(destroyed) {}

    ~FakeFileFormatTransformer() override {
        if (_destroyed != nullptr) {
            *_destroyed = true;
        }
    }

    Status open() override { return Status::OK(); }
    Status write(const Block&) override { return Status::OK(); }
    Status close() override { return _close_status; }
    int64_t written_len() override { return 64; }

private:
    Status _close_status;
    bool* _destroyed;
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
            const std::map<std::string, std::string>& hadoop_conf,
            TFileFormatType::type format = TFileFormatType::FORMAT_ORC) {
        IPartitionWriterBase::WriteInfo write_info;
        write_info.file_type = TFileType::FILE_LOCAL;
        return std::make_unique<VIcebergPartitionWriter>(
                sink, std::vector<std::string> {}, output_exprs, schema, schema_json,
                std::vector<std::string> {}, std::move(write_info), "data", 0, format,
                TFileCompressType::ZLIB, hadoop_conf);
    }

    static void install_fake_transformer(VIcebergPartitionWriter* writer,
                                         const VExprContextSPtrs& output_exprs,
                                         Status close_status = Status::OK(),
                                         bool* destroyed = nullptr) {
        writer->_file_format_transformer = std::make_unique<FakeFileFormatTransformer>(
                output_exprs, std::move(close_status), destroyed);
    }

    static void install_parquet_transformer(VIcebergPartitionWriter* writer,
                                            std::unique_ptr<VIcebergParquetWriter> transformer) {
        writer->_file_format_transformer = std::move(transformer);
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

TEST_F(VIcebergPartitionWriterTest, ParquetKeepsSplitOffsetsWhenColumnMetricsAreDisabled) {
    TimezoneUtils::load_timezones_to_cache();
    auto* env = ExecEnv::GetInstance();
    auto* previous_pool = env->_arrow_memory_pool;
    env->_arrow_memory_pool = arrow::default_memory_pool();
    const auto path = "./iceberg_split_offsets_" + UniqueId::gen_uid().to_string() + ".parquet";
    auto fs = io::global_local_filesystem();
    Defer cleanup([&] {
        env->_arrow_memory_pool = previous_pool;
        static_cast<void>(fs->delete_file(path));
    });
    auto type = std::make_shared<DataTypeInt32>();
    auto output_exprs = MockSlotRef::create_mock_contexts(DataTypes {type});
    const std::string json =
            R"({"type":"struct","fields":[{"id":1,"name":"value","required":true,"type":"int"}]})";
    auto schema = iceberg::SchemaParser::from_json(json);
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(fs->create_file(path, &file_writer).ok());
    RuntimeState state;
    state.set_timezone("UTC");
    ParquetFileOptions options {.compression_type = TParquetCompressionType::SNAPPY,
                                .parquet_version = TParquetVersion::PARQUET_1_0,
                                .parquet_disable_dictionary = false,
                                .enable_int96_timestamps = false};
    auto transformer = std::make_unique<VIcebergParquetWriter>(
            &state, file_writer.get(), output_exprs, std::vector<std::string> {"value"}, false,
            options, &json, *schema);
    ASSERT_TRUE(transformer->open().ok());
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_INT>(7));
    Block block;
    block.insert({std::move(column), type, "value"});
    ASSERT_TRUE(transformer->write(block).ok());
    ASSERT_TRUE(transformer->close().ok());
    auto writer = make_writer(make_table_sink(false), output_exprs, *schema, &json, {},
                              TFileFormatType::FORMAT_PARQUET);
    install_parquet_transformer(writer.get(), std::move(transformer));
    TIcebergCommitData commit;
    ASSERT_TRUE(build_commit_data(writer.get(), &commit).ok());
    EXPECT_FALSE(commit.__isset.column_stats);
    ASSERT_TRUE(commit.__isset.split_offsets);
    ASSERT_EQ(1, commit.split_offsets.size());
    EXPECT_EQ(4, commit.split_offsets[0]);
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

TEST_F(VIcebergPartitionWriterTest, UnsupportedParquetCodecDoesNotCreateFile) {
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    IPartitionWriterBase::WriteInfo write_info;
    write_info.file_type = TFileType::FILE_LOCAL;
    write_info.write_path = ".";
    auto writer = std::make_unique<VIcebergPartitionWriter>(
            make_table_sink(std::nullopt), std::vector<std::string> {}, output_exprs, schema,
            &schema_json, std::vector<std::string> {}, std::move(write_info),
            "unsupported_parquet_codec_" + UniqueId::gen_uid().to_string(), 0,
            TFileFormatType::FORMAT_PARQUET, TFileCompressType::ZLIB, hadoop_conf);

    auto* sync_point = SyncPoint::get_instance();
    const bool sync_point_was_enabled = sync_point->get_enable();
    int create_file_calls = 0;
    SyncPoint::CallbackGuard callback_guard;
    sync_point->set_call_back(
            "LocalFileSystem::create_file_impl", [&](auto&&) { ++create_file_calls; },
            &callback_guard);
    sync_point->enable_processing();
    Defer restore_sync_point([&] {
        if (!sync_point_was_enabled) {
            sync_point->disable_processing();
        }
    });

    MockRuntimeState state;
    Status status = writer->open(&state, nullptr, nullptr);

    EXPECT_EQ(0, create_file_calls);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("Unsupported compress type"));
    EXPECT_EQ(nullptr, writer->_file_writer);
    ASSERT_NE(nullptr, writer->_fs);
    bool exists = true;
    ASSERT_TRUE(writer->_fs->exists(writer->_path, &exists).ok());
    EXPECT_FALSE(exists);
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

TEST_F(VIcebergPartitionWriterTest, FailedCloseDefersFileCleanupToOuterCallback) {
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto writer = std::shared_ptr<VIcebergPartitionWriter>(
            make_writer(make_table_sink(false), output_exprs, schema, &schema_json, hadoop_conf));
    bool transformer_destroyed = false;
    install_fake_transformer(writer.get(), output_exprs, Status::OK(), &transformer_destroyed);
    writer->_fs = io::global_local_filesystem();
    writer->_path = "/tmp/doris-iceberg-failed-close.orc";
    int cleanup_calls = 0;
    writer->_closed_file_callback = [&](std::shared_ptr<io::FileSystem> fs,
                                        const std::string& path) {
        EXPECT_EQ(writer->_fs, fs);
        EXPECT_EQ(writer->_path, path);
        EXPECT_TRUE(transformer_destroyed);
        ++cleanup_calls;
    };

    EXPECT_TRUE(writer->close(Status::Cancelled("injected write failure")).ok());
    EXPECT_EQ(1, cleanup_calls);
    EXPECT_EQ(0, writer->written_len());

    EXPECT_TRUE(writer->close(Status::Cancelled("injected write failure")).ok());
    EXPECT_EQ(1, cleanup_calls);
}

TEST_F(VIcebergPartitionWriterTest, RepeatedClosePreservesTerminalError) {
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto writer = std::shared_ptr<VIcebergPartitionWriter>(
            make_writer(make_table_sink(false), output_exprs, schema, &schema_json, hadoop_conf));
    install_fake_transformer(writer.get(), output_exprs, Status::IOError("injected close failure"));
    writer->_fs = io::global_local_filesystem();
    writer->_path = "/tmp/doris-iceberg-close-failure.orc";
    int cleanup_calls = 0;
    writer->_closed_file_callback = [&](std::shared_ptr<io::FileSystem>, const std::string&) {
        ++cleanup_calls;
    };

    Status first_status = writer->close(Status::OK());
    Status second_status = writer->close(Status::OK());

    EXPECT_FALSE(first_status.ok());
    EXPECT_FALSE(second_status.ok());
    EXPECT_NE(first_status.to_string().find("injected close failure"), std::string::npos);
    EXPECT_EQ(first_status, second_status);
    EXPECT_EQ(1, cleanup_calls);
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

} // namespace doris
