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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>

#include "exec/sink/writer/iceberg/viceberg_partition_writer.h"
#include "exec/sink/writer/iceberg/viceberg_sort_writer.h"
#include "format/transformer/vparquet_writer.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

class CleanupFileSystem final : public io::FileSystem {
public:
    CleanupFileSystem() : FileSystem("cleanup", io::FileSystemType::LOCAL) {}
    MOCK_METHOD(Status, create_file_impl,
                (const io::Path&, io::FileWriterPtr*, const io::FileWriterOptions*), (override));
    MOCK_METHOD(Status, open_file_impl,
                (const io::Path&, io::FileReaderSPtr*, const io::FileReaderOptions*), (override));
    MOCK_METHOD(Status, create_directory_impl, (const io::Path&, bool), (override));
    MOCK_METHOD(Status, delete_file_impl, (const io::Path&), (override));
    MOCK_METHOD(Status, batch_delete_impl, (const std::vector<io::Path>&), (override));
    MOCK_METHOD(Status, delete_directory_impl, (const io::Path&), (override));
    MOCK_METHOD(Status, exists_impl, (const io::Path&, bool*), (const, override));
    MOCK_METHOD(Status, file_size_impl, (const io::Path&, int64_t*), (const, override));
    MOCK_METHOD(Status, list_impl, (const io::Path&, bool, std::vector<io::FileInfo>*, bool*),
                (override));
    MOCK_METHOD(Status, rename_impl, (const io::Path&, const io::Path&), (override));
    MOCK_METHOD(Status, absolute_path, (const io::Path&, io::Path&), (const, override));
};

namespace {

class PublishingFileWriter final : public io::FileWriter {
public:
    PublishingFileWriter(std::vector<std::string>& events, bool& object_exists, State state,
                         bool fail_close)
            : _events(events),
              _object_exists(object_exists),
              _state(state),
              _fail_close(fail_close) {}
    ~PublishingFileWriter() override {
        // Model pending S3 work that must finish in destruction even after a failed close.
        if (_state != State::CLOSED) {
            _object_exists = true;
        }
        _events.emplace_back("destroy");
    }
    Status close(bool non_block = false) override {
        EXPECT_FALSE(non_block);
        EXPECT_TRUE(_state != State::CLOSED);
        _events.emplace_back("close");
        _object_exists = true;
        if (_fail_close) {
            return Status::IOError("injected close failure");
        }
        _state = State::CLOSED;
        return Status::OK();
    }
    Status appendv(const Slice*, size_t) override { return Status::OK(); }
    const io::Path& path() const override { return _path; }
    size_t bytes_appended() const override { return 0; }
    State state() const override { return _state; }

private:
    std::vector<std::string>& _events;
    bool& _object_exists;
    State _state;
    bool _fail_close;
    io::Path _path = "failed.parquet";
};

class StreamFileFormatTransformer final : public VFileFormatTransformer {
public:
    StreamFileFormatTransformer(const VExprContextSPtrs& output_exprs, io::FileWriter* writer)
            : VFileFormatTransformer(nullptr, output_exprs, false), _stream(writer) {}
    Status open() override { return Status::NotSupported("Referenced spatial CRS"); }
    Status write(const Block&) override { return Status::OK(); }
    Status close() override { return Status::IOError("transformer close failed"); }
    int64_t written_len() override { return 0; }

private:
    ParquetOutputStream _stream;
};

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
                                         Status close_status = Status::OK(),
                                         bool* destroyed = nullptr) {
        writer->_file_format_transformer = std::make_unique<FakeFileFormatTransformer>(
                output_exprs, std::move(close_status), destroyed);
    }

    static Status build_commit_data(VIcebergPartitionWriter* writer,
                                    TIcebergCommitData* commit_data) {
        return writer->_build_iceberg_commit_data(commit_data);
    }

    static bool collect_column_stats(const VIcebergPartitionWriter& writer) {
        return writer._collect_column_stats;
    }

    static void install_cleanup_resources(VIcebergPartitionWriter* writer,
                                          const VExprContextSPtrs& output_exprs,
                                          std::shared_ptr<io::FileSystem> fs,
                                          std::unique_ptr<io::FileWriter> file_writer,
                                          bool with_stream) {
        writer->_fs = std::move(fs);
        writer->_path = "failed.parquet";
        writer->_file_writer = std::move(file_writer);
        if (with_stream) {
            writer->_file_format_transformer = std::make_unique<StreamFileFormatTransformer>(
                    output_exprs, writer->_file_writer.get());
        }
    }

    static void cleanup_open_error(VIcebergPartitionWriter* writer) {
        writer->_defer_file_cleanup();
    }
};

TEST_F(VIcebergPartitionWriterTest, FailedOpenFinishesUploadsBeforeDeletingObject) {
    for (bool with_stream : {false, true}) {
        for (bool fail_close : {false, true}) {
            VExprContextSPtrs output_exprs;
            iceberg::Schema schema(std::vector<iceberg::NestedField> {});
            std::string schema_json;
            std::map<std::string, std::string> hadoop_conf;
            auto writer = make_writer(make_table_sink(false), output_exprs, schema, &schema_json,
                                      hadoop_conf);
            std::vector<std::string> events;
            bool object_exists = false;
            auto fs = std::make_shared<CleanupFileSystem>();
            EXPECT_CALL(*fs, delete_file_impl(testing::_)).WillOnce([&](const io::Path&) {
                events.emplace_back("delete");
                object_exists = false;
                return Status::OK();
            });
            install_cleanup_resources(
                    writer.get(), output_exprs, fs,
                    std::make_unique<PublishingFileWriter>(
                            events, object_exists, io::FileWriter::State::OPENED, fail_close),
                    with_stream);
            cleanup_open_error(writer.get());
            writer.reset();
            EXPECT_FALSE(object_exists);
            ASSERT_GE(events.size(), 3);
            EXPECT_EQ("close", events.front());
            EXPECT_EQ("destroy", events[events.size() - 2]);
            EXPECT_EQ("delete", events.back());
        }
    }
}

TEST_F(VIcebergPartitionWriterTest, FailedOpenHandlesClosedAndAsyncClosingWriters) {
    for (auto state : {io::FileWriter::State::CLOSED, io::FileWriter::State::ASYNC_CLOSING}) {
        VExprContextSPtrs output_exprs;
        iceberg::Schema schema(std::vector<iceberg::NestedField> {});
        std::string schema_json;
        std::map<std::string, std::string> hadoop_conf;
        auto writer = make_writer(make_table_sink(false), output_exprs, schema, &schema_json,
                                  hadoop_conf);
        std::vector<std::string> events;
        bool object_exists = true;
        auto fs = std::make_shared<CleanupFileSystem>();
        EXPECT_CALL(*fs, delete_file_impl(testing::_)).WillOnce([&](const io::Path&) {
            events.emplace_back("delete");
            return Status::IOError("injected delete failure");
        });
        install_cleanup_resources(
                writer.get(), output_exprs, fs,
                std::make_unique<PublishingFileWriter>(events, object_exists, state, false), false);
        cleanup_open_error(writer.get());
        writer.reset();
        // A provider deletion failure can leave an object, but all publishing owners must
        // already be gone so destruction cannot recreate it after a later cleanup attempt.
        EXPECT_TRUE(object_exists);
        EXPECT_EQ(state == io::FileWriter::State::CLOSED ? 2 : 3, events.size());
        EXPECT_EQ("destroy", events[events.size() - 2]);
        EXPECT_EQ("delete", events.back());
    }
}

TEST_F(VIcebergPartitionWriterTest, FailedOpenFinishesUploadsBeforeOuterCleanup) {
    VExprContextSPtrs output_exprs;
    iceberg::Schema schema(std::vector<iceberg::NestedField> {});
    std::string schema_json;
    std::map<std::string, std::string> hadoop_conf;
    auto writer =
            make_writer(make_table_sink(false), output_exprs, schema, &schema_json, hadoop_conf);
    std::vector<std::string> events;
    bool object_exists = false;
    auto fs = std::make_shared<CleanupFileSystem>();
    EXPECT_CALL(*fs, delete_file_impl(testing::_)).Times(0);
    install_cleanup_resources(
            writer.get(), output_exprs, fs,
            std::make_unique<PublishingFileWriter>(events, object_exists,
                                                   io::FileWriter::State::ASYNC_CLOSING, true),
            true);
    int cleanup_calls = 0;
    writer->_closed_file_callback = [&](std::shared_ptr<io::FileSystem> cleanup_fs,
                                        const std::string& path) {
        EXPECT_EQ(fs, cleanup_fs);
        EXPECT_EQ("failed.parquet", path);
        EXPECT_TRUE(object_exists);
        ASSERT_FALSE(events.empty());
        EXPECT_EQ("destroy", events.back());
        events.emplace_back("cleanup");
        ++cleanup_calls;
    };

    cleanup_open_error(writer.get());
    EXPECT_TRUE(writer->close(Status::Cancelled("injected open failure")).ok());
    writer.reset();
    EXPECT_EQ(1, cleanup_calls);
    EXPECT_EQ("cleanup", events.back());
}

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
