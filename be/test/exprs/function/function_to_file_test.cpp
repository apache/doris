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

#include "exprs/function/function_to_file.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "io/fs/file_system.h"
#include "io/fs/s3_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

using testing::_;
using testing::Invoke;
using testing::Return;

class ToFileMockFS : public io::FileSystem {
public:
    ToFileMockFS() : FileSystem("test-resource", io::FileSystemType::S3) {}
    MOCK_METHOD(Status, stat_impl, (const io::Path&, io::FileStat*, io::FileStatContext*),
                (const, override));
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
    Status absolute_path(const io::Path& path, io::Path& result) const override {
        result = path;
        return Status::OK();
    }
};

class ToFileWithMockFS : public FunctionToFile {
public:
    MOCK_METHOD(Status, create_filesystem,
                (const TFileResourceSnapshot&, const std::string&,
                 std::shared_ptr<io::FileSystem>*),
                (const, override));
};

class ToFileWithRealFS : public FunctionToFile {
public:
    using FunctionToFile::create_filesystem;
};

class FunctionToFileTest : public testing::Test {
protected:
    DataTypePtr text = std::make_shared<DataTypeString>();
    DataTypePtr file = std::make_shared<DataTypeFile>();
    std::shared_ptr<ToFileMockFS> filesystem = std::make_shared<ToFileMockFS>();
    ToFileWithMockFS function;

    static Field string(const std::string& value) {
        return Field::create_field<TYPE_STRING>(value);
    }

    ColumnPtr resource_column(size_t rows = 1) {
        return text->create_column_const(rows, string("test-resource"));
    }

    std::unique_ptr<FunctionContext> make_context(RuntimeState* runtime = nullptr,
                                                  ColumnPtr constant_uri = nullptr) {
        auto context = FunctionContext::create_context(runtime, make_nullable(file),
                                                       {text, make_nullable(text)});
        context->set_constant_cols(
                {std::make_shared<ColumnPtrWrapper>(resource_column()),
                 constant_uri ? std::make_shared<ColumnPtrWrapper>(constant_uri) : nullptr});
        TFileResourceSnapshot snapshot;
        snapshot.resource_name = "test-resource";
        snapshot.file_type = TFileType::FILE_S3;
        snapshot.properties = {{"AWS_BUCKET", "bucket"}, {"AWS_ENDPOINT", "endpoint"}};
        context->set_file_resource(snapshot);
        return context;
    }

    ColumnWithTypeAndName uris(std::initializer_list<Field> values) {
        auto type = make_nullable(text);
        auto column = type->create_column();
        for (const auto& value : values) column->insert(value);
        return {std::move(column), type, "uri"};
    }

    Status execute(FunctionContext* context, const ColumnWithTypeAndName& input,
                   ColumnPtr& output) {
        const auto type = input.type->is_nullable() ? make_nullable(file) : file;
        Block block {{resource_column(input.column->size()), text, "resource"},
                     input,
                     {output, type, "result"}};
        auto status = function.execute(context, block, {0, 1}, 2, input.column->size());
        output = block.get_by_position(2).column;
        return status;
    }

    void expect_client_creation() {
        EXPECT_CALL(function, create_filesystem(_, _, _))
                .WillOnce(Invoke([this](const TFileResourceSnapshot& snapshot, const std::string&,
                                        std::shared_ptr<io::FileSystem>* result) {
                    EXPECT_EQ(snapshot.resource_name, "test-resource");
                    EXPECT_EQ(snapshot.properties.at("AWS_BUCKET"), "bucket");
                    *result = filesystem;
                    return Status::OK();
                }));
    }
};

TEST_F(FunctionToFileTest, AllNullBatchesAndQueriesNeverCreateClient) {
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    EXPECT_CALL(*filesystem, stat_impl(_, _, _)).Times(0);
    for (auto type : {TFileType::FILE_S3, TFileType::FILE_HDFS}) {
        auto context = make_context();
        auto snapshot = *context->file_resource();
        snapshot.file_type = type;
        snapshot.properties["fs.defaultFS"] = "hdfs://namenode";
        context->set_file_resource(snapshot);
        ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        auto clone = context->clone();
        ASSERT_TRUE(function.open(clone.get(), FunctionContext::THREAD_LOCAL).ok());
        const auto null_input = uris({Field(), Field()});
        ASSERT_FALSE(is_column_const(*null_input.column));
        for (auto* active : {context.get(), clone.get()}) {
            ColumnPtr output;
            ASSERT_TRUE(execute(active, null_input, output).ok());
            ASSERT_EQ(output->size(), 2);
            EXPECT_TRUE(output->is_null_at(0));
            EXPECT_TRUE(output->is_null_at(1));
            ASSERT_TRUE(execute(active, uris({}), output).ok());
            EXPECT_EQ(output->size(), 0);
        }
        ASSERT_TRUE(function.close(clone.get(), FunctionContext::THREAD_LOCAL).ok());
        ASSERT_TRUE(function.close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }

    auto null_uri = make_nullable(text)->create_column_const(4, Field());
    auto null_context = make_context(nullptr, null_uri);
    ASSERT_TRUE(function.open(null_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr output;
    ASSERT_TRUE(execute(null_context.get(), {null_uri, make_nullable(text), "uri"}, output).ok());
    ASSERT_EQ(output->size(), 4);
    for (size_t row = 0; row < 4; ++row) EXPECT_TRUE(output->is_null_at(row));
}

TEST_F(FunctionToFileTest, CreatesClientOnFirstNonNullUsingSnapshotCapturedAtOpen) {
    auto context = make_context();
    auto snapshot = *context->file_resource();
    snapshot.file_type = TFileType::FILE_HDFS;
    snapshot.properties = {{"fs.defaultFS", "hdfs://original"}};
    context->set_file_resource(snapshot);
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    EXPECT_CALL(*filesystem, stat_impl(_, _, _)).Times(0);
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto clone = context->clone();
    ASSERT_TRUE(function.open(clone.get(), FunctionContext::THREAD_LOCAL).ok());
    ColumnPtr output;
    ASSERT_TRUE(execute(context.get(), uris({Field(), Field()}), output).ok());
    ASSERT_TRUE(execute(clone.get(), uris({Field()}), output).ok());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&function));
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(filesystem.get()));

    // Lazy creation must retain the snapshot pinned by fragment-local open.
    snapshot.properties["fs.defaultFS"] = "hdfs://changed";
    context->set_file_resource(snapshot);
    clone->set_file_resource(snapshot);
    EXPECT_CALL(function, create_filesystem(_, _, _))
            .WillOnce(Invoke([&](const TFileResourceSnapshot& resource, const std::string&,
                                 std::shared_ptr<io::FileSystem>* result) {
                EXPECT_EQ(resource.file_type, TFileType::FILE_HDFS);
                EXPECT_EQ(resource.properties.at("fs.defaultFS"), "hdfs://original");
                *result = filesystem;
                return Status::OK();
            }));
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .Times(2)
            .WillRepeatedly(
                    Invoke([](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                        *metadata = io::FileStat {.size = 0};
                        return Status::OK();
                    }));
    ASSERT_TRUE(
            execute(clone.get(), uris({Field(), string("hdfs://original/a.txt")}), output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_FALSE(output->is_null_at(1));
    ASSERT_TRUE(
            execute(context.get(), uris({string("hdfs://original/b.txt"), Field()}), output).ok());
    EXPECT_FALSE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
    ASSERT_TRUE(execute(context.get(), uris({Field(), Field()}), output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
}

TEST_F(FunctionToFileTest, LazyClientCreationFailureDoesNotStatOrPublishOutput) {
    auto context = make_context();
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&function));
    EXPECT_CALL(function, create_filesystem(_, _, _))
            .WillOnce(Return(Status::IOError("client connection failed")));
    EXPECT_CALL(*filesystem, stat_impl(_, _, _)).Times(0);
    ColumnPtr output = make_nullable(file)->create_column_const(2, Field());
    const auto original = output;
    EXPECT_FALSE(execute(context.get(), uris({Field(), string("s3://bucket/a.txt")}), output).ok());
    EXPECT_EQ(output.get(), original.get());
    ASSERT_TRUE(execute(context.get(), uris({Field(), Field()}), output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
}

TEST_F(FunctionToFileTest, CancellationBeforeLazyCreationDoesNotCreateClient) {
    RuntimeState runtime;
    auto context = make_context(&runtime);
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    EXPECT_CALL(*filesystem, stat_impl(_, _, _)).Times(0);
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    runtime.cancel(Status::Cancelled("cancel before client creation"));
    ColumnPtr output;
    EXPECT_FALSE(execute(context.get(), uris({string("s3://bucket/a.txt")}), output).ok());
    EXPECT_EQ(output.get(), nullptr);
}

TEST_F(FunctionToFileTest, CancellationDuringLazyCreationDoesNotStat) {
    RuntimeState runtime;
    auto context = make_context(&runtime);
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&function));
    EXPECT_CALL(function, create_filesystem(_, _, _))
            .WillOnce(Invoke([&](const TFileResourceSnapshot&, const std::string&,
                                 std::shared_ptr<io::FileSystem>* result) {
                *result = filesystem;
                runtime.cancel(Status::Cancelled("cancel during client creation"));
                return Status::OK();
            }));
    EXPECT_CALL(*filesystem, stat_impl(_, _, _)).Times(0);
    ColumnPtr output;
    EXPECT_FALSE(execute(context.get(), uris({string("s3://bucket/a.txt")}), output).ok());
    EXPECT_EQ(output.get(), nullptr);
}

TEST_F(FunctionToFileTest, RealS3FactoryUsesUriWithoutAwsBucketProperty) {
    ToFileWithRealFS real_function;
    TFileResourceSnapshot snapshot;
    snapshot.resource_name = "test-resource";
    snapshot.file_type = TFileType::FILE_S3;
    snapshot.properties = {{"AWS_ENDPOINT", "http://127.0.0.1:1"},
                           {"AWS_REGION", "us-east-1"},
                           {"AWS_ACCESS_KEY", "test-ak"},
                           {"AWS_SECRET_KEY", "test-sk"},
                           {"use_path_style", "true"}};
    std::shared_ptr<io::FileSystem> result;
    ASSERT_TRUE(real_function.create_filesystem(snapshot, "s3://uri-bucket/a.txt", &result).ok());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(assert_cast<io::S3FileSystem&>(*result).bucket(), "uri-bucket");
    snapshot.properties["AWS_BUCKET"] = "different-resource-bucket";
    ASSERT_TRUE(real_function.create_filesystem(snapshot, "s3://other-bucket/a.txt", &result).ok());
    EXPECT_EQ(assert_cast<io::S3FileSystem&>(*result).bucket(), "other-bucket");
}

// A filesystem is bound to its S3 bucket. Reusing it for another bucket must
// not send metadata requests to the first bucket.
TEST_F(FunctionToFileTest, SwitchesFilesystemWhenUriBucketChanges) {
    auto context = make_context();
    std::vector<std::string> created_uris;
    EXPECT_CALL(function, create_filesystem(_, _, _))
            .Times(3)
            .WillRepeatedly(Invoke([&](const TFileResourceSnapshot&, const std::string& uri,
                                       std::shared_ptr<io::FileSystem>* result) {
                created_uris.push_back(uri);
                *result = filesystem;
                return Status::OK();
            }));
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .Times(4)
            .WillRepeatedly(
                    Invoke([](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                        *metadata = io::FileStat {.size = 1};
                        return Status::OK();
                    }));
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr output;
    ASSERT_TRUE(execute(context.get(),
                        uris({string("s3://bucket/a.txt"), string("s3://bucket/b.txt"),
                              string("s3://other/a.txt"), string("s3://bucket/c.txt")}),
                        output)
                        .ok());
    EXPECT_EQ(created_uris, (std::vector<std::string> {"s3://bucket/a.txt", "s3://other/a.txt",
                                                       "s3://bucket/c.txt"}));
}

TEST_F(FunctionToFileTest, ReusesClientAcrossBlocksAndClones) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto clone = context->clone();
    EXPECT_EQ(clone->file_resource().get(), context->file_resource().get());
    ASSERT_TRUE(function.open(clone.get(), FunctionContext::THREAD_LOCAL).ok());
    io::FileStatContext* first_context = nullptr;
    size_t calls = 0;
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .Times(3)
            .WillRepeatedly(Invoke([&](const io::Path&, io::FileStat* metadata,
                                       io::FileStatContext* stat_context) {
                if (calls++ == 0) {
                    first_context = stat_context;
                    EXPECT_FALSE(stat_context->is_cancelled);
                } else {
                    EXPECT_EQ(stat_context, first_context);
                }
                *metadata = io::FileStat {.size = 1};
                return Status::OK();
            }));
    ColumnPtr output;
    ASSERT_TRUE(execute(context.get(), uris({string("s3://bucket/a.txt"), Field()}), output).ok());
    EXPECT_TRUE(output->is_null_at(1));
    ASSERT_TRUE(execute(clone.get(), uris({string("s3://bucket/b.txt")}), output).ok());
    ASSERT_TRUE(function.close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(execute(clone.get(), uris({string("s3://bucket/c.txt")}), output).ok());
}

TEST_F(FunctionToFileTest, PreservesRawUriServerMetadataAndZeroSize) {
    const std::string uri = "s3://bucket/a%2Fb.PNG?versionId=Ab%2FC";
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .WillOnce(
                    Invoke([&](const io::Path& path, io::FileStat* metadata, io::FileStatContext*) {
                        EXPECT_EQ(path.native(), uri);
                        *metadata = io::FileStat {.size = 0,
                                                  .content_type = "Image/PNG; name=Original",
                                                  .checksum = "ETAG:Opaque-2"};
                        return Status::OK();
                    }));
    ColumnPtr output;
    ASSERT_TRUE(execute(context.get(), uris({string(uri)}), output).ok());
    ASSERT_FALSE(output->is_null_at(0));
    const auto value = (*output)[0];
    ASSERT_EQ(value.get_type(), TYPE_FILE);
    const auto& fields = value.get<TYPE_FILE>();
    ASSERT_EQ(fields.size(), 6);
    EXPECT_EQ(fields[0].get<TYPE_STRING>(), uri);
    EXPECT_TRUE(fields[1].is_null());
    EXPECT_EQ(fields[2].get<TYPE_BIGINT>(), 0);
    EXPECT_EQ(fields[3].get<TYPE_STRING>(), "Image/PNG; name=Original");
    EXPECT_EQ(fields[4].get<TYPE_STRING>(), "ETAG:Opaque-2");
    EXPECT_TRUE(fields[5].is_null());
}

TEST_F(FunctionToFileTest, InfersOrdinaryPathExtensionsWithoutQueryAndDefaultsUnknownNames) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    size_t calls = 0;
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .Times(3)
            .WillRepeatedly(
                    Invoke([&](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                        *metadata = io::FileStat {.size = 5};
                        if (calls++ == 1) metadata->content_type = "not a MIME type";
                        return Status::OK();
                    }));
    ColumnPtr output;
    ASSERT_TRUE(execute(context.get(),
                        uris({string("s3://bucket/a.PNG?name=wrong.txt"),
                              string("s3://bucket/a.csv"), string("s3://bucket/unknown")}),
                        output)
                        .ok());
    EXPECT_EQ((*output)[0].get<TYPE_FILE>()[3].get<TYPE_STRING>(), "image/png");
    EXPECT_EQ((*output)[1].get<TYPE_FILE>()[3].get<TYPE_STRING>(), "text/csv");
    EXPECT_EQ((*output)[2].get<TYPE_FILE>()[3].get<TYPE_STRING>(), "application/octet-stream");
    EXPECT_TRUE((*output)[0].get<TYPE_FILE>()[4].is_null());
}

TEST_F(FunctionToFileTest, MetadataErrorsPropagateWithoutPublishingPartialOutput) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    {
        testing::InSequence sequence;
        EXPECT_CALL(*filesystem, stat_impl(_, _, _))
                .WillOnce(Invoke([](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                    *metadata = io::FileStat {.size = 1};
                    return Status::OK();
                }));
        EXPECT_CALL(*filesystem, stat_impl(_, _, _))
                .WillOnce(Return(Status::IOError("stat failed")));
    }
    ColumnPtr output = make_nullable(file)->create_column_const(2, Field());
    const auto original = output;
    EXPECT_FALSE(execute(context.get(),
                         uris({string("s3://bucket/a.txt"), string("s3://bucket/b.txt")}), output)
                         .ok());
    EXPECT_EQ(output.get(), original.get());
}

TEST_F(FunctionToFileTest, InfersExtensionFromRawLastPathSegmentWithoutQuery) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    const std::vector<std::pair<std::string, std::string>> cases {
            {"s3://bucket/a%2E%50%4eg?filename=a.txt", "application/octet-stream"},
            {"s3://bucket/a%252Epng", "application/octet-stream"},
            {"s3://bucket/directory.txt/filename", "application/octet-stream"},
            {"s3://bucket/directory%2Etxt/filename", "application/octet-stream"},
            {"s3://bucket/a%2Fb%2Ecsv?filename=a.pdf", "application/octet-stream"},
            {"s3://bucket/a%3Fpart%2Etxt", "application/octet-stream"},
            {"s3://bucket/a.txt%2F", "application/octet-stream"},
            {"s3://bucket/a%2Epng%00", "application/octet-stream"},
            {"s3://bucket/a.png?filename=a.txt", "image/png"},
            {"s3://bucket/a%2Fb.csv?filename=a.pdf", "text/csv"}};
    size_t calls = 0;
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .Times(static_cast<int>(cases.size()))
            .WillRepeatedly(
                    Invoke([&](const io::Path& path, io::FileStat* metadata, io::FileStatContext*) {
                        EXPECT_EQ(path.native(), cases[calls++].first);
                        *metadata = io::FileStat {.size = 1};
                        return Status::OK();
                    }));
    for (const auto& [uri, content_type] : cases) {
        ColumnPtr output;
        ASSERT_TRUE(execute(context.get(), uris({string(uri)}), output).ok());
        EXPECT_EQ((*output)[0].get<TYPE_FILE>()[0].get<TYPE_STRING>(), uri);
        EXPECT_EQ((*output)[0].get<TYPE_FILE>()[3].get<TYPE_STRING>(), content_type);
    }
}

TEST_F(FunctionToFileTest, ConstantUriStillExecutesEachRow) {
    auto uri = text->create_column_const(2, string("s3://bucket/a.txt"));
    auto context = make_context(nullptr, uri);
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    int64_t size = 0;
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .Times(2)
            .WillRepeatedly(
                    Invoke([&](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                        *metadata = io::FileStat {.size = ++size};
                        return Status::OK();
                    }));
    ColumnPtr output;
    ASSERT_TRUE(execute(context.get(), {uri, text, "uri"}, output).ok());
    EXPECT_FALSE(is_column_const(*output));
    EXPECT_EQ((*output)[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 1);
    EXPECT_EQ((*output)[1].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 2);
}

TEST_F(FunctionToFileTest, PrepareCopiesDescriptorSnapshotIntoFunctionContext) {
    TFileResourceSnapshot snapshot;
    snapshot.resource_name = "test-resource";
    snapshot.file_type = TFileType::FILE_S3;
    snapshot.properties = {{"AWS_BUCKET", "bucket"}};
    TFunction definition;
    definition.name.function_name = "to_file";
    definition.binary_type = TFunctionBinaryType::BUILTIN;
    definition.__set_file_resource(snapshot);
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(file->to_thrift());
    node.__set_is_nullable(true);
    node.__set_fn(definition);
    auto expression = VectorizedFnCall::create_shared(node);
    expression->add_child(VLiteral::create_shared(text, string("test-resource")));
    expression->add_child(VLiteral::create_shared(make_nullable(text), Field()));
    auto context = VExprContext::create_shared(expression);
    RuntimeState runtime;
    ASSERT_TRUE(expression->prepare(&runtime, RowDescriptor(), context.get()).ok());
    const auto& copied = context->fn_context(expression->_fn_context_index)->file_resource();
    ASSERT_NE(copied, nullptr);
    EXPECT_EQ(copied->resource_name, snapshot.resource_name);
    EXPECT_EQ(copied->file_type, snapshot.file_type);
    EXPECT_EQ(copied->properties, snapshot.properties);
    expression->_fn.file_resource.properties["AWS_BUCKET"] = "changed";
    EXPECT_EQ(copied->properties.at("AWS_BUCKET"), "bucket");
}

TEST_F(FunctionToFileTest, ValidatesFileOnlyAfterSuccessfulStat) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr output;
    for (const std::string uri : {"s3://bucket/a%ZZ.txt", "s3://bucket/a%"}) {
        EXPECT_CALL(*filesystem, stat_impl(_, _, _))
                .WillOnce(Invoke(
                        [&](const io::Path& path, io::FileStat* metadata, io::FileStatContext*) {
                            EXPECT_EQ(path.native(), uri);
                            *metadata = io::FileStat {.size = 1, .content_type = "text/plain"};
                            return Status::OK();
                        }));
        EXPECT_FALSE(execute(context.get(), uris({string(uri)}), output).ok());
    }
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .WillOnce(Invoke([](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                *metadata = io::FileStat {.size = -1};
                return Status::OK();
            }));
    EXPECT_FALSE(execute(context.get(), uris({string("s3://bucket/a.txt")}), output).ok());
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .WillOnce(Invoke([](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                *metadata = io::FileStat {.size = 1,
                                          .checksum = "MD5:ABCDEF0123456789ABCDEF0123456789"};
                return Status::OK();
            }));
    EXPECT_FALSE(execute(context.get(), uris({string("s3://bucket/a.txt")}), output).ok());
}

TEST_F(FunctionToFileTest, CancellationStopsFurtherMetadataCalls) {
    RuntimeState runtime;
    auto context = make_context(&runtime);
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .WillOnce(Invoke([&](const io::Path&, io::FileStat* metadata,
                                 io::FileStatContext* stat_context) {
                EXPECT_TRUE(stat_context->is_cancelled);
                EXPECT_FALSE(stat_context->is_cancelled());
                *metadata = io::FileStat {.size = 1};
                runtime.cancel(Status::Cancelled("cancel during stat"));
                EXPECT_TRUE(stat_context->is_cancelled());
                return Status::OK();
            }));
    ColumnPtr output;
    EXPECT_FALSE(execute(context.get(),
                         uris({string("s3://bucket/a.txt"), string("s3://bucket/b.txt")}), output)
                         .ok());
    EXPECT_EQ(output.get(), nullptr);
    EXPECT_FALSE(execute(context.get(), uris({string("s3://bucket/c.txt")}), output).ok());
}

TEST_F(FunctionToFileTest, CancellationCallbackSurvivesOriginalContextCloseAndDestruction) {
    RuntimeState runtime;
    auto context = make_context(&runtime);
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto clone = context->clone();
    ASSERT_TRUE(function.open(clone.get(), FunctionContext::THREAD_LOCAL).ok());
    ASSERT_TRUE(function.close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    context.reset();
    const auto cancellation = Status::Cancelled("cancel cloned metadata request");
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .WillOnce(
                    Invoke([&](const io::Path&, io::FileStat*, io::FileStatContext* stat_context) {
                        EXPECT_TRUE(stat_context->is_cancelled);
                        EXPECT_FALSE(stat_context->is_cancelled());
                        runtime.cancel(cancellation);
                        EXPECT_TRUE(stat_context->is_cancelled());
                        return Status::IOError("metadata request aborted");
                    }));
    ColumnPtr output;
    const auto status = execute(
            clone.get(), uris({string("s3://bucket/a.txt"), string("s3://bucket/b.txt")}), output);
    EXPECT_EQ(status.to_string(), cancellation.to_string());
    EXPECT_EQ(output.get(), nullptr);
    ASSERT_TRUE(function.close(clone.get(), FunctionContext::THREAD_LOCAL).ok());
}

TEST_F(FunctionToFileTest, QueryCancellationReasonTakesPrecedenceOverAbortedStatError) {
    RuntimeState runtime;
    auto context = make_context(&runtime);
    expect_client_creation();
    ASSERT_TRUE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    const auto cancellation = Status::Cancelled("query cancelled during metadata request");
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .WillOnce(Invoke([&](const io::Path&, io::FileStat*, io::FileStatContext*) {
                runtime.cancel(cancellation);
                return Status::IOError("metadata request aborted");
            }));
    ColumnPtr output;
    const auto status =
            execute(context.get(), uris({string("s3://bucket/a.txt"), string("s3://bucket/b.txt")}),
                    output);
    EXPECT_EQ(status.to_string(), cancellation.to_string());
    EXPECT_EQ(output.get(), nullptr);
}

TEST_F(FunctionToFileTest, SnapshotIsImmutableAndMissingOrMismatchedSnapshotFailsOpen) {
    auto context = make_context();
    auto snapshot = *context->file_resource();
    snapshot.properties["AWS_BUCKET"] = "original";
    context->set_file_resource(snapshot);
    snapshot.properties["AWS_BUCKET"] = "changed";
    EXPECT_EQ(context->file_resource()->properties.at("AWS_BUCKET"), "original");
    auto clone = context->clone();
    EXPECT_EQ(clone->file_resource().get(), context->file_resource().get());
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    snapshot.resource_name = "other-resource";
    context->set_file_resource(snapshot);
    EXPECT_FALSE(function.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto missing = FunctionContext::create_context(nullptr, file, {text, text});
    EXPECT_FALSE(function.open(missing.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(FunctionToFileTest, RegistersBlockableNonConstantFunctionWithNullableUriResult) {
    ColumnsWithTypeAndName arguments {{resource_column(), text, "resource"},
                                      {nullptr, make_nullable(text), "uri"}};
    auto registered = SimpleFunctionFactory::instance().get_function("to_file", arguments,
                                                                     make_nullable(file));
    ASSERT_NE(registered, nullptr);
    EXPECT_TRUE(registered->is_blockable());
    EXPECT_FALSE(registered->is_use_default_implementation_for_constants());
    EXPECT_TRUE(registered->get_return_type()->is_nullable());
    arguments[1].type = text;
    auto nonnullable = SimpleFunctionFactory::instance().get_function("to_file", arguments, file);
    ASSERT_NE(nonnullable, nullptr);
    EXPECT_FALSE(nonnullable->get_return_type()->is_nullable());
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(file->to_thrift());
    node.__set_is_nullable(false);
    TFunction definition;
    definition.name.function_name = "to_file";
    definition.binary_type = TFunctionBinaryType::BUILTIN;
    node.__set_fn(definition);
    auto expression = VectorizedFnCall::create_shared(node);
    expression->_function = nonnullable;
    EXPECT_FALSE(expression->is_constant());
    EXPECT_FALSE(expression->is_deterministic());
}

TEST_F(FunctionToFileTest, RejectsDynamicResourceAndNonStringUriTypes) {
    auto dynamic_resource = text->create_column();
    dynamic_resource->insert(string("test-resource"));
    EXPECT_THROW(function.get_return_type_impl(ColumnsWithTypeAndName {
                         {std::move(dynamic_resource), text, "resource"}, {nullptr, text, "uri"}}),
                 Exception);
    EXPECT_THROW(function.get_return_type_impl(ColumnsWithTypeAndName {
                         {resource_column(), text, "resource"},
                         {nullptr, std::make_shared<DataTypeInt64>(), "uri"}}),
                 Exception);
}

} // namespace doris
