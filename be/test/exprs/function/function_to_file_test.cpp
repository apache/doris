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

#include <future>

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
#include "runtime/thread_context.h"
#include "testutil/mock/mock_query_context.h"

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
                (const TS3Resource&, const std::string&, std::shared_ptr<io::FileSystem>*),
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
    std::shared_ptr<MockQueryContext> query = MockQueryContext::create();
    RuntimeState query_runtime;

    TS3Resource resource_config() const { return query->get_s3_resources()->at("test-resource"); }

    void set_resource_config(const TS3Resource& resource) {
        query->set_s3_resources({{"test-resource", resource}});
    }

    static Field string(const std::string& value) {
        return Field::create_field<TYPE_STRING>(value);
    }

    ColumnPtr resource_column(size_t rows = 1) {
        return text->create_column_const(rows, string("test-resource"));
    }

    std::unique_ptr<FunctionContext> make_context(RuntimeState* runtime = nullptr,
                                                  ColumnPtr constant_uri = nullptr) {
        if (!runtime) runtime = &query_runtime;
        runtime->_query_ctx = query.get();
        auto context = FunctionContext::create_context(runtime, make_nullable(file),
                                                       {text, make_nullable(text)});
        context->set_constant_cols(
                {std::make_shared<ColumnPtrWrapper>(resource_column()),
                 constant_uri ? std::make_shared<ColumnPtrWrapper>(constant_uri) : nullptr});
        TS3Resource resource;

        resource.properties = {{"AWS_BUCKET", "bucket"}, {"AWS_ENDPOINT", "endpoint"}};
        set_resource_config(resource);
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
        Block block {{context->get_constant_col(0)->column_ptr->clone_resized(input.column->size()),
                      text, "resource"},
                     input,
                     {output, type, "result"}};
        auto status = function.execute(context, block, {0, 1}, 2, input.column->size());
        output = block.get_by_position(2).column;
        return status;
    }

    void expect_client_creation() {
        EXPECT_CALL(function, create_filesystem(_, _, _))
                .WillOnce(Invoke([this](const TS3Resource& resource, const std::string&,
                                        std::shared_ptr<io::FileSystem>* result) {
                    EXPECT_EQ(resource.properties.at("AWS_BUCKET"), "bucket");
                    *result = filesystem;
                    return Status::OK();
                }));
    }
};

TEST_F(FunctionToFileTest, AllNullBatchesAndQueriesNeverCreateClient) {
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    EXPECT_CALL(*filesystem, stat_impl(_, _, _)).Times(0);
    auto context = make_context();
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto clone = context->clone();
    ASSERT_TRUE(function.open(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
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
    ASSERT_TRUE(
            function.close(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ASSERT_TRUE(function.close(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    auto null_uri = make_nullable(text)->create_column_const(4, Field());
    auto null_context = make_context(nullptr, null_uri);
    ASSERT_TRUE(
            function.open(null_context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                    .ok());
    ColumnPtr output;
    ASSERT_TRUE(execute(null_context.get(), {null_uri, make_nullable(text), "uri"}, output).ok());
    ASSERT_EQ(output->size(), 4);
    for (size_t row = 0; row < 4; ++row) EXPECT_TRUE(output->is_null_at(row));
}

TEST_F(FunctionToFileTest, CreatesClientOnFirstNonNullUsingConfigurationCapturedAtOpen) {
    auto context = make_context();
    auto resource = resource_config();

    resource.properties = {{"AWS_ENDPOINT", "http://original"}};
    set_resource_config(resource);
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    EXPECT_CALL(*filesystem, stat_impl(_, _, _)).Times(0);
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto clone = context->clone();
    ASSERT_TRUE(function.open(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ColumnPtr output;
    ASSERT_TRUE(execute(context.get(), uris({Field(), Field()}), output).ok());
    ASSERT_TRUE(execute(clone.get(), uris({Field()}), output).ok());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&function));
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(filesystem.get()));

    // Lazy creation must retain the configuration pinned by fragment-local open.
    resource.properties["AWS_ENDPOINT"] = "http://changed";
    set_resource_config(resource);
    EXPECT_CALL(function, create_filesystem(_, _, _))
            .WillOnce(Invoke([&](const TS3Resource& resource, const std::string&,
                                 std::shared_ptr<io::FileSystem>* result) {
                EXPECT_EQ(resource.properties.at("AWS_ENDPOINT"), "http://original");
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
    ASSERT_TRUE(execute(clone.get(), uris({Field(), string("s3://bucket/a.txt")}), output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_FALSE(output->is_null_at(1));
    ASSERT_TRUE(execute(context.get(), uris({string("s3://bucket/b.txt"), Field()}), output).ok());
    EXPECT_FALSE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
    ASSERT_TRUE(execute(context.get(), uris({Field(), Field()}), output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
}

TEST_F(FunctionToFileTest, LazyClientCreationFailureDoesNotStatOrPublishOutput) {
    auto context = make_context();
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
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
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    runtime.cancel(Status::Cancelled("cancel before client creation"));
    ColumnPtr output;
    EXPECT_FALSE(execute(context.get(), uris({string("s3://bucket/a.txt")}), output).ok());
    EXPECT_EQ(output.get(), nullptr);
}

TEST_F(FunctionToFileTest, CancellationDuringLazyCreationDoesNotStat) {
    RuntimeState runtime;
    auto context = make_context(&runtime);
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&function));
    EXPECT_CALL(function, create_filesystem(_, _, _))
            .WillOnce(Invoke([&](const TS3Resource&, const std::string&,
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
    TS3Resource resource;

    resource.properties = {{"AWS_ENDPOINT", "http://127.0.0.1:1"},
                           {"AWS_REGION", "us-east-1"},
                           {"AWS_ACCESS_KEY", "test-ak"},
                           {"AWS_SECRET_KEY", "test-sk"},
                           {"use_path_style", "true"}};
    std::shared_ptr<io::FileSystem> result;
    ASSERT_TRUE(real_function.create_filesystem(resource, "s3://uri-bucket/a.txt", &result).ok());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(assert_cast<io::S3FileSystem&>(*result).bucket(), "uri-bucket");
    for (const auto* key : {"dir/a://b.txt", "dir/a?b.txt", "dir/a#b.txt", "dir/ a.txt "}) {
        ASSERT_TRUE(
                real_function
                        .create_filesystem(resource, std::string("s3://uri-bucket/") + key, &result)
                        .ok());
        EXPECT_EQ(assert_cast<io::S3FileSystem&>(*result).bucket(), "uri-bucket");
    }
    resource.properties["AWS_BUCKET"] = "different-resource-bucket";
    ASSERT_TRUE(real_function.create_filesystem(resource, "s3://other-bucket/a.txt", &result).ok());
    EXPECT_EQ(assert_cast<io::S3FileSystem&>(*result).bucket(), "other-bucket");
}

// A filesystem is bound to its S3 bucket. Reusing it for another bucket must
// not send metadata requests to the first bucket.
TEST_F(FunctionToFileTest, SwitchesFilesystemWhenUriBucketChanges) {
    auto context = make_context();
    std::vector<std::string> created_uris;
    EXPECT_CALL(function, create_filesystem(_, _, _))
            .Times(3)
            .WillRepeatedly(Invoke([&](const TS3Resource&, const std::string& uri,
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
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
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
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto clone = context->clone();
    EXPECT_EQ(clone->get_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL),
              context->get_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    ASSERT_TRUE(function.open(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    io::FileStatContext* first_context = nullptr;
    size_t calls = 0;
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .Times(3)
            .WillRepeatedly(Invoke([&](const io::Path&, io::FileStat* metadata,
                                       io::FileStatContext* stat_context) {
                if (calls++ == 0) {
                    first_context = stat_context;
                    EXPECT_TRUE(stat_context->is_cancelled);
                    EXPECT_FALSE(stat_context->is_cancelled());
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
    ASSERT_TRUE(function.close(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
    ASSERT_TRUE(execute(clone.get(), uris({string("s3://bucket/c.txt")}), output).ok());
}

TEST_F(FunctionToFileTest, ClonesStatConcurrentlyDuringSameBucketReuseAndBucketSwitch) {
    for (const bool switch_bucket : {false, true}) {
        SCOPED_TRACE(switch_bucket);
        auto context = make_context();
        auto other_filesystem = std::make_shared<ToFileMockFS>();
        EXPECT_CALL(function, create_filesystem(_, _, _))
                .Times(switch_bucket ? 2 : 1)
                .WillRepeatedly(Invoke([&](const TS3Resource&, const std::string& uri,
                                           std::shared_ptr<io::FileSystem>* result) {
                    *result = uri.starts_with("s3://other/") ? other_filesystem : filesystem;
                    return Status::OK();
                }));
        ASSERT_TRUE(
                function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
        auto clone = context->clone();
        ASSERT_TRUE(
                function.open(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        std::promise<void> first_entered;
        std::promise<void> second_entered;
        std::promise<void> release_first;
        auto first_ready = first_entered.get_future();
        auto second_ready = second_entered.get_future();
        auto release = release_first.get_future().share();
        EXPECT_CALL(*filesystem, stat_impl(io::Path("s3://bucket/first.txt"), _, _))
                .WillOnce(
                        Invoke([&](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                            first_entered.set_value();
                            release.wait();
                            *metadata = io::FileStat {.size = 1};
                            return Status::OK();
                        }));
        const std::string second_uri =
                switch_bucket ? "s3://other/second.txt" : "s3://bucket/second.txt";
        EXPECT_CALL(*(switch_bucket ? other_filesystem : filesystem),
                    stat_impl(io::Path(second_uri), _, _))
                .WillOnce(
                        Invoke([&](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                            second_entered.set_value();
                            *metadata = io::FileStat {.size = 2};
                            return Status::OK();
                        }));
        ColumnPtr first_output;
        ColumnPtr second_output;
        auto first = std::async(std::launch::async, [&] {
            SCOPED_INIT_THREAD_CONTEXT();
            return execute(context.get(), uris({string("s3://bucket/first.txt")}), first_output);
        });
        const auto first_status = first_ready.wait_for(std::chrono::seconds(5));
        const std::weak_ptr<ToFileMockFS> first_filesystem = filesystem;
        if (switch_bucket && first_status == std::future_status::ready) {
            // Leave ownership with function state and the pending request only.
            filesystem.reset();
        }
        auto second = std::async(std::launch::async, [&] {
            SCOPED_INIT_THREAD_CONTEXT();
            return execute(clone.get(), uris({string(second_uri)}), second_output);
        });
        // The second request must enter stat while the first is still blocked.
        // The deadline only prevents a failed implementation from hanging the test.
        const auto second_status = second_ready.wait_for(std::chrono::seconds(5));
        EXPECT_FALSE(first_filesystem.expired());
        release_first.set_value();
        EXPECT_TRUE(first.get().ok());
        EXPECT_TRUE(second.get().ok());
        EXPECT_EQ(first_status, std::future_status::ready);
        EXPECT_EQ(second_status, std::future_status::ready);
        EXPECT_EQ((*first_output)[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 1);
        EXPECT_EQ((*second_output)[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 2);
        ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&function));
        if (switch_bucket && !filesystem) {
            EXPECT_TRUE(first_filesystem.expired());
        } else {
            ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(filesystem.get()));
        }
    }
}

TEST_F(FunctionToFileTest, PreservesRawUriServerMetadataAndZeroSize) {
    const std::string uri = "s3://bucket/a%2Fb.PNG?versionId=Ab%2FC";
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
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

TEST_F(FunctionToFileTest, InfersRawKeyExtensionsAndDefaultsUnknownNames) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
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
    EXPECT_EQ((*output)[0].get<TYPE_FILE>()[3].get<TYPE_STRING>(), "text/plain");
    EXPECT_EQ((*output)[1].get<TYPE_FILE>()[3].get<TYPE_STRING>(), "text/csv");
    EXPECT_EQ((*output)[2].get<TYPE_FILE>()[3].get<TYPE_STRING>(), "application/octet-stream");
    EXPECT_TRUE((*output)[0].get<TYPE_FILE>()[4].is_null());
}

TEST_F(FunctionToFileTest, MetadataErrorsPropagateWithoutPublishingPartialOutput) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
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

TEST_F(FunctionToFileTest, InfersExtensionFromCompleteRawKey) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    const std::vector<std::pair<std::string, std::string>> cases {
            {"s3://bucket/a%2E%50%4eg?filename=a.txt", "text/plain"},
            {"s3://bucket/a%252Epng", "application/octet-stream"},
            {"s3://bucket/directory.txt/filename", "application/octet-stream"},
            {"s3://bucket/directory%2Etxt/filename", "application/octet-stream"},
            {"s3://bucket/a%2Fb%2Ecsv?filename=a.pdf", "application/pdf"},
            {"s3://bucket/a%3Fpart%2Etxt", "application/octet-stream"},
            {"s3://bucket/a.txt%2F", "application/octet-stream"},
            {"s3://bucket/a%2Epng%00", "application/octet-stream"},
            {"s3://bucket/a.png?filename=a.txt", "text/plain"},
            {"s3://bucket/a%2Fb.csv?filename=a.pdf", "application/pdf"},
            {"s3://bucket/dir/photo?part.jpg", "image/jpeg"},
            {"s3://bucket/dir/photo#part.PNG", "image/png"},
            {"https://host/bucket/dir/photo?part.jpg", "image/jpeg"},
            {"http://host/bucket/dir/photo#part.PNG", "image/png"}};
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
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
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

TEST_F(FunctionToFileTest,
       ExpressionOpensUsingQueryResourcesWithoutFunctionDescriptorConfiguration) {
    auto source = make_context();
    TFunction definition;
    definition.name.function_name = "to_file";
    definition.binary_type = TFunctionBinaryType::BUILTIN;
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(file->to_thrift());
    node.__set_is_nullable(true);
    node.__set_fn(definition);
    auto expression = VectorizedFnCall::create_shared(node);
    expression->add_child(VLiteral::create_shared(text, string("test-resource")));
    expression->add_child(VLiteral::create_shared(make_nullable(text), Field()));
    auto context = VExprContext::create_shared(expression);
    ASSERT_TRUE(context->prepare(source->state(), RowDescriptor()).ok());
    ASSERT_TRUE(context->open(source->state()).ok());
    Block input {{resource_column(), text, "resource"}};
    ColumnPtr result;
    ASSERT_TRUE(context->execute(&input, result).ok());
    ASSERT_EQ(result->size(), 1);
    EXPECT_TRUE(result->is_null_at(0));
    context->close();
}

TEST_F(FunctionToFileTest, PreservesUriAndValidatesMetadataAfterSuccessfulStat) {
    auto context = make_context();
    expect_client_creation();
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ColumnPtr output;
    for (const std::string uri : {"s3://bucket/a%ZZ.txt", "s3://bucket/a%"}) {
        EXPECT_CALL(*filesystem, stat_impl(_, _, _))
                .WillOnce(Invoke(
                        [&](const io::Path& path, io::FileStat* metadata, io::FileStatContext*) {
                            EXPECT_EQ(path.native(), uri);
                            *metadata = io::FileStat {.size = 1, .content_type = "text/plain"};
                            return Status::OK();
                        }));
        ASSERT_TRUE(execute(context.get(), uris({string(uri)}), output).ok());
        EXPECT_EQ((*output)[0].get<TYPE_FILE>()[0].get<TYPE_STRING>(), uri);
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
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
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
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto clone = context->clone();
    ASSERT_TRUE(function.open(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ASSERT_TRUE(function.close(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
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
    ASSERT_TRUE(
            function.close(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
}

TEST_F(FunctionToFileTest, QueryCancellationReasonTakesPrecedenceOverAbortedStatError) {
    RuntimeState runtime;
    auto context = make_context(&runtime);
    expect_client_creation();
    ASSERT_TRUE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
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

TEST_F(FunctionToFileTest, QueryConfigurationIsCopiedAndMissingResourceFailsOpen) {
    auto context = make_context();
    auto resource = resource_config();
    resource.properties["AWS_BUCKET"] = "original";
    set_resource_config(resource);
    resource.properties["AWS_BUCKET"] = "changed";
    EXPECT_EQ(resource_config().properties.at("AWS_BUCKET"), "original");
    EXPECT_CALL(function, create_filesystem(_, _, _)).Times(0);
    query->set_s3_resources({{"other-resource", resource}});
    EXPECT_FALSE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    query->set_s3_resources({});
    EXPECT_FALSE(
            function.open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto missing = FunctionContext::create_context(nullptr, file, {text, text});
    EXPECT_FALSE(
            function.open(missing.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
}

TEST_F(FunctionToFileTest, SelectsResourcesByNameWithinOneQuery) {
    auto first = make_context();
    TS3Resource other;

    other.properties = {{"AWS_ENDPOINT", "other-endpoint"}};
    query->set_s3_resources({{"test-resource", resource_config()}, {"other-resource", other}});
    auto second = FunctionContext::create_context(&query_runtime, make_nullable(file),
                                                  {text, make_nullable(text)});
    second->set_constant_cols({std::make_shared<ColumnPtrWrapper>(
                                       text->create_column_const(1, string("other-resource"))),
                               nullptr});
    ASSERT_TRUE(
            function.open(first.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(
            function.open(second.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    expect_client_creation();
    EXPECT_CALL(function, create_filesystem(_, "s3://other-bucket/a.txt", _))
            .WillOnce(Invoke([&](const TS3Resource& resource, const std::string&,
                                 std::shared_ptr<io::FileSystem>* result) {
                EXPECT_EQ(resource.properties, other.properties);
                *result = filesystem;
                return Status::OK();
            }));
    EXPECT_CALL(*filesystem, stat_impl(_, _, _))
            .Times(2)
            .WillRepeatedly(
                    Invoke([](const io::Path&, io::FileStat* metadata, io::FileStatContext*) {
                        *metadata = io::FileStat {.size = 1};
                        return Status::OK();
                    }));
    ColumnPtr output;
    ASSERT_TRUE(execute(first.get(), uris({string("s3://bucket/a.txt")}), output).ok());
    EXPECT_EQ((*output)[0].get<TYPE_FILE>()[0].get<TYPE_STRING>(), "s3://bucket/a.txt");
    ASSERT_TRUE(execute(second.get(), uris({string("s3://other-bucket/a.txt")}), output).ok());
    EXPECT_EQ((*output)[0].get<TYPE_FILE>()[0].get<TYPE_STRING>(), "s3://other-bucket/a.txt");
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
