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

#include "exprs/function/ai/embed.h"

#include <curl/curl.h>
#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_number.h"
#include "core/value/jsonb_value.h"
#include "exprs/function/ai/ai_adapter.h"
#include "io/fs/obj_storage_client.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class MockHttpClient : public HttpClient {
public:
    curl_slist* get() { return this->_header_list; }

private:
    std::unordered_map<std::string, std::string> _headers;
    std::string _content_type;
};

class MockEmbedObjStorageClient : public io::ObjStorageClient {
public:
    io::ObjectStorageUploadResponse create_multipart_upload(
            const io::ObjectStoragePathOptions& /*opts*/) override {
        return {};
    }

    io::ObjectStorageResponse put_object(const io::ObjectStoragePathOptions& /*opts*/,
                                         std::string_view /*stream*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageUploadResponse upload_part(const io::ObjectStoragePathOptions& /*opts*/,
                                                std::string_view /*stream*/,
                                                int /*part_num*/) override {
        return {};
    }

    io::ObjectStorageResponse complete_multipart_upload(
            const io::ObjectStoragePathOptions& /*opts*/,
            const std::vector<io::ObjectCompleteMultiPart>& /*completed_parts*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageHeadResponse head_object(
            const io::ObjectStoragePathOptions& /*opts*/) override {
        return {};
    }

    io::ObjectStorageResponse get_object(const io::ObjectStoragePathOptions& /*opts*/,
                                         void* /*buffer*/, size_t /*offset*/, size_t /*bytes_read*/,
                                         size_t* /*size_return*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse list_objects(const io::ObjectStoragePathOptions& /*opts*/,
                                           std::vector<io::FileInfo>* /*files*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_objects(const io::ObjectStoragePathOptions& /*opts*/,
                                             std::vector<std::string> /*objs*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_object(const io::ObjectStoragePathOptions& /*opts*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_objects_recursively(
            const io::ObjectStoragePathOptions& /*opts*/) override {
        return io::ObjectStorageResponse::OK();
    }

    std::string generate_presigned_url(const io::ObjectStoragePathOptions& opts,
                                       int64_t expiration_secs, const S3ClientConf& conf) override {
        last_opts = opts;
        last_expiration_secs = expiration_secs;
        last_conf = conf;
        return fmt::format("mock-s3://{}/{}?ttl={}", opts.bucket, opts.key, expiration_secs);
    }

    io::ObjectStoragePathOptions last_opts;
    int64_t last_expiration_secs = 0;
    S3ClientConf last_conf;
};

class CountingMultimodalMockAdapter : public MockAdapter {
public:
    Status build_multimodal_embedding_request(const std::vector<MultimodalType>& media_types,
                                              const std::vector<std::string>& media_urls,
                                              const std::vector<std::string>& media_content_types,
                                              std::string& request_body) const override {
        EXPECT_EQ(media_types.size(), media_urls.size());
        EXPECT_EQ(media_content_types.size(), media_urls.size());
        batch_sizes.push_back(media_urls.size());
        request_body = "{}";
        return Status::OK();
    }

    mutable std::vector<size_t> batch_sizes;
};

class CountingTextMockAdapter : public MockAdapter {
public:
    Status build_embedding_request(const std::vector<std::string>& inputs,
                                   std::string& request_body) const override {
        batch_sizes.push_back(inputs.size());
        request_body = "{}";
        return Status::OK();
    }

    mutable std::vector<size_t> batch_sizes;
};

static ColumnString::MutablePtr create_jsonb_column(const std::vector<std::string>& json_rows) {
    auto column = ColumnString::create();
    for (const auto& json_row : json_rows) {
        JsonBinaryValue jsonb_value;
        Status st = jsonb_value.from_json_string(json_row);
        EXPECT_TRUE(st.ok()) << st.to_string();
        column->insert_data(jsonb_value.value(), jsonb_value.size());
    }
    return column;
}

static void assert_mock_embedding_column(const ColumnArray& col_array, size_t row_count) {
    const auto& offsets = col_array.get_offsets();
    ASSERT_EQ(offsets.size(), row_count);

    const auto& nested_nullable_col = assert_cast<const ColumnNullable&>(col_array.get_data());
    const auto& nested_col =
            assert_cast<const ColumnFloat32&>(*nested_nullable_col.get_nested_column_ptr());
    ASSERT_EQ(nested_col.size(), row_count * 5);

    for (size_t row = 0; row < row_count; ++row) {
        ASSERT_EQ(offsets[row], (row + 1) * 5);
        for (size_t i = 0; i < 5; ++i) {
            ASSERT_FLOAT_EQ(nested_col.get_element(row * 5 + i), static_cast<float>(i));
        }
    }
}

TEST(EMBED_TEST, embed_function_build_test) {
    FunctionEmbed function;

    std::vector<std::string> resources = {"resource_name"};
    std::vector<std::string> texts = {"this is a test prompt"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});

    ColumnNumbers arguments = {0, 1};
    std::string prompt;
    Status status = function.build_prompt(block, arguments, 0, prompt);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(prompt, "this is a test prompt");
}

TEST(EMBED_TEST, embed_function_test) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> texts = {"test input"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto sentiment_func = FunctionEmbed::create();
    Status exec_status =
            sentiment_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    const auto& col_array =
            assert_cast<const ColumnArray&>(*block.get_by_position(result_idx).column);
    const auto& offsets = col_array.get_offsets();
    ASSERT_EQ(offsets.size(), 1U);
    const auto& nested_nullable_col = assert_cast<const ColumnNullable&>(col_array.get_data());
    const auto& nested_col =
            assert_cast<const ColumnFloat32&>(*nested_nullable_col.get_nested_column_ptr());
    ASSERT_EQ(nested_col.size(), 5U);
    for (int i = 0; i < 5; ++i) {
        ASSERT_FLOAT_EQ(nested_col.get_element(i), static_cast<float>(i));
    }
}

TEST(EMBED_TEST, embed_function_text_multi_rows) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource", "mock_resource"};
    std::vector<std::string> texts = {"test input 1", "test input 2"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto embed_func = FunctionEmbed::create();
    Status exec_status =
            embed_func->execute_impl(ctx.get(), block, arguments, result_idx, texts.size());

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    const auto& col_array =
            assert_cast<const ColumnArray&>(*block.get_by_position(result_idx).column);
    assert_mock_embedding_column(col_array, texts.size());
}

TEST(EMBED_TEST, embed_function_multimodal_direct_url) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_file_presigned_url_ttl_seconds(0);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    query_ctx->set_mock_ai_resource();
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    std::vector<std::string> resources = {"mock_resource", "mock_resource", "mock_resource"};
    std::vector<std::string> file_json_rows = {
            R"({"content_type":"image/png","uri":"https://example.com/a.png"})",
            R"({"content_type":"video/mp4","uri":"https://example.com/b.mp4"})",
            R"({"content_type":"audio/mpeg","uri":"https://example.com/c.mp3"})"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_file = create_jsonb_column(file_json_rows);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_file), std::make_shared<DataTypeJsonb>(), "file"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto embed_func = FunctionEmbed::create();
    Status exec_status = embed_func->execute_impl(ctx.get(), block, arguments, result_idx,
                                                  file_json_rows.size());

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    const auto& col_array =
            assert_cast<const ColumnArray&>(*block.get_by_position(result_idx).column);
    assert_mock_embedding_column(col_array, file_json_rows.size());
}

TEST(EMBED_TEST, embed_function_multimodal_batch_request) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource", "mock_resource", "mock_resource"};
    std::vector<std::string> file_json_rows = {
            R"({"content_type":"image/png","uri":"https://example.com/a.png"})",
            R"({"content_type":"video/mp4","uri":"https://example.com/b.mp4"})",
            R"({"content_type":"audio/mpeg","uri":"https://example.com/c.mp3"})"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_file = create_jsonb_column(file_json_rows);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_file), std::make_shared<DataTypeJsonb>(), "file"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    TAIResource config;
    config.provider_type = "MOCK";
    auto counting_adapter = std::make_shared<CountingMultimodalMockAdapter>();
    std::shared_ptr<AIAdapter> adapter = counting_adapter;
    adapter->init(config);

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;
    FunctionEmbed embed_func;
    Status exec_status = embed_func.execute_with_adapter(ctx.get(), block, arguments, result_idx,
                                                         file_json_rows.size(), config, adapter);

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    EXPECT_THAT(counting_adapter->batch_sizes, ::testing::ElementsAre(3));

    const auto& col_array =
            assert_cast<const ColumnArray&>(*block.get_by_position(result_idx).column);
    assert_mock_embedding_column(col_array, file_json_rows.size());
}

TEST(EMBED_TEST, embed_function_multimodal_batch_split_by_session_variable) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_embed_max_batch_size(2);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    std::vector<std::string> resources = {"mock_resource", "mock_resource", "mock_resource"};
    std::vector<std::string> file_json_rows = {
            R"({"content_type":"image/png","uri":"https://example.com/a.png"})",
            R"({"content_type":"image/png","uri":"https://example.com/b.png"})",
            R"({"content_type":"image/png","uri":"https://example.com/c.png"})"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_file = create_jsonb_column(file_json_rows);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_file), std::make_shared<DataTypeJsonb>(), "file"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    TAIResource config;
    config.provider_type = "MOCK";
    auto counting_adapter = std::make_shared<CountingMultimodalMockAdapter>();
    std::shared_ptr<AIAdapter> adapter = counting_adapter;
    adapter->init(config);

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;
    FunctionEmbed embed_func;
    Status exec_status = embed_func.execute_with_adapter(ctx.get(), block, arguments, result_idx,
                                                         file_json_rows.size(), config, adapter);

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    EXPECT_THAT(counting_adapter->batch_sizes, ::testing::ElementsAre(2, 1));

    const auto& col_array =
            assert_cast<const ColumnArray&>(*block.get_by_position(result_idx).column);
    assert_mock_embedding_column(col_array, file_json_rows.size());
}

TEST(EMBED_TEST, embed_function_text_batch_split_by_session_variable) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_embed_max_batch_size(2);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    std::vector<std::string> resources = {"mock_resource", "mock_resource", "mock_resource"};
    std::vector<std::string> texts = {"text-a", "text-b", "text-c"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_text = ColumnHelper::create_column<DataTypeString>(texts);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_text), std::make_shared<DataTypeString>(), "text"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    TAIResource config;
    config.provider_type = "MOCK";
    auto counting_adapter = std::make_shared<CountingTextMockAdapter>();
    std::shared_ptr<AIAdapter> adapter = counting_adapter;
    adapter->init(config);

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;
    FunctionEmbed embed_func;
    Status exec_status = embed_func.execute_with_adapter(ctx.get(), block, arguments, result_idx,
                                                         texts.size(), config, adapter);

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    EXPECT_THAT(counting_adapter->batch_sizes, ::testing::ElementsAre(2, 1));

    const auto& col_array =
            assert_cast<const ColumnArray&>(*block.get_by_position(result_idx).column);
    assert_mock_embedding_column(col_array, texts.size());
}

TEST(EMBED_TEST, embed_function_multimodal_s3_presigned_url) {
    TQueryOptions query_options = create_fake_query_options();
    query_options.__set_file_presigned_url_ttl_seconds(123);
    auto query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), query_options);
    query_ctx->set_mock_ai_resource();
    TQueryGlobals query_globals;
    RuntimeState runtime_state(TUniqueId(), 0, query_options, query_globals, nullptr,
                               query_ctx.get());
    auto ctx = FunctionContext::create_context(&runtime_state, {}, {});

    auto mock_client = std::make_shared<MockEmbedObjStorageClient>();
    S3ClientFactory::instance().set_client_creator_for_test(
            [mock_client](const S3ClientConf&) { return mock_client; });

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> file_json_rows = {R"({
        "content_type":"image/png",
        "uri":"s3://test-bucket/path/to/image.png",
        "endpoint":"cos.ap-beijing.myqcloud.com",
        "region":"ap-beijing",
        "ak":"test-ak",
        "sk":"test-sk",
        "role_arn":"test-role",
        "external_id":"test-external-id"
    })"};

    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_file = create_jsonb_column(file_json_rows);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_file), std::make_shared<DataTypeJsonb>(), "file"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto embed_func = FunctionEmbed::create();
    Status exec_status = embed_func->execute_impl(ctx.get(), block, arguments, result_idx,
                                                  file_json_rows.size());

    S3ClientFactory::instance().clear_client_creator_for_test();

    ASSERT_TRUE(exec_status.ok()) << exec_status.to_string();
    const auto& col_array =
            assert_cast<const ColumnArray&>(*block.get_by_position(result_idx).column);
    assert_mock_embedding_column(col_array, file_json_rows.size());

    ASSERT_EQ(mock_client->last_opts.bucket, "test-bucket");
    ASSERT_EQ(mock_client->last_opts.key, "path/to/image.png");
    ASSERT_EQ(mock_client->last_expiration_secs, 123);
    ASSERT_EQ(mock_client->last_conf.endpoint, "cos.ap-beijing.myqcloud.com");
    ASSERT_EQ(mock_client->last_conf.region, "ap-beijing");
    ASSERT_EQ(mock_client->last_conf.ak, "test-ak");
    ASSERT_EQ(mock_client->last_conf.sk, "test-sk");
    ASSERT_EQ(mock_client->last_conf.role_arn, "test-role");
    ASSERT_EQ(mock_client->last_conf.external_id, "test-external-id");
}

TEST(EMBED_TEST, embed_function_multimodal_s3_missing_endpoint) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> file_json_rows = {R"({
        "content_type":"image/png",
        "uri":"s3://test-bucket/path/to/image.png",
        "region":"ap-beijing"
    })"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_file = create_jsonb_column(file_json_rows);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_file), std::make_shared<DataTypeJsonb>(), "file"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto embed_func = FunctionEmbed::create();
    Status exec_status = embed_func->execute_impl(ctx.get(), block, arguments, result_idx,
                                                  file_json_rows.size());

    ASSERT_FALSE(exec_status.ok());
    ASSERT_NE(exec_status.to_string().find("field 'endpoint' is required"), std::string::npos);
}

TEST(EMBED_TEST, embed_function_multimodal_s3_missing_region) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> file_json_rows = {R"({
        "content_type":"image/png",
        "uri":"s3://test-bucket/path/to/image.png",
        "endpoint":"cos.ap-beijing.myqcloud.com"
    })"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_file = create_jsonb_column(file_json_rows);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_file), std::make_shared<DataTypeJsonb>(), "file"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto embed_func = FunctionEmbed::create();
    Status exec_status = embed_func->execute_impl(ctx.get(), block, arguments, result_idx,
                                                  file_json_rows.size());

    ASSERT_FALSE(exec_status.ok());
    ASSERT_NE(exec_status.to_string().find("field 'region' is required"), std::string::npos);
}

TEST(EMBED_TEST, embed_function_wrong_argument_count) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0};
    size_t result_idx = 1;

    auto embed_func = FunctionEmbed::create();
    Status exec_status =
            embed_func->execute_impl(ctx.get(), block, arguments, result_idx, resources.size());

    ASSERT_FALSE(exec_status.ok());
    ASSERT_NE(exec_status.to_string().find("Function EMBED expects 2 arguments"),
              std::string::npos);
}

TEST(EMBED_TEST, embed_function_invalid_input_type) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<Int32> ids = {1};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_ids = ColumnHelper::create_column<DataTypeInt32>(ids);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_ids), std::make_shared<DataTypeInt32>(), "id"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto embed_func = FunctionEmbed::create();
    Status exec_status =
            embed_func->execute_impl(ctx.get(), block, arguments, result_idx, resources.size());

    ASSERT_FALSE(exec_status.ok());
    ASSERT_NE(exec_status.to_string().find(
                      "Function EMBED expects the second argument to be STRING or JSON"),
              std::string::npos);
}

TEST(EMBED_TEST, embed_function_missing_required_json_field) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> file_json_rows = {R"({"uri":"https://example.com/a.png"})"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_file = create_jsonb_column(file_json_rows);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_file), std::make_shared<DataTypeJsonb>(), "file"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto embed_func = FunctionEmbed::create();
    Status exec_status = embed_func->execute_impl(ctx.get(), block, arguments, result_idx,
                                                  file_json_rows.size());

    ASSERT_FALSE(exec_status.ok());
    ASSERT_NE(exec_status.to_string().find("field 'content_type' is required"), std::string::npos);
}

TEST(EMBED_TEST, embed_function_unsupported_content_type) {
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto ctx = FunctionContext::create_context(runtime_state.get(), {}, {});

    std::vector<std::string> resources = {"mock_resource"};
    std::vector<std::string> file_json_rows = {
            R"({"content_type":"text/plain","uri":"https://example.com/a.txt"})"};
    auto col_resource = ColumnHelper::create_column<DataTypeString>(resources);
    auto col_file = create_jsonb_column(file_json_rows);

    Block block;
    block.insert({std::move(col_resource), std::make_shared<DataTypeString>(), "resource"});
    block.insert({std::move(col_file), std::make_shared<DataTypeJsonb>(), "file"});
    block.insert(
            {nullptr,
             std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>())),
             "result"});

    ColumnNumbers arguments = {0, 1};
    size_t result_idx = 2;

    auto embed_func = FunctionEmbed::create();
    Status exec_status = embed_func->execute_impl(ctx.get(), block, arguments, result_idx,
                                                  file_json_rows.size());

    ASSERT_FALSE(exec_status.ok());
    ASSERT_NE(exec_status.to_string().find("Unsupported content_type for EMBED"),
              std::string::npos);
}

TEST(EMBED_TEST, local_adapter_embedding_request) {
    LocalAdapter adapter;
    TAIResource config;
    config.model_name = "local-embedding-model";
    config.dimensions = 1536;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    curl_slist* headers = mock_client.get();
    ASSERT_TRUE(headers != nullptr);
    EXPECT_STREQ(headers->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"test sentence for embedding"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "local-embedding-model");

    // input
    ASSERT_TRUE(doc.HasMember("input")) << "Missing input field";
    ASSERT_TRUE(doc["input"].IsString() || doc["input"].IsArray())
            << "Input field is not a string or array";
    if (doc["input"].IsString()) {
        ASSERT_STREQ(doc["input"].GetString(), "test sentence for embedding");
    } else {
        ASSERT_EQ(doc["input"].Size(), 1);
        ASSERT_STREQ(doc["input"][0].GetString(), "test sentence for embedding");
    }
}

TEST(EMBED_TEST, local_adapter_parse_embedding_response) {
    LocalAdapter adapter;

    std::string resp1 = R"({
        "object": "list",
        "data": [
            {
                "object": "embedding",
                "embedding": [0.1, 0.2, 0.3],
                "index": 0
            },
            {
                "object": "embedding",
                "embedding": [0.4, 0.5],
                "index": 1
            }
        ],
        "model": "mxbai-embed-large",
        "usage": {
            "prompt_tokens": 8,
            "total_tokens": 8
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp1, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_EQ(results[1].size(), 2);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][1], 0.2F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);
    ASSERT_FLOAT_EQ(results[1][0], 0.4F);
    ASSERT_FLOAT_EQ(results[1][1], 0.5F);

    std::string resp2 = R"({
        "embedding": [0.1, 0.2, 0.3]
    })";
    results.clear();
    st = adapter.parse_embedding_response(resp2, results);
    ASSERT_TRUE(st.ok()) << "Format 2 failed: " << st.to_string();
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);

    std::string resp3 = R"({
        "embeddings": [[0.6, 0.7]]
    })";
    results.clear();
    st = adapter.parse_embedding_response(resp3, results);
    ASSERT_TRUE(st.ok()) << "Format 3 failed: " << st.to_string();
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 2);
    ASSERT_FLOAT_EQ(results[0][0], 0.6F);
    ASSERT_FLOAT_EQ(results[0][1], 0.7F);
}

TEST(EMBED_TEST, openai_adapter_embedding_request) {
    OpenAIAdapter adapter;
    TAIResource config;
    config.model_name = "text-embedding-ada-002";
    config.api_key = "test_openai_key";
    config.dimensions = 1536;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "Authorization: Bearer test_openai_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed this text"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "text-embedding-ada-002");

    // input
    ASSERT_TRUE(doc.HasMember("input")) << "Missing input field";
    ASSERT_TRUE(doc["input"].IsArray()) << "Input field is not an array";
    ASSERT_EQ(doc["input"].Size(), 1);
    ASSERT_STREQ(doc["input"][0].GetString(), "embed this text");

    // should not support dimensions param
    ASSERT_FALSE(doc.HasMember("dimensions"));

    config.model_name = "text-embedding-3";
    adapter.init(config);
    st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_TRUE(doc.HasMember("dimensions")) << request_body;
    ASSERT_TRUE(doc["dimensions"].IsInt()) << "Dimensions is not an integer";
    ASSERT_EQ(doc["dimensions"].GetInt(), 1536);
}

TEST(EMBED_TEST, openai_adapter_parse_embedding_response) {
    OpenAIAdapter adapter;

    std::string resp = R"({
        "object": "list",
        "data": [
            {
                "object": "embedding",
                "embedding": [0.1, 0.2, 0.3],
                "index": 0
            },
            {
                "object": "embedding",
                "embedding": [0.4, 0.5],
                "index": 1
            }
        ],
        "model": "text-embedding-ada-002",
        "usage": {
            "prompt_tokens": 8,
            "total_tokens": 8
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_EQ(results[1].size(), 2);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][1], 0.2F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);
    ASSERT_FLOAT_EQ(results[1][0], 0.4F);
    ASSERT_FLOAT_EQ(results[1][1], 0.5F);
}

TEST(EMBED_TEST, zhipu_embedding_request) {
    ZhipuAdapter adapter;
    TAIResource config;
    config.model_name = "embedding-2";
    config.api_key = "test_zhipu_key";
    config.dimensions = 1024;
    adapter.init(config);

    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());
    EXPECT_STREQ(mock_client.get()->data, "Authorization: Bearer test_zhipu_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed this text"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_FALSE(doc.HasMember("dimensions")) << request_body;

    config.model_name = "embedding-3";
    adapter.init(config);
    st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_TRUE(doc.HasMember("dimensions")) << request_body;
    ASSERT_TRUE(doc["dimensions"].IsInt()) << "Dimensions is not an integer";
    ASSERT_EQ(doc["dimensions"].GetInt(), config.dimensions);
}

TEST(EMBED_TEST, qwen_embedding_request) {
    QwenAdapter adapter;
    TAIResource config;
    config.model_name = "text-embedding-v2";
    config.api_key = "test_qwen_key";
    config.dimensions = 1024;
    adapter.init(config);

    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());
    EXPECT_STREQ(mock_client.get()->data, "Authorization: Bearer test_qwen_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed this text"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_FALSE(doc.HasMember("dimension")) << request_body;

    config.model_name = "test-embedding-v4";
    adapter.init(config);
    st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_TRUE(doc.HasMember("dimension")) << request_body;
    ASSERT_TRUE(doc["dimension"].IsInt()) << "Dimension is not an integer";
    ASSERT_EQ(doc["dimension"].GetInt(), config.dimensions);
}

TEST(EMBED_TEST, gemini_adapter_embedding_request) {
    GeminiAdapter adapter;
    TAIResource config;
    config.model_name = "embedding-001";
    config.api_key = "test_gemini_key";
    config.dimensions = 768;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "x-goog-api-key: test_gemini_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with gemini", "embed batch with gemini"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    ASSERT_TRUE(doc.HasMember("requests")) << "Missing requests field";
    ASSERT_TRUE(doc["requests"].IsArray()) << request_body;
    ASSERT_EQ(doc["requests"].Size(), 2);

    const auto& request0 = doc["requests"][0];
    ASSERT_TRUE(request0.HasMember("model")) << request_body;
    ASSERT_STREQ(request0["model"].GetString(), "models/embedding-001");
    ASSERT_TRUE(request0.HasMember("content")) << request_body;
    ASSERT_TRUE(request0["content"].IsObject()) << request_body;
    ASSERT_TRUE(request0["content"].HasMember("parts")) << request_body;
    ASSERT_TRUE(request0["content"]["parts"].IsArray()) << request_body;
    ASSERT_EQ(request0["content"]["parts"].Size(), 1);
    ASSERT_TRUE(request0["content"]["parts"][0].HasMember("text")) << request_body;
    ASSERT_STREQ(request0["content"]["parts"][0]["text"].GetString(), "embed with gemini");
    ASSERT_FALSE(request0.HasMember("outputDimensionality"));

    const auto& request1 = doc["requests"][1];
    ASSERT_TRUE(request1.HasMember("model")) << request_body;
    ASSERT_STREQ(request1["model"].GetString(), "models/embedding-001");
    ASSERT_TRUE(request1.HasMember("content")) << request_body;
    ASSERT_TRUE(request1["content"].IsObject()) << request_body;
    ASSERT_TRUE(request1["content"].HasMember("parts")) << request_body;
    ASSERT_TRUE(request1["content"]["parts"].IsArray()) << request_body;
    ASSERT_EQ(request1["content"]["parts"].Size(), 1);
    ASSERT_TRUE(request1["content"]["parts"][0].HasMember("text")) << request_body;
    ASSERT_STREQ(request1["content"]["parts"][0]["text"].GetString(), "embed batch with gemini");

    config.model_name = "gemini-embedding-001";
    adapter.init(config);
    st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_TRUE(doc.HasMember("requests")) << request_body;
    ASSERT_TRUE(doc["requests"].IsArray()) << request_body;
    ASSERT_EQ(doc["requests"].Size(), 2);
    ASSERT_TRUE(doc["requests"][0].HasMember("outputDimensionality")) << request_body;
    ASSERT_EQ(doc["requests"][0]["outputDimensionality"].GetInt(), 768) << request_body;
    ASSERT_TRUE(doc["requests"][1].HasMember("outputDimensionality")) << request_body;
    ASSERT_EQ(doc["requests"][1]["outputDimensionality"].GetInt(), 768) << request_body;
}

TEST(EMBED_TEST, gemini_adapter_parse_embedding_response) {
    GeminiAdapter adapter;

    std::string resp = R"({
        "embedding": {
            "values":[
                0.1,
                0.2,
                0.3
            ]
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][1], 0.2F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);

    resp = R"({
        "embeddings": [
            {
                "values":[
                    1.1,
                    1.2
                ]
            },
            {
                "values":[
                    2.1,
                    2.2,
                    2.3
                ]
            }
        ]
    })";

    results.clear();
    st = adapter.parse_embedding_response(resp, results);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0].size(), 2);
    ASSERT_EQ(results[1].size(), 3);
    ASSERT_FLOAT_EQ(results[0][0], 1.1F);
    ASSERT_FLOAT_EQ(results[0][1], 1.2F);
    ASSERT_FLOAT_EQ(results[1][0], 2.1F);
    ASSERT_FLOAT_EQ(results[1][1], 2.2F);
    ASSERT_FLOAT_EQ(results[1][2], 2.3F);
}

TEST(EMBED_TEST, voyageai_adapter_embedding_request) {
    VoyageAIAdapter adapter;
    TAIResource config;
    config.model_name = "voyage-multimodal-3";
    config.api_key = "test_voyage_key";
    config.dimensions = 1024;
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    EXPECT_STREQ(mock_client.get()->data, "Authorization: Bearer test_voyage_key");
    EXPECT_STREQ(mock_client.get()->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with voyage"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "voyage-multimodal-3");

    // input
    ASSERT_TRUE(doc.HasMember("input")) << "Missing input field";
    ASSERT_TRUE(doc["input"].IsArray()) << "Input field is not an array";
    ASSERT_EQ(doc["input"].Size(), 1);
    ASSERT_STREQ(doc["input"][0].GetString(), "embed with voyage");

    // dimension parameter
    ASSERT_FALSE(doc.HasMember("output_dimension"));

    config.model_name = "voyage-3.5";
    adapter.init(config);
    st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";
    ASSERT_TRUE(doc.HasMember("model"));
    ASSERT_STREQ(doc["model"].GetString(), "voyage-3.5") << request_body;
    ASSERT_TRUE(doc.HasMember("output_dimension")) << request_body;
}

TEST(EMBED_TEST, voyageai_adapter_parse_embedding_response) {
    VoyageAIAdapter adapter;

    std::string resp = R"({
        "object": "list",
        "data": [
            {
            "embedding": [0.1, 0.2, 0.3],
            "index": 0
            },
            {
            "embedding": [0.4, 0.5],
            "index": 1
            }
        ],
        "model": "voyage-3.5",
        "usage": {
            "total_tokens": 10
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0].size(), 3);
    ASSERT_EQ(results[1].size(), 2);
    ASSERT_FLOAT_EQ(results[0][0], 0.1F);
    ASSERT_FLOAT_EQ(results[0][1], 0.2F);
    ASSERT_FLOAT_EQ(results[0][2], 0.3F);
    ASSERT_FLOAT_EQ(results[1][0], 0.4F);
    ASSERT_FLOAT_EQ(results[1][1], 0.5F);
}

TEST(EMBED_TEST, voyageai_adapter_parse_error_test) {
    VoyageAIAdapter adapter;

    // doc is not an object
    std::string resp = R"(
        "object": "list",
        "data": [
            {
            "embedding": [0.1, 0.2, 0.3],
            "index": 0
            },
            {
            "embedding": [0.4, 0.5],
            "index": 1
            }
        ],
        "model": "voyage-3.5",
        "usage": {
            "total_tokens": 10
        }
    )";
    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_FALSE(st.ok());
    ASSERT_THAT(st.to_string().c_str(), ::testing::HasSubstr("Failed to parse  response"));

    // `data` is not an array
    resp = R"({
        "object": "list",
        "data": {
            "embedding": [0.1, 0.2, 0.3],
            "index": 0
        },
        "model": "voyage-3.5",
        "usage": {
            "total_tokens": 10
        }
    })";
    st = adapter.parse_embedding_response(resp, results);
    ASSERT_FALSE(st.ok());
    ASSERT_THAT(st.to_string().c_str(), ::testing::HasSubstr("Invalid  response format"));

    // member `embedding` is missing
    resp = R"({
        "object": "list",
        "data": [
            {
            "embeddings": [0.1, 0.2, 0.3],
            "index": 0
            }
        ],
        "model": "voyage-3.5",
        "usage": {
            "total_tokens": 10
        }
    })";
    st = adapter.parse_embedding_response(resp, results);
    ASSERT_FALSE(st.ok());
    ASSERT_THAT(st.to_string().c_str(), ::testing::HasSubstr("Invalid  response format"));
}

TEST(EMBED_TEST, deepseek_adapter_embedding_request) {
    DeepSeekAdapter adapter;
    TAIResource config;
    config.model_name = "deepseek-embedding";
    config.api_key = "test_deepseek_key";
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    curl_slist* headers = mock_client.get();
    ASSERT_TRUE(headers != nullptr);
    EXPECT_STREQ(headers->data, "Authorization: Bearer test_deepseek_key");
    EXPECT_STREQ(headers->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with deepseek"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_FALSE(st.ok());
    ASSERT_THAT(st.to_string(),
                ::testing::HasSubstr("Currently supported providers are OpenAI, Gemini, "
                                     "Voyage, Jina, Qwen, and Minimax"));
}

TEST(EMBED_TEST, deepseek_adapter_parse_embedding_response) {
    DeepSeekAdapter adapter;

    std::string resp = R"({
        "data": [
            {
                "embedding": [0.1, 0.2, 0.3, 0.4, 0.5],
                "index": 0,
                "object": "embedding"
            }
        ],
        "model": "deepseek-embedding",
        "object": "list",
        "usage": {
            "prompt_tokens": 4,
            "total_tokens": 4
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_FALSE(st.ok());
    ASSERT_THAT(st.to_string(),
                ::testing::HasSubstr("Currently supported providers are OpenAI, Gemini, "
                                     "Voyage, Jina, Qwen, and Minimax"));
}

TEST(EMBED_TEST, moonshot_adapter_embedding_request) {
    MoonShotAdapter adapter;
    TAIResource config;
    config.model_name = "moonshot-embedding";
    config.api_key = "test_moonshot_key";
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    curl_slist* headers = mock_client.get();
    ASSERT_TRUE(headers != nullptr);
    EXPECT_STREQ(headers->data, "Authorization: Bearer test_moonshot_key");
    EXPECT_STREQ(headers->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with moonshot"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_FALSE(st.ok());
    ASSERT_THAT(st.to_string(),
                ::testing::HasSubstr("Currently supported providers are OpenAI, Gemini, "
                                     "Voyage, Jina, Qwen, and Minimax"));
}

TEST(EMBED_TEST, moonshot_adapter_parse_embedding_response) {
    MoonShotAdapter adapter;

    std::string resp = R"({
        "id": "embedding-123",
        "object": "list",
        "data": [
            {
                "embedding": [0.1, 0.2, 0.3, 0.4, 0.5],
                "index": 0,
                "object": "embedding"
            }
        ],
        "model": "moonshot-embedding",
        "usage": {
            "prompt_tokens": 3,
            "total_tokens": 3
        }
    })";

    std::vector<std::vector<float>> results;
    Status st = adapter.parse_embedding_response(resp, results);
    ASSERT_FALSE(st.ok());
    ASSERT_THAT(st.to_string(),
                ::testing::HasSubstr("Currently supported providers are OpenAI, Gemini, "
                                     "Voyage, Jina, Qwen, and Minimax"));
}

TEST(EMBED_TEST, minimax_adapter_embedding_request) {
    MinimaxAdapter adapter;
    TAIResource config;
    config.model_name = "minimax-embedding";
    config.api_key = "test_minimax_key";
    adapter.init(config);

    // header test
    MockHttpClient mock_client;
    Status auth_status = adapter.set_authentication(&mock_client);
    ASSERT_TRUE(auth_status.ok());

    curl_slist* headers = mock_client.get();
    ASSERT_TRUE(headers != nullptr);
    EXPECT_STREQ(headers->data, "Authorization: Bearer test_minimax_key");
    EXPECT_STREQ(headers->next->data, "Content-Type: application/json");

    std::vector<std::string> inputs = {"embed with minimax"};
    std::string request_body;
    Status st = adapter.build_embedding_request(inputs, request_body);
    ASSERT_TRUE(st.ok());

    // body test
    rapidjson::Document doc;
    doc.Parse(request_body.c_str());
    ASSERT_FALSE(doc.HasParseError()) << "JSON parse error";
    ASSERT_TRUE(doc.IsObject()) << "JSON is not an object";

    // model name
    ASSERT_TRUE(doc.HasMember("model")) << "Missing model field";
    ASSERT_TRUE(doc["model"].IsString()) << "Model field is not a string";
    ASSERT_STREQ(doc["model"].GetString(), "minimax-embedding");

    // type
    ASSERT_TRUE(doc.HasMember("type")) << "Missing type field";
    ASSERT_TRUE(doc["type"].IsString());
    ASSERT_STREQ(doc["type"].GetString(), "db");

    // input
    ASSERT_TRUE(doc.HasMember("texts")) << "Missing texts field";
    ASSERT_TRUE(doc["texts"].IsArray()) << "Texts field is not an array";
    ASSERT_EQ(doc["texts"].Size(), 1);
    ASSERT_STREQ(doc["texts"][0].GetString(), "embed with minimax");
}

} // namespace doris
