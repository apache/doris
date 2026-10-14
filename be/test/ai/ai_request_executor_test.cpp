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

#include "exprs/function/ai/ai_request_executor.h"

#include <event2/http.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <functional>
#include <future>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/data_type/data_type_jsonb.h"
#include "core/value/jsonb_value.h"
#include "exprs/aggregate/aggregate_function_ai_agg.h"
#include "exprs/function/ai/ai_adapter.h"
#include "exprs/function/ai/ai_summarize.h"
#include "exprs/function/ai/embed.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_client.h"
#include "service/http/http_handler.h"
#include "service/http/http_request.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

class AIResponseHandler : public HttpHandler {
public:
    void set_responses(std::vector<std::pair<HttpStatus, std::string>> values) {
        std::lock_guard lock(mutex);
        responses = std::move(values);
        response_index = 0;
    }

    void handle(HttpRequest* request) override {
        const auto callback = on_request;
        if (callback) {
            callback();
        }
        std::lock_guard lock(mutex);
        char* address = nullptr;
        uint16_t port = 0;
        // Peer ports identify actual TCP reuse, rather than C++ object reuse.
        evhttp_connection_get_peer(evhttp_request_get_connection(request->get_evhttp_request()),
                                   &address, &port);
        peer_ports.push_back(port);
        bodies.push_back(request->get_request_body());
        authorizations.push_back(request->header("Authorization"));
        content_types.push_back(request->header("Content-Type"));
        paths.push_back(request->uri());
        if (close_first_connection && bodies.size() == 1) {
            evhttp_add_header(evhttp_request_get_output_headers(request->get_evhttp_request()),
                              "Connection", "close");
        }
        if (!retry_after.empty()) {
            evhttp_add_header(evhttp_request_get_output_headers(request->get_evhttp_request()),
                              "Retry-After", retry_after.c_str());
        }
        const auto& [status, body] = responses[std::min(response_index++, responses.size() - 1)];
        if (truncate_response || (truncate_first_response && bodies.size() == 1)) {
            evhttp_add_header(evhttp_request_get_output_headers(request->get_evhttp_request()),
                              "Content-Length", std::to_string(body.size() + 10).c_str());
            evhttp_add_header(evhttp_request_get_output_headers(request->get_evhttp_request()),
                              "Connection", "close");
        }
        HttpChannel::send_reply(request, status, body);
    }

    std::function<void()> on_request;
    bool truncate_response = false;
    bool truncate_first_response = false;
    std::string retry_after;
    bool close_first_connection = false;
    std::mutex mutex;
    size_t response_index = 0;
    std::vector<std::pair<HttpStatus, std::string>> responses;
    std::vector<uint16_t> peer_ports;
    std::vector<std::string> bodies;
    std::vector<std::string> authorizations;
    std::vector<std::string> content_types;
    std::vector<std::string> paths;
};

class AIRequestExecutorTest : public testing::Test {
protected:
    void SetUp() override {
        auto options = create_fake_query_options();
        options.__set_query_timeout(10);
        options.__set_execution_timeout(10);
        options.__set_ai_context_window_size(1);
        options.__set_embed_max_batch_size(1);
        query_ctx = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), options);
        TQueryGlobals globals;
        state = std::make_unique<RuntimeState>(TUniqueId(), 0, options, globals, nullptr,
                                               query_ctx.get());
        context = FunctionContext::create_context(state.get(), {}, {});
        ASSERT_TRUE(server.register_handler(POST, "/ai", &handler));
        ASSERT_TRUE(server.register_handler(POST, "/other", &handler));
        static_cast<void>(server.start());
        ASSERT_NE(server.get_real_port(), 0);
        config.endpoint = "http://127.0.0.1:" + std::to_string(server.get_real_port()) + "/ai";
        config.provider_type = "OPENAI";
        config.model_name = "test-model";
        config.api_key = "test-key";
        config.max_retries = 3;
        config.retry_delay_second = 0;
        adapter = std::make_shared<OpenAIAdapter>();
        adapter->init(config);
    }

    void use_fake_clock() {
        fake_now = std::chrono::steady_clock::now();
        executor._now = [&] { return fake_now; };
        executor._sleep = [&](auto duration) { fake_now += duration; };
    }

    void replay_legacy_authentication_retry_policy() {
        HttpClient legacy_client;
        for (int attempt = 0; attempt <= config.max_retries; ++attempt) {
            ASSERT_TRUE(legacy_client.init(config.endpoint, false).ok());
            ASSERT_TRUE(adapter->set_authentication(&legacy_client).ok());
            std::string body;
            ASSERT_TRUE(legacy_client.execute_post_request("{}", &body).ok());
            ASSERT_EQ(legacy_client.get_http_status(), 401);
            if (attempt != config.max_retries) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    }

    std::chrono::steady_clock::time_point fake_now;

    Status execute(const std::string& body, std::string& response) {
        return executor.execute(body, response, config, *adapter, query_ctx.get());
    }

    void set_resource() {
        TAIResource resource;
        resource.endpoint = config.endpoint;
        resource.provider_type = config.provider_type;
        resource.model_name = config.model_name;
        resource.api_key = config.api_key;
        resource.max_retries = config.max_retries;
        resource.retry_delay_second = config.retry_delay_second;
        query_ctx->set_ai_resources(std::map<std::string, TAIResource> {{"resource", resource}});
    }

    size_t connection_count() {
        std::lock_guard lock(handler.mutex);
        return std::set<uint16_t>(handler.peer_ports.begin(), handler.peer_ports.end()).size();
    }

    void verify_concurrent_requests(bool share_executor) {
        handler.set_responses(
                {{HttpStatus::OK, R"({"data":[{"index":0,"embedding":[0.5,1.0]}]})"}});
        // Separate server event loops make simultaneous arrival deterministic.
        EvHttpServer second_server {0};
        ASSERT_TRUE(second_server.register_handler(POST, "/ai", &handler));
        second_server.start();
        AIResource second_config = config;
        second_config.endpoint =
                "http://127.0.0.1:" + std::to_string(second_server.get_real_port()) + "/ai";
        auto second_adapter = std::make_shared<OpenAIAdapter>();
        second_adapter->init(second_config);
        auto second_context = context->clone();
        if (share_executor) {
            auto shared_executor = std::make_shared<AIRequestExecutor>();
            context->set_function_state(FunctionContext::THREAD_LOCAL, shared_executor);
            second_context->set_function_state(FunctionContext::THREAD_LOCAL, shared_executor);
        }
        std::mutex gate_mutex;
        std::condition_variable gate;
        int arrivals = 0;
        bool both_arrived = true;
        handler.on_request = [&] {
            std::unique_lock lock(gate_mutex);
            ++arrivals;
            gate.notify_all();
            both_arrived &=
                    gate.wait_for(lock, std::chrono::seconds(2), [&] { return arrivals == 2; });
        };
        auto send = [&](FunctionContext* ctx, const AIResource& resource,
                        std::shared_ptr<AIAdapter> provider) {
            std::vector<std::vector<float>> result;
            return FunctionEmbed()._execute_prebuilt_embedding_request(
                    R"({"input":["first"]})", result, 1, resource, provider, ctx);
        };
        auto first = std::async(std::launch::async, send, context.get(), config, adapter);
        auto second = std::async(std::launch::async, send, second_context.get(), second_config,
                                 second_adapter);
        EXPECT_TRUE(first.get().ok());
        EXPECT_TRUE(second.get().ok());
        EXPECT_TRUE(both_arrived);
        EXPECT_EQ(context->get_function_state(FunctionContext::THREAD_LOCAL) ==
                          second_context->get_function_state(FunctionContext::THREAD_LOCAL),
                  share_executor);
        EXPECT_EQ(connection_count(), 2);
        handler.on_request = {};
    }

    // The server stops before the handler and its recorded requests are destroyed.
    AIRequestExecutor executor;
    AIResponseHandler handler;
    EvHttpServer server {0};
    std::shared_ptr<QueryContext> query_ctx;
    std::unique_ptr<RuntimeState> state;
    std::unique_ptr<FunctionContext> context;
    AIResource config {TAIResource {}};
    std::shared_ptr<AIAdapter> adapter;
};

TEST_F(AIRequestExecutorTest, ReusesConnectionAcrossTenRequests) {
    handler.set_responses({{HttpStatus::OK, R"({"result":"ok"})"}});
    for (int i = 0; i < 10; ++i) {
        std::string response;
        ASSERT_TRUE(execute(std::to_string(i), response).ok());
        EXPECT_EQ(response, R"({"result":"ok"})");
    }
    std::lock_guard lock(handler.mutex);
    ASSERT_EQ(handler.peer_ports.size(), 10);
    EXPECT_EQ(std::set<uint16_t>(handler.peer_ports.begin(), handler.peer_ports.end()).size(), 1);
}

TEST_F(AIRequestExecutorTest, CachedClientsDiscardRequestOptionsAfterSuccessAndFailure) {
    class InspectingAdapter : public OpenAIAdapter {
    public:
        Status set_authentication(HttpClient* client) const override {
            saved_client = client;
            RETURN_IF_ERROR(OpenAIAdapter::set_authentication(client));
            curl_easy_setopt(client->_curl, CURLOPT_PRIVATE, "request state");
            return Status::OK();
        }
        mutable HttpClient* saved_client = nullptr;
    } inspecting_adapter;
    inspecting_adapter.init(config);
    config.max_retries = 1;
    for (const auto code :
         {HttpStatus::OK, HttpStatus::UNAUTHORIZED, HttpStatus::SERVICE_UNAVAILABLE}) {
        handler.set_responses({{code, "response"}});
        std::string response;
        const auto status = executor.execute("request body", response, config, inspecting_adapter,
                                             query_ctx.get());
        EXPECT_EQ(status.ok(), code == HttpStatus::OK);
        ASSERT_NE(inspecting_adapter.saved_client, nullptr);
        char* request_state = nullptr;
        ASSERT_EQ(curl_easy_getinfo(inspecting_adapter.saved_client->_curl, CURLINFO_PRIVATE,
                                    &request_state),
                  CURLE_OK);
        EXPECT_EQ(request_state, nullptr);
        EXPECT_EQ(inspecting_adapter.saved_client->_header_list, nullptr);
    }
    EXPECT_EQ(connection_count(), 1);
    for (const auto& body : handler.bodies) {
        EXPECT_EQ(body, "request body");
    }
}

TEST_F(AIRequestExecutorTest, ScalarReusesConnectionAcrossBatchesAndBlocksWithNulls) {
    handler.set_responses(
            {{HttpStatus::OK, R"({"choices":[{"message":{"content":"[\"summary\"]"}}]})"}});
    FunctionAISummarize function;
    for (int block_index = 0; block_index < 5; ++block_index) {
        Block block;
        block.insert({ColumnHelper::create_column<DataTypeString>({"resource"}),
                      std::make_shared<DataTypeString>(), "resource"});
        block.insert({ColumnHelper::create_nullable_column<DataTypeString>(
                              {"first", "ignored", "second"}, {0, 1, 0}),
                      make_nullable(std::make_shared<DataTypeString>()), "input"});
        block.insert({nullptr, make_nullable(std::make_shared<DataTypeString>()), "result"});
        ASSERT_TRUE(function.execute(context.get(), block, {0, 1}, 2, 3, config, adapter).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        ASSERT_EQ(result.size(), 3);
        EXPECT_EQ(result.get_null_map_data()[1], 1);
        EXPECT_EQ(result.get_data_at(0).to_string(), "summary");
        EXPECT_EQ(result.get_data_at(2).to_string(), "summary");
    }
    ASSERT_EQ(handler.bodies.size(), 10);
    EXPECT_EQ(connection_count(), 1);
    EXPECT_NE(handler.bodies[0].find("first"), std::string::npos);
    EXPECT_NE(handler.bodies[1].find("second"), std::string::npos);
    std::weak_ptr<void> owner = context->_thread_local_fn_state;
    ASSERT_TRUE(function.close(context.get(), FunctionContext::THREAD_LOCAL).ok());
    EXPECT_TRUE(owner.expired());
}

TEST_F(AIRequestExecutorTest, EmbedReusesConnectionAcrossBatchesAndBlocksWithNulls) {
    handler.set_responses({{HttpStatus::OK, R"({"data":[{"index":0,"embedding":[0.5,1.0]}]})"}});
    FunctionEmbed function;
    for (int block_index = 0; block_index < 5; ++block_index) {
        Block block;
        block.insert({ColumnHelper::create_column<DataTypeString>({"resource"}),
                      std::make_shared<DataTypeString>(), "resource"});
        block.insert({ColumnHelper::create_nullable_column<DataTypeString>(
                              {"first", "ignored", "second"}, {0, 1, 0}),
                      make_nullable(std::make_shared<DataTypeString>()), "input"});
        block.insert({nullptr,
                      make_nullable(std::make_shared<DataTypeArray>(
                              make_nullable(std::make_shared<DataTypeFloat32>()))),
                      "result"});
        ASSERT_TRUE(function.execute(context.get(), block, {0, 1}, 2, 3, config, adapter).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        ASSERT_EQ(result.size(), 3);
        EXPECT_EQ(result.get_null_map_data()[1], 1);
        const auto& arrays = assert_cast<const ColumnArray&>(result.get_nested_column());
        EXPECT_EQ(arrays.get_offsets()[0], 2);
        EXPECT_EQ(arrays.get_offsets()[1], 2);
        EXPECT_EQ(arrays.get_offsets()[2], 4);
    }
    ASSERT_EQ(handler.bodies.size(), 10);
    EXPECT_EQ(connection_count(), 1);
}

TEST_F(AIRequestExecutorTest, MultimodalEmbedReusesConnectionAcrossBatchesAndBlocks) {
    handler.set_responses({{HttpStatus::OK, R"({"data":[{"index":0,"embedding":[0.5,1.0]}]})"}});
    config.provider_type = "VOYAGE";
    config.model_name = "voyage-multimodal-3";
    adapter = std::make_shared<VoyageAIAdapter>();
    adapter->init(config);
    FunctionEmbed function;
    for (int block_index = 0; block_index < 5; ++block_index) {
        auto input = ColumnString::create();
        for (const auto& file :
             {R"({"content_type":"image/png","uri":"https://example.com/a.png"})",
              R"({"content_type":"video/mp4","uri":"https://example.com/b.mp4"})"}) {
            JsonBinaryValue value;
            ASSERT_TRUE(value.from_json_string(file).ok());
            input->insert_data(value.value(), value.size());
        }
        Block block;
        block.insert({ColumnHelper::create_column<DataTypeString>({"resource"}),
                      std::make_shared<DataTypeString>(), "resource"});
        block.insert({std::move(input), std::make_shared<DataTypeJsonb>(), "input"});
        block.insert({nullptr,
                      std::make_shared<DataTypeArray>(
                              make_nullable(std::make_shared<DataTypeFloat32>())),
                      "result"});
        ASSERT_TRUE(function.execute(context.get(), block, {0, 1}, 2, 2, config, adapter).ok());
        const auto& arrays = assert_cast<const ColumnArray&>(*block.get_by_position(2).column);
        ASSERT_EQ(arrays.size(), 2);
        EXPECT_EQ(arrays.get_offsets()[0], 2);
        EXPECT_EQ(arrays.get_offsets()[1], 4);
    }
    ASSERT_EQ(handler.bodies.size(), 10);
    EXPECT_EQ(connection_count(), 1);
    EXPECT_NE(handler.bodies[0].find("image_url"), std::string::npos);
    EXPECT_NE(handler.bodies[1].find("video_url"), std::string::npos);
}

TEST_F(AIRequestExecutorTest, ColdAndWarmConnectionsForFastAndDelayedMock) {
    handler.set_responses({{HttpStatus::OK, "ok"}});
    for (int delay_ms : {0, 5}) {
        handler.on_request = [delay_ms] {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        };
        auto measure = [&](bool reuse) {
            const size_t first_request = handler.peer_ports.size();
            const auto start = std::chrono::steady_clock::now();
            AIRequestExecutor warm;
            for (int i = 0; i < 10; ++i) {
                AIRequestExecutor cold;
                std::string response;
                ASSERT_TRUE((reuse ? warm : cold)
                                    .execute("{}", response, config, *adapter, query_ctx.get())
                                    .ok());
                EXPECT_EQ(response, "ok");
            }
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                         std::chrono::steady_clock::now() - start)
                                         .count();
            const size_t connections =
                    std::set<uint16_t>(handler.peer_ports.begin() + first_request,
                                       handler.peer_ports.end())
                            .size();
            EXPECT_EQ(connections, reuse ? 1 : 10);
            // Diagnostic timings from an ASAN local mock, not a production speedup claim.
            const std::string label =
                    std::string(reuse ? "warm_" : "cold_") + std::to_string(delay_ms) + "ms";
            RecordProperty(label + "_connections", std::to_string(connections));
            RecordProperty(label + "_total_us", std::to_string(elapsed));
        };
        measure(false);
        measure(true);
    }
}

TEST_F(AIRequestExecutorTest, AggregateReusesConnectionAcrossFlushesAndResets) {
    handler.set_responses({{HttpStatus::OK, R"({"choices":[{"message":{"content":"summary"}}]})"}});
    set_resource();
    AggregateFunctionAIAggData data;
    data.set_query_context(query_ctx.get(), std::make_shared<AIRequestExecutor>());
    const std::string first(AggregateFunctionAIAggData::MAX_CONTEXT_SIZE, 'x');
    for (int block_index = 0; block_index < 5; ++block_index) {
        data.prepare(StringRef("resource", 8), StringRef("summarize", 9));
        data.add(StringRef(first.data(), first.size()));
        data.add(StringRef("second", 6));
        EXPECT_EQ(data._execute_task(), "summary");
        data.reset();
    }
    ASSERT_EQ(handler.bodies.size(), 10);
    EXPECT_EQ(connection_count(), 1);
}

TEST_F(AIRequestExecutorTest, AggregateGroupsShareConnectionsRatherThanRetainingOnePerGroup) {
    handler.set_responses({{HttpStatus::OK, R"({"choices":[{"message":{"content":"summary"}}]})"}});
    set_resource();
    AggregateFunctionAIAgg function({std::make_shared<DataTypeString>(),
                                     std::make_shared<DataTypeString>(),
                                     std::make_shared<DataTypeString>()});
    function.set_query_context(query_ctx.get());
    std::vector<std::unique_ptr<char[]>> groups;
    for (int i = 0; i < 10; ++i) {
        auto group = std::make_unique<char[]>(function.size_of_data());
        function.create(group.get());
        auto& data = *reinterpret_cast<AggregateFunctionAIAggData*>(group.get());
        data.prepare(StringRef("resource", 8), StringRef("summarize", 9));
        data.add(StringRef("hello", 5));
        EXPECT_EQ(data._execute_task(), "summary");
        groups.push_back(std::move(group));
    }
    EXPECT_EQ(handler.bodies.size(), 10);
    EXPECT_EQ(connection_count(), 1);
    for (auto& group : groups) {
        function.destroy(group.get());
    }
}

TEST_F(AIRequestExecutorTest, RebuildsConnectionAfterServerClose) {
    handler.close_first_connection = true;
    handler.set_responses({{HttpStatus::OK, "ok"}});
    for (int i = 0; i < 3; ++i) {
        std::string response;
        ASSERT_TRUE(execute("{}", response).ok());
        EXPECT_EQ(response, "ok");
    }
    EXPECT_EQ(connection_count(), 2);
}

TEST_F(AIRequestExecutorTest, ResetsEndpointHeadersAndPayloadBetweenRequests) {
    handler.set_responses({{HttpStatus::OK, "ok"}});
    std::string response;
    ASSERT_TRUE(execute("first", response).ok());
    config.endpoint.replace(config.endpoint.size() - 3, 3, "/other");
    config.provider_type = "LOCAL";
    config.api_key.clear();
    adapter = std::make_shared<LocalAdapter>();
    adapter->init(config);
    ASSERT_TRUE(execute("second", response).ok());
    EXPECT_EQ(handler.bodies, (std::vector<std::string> {"first", "second"}));
    EXPECT_EQ(handler.authorizations, (std::vector<std::string> {"Bearer test-key", ""}));
    EXPECT_EQ(handler.paths, (std::vector<std::string> {"/ai", "/other"}));
    EXPECT_EQ(connection_count(), 1);
}

TEST_F(AIRequestExecutorTest, FailedRequestDoesNotPoisonTheNextRequest) {
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"}, {HttpStatus::OK, "ok"}});
    config.max_retries = 0;
    std::string response;
    EXPECT_FALSE(execute("first", response).ok());
    ASSERT_TRUE(execute("second", response).ok());
    EXPECT_EQ(response, "ok");
    EXPECT_EQ(connection_count(), 1);
}

TEST_F(AIRequestExecutorTest, IndependentClonedContextsCanSendConcurrently) {
    verify_concurrent_requests(false);
}

TEST_F(AIRequestExecutorTest, SharedExecutorLoansSeparateClientsForConcurrentRequests) {
    verify_concurrent_requests(true);
}

TEST_F(AIRequestExecutorTest, CancellationStopsInFlightWithoutRetriesAndDetachesQuery) {
    // Match HttpClientTest.abort_in_flight_request: stack-trace symbolization in
    // BE_TEST can take seconds after curl has already aborted the transfer.
    query_ctx->_timeout_second = 60;
    handler.set_responses({{HttpStatus::OK, "ok"}});
    std::promise<void> arrived;
    std::promise<void> release;
    auto released = release.get_future().share();
    handler.on_request = [&] {
        arrived.set_value();
        released.wait();
    };
    auto pending = std::async(std::launch::async, [&] {
        std::string response;
        return execute("cancelled", response);
    });
    arrived.get_future().wait();
    query_ctx->_exec_status.update(Status::Cancelled("test cancellation"));
    // The server cannot reply until after we observe cancellation. This checks the
    // in-flight abort callback rather than merely checking status after a reply.
    const auto completion = pending.wait_for(std::chrono::seconds(15));
    release.set_value();
    EXPECT_EQ(completion, std::future_status::ready);
    EXPECT_TRUE(pending.get().is<ErrorCode::CANCELLED>());
    // Wait for the mock handler to complete before changing its callback.
    server.stop();
    server.join();
    EXPECT_EQ(handler.bodies.size(), 1);
    // A reused lease must bind a healthy query rather than retain cancellation.
    handler.on_request = {};
    EvHttpServer next_server {0};
    ASSERT_TRUE(next_server.register_handler(POST, "/ai", &handler));
    next_server.start();
    config.endpoint = "http://127.0.0.1:" + std::to_string(next_server.get_real_port()) + "/ai";
    auto next_options = create_fake_query_options();
    next_options.__set_query_timeout(10);
    auto next_query = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), next_options);
    std::string response;
    ASSERT_TRUE(executor.execute("healthy", response, config, *adapter, next_query.get()).ok());
    EXPECT_EQ(response, "ok");
}

TEST_F(AIRequestExecutorTest, TimeoutDoesNotPoisonClientForTheNextQuery) {
    handler.set_responses({{HttpStatus::OK, "ok"}});
    handler.on_request = [] { std::this_thread::sleep_for(std::chrono::milliseconds(1500)); };
    query_ctx->_timeout_second = 1;
    config.max_retries = 0;
    std::string response;
    EXPECT_FALSE(execute("timeout", response).ok());
    // The first handler will have finished by the time the second is dispatched.
    handler.on_request = {};
    auto next_options = create_fake_query_options();
    next_options.__set_query_timeout(10);
    auto next_query = MockQueryContext::create(TUniqueId(), ExecEnv::GetInstance(), next_options);
    ASSERT_TRUE(executor.execute("healthy", response, config, *adapter, next_query.get()).ok());
    EXPECT_EQ(response, "ok");
    EXPECT_EQ(handler.bodies.size(), 2);
    EXPECT_EQ(connection_count(), 2);
}

TEST_F(AIRequestExecutorTest, SendsPayloadAndProviderHeaders) {
    handler.set_responses({{HttpStatus::OK, R"({"result":"ok"})"}});
    std::string response = "previous response";
    ASSERT_TRUE(execute(R"({"input":"hello"})", response).ok());
    EXPECT_EQ(response, R"({"result":"ok"})");
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies, std::vector<std::string>({R"({"input":"hello"})"}));
    EXPECT_EQ(handler.authorizations, std::vector<std::string>({"Bearer test-key"}));
    EXPECT_EQ(handler.content_types, std::vector<std::string>({"application/json"}));
}

TEST_F(AIRequestExecutorTest, RetryReturnsOnlyTheSuccessfulAttemptBody) {
    config.max_retries = 1;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, R"({"error":"busy"})"},
                           {HttpStatus::OK, R"({"result":"ok"})"}});
    std::string response;
    ASSERT_TRUE(execute("{}", response).ok());
    EXPECT_EQ(response, R"({"result":"ok"})");
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies, std::vector<std::string>({"{}", "{}"}));
    EXPECT_EQ(handler.peer_ports[0], handler.peer_ports[1]);
}

TEST_F(AIRequestExecutorTest, ZeroRetriesStillSendsSuccessfulInitialRequest) {
    config.max_retries = 0;
    handler.set_responses({{HttpStatus::OK, R"({"result":"ok"})"}});
    std::string response;
    ASSERT_TRUE(execute("{}", response).ok());
    EXPECT_EQ(response, R"({"result":"ok"})");
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, ZeroRetriesReturnsInitialRequestFailure) {
    config.max_retries = 0;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, R"({"error":"busy"})"}});
    std::string response;
    const auto status = execute("{}", response);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("503"), std::string::npos);
    EXPECT_EQ(response, R"({"error":"busy"})");
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, ZeroRetriesDoesNotWaitAfterFailure) {
    config.max_retries = 0;
    config.retry_delay_second = 3;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"}});
    const auto started = std::chrono::steady_clock::now();
    std::string response;
    const auto status = execute("{}", response);
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(2));
    EXPECT_TRUE(status.is<ErrorCode::HTTP_ERROR>()) << status;
    EXPECT_EQ(response, "busy");
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, FinalFailureOnlyWaitsBeforeTheRetry) {
    config.max_retries = 1;
    config.retry_delay_second = 2;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"}});
    const auto started = std::chrono::steady_clock::now();
    std::string response;
    const auto status = execute("{}", response);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    EXPECT_GE(elapsed, std::chrono::seconds(2));
    EXPECT_LT(elapsed, std::chrono::milliseconds(3500));
    EXPECT_TRUE(status.is<ErrorCode::HTTP_ERROR>()) << status;
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 2);
    ASSERT_EQ(handler.peer_ports.size(), 2);
    EXPECT_EQ(handler.peer_ports[0], handler.peer_ports[1]);
}

TEST_F(AIRequestExecutorTest, CancellationInterruptsRetryWait) {
    query_ctx->_timeout_second = 60;
    config.max_retries = 1;
    config.retry_delay_second = 3;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"}});
    std::promise<void> arrived;
    std::once_flag first_request;
    handler.on_request = [&] { std::call_once(first_request, [&] { arrived.set_value(); }); };
    auto pending = std::async(std::launch::async, [&] {
        std::string response;
        return execute("{}", response);
    });
    arrived.get_future().wait();
    // Let the immediate 503 complete and leave the request waiting to retry.
    EXPECT_EQ(pending.wait_for(std::chrono::milliseconds(300)), std::future_status::timeout);
    query_ctx->_exec_status.update(Status::Cancelled("cancel during AI retry wait"));
    EXPECT_EQ(pending.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_TRUE(pending.get().is<ErrorCode::CANCELLED>());
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, QueryTimeoutInterruptsRetryWait) {
    query_ctx->_timeout_second = 1;
    config.max_retries = 1;
    config.retry_delay_second = 3;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"}});
    const auto started = std::chrono::steady_clock::now();
    std::string response;
    const auto status = execute("{}", response);
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(2));
    EXPECT_TRUE(status.is<ErrorCode::TIMEOUT>()) << status;
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, MaxRetriesCountsRetriesAfterInitialAttempt) {
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, R"({"error":"busy"})"}});
    std::string response;
    const auto status = execute("{}", response);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("503"), std::string::npos);
    EXPECT_NE(status.to_string().find("busy"), std::string::npos);
    EXPECT_EQ(response, R"({"error":"busy"})");
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 4);
}

TEST_F(AIRequestExecutorTest, AuthenticationErrorsFailWithoutRetry) {
    handler.set_responses({{HttpStatus::UNAUTHORIZED, R"({"error":"invalid key"})"}});
    std::string response;
    EXPECT_FALSE(execute("{}", response).ok());
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, AuthenticationSetupFailureDoesNotReadResponseOrRetry) {
    class FailedAuthenticationAdapter : public OpenAIAdapter {
    public:
        Status set_authentication(HttpClient*) const override {
            return Status::InternalError("authentication setup failed");
        }
    } failed_adapter;
    std::string response;
    AIRequestStats stats;
    const auto status =
            executor.execute("{}", response, config, failed_adapter, query_ctx.get(), &stats);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("authentication setup failed"), std::string::npos);
    EXPECT_EQ(stats.attempts, 0);
    EXPECT_TRUE(handler.bodies.empty());
}

TEST_F(AIRequestExecutorTest, PermanentHttpErrorsFailWithoutRetry) {
    for (auto code : {400, 401, 403, 404, 422}) {
        handler.set_responses({{static_cast<HttpStatus>(code), "invalid request"}});
        const auto before = handler.bodies.size();
        std::string response;
        EXPECT_FALSE(execute("{}", response).ok());
        EXPECT_EQ(handler.bodies.size(), before + 1) << code;
    }
}

TEST_F(AIRequestExecutorTest, RetryAfterExceedingQueryBudgetDoesNotSendAgain) {
    handler.retry_after = "60";
    handler.set_responses({{HttpStatus::TOO_MANY_REQUESTS, "rate limited"}});
    const auto started = std::chrono::steady_clock::now();
    std::string response;
    const auto status = execute("{}", response);
    EXPECT_TRUE(status.is<ErrorCode::TIMEOUT>()) << status;
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(2));
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, TruncatedAuthenticationResponseDoesNotRetry) {
    handler.truncate_response = true;
    handler.set_responses({{HttpStatus::UNAUTHORIZED, "invalid key"}});
    std::string response;
    AIRequestStats stats;
    EXPECT_FALSE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_EQ(stats.attempts, 1);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, TruncatedRateLimitResponsesUseConfiguredRetryBudget) {
    query_ctx->_timeout_second = 60;
    handler.truncate_response = true;
    config.max_retries = 5;
    handler.set_responses({{HttpStatus::TOO_MANY_REQUESTS, "throttled"}});
    std::string response;
    AIRequestStats stats;
    EXPECT_FALSE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_EQ(stats.attempts, 6);
    EXPECT_EQ(handler.bodies.size(), 6);
}

TEST_F(AIRequestExecutorTest, TruncatedSuccessfulResponseCanRecover) {
    query_ctx->_timeout_second = 60;
    handler.truncate_first_response = true;
    handler.set_responses({{HttpStatus::OK, "partial"}, {HttpStatus::OK, "success"}});
    std::string response;
    AIRequestStats stats;
    EXPECT_TRUE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_EQ(stats.attempts, 2);
    EXPECT_EQ(response, "success");
    EXPECT_EQ(handler.bodies.size(), 2);
}

TEST_F(AIRequestExecutorTest, TransientHttpStatusPolicy) {
    for (auto code : {408, 500, 502, 503, 504, 529, 429}) {
        const auto before = handler.bodies.size();
        config.max_retries = 1;
        handler.set_responses({{static_cast<HttpStatus>(code), "busy"}});
        AIRequestStats stats;
        std::string response;
        EXPECT_FALSE(
                executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
        EXPECT_EQ(handler.bodies.size(), before + 2) << code;
        EXPECT_EQ(stats.attempts, 2);
    }
}

TEST_F(AIRequestExecutorTest, StructuredProviderErrorsDoNotOverrideHttpStatus) {
    const std::vector<std::pair<std::string, std::string>> cases {
            {"OPENAI", R"({"error":{"code":"rate_limit_exceeded"}})"},
            {"OPENAI", R"({"error":{"type":"server_error"}})"},
            {"ANTHROPIC", R"({"error":{"type":"overloaded_error"}})"},
            {"GEMINI", R"({"error":{"status":"UNAVAILABLE"}})"},
            {"GEMINI", R"({"error":{"status":"RESOURCE_EXHAUSTED"}})"}};
    for (const auto& [provider, body] : cases) {
        config.provider_type = provider;
        adapter = AIAdapterFactory::create_adapter(provider);
        adapter->init(config);
        config.max_retries = 1;
        handler.set_responses({{HttpStatus::BAD_REQUEST, body}});
        const auto before = handler.bodies.size();
        std::string response;
        EXPECT_FALSE(execute("{}", response).ok());
        EXPECT_EQ(handler.bodies.size(), before + 1) << provider << body;
    }
}

TEST_F(AIRequestExecutorTest, AccountQuota429UsesRateLimitBudget) {
    handler.set_responses(
            {{HttpStatus::TOO_MANY_REQUESTS,
              R"({"error":{"code":"insufficient_quota","type":"rate_limit_error"}})"}});
    std::string response;
    EXPECT_FALSE(execute("{}", response).ok());
    EXPECT_EQ(handler.bodies.size(), 4);
}

TEST_F(AIRequestExecutorTest, ErrorMessageTextDoesNotControlRetryPolicy) {
    handler.set_responses({{HttpStatus::BAD_REQUEST,
                            R"({"error":{"message":"rate_limit_exceeded server_error"}})"}});
    std::string response;
    EXPECT_FALSE(execute("{}", response).ok());
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, PermanentFailureLatencyAndAttemptsComparedWithLegacyPolicy) {
    handler.set_responses({{HttpStatus::UNAUTHORIZED, "invalid key"}});
    // Replay the previous executor policy: retry every error, with a fixed one-second wait.
    const auto legacy_start = std::chrono::steady_clock::now();
    replay_legacy_authentication_retry_policy();
    const auto legacy_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - legacy_start)
                                   .count();
    const auto legacy_attempts = handler.bodies.size();
    config.retry_delay_second = 1;
    const auto current_start = std::chrono::steady_clock::now();
    std::string response;
    AIRequestStats stats;
    EXPECT_FALSE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    const auto current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - current_start)
                                    .count();
    EXPECT_EQ(legacy_attempts, 4);
    EXPECT_EQ(stats.attempts, 1);
    EXPECT_EQ(stats.retry_wait_ms, 0);
    EXPECT_GE(legacy_ms, 3000);
    EXPECT_LT(current_ms, legacy_ms);
    RecordProperty("legacy_auth_attempts", static_cast<int>(legacy_attempts));
    RecordProperty("current_auth_attempts", static_cast<int>(stats.attempts));
    RecordProperty("legacy_auth_ms", static_cast<int>(legacy_ms));
    RecordProperty("current_auth_ms", static_cast<int>(current_ms));
}

TEST_F(AIRequestExecutorTest, FixedRetryDelayDoesNotGrowAcrossAttempts) {
    use_fake_clock();
    config.retry_delay_second = 1;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"},
                           {HttpStatus::SERVICE_UNAVAILABLE, "busy"},
                           {HttpStatus::SERVICE_UNAVAILABLE, "busy"},
                           {HttpStatus::OK, "success"}});
    AIRequestStats stats;
    std::string response;
    EXPECT_TRUE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_EQ(stats.attempts, 4);
    EXPECT_EQ(stats.retry_wait_ms, 3000);
    EXPECT_EQ(response, "success");
    EXPECT_EQ(connection_count(), 1);
}

TEST_F(AIRequestExecutorTest, MixedFailuresKeepTheConfiguredRetryInterval) {
    use_fake_clock();
    config.retry_delay_second = 1;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"},
                           {HttpStatus::TOO_MANY_REQUESTS, "throttled"},
                           {HttpStatus::SERVICE_UNAVAILABLE, "busy"},
                           {HttpStatus::TOO_MANY_REQUESTS, "throttled"}});
    AIRequestStats stats;
    std::string response;
    EXPECT_FALSE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_EQ(stats.attempts, 4);
    EXPECT_EQ(stats.retry_wait_ms, 3000);
    EXPECT_EQ(handler.bodies.size(), 4);
}

TEST_F(AIRequestExecutorTest, RateLimitAndServerErrorsShareConfiguredRetryBudget) {
    config.max_retries = 5;
    handler.set_responses({{HttpStatus::TOO_MANY_REQUESTS, "throttled"}});
    AIRequestStats stats;
    std::string response;
    EXPECT_FALSE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_EQ(stats.attempts, 6);
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"}});
    EXPECT_FALSE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_EQ(stats.attempts, 6);
}

TEST_F(AIRequestExecutorTest, ScalarEmbedAndAggregateShareTheErrorPolicy) {
    set_resource();
    AggregateFunctionAIAggData data;
    data.set_query_context(query_ctx.get(), std::make_shared<AIRequestExecutor>());
    data.prepare(StringRef("resource", 8), StringRef("summarize", 9));
    std::string response;
    for (auto code : {400, 401, 403, 429, 503}) {
        handler.set_responses({{static_cast<HttpStatus>(code), "error"}});
        const auto expected = code == 429 || code == 503 ? 4 : 1;
        auto before = handler.bodies.size();
        EXPECT_FALSE(FunctionAISummarize()
                             .send_request_to_llm("{}", response, config, adapter, context.get())
                             .ok());
        EXPECT_EQ(handler.bodies.size(), before + expected) << "scalar " << code;
        before = handler.bodies.size();
        EXPECT_FALSE(FunctionEmbed()
                             .send_request_to_llm("{}", response, config, adapter, context.get())
                             .ok());
        EXPECT_EQ(handler.bodies.size(), before + expected) << "embed " << code;
        before = handler.bodies.size();
        EXPECT_FALSE(data.send_request_to_ai("{}", response).ok());
        EXPECT_EQ(handler.bodies.size(), before + expected) << "aggregate " << code;
    }
}

TEST_F(AIRequestExecutorTest, ParseFailuresDoNotResendSuccessfulHttpRequests) {
    handler.set_responses({{HttpStatus::OK, "malformed json"}});
    std::vector<std::vector<float>> results;
    EXPECT_FALSE(FunctionEmbed()
                         ._execute_prebuilt_embedding_request("{}", results, 1, config, adapter,
                                                              context.get())
                         .ok());
    EXPECT_EQ(handler.bodies.size(), 1);
    set_resource();
    AggregateFunctionAIAggData data;
    data.set_query_context(query_ctx.get(), std::make_shared<AIRequestExecutor>());
    data.prepare(StringRef("resource", 8), StringRef("summarize", 9));
    data.add(StringRef("hello", 5));
    EXPECT_THROW(data._execute_task(), Exception);
    EXPECT_EQ(handler.bodies.size(), 2);
}

TEST_F(AIRequestExecutorTest, RetryAfterSetsWaitFloorEvenWithExplicitZeroDelay) {
    use_fake_clock();
    config.max_retries = 1;
    handler.retry_after = "2";
    handler.set_responses(
            {{HttpStatus::TOO_MANY_REQUESTS, "throttled"}, {HttpStatus::OK, "success"}});
    AIRequestStats stats;
    std::string response;
    EXPECT_TRUE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_EQ(stats.attempts, 2);
    EXPECT_EQ(stats.retry_wait_ms, 2000);
}

TEST_F(AIRequestExecutorTest, RetryAfterHttpDateSetsWaitFloor) {
    use_fake_clock();
    config.max_retries = 1;
    const auto unix_now = std::time(nullptr);
    const auto date = unix_now + 3;
    std::tm utc {};
    gmtime_r(&date, &utc);
    char header[64];
    std::strftime(header, sizeof(header), "%a, %d %b %Y %H:%M:%S GMT", &utc);
    handler.retry_after = header;
    handler.set_responses(
            {{HttpStatus::TOO_MANY_REQUESTS, "throttled"}, {HttpStatus::OK, "success"}});
    AIRequestStats stats;
    std::string response;
    EXPECT_TRUE(executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats).ok());
    EXPECT_GE(stats.retry_wait_ms, 2000);
    EXPECT_LE(stats.retry_wait_ms, 3000);
}

TEST_F(AIRequestExecutorTest, OneDeadlineCoversEveryAttemptAndWait) {
    use_fake_clock();
    config.retry_delay_second = 4;
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"}});
    AIRequestStats stats;
    std::string response;
    const auto status = executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats);
    EXPECT_TRUE(status.is<ErrorCode::TIMEOUT>()) << status;
    EXPECT_EQ(stats.attempts, 3);
    EXPECT_EQ(stats.retry_wait_ms, 8000);
    EXPECT_EQ(handler.bodies.size(), 3);
}

TEST_F(AIRequestExecutorTest, OversleptRetryWaitDoesNotSendAfterDeadline) {
    use_fake_clock();
    config.retry_delay_second = 1;
    executor._sleep = [&](auto) { fake_now += std::chrono::seconds(20); };
    handler.set_responses({{HttpStatus::SERVICE_UNAVAILABLE, "busy"}});
    std::string response;
    AIRequestStats stats;
    const auto status = executor.execute("{}", response, config, *adapter, query_ctx.get(), &stats);
    EXPECT_TRUE(status.is<ErrorCode::TIMEOUT>()) << status;
    EXPECT_EQ(stats.attempts, 1);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, RetryAfterParsingIsDeterministic) {
    EXPECT_EQ(AIRequestExecutor::parse_retry_after_ms(" 2 ", 0), 2000);
    EXPECT_EQ(AIRequestExecutor::parse_retry_after_ms("0", 0), 0);
    EXPECT_EQ(AIRequestExecutor::parse_retry_after_ms("-1", 0), 0);
    EXPECT_EQ(AIRequestExecutor::parse_retry_after_ms("1.5", 0), 0);
    EXPECT_EQ(AIRequestExecutor::parse_retry_after_ms("invalid", 0), 0);
    EXPECT_EQ(AIRequestExecutor::parse_retry_after_ms("Wed, 21 Oct 2015 07:28:00 GMT", 1445412479),
              1000);
    EXPECT_EQ(AIRequestExecutor::parse_retry_after_ms("Wed, 21 Oct 2015 07:28:00 GMT", 1445412481),
              0);
    EXPECT_EQ(AIRequestExecutor::parse_retry_after_ms("9999999999999999999999999", 0),
              std::numeric_limits<int64_t>::max());
}

TEST_F(AIRequestExecutorTest, TransportFailuresUseCodesRatherThanStatusStrings) {
    for (auto code :
         {CURLE_COULDNT_RESOLVE_HOST, CURLE_COULDNT_CONNECT, CURLE_SEND_ERROR, CURLE_RECV_ERROR,
          CURLE_GOT_NOTHING, CURLE_PARTIAL_FILE, CURLE_HTTP2_STREAM}) {
        EXPECT_EQ(AIRequestExecutor::classify_curl_error(code), AIErrorType::RETRYABLE);
    }
    for (auto code : {CURLE_OPERATION_TIMEDOUT, CURLE_ABORTED_BY_CALLBACK, CURLE_OUT_OF_MEMORY,
                      CURLE_WRITE_ERROR, CURLE_PEER_FAILED_VERIFICATION, CURLE_URL_MALFORMAT}) {
        EXPECT_EQ(AIRequestExecutor::classify_curl_error(code), AIErrorType::NON_RETRYABLE);
    }
}

TEST_F(AIRequestExecutorTest, ExpiredQueryDoesNotSend) {
    query_ctx->_timeout_second = 0;
    std::string response;
    const auto status = execute("{}", response);
    EXPECT_TRUE(status.is<ErrorCode::TIMEOUT>()) << status;
    std::lock_guard lock(handler.mutex);
    EXPECT_TRUE(handler.bodies.empty());
}

TEST_F(AIRequestExecutorTest, LocalProviderSetsJsonHeadersWithoutAnApiKey) {
    handler.set_responses({{HttpStatus::OK, R"({"result":"ok"})"}});
    config.provider_type = "LOCAL";
    config.api_key.clear();
    adapter = std::make_shared<LocalAdapter>();
    adapter->init(config);
    std::string response;
    ASSERT_TRUE(execute("{}", response).ok());
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.content_types, std::vector<std::string>({"application/json"}));
    EXPECT_EQ(handler.authorizations, std::vector<std::string>({""}));
}

TEST_F(AIRequestExecutorTest, AggregateHttpModeRejectsNon200WithoutAddingRetries) {
    handler.set_responses({{HttpStatus::CREATED, R"({"result":"created"})"}});
    std::string response;
    const auto status = executor.execute("{}", response, config, *adapter, query_ctx.get());
    EXPECT_FALSE(status.ok());
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies.size(), 1);
}

TEST_F(AIRequestExecutorTest, MissingQueryContextDoesNotSend) {
    std::string response;
    const auto status = executor.execute("{}", response, config, *adapter, nullptr);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Query context is null"), std::string::npos);
    std::lock_guard lock(handler.mutex);
    EXPECT_TRUE(handler.bodies.empty());
}

TEST_F(AIRequestExecutorTest, EmbedUsesTheSharedSenderAndParsesVectors) {
    handler.set_responses(
            {{HttpStatus::OK,
              R"({"data":[{"index":0,"embedding":[0.5,1.0]},{"index":1,"embedding":[2.0,3.0]}]})"}});
    std::vector<std::vector<float>> results;
    const auto status = FunctionEmbed()._execute_prebuilt_embedding_request(
            R"({"input":["first","second"]})", results, 2, config, adapter, context.get());
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(results, (std::vector<std::vector<float>> {{0.5F, 1.0F}, {2.0F, 3.0F}}));
    std::lock_guard lock(handler.mutex);
    EXPECT_EQ(handler.bodies, std::vector<std::string>({R"({"input":["first","second"]})"}));
}

TEST_F(AIRequestExecutorTest, AggregateUsesTheSharedSenderAndParsesOneResult) {
    handler.set_responses({{HttpStatus::OK, R"({"choices":[{"message":{"content":"summary"}}]})"}});
    TAIResource resource;
    resource.endpoint = config.endpoint;
    resource.provider_type = config.provider_type;
    resource.model_name = config.model_name;
    resource.api_key = config.api_key;
    resource.max_retries = config.max_retries;
    resource.retry_delay_second = config.retry_delay_second;
    query_ctx->set_ai_resources(std::map<std::string, TAIResource> {{"resource", resource}});
    AggregateFunctionAIAggData data;
    data.set_query_context(query_ctx.get(), std::make_shared<AIRequestExecutor>());
    data.prepare(StringRef("resource", 8), StringRef("summarize", 9));
    data.add(StringRef("hello", 5));
    EXPECT_EQ(data._execute_task(), "summary");
    std::lock_guard lock(handler.mutex);
    ASSERT_EQ(handler.bodies.size(), 1);
    EXPECT_NE(handler.bodies[0].find("hello"), std::string::npos);
}

} // namespace
} // namespace doris
