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

#include <algorithm>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "exprs/function/ai/ai_adapter.h"
#include "runtime/query_context.h"
#include "service/http/http_client.h"
#include "util/defer_op.h"
#include "util/security.h"

namespace doris {
namespace {

Status wait_before_retry(int32_t delay_seconds, QueryContext* query_context) {
    const auto retry_at = std::chrono::steady_clock::now() + std::chrono::seconds(delay_seconds);
    while (true) {
        if (query_context != nullptr) {
            if (query_context->is_cancelled()) {
                return query_context->exec_status();
            }
            if (query_context->get_remaining_query_time_seconds() <= 0) {
                return Status::TimedOut("Query timeout exceeded during AI retry wait");
            }
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= retry_at) {
            return Status::OK();
        }
        // Poll query state during backoff, including before the next attempt.
        std::this_thread::sleep_until(std::min(retry_at, now + std::chrono::milliseconds(100)));
    }
}

// Execute one attempt using the caller's borrowed client.
Status send_request(HttpClient* client, const std::string& request_body, std::string& response,
                    const AIResource& config, const AIAdapter& adapter, QueryContext* query_context,
                    bool fail_on_http_error) {
    // Discard any error body or partial response from the preceding attempt.
    response.clear();
    // init() resets request options/headers while preserving the connection cache.
    RETURN_IF_ERROR(client->init(config.endpoint, fail_on_http_error));
    if (query_context == nullptr) {
        return Status::InternalError("Query context is null");
    }
    if (query_context->is_cancelled()) {
        return query_context->exec_status();
    }

    const int64_t remaining_query_time = query_context->get_remaining_query_time_seconds();
    if (remaining_query_time <= 0) {
        return Status::TimedOut("Query timeout exceeded before AI request");
    }
    client->set_timeout_ms(remaining_query_time * 1000);

    // Adapters also set provider headers, including JSON Content-Type for LOCAL.
    RETURN_IF_ERROR(adapter.set_authentication(client));

    // Rebind after init(), then detach before returning the client to the cache.
    client->set_abort_callback([query_context] { return query_context->is_cancelled(); });

    Status status = client->execute_post_request(request_body, &response);
    client->set_abort_callback({});
    if (query_context->is_cancelled()) {
        return query_context->exec_status();
    }
    if (!status.ok()) {
        LOG(INFO) << "AI HTTP request failed before status validation, provider="
                  << config.provider_type << ", model=" << config.model_name
                  << ", endpoint=" << mask_token(config.endpoint)
                  << ", exec_status=" << status.to_string() << ", response_body=" << response;
        return status;
    }

    const long http_status = client->get_http_status();
    // Preserve scalar/EMBED non-200 errors; AI_AGG uses curl's error mode.
    if (!fail_on_http_error && http_status != 200) {
        return Status::HttpError("http status code is not 200, code={}, url={}, response_body={}",
                                 http_status, mask_token(config.endpoint), response);
    }
    return Status::OK();
}

} // namespace

// Borrowing removes a client from idle, ensuring exclusive use of its easy handle.
struct AIRequestExecutor::ClientCache {
    std::unique_ptr<HttpClient> borrow() {
        {
            std::lock_guard lock(mutex);
            if (!idle.empty()) {
                auto client = std::move(idle.back());
                idle.pop_back();
                return client;
            }
        }
        return std::make_unique<HttpClient>();
    }

    void release(std::unique_ptr<HttpClient> client) {
        // Bound idle clients, not TCP connections or in-flight requests.
        // Excess clients are destroyed after releasing the lock.
        constexpr size_t max_idle_clients = 8;
        std::lock_guard lock(mutex);
        if (idle.size() < max_idle_clients) {
            idle.push_back(std::move(client));
        }
    }

    std::mutex mutex;
    std::vector<std::unique_ptr<HttpClient>> idle;
};

AIRequestExecutor::AIRequestExecutor() : _clients(std::make_unique<ClientCache>()) {}
AIRequestExecutor::~AIRequestExecutor() = default;

Status AIRequestExecutor::execute(const std::string& request_body, std::string& response,
                                  const AIResource& config, const AIAdapter& adapter,
                                  QueryContext* query_context, bool fail_on_http_error) {
    auto client = _clients->borrow();
    // Return the client on every exit path; network I/O holds no cache lock.
    Defer return_client([&] { _clients->release(std::move(client)); });
    Status status;
    for (int64_t attempt = 0; attempt <= config.max_retries; ++attempt) {
        status = send_request(client.get(), request_body, response, config, adapter, query_context,
                              fail_on_http_error);
        if (query_context != nullptr && query_context->is_cancelled()) {
            return query_context->exec_status();
        }
        if (status.ok()) {
            const long http_status = client->get_http_status();
            // Preserve AI_AGG's immediate rejection of transport-successful non-200.
            if (http_status != 200) {
                return Status::HttpError("http status code is not 200, code={}, url={}",
                                         http_status, mask_token(config.endpoint));
            }
            return status;
        }
        if (attempt == config.max_retries) {
            return status;
        }
        RETURN_IF_ERROR(wait_before_retry(config.retry_delay_second, query_context));
    }
    return status;
}

} // namespace doris
