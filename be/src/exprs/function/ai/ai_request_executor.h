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

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common/status.h"

namespace doris {

struct AIResource;
class AIAdapter;
class QueryContext;
class HttpClient;
enum class AIErrorType {
    NO_ERROR,      // HTTP succeeded; the caller still needs to parse the response.
    NON_RETRYABLE, // Stop retrying this request.
    RETRYABLE,     // Retry within the configured count and query deadline.
};

struct AIRequestStats {
    int64_t attempts = 0;
    int64_t retry_wait_ms = 0;
};

// Shared synchronous AI transport with a client cache spanning batches and Blocks.
// Functions own payloads and result parsing; concurrent calls borrow exclusive clients.
class AIRequestExecutor {
public:
    AIRequestExecutor();
    ~AIRequestExecutor();
    AIRequestExecutor(const AIRequestExecutor&) = delete;
    AIRequestExecutor& operator=(const AIRequestExecutor&) = delete;

    // max_retries counts extra attempts for all retryable HTTP/curl errors.
    Status execute(const std::string& request_body, std::string& response, const AIResource& config,
                   const AIAdapter& adapter, QueryContext* query_context,
                   AIRequestStats* stats = nullptr);

private:
    using Clock = std::chrono::steady_clock;
    static int64_t calculate_retry_delay_ms(int32_t base_seconds);
    static int64_t parse_retry_after_ms(const std::string& value, int64_t unix_seconds);
    static AIErrorType classify_http_status(long http_status);
    static AIErrorType classify_curl_error(int curl_code);
    static AIErrorType classify_response(int curl_code, long http_status, const AIResource& config,
                                         const std::string& response, Status& status);
    Status check_query_status(QueryContext* query_context, Clock::time_point deadline) const;
    Status wait_for_retry(int64_t delay_ms, QueryContext* query_context, Clock::time_point deadline,
                          AIRequestStats& stats) const;

    // Return the original error when retries stop; otherwise wait before the next send.
    Status prepare_retry(HttpClient& client, const AIResource& config, QueryContext* query_context,
                         Clock::time_point deadline, AIErrorType error,
                         const Status& request_status, AIRequestStats& stats) const;

    Status execute_single_request(HttpClient* client, const std::string& request_body,
                                  std::string& response, const AIResource& config,
                                  const AIAdapter& adapter, QueryContext* query_context,
                                  Clock::time_point deadline, AIRequestStats& stats);

    // Clock hooks keep waiting tests deterministic without real sleeps.
    std::function<Clock::time_point()> _now;
    std::function<void(Clock::duration)> _sleep;
    struct ClientCache;
    std::unique_ptr<ClientCache> _clients;
};

} // namespace doris
