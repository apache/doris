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
#include <charconv>
#include <chrono>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>

#include "exprs/function/ai/ai_adapter.h"
#include "runtime/query_context.h"
#include "service/http/http_client.h"
#include "util/defer_op.h"
#include "util/security.h"

namespace doris {
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
        (*client).reset();
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

AIRequestExecutor::AIRequestExecutor()
        : _now([] { return Clock::now(); }),
          _sleep([](Clock::duration delay) { std::this_thread::sleep_for(delay); }),
          _clients(std::make_unique<ClientCache>()) {}
AIRequestExecutor::~AIRequestExecutor() = default;

AIErrorType AIRequestExecutor::classify_http_status(long http_status) {
    switch (http_status) {
    case 200:
        return AIErrorType::NO_ERROR;
    case 429:
    case 408:
    case 500:
    case 502:
    case 503:
    case 504:
    case 529: // Claude reports temporary overload with this status.
        return AIErrorType::RETRYABLE;
    default:
        return AIErrorType::NON_RETRYABLE;
    }
}

AIErrorType AIRequestExecutor::classify_curl_error(int curl_code) {
    switch (curl_code) {
    case CURLE_COULDNT_RESOLVE_PROXY:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_CONNECT:
    case CURLE_SEND_ERROR:
    case CURLE_RECV_ERROR:
    case CURLE_GOT_NOTHING:
    case CURLE_PARTIAL_FILE:
    case CURLE_HTTP2:
    case CURLE_HTTP2_STREAM:
        return AIErrorType::RETRYABLE;
    default:
        // Cancellation, timeout, TLS/configuration and local failures are terminal.
        return AIErrorType::NON_RETRYABLE;
    }
}

int64_t AIRequestExecutor::calculate_retry_delay_ms(int32_t base_seconds) {
    return static_cast<int64_t>(base_seconds) * 1000;
}

int64_t AIRequestExecutor::parse_retry_after_ms(const std::string& value, int64_t unix_seconds) {
    const auto first = value.find_first_not_of(" \t");
    if (first == std::string::npos) {
        return 0;
    }
    const auto last = value.find_last_not_of(" \t");
    const std::string trimmed = value.substr(first, last - first + 1);
    int64_t seconds = 0;
    if (trimmed.find_first_not_of("0123456789") == std::string::npos) {
        const auto parsed =
                std::from_chars(trimmed.data(), trimmed.data() + trimmed.size(), seconds);
        if (parsed.ec == std::errc::result_out_of_range) {
            return std::numeric_limits<int64_t>::max();
        }
    } else {
        if (trimmed.find(':') == std::string::npos) {
            return 0;
        }
        const auto date = curl_getdate(trimmed.c_str(), nullptr);
        if (date < 0 || date <= unix_seconds) {
            return 0;
        }
        seconds = date - unix_seconds;
    }
    return seconds > std::numeric_limits<int64_t>::max() / 1000
                   ? std::numeric_limits<int64_t>::max()
                   : seconds * 1000;
}

Status AIRequestExecutor::check_query_status(QueryContext* query_context,
                                             Clock::time_point deadline) const {
    if (query_context->is_cancelled()) {
        return query_context->exec_status();
    }
    if (_now() >= deadline || query_context->get_remaining_query_time_seconds() <= 0) {
        return Status::TimedOut("Query timeout exceeded during AI request");
    }
    return Status::OK();
}

Status AIRequestExecutor::wait_for_retry(int64_t delay_ms, QueryContext* query_context,
                                         Clock::time_point deadline, AIRequestStats& stats) const {
    RETURN_IF_ERROR(check_query_status(query_context, deadline));
    const auto started = _now();
    const auto remaining_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - started).count();
    // Never truncate Retry-After to the remaining budget and send earlier than requested.
    if (delay_ms >= remaining_ms) {
        return Status::TimedOut("AI retry delay {} ms exceeds remaining query budget {} ms",
                                delay_ms, remaining_ms);
    }
    Defer record_wait([&] {
        stats.retry_wait_ms +=
                std::chrono::duration_cast<std::chrono::milliseconds>(_now() - started).count();
    });
    const auto retry_at = started + std::chrono::milliseconds(delay_ms);
    while (_now() < retry_at) {
        RETURN_IF_ERROR(check_query_status(query_context, deadline));
        _sleep(std::min<Clock::duration>(retry_at - _now(), std::chrono::milliseconds(100)));
    }
    return check_query_status(query_context, deadline);
}

AIErrorType AIRequestExecutor::classify_response(int curl_code, long http_status,
                                                 const AIResource& config,
                                                 const std::string& response, Status& status) {
    if (!status.ok()) {
        if (curl_code == CURLE_OPERATION_TIMEDOUT) {
            status = Status::TimedOut("AI HTTP attempt timed out, endpoint={}",
                                      mask_token(config.endpoint));
            return AIErrorType::NON_RETRYABLE;
        }
        const auto error = classify_curl_error(curl_code);
        // Received HTTP error headers still govern partial/disconnected responses.
        return error == AIErrorType::RETRYABLE && http_status >= 400
                       ? classify_http_status(http_status)
                       : error;
    }
    const auto error = classify_http_status(http_status);
    if (error != AIErrorType::NO_ERROR) {
        status = Status::HttpError("http status code is not 200, code={}, url={}, response_body={}",
                                   http_status, mask_token(config.endpoint), response);
    }
    return error;
}

Status AIRequestExecutor::execute_single_request(
        HttpClient* client, const std::string& request_body, std::string& response,
        const AIResource& config, const AIAdapter& adapter, QueryContext* query_context,
        Clock::time_point deadline, AIRequestStats& stats) {
    // Read HTTP error bodies for diagnostics, using the same mode for all AI functions.
    RETURN_IF_ERROR(client->init(config.endpoint, false));
    RETURN_IF_ERROR(adapter.set_authentication(client));

    // This attempt shares the logical request's deadline.
    const auto remaining_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - _now()).count();
    RETURN_IF_ERROR(check_query_status(query_context, deadline));
    client->set_timeout_ms(std::max<int64_t>(1, remaining_ms));

    // Check cancellation during the transfer, then clear the callback before reuse.
    client->set_abort_callback([this, query_context, deadline] {
        return query_context->is_cancelled() || _now() >= deadline;
    });
    ++stats.attempts;
    Status status = client->execute_post_request(request_body, &response);
    client->set_abort_callback({});
    return status;
}

Status AIRequestExecutor::prepare_retry(HttpClient& client, const AIResource& config,
                                        QueryContext* query_context, Clock::time_point deadline,
                                        AIErrorType error, const Status& request_status,
                                        AIRequestStats& stats) const {
    // Stop on non-retryable errors or exhausted retry budgets.
    if (error == AIErrorType::NON_RETRYABLE || stats.attempts - 1 >= config.max_retries) {
        return request_status;
    }

    // Keep the configured interval fixed; Retry-After may require a longer wait.
    int64_t delay_ms = calculate_retry_delay_ms(config.retry_delay_second);
    std::string retry_after;
    RETURN_IF_ERROR(client.get_response_header("Retry-After", &retry_after));
    const auto unix_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count();
    delay_ms = std::max(delay_ms, parse_retry_after_ms(retry_after, unix_seconds));

    // Wait within the query deadline, polling for cancellation before the next send.
    LOG(INFO) << "AI retry scheduled, next_attempt=" << stats.attempts + 1
              << ", delay_ms=" << delay_ms;
    return wait_for_retry(delay_ms, query_context, deadline, stats);
}

Status AIRequestExecutor::execute(const std::string& request_body, std::string& response,
                                  const AIResource& config, const AIAdapter& adapter,
                                  QueryContext* query_context, AIRequestStats* output_stats) {
    // Preserve statistics on success, failure, cancellation and timeout.
    AIRequestStats stats;
    Defer publish_stats([&] {
        if (output_stats != nullptr) {
            *output_stats = stats;
        }
    });

    // Establish one deadline for all attempts and waits.
    if (query_context == nullptr) {
        return Status::InternalError("Query context is null");
    }
    const auto deadline =
            _now() + std::chrono::seconds(query_context->get_remaining_query_time_seconds());
    RETURN_IF_ERROR(check_query_status(query_context, deadline));

    // Keep the borrowed client across retries, then return it for subsequent batches.
    auto client = _clients->borrow();
    Defer return_client([&] { _clients->release(std::move(client)); });

    while (true) {
        // Send one request; setup failures return without inspecting an HTTP response.
        RETURN_IF_ERROR(check_query_status(query_context, deadline));
        response.clear();
        const auto attempts_before = stats.attempts;
        Status status = execute_single_request(client.get(), request_body, response, config,
                                               adapter, query_context, deadline, stats);
        if (stats.attempts == attempts_before) {
            return status;
        }

        // Cancellation and timeout take precedence over HTTP/curl error classification.
        RETURN_IF_ERROR(check_query_status(query_context, deadline));
        const auto curl_code = client->get_last_curl_code();
        const long http_status = client->get_http_status();
        const auto error = classify_response(curl_code, http_status, config, response, status);
        if (error == AIErrorType::NO_ERROR) {
            return Status::OK();
        }

        // Record the failed transfer and provider response for diagnosis.
        LOG(INFO) << "AI request failed, provider=" << config.provider_type
                  << ", model=" << config.model_name << ", endpoint=" << mask_token(config.endpoint)
                  << ", attempt=" << stats.attempts << ", http_status=" << http_status
                  << ", curl_code=" << curl_code
                  << ", curl_error=" << curl_easy_strerror(static_cast<CURLcode>(curl_code))
                  << ", retryable=" << (error != AIErrorType::NON_RETRYABLE)
                  << ", response_body=" << response;

        // Complete retry checks and waiting before starting another request.
        RETURN_IF_ERROR(
                prepare_retry(*client, config, query_context, deadline, error, status, stats));
    }
}

} // namespace doris
